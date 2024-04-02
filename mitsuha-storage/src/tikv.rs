use std::path::Path;
use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use mitsuha_core::err_unsupported_op;
use mitsuha_core::errors::Error;
use mitsuha_core::storage::Storage;
use mitsuha_core::{
    err_unknown,
    storage::{FileSystem, GarbageCollectable, RawStorage, StorageClass},
    types,
};
use mitsuha_core_types::{kernel::StorageSpec, storage::StorageCapability};
use mitsuha_filesystem::constant::NativeFileSystemConstants;
use mitsuha_filesystem::util::PathExt;
use mitsuha_filesystem::{NativeFileLease, NativeFileMetadata};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tikv_client::{Key, KvPair, Transaction, TransactionClient};

use crate::conf::ConfKey;
use crate::util;
use lazy_static::lazy_static;

lazy_static! {
    static ref INTERNAL_METADATA_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.internalmetadata";
    static ref NATIVE_METADATA_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.nativemetadata";
    static ref PART_FILES_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.partfiles";
    static ref LIST_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.list";
    static ref EXIST_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.exist";
    static ref LEASE_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.tikv.lease";
}

// Maximum file part size is 128KB. Changing this value would be a breaking change.
const NORMALIZED_FILE_PART_MAX_SIZE: usize = 0x20000;

const LOWER_BOUND_SUFFIX: &str = "/";
const UPPER_BOUND_SUFFIX: &str = "0";

#[derive(strum_macros::EnumString)]
enum ConcurrencyMode {
    #[strum(serialize = "optimistic")]
    Optimistic,

    #[strum(serialize = "pessimistic")]
    Pessimistic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalMetadata {
    pub handle: String,
    pub expiry: DateTime<Utc>,
    pub ttl: u64,
}

pub struct TikvStorage {
    concurrency_mode: ConcurrencyMode,
    client: TransactionClient,
}

#[async_trait]
impl RawStorage for TikvStorage {
    async fn store(&self, spec: StorageSpec) -> types::Result<()> {
        let mut tx = self.tx().await?;

        if let Err(e) = self.store_by_tx(&mut tx, spec).await {
            tx.rollback().await.map_err(|e| err_unknown!(e))?;

            return Err(e);
        }

        tx.commit().await.map_err(|e| err_unknown!(e))?;

        Ok(())
    }

    async fn load(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let mut tx = self.tx().await?;

        match self.load_data(&mut tx, handle).await {
            Ok(data) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(data)
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn exists(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        let mut tx = self.tx().await?;

        match self.exists_data(&mut tx, handle).await {
            Ok(data) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(data)
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn persist(
        &self,
        handle: String,
        time: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        match self.persist_by_tx(&mut tx, handle, time, extensions).await {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn clear(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        match self.clear_by_tx(&mut tx, handle, extensions).await {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn capabilities(
        &self,
        _handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        Ok(vec![
            StorageCapability::StorageShared,
            StorageCapability::StorageAtomic,
        ])
    }
}

#[async_trait]
impl GarbageCollectable for TikvStorage {
    async fn garbage_collect(&self) -> types::Result<Vec<String>> {
        let lower_bound = INTERNAL_METADATA_EXT.to_string() + LOWER_BOUND_SUFFIX;
        let upper_bound = INTERNAL_METADATA_EXT.to_string() + UPPER_BOUND_SUFFIX;
        let page_size = 32u32;

        let mut lower_bound_key: Key = lower_bound.as_bytes().to_vec().into();
        let upper_bound_key: Key = upper_bound.as_bytes().to_vec().into();
        let mut deleted_handles = Vec::<String>::new();

        loop {
            let mut tx = self.tx().await?;

            let result = tx
                .scan(lower_bound_key.clone()..upper_bound_key.clone(), page_size)
                .await
                .map_err(|e| err_unknown!(e));

            if let Err(e) = result {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                return Err(e);
            }

            tx.commit().await.map_err(|e| err_unknown!(e))?;

            let kvs: Vec<KvPair> = result.unwrap().collect();

            let should_break = kvs.len() < page_size as usize;

            for kv in kvs {
                let (key, data) = (kv.0, kv.1);
                lower_bound_key = key;

                let value =
                    musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;

                let internal_metadata: InternalMetadata =
                    musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

                if internal_metadata.expiry <= Utc::now() {
                    let mut tx = self.tx().await?;

                    if let Err(e) = tx
                        .delete(internal_metadata.handle.clone())
                        .await
                        .map_err(|e| err_unknown!(e))
                    {
                        tx.rollback().await.map_err(|e| err_unknown!(e))?;
                        tracing::error!(
                            "failed to perform gc operation on handle: '{}', error: {}",
                            &internal_metadata.handle,
                            e
                        );
                        continue;
                    }

                    if let Err(e) = tx
                        .delete(lower_bound_key.clone())
                        .await
                        .map_err(|e| err_unknown!(e))
                    {
                        tx.rollback().await.map_err(|e| err_unknown!(e))?;
                        tracing::error!(
                            "failed to perform gc operation on handle: '{}', error: {}",
                            &internal_metadata.handle,
                            e
                        );
                        continue;
                    }

                    tx.commit().await.map_err(|e| err_unknown!(e))?;

                    let key_data = Vec::from(lower_bound_key.clone());
                    let key_str = String::from_utf8(key_data).map_err(|e| err_unknown!(e))?;

                    deleted_handles.push(key_str);
                }
            }

            if should_break {
                break;
            }
        }

        Ok(deleted_handles)
    }
}

#[async_trait]
impl FileSystem for TikvStorage {
    async fn store_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        ttl: u64,
        data: Vec<u8>,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let offset = part_index * part_size;

        let mut tx = self.tx().await?;

        if let Err(e) = self
            .store_normalized_file_parts(&mut tx, &handle, ttl, offset, &data, extensions.clone())
            .await
        {
            tx.rollback().await.map_err(|e| err_unknown!(e))?;

            return Err(e);
        }

        if let Err(e) = self
            .store_existence_metadata(&mut tx, handle, ttl, extensions)
            .await
        {
            tx.rollback().await.map_err(|e| err_unknown!(e))?;

            return Err(e);
        }

        tx.commit().await.map_err(|e| err_unknown!(e))?;

        Ok(())
    }

    async fn load_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let offset = part_index * part_size;

        let mut tx = self.tx().await?;

        match self
            .load_normalized_file_parts(&mut tx, &handle, offset, part_size as usize, extensions)
            .await
        {
            Ok(data) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(data)
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn get_file_part_count(
        &self,
        handle: String,
        part_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<u64> {
        let mut tx = self.tx().await?;

        let result = match self.load_total_parts_len(&mut tx, &handle).await {
            Ok(data) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(data.unwrap_or_default())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        };

        let total_bytes = result?;

        Ok(total_bytes / part_size + (total_bytes % part_size > 0) as u64)
    }

    async fn get_metadata(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<NativeFileMetadata> {
        let mut tx = self.tx().await?;

        match self.load_native_metadata(&mut tx, handle).await {
            Ok(metadata) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(metadata)
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn set_metadata(
        &self,
        handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        match self
            .store_native_metadata(&mut tx, handle, metadata, ttl, extensions)
            .await
        {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn path_exists(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        let exist_handle = Self::gen_exist_metadata_handle(&handle);
        self.exists(exist_handle, extensions).await
    }

    async fn list(
        &self,
        handle: String,
        page_index: u64,
        page_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<String>> {
        let list_metadata_handle = Self::gen_list_metadata_handle(&handle);

        let lower_bound = list_metadata_handle.clone() + LOWER_BOUND_SUFFIX;
        let upper_bound = list_metadata_handle + UPPER_BOUND_SUFFIX;

        let lower_bound_key: Key = lower_bound.as_bytes().to_vec().into();
        let upper_bound_key: Key = upper_bound.as_bytes().to_vec().into();

        let mut tx = self.tx().await?;

        let start_offset = (page_index * page_size) as usize;
        let limit = ((page_index + 1) * page_size) as u32;

        let result = match tx.scan_keys(lower_bound_key..upper_bound_key, limit).await {
            Ok(data) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(data)
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        };

        let mut count = 0;
        let mut output = Vec::with_capacity(page_index as usize);

        for key in result.map_err(|e| err_unknown!(e))?.into_iter() {
            count += 1;

            if start_offset >= count {
                continue;
            }

            let key_data = Vec::from(key);
            let full_raw_path = String::from_utf8_lossy(&key_data).to_string();
            let full_path = Path::new(&full_raw_path);

            output.push(full_path.get_file_name().map_err(|e| err_unknown!(e))?);
        }

        Ok(output)
    }

    async fn add_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        let result = match self.load_internal_metadata(&mut tx, handle.clone()).await {
            Ok(data) => Ok(data),
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        };

        let internal_metadata = result?;

        let list_metadata_handle = Self::gen_list_item_metadata_handle(&handle, &item);

        let spec = StorageSpec {
            handle: list_metadata_handle,
            data: Vec::new(),
            ttl: internal_metadata.ttl,
            extensions,
        };

        match self.store_by_tx(&mut tx, spec).await {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn remove_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let list_metadata_handle = Self::gen_list_item_metadata_handle(&handle, &item);

        self.clear(list_metadata_handle, extensions).await
    }

    async fn acquire_lease(
        &self,
        handle: String,
        lease: NativeFileLease,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        let result = self
            .acquire_lease_by_tx(&mut tx, handle, lease, ttl, extensions)
            .await;

        match result {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn renew_lease(
        &self,
        handle: String,
        lease_id: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        let result = self
            .renew_lease_by_tx(&mut tx, handle, lease_id, ttl, extensions)
            .await;

        match result {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn release_lease(
        &self,
        handle: String,
        lease_id: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        let result = self
            .release_lease_by_tx(&mut tx, handle, lease_id, extensions)
            .await;

        match result {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }

    async fn delete_path(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut tx = self.tx().await?;

        let result = self.delete_path_by_tx(&mut tx, handle, extensions).await;

        match result {
            Ok(_) => {
                tx.commit().await.map_err(|e| err_unknown!(e))?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await.map_err(|e| err_unknown!(e))?;
                Err(e)
            }
        }
    }
}

impl TikvStorage {
    pub async fn new(class: StorageClass) -> types::Result<Arc<Box<dyn Storage>>> {
        let pd_endpoints = class.get_extension_property(&ConfKey::PdEndpoints.to_string())?;

        let client = TransactionClient::new(pd_endpoints.split(",").collect())
            .await
            .map_err(|e| err_unknown!(e))?;

        let concurrency_mode = ConcurrencyMode::from_str(
            class
                .get_extension_property(&ConfKey::ConcurrencyMode.to_string())?
                .as_str(),
        )
        .map_err(|e| err_unknown!(e))?;

        let storage = Self {
            concurrency_mode,
            client,
        };

        Ok(Arc::new(Box::new(storage)))
    }

    async fn tx(&self) -> types::Result<Transaction> {
        let tx = match self.concurrency_mode {
            ConcurrencyMode::Optimistic => self
                .client
                .begin_optimistic()
                .await
                .map_err(|e| err_unknown!(e))?,
            ConcurrencyMode::Pessimistic => self
                .client
                .begin_pessimistic()
                .await
                .map_err(|e| err_unknown!(e))?,
        };

        Ok(tx)
    }

    async fn store_by_tx(&self, tx: &mut Transaction, spec: StorageSpec) -> types::Result<()> {
        let handle = spec.handle;

        let expiry = util::get_expiry(&handle, spec.ttl, &spec.extensions)?;

        let metadata = InternalMetadata {
            handle: handle.clone(),
            ttl: spec.ttl,
            expiry,
        };

        self.store_internal_metadata(tx, handle.clone(), metadata)
            .await?;

        self.store_data(tx, handle, spec.data).await?;

        Ok(())
    }

    async fn clear_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.clear_data(tx, handle.clone()).await?;
        self.clear_internal_metadata(tx, handle).await
    }

    async fn persist_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        time: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut internal_metadata = self.load_internal_metadata(tx, handle.clone()).await?;

        internal_metadata.expiry += Duration::seconds(time as i64);

        self.store_internal_metadata(tx, handle, internal_metadata)
            .await
    }

    async fn store_data(
        &self,
        tx: &mut Transaction,
        handle: String,
        data: Vec<u8>,
    ) -> types::Result<()> {
        tx.put(handle, data).await.map_err(|e| err_unknown!(e))?;
        Ok(())
    }

    async fn load_data(&self, tx: &mut Transaction, handle: String) -> types::Result<Vec<u8>> {
        let data = self.load_optional_data(tx, handle.clone()).await?;

        if data.is_none() {
            return Err(Error::StorageLoadFailed {
                message: format!("storage handle was not found: '{}'", handle),
                source: anyhow!(""),
            });
        }

        Ok(data.unwrap())
    }

    async fn load_optional_data(
        &self,
        tx: &mut Transaction,
        handle: String,
    ) -> types::Result<Option<Vec<u8>>> {
        let data = tx.get(handle).await.map_err(|e| err_unknown!(e))?;

        Ok(data)
    }

    async fn exists_data(&self, tx: &mut Transaction, handle: String) -> types::Result<bool> {
        tx.key_exists(handle).await.map_err(|e| err_unknown!(e))
    }

    async fn clear_data(&self, tx: &mut Transaction, handle: String) -> types::Result<()> {
        tx.delete(handle).await.map_err(|e| err_unknown!(e))?;
        Ok(())
    }

    fn hash_handle(handle: &String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(handle);

        hex::encode(hasher.finalize())
    }

    fn gen_internal_metadata_handle(handle: &String) -> String {
        format!(
            "{}/{}",
            INTERNAL_METADATA_EXT.as_str(),
            Self::hash_handle(handle)
        )
    }

    fn gen_native_metadata_handle(handle: &String) -> String {
        format!(
            "{}/{}",
            NATIVE_METADATA_EXT.as_str(),
            Self::hash_handle(handle)
        )
    }

    fn gen_normalized_part_file_handle(handle: &String, index: u64) -> String {
        format!(
            "{}/{}/{}",
            PART_FILES_EXT.as_str(),
            Self::hash_handle(handle),
            index
        )
    }

    fn gen_normalized_parts_handle(handle: &String) -> String {
        format!("{}/{}", PART_FILES_EXT.as_str(), Self::hash_handle(handle),)
    }

    fn gen_list_item_metadata_handle(handle: &String, item: &String) -> String {
        format!(
            "{}/{}/{}",
            LIST_EXT.as_str(),
            Self::hash_handle(handle),
            item
        )
    }

    fn gen_list_metadata_handle(handle: &String) -> String {
        format!("{}/{}", LIST_EXT.as_str(), Self::hash_handle(handle),)
    }

    fn gen_exist_metadata_handle(handle: &String) -> String {
        format!("{}/{}", EXIST_EXT.as_str(), Self::hash_handle(handle),)
    }

    fn gen_lease_handle(handle: &String) -> String {
        format!("{}/{}", LEASE_EXT.as_str(), Self::hash_handle(handle),)
    }

    fn gen_total_parts_len_handle(handle: &String) -> String {
        format!(
            "{}/{}/.length",
            PART_FILES_EXT.as_str(),
            Self::hash_handle(handle),
        )
    }

    async fn store_total_parts_len(
        &self,
        tx: &mut Transaction,
        handle: &String,
        ttl: u64,
        len: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let v = musubi_api::types::Value::U64(len);
        let data: Vec<u8> = v.try_into().map_err(|e: anyhow::Error| err_unknown!(e))?;
        let count_handle = Self::gen_total_parts_len_handle(handle);

        let spec = StorageSpec {
            handle: count_handle,
            ttl,
            data,
            extensions,
        };

        self.store_by_tx(tx, spec).await
    }

    async fn load_total_parts_len(
        &self,
        tx: &mut Transaction,
        handle: &String,
    ) -> types::Result<Option<u64>> {
        let count_handle = Self::gen_total_parts_len_handle(handle);

        let data = self.load_optional_data(tx, count_handle).await?;

        if data.is_none() {
            return Ok(None);
        }

        let v = musubi_api::types::Value::try_from(data.unwrap()).map_err(|e| err_unknown!(e))?;

        if let musubi_api::types::Value::U64(x) = v {
            Ok(Some(x))
        } else {
            Err(err_unknown!("failed to parse part count"))
        }
    }

    async fn store_normalized_file_parts(
        &self,
        tx: &mut Transaction,
        handle: &String,
        ttl: u64,
        offset: u64,
        mut data: &[u8],
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let len = data.len() as u64;
        let mut normalized_index = offset / NORMALIZED_FILE_PART_MAX_SIZE as u64;
        let part_buffer_offset = offset as usize % NORMALIZED_FILE_PART_MAX_SIZE;

        let part_buffer_unfilled = NORMALIZED_FILE_PART_MAX_SIZE - part_buffer_offset;

        let mut part_buffer = vec![0u8; NORMALIZED_FILE_PART_MAX_SIZE];

        loop {
            let part_handle = Self::gen_normalized_part_file_handle(handle, normalized_index);

            let mut buf = &data[..];
            if buf.len() > part_buffer_unfilled {
                buf = &buf[..part_buffer_unfilled];
            }

            if buf.len() == 0 {
                break;
            }

            let mut part_buffer_filled = 0usize;

            let existing_part = self
                .load_normalized_file_part(tx, handle, normalized_index)
                .await?;

            if let Some(existing_buf) = existing_part {
                part_buffer_filled = existing_buf.len();

                // forbid part file fragmentation
                if part_buffer_filled < part_buffer_offset {
                    return Err(err_unsupported_op!(
                        "part file fragmentation attempt was detected"
                    ));
                }

                for i in 0..existing_buf.len() {
                    part_buffer[i] = existing_buf[i];
                }
            }

            for i in 0..buf.len() {
                part_buffer[part_buffer_offset + i] = buf[i];
            }

            part_buffer_filled = std::cmp::max(part_buffer_filled, part_buffer_offset + buf.len());

            let spec = StorageSpec {
                handle: part_handle,
                ttl,
                data: part_buffer[..part_buffer_filled].to_vec(),
                extensions: extensions.clone(),
            };

            self.store_by_tx(tx, spec).await?;

            if buf.len() < part_buffer_unfilled {
                break;
            }

            data = &data[part_buffer_unfilled..];
            normalized_index += 1;
        }

        let existing_len = self.load_total_parts_len(tx, handle).await?;
        let new_len = offset + len;

        let count = std::cmp::max(new_len, existing_len.unwrap_or_default());

        self.store_total_parts_len(tx, handle, ttl, count, extensions)
            .await?;

        Ok(())
    }

    async fn load_normalized_file_parts(
        &self,
        tx: &mut Transaction,
        handle: &String,
        offset: u64,
        size: usize,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        if size == 0 {
            return Ok(Vec::new());
        }

        let mut normalized_index = offset / NORMALIZED_FILE_PART_MAX_SIZE as u64;
        let mut data = Vec::<u8>::with_capacity(size);

        loop {
            if data.len() == size {
                break;
            }

            let existing_part = self
                .load_normalized_file_part(tx, handle, normalized_index)
                .await?;

            if let Some(existing_buf) = existing_part {
                for i in 0..existing_buf.len() {
                    if data.len() == size {
                        break;
                    }

                    data.push(existing_buf[i]);
                }
            } else {
                break;
            }

            normalized_index += 1;
        }

        Ok(data)
    }

    async fn load_normalized_file_part(
        &self,
        tx: &mut Transaction,
        handle: &String,
        index: u64,
    ) -> types::Result<Option<Vec<u8>>> {
        let part_handle = Self::gen_normalized_part_file_handle(handle, index);

        let data = self.load_optional_data(tx, part_handle).await?;

        Ok(data)
    }

    async fn store_internal_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
        metadata: InternalMetadata,
    ) -> types::Result<()> {
        let metadata_handle = Self::gen_internal_metadata_handle(&handle);

        let value = musubi_api::types::to_value(&metadata).map_err(|e| err_unknown!(e))?;
        let data: Vec<u8> = value
            .try_into()
            .map_err(|e: anyhow::Error| err_unknown!(e))?;

        tx.put(metadata_handle, data)
            .await
            .map_err(|e| err_unknown!(e))
    }

    async fn load_internal_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
    ) -> types::Result<InternalMetadata> {
        let metadata_handle = Self::gen_internal_metadata_handle(&handle);

        let data = tx
            .get(metadata_handle)
            .await
            .map_err(|e| err_unknown!(e))?
            .ok_or(err_unknown!("failed to load internal metadata"))?;

        let value = musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;

        let internal_metadata =
            musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        Ok(internal_metadata)
    }

    async fn clear_internal_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
    ) -> types::Result<()> {
        let metadata_handle = Self::gen_internal_metadata_handle(&handle);

        self.clear_data(tx, metadata_handle).await?;
        Ok(())
    }

    async fn load_native_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
    ) -> types::Result<NativeFileMetadata> {
        let metadata_handle = Self::gen_native_metadata_handle(&handle);

        let data = tx
            .get(metadata_handle)
            .await
            .map_err(|e| err_unknown!(e))?
            .ok_or(err_unknown!("failed to load native file metadata"))?;

        let value = musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;

        let native_metadata = musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        Ok(native_metadata)
    }

    async fn store_native_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let metadata_handle = Self::gen_native_metadata_handle(&handle);

        let value = musubi_api::types::to_value(&metadata).map_err(|e| err_unknown!(e))?;
        let data: Vec<u8> = value
            .try_into()
            .map_err(|e: anyhow::Error| err_unknown!(e))?;

        let spec = StorageSpec {
            handle: metadata_handle,
            data,
            ttl,
            extensions: extensions.clone(),
        };

        self.store_by_tx(tx, spec).await?;
        self.store_existence_metadata(tx, handle, ttl, extensions)
            .await
    }

    async fn store_existence_metadata(
        &self,
        tx: &mut Transaction,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let exist_metadata_handle = Self::gen_exist_metadata_handle(&handle);

        let spec = StorageSpec {
            handle: exist_metadata_handle,
            data: Vec::new(),
            ttl,
            extensions,
        };

        self.store_by_tx(tx, spec).await
    }

    async fn delete_path_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let exist_handle = Self::gen_exist_metadata_handle(&handle);
        let internal_metadata_handle = Self::gen_internal_metadata_handle(&handle);
        let native_metadata_handle = Self::gen_native_metadata_handle(&handle);
        let part_metadata_handle = Self::gen_normalized_parts_handle(&handle);

        self.clear_by_tx(tx, exist_handle, extensions.clone())
            .await?;
        self.clear_by_tx(tx, native_metadata_handle, extensions.clone())
            .await?;

        // Start scanning all file parts including parent and remove them
        let lower_bound = part_metadata_handle.clone();
        let upper_bound = part_metadata_handle + UPPER_BOUND_SUFFIX;

        let mut lower_bound_key: Key = lower_bound.as_bytes().to_vec().into();
        let upper_bound_key: Key = upper_bound.as_bytes().to_vec().into();

        let page_size = 32u32;

        let mut read_next_page = true;

        while read_next_page {
            read_next_page = false;

            for key in tx
                .scan_keys(lower_bound_key.clone()..upper_bound_key.clone(), page_size)
                .await
                .map_err(|e| err_unknown!(e))?
            {
                lower_bound_key = key.clone();

                let key_data = Vec::from(key);
                let full_raw_path = String::from_utf8_lossy(&key_data).to_string();

                self.clear_by_tx(tx, full_raw_path, extensions.clone())
                    .await?;

                read_next_page = true;
            }
        }

        self.clear_by_tx(tx, internal_metadata_handle, extensions)
            .await
    }

    async fn acquire_lease_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        lease: NativeFileLease,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let lease_handle = Self::gen_lease_handle(&handle);

        let existing_lease_data = self.load_optional_data(tx, lease_handle.clone()).await?;
        if let Some(data) = existing_lease_data {
            let value = musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;
            let existing_lease: NativeFileLease =
                musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

            if existing_lease.id != lease.id {
                return Err(err_unsupported_op!("lease acquisition failed for id='{}' as an existing lease was found with id='{}'", lease.id, existing_lease.id));
            }
        }

        let value = musubi_api::types::to_value(&lease).map_err(|e| err_unknown!(e))?;
        let data: Vec<u8> = value
            .try_into()
            .map_err(|e: anyhow::Error| err_unknown!(e))?;

        let spec = StorageSpec {
            handle: lease_handle,
            data,
            ttl,
            extensions,
        };

        self.store_by_tx(tx, spec).await
    }

    async fn renew_lease_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        lease_id: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let lease_handle = Self::gen_lease_handle(&handle);

        let data = self.load_data(tx, lease_handle.clone()).await?;
        let value = musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;
        let existing_lease: NativeFileLease =
            musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        if existing_lease.id != lease_id {
            return Err(err_unsupported_op!(
                "lease renewal failed for id='{}' as an existing lease was found with id='{}'",
                lease_id,
                existing_lease.id
            ));
        }

        self.persist_by_tx(tx, lease_handle, ttl, extensions).await
    }

    async fn release_lease_by_tx(
        &self,
        tx: &mut Transaction,
        handle: String,
        lease_id: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let lease_handle = Self::gen_lease_handle(&handle);

        let data = self.load_data(tx, lease_handle.clone()).await?;
        let value = musubi_api::types::Value::try_from(data).map_err(|e| err_unknown!(e))?;
        let existing_lease: NativeFileLease =
            musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        if existing_lease.id != lease_id {
            return Err(err_unsupported_op!(
                "lease release failed for id='{}' as an existing lease was found with id='{}'",
                lease_id,
                existing_lease.id
            ));
        }

        self.clear_by_tx(tx, lease_handle, extensions).await
    }
}
