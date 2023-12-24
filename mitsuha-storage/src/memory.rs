use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;

use mitsuha_core::err_unknown;
use mitsuha_core::{errors::Error, types};
use mitsuha_core_types::kernel::StorageSpec;

use mitsuha_core::storage::{FileSystem, GarbageCollectable, RawStorage, Storage};
use mitsuha_core_types::storage::StorageCapability;
use mitsuha_filesystem::constant::NativeFileSystemConstants;
use mitsuha_filesystem::NativeFileMetadata;
use musubi_api::types::Value;

use crate::util;

pub struct MemoryStorage {
    store: DashMap<String, Vec<u8>>,
    expiry_map: DashMap<String, DateTime<Utc>>,
}

#[async_trait]
impl RawStorage for MemoryStorage {
    async fn store(&self, spec: StorageSpec) -> types::Result<()> {
        let handle = spec.handle;

        let expiry = util::get_expiry(&handle, spec.ttl, &spec.extensions)?;

        self.expiry_map.insert(handle.clone(), expiry);

        let data = spec.data;

        self.store.insert(handle, data);

        Ok(())
    }

    async fn load(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        match self.expiry_map.get(&handle) {
            Some(date_time) => {
                if *date_time <= Utc::now() {
                    return Err(Error::StorageLoadFailed {
                        message: format!(
                            "storage handle has already expired: '{}'",
                            handle.clone()
                        ),
                        source: anyhow::anyhow!(""),
                    });
                }
            }
            None => {
                return Err(Error::StorageLoadFailed {
                    message: format!("storage handle was not found: '{}'", handle.clone()),
                    source: anyhow::anyhow!(""),
                });
            }
        }

        match self.store.get(&handle) {
            Some(data) => Ok(data.clone()),
            None => Err(Error::StorageLoadFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: anyhow::anyhow!(""),
            }),
        }
    }

    async fn exists(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        Ok(self.store.contains_key(&handle))
    }

    async fn persist(
        &self,
        handle: String,
        time: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let value = self.expiry_map.get(&handle).map(|x| (*x).clone());

        match value {
            Some(date_time) => {
                self.expiry_map
                    .insert(handle.clone(), date_time + Duration::seconds(time as i64));
                Ok(())
            }
            None => Err(Error::StoragePersistFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: anyhow::anyhow!(""),
            }),
        }
    }

    async fn clear(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.expiry_map.remove(&handle);
        self.store.remove(&handle);

        Ok(())
    }

    async fn capabilities(
        &self,
        _handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        Ok(vec![
            StorageCapability::StorageLocal,
            StorageCapability::StorageAtomic,
        ])
    }
}

#[async_trait]
impl GarbageCollectable for MemoryStorage {
    async fn garbage_collect(&self) -> types::Result<Vec<String>> {
        let mut delete_handles = vec![];

        for entry in self.expiry_map.iter() {
            let handle = entry.key().clone();
            let expiry = entry.value().clone();

            if expiry <= Utc::now() {
                delete_handles.push(handle);
            }
        }

        for handle in delete_handles.iter() {
            self.store.remove(handle);
            self.expiry_map.remove(handle);
        }

        Ok(delete_handles)
    }
}

#[async_trait]
impl FileSystem for MemoryStorage {
    async fn store_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        ttl: u64,
        data: Vec<u8>,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut offset = (part_index * part_size) as usize;

        let mut existing_data = self
            .store
            .get_mut(&handle)
            .ok_or(err_unknown!("could not find store handle"))?;

        if offset > existing_data.len() {
            return Err(err_unknown!("offset is out of bounds"));
        }

        for byte in data.iter() {
            if existing_data.len() == offset {
                existing_data.push(*byte);
            } else {
                existing_data[offset] = *byte;
            }

            offset += 1;
        }

        let mut expiry = self
            .expiry_map
            .get_mut(&handle)
            .ok_or(err_unknown!("could not find expiry_map handle"))?;

        *expiry += Duration::seconds(ttl as i64);

        Ok(())
    }

    async fn load_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let offset = (part_index * part_size) as usize;

        let data = self
            .store
            .get(&handle)
            .ok_or(err_unknown!("could not find store handle"))?;

        if offset >= data.len() {
            return Ok(vec![]);
        }

        let end_offset = offset + part_size as usize;

        if data.len() >= end_offset {
            Ok(data[offset..end_offset].to_vec())
        } else {
            Ok(data[offset..].to_vec())
        }
    }

    async fn get_file_part_count(
        &self,
        handle: String,
        part_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<u64> {
        let data = self
            .store
            .get(&handle)
            .ok_or(err_unknown!("could not find store handle"))?;

        let data_len = data.len() as u64;
        let part_count = data_len / part_size + (data_len % part_size > 0) as u64;

        Ok(part_count)
    }

    async fn get_metadata(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<NativeFileMetadata> {
        let metadata_handle = format!(
            "{}/{}",
            handle,
            NativeFileSystemConstants::MetadataSuffix.to_string()
        );

        let data = self
            .store
            .get(&metadata_handle)
            .ok_or(err_unknown!("could not find metadata handle"))?;

        let value =
            musubi_api::types::Value::try_from(data.clone()).map_err(|e| err_unknown!(e))?;
        let metadata = musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        Ok(metadata)
    }

    async fn set_metadata(
        &self,
        handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let metadata_handle = format!(
            "{}/{}",
            handle,
            NativeFileSystemConstants::MetadataSuffix.to_string()
        );

        let value = musubi_api::types::to_value(&metadata).map_err(|e| err_unknown!(e))?;
        let data: Vec<u8> = value
            .try_into()
            .map_err(|e: anyhow::Error| err_unknown!(e))?;

        let spec = StorageSpec {
            handle: metadata_handle,
            data,
            ttl,
            extensions,
        };

        self.store(spec).await
    }

    async fn path_exists(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        self.exists(handle, extensions).await
    }

    async fn list(
        &self,
        handle: String,
        page_index: u64,
        page_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<String>> {
        let list_handle = format!(
            "{}/{}",
            handle,
            NativeFileSystemConstants::DirListSuffix.to_string()
        );

        let data = self.store.get(&list_handle);

        if data.is_none() {
            return Ok(vec![]);
        }

        let value = musubi_api::types::Value::try_from(data.unwrap().clone())
            .map_err(|e| err_unknown!(e))?;
        let list: Vec<String> =
            musubi_api::types::from_value(&value).map_err(|e| err_unknown!(e))?;

        let offset = (page_index * page_size) as usize;

        if offset >= list.len() {
            return Ok(vec![]);
        }

        let end_offset = offset + page_size as usize;

        if end_offset <= list.len() {
            Ok(list[offset..end_offset].to_vec())
        } else {
            Ok(list[offset..].to_vec())
        }
    }

    async fn add_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let list_handle = format!(
            "{}/{}",
            handle,
            NativeFileSystemConstants::DirListSuffix.to_string()
        );

        let data = self.store.get(&list_handle);

        let mut value = musubi_api::types::Value::Array(vec![]);

        if let Some(data) = data {
            value = data
                .clone()
                .try_into()
                .map_err(|e: anyhow::Error| err_unknown!(e))?;
        }

        match &mut value {
            musubi_api::types::Value::Array(v) => {
                v.push(musubi_api::types::Value::String(item));

                v.sort_by(|x, y| match (&x, &y) {
                    (&Value::String(x), &Value::String(y)) => x.cmp(y),
                    _ => Ordering::Equal,
                });

                v.dedup_by(|a, b| match (&a, &b) {
                    (Value::String(x), Value::String(y)) => x == y,
                    _ => false,
                });
            }
            _ => {
                return Err(err_unknown!(
                    "unexpected error encountered while parsing list"
                ));
            }
        }

        let expiry = self
            .expiry_map
            .get(&handle)
            .ok_or(err_unknown!("could not find expiry handle"))?;
        let ttl_duration = expiry.clone() - Utc::now();

        let mut ttl = ttl_duration.num_seconds();
        if ttl < 0 {
            ttl = 0;
        }

        let spec = StorageSpec {
            handle: list_handle,
            data: value
                .try_into()
                .map_err(|e: anyhow::Error| err_unknown!(e))?,
            ttl: ttl as u64,
            extensions,
        };

        self.store(spec).await
    }

    async fn remove_list_item(
        &self,
        handle: String,
        item: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let list_handle = format!(
            "{}/{}",
            handle,
            NativeFileSystemConstants::DirListSuffix.to_string()
        );

        let data = self.store.get(&list_handle);

        let mut value = musubi_api::types::Value::Array(vec![]);

        if let Some(data) = data {
            value = data
                .clone()
                .try_into()
                .map_err(|e: anyhow::Error| err_unknown!(e))?;
        }

        match &mut value {
            musubi_api::types::Value::Array(v) => {
                v.retain(|x| {
                    if let musubi_api::types::Value::String(s) = x {
                        s != &item
                    } else {
                        true
                    }
                });
            }
            _ => {
                return Err(err_unknown!(
                    "unexpected error encountered while parsing list"
                ));
            }
        }

        let expiry = self
            .expiry_map
            .get(&handle)
            .ok_or(err_unknown!("could not find expiry handle"))?;
        let ttl_duration = expiry.clone() - Utc::now();

        let mut ttl = ttl_duration.num_seconds();
        if ttl < 0 {
            ttl = 0;
        }

        let spec = StorageSpec {
            handle: list_handle,
            data: value
                .try_into()
                .map_err(|e: anyhow::Error| err_unknown!(e))?,
            ttl: ttl as u64,
            extensions,
        };

        self.store(spec).await
    }

    async fn truncate(
        &self,
        handle: String,
        len: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let mut data = self
            .store
            .get_mut(&handle)
            .ok_or(err_unknown!("failed to get handle"))?;

        data.truncate(len as usize);

        Ok(())
    }

    async fn delete_path(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.clear(handle, extensions).await
    }

    async fn get_capabilities(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<StorageCapability>> {
        self.capabilities(handle, extensions).await
    }
}

impl MemoryStorage {
    pub fn new() -> types::Result<Arc<Box<dyn Storage>>> {
        let obj = Self {
            store: Default::default(),
            expiry_map: Default::default(),
        };

        Ok(Arc::new(Box::new(obj)))
    }
}
