use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{fs::{OpenOptions, File}, io::{AsyncSeekExt, AsyncWriteExt, AsyncReadExt}};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;
use mitsuha_core::{
    constants::StorageControlConstants,
    errors::Error,
    storage::{FileSystem, GarbageCollectable, Storage, StorageClass},
    types, unknown_err,
};
use mitsuha_core_types::kernel::StorageSpec;
use mitsuha_filesystem::{
    constant::NativeFileSystemConstants, util::PathExt, NativeFileMetadata, NativeFileType,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

lazy_static! {
    static ref INTERNAL_METADATA_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.local.internalmetadata";
    static ref NATIVE_METADATA_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.local.nativemetadata";
    static ref DIRDATA_EXT: String =
        NativeFileSystemConstants::MnfsSuffix.to_string() + ".custom.local.dirdata";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFileMetadata {
    handle: String,
    expiry: DateTime<Utc>,
}

pub struct LocalStorage {
    root_dir: String,
    enable_gc: bool,
    total_size: Arc<RwLock<usize>>,
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, spec: StorageSpec) -> types::Result<()> {
        let handle = spec.handle;

        match spec
            .extensions
            .get(&StorageControlConstants::StorageExpiryTimestamp.to_string())
        {
            Some(value) => {
                let date_time =
                    value
                        .parse::<DateTime<Utc>>()
                        .map_err(|e| Error::StorageStoreFailed {
                            message: format!(
                                "failed to parse storage expiry time for storage handle: '{}'",
                                handle.clone()
                            ),
                            source: e.into(),
                        })?;

                if date_time <= Utc::now() {
                    return Err(Error::StorageStoreFailed {
                        message: format!(
                            "storage handle has already expired: '{}'",
                            handle.clone()
                        ),
                        source: anyhow::anyhow!(""),
                    });
                }

                let metadata = LocalFileMetadata {
                    handle: handle.clone(),
                    expiry: date_time,
                };

                self.store_metadata_internal(handle.clone(), metadata)
                    .await?;
            }
            None => {
                // TODO: Add warnings here for adding calculated timestamp

                let metadata = LocalFileMetadata {
                    handle: handle.clone(),
                    expiry: Utc::now() + Duration::seconds(spec.ttl as i64),
                };

                self.store_metadata_internal(handle.clone(), metadata.clone())
                    .await?;
            }
        }

        let data = spec.data;

        *self.total_size.write().await += data.len();
        self.store_data(handle.clone(), data).await?;

        Ok(())
    }

    async fn load(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let metadata = self.load_metadata_internal(handle.clone()).await;

        match metadata {
            Ok(LocalFileMetadata { expiry, .. }) => {
                if expiry <= Utc::now() {
                    return Err(Error::StorageLoadFailed {
                        message: format!(
                            "storage handle has already expired: '{}'",
                            handle.clone()
                        ),
                        source: anyhow::anyhow!(""),
                    });
                }
            }
            Err(e) => {
                return Err(Error::StorageLoadFailed {
                    message: format!("storage handle was not found: '{}'", handle.clone()),
                    source: e.into(),
                });
            }
        }

        let result = self.load_data(handle.clone()).await;

        match result {
            Ok(data) => Ok(data.clone()),
            Err(e) => Err(Error::StorageLoadFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: e.into(),
            }),
        }
    }

    async fn exists(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<bool> {
        Ok(self.get_full_handle(&handle).exists())
    }

    async fn persist(
        &self,
        handle: String,
        time: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let value = self.load_metadata_internal(handle.clone()).await;

        match value {
            Ok(mut metadata) => {
                metadata.expiry += Duration::seconds(time as i64);
                self.store_metadata_internal(handle.clone(), metadata)
                    .await?;
                Ok(())
            }
            Err(e) => Err(Error::StoragePersistFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: e.into(),
            }),
        }
    }

    async fn clear(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        tracing::debug!("clearing handle: '{}'", handle);

        match self.load_data(handle.clone()).await {
            Ok(data) => {
                if *self.total_size.read().await > data.len() {
                    *self.total_size.write().await -= data.len();
                }
            }
            _ => {}
        }

        let file_name = self.get_full_handle(&handle);
        let metadata_file_name = self.get_full_handle(&Self::gen_internal_metadata_handle(&handle));

        std::fs::remove_file(file_name)
            .map_err(|e| Error::Unknown { source: e.into() })?;

        std::fs::remove_file(metadata_file_name)
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        Ok(self.total_size.read().await.clone())
    }
}

#[async_trait]
impl FileSystem for LocalStorage {
    async fn store_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        ttl: u64,
        data: Vec<u8>,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let file_name = self.get_full_handle(&handle);

        let offset = part_index * part_size;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_name)
            .await
            .map_err(|e| unknown_err!(e))?;

        file.seek(SeekFrom::Start(offset)).await
            .map_err(|e| unknown_err!(e))?;

        file.write(&data).await.map_err(|e| unknown_err!(e))?;

        file.flush().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let mut metadata = self.load_metadata_internal(handle.clone()).await?;

        metadata.expiry += Duration::seconds(ttl as i64);

        self.store_metadata_internal(handle, metadata).await?;

        Ok(())
    }

    async fn load_file_part(
        &self,
        handle: String,
        part_index: u64,
        part_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let file_name = self.get_full_handle(&handle);

        let offset = part_index * part_size;

        let mut file = OpenOptions::new()
            .read(true)
            .open(file_name)
            .await
            .map_err(|e| unknown_err!(e))?;

        file.seek(SeekFrom::Start(offset)).await
            .map_err(|e| unknown_err!(e))?;

        let mut data = vec![0u8; part_size as usize];

        let bytes_read = file.read(&mut data).await.map_err(|e| unknown_err!(e))?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        data.truncate(bytes_read);

        Ok(data)
    }

    async fn get_file_part_count(
        &self,
        handle: String,
        part_size: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<u64> {
        let file_name = self.get_full_handle(&handle);

        let data_len = tokio::fs::metadata(file_name)
            .await
            .map_err(|e| unknown_err!(e))?
            .len();

        let part_count = data_len / part_size + (data_len % part_size > 0) as u64;

        Ok(part_count)
    }

    async fn get_metadata(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<NativeFileMetadata> {
        let metadata_handle = Self::gen_native_metadata_handle(&handle);

        let data = self.load_data(metadata_handle).await?;

        let value = musubi_api::types::Value::try_from(data).map_err(|e| unknown_err!(e))?;

        let metadata = musubi_api::types::from_value(&value).map_err(|e| unknown_err!(e))?;

        Ok(metadata)
    }

    async fn set_metadata(
        &self,
        handle: String,
        metadata: NativeFileMetadata,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let value = musubi_api::types::to_value(&metadata).map_err(|e| unknown_err!(e))?;
        let data: Vec<u8> = value
            .try_into()
            .map_err(|e: anyhow::Error| unknown_err!(e))?;

        let metadata_handle = Self::gen_native_metadata_handle(&handle);

        let spec = StorageSpec {
            handle: metadata_handle,
            data,
            ttl,
            extensions,
        };

        let full_handle = self
            .get_full_handle(&handle)
            .to_string()
            .map_err(|e| unknown_err!(e))?;

        // Create directory based on metadata
        if metadata.file_type == NativeFileType::Dir {
            if !tokio::fs::try_exists(&full_handle).await
                .map_err(|e| unknown_err!(e))?
            {
                tokio::fs::create_dir(full_handle).await
                    .map_err(|e| unknown_err!(e))?;

            }
        }

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
        let offset = page_index * page_size;

        let path = self.get_full_handle(&handle);

        let list_iter = std::fs::read_dir(&path)
            .map_err(|e| unknown_err!(e))?;

        let mut data = Vec::new();

        for item in list_iter {
            let entry = item
                .map_err(|e| unknown_err!(e))?
                .file_name()
                .to_str()
                .map(|x| x.to_string())
                .ok_or(unknown_err!("encountered non utf-8 chars in dir entry"))?;

            if entry.contains(&NativeFileSystemConstants::MnfsSuffix.to_string()) {
                continue;
            }

            data.push(entry);
        }

        data.sort();

        if offset as usize >= data.len() {
            return Ok(vec![]);
        }

        if data.len() > page_size as usize {
            data = data[offset as usize..].to_vec();
            data.truncate(page_size as usize);
        }

        Ok(data)
    }

    async fn add_list_item(
        &self,
        _handle: String,
        _item: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Ok(())
    }

    async fn remove_list_item(
        &self,
        _handle: String,
        _item: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        Ok(())
    }

    async fn truncate(
        &self,
        handle: String,
        len: u64,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let file_name = self.get_full_handle(&handle);

        let mut file = OpenOptions::new()
            .write(true)
            .open(file_name).await
            .map_err(|e| unknown_err!(e))?;

        file.set_len(len).await.map_err(|e| unknown_err!(e))?;

        file.flush().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn move_path(
        &self,
        source_handle: String,
        destination_handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let full_source_handle = self.get_full_handle(&source_handle);
        let full_destination_handle = self.get_full_handle(&destination_handle);

        std::fs::rename(full_source_handle, full_destination_handle)
            .map_err(|e| unknown_err!(e))
    }

    async fn delete_path(
        &self,
        handle: String,
        _extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        let path = self.get_full_handle(&handle);

        let metadata = std::fs::metadata(&path)
            .map_err(|e| unknown_err!(e))?;

        let metadata_handle = self.get_full_handle(&Self::gen_internal_metadata_handle(&handle));
        let native_metadata_handle =
            self.get_full_handle(&Self::gen_native_metadata_handle(&handle));

        if metadata.file_type().is_dir() {
            std::fs::remove_dir_all(path)
                .map_err(|e| unknown_err!(e))?;
        } else if metadata.file_type().is_file() {
            std::fs::remove_file(path)
                .map_err(|e| unknown_err!(e))?;
        } else {
            return Err(unknown_err!("symlink deletion is not allowed"));
        }

        std::fs::remove_file(metadata_handle)
            .map_err(|e| unknown_err!(e))?;

        std::fs::remove_file(native_metadata_handle)
            .map_err(|e| unknown_err!(e))?;

        Ok(())
    }
}

#[async_trait]
impl GarbageCollectable for LocalStorage {
    async fn garbage_collect(&self) -> types::Result<Vec<String>> {
        if !self.enable_gc {
            return Ok(vec![]);
        }

        self.run_gc()
            .await
            .map_err(|e| Error::Unknown { source: e })
    }
}

impl LocalStorage {
    pub fn new(class: StorageClass) -> types::Result<Arc<Box<dyn Storage>>> {
        let root_dir = class.get_extension_property("rootDirectory")?;

        let enable_gc = class
            .get_extension_property("enableGC")?
            .parse::<bool>()
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let storage = Self {
            root_dir,
            enable_gc,
            total_size: Arc::new(RwLock::new(0)),
        };

        Ok(Arc::new(Box::new(storage)))
    }

    fn hash_handle(handle: &String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(handle);

        hex::encode(hasher.finalize())
    }

    fn gen_internal_metadata_handle(handle: &String) -> String {
        format!(
            "{}.{}",
            INTERNAL_METADATA_EXT.as_str(),
            Self::hash_handle(handle)
        )
    }

    fn gen_native_metadata_handle(handle: &String) -> String {
        format!(
            "{}.{}",
            NATIVE_METADATA_EXT.as_str(),
            Self::hash_handle(handle)
        )
    }

    async fn store_metadata_internal(
        &self,
        handle: String,
        metadata: LocalFileMetadata,
    ) -> types::Result<()> {
        let internal_metadata_handle = Self::gen_internal_metadata_handle(&handle);

        let full_handle = self
            .get_full_handle(&internal_metadata_handle)
            .to_string()
            .map_err(|e| unknown_err!(e))?;

        let mut file = File::create(full_handle).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata_json =
            serde_json::to_string(&metadata).map_err(|e| Error::Unknown { source: e.into() })?;

        file.write_all(metadata_json.as_bytes()).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.flush().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn store_data(&self, handle: String, data: Vec<u8>) -> types::Result<()> {
        let mut file_name = self.get_full_handle(&handle);

        if tokio::fs::try_exists(&file_name).await
            .map_err(|e| unknown_err!(e))?
        {
            let metadata = tokio::fs::metadata(&file_name).await
                .map_err(|e| unknown_err!(e))?;

            if metadata.is_dir() {
                file_name.push(DIRDATA_EXT.as_str());
            }
        }

        let mut file = File::create(file_name).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.write_all(&data).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.flush().await.map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn load_metadata_internal(&self, handle: String) -> types::Result<LocalFileMetadata> {
        let expiry_handle = Self::gen_internal_metadata_handle(
            &handle
        );

        let full_handle = self.get_full_handle(&expiry_handle);

        let mut file = File::open(full_handle).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let mut metadata_json_buf = Vec::new();

        file.read_to_end(&mut metadata_json_buf).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata_json = String::from_utf8(metadata_json_buf)
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata: LocalFileMetadata = serde_json::from_str(metadata_json.as_str())
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(metadata)
    }

    fn get_full_handle(&self, handle: &String) -> PathBuf {
        let mut parent_dir = self.root_dir.clone();
        let mut child_path = handle.clone();

        if parent_dir.ends_with("/") {
            parent_dir.pop();
        }

        if child_path.starts_with("/") {
            child_path.remove(0);
        }

        let path = format!("{}/{}", parent_dir, child_path);

        let path_buf = Path::new(&path).to_path_buf();

        path_buf
    }

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>> {
        let mut file_name = self.get_full_handle(&handle);

        if tokio::fs::try_exists(&file_name).await
            .map_err(|e| unknown_err!(e))?
        {
            let metadata = tokio::fs::metadata(&file_name).await
                .map_err(|e| unknown_err!(e))?;

            if metadata.is_dir() {
                file_name.push(DIRDATA_EXT.as_str());
            }
        }

        let mut file = File::open(file_name).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let mut buf = Vec::new();

        file.read_to_end(&mut buf).await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        file.shutdown().await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(buf)
    }

    async fn run_gc(&self) -> anyhow::Result<Vec<String>> {
        let mut delete_handles = vec![];

        let read_dir = std::fs::read_dir(&self.root_dir)?;

        for entry in read_dir {
            let entry = entry?;

            if !entry.file_type()?.is_file() {
                continue;
            }

            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.starts_with(INTERNAL_METADATA_EXT.as_str()) {
                    let entry_path = entry.path();
                    let full_path = entry_path.to_str().clone();

                    if full_path.is_none() {
                        continue;
                    }

                    let metadata_json = std::fs::read_to_string(full_path.unwrap());
                    if metadata_json.is_err() {
                        tracing::warn!(
                            "failed to read metadata for file: '{}', error: {}",
                            full_path.unwrap(),
                            metadata_json.err().unwrap()
                        );
                        continue;
                    }

                    let metadata: Result<LocalFileMetadata, serde_json::Error> =
                        serde_json::from_str(&metadata_json.unwrap());

                    if metadata.is_err() {
                        tracing::warn!(
                            "failed to read metadata for file: '{}', error: {}",
                            full_path.unwrap(),
                            metadata.err().unwrap()
                        );
                        continue;
                    }

                    let metadata = metadata.unwrap();

                    tracing::debug!("checking expiry for handle: '{}'", metadata.handle);

                    if metadata.expiry <= Utc::now() {
                        if let Err(e) = self
                            .clear(metadata.handle.clone(), Default::default())
                            .await
                        {
                            tracing::error!(
                                "failed to perform gc operation on handle: '{}', error: {}",
                                metadata.handle,
                                e
                            );
                        }

                        delete_handles.push(metadata.handle);
                    }
                }
            }
        }

        Ok(delete_handles)
    }
}
