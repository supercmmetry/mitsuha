use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use mitsuha_core::{
    constants::Constants,
    errors::Error,
    kernel::StorageSpec,
    storage::{GarbageCollectable, Storage, StorageClass},
    types,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};

static METADATA_EXT: &str = ".metadata";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFileMetadata {
    handle: String,
    expiry: DateTime<Utc>,
}

pub struct LocalStorage {
    root_dir: String,
    enable_gc: bool,
    metadata_cache: moka::future::Cache<String, LocalFileMetadata>,
    total_size: Arc<RwLock<usize>>,
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, spec: StorageSpec) -> types::Result<()> {
        let handle = spec.handle;

        match spec
            .extensions
            .get(&Constants::StorageExpiryTimestamp.to_string())
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

                self.store_metadata(handle.clone(), metadata).await?;
            }
            None => {
                // TODO: Add warnings here for adding calculated timestamp

                let metadata = LocalFileMetadata {
                    handle: handle.clone(),
                    expiry: Utc::now() + Duration::seconds(spec.ttl as i64),
                };

                self.store_metadata(handle.clone(), metadata.clone())
                    .await?;
                self.metadata_cache.insert(handle.clone(), metadata).await;
            }
        }

        let data = spec.data;

        *self.total_size.write().await += data.len();
        self.store_data(handle.clone(), data).await?;

        Ok(())
    }

    async fn load(&self, handle: String) -> types::Result<Vec<u8>> {
        let metadata = self.load_metadata(handle.clone()).await;

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

    async fn exists(&self, handle: String) -> types::Result<bool> {
        Ok(
            std::path::Path::new(&format!("{}/{}", self.root_dir, Self::hash_handle(handle)))
                .exists(),
        )
    }

    async fn persist(&self, handle: String, time: u64) -> types::Result<()> {
        let value = self.load_metadata(handle.clone()).await;

        match value {
            Ok(mut metadata) => {
                metadata.expiry += Duration::seconds(time as i64);
                self.store_metadata(handle.clone(), metadata).await?;
                Ok(())
            }
            Err(e) => Err(Error::StoragePersistFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: e.into(),
            }),
        }
    }

    async fn clear(&self, handle: String) -> types::Result<()> {
        log::debug!("clearing handle: '{}'", handle);

        match self.load_data(handle.clone()).await {
            Ok(data) => {
                if *self.total_size.read().await > data.len() {
                    *self.total_size.write().await -= data.len();
                }
            }
            _ => {}
        }

        self.metadata_cache.invalidate(&handle).await;

        tokio::fs::remove_file(format!(
            "{}/{}",
            self.root_dir,
            Self::hash_handle(handle.clone())
        ))
        .await
        .map_err(|e| Error::Unknown { source: e.into() })?;

        tokio::fs::remove_file(format!(
            "{}/{}",
            self.root_dir,
            Self::gen_metadata_handle(&handle)
        ))
        .await
        .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        Ok(self.total_size.read().await.clone())
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
            metadata_cache: moka::future::Cache::new(16),
            total_size: Arc::new(RwLock::new(0)),
        };

        Ok(Arc::new(Box::new(storage)))
    }

    fn hash_handle(handle: String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(handle);

        hex::encode(hasher.finalize())
    }

    fn gen_metadata_handle(handle: &String) -> String {
        format!("{}{}", Self::hash_handle(handle.clone()), METADATA_EXT)
    }

    async fn store_metadata(
        &self,
        handle: String,
        metadata: LocalFileMetadata,
    ) -> types::Result<()> {
        let expiry_handle = Self::gen_metadata_handle(&handle);
        let mut file_name = PathBuf::new();

        file_name.push(self.root_dir.clone());
        file_name.push(expiry_handle);

        let mut file = File::create(file_name)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata_json =
            serde_json::to_string(&metadata).map_err(|e| Error::Unknown { source: e.into() })?;

        file.write_all(metadata_json.as_bytes())
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        self.metadata_cache.insert(handle, metadata).await;

        Ok(())
    }

    async fn store_data(&self, handle: String, data: Vec<u8>) -> types::Result<()> {
        let mut file_name = PathBuf::new();

        file_name.push(self.root_dir.clone());
        file_name.push(Self::hash_handle(handle));

        let mut file = File::create(file_name)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;
        file.write_all(&data)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(())
    }

    async fn load_metadata(&self, handle: String) -> types::Result<LocalFileMetadata> {
        if let Some(v) = self.metadata_cache.get(&handle) {
            return Ok(v);
        }

        let expiry_handle = Self::gen_metadata_handle(&handle);
        let mut file_name = PathBuf::new();

        file_name.push(self.root_dir.clone());
        file_name.push(expiry_handle);

        let mut file = File::open(file_name)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let mut metadata_json_buf = Vec::new();

        file.read_to_end(&mut metadata_json_buf)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata_json = String::from_utf8(metadata_json_buf)
            .map_err(|e| Error::Unknown { source: e.into() })?;

        let metadata: LocalFileMetadata = serde_json::from_str(metadata_json.as_str())
            .map_err(|e| Error::Unknown { source: e.into() })?;

        self.metadata_cache.insert(handle, metadata.clone()).await;

        Ok(metadata)
    }

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>> {
        let mut file_name = PathBuf::new();

        file_name.push(self.root_dir.clone());
        file_name.push(Self::hash_handle(handle));

        let mut file = File::open(file_name)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;
        let mut buf = Vec::new();

        file.read_to_end(&mut buf)
            .await
            .map_err(|e| Error::Unknown { source: e.into() })?;

        Ok(buf)
    }

    async fn run_gc(&self) -> anyhow::Result<Vec<String>> {
        let mut delete_handles = vec![];

        let mut read_dir = tokio::fs::read_dir(&self.root_dir).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }

            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(METADATA_EXT) {
                    let entry_path = entry.path();
                    let full_path = entry_path.to_str().clone();

                    if full_path.is_none() {
                        continue;
                    }

                    let metadata_json = tokio::fs::read_to_string(full_path.unwrap()).await;
                    if metadata_json.is_err() {
                        log::warn!(
                            "failed to read metadata for file: '{}', error: {}",
                            full_path.unwrap(),
                            metadata_json.err().unwrap()
                        );
                        continue;
                    }

                    let metadata: Result<LocalFileMetadata, serde_json::Error> =
                        serde_json::from_str(&metadata_json.unwrap());

                    if metadata.is_err() {
                        log::warn!(
                            "failed to read metadata for file: '{}', error: {}",
                            full_path.unwrap(),
                            metadata.err().unwrap()
                        );
                        continue;
                    }

                    let metadata = metadata.unwrap();

                    log::debug!("checking expiry for handle: '{}'", metadata.handle);

                    if metadata.expiry <= Utc::now() {
                        // TODO: Fail silently here to avoid failing the global GC

                        if let Err(e) = self.clear(metadata.handle.clone()).await {
                            log::error!(
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
