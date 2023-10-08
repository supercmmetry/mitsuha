use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use mitsuha_core_types::kernel::StorageSpec;
use mitsuha_core::{constants::Constants, errors::Error, types};

use mitsuha_core::storage::{GarbageCollectable, Storage};

pub struct MemoryStorage {
    store: DashMap<String, Vec<u8>>,
    expiry_map: DashMap<String, DateTime<Utc>>,
    total_size: Arc<RwLock<usize>>,
}

#[async_trait]
impl Storage for MemoryStorage {
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

                self.expiry_map.insert(handle.clone(), date_time);
            }
            None => {
                // TODO: Add warnings here for adding calculated timestamp

                self.expiry_map.insert(
                    handle.clone(),
                    Utc::now() + Duration::seconds(spec.ttl as i64),
                );
            }
        }

        let data = spec.data;

        *self.total_size.write().unwrap() += data.len();
        self.store.insert(handle.clone(), data);

        Ok(())
    }

    async fn load(&self, handle: String) -> types::Result<Vec<u8>> {
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

    async fn exists(&self, handle: String) -> types::Result<bool> {
        Ok(self.store.contains_key(&handle))
    }

    async fn persist(&self, handle: String, time: u64) -> types::Result<()> {
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

    async fn clear(&self, handle: String) -> types::Result<()> {
        if let Some(data) = self.store.get(&handle) {
            *self.total_size.write().unwrap() -= data.len();
        }

        self.expiry_map.remove(&handle);
        self.store.remove(&handle);

        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        Ok(self.total_size.read().unwrap().clone())
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

impl MemoryStorage {
    pub fn new() -> types::Result<Arc<Box<dyn Storage>>> {
        let obj = Self {
            store: Default::default(),
            expiry_map: Default::default(),
            total_size: Arc::new(RwLock::new(0usize)),
        };

        Ok(Arc::new(Box::new(obj)))
    }
}
