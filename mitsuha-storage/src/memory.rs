use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use mitsuha_core::{errors::Error, kernel::StorageSpec, types};

use crate::{constants::Constants, Storage};

pub struct MemoryStorage {
    store: HashMap<String, Vec<u8>>,
    expiry_map: HashMap<String, DateTime<Utc>>,
    total_size: usize,
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn store(&mut self, spec: StorageSpec) -> types::Result<()> {
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

        self.total_size += data.len();
        self.store.insert(handle.clone(), data);

        Ok(())
    }

    async fn load(&mut self, handle: String) -> types::Result<Vec<u8>> {
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

    async fn exists(&mut self, handle: String) -> types::Result<bool> {
        Ok(self.store.contains_key(&handle))
    }

    async fn persist(&mut self, handle: String, time: u64) -> types::Result<()> {
        match self.expiry_map.get(&handle) {
            Some(date_time) => {
                self.expiry_map
                    .insert(handle.clone(), *date_time + Duration::seconds(time as i64));
                Ok(())
            }
            None => Err(Error::StoragePersistFailed {
                message: format!("storage handle was not found: '{}'", handle.clone()),
                source: anyhow::anyhow!(""),
            }),
        }
    }

    async fn clear(&mut self, handle: String) -> types::Result<()> {
        if let Some(data) = self.store.get(&handle) {
            self.total_size -= data.len();
        }

        self.expiry_map.remove(&handle);
        self.store.remove(&handle);

        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        Ok(self.total_size)
    }
}

impl MemoryStorage {
    pub fn new() -> types::Result<Self> {
        Ok(Self {
            store: Default::default(),
            expiry_map: Default::default(),
            total_size: 0usize,
        })
    }
}
