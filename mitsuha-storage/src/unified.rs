use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use mitsuha_core::{
    config,
    errors::Error,
    kernel::StorageSpec,
    selector::Label,
    storage::{StorageClass, StorageKind, StorageLocality},
    types,
};

use crate::{constants::Constants, memory::MemoryStorage, Storage};

pub struct UnifiedStorage {
    stores: HashMap<String, Box<dyn Storage>>,
    classes: HashMap<String, StorageClass>,
    handle_location: HashMap<String, String>,
}

#[async_trait]
impl Storage for UnifiedStorage {
    async fn store(&mut self, spec: StorageSpec) -> types::Result<()> {
        let storage_name = self.get_solid_storage_name_by_selector(&spec)?;

        let class = self.classes.get(&storage_name).unwrap();
        let storage = self.stores.get_mut(&storage_name).unwrap();

        let handle = spec.handle.clone();

        storage.store(spec).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get_mut(&cache_name).unwrap();
            cache.clear(handle).await?;
        }

        Ok(())
    }

    async fn load(&mut self, handle: String) -> types::Result<Vec<u8>> {
        let storage_name = self.get_solid_storage_name_by_handle(&handle).await?;
        let class = self.classes.get(&storage_name).unwrap();

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get_mut(&cache_name).unwrap();
            let result = cache.load(handle.clone()).await;

            match result {
                Ok(data) => return Ok(data),
                Err(_e) => {
                    // Log error
                }
            }
        }

        let storage = self.stores.get_mut(&storage_name).unwrap();
        let data = storage.load(handle.clone()).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get_mut(&cache_name).unwrap();
            let cache_class = self.classes.get(&cache_name).unwrap();

            match cache_class.locality {
                StorageLocality::Cache { ttl } => {
                    let spec = StorageSpec {
                        handle: handle.clone(),
                        ttl,
                        data: data.clone(),
                        extensions: HashMap::new(),
                    };

                    let result = cache.store(spec).await;

                    match result {
                        Err(_e) => {
                            // Log error
                        },
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(data)
    }

    async fn exists(&mut self, handle: String) -> types::Result<bool> {
        Ok(self.get_solid_storage_name_by_handle(&handle).await.is_ok())
    }

    async fn persist(&mut self, handle: String, time: u64) -> types::Result<()> {
        Ok(())
    }

    async fn clear(&mut self, handle: String) -> types::Result<()> {
        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        Ok(0usize)
    }
}

impl UnifiedStorage {
    pub fn new(config: &config::storage::Storage) -> types::Result<Self> {
        let mut unified_storage = Self {
            stores: Default::default(),
            classes: Default::default(),
            handle_location: Default::default(),
        };

        for storage_class in config.classes.iter() {
            let name = &storage_class.name;

            if unified_storage.stores.contains_key(name)
                || unified_storage.classes.contains_key(name)
            {
                return Err(Error::StorageInitFailed {
                    message: format!("duplicate storage class name '{}' was found during unified storage initialization.", name),
                    source: anyhow::anyhow!("")
                });
            }

            let storage_impl: Box<dyn Storage> = match storage_class.kind {
                StorageKind::Memory => Box::new(MemoryStorage::new()?),
            };

            let mut processed_storage_class = storage_class.clone();

            processed_storage_class.labels.push(Label {
                name: Constants::StorageLabel.to_string(),
                value: storage_class.name.clone(),
            });

            unified_storage
                .classes
                .insert(name.clone(), processed_storage_class);
            unified_storage.stores.insert(name.clone(), storage_impl);
        }

        Ok(unified_storage)
    }

    fn get_solid_storage_name_by_selector(&self, spec: &StorageSpec) -> types::Result<String> {
        if let Some(query) = spec
            .extensions
            .get(&Constants::StorageSelectorQuery.to_string())
        {
            let label: Label = serde_json::from_str(query.as_str()).map_err(|e| {
                Error::StorageOperationFailed {
                    message: format!("failed to parse storage selector."),
                    source: e.into(),
                }
            })?;

            for (name, class) in self.classes.iter() {
                match class.locality {
                    mitsuha_core::storage::StorageLocality::Solid { .. } => {
                        if class.labels.contains(&label) {
                            return Ok(name.clone());
                        }
                    }
                    mitsuha_core::storage::StorageLocality::Cache { .. } => continue,
                }
            }
        }

        return Err(Error::StorageOperationFailed {
            message: format!("failed to get a storage that satisfies the selector."),
            source: anyhow::anyhow!(""),
        });
    }

    async fn get_solid_storage_name_by_handle(&mut self, handle: &String) -> types::Result<String> {
        if let Some(storage_name) = self.handle_location.get(handle) {
            return Ok(storage_name.clone());
        }

        for (name, class) in self.classes.iter() {
            match class.locality {
                StorageLocality::Solid { .. } => {
                    let storage = self.stores.get_mut(name).unwrap();
                    let result = storage.exists(handle.clone()).await;

                    match result {
                        Ok(true) => {
                            self.handle_location.insert(handle.clone(), name.clone());
                            return Ok(name.clone());
                        }
                        Ok(_) => {}
                        Err(_e) => {
                            // Log error
                        }
                    }
                }
                StorageLocality::Cache { .. } => continue,
            }
        }

        Err(Error::StorageOperationFailed {
            message: format!(
                "failed to get a storage that serves the handle: {}.",
                handle.clone()
            ),
            source: anyhow::anyhow!(""),
        })
    }
}
