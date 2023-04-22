use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::DashMap;
use mitsuha_core::{
    config,
    constants::Constants,
    errors::Error,
    kernel::StorageSpec,
    selector::Label,
    storage::{GarbageCollectable, Storage, StorageClass, StorageKind, StorageLocality},
    types,
};

use crate::memory::MemoryStorage;

pub struct UnifiedStorage {
    stores: Arc<HashMap<String, Arc<Box<dyn Storage>>>>,
    classes: Arc<HashMap<String, StorageClass>>,
    handle_location: DashMap<String, String>,
}

#[async_trait]
impl Storage for UnifiedStorage {
    async fn store(&self, spec: StorageSpec) -> types::Result<()> {
        let storage_name = self.get_solid_storage_name_by_selector(&spec)?;

        let class = self.classes.get(&storage_name).unwrap();
        let storage = self.stores.get(&storage_name).unwrap();

        let handle = spec.handle.clone();

        storage.store(spec).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            cache.clear(handle).await?;
        }

        Ok(())
    }

    async fn load(&self, handle: String) -> types::Result<Vec<u8>> {
        let storage_name = self.get_solid_storage_name_by_handle(&handle).await?;
        let class = self.classes.get(&storage_name).unwrap();

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            let result = cache.load(handle.clone()).await;

            match result {
                Ok(data) => return Ok(data),
                Err(_e) => {
                    // Log error
                }
            }
        }

        let storage = self.stores.get(&storage_name).unwrap();
        let data = storage.load(handle.clone()).await?;

        if let StorageLocality::Solid {
            cache_name: Some(cache_name),
        } = class.locality.clone()
        {
            let cache = self.stores.get(&cache_name).unwrap();
            let cache_class = self.classes.get(&cache_name).unwrap();

            match cache_class.locality {
                StorageLocality::Cache { ttl } => {
                    let spec = StorageSpec {
                        handle: handle.clone(),
                        ttl,
                        data: data.clone(),
                        extensions: HashMap::new(),
                    };

                    // TODO: spawn a different task for cache store operation
                    let result = cache.store(spec).await;

                    match result {
                        Err(_e) => {
                            // Log error
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(data)
    }

    async fn exists(&self, handle: String) -> types::Result<bool> {
        Ok(self.get_solid_storage_name_by_handle(&handle).await.is_ok())
    }

    async fn persist(&self, handle: String, time: u64) -> types::Result<()> {
        let storage_name = self.get_solid_storage_name_by_handle(&handle).await?;
        let storage = self.stores.get(&storage_name).unwrap();

        storage.persist(handle, time).await
    }

    async fn clear(&self, handle: String) -> types::Result<()> {
        let storage_name = self.get_solid_storage_name_by_handle(&handle).await?;
        let storage = self.stores.get(&storage_name).unwrap();

        storage.clear(handle.clone()).await?;

        self.handle_location.remove(&handle);

        Ok(())
    }

    async fn size(&self) -> types::Result<usize> {
        let mut total_size = 0usize;

        for storage in self.stores.values() {
            total_size += storage.size().await?;
        }

        Ok(total_size)
    }
}

#[async_trait]
impl GarbageCollectable for UnifiedStorage {
    async fn garbage_collect(&self) -> types::Result<Vec<String>> {
        let mut deleted_handles = vec![];

        for (name, collectable) in self.stores.iter() {
            let handles = collectable.garbage_collect().await?;

            if !self.classes.get(name).unwrap().locality.is_cache() {
                deleted_handles.extend(handles);
            }
        }

        for handle in deleted_handles.iter() {
            self.handle_location.remove(handle);
        }

        Ok(deleted_handles)
    }
}

impl UnifiedStorage {
    pub fn new(config: &config::storage::Storage) -> types::Result<Arc<Box<dyn Storage>>> {
        let mut unified_storage = Self {
            stores: Default::default(),
            classes: Default::default(),
            handle_location: Default::default(),
        };

        let mut stores = HashMap::new();
        let mut classes = HashMap::new();

        for storage_class in config.classes.iter() {
            let name = &storage_class.name;

            if stores.contains_key(name) || classes.contains_key(name) {
                return Err(Error::StorageInitFailed {
                    message: format!("duplicate storage class name '{}' was found during unified storage initialization.", name),
                    source: anyhow::anyhow!("")
                });
            }

            let storage_impl: Arc<Box<dyn Storage>> = match storage_class.kind {
                StorageKind::Memory => MemoryStorage::new()?,
            };

            let mut processed_storage_class = storage_class.clone();

            processed_storage_class.labels.push(Label {
                name: Constants::StorageLabel.to_string(),
                value: storage_class.name.clone(),
            });

            classes.insert(name.clone(), processed_storage_class);

            stores.insert(name.clone(), storage_impl);
        }

        unified_storage.classes = Arc::new(classes);
        unified_storage.stores = Arc::new(stores);

        let output: Arc<Box<dyn Storage>> = Arc::new(Box::new(unified_storage));

        Self::start_gc(output.clone());

        Ok(output)
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

    async fn get_solid_storage_name_by_handle(&self, handle: &String) -> types::Result<String> {
        if let Some(storage_name) = self.handle_location.get(handle) {
            return Ok(storage_name.clone());
        }

        for (name, class) in self.classes.iter() {
            match class.locality {
                StorageLocality::Solid { .. } => {
                    let storage = self.stores.get(name).unwrap();
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

    fn start_gc(collectable: Arc<Box<dyn Storage>>) {
        tokio::task::spawn(async move {
            loop {
                collectable.garbage_collect().await.unwrap();

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
