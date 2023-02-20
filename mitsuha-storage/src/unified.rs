use std::{collections::HashMap, sync::{Arc, RwLock}};

use async_trait::async_trait;
use mitsuha_core::{
    config,
    errors::Error,
    kernel::StorageSpec,
    storage::{StorageClass, StorageKind},
    types::{self, SharedMany},
};

use crate::{Storage, memory::MemoryStorage};

pub struct UnifiedStorage {
    stores: HashMap<usize, SharedMany<Box<dyn Storage>>>,
    classes: HashMap<usize, StorageClass>,
}

#[async_trait]
impl Storage for UnifiedStorage {
    async fn store(&mut self, spec: StorageSpec) -> types::Result<()> {
        Ok(())
    }

    async fn load(&self, handle: String) -> types::Result<Vec<u8>> {
        Ok(vec![])
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
        };

        for storage_class in config.classes.iter() {
            let index = storage_class.index;
 
            if unified_storage.stores.contains_key(&index)
                || unified_storage.classes.contains_key(&index)
            {
                return Err(Error::StorageInitFailed {
                    message: format!("duplicate storage class index '{}' was found during unified storage initialization.", index),
                    source: anyhow::anyhow!("")
                });
            }

            let storage_impl: Box<dyn Storage> = match storage_class.kind {
                StorageKind::Memory => Box::new(MemoryStorage::new()?)
            };

            unified_storage.classes.insert(index, storage_class.clone());
            unified_storage.stores.insert(index, Arc::new(RwLock::new(storage_impl)));

        }

        Ok(unified_storage)
    }
}
