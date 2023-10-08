use std::collections::HashMap;

use async_trait::async_trait;
use mitsuha_core_types::kernel::StorageSpec;
use serde::{Deserialize, Serialize};

use crate::{errors::Error, selector::Label, types};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StorageLocality {
    Solid { cache_name: Option<String> },
    Cache { ttl: u64 },
}

impl StorageLocality {
    pub fn is_cache(&self) -> bool {
        match self {
            Self::Cache { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Memory,
    Local,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageClass {
    pub kind: StorageKind,
    pub locality: StorageLocality,
    pub name: String,
    pub labels: Vec<Label>,
    pub extensions: HashMap<String, String>,
}

impl StorageClass {
    pub fn get_extension_property(&self, key: &str) -> types::Result<String> {
        self.extensions
            .get(key)
            .ok_or(Error::UnknownWithMsgOnly {
                message: format!(
                    "extension '{}' was not found in storage class '{}'",
                    key, self.name
                ),
            })
            .map(|x| x.clone())
    }
}

#[async_trait]
pub trait Storage: GarbageCollectable + Send + Sync {
    async fn store(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load(&self, handle: String) -> types::Result<Vec<u8>>;

    async fn exists(&self, handle: String) -> types::Result<bool>;

    async fn persist(&self, handle: String, time: u64) -> types::Result<()>;

    async fn clear(&self, handle: String) -> types::Result<()>;

    async fn size(&self) -> types::Result<usize>;
}

#[async_trait]
pub trait GarbageCollectable: Send + Sync {
    async fn garbage_collect(&self) -> types::Result<Vec<String>>;
}
