use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;

use crate::{kernel::StorageSpec, selector::Label, types};

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum StorageKind {
    Memory,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageClass {
    pub kind: StorageKind,
    pub locality: StorageLocality,
    pub name: String,
    pub labels: Vec<Label>,
    pub extensions: HashMap<String, String>,
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
