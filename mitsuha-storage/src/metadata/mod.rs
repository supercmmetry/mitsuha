pub mod tikv;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mitsuha_core::types;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Metadata {
    physical_location: String,
    expiry: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataKey {
    storage_name: String,
    handle: String,
}

#[async_trait]
pub trait MetadataProvider {
    async fn get_expiry(&self, key: &MetadataKey) -> types::Result<DateTime<Utc>>;

    async fn extend_expiry(&self, key: &MetadataKey, ttl: u64) -> types::Result<()>;

    async fn get_physical_location(&self, key: &MetadataKey) -> types::Result<String>;

    async fn set_physical_location(&self, key: &MetadataKey, location: String) -> types::Result<()>;
}