use async_trait::async_trait;
use mitsuha_core::{kernel::StorageSpec, types};

mod memory;
pub mod unified;
pub mod resolver;

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