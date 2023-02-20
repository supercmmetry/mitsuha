use async_trait::async_trait;
use mitsuha_core::{kernel::StorageSpec, types};

mod constants;
mod memory;
pub mod unified;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn store(&mut self, spec: StorageSpec) -> types::Result<()>;

    async fn load(&self, handle: String) -> types::Result<Vec<u8>>;

    async fn persist(&mut self, handle: String, time: u64) -> types::Result<()>;

    async fn clear(&mut self, handle: String) -> types::Result<()>;

    async fn size(&self) -> types::Result<usize>;
}
