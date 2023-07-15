use async_trait::async_trait;
use mitsuha_core::channel::ComputeInput;

pub mod tikv;
pub mod util;

#[async_trait]
pub trait Reader: Send + Sync {
    async fn read_compute_input(&self, client_id: String) -> anyhow::Result<ComputeInput>;
}

#[async_trait]
pub trait Writer: Send + Sync {
    async fn write_compute_input(&self, input: ComputeInput) -> anyhow::Result<()>;
}
