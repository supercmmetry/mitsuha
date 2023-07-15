use async_trait::async_trait;
use mitsuha_core::channel::ComputeInput;

pub mod consumer;
pub mod producer;
pub mod qmux;
pub mod util;

#[async_trait]
pub trait QReader {
    async fn read_compute_input(&self, client_id: String) -> anyhow::Result<ComputeInput>;
}
