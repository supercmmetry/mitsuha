use crate::{symbol::Symbol, types};

use async_trait::async_trait;

#[async_trait]
pub trait Kernel: Send + Sync {
    async fn run_task(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}
