use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{
    kernel::{JobSpec, StorageSpec},
    types,
};

pub enum ComputeInput {
    Store { spec: StorageSpec },
    Load { handle: String },
    Persist { handle: String, ttl: u64 },
    Clear { handle: String },

    Run { spec: JobSpec },
    Extend { handle: String },
}

pub enum ComputeOutput {
    Loaded { data: Vec<u8> },
    Completed,
}

pub type ComputeHandle = JoinHandle<types::Result<ComputeOutput>>;

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    async fn id(&self) -> types::Result<String>;

    async fn compute(&self, elem: ComputeInput) -> types::Result<ComputeHandle>;

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel>>);
}
