use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{kernel::{StorageSpec, JobSpec}, types};

pub enum ComputeInput {
    Store { spec: StorageSpec },
    Load { handle: String },
    Persist { handle: String, ttl: u64 },
    Clear { handle: String },

    Run { spec: JobSpec },
    Extend { handle: String },
}

pub enum ComputeOutput {
    Stored { handle: String },
    Loaded { data: Vec<u8> },
    Completed,
    Failed { msg: String },
}

pub type ComputeHandle = JoinHandle<ComputeOutput>;

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    async fn id(&self) -> types::Result<String>;

    async fn compute(&self, elem: ComputeInput) -> types::Result<ComputeHandle>;

    async fn connect(&mut self, next: Box<dyn ComputeChannel>);
}
