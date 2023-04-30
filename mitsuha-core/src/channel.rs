use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{
    kernel::{Kernel, JobSpec, StorageSpec, JobStatus},
    types,
};

pub enum ComputeInput {
    Store { spec: StorageSpec },
    Load { handle: String },
    Persist { handle: String, ttl: u64 },
    Clear { handle: String },

    Run { spec: JobSpec },
    Extend { handle: String },
    Status { handle: String },
    Abort { handle: String },
}

pub enum ComputeOutput {
    Status { status: JobStatus },
    Loaded { data: Vec<u8> },
    Completed,
}

pub type ComputeHandle = JoinHandle<types::Result<ComputeOutput>>;

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    type Context;

    async fn id(&self) -> types::Result<String>;

    async fn compute(&self, ctx: Self::Context, elem: ComputeInput) -> types::Result<ComputeHandle>;

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>);
}


pub struct ComputeKernel<Context> {
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
    
}

// #[async_trait]
// impl Kernel for ComputeKernel {
//     async fn run_job(&self, spec: &JobSpec) -> types::Result<()>;

//     async fn extend_job(&self, handle: String, time: u64) -> types::Result<()>;

//     async fn abort_job(&self, handle: String) -> types::Result<()>;

//     async fn get_job_status(&self, handle: String) -> types::Result<JobStatus>;

//     async fn store_data(&self, spec: StorageSpec) -> types::Result<()> {
//         self.channel.
//     }

//     async fn load_data(&self, handle: String) -> types::Result<Vec<u8>>;

//     async fn persist_data(&self, handle: String, time: u64) -> types::Result<()>;

//     async fn clear_data(&self, handle: String) -> types::Result<()>;
// }