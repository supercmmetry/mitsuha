use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    errors::Error,
    kernel::{JobSpec, JobStatus, Kernel, StorageSpec},
    types,
};

pub enum ComputeInput {
    Store { spec: StorageSpec },
    Load { handle: String },
    Persist { handle: String, ttl: u64 },
    Clear { handle: String },

    Run { spec: JobSpec },
    Extend { handle: String, ttl: u64 },
    Status { handle: String },
    Abort { handle: String },
}

pub enum ComputeOutput {
    Status { status: JobStatus },
    Loaded { data: Vec<u8> },
    Completed,
}

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    type Context;

    async fn id(&self) -> types::Result<String>;

    async fn compute(&self, ctx: Self::Context, elem: ComputeInput)
        -> types::Result<ComputeOutput>;

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>);
}

pub struct ComputeKernel<Context> {
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
}

#[async_trait]
impl<Context> Kernel for ComputeKernel<Context>
where
    Context: Send + Default,
{
    async fn run_job(&self, spec: JobSpec) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Run { spec })
            .await?;
        Ok(())
    }

    async fn extend_job(&self, handle: String, ttl: u64) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Extend { handle, ttl })
            .await?;
        Ok(())
    }

    async fn abort_job(&self, handle: String) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Abort { handle })
            .await?;
        Ok(())
    }

    async fn get_job_status(&self, handle: String) -> types::Result<JobStatus> {
        let output = self
            .channel
            .compute(Context::default(), ComputeInput::Status { handle })
            .await?;
        match output {
            ComputeOutput::Status { status } => Ok(status),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected ComputeOutput with status type"),
            }),
        }
    }

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Store { spec })
            .await?;
        Ok(())
    }

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>> {
        let output = self
            .channel
            .compute(Context::default(), ComputeInput::Load { handle })
            .await?;
        match output {
            ComputeOutput::Loaded { data } => Ok(data),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected ComputeOutput with loaded type"),
            }),
        }
    }

    async fn persist_data(&self, handle: String, ttl: u64) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Persist { handle, ttl })
            .await?;
        Ok(())
    }

    async fn clear_data(&self, handle: String) -> types::Result<()> {
        self.channel
            .compute(Context::default(), ComputeInput::Clear { handle })
            .await?;
        Ok(())
    }
}

impl<Context> ComputeKernel<Context> where Context: Send {
    pub fn new(channel: Arc<Box<dyn ComputeChannel<Context = Context>>>) -> Self {
        Self {
            channel
        }
    }
}