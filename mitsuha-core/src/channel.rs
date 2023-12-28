use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mitsuha_core_types::{
    channel::{ComputeInput, ComputeOutput},
    kernel::{JobSpec, JobStatus, StorageSpec, AsyncKernel},
};

use crate::{constants::Constants, errors::Error, kernel::Kernel, types};

pub trait ComputeInputExt {
    fn get_handle(&self) -> String;

    fn get_original_handle(&self) -> String;

    fn get_extensions(&self) -> &HashMap<String, String>;

    fn get_extensions_mut(&mut self) -> &mut HashMap<String, String>;

    fn is_storage_input(&self) -> bool;
}

impl ComputeInputExt for ComputeInput {
    fn get_handle(&self) -> String {
        match self {
            ComputeInput::Store { spec } => spec.handle.clone(),
            ComputeInput::Load { handle, .. } => handle.clone(),
            ComputeInput::Persist { handle, .. } => handle.clone(),
            ComputeInput::Clear { handle, .. } => handle.clone(),
            ComputeInput::Run { spec } => spec.handle.clone(),
            ComputeInput::Extend { handle, .. } => handle.clone(),
            ComputeInput::Status { handle, .. } => handle.clone(),
            ComputeInput::Abort { handle, .. } => handle.clone(),
        }
    }

    fn get_original_handle(&self) -> String {
        self.get_extensions()
            .get(&Constants::OriginalHandle.to_string())
            .cloned()
            .unwrap_or(self.get_handle())
    }

    fn get_extensions(&self) -> &HashMap<String, String> {
        match self {
            ComputeInput::Store { spec } => &spec.extensions,
            ComputeInput::Load { extensions, .. } => extensions,
            ComputeInput::Persist { extensions, .. } => extensions,
            ComputeInput::Clear { extensions, .. } => extensions,
            ComputeInput::Run { spec } => &spec.extensions,
            ComputeInput::Extend { extensions, .. } => extensions,
            ComputeInput::Status { extensions, .. } => extensions,
            ComputeInput::Abort { extensions, .. } => extensions,
        }
    }

    fn get_extensions_mut(&mut self) -> &mut HashMap<String, String> {
        match self {
            ComputeInput::Store { spec } => &mut spec.extensions,
            ComputeInput::Load { extensions, .. } => extensions,
            ComputeInput::Persist { extensions, .. } => extensions,
            ComputeInput::Clear { extensions, .. } => extensions,
            ComputeInput::Run { spec } => &mut spec.extensions,
            ComputeInput::Extend { extensions, .. } => extensions,
            ComputeInput::Status { extensions, .. } => extensions,
            ComputeInput::Abort { extensions, .. } => extensions,
        }
    }

    fn is_storage_input(&self) -> bool {
        match self {
            ComputeInput::Store { .. } => true,
            ComputeInput::Load { .. } => true,
            ComputeInput::Persist { .. } => true,
            ComputeInput::Clear { .. } => true,
            _ => false,
        }
    }
}

#[async_trait]
pub trait ComputeChannel: Send + Sync {
    type Context;

    fn id(&self) -> String;

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

    async fn extend_job(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Extend {
                    handle,
                    ttl,
                    extensions,
                },
            )
            .await?;
        Ok(())
    }

    async fn abort_job(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Abort { handle, extensions },
            )
            .await?;
        Ok(())
    }

    async fn get_job_status(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<JobStatus> {
        let output = self
            .channel
            .compute(
                Context::default(),
                ComputeInput::Status { handle, extensions },
            )
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

    async fn load_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>> {
        let output = self
            .channel
            .compute(
                Context::default(),
                ComputeInput::Load { handle, extensions },
            )
            .await?;
        match output {
            ComputeOutput::Loaded { data } => Ok(data),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected ComputeOutput with loaded type"),
            }),
        }
    }

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Persist {
                    handle,
                    ttl,
                    extensions,
                },
            )
            .await?;
        Ok(())
    }

    async fn clear_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()> {
        self.channel
            .compute(
                Context::default(),
                ComputeInput::Clear { handle, extensions },
            )
            .await?;
        Ok(())
    }
}

impl<Context> ComputeKernel<Context>
where
    Context: Send,
{
    pub fn new(channel: Arc<Box<dyn ComputeChannel<Context = Context>>>) -> Self {
        Self { channel }
    }
}

pub struct MusubiKernelWrapper<T>(T) where T: Kernel;

#[async_trait]
impl<T> AsyncKernel for MusubiKernelWrapper<T>
where
    T: Kernel,
{
    async fn run_job(&self, spec: JobSpec) -> anyhow::Result<()> {
        self.0.run_job(spec).await?;
        Ok(())
    }

    async fn extend_job(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.extend_job(handle, ttl, extensions).await?;
        Ok(())
    }

    async fn abort_job(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.abort_job(handle, extensions).await?;
        Ok(())
    }

    async fn get_job_status(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<JobStatus> {
        let output = self.0.get_job_status(handle, extensions).await?;
        Ok(output)
    }

    async fn store_data(&self, spec: StorageSpec) -> anyhow::Result<()> {
        self.0.store_data(spec).await?;
        Ok(())
    }

    async fn load_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<Vec<u8>> {
        let output = self.0.load_data(handle, extensions).await?;
        Ok(output)
    }

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.persist_data(handle, ttl, extensions).await?;
        Ok(())
    }

    async fn clear_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.0.clear_data(handle, extensions).await?;
        Ok(())
    }
}