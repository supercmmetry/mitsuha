use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    kernel::JobStatus,
    module::ModuleInfo,
    types,
};
use tokio::sync::mpsc::{Sender, Receiver};

use crate::{job_controller::JobState, util};


pub struct JobContext {
    updater: Sender<JobState>,
    reader: Receiver<JobState>,
    desired: JobState,
    actual: JobState,
}

impl JobContext {
    pub async fn new(updater: Sender<JobState>, reader: Receiver<JobState>, desired: JobState) -> Self {
        updater.send(desired.clone()).await.unwrap();

        Self {
            updater,
            reader,
            actual: desired.clone(),
            desired,
        }
    }

    pub async fn set_state(&mut self, desired: JobState) -> types::Result<()> {
        self.desired = desired;

        if self.desired != self.actual {
            self.updater.send(self.desired.clone()).await.map_err(|e| Error::Unknown { source: e.into() })?;
        }

        Ok(())
    }

    pub fn get_state(&mut self) -> types::Result<JobState> {
        match self.reader.try_recv() {
            Ok(x) => {
                self.actual = x;
                
            },
            Err(_) => {}
        }

        Ok(self.actual.clone())
    }
}

pub struct SystemChannel<Context: Send> {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>,
    id: String,
}

#[async_trait]
pub trait SystemContext {
    async fn get_job_status(&self, handle: &String) -> types::Result<JobStatus>;

    async fn extend_job(&self, handle: &String, ttl: u64) -> types::Result<()>;

    async fn abort_job(&self, handle: &String) -> types::Result<()>;

    fn register_job_context(&self, handle: String, ctx: JobContext);
}

#[async_trait]
impl<Context> ComputeChannel for SystemChannel<Context>
where
    Context: SystemContext + Send + Sync,
{
    type Context = Context;

    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())
    }

    async fn compute(&self, ctx: Context, mut elem: ComputeInput) -> types::Result<ComputeOutput> {
        match &mut elem {
            ComputeInput::Store { ref mut spec } => {
                if ModuleInfo::equals_identifier_type(&spec.handle) {
                    spec.ttl = 864000;
                }
            }
            ComputeInput::Extend { handle, ttl } => {
                ctx.extend_job(handle, *ttl).await?;
                return Ok(ComputeOutput::Completed)
            }
            ComputeInput::Abort { handle } => {
                ctx.abort_job(handle).await?;
                return Ok(ComputeOutput::Completed)
            }
            _ => {}
        }

        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Context>>>) {
        *self.next.write().await = Some(next);
    }
}

impl<Context> SystemChannel<Context> where Context: Send {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/system"
    }

    pub fn new() -> Self {
        let id = format!(
            "{}/{}",
            Self::get_identifier_type(),
            util::generate_random_id()
        );

        Self { next: Arc::new(tokio::sync::RwLock::new(None)), id }
    }
}
