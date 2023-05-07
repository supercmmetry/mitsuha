use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    kernel::JobStatus,
    module::ModuleInfo,
    types,
};

use crate::{job_future::JobState, util};

pub struct SystemChannel<Context: Send> {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>,
    id: String,
}

pub trait SystemContext {
    fn get_job_status(&self, handle: &String) -> types::Result<JobStatus>;

    fn extend_job(&self, handle: &String, ttl: u64) -> types::Result<()>;

    fn abort_job(&self, handle: &String) -> types::Result<()>;

    fn make_job_state(&self, handle: String, state: JobState) -> Arc<RwLock<JobState>>;
}

#[async_trait]
impl<Context> ComputeChannel for SystemChannel<Context>
where
    Context: SystemContext + Send,
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
                ctx.extend_job(handle, *ttl)?;
                return Ok(ComputeOutput::Completed)
            }
            ComputeInput::Abort { handle } => {
                ctx.abort_job(handle)?;
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
