use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::channel::ChannelContext;
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::{channel::ComputeChannel, err_unsupported_op, errors::Error, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};

use crate::{NextComputeChannel, WrappedComputeChannel};

pub struct SystemChannel {
    next: NextComputeChannel<ChannelContext>,
    id: String,
}

#[async_trait]
impl ComputeChannel for SystemChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        mut elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        match &mut elem {
            ComputeInput::Run { spec } => {
                if !ctx.get_job_mgr().await.queue_job(spec).await? {
                    tracing::error!("job allocation failed!");
                    return Err(err_unsupported_op!("cannot allocate resources for job"));
                }
            }
            ComputeInput::Extend { handle, ttl, .. } => {
                ctx.get_job_mgr().await.extend_job(handle, *ttl).await?;
                return Ok(ComputeOutput::Completed);
            }
            ComputeInput::Abort { handle, .. } => {
                ctx.get_job_mgr().await.abort_job(handle).await?;
                return Ok(ComputeOutput::Completed);
            }
            ComputeInput::Status { handle, extensions } => {
                let status = ctx
                    .get_job_mgr()
                    .await
                    .get_job_status(handle, extensions.clone())
                    .await?;
                return Ok(ComputeOutput::Status { status });
            }
            _ => {}
        }

        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl SystemChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/system"
    }

    pub fn new() -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
