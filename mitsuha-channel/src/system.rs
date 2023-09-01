use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    types,
};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{context::ChannelContext, job_controller::JobState, WrappedComputeChannel};

pub struct JobContext {
    handle: String,
    updater: Sender<JobState>,
    reader: Receiver<JobState>,
    desired: JobState,
    actual: JobState,
}

impl JobContext {
    pub async fn new(
        handle: String,
        updater: Sender<JobState>,
        reader: Receiver<JobState>,
        desired: JobState,
    ) -> Self {
        updater.send(desired.clone()).await.unwrap();

        Self {
            handle,
            updater,
            reader,
            actual: desired.clone(),
            desired,
        }
    }

    pub async fn set_state(&mut self, desired: JobState) -> types::Result<()> {
        self.desired = desired;

        if self.desired != self.actual {
            tracing::info!(
                "updating job context from {:?} to {:?} for handle: '{}'",
                self.actual,
                self.desired,
                self.handle
            );

            self.updater
                .send(self.desired.clone())
                .await
                .map_err(|e| Error::Unknown { source: e.into() })?;
        }

        Ok(())
    }

    pub fn get_state(&mut self) -> types::Result<JobState> {
        match self.reader.try_recv() {
            Ok(x) => {
                tracing::info!(
                    "received new job state {:?} for handle: '{}'",
                    x,
                    self.handle
                );

                self.actual = x;
            }
            Err(_) => {}
        }

        Ok(self.actual.clone())
    }
}

pub struct SystemChannel {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
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
            ComputeInput::Extend { handle, ttl, .. } => {
                ctx.extend_job(handle, *ttl).await?;
                return Ok(ComputeOutput::Completed);
            }
            ComputeInput::Abort { handle, .. } => {
                ctx.abort_job(handle).await?;
                return Ok(ComputeOutput::Completed);
            }
            ComputeInput::Status { handle, .. } => {
                let status = ctx.get_job_status(handle).await?;
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
