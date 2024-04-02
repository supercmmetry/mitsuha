use std::sync::Arc;

use async_trait::async_trait;
use backoff::backoff::Backoff;
use mitsuha_core::channel::{ChannelContext, ChannelUtilityProvider};
use mitsuha_core::{channel::ComputeChannel, errors::Error, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use tokio::sync::RwLock;

use crate::{NextComputeChannel, WrappedComputeChannel};

pub struct QFlowWriterChannel<Context: Send, B: Backoff> {
    backoff: B,
    writer: Arc<Box<dyn mitsuha_qflow::Writer>>,
    next: NextComputeChannel<Context>,
    id: String,
}

#[async_trait]
impl<B> ComputeChannel for QFlowWriterChannel<ChannelContext, B>
where
    B: Send + Sync + Backoff + Clone,
{
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        mut elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        let mut backoff = self.backoff.clone();
        backoff.reset();

        loop {
            ctx.unsign_compute_input(&mut elem).await;

            let result = self
                .writer
                .write_compute_input(elem.clone())
                .await
                .map_err(|e| Error::Unknown { source: e });

            let backoff_duration = backoff.next_backoff();

            if let Err(e) = result {
                tracing::warn!("failed to write to qflow queues, error: {}", e);

                if backoff_duration.is_none() {
                    tracing::error!("all attempts to write to the qflow queues failed");
                    return Err(e);
                } else {
                    tokio::time::sleep(backoff_duration.unwrap()).await;
                }
            } else {
                break;
            }
        }

        Ok(ComputeOutput::Submitted)
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl<B> QFlowWriterChannel<ChannelContext, B>
where
    B: Backoff + Clone + Send + Sync,
{
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/qflow_writer"
    }

    pub fn new(
        writer: Arc<Box<dyn mitsuha_qflow::Writer>>,
        backoff: B,
    ) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            backoff,
            writer,
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
