use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::channel::{ChannelContext, ChannelUtilityProvider};
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::{channel::ComputeChannel, errors::Error, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use tokio::sync::RwLock;

use crate::{NextComputeChannel, WrappedComputeChannel};

#[derive(Clone)]
pub struct DelegatorChannel {
    id: String,
    next: NextComputeChannel<ChannelContext>,
    slave_id: String,
    slave: NextComputeChannel<ChannelContext>,
}

#[async_trait]
impl ComputeChannel for DelegatorChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: Self::Context,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        if self.slave.read().await.is_none() {
            match ctx.get_mgr().read().await.channel_map.get(&self.slave_id) {
                Some(slave) => *self.slave.write().await = Some(slave.clone()),
                None => {
                    panic!("slave missing")
                }
            }
        }

        match &elem {
            ComputeInput::Run { spec } => {
                if ctx.is_compute_input_signed(&elem).await {
                    self.forward(ctx, elem).await
                } else {
                    if ctx.get_job_mgr().await.queue_job(spec).await? {
                        self.forward(ctx, elem).await
                    } else {
                        self.delegate(ctx, elem).await
                    }
                }
            }
            _ => self.forward(ctx, elem).await,
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>) {
        *self.next.write().await = Some(next);
    }
}

impl DelegatorChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/delegator"
    }

    pub fn new(slave_id: String) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            id: Self::get_identifier_type().to_string(),
            next: Arc::new(RwLock::new(None)),
            slave_id,
            slave: Arc::new(RwLock::new(None)),
        })
    }

    async fn forward(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        match self.next.read().await.clone() {
            Some(chan) => {
                tracing::debug!("forwarding compute to channel '{}'", chan.id());
                chan.compute(ctx, elem).await
            }
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn delegate(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        tracing::debug!("delegating compute to slave channel: '{}'", self.slave_id);

        self.slave
            .read()
            .await
            .as_ref()
            .ok_or(Error::UnknownWithMsgOnly {
                message: "could not find slave channel".to_string(),
            })?
            .compute(ctx, elem)
            .await
    }
}
