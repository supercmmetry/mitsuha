use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    kernel::JobStatusType,
    types,
};
use tokio::sync::RwLock;

use crate::{context::ChannelContext, WrappedComputeChannel};

pub struct DelegatorChannel {
    id: String,
    next: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    slave_id: String,
    slave: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    max_jobs: usize,
    job_handles: Arc<RwLock<HashSet<String>>>,
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
            match ctx.channel_map.get(&self.slave_id) {
                Some(slave) => *self.slave.write().await = Some(slave.clone()),
                None => {
                    panic!("slave missing")
                }
            }
        }

        self.run_gc(ctx.clone()).await?;

        match &elem {
            ComputeInput::Run { spec } => {
                if self.job_handles.read().await.len() < self.max_jobs {
                    self.job_handles.write().await.insert(spec.handle.clone());

                    self.forward_compute(ctx, elem).await
                } else {
                    self.delegate_compute(ctx, elem).await
                }
            }
            _ => self.forward_compute(ctx, elem).await,
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

    pub fn new(slave_id: String, max_jobs: usize) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            id: todo!(),
            next: todo!(),
            slave_id,
            slave: Arc::new(RwLock::new(None)),
            max_jobs,
            job_handles: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    async fn forward_compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn delegate_compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        self.slave
            .read()
            .await
            .as_ref()
            .ok_or(Error::UnknownWithMsgOnly {
                message: format!("could not find slave channel"),
            })?
            .compute(ctx, elem)
            .await
    }

    async fn run_gc(&self, ctx: ChannelContext) -> types::Result<()> {
        if self.job_handles.read().await.len() < self.max_jobs {
            return Ok(());
        }

        for handle in self.job_handles.read().await.clone() {
            let status = self
                .forward_compute(
                    ctx.clone(),
                    ComputeInput::Status {
                        handle: handle.clone(),
                    },
                )
                .await?;

            match status {
                ComputeOutput::Status { status } => {
                    if status.status != JobStatusType::Running {
                        if let Ok(mut v) = self.job_handles.try_write() {
                            v.remove(&handle);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}
