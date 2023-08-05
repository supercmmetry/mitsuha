use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    kernel::{JobStatusType, JobStatus},
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

        match &elem {
            ComputeInput::Run { spec } => {
                self.run_gc(ctx.clone()).await?;

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
            id: Self::get_identifier_type().to_string(),
            next: Arc::new(RwLock::new(None)),
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
            Some(chan) => {
                log::debug!("forwarding compute to channel '{}'", chan.id());
                chan.compute(ctx, elem).await
            },
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn delegate_compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        log::debug!("delegating compute to slave: '{}'", self.slave_id);

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

        let handles = self.job_handles.read().await.clone();

        for handle in handles { 
            log::debug!("running gc for job with handle: '{}'", handle.clone());

            match ctx.get_job_status(&handle).await {
                Ok(JobStatus { status, .. }) => {
                    if status != JobStatusType::Running {
                        if let Ok(mut v) = self.job_handles.try_write() {
                            v.remove(&handle);
                        } else {
                            log::error!("failed to attain lock on job_handles!");
                        }
                    }
                }
                _ => {
                    if let Ok(mut v) = self.job_handles.try_write() {
                        v.remove(&handle);
                    }
                }
            }
        }

        Ok(())
    }
}
