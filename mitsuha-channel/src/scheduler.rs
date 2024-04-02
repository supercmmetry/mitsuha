use crate::{NextComputeChannel, WrappedComputeChannel};
use async_trait::async_trait;
use mitsuha_core::channel::{
    ChannelContext, ChannelUtilityProvider, ComputeChannel, StateProvider,
};
use mitsuha_core::errors::{Error, ToUnknownErrorResult};
use mitsuha_core::{err_unsupported_op, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_scheduler::constant::SchedulerConstants;
use mitsuha_scheduler::scheduler::Scheduler;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SchedulerChannel {
    id: String,
    next: NextComputeChannel<ChannelContext>,
    scheduler: Scheduler<ChannelContext>,
}

#[async_trait]
impl ComputeChannel for SchedulerChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        match &elem {
            ComputeInput::Run { .. } | ComputeInput::Abort { .. } | ComputeInput::Extend { .. } => {
                if self.scheduler.schedule(ctx.clone(), &elem).await? {
                    if ctx.is_compute_input_signed(&elem).await {
                        let result = self
                            .next
                            .read()
                            .await
                            .as_ref()
                            .unwrap()
                            .compute(ctx.clone(), elem)
                            .await;

                        // Note that we don't perform job_queue cleanup as it is already handled in PostJobHooks.
                        self.perform_job_command_cleanup(ctx).await?;

                        result
                    } else {
                        Err(err_unsupported_op!(
                            "cannot process unsigned compute input!"
                        ))
                    }
                } else {
                    Ok(ComputeOutput::Submitted)
                }
            }
            _ => {
                self.next
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .compute(ctx, elem)
                    .await
            }
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>) {
        *self.next.write().await = Some(next);
    }
}

impl SchedulerChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/scheduler"
    }

    pub fn new(scheduler: Scheduler<ChannelContext>) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            scheduler,
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }

    async fn perform_job_command_cleanup(&self, ctx: ChannelContext) -> types::Result<()> {
        match (
            ctx.get_value(&SchedulerConstants::JobCommandIdParameter.to_string()),
            ctx.get_value(&SchedulerConstants::StorageHandleParameter.to_string()),
        ) {
            (Some(command_id), Some(storage_handle)) => {
                self.scheduler
                    .remove_job_command(
                        ctx,
                        command_id.parse().to_unknown_err_result()?,
                        storage_handle,
                    )
                    .await?;
            }
            _ => {}
        }

        Ok(())
    }
}
