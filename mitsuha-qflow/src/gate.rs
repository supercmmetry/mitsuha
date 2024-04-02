use crate::ComputeInputGate;
use async_trait::async_trait;
use mitsuha_core::channel::ChannelContext;
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::types;
use mitsuha_core_types::channel::ComputeInput;

pub struct StandardComputeInputGate<T: JobManagerProvider> {
    ctx: T,
}

impl<T> StandardComputeInputGate<T>
where
    T: JobManagerProvider,
{
    pub fn new(ctx: T) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl ComputeInputGate for StandardComputeInputGate<ChannelContext> {
    async fn evaluate_compute_input(&self, input: &mut ComputeInput) -> types::Result<()> {
        self.ctx.get_mgr().read().await.sign_compute_input(input);

        match input {
            ComputeInput::Run { spec } => {
                let job_mgr = self.ctx.get_job_mgr().await;

                job_mgr.queue_job(spec).await?;
            }
            _ => {}
        }

        Ok(())
    }
}
