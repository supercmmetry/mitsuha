use crate::plugin::{initialize_channel, Plugin, PluginContext};
use async_trait::async_trait;
use mitsuha_channel::scheduler::SchedulerChannel;
use mitsuha_core::channel::ChannelManager;
use mitsuha_core::types;
use mitsuha_scheduler::scheduler::{Scheduler, SchedulerPostJobHook};
use std::sync::Arc;

#[derive(Clone)]
pub struct SchedulerPlugin;

#[async_trait]
impl Plugin for SchedulerPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.scheduler"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let scheduler =
            Scheduler::new(ctx.channel_start.clone(), ctx.current_properties.clone()).await?;

        ChannelManager::global_rw()
            .read()
            .await
            .get_job_mgr()
            .add_post_job_hook(Arc::new(SchedulerPostJobHook::new(scheduler.clone())))
            .await;

        let raw_channel = SchedulerChannel::new(scheduler);
        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
