use crate::plugin::{initialize_channel, Plugin, PluginContext};
use async_trait::async_trait;
use mitsuha_channel::scheduler::SchedulerChannel;
use mitsuha_core::{channel::ChannelManager, errors::ToUnknownErrorResult};
use mitsuha_core::types;
use mitsuha_scheduler::scheduler::{Scheduler, SchedulerPostJobHook};
use std::sync::Arc;

#[derive(Clone)]
pub struct SchedulerPlugin;

#[async_trait]
impl Plugin for SchedulerPlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.scheduler"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let scheduler =
            Scheduler::new(ctx.channel_start.clone(), ctx.plugin_configuration.get_spec().to_unknown_err_result()?).await?;

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
