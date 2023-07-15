use async_trait::async_trait;
use mitsuha_channel::delegator::DelegatorChannel;
use mitsuha_core::types;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct DelegatorPlugin;

#[async_trait]
impl Plugin for DelegatorPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.delegator"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let raw_channel = DelegatorChannel::new(
            ctx.extensions.get("slave_id").unwrap().clone(),
            ctx.extensions
                .get("max_jobs")
                .unwrap()
                .clone()
                .parse()
                .unwrap(),
        );

        let channel = initialize_channel(&ctx, raw_channel)?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
