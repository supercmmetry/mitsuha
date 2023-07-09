use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_channel::{InitChannel, context::ChannelContext};
use mitsuha_core::{channel::ComputeChannel, types};

use super::{Plugin, PluginContext};

#[derive(Clone)]
pub struct InitPlugin;

#[async_trait]
impl Plugin for InitPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.init"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> = Arc::new(Box::new(InitChannel::new()));
        channel.connect(ctx.channel_start.clone()).await;

        ctx.channel_start = channel.clone();
        ctx.channel_context.channel_start = Some(channel);

        Ok(ctx)
    }
}
