use async_trait::async_trait;
use mitsuha_channel::namespacer::NamespacerChannel;
use mitsuha_core::types;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct NamespacerPlugin;

#[async_trait]
impl Plugin for NamespacerPlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.namespacer"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let raw_channel = NamespacerChannel::new();
        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
