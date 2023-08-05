use async_trait::async_trait;
use mitsuha_channel::{EofChannel, InitChannel, system::SystemChannel};
use mitsuha_core::types;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct SystemPlugin;

#[async_trait]
impl Plugin for SystemPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.system"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let channel = initialize_channel(&ctx, SystemChannel::new())?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}

#[derive(Clone)]
pub struct InitPlugin;

#[async_trait]
impl Plugin for InitPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.init"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let channel = initialize_channel(&ctx, InitChannel::new())?;

        channel.connect(ctx.channel_start.clone()).await;

        ctx.channel_start = channel.clone();
        ctx.channel_context.channel_start = Some(channel);

        Ok(ctx)
    }
}

#[derive(Clone)]
pub struct EofPlugin;

#[async_trait]
impl Plugin for EofPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.eof"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let channel = initialize_channel(&ctx, EofChannel::new())?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
