use async_trait::async_trait;
use mitsuha_channel::enforcer::EnforcerChannel;
use mitsuha_core::types;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct EnforcerPlugin;

#[async_trait]
impl Plugin for EnforcerPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.enforcer"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let policy_blob_key = ctx.extensions.get("policy_blob_key").unwrap();

        let raw_channel = EnforcerChannel::new(policy_blob_key.clone());
        let channel = initialize_channel(&ctx, raw_channel)?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
