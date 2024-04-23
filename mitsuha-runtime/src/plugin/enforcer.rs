use async_trait::async_trait;
use mitsuha_channel::enforcer::EnforcerChannel;
use mitsuha_core::{errors::ToUnknownErrorResult, types};
use serde::Deserialize;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnforcerConfiguration {
    pub policy_blob_key: String,
}

#[derive(Clone)]
pub struct EnforcerPlugin;

#[async_trait]
impl Plugin for EnforcerPlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.enforcer"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let cfg: EnforcerConfiguration = ctx.plugin_configuration.get_spec().to_unknown_err_result()?;

        let raw_channel = EnforcerChannel::new(cfg.policy_blob_key);
        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
