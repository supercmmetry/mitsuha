use async_trait::async_trait;
use mitsuha_channel::delegator::DelegatorChannel;
use mitsuha_core::{errors::ToUnknownErrorResult, types};
use serde::Deserialize;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DelegatorConfiguration {
    pub slave_id: String,
}

#[derive(Clone)]
pub struct DelegatorPlugin;

#[async_trait]
impl Plugin for DelegatorPlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.delegator"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let cfg: DelegatorConfiguration = ctx.plugin_configuration.get_spec().to_unknown_err_result()?;

        let raw_channel =
            DelegatorChannel::new(cfg.slave_id);

        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
