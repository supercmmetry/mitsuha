use async_trait::async_trait;
use mitsuha_channel::interceptor::InterceptorChannel;
use mitsuha_core::errors::ToUnknownErrorResult;
use mitsuha_core::{errors::Error, types};
use mitsuha_runtime_rpc::proto::channel::interceptor_client::InterceptorClient;
use serde::Deserialize;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InterceptorConfiguration {
    pub address: String,
}

#[derive(Clone)]
pub struct InterceptorPlugin;

#[async_trait]
impl Plugin for InterceptorPlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.interceptor"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let cfg: InterceptorConfiguration = ctx.plugin_configuration.get_spec().to_unknown_err_result()?;

        let conn = tonic::transport::Endpoint::new(cfg.address)
            .to_unknown_err_result()?
            .connect_lazy();

        let client = InterceptorClient::new(conn);

        let raw_channel = InterceptorChannel::new(client);
        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
