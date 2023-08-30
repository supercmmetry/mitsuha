use async_trait::async_trait;
use mitsuha_channel::interceptor::InterceptorChannel;
use mitsuha_core::{errors::Error, types};
use mitsuha_runtime_rpc::proto::channel::interceptor_client::InterceptorClient;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct InterceptorPlugin;

#[async_trait]
impl Plugin for InterceptorPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.interceptor"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let target_address = ctx.extensions.get("address").unwrap();

        let conn = tonic::transport::Endpoint::new(target_address.clone())
            .map_err(|e| Error::Unknown { source: e.into() })?
            .connect_lazy();
        
        let client = InterceptorClient::new(conn);

        let raw_channel = InterceptorChannel::new(client);
        let channel = initialize_channel(&ctx, raw_channel)?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
