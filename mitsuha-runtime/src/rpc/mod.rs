use crate::plugin::{load_plugins, PluginContext};
use crate::rpc::channel::ChannelService;
use anyhow::anyhow;
use mitsuha_core::config::Config;
use mitsuha_core::types;

pub mod channel;

pub trait Service: Send + Sync {
    fn register_rpc(
        &self,
        server: tonic::transport::server::Router,
    ) -> tonic::transport::server::Router;
}

pub async fn init_channel_manager(config: &Config) -> types::Result<()> {
    let mut plugin_ctx = PluginContext::new(config.clone(), Default::default()).await?;

    plugin_ctx = load_plugins(plugin_ctx).await?;

    Ok(())
}

pub async fn start_server(config: Config) -> anyhow::Result<()> {
    let channel_context = init_channel_manager(&config).await;

    if let Err(e) = channel_context {
        tracing::error!("failed to initialize channel context, {}", e);
        return Err(anyhow!("failed to initialize channel context, {}", e));
    }

    let channel_service = ChannelService::new();

    let (_, health_service) = tonic_health::server::health_reporter();

    let mut rpc_server = tonic::transport::Server::builder().add_service(health_service);

    rpc_server = channel_service.register_rpc(rpc_server);

    tracing::info!("Starting RPC server on port: {}", config.api.rpc_port);

    rpc_server
        .serve(
            format!("{}:{}", config.api.address, config.api.rpc_port)
                .parse()
                .unwrap(),
        )
        .await?;

    Ok(())
}
