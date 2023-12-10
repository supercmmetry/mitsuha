mod plugin;
mod service;
mod telemetry;

use mitsuha_core::config::Config;
use plugin::{load_plugins, PluginContext};
use service::channel::ChannelService;

use mitsuha_channel::context::ChannelContext;

use std::sync::Once;

static LOG_INIT_ONCE: Once = Once::new();

pub fn init_basic_logging() {
    LOG_INIT_ONCE.call_once(|| {
        env_logger::init();
    });
}

pub async fn make_channel_context(config: &Config) -> ChannelContext {
    // init_basic_logging();

    let mut plugin_ctx = PluginContext::new(config.clone(), Default::default());

    plugin_ctx = load_plugins(plugin_ctx).await;

    plugin_ctx.channel_context
}

#[tokio::main]
async fn main() {
    let _ = dotenv::dotenv();

    let config = Config::new().unwrap();

    telemetry::setup(&config).unwrap();

    let channel_context = make_channel_context(&config).await;

    let channel_service = ChannelService::new(channel_context.clone());

    let (_, health_service) = tonic_health::server::health_reporter();

    let mut rpc_server = tonic::transport::Server::builder().add_service(health_service);

    rpc_server = channel_service.register_rpc(rpc_server);

    tracing::info!("Starting RPC server on port: {}", config.api.port);

    rpc_server
        .serve(
            format!("{}:{}", config.api.address, config.api.port)
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
}
