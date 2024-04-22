mod http;
mod plugin;
mod rpc;
mod telemetry;

use mitsuha_core::config::Config;

#[tokio::main]
async fn main() {
    let _ = dotenv::dotenv();

    let config = Config::new().unwrap();

    mitsuha_persistence::apply_migrations().await;

    telemetry::setup(&config).unwrap();

    let http_server = tokio::task::spawn(http::start(config.clone()));
    let rpc_server = tokio::task::spawn(rpc::start(config));

    tokio::try_join!(http_server, rpc_server).unwrap();
}
