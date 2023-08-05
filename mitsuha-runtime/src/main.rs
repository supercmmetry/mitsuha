mod plugin;
mod service;

use std::collections::HashMap;

use mitsuha_core::{
    channel::ComputeInput,
    kernel::StorageSpec,
    module::{ModuleInfo, ModuleType},
};
use plugin::{
    load_plugins, one_storage::OneStoragePlugin, wasmtime::WasmtimePlugin, Plugin, PluginContext, common::{EofPlugin, SystemPlugin}, qflow::QFlowPlugin, delegator::DelegatorPlugin,
};
use service::channel::ChannelService;

use std::sync::Arc;

use mitsuha_channel::{
    context::ChannelContext, system::SystemChannel,
};
use mitsuha_core::{
    channel::ComputeChannel,
    config,
    selector::Label,
    storage::{StorageClass, StorageLocality},
};
use std::sync::Once;

static LOG_INIT_ONCE: Once = Once::new();

pub fn init_basic_logging() {
    LOG_INIT_ONCE.call_once(|| {
        env_logger::init();
    });
}

pub fn make_system_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(SystemChannel::new()))
}

pub fn make_basic_storage_config() -> config::storage::Storage {
    config::storage::Storage {
        classes: vec![
            StorageClass {
                kind: mitsuha_core::storage::StorageKind::Memory,
                locality: StorageLocality::Solid {
                    cache_name: Some("cache_memory_1".to_string()),
                },
                name: "solid_memory_1".to_string(),
                labels: vec![Label {
                    name: "storage".to_string(),
                    value: "sample".to_string(),
                }],
                extensions: HashMap::new(),
            },
            StorageClass {
                kind: mitsuha_core::storage::StorageKind::Memory,
                locality: StorageLocality::Cache { ttl: 1 },
                name: "cache_memory_1".to_string(),
                labels: vec![],
                extensions: HashMap::new(),
            },
        ],
    }
}

pub fn make_basic_config() -> config::Config {
    config::Config {
        executor: config::executor::Executor { max_instances: 2 },
        storage: make_basic_storage_config(),
        plugins: vec![
            config::plugin::Plugin {
                name: DelegatorPlugin.name().to_string(),
                extensions: [(
                    "channel_id".to_string(),
                    "delegator-0".to_string(),
                ),
                (
                    "slave_id".to_string(),
                    "qflow-0".to_string(),
                ),
                (
                    "max_jobs".to_string(),
                    "1".to_string(),
                )]
                .into_iter()
                .collect(),
            },
            config::plugin::Plugin {
                name: SystemPlugin.name().to_string(),
                extensions: [(
                    "channel_id".to_string(),
                    "system-0".to_string(),
                )]
                .into_iter()
                .collect(),
            },
            config::plugin::Plugin {
                name: WasmtimePlugin.name().to_string(),
                extensions: [(
                    "channel_id".to_string(),
                    "wasmtime-0".to_string(),
                )]
                .into_iter()
                .collect(),
            },
            config::plugin::Plugin {
                name: OneStoragePlugin.name().to_string(),
                extensions: [(
                    "selector".to_string(),
                    "{\"name\": \"storage\", \"value\": \"sample\"}".to_string(),
                ),(
                    "channel_id".to_string(),
                    "onestorage-0".to_string(),
                )]
                .into_iter()
                .collect(),
            },
            config::plugin::Plugin {
                name: EofPlugin.name().to_string(),
                extensions: [(
                    "channel_id".to_string(),
                    "eof-0".to_string(),
                )]
                .into_iter()
                .collect(),
            },
            config::plugin::Plugin {
                name: QFlowPlugin.name().to_string(),
                extensions: [(
                    "channel_id".to_string(),
                    "qflow-0".to_string(),
                ),(
                    "kind".to_string(),
                    "tikv".to_string(),
                ),(
                    "client_id".to_string(),
                    "localhost".to_string(),
                ),(
                    "desired_queue_count".to_string(),
                    "16".to_string(),
                ),(
                    "pd_endpoints".to_string(),
                    "127.0.0.1:2379".to_string()
                )]
                .into_iter()
                .collect(),
            },
        ],
    }
}

pub async fn make_channel_context() -> ChannelContext {
    init_basic_logging();

    let mut plugin_ctx = PluginContext::new(make_basic_config(), Default::default());

    plugin_ctx = load_plugins(plugin_ctx).await;

    plugin_ctx.channel_context
}

pub async fn upload_artifacts(channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>, ctx: ChannelContext) {
    let wasm_echo: Vec<u8> = include_bytes!(
        "../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm"
    )
    .to_vec();
    let wasm_loop: Vec<u8> = include_bytes!(
        "../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm"
    )
    .to_vec();
    let wasm_main: Vec<u8> = include_bytes!(
        "../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm"
    )
    .to_vec();

    let module_info_echo = ModuleInfo {
        name: "mitsuha.test.echo".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let module_info_loop = ModuleInfo {
        name: "mitsuha.test.loop".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let module_info_main = ModuleInfo {
        name: "mitsuha.test.main".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let spec_echo = StorageSpec {
        handle: module_info_echo.get_identifier(),
        data: wasm_echo,
        ttl: 86400,
        extensions: Default::default(),
    };

    let spec_loop = StorageSpec {
        handle: module_info_loop.get_identifier(),
        data: wasm_loop,
        ttl: 86400,
        extensions: Default::default(),
    };

    let spec_main = StorageSpec {
        handle: module_info_main.get_identifier(),
        data: wasm_main,
        ttl: 86400,
        extensions: Default::default(),
    };

    channel
        .compute(
            ctx.clone(),
            ComputeInput::Store { spec: spec_echo },
        )
        .await
        .unwrap();
    channel
        .compute(
            ctx.clone(),
            ComputeInput::Store { spec: spec_loop },
        )
        .await
        .unwrap();
    channel
        .compute(
            ctx.clone(),
            ComputeInput::Store { spec: spec_main },
        )
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    let channel_context = make_channel_context().await;

    upload_artifacts(channel_context.channel_start.clone().unwrap(), channel_context.clone()).await;

    let channel_service = ChannelService::new(channel_context.clone());

    let (_, health_service) = tonic_health::server::health_reporter();

    let mut rpc_server = tonic::transport::Server::builder().add_service(health_service);

    rpc_server = channel_service.register_rpc(rpc_server);

    rpc_server
        .serve("[::1]:50051".parse().unwrap())
        .await
        .unwrap();
}
