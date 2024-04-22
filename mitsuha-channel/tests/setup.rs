use std::{collections::HashMap, sync::Arc};

use mitsuha_channel::{
    labeled_storage::LabeledStorageChannel, system::SystemChannel, wasmtime::WasmtimeChannel,
    EntrypointChannel,
};
use mitsuha_core::channel::{ChannelContext, ChannelManager};
use mitsuha_core::config::Config;
use mitsuha_core::job::cost::{JobCost, StandardJobCostEvaluator};
use mitsuha_core::job::mgr::JobManager;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeKernel},
    config,
    kernel::Kernel,
    resolver::{blob::BlobResolver, Resolver},
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_core_types::module::ModuleInfo;
use mitsuha_storage::UnifiedStorage;
use std::sync::Once;

#[allow(dead_code)]
static LOG_INIT_ONCE: Once = Once::new();

#[allow(dead_code)]
pub fn init_basic_logging() {
    LOG_INIT_ONCE.call_once(|| {
        env_logger::init();
    });
}

#[allow(dead_code)]
pub fn make_init_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(EntrypointChannel::new()))
}

#[allow(dead_code)]
pub fn make_system_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(
        SystemChannel::new().with_id("system-0".to_string()),
    ))
}

pub async fn init_global_channel_ctx(
    init_channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) {
    let job_manager = JobManager::new(
        init_channel.clone(),
        Arc::new(Box::new(ChannelContext::default())),
        JobCost { compute: u64::MAX },
        Arc::new(Box::new(StandardJobCostEvaluator)),
        "instance".to_string(),
    )
    .unwrap();

    ChannelManager::global_rw().write().await.job_manager = Some(job_manager);
    ChannelManager::global_rw().write().await.channel_start = Some(init_channel);
}

#[allow(dead_code)]
pub fn make_basic_config() -> config::storage::Storage {
    config::storage::Storage {
        classes: vec![
            StorageClass {
                kind: mitsuha_core::storage::StorageKind::Memory,
                locality: StorageLocality::Solid {
                    cache_name: Some("cache_memory_1".to_string()),
                },
                name: "solid_memory_1".to_string(),
                labels: vec![Label {
                    key: "storage".to_string(),
                    value: "sample".to_string(),
                }],
                properties: HashMap::new(),
            },
            StorageClass {
                kind: mitsuha_core::storage::StorageKind::Memory,
                locality: StorageLocality::Cache { ttl: 1 },
                name: "cache_memory_1".to_string(),
                labels: vec![],
                properties: HashMap::new(),
            },
        ],
    }
}

pub async fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).await.unwrap()
}

pub async fn make_labeled_storage_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>
{
    let storage = make_unified_storage().await;

    Arc::new(Box::new(
        LabeledStorageChannel::new(
            storage,
            Label {
                key: "storage".to_string(),
                value: "sample".to_string(),
            },
        )
        .with_id("store-0".to_string()),
    ))
}

#[allow(dead_code)]
pub fn make_kernel(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn Kernel>> {
    Arc::new(Box::new(ComputeKernel::new(chan)))
}

#[allow(dead_code)]
pub fn make_blob_resolver(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>> {
    Arc::new(Box::new(BlobResolver::new(chan)))
}

#[allow(dead_code)]
pub fn make_wasmtime_channel(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(
        WasmtimeChannel::new(make_kernel(chan.clone())).with_id("wasmtime-0".to_string()),
    ))
}
