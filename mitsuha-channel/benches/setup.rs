use std::{collections::HashMap, sync::Arc};

use mitsuha_channel::{
    context::ChannelContext, labeled_storage::LabeledStorageChannel, system::SystemChannel,
    wasmtime::WasmtimeChannel,
};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeKernel},
    config,
    kernel::Kernel,
    resolver::{blob::BlobResolver, Resolver},
    selector::Label,
    storage::{RawStorage, StorageClass, StorageLocality},
};
use mitsuha_core_types::module::ModuleInfo;
use mitsuha_storage::UnifiedStorage;

pub fn make_system_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(SystemChannel::new()))
}

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

pub fn make_unified_storage() -> Arc<Box<dyn RawStorage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).unwrap()
}

pub fn make_labeled_storage_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    let storage = make_unified_storage();

    Arc::new(Box::new(LabeledStorageChannel::new(
        storage,
        Label {
            key: "storage".to_string(),
            value: "sample".to_string(),
        },
    )))
}

pub fn make_kernel(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn Kernel>> {
    Arc::new(Box::new(ComputeKernel::new(chan)))
}

pub fn make_blob_resolver(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>> {
    Arc::new(Box::new(BlobResolver::new(chan)))
}

pub fn make_wasmtime_channel(
    chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
) -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    Arc::new(Box::new(WasmtimeChannel::new(make_kernel(chan.clone()))))
}
