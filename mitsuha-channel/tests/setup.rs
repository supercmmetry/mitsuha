use std::{collections::HashMap, sync::Arc, time::Duration};

use mitsuha_channel::{
    context::ChannelContext,
    labeled_storage::{self, LabeledStorageChannel},
    system::SystemChannel,
};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    config,
    kernel::StorageSpec,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_storage::UnifiedStorage;

pub fn make_system_channel() -> SystemChannel<ChannelContext> {
    SystemChannel::new()
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

pub fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).unwrap()
}

pub fn make_labeled_storage_channel() -> LabeledStorageChannel<ChannelContext> {
    let storage = make_unified_storage();

    LabeledStorageChannel::new(
        storage,
        Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        },
    )
}

pub async fn make_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    let mut system_channel = make_system_channel();
    let labeled_storage_channel = make_labeled_storage_channel();

    system_channel
        .connect(Arc::new(Box::new(labeled_storage_channel)))
        .await;

    Arc::new(Box::new(system_channel))
}
