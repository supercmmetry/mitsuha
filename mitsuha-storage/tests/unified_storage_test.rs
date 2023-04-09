use std::collections::HashMap;

use mitsuha_core::{
    config,
    kernel::StorageSpec,
    selector::Label,
    storage::{StorageClass, StorageLocality},
};
use mitsuha_storage::unified::UnifiedStorage;
use mitsuha_storage::{constants::Constants, Storage};

fn make_basic_config() -> config::storage::Storage {
    config::storage::Storage {
        classes: vec![StorageClass {
            kind: mitsuha_core::storage::StorageKind::Memory,
            locality: StorageLocality::Solid { cache_name: None },
            name: "memory_1".to_string(),
            labels: vec![Label {
                name: "storage".to_string(),
                value: "sample".to_string(),
            }],
            extensions: HashMap::new(),
        }],
    }
}

#[tokio::test]
async fn sample() -> anyhow::Result<()> {
    let config = make_basic_config();
    let mut storage = UnifiedStorage::new(&config)?;

    let mut spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        Constants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    let data = storage.load(spec.handle.clone()).await?;

    assert_eq!("Hello world!".to_string(), String::from_utf8(data).unwrap());

    Ok(())
}
