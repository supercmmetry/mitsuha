use std::{collections::HashMap, sync::Arc, time::Duration};

use mitsuha_core::{
    config,
    constants::Constants,
    kernel::StorageSpec,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_storage::unified::UnifiedStorage;

mod setup;
use setup::*;

static LOCAL_ROOT_DIR_SETUP_ONCE: std::sync::Once = std::sync::Once::new();

fn get_root_dir() -> String {
    format!(
        "{}/{}",
        std::env::temp_dir().as_os_str().to_str().unwrap(),
        "mitsuha-storage-local-test"
    )
}

fn setup_root_dir() {
    let root_dir = get_root_dir();

    log::info!("test root dir set to '{}'", root_dir);

    LOCAL_ROOT_DIR_SETUP_ONCE.call_once(|| {
        _ = std::fs::remove_dir_all(root_dir.clone());
        _ = std::fs::create_dir(root_dir.clone());
    })
}

fn make_basic_config() -> config::storage::Storage {
    init_basic_logging();
    setup_root_dir();

    let root_dir = get_root_dir();

    config::storage::Storage {
        classes: vec![
            StorageClass {
                kind: mitsuha_core::storage::StorageKind::Local,
                locality: StorageLocality::Solid {
                    cache_name: Some("cache_memory_1".to_string()),
                },
                name: "solid_memory_1".to_string(),
                labels: vec![Label {
                    name: "storage".to_string(),
                    value: "sample".to_string(),
                }],
                extensions: [
                    ("rootDirectory".to_string(), root_dir),
                    ("enableGC".to_string(), "true".to_string()),
                ]
                .iter()
                .cloned()
                .collect(),
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

fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).unwrap()
}

#[tokio::test]
async fn store_and_load() -> anyhow::Result<()> {
    let storage = make_unified_storage();

    let mut spec = StorageSpec {
        handle: "store_and_load_spec1".to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_and_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage();

    let mut spec = StorageSpec {
        handle: "store_and_expire_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 1,
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

    tokio::time::sleep(Duration::from_secs(4)).await;

    let result = storage.exists(spec.handle.clone()).await;

    assert_eq!(result.unwrap(), false);
    Ok(())
}

#[tokio::test]
async fn store_persist_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage();

    let mut spec = StorageSpec {
        handle: "store_persist_expire_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 4,
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

    storage.persist(spec.handle.clone(), 4).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut result = storage.load(spec.handle.clone()).await;

    assert!(result.is_ok());

    tokio::time::sleep(Duration::from_secs(7)).await;

    assert_eq!(storage.exists(spec.handle.clone()).await.unwrap(), false);

    result = storage.load(spec.handle.clone()).await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn store_and_clear() -> anyhow::Result<()> {
    let storage = make_unified_storage();

    let mut spec = StorageSpec {
        handle: "store_and_clear_spec1".to_string(),
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
    storage.clear(spec.handle.clone()).await?;

    let result = storage.load(spec.handle.clone()).await;

    assert!(result.is_err());
    Ok(())
}
