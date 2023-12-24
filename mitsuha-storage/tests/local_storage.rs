#![feature(async_iterator)]

use serial_test::serial;
use std::{collections::HashMap, sync::Arc, time::Duration};

use mitsuha_core::{
    config,
    constants::StorageControlConstants,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_core_types::kernel::StorageSpec;
use mitsuha_storage::conf::ConfKey;
use mitsuha_storage::unified::UnifiedStorage;

mod setup;
use setup::*;

use lazy_static::lazy_static;

fn get_root_dir() -> String {
    format!("{}/{}", env!("OUT_DIR"), "mitsuha-storage-local-test")
}

fn setup_root_dir() {
    let root_dir = get_root_dir();

    tracing::info!("test root dir set to '{}'", root_dir);

    _ = std::fs::remove_dir_all(root_dir.clone());
    _ = std::fs::create_dir(root_dir.clone());
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
                    (ConfKey::RootDir.to_string(), root_dir),
                    (ConfKey::EnableGC.to_string(), "true".to_string()),
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

async fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).await.unwrap()
}

#[tokio::test]
#[serial]
async fn store_and_load() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "store_and_load_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    let data = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await?;

    assert_eq!("Hello world!".to_string(), String::from_utf8(data).unwrap());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn store_and_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "store_and_expire_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 1,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let result = storage
        .exists(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert_eq!(result.unwrap(), false);
    Ok(())
}

#[tokio::test]
#[serial]
async fn store_persist_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "store_persist_expire_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 4,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    storage
        .persist(spec.handle.clone(), 4, spec.extensions.clone())
        .await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut result = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert!(result.is_ok());

    tokio::time::sleep(Duration::from_secs(7)).await;

    assert_eq!(
        storage
            .exists(spec.handle.clone(), spec.extensions.clone())
            .await
            .unwrap(),
        false
    );

    result = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn store_and_clear() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "store_and_clear_spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;
    storage
        .clear(spec.handle.clone(), spec.extensions.clone())
        .await?;

    let result = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert!(result.is_err());
    Ok(())
}

// MNFS tests

async fn reset_mnfs_async() {
    reset_mnfs();
}

fn reset_mnfs() {
    _ = std::fs::remove_dir_all(get_root_dir());
}

fn make_mnfs_config() -> config::storage::Storage {
    let s = make_basic_config();
    reset_mnfs();

    s
}

lazy_static! {
    static ref CONFIG: config::storage::Storage = make_mnfs_config();
    static ref EXTENSIONS: HashMap<String, String> = [(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })
        .unwrap()
    )]
    .into_iter()
    .collect();
}

mod fs;

mnfs_test_suite!(&CONFIG, &EXTENSIONS, reset_mnfs_async);
