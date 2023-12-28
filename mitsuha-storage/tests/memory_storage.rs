#![feature(async_iterator)]

use serial_test::serial;
use std::{collections::HashMap, sync::Arc, time::Duration};

use lazy_static::lazy_static;
use mitsuha_core::{
    config,
    constants::StorageControlConstants,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_core_types::kernel::StorageSpec;
use mitsuha_storage::{conf::ConfKey, unified::UnifiedStorage};

mod fs;

fn make_basic_config() -> config::storage::Storage {
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
                properties: [(ConfKey::EnableGC.to_string(), "true".to_string())]
                    .iter()
                    .cloned()
                    .collect(),
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

async fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).await.unwrap()
}

#[tokio::test]
#[serial]
async fn store_and_load() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            key: "storage".to_string(),
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

#[tokio::test]
#[serial]
async fn store_and_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 1,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            key: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let result = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
#[serial]
async fn store_persist_expire() -> anyhow::Result<()> {
    let storage = make_unified_storage().await;

    let mut spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 2,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            key: "storage".to_string(),
            value: "sample".to_string(),
        })?,
    );

    storage.store(spec.clone()).await?;

    storage
        .persist(spec.handle.clone(), 2, spec.extensions.clone())
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut result = storage
        .load(spec.handle.clone(), spec.extensions.clone())
        .await;

    assert!(result.is_ok());

    tokio::time::sleep(Duration::from_secs(3)).await;

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
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            key: "storage".to_string(),
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

lazy_static! {
    static ref CONFIG: config::storage::Storage = make_basic_config();
    static ref EXTENSIONS: HashMap<String, String> = [(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            key: "storage".to_string(),
            value: "sample".to_string(),
        })
        .unwrap()
    )]
    .into_iter()
    .collect();
}

mnfs_test_suite!(&CONFIG, &EXTENSIONS);
