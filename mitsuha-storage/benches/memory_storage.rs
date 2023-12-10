use std::{collections::HashMap, sync::Arc};

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use mitsuha_core::{
    config,
    constants::StorageControlConstants,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_core_types::kernel::StorageSpec;
use mitsuha_storage::unified::UnifiedStorage;
use rand::distributions::Alphanumeric;
use rand::Rng;

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

async fn make_unified_storage() -> Arc<Box<dyn Storage>> {
    let config = make_basic_config();
    UnifiedStorage::new(&config).unwrap()
}

async fn store_and_load_light(storage: Arc<Box<dyn Storage>>) {
    let handle: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    let mut spec = StorageSpec {
        handle: handle,
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })
        .unwrap(),
    );

    storage.store(spec.clone()).await.unwrap();

    let data = storage
        .load(spec.handle.clone(), Default::default())
        .await
        .unwrap();

    assert_eq!("Hello world!".to_string(), String::from_utf8(data).unwrap());
}

async fn store_and_load_heavy(storage: Arc<Box<dyn Storage>>) {
    let handle: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    let mut spec = StorageSpec {
        handle: handle,
        data: vec![0; 1048576],
        ttl: 100,
        extensions: HashMap::new(),
    };

    spec.extensions.insert(
        StorageControlConstants::StorageSelectorQuery.to_string(),
        serde_json::to_string(&Label {
            name: "storage".to_string(),
            value: "sample".to_string(),
        })
        .unwrap(),
    );

    storage.store(spec.clone()).await.unwrap();
    storage
        .load(spec.handle.clone(), Default::default())
        .await
        .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    let storage = tokio_rt.block_on(make_unified_storage());

    c.bench_function("store_and_load_light", |b| {
        b.to_async(&tokio_rt)
            .iter(|| store_and_load_light(black_box(storage.clone())))
    });
    c.bench_function("store_and_load_heavy", |b| {
        b.to_async(&tokio_rt)
            .iter(|| store_and_load_heavy(black_box(storage.clone())))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
