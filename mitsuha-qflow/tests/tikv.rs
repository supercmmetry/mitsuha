use std::{collections::HashMap, sync::Arc};

use mitsuha_qflow::{
    tikv::{make_tikv_reader, make_tikv_writer},
    Reader, Writer,
};

mod common;

async fn writer_fn(_index: u64, _len: u64) -> Arc<Box<dyn Writer>> {
    let extensions: HashMap<String, String> = [
        ("desired_queue_count".to_string(), "1000".to_string()),
        (
            "pd_endpoints".to_string(),
            "127.0.0.1:2379,127.0.0.1:2382,127.0.0.1:2384".to_string(),
        ),
    ]
    .into_iter()
    .collect();

    let writer = make_tikv_writer(&extensions).await.unwrap();

    writer
}

async fn reader_fn(_index: u64, _len: u64) -> Arc<Box<dyn Reader>> {
    let extensions: HashMap<String, String> = [
        ("desired_queue_count".to_string(), "1000".to_string()),
        (
            "pd_endpoints".to_string(),
            "127.0.0.1:2379,127.0.0.1:2382,127.0.0.1:2384".to_string(),
        ),
    ]
    .into_iter()
    .collect();

    let reader = make_tikv_reader(&extensions).await.unwrap();

    reader
}

#[tokio::test]
async fn test_spsc_processing() {
    common::test_rw_processing(
        1,
        |x, y| Box::pin(writer_fn(x, y)),
        1,
        |x, y| Box::pin(reader_fn(x, y)),
        1,
        10000,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_spmc_processing() {
    common::test_rw_processing(
        1,
        |x, y| Box::pin(writer_fn(x, y)),
        100,
        |x, y| Box::pin(reader_fn(x, y)),
        100,
        1000,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_mpsc_processing() {
    common::test_rw_processing(
        100,
        |x, y| Box::pin(writer_fn(x, y)),
        1,
        |x, y| Box::pin(reader_fn(x, y)),
        100,
        100,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_mpmc_processing() {
    common::test_rw_processing(
        100,
        |x, y| Box::pin(writer_fn(x, y)),
        100,
        |x, y| Box::pin(reader_fn(x, y)),
        100,
        100,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_sticky_mpmc_processing() {
    common::test_rw_processing(
        100,
        |x, y| Box::pin(writer_fn(x, y)),
        100,
        |x, y| Box::pin(reader_fn(x, y)),
        100,
        100,
        true,
    )
    .await;
}
