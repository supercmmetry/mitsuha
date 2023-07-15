use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use mitsuha_core::channel::ComputeInput;
use mitsuha_qflow::{consumer::Consumer, producer::Producer, qmux::QueueMuxer, util};
use tikv_client::Timestamp;
use tikv_client::TimestampExt;
use tokio::sync::RwLock;

#[tokio::test]
async fn test_single_queue_processing() {
    let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
        .await
        .unwrap();
    let client = Arc::new(client);

    let muxer = Arc::new(QueueMuxer::new(client.clone(), 1).await.unwrap());

    let mut producer = Producer::new(client.clone(), muxer.clone()).await.unwrap();
    let consumer = Consumer::new(client.clone(), muxer.clone()).await.unwrap();

    for i in 0..10 {
        producer
            .write_compute_input(ComputeInput::Clear {
                handle: format!("some-handle-{}", i),
            })
            .await
            .unwrap();
    }

    for i in 0..10 {
        consumer.read_compute_input(format!("client-{}", i)).await.unwrap();
    }
}

#[tokio::test]
async fn test_multi_queue_processing() {
    let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
        .await
        .unwrap();
    let client = Arc::new(client);

    let muxer = Arc::new(QueueMuxer::new(client.clone(), 5).await.unwrap());

    let mut producer = Producer::new(client.clone(), muxer.clone()).await.unwrap();
    let consumer = Consumer::new(client.clone(), muxer.clone()).await.unwrap();

    for i in 0..10 {
        producer
            .write_compute_input(ComputeInput::Clear {
                handle: format!("some-handle-{}", i),
            })
            .await
            .unwrap();
    }

    let mut count = 0u64;
    while count < 10 {
        match consumer.read_compute_input(format!("client-x")).await {
            Ok(_) => count += 1,
            Err(_) => {}
        }
    }
}

async fn test_muxed_processing(
    producers: u64,
    consumers: u64,
    messages_per_producer: u64,
    verify_consumption: bool,
) {
    let mut producer_join_handles = vec![];
    let mut consumer_join_handles = vec![];
    let desired_queue_count = producers * 2;

    let producer_message_counter = Arc::new(RwLock::new(0u64));

    for producer_index in 0..producers {
        let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
            .await
            .unwrap();
        let producer_client = Arc::new(client);
        let producer_muxer = Arc::new(
            QueueMuxer::new(producer_client.clone(), desired_queue_count)
                .await
                .unwrap(),
        );
        let producer_message_counter = producer_message_counter.clone();

        let join_handle = tokio::task::spawn(async move {
            let mut producer = Producer::new(producer_client, producer_muxer)
                .await
                .unwrap();

            let mut counter = 0u64;
            while counter < messages_per_producer {
                match producer
                    .write_compute_input(ComputeInput::Clear {
                        handle: format!("some-handle-{}", counter),
                    })
                    .await
                {
                    Ok(_) => {
                        counter += 1;
                        if verify_consumption {
                            println!(
                                "produced {} messages!",
                                *producer_message_counter.read().await
                            );
                            *producer_message_counter.write().await += 1;
                        }
                    }
                    Err(e) => {
                        // dbg!(e);
                    }
                }
            }
        });

        producer_join_handles.push(join_handle);
    }

    let total_message_count = producers * messages_per_producer;
    let consumer_message_counter = Arc::new(RwLock::new(0u64));

    for consumer_index in 0..consumers {
        let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
            .await
            .unwrap();
        let consumer_client = Arc::new(client);
        let consumer_muxer = Arc::new(
            QueueMuxer::new(consumer_client.clone(), desired_queue_count)
                .await
                .unwrap(),
        );
        let consumer_message_counter = consumer_message_counter.clone();
        let max_messages = total_message_count / consumers;

        let join_handle = tokio::task::spawn(async move {
            let consumer = Consumer::new(consumer_client, consumer_muxer)
                .await
                .unwrap();
            let mut local_message_counter = 0u64;

            while local_message_counter < max_messages {
                match consumer.read_compute_input(format!("client-{}", consumer_index)).await {
                    Ok(_) => {
                        local_message_counter += 1;

                        if verify_consumption {
                            println!(
                                "consumed {} messages!",
                                *consumer_message_counter.read().await
                            );
                            *consumer_message_counter.write().await += 1;
                        }
                    }
                    Err(e) => {
                        // dbg!(e);
                    }
                }
            }
        });

        consumer_join_handles.push(join_handle);
    }

    for join_handle in producer_join_handles {
        join_handle.await.unwrap();
    }

    for join_handle in consumer_join_handles {
        join_handle.await.unwrap();
    }

    // tokio::time::sleep(Duration::from_secs(30)).await;

    println!(
        "Total messages produced: {}, total messages consumed: {}",
        *producer_message_counter.read().await,
        *consumer_message_counter.read().await
    );
}

#[tokio::test]
async fn test_mpsc_processing() {
    test_muxed_processing(10, 1, 100, true).await;
}

#[tokio::test]
async fn test_spmc_processing() {
    test_muxed_processing(1, 10, 100, true).await;
}

#[tokio::test]
async fn test_mpmc_processing() {
    test_muxed_processing(100, 100, 100, true).await;
}
