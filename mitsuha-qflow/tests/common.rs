use std::{future::Future, pin::Pin, sync::Arc};

use mitsuha_core::channel::ComputeInput;
use mitsuha_qflow::{Reader, Writer};
use tokio::sync::RwLock;

pub async fn test_rw_processing(
    writers: u64,
    writer_fn: fn(u64, u64) -> Pin<Box<dyn Future<Output = Arc<Box<dyn Writer>>>>>,
    readers: u64,
    reader_fn: fn(u64, u64) -> Pin<Box<dyn Future<Output = Arc<Box<dyn Reader>>>>>,
    messages_per_writer: u64,
    retries: i64,
) {
    let mut writer_join_handles = vec![];
    let mut reader_join_handles = vec![];

    let writer_message_counter = Arc::new(RwLock::new(0u64));

    for writer_index in 0..writers {
        let writer_message_counter = writer_message_counter.clone();
        let writer = writer_fn(writer_index, writers).await;

        let join_handle = tokio::task::spawn(async move {
            let mut counter = 0u64;
            let mut retry_counter = messages_per_writer as i64 * retries;

            while counter < messages_per_writer && retry_counter >= 0 {
                match writer
                    .write_compute_input(ComputeInput::Clear {
                        handle: format!("some-handle-{}", counter),
                    })
                    .await
                {
                    Ok(_) => {
                        counter += 1;
                        println!(
                            "produced {} messages!",
                            *writer_message_counter.read().await
                        );
                        *writer_message_counter.write().await += 1;
                    }
                    Err(e) => {
                        retry_counter -= 1;
                        // dbg!(e);
                    }
                }
            }
        });

        writer_join_handles.push(join_handle);
    }

    let total_message_count = writers * messages_per_writer;
    let reader_message_counter = Arc::new(RwLock::new(0u64));

    for reader_index in 0..readers {
        let reader_message_counter = reader_message_counter.clone();
        let max_messages = total_message_count / readers;
        let reader = reader_fn(reader_index, readers).await;

        let join_handle = tokio::task::spawn(async move {
            let mut local_message_counter = 0u64;
            let mut retry_counter = max_messages as i64 * retries;

            while local_message_counter < max_messages && retry_counter >= 0 {
                match reader
                    .read_compute_input(format!("client-{}", reader_index))
                    .await
                {
                    Ok(_) => {
                        local_message_counter += 1;

                        println!(
                            "consumed {} messages!",
                            *reader_message_counter.read().await
                        );
                        *reader_message_counter.write().await += 1;
                    }
                    Err(e) => {
                        retry_counter -= 1;
                        // dbg!(e);
                    }
                }
            }
        });

        reader_join_handles.push(join_handle);
    }

    for join_handle in writer_join_handles {
        join_handle.await.unwrap();
    }

    for join_handle in reader_join_handles {
        join_handle.await.unwrap();
    }

    // tokio::time::sleep(Duration::from_secs(30)).await;

    println!(
        "Total messages produced: {}, total messages consumed: {}",
        *writer_message_counter.read().await,
        *reader_message_counter.read().await
    );

    assert_eq!(*writer_message_counter.read().await, total_message_count);
    assert_eq!(*reader_message_counter.read().await, total_message_count);
}
