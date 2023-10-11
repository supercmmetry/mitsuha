use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use mitsuha_core_types::{channel::ComputeInput, kernel::JobSpec, module::ModuleInfo, symbol::Symbol};
use mitsuha_qflow::{Reader, Writer};
use tokio::sync::RwLock;

pub async fn test_rw_processing(
    writers: u64,
    writer_fn: fn(u64, u64) -> Pin<Box<dyn Future<Output = Arc<Box<dyn Writer>>>>>,
    readers: u64,
    reader_fn: fn(u64, u64) -> Pin<Box<dyn Future<Output = Arc<Box<dyn Reader>>>>>,
    messages_per_writer: u64,
    retries: i64,
    run_sticky_test: bool,
) {
    let mut writer_join_handles = vec![];
    let mut reader_join_handles = vec![];

    let writer_message_counter = Arc::new(RwLock::new(0u64));

    let mut jobs = vec![];

    for _ in 0..writers {
        jobs.push(JobSpec {
            handle: uuid::Uuid::new_v4().to_string(),
            symbol: Symbol {
                module_info: ModuleInfo {
                    name: "sample".to_string(),
                    version: "0.1.0".to_string(),
                    modtype: mitsuha_core_types::module::ModuleType::WASM,
                },
                name: "run".to_string(),
            },
            input_handle: uuid::Uuid::new_v4().to_string(),
            output_handle: uuid::Uuid::new_v4().to_string(),
            ttl: 86400,
            extensions: Default::default(),
        });
    }

    for writer_index in 0..writers {
        let writer_message_counter = writer_message_counter.clone();
        let writer = writer_fn(writer_index, writers).await;

        if run_sticky_test {
            for _ in 0..retries {
                if writer
                    .write_compute_input(ComputeInput::Run {
                        spec: jobs[writer_index as usize].clone(),
                    })
                    .await
                    .is_ok()
                {
                    break;
                }
            }

            for _ in 0..writer_index * 2 + 1 {
                _ = writer
                    .write_compute_input(ComputeInput::Status {
                        handle: jobs[writer_index as usize].handle.clone(),
                        extensions: Default::default(),
                    })
                    .await;
                _ = writer
                    .write_compute_input(ComputeInput::Extend {
                        handle: jobs[writer_index as usize].handle.clone(),
                        ttl: 1,
                        extensions: Default::default(),
                    })
                    .await;
                _ = writer
                    .write_compute_input(ComputeInput::Abort {
                        handle: jobs[writer_index as usize].handle.clone(),
                        extensions: Default::default(),
                    })
                    .await;
            }
        }

        let join_handle = tokio::task::spawn(async move {
            let mut counter = 0u64;
            let mut retry_counter = messages_per_writer as i64 * retries;

            while counter < messages_per_writer && retry_counter >= 0 {
                match writer
                    .write_compute_input(ComputeInput::Clear {
                        handle: format!("some-handle-{}", counter),
                        extensions: Default::default(),
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
                    Err(_e) => {
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
    let sticky_map = Arc::new(RwLock::new(HashMap::new()));

    for reader_index in 0..readers {
        let reader_message_counter = reader_message_counter.clone();
        let max_messages = total_message_count / readers;
        let reader = reader_fn(reader_index, readers).await;
        let sticky_map = sticky_map.clone();

        let join_handle = tokio::task::spawn(async move {
            let mut local_message_counter = 0u64;
            let mut retry_counter = max_messages as i64 * retries;

            let client_id = format!("client-{}", reader_index);

            if run_sticky_test {
                sticky_map
                    .write()
                    .await
                    .insert(client_id.clone(), HashSet::new());
            }

            while local_message_counter < max_messages && retry_counter >= 0 {
                match reader.read_compute_input(client_id.clone()).await {
                    Ok(input) => {
                        local_message_counter += 1;

                        println!(
                            "consumed {} messages!",
                            *reader_message_counter.read().await
                        );
                        *reader_message_counter.write().await += 1;

                        if run_sticky_test {
                            match input {
                                ComputeInput::Run { spec } => {
                                    sticky_map
                                        .write()
                                        .await
                                        .get_mut(&client_id)
                                        .unwrap()
                                        .insert(spec.handle);
                                }
                                ComputeInput::Abort { handle, .. }
                                | ComputeInput::Extend { handle, .. }
                                | ComputeInput::Status { handle, .. } => {
                                    assert!(sticky_map
                                        .read()
                                        .await
                                        .get(&client_id)
                                        .unwrap()
                                        .contains(&handle));
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(_e) => {
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
