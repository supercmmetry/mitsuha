use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;

use crate::{
    tikv::{muxer::TikvQueueMuxer, writer::TikvWriter},
    ComputeInputGate, Reader, Writer,
};

use self::reader::TikvReader;

mod constants;
mod muxer;
mod reader;
mod writer;

pub async fn make_tikv_writer(
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Writer>>> {
    let desired_queue_count = extensions
        .get("desired_queue_count")
        .ok_or(anyhow!("cannot find desired_queue_count"))?
        .parse::<u64>()?;
    let pd_endpoints = extensions
        .get("pd_endpoints")
        .ok_or(anyhow!("cannot find pd_endpoints"))?;

    let client = tikv_client::TransactionClient::new(pd_endpoints.split(",").collect()).await?;

    let client = Arc::new(client);

    let muxer = Arc::new(TikvQueueMuxer::new(client.clone(), desired_queue_count).await?);

    let writer = TikvWriter::new(client, muxer).await?;

    Ok(Arc::new(Box::new(writer)))
}

pub async fn make_tikv_reader(
    gate: Arc<Box<dyn ComputeInputGate>>,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Reader>>> {
    let desired_queue_count = extensions
        .get("desired_queue_count")
        .ok_or(anyhow!("cannot find desired_queue_count"))?
        .parse::<u64>()?;
    let pd_endpoints = extensions
        .get("pd_endpoints")
        .ok_or(anyhow!("cannot find pd_endpoints"))?;

    let client = tikv_client::TransactionClient::new(pd_endpoints.split(",").collect()).await?;

    let client = Arc::new(client);

    let muxer = Arc::new(TikvQueueMuxer::new(client.clone(), desired_queue_count).await?);

    let reader = TikvReader::new(client, gate, muxer).await?;

    Ok(Arc::new(Box::new(reader)))
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use mitsuha_core_types::channel::ComputeInput;

    use crate::{
        tikv::{muxer::TikvQueueMuxer, reader::TikvReader, writer::TikvWriter},
        Reader, System, Writer,
    };

    #[tokio::test]
    async fn test_scale_down() {
        let client = tikv_client::TransactionClient::new(
            "127.0.0.1:2379,127.0.0.1:2382,127.0.0.1:2384"
                .split(",")
                .collect(),
        )
        .await
        .unwrap();

        let client = Arc::new(client);

        let writer_muxer = Arc::new(TikvQueueMuxer::new(client.clone(), 1000).await.unwrap());
        let reader_muxer = Arc::new(TikvQueueMuxer::new(client.clone(), 1000).await.unwrap());

        let writer = Arc::new(Box::new(
            TikvWriter::new(client.clone(), writer_muxer.clone())
                .await
                .unwrap(),
        ));
        let reader = Arc::new(Box::new(
            TikvReader::new(client, reader_muxer.clone()).await.unwrap(),
        ));

        let cloned_writer = writer.clone();
        let cloned_reader = reader.clone();

        let writer_handle = tokio::task::spawn(async move {
            for counter in 0..10000 {
                let mut retries = 1000;

                while retries > 0 {
                    match writer
                        .write_compute_input(ComputeInput::Clear {
                            handle: format!("some-handle-{}", counter),
                            extensions: Default::default(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("produced {} messages!", counter);
                            break;
                        }
                        Err(_e) => {
                            retries -= 1;
                        }
                    }
                }
            }
        });

        let reader_handle = tokio::task::spawn(async move {
            for counter in 0..10000 {
                let mut retries = 1000;

                while retries > 0 {
                    match reader.read_compute_input("client-id-1".to_string()).await {
                        Ok(_) => {
                            println!("consumed {} messages!", counter);
                            break;
                        }
                        Err(_e) => {
                            retries -= 1;
                        }
                    }
                }
            }
        });

        let scaler = tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(4)).await;

            let patch: HashMap<String, String> =
                [("desired_queue_count".to_string(), "100".to_string())]
                    .into_iter()
                    .collect();

            cloned_reader
                .update_configuration(patch.clone())
                .await
                .unwrap();
            cloned_writer
                .update_configuration(patch.clone())
                .await
                .unwrap();

            let mut retries = 600;

            loop {
                let writer_dc = writer_muxer.get_desired_queue_count().await;
                let reader_obc = reader_muxer.get_observed_queue_count().await;

                dbg!("writer_muxer.desired_queue_count = {}", writer_dc);
                dbg!("reader_muxer.observed_queue_count = {}", reader_obc);

                if writer_dc == 100 && reader_obc == 100 {
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;

                retries -= 1;

                if retries == 0 {
                    panic!("maximum retries reached for scaling verification");
                }
            }

            assert_eq!(writer_muxer.get_desired_queue_count().await, 100);
            assert_eq!(reader_muxer.get_observed_queue_count().await, 100);
        });

        writer_handle.await.unwrap();
        reader_handle.await.unwrap();
        scaler.await.unwrap();
    }

    #[tokio::test]
    async fn test_scale_up() {
        let client = tikv_client::TransactionClient::new(
            "127.0.0.1:2379,127.0.0.1:2382,127.0.0.1:2384"
                .split(",")
                .collect(),
        )
        .await
        .unwrap();

        let client = Arc::new(client);

        let writer_muxer = Arc::new(TikvQueueMuxer::new(client.clone(), 100).await.unwrap());
        let reader_muxer = Arc::new(TikvQueueMuxer::new(client.clone(), 100).await.unwrap());

        let writer = Arc::new(Box::new(
            TikvWriter::new(client.clone(), writer_muxer.clone())
                .await
                .unwrap(),
        ));
        let reader = Arc::new(Box::new(
            TikvReader::new(client, reader_muxer.clone()).await.unwrap(),
        ));

        let cloned_writer = writer.clone();
        let cloned_reader = reader.clone();

        let writer_handle = tokio::task::spawn(async move {
            for counter in 0..10000 {
                let mut retries = 1000;

                while retries > 0 {
                    match writer
                        .write_compute_input(ComputeInput::Clear {
                            handle: format!("some-handle-{}", counter),
                            extensions: Default::default(),
                        })
                        .await
                    {
                        Ok(_) => {
                            println!("produced {} messages!", counter);
                            break;
                        }
                        Err(_e) => {
                            retries -= 1;
                        }
                    }
                }
            }
        });

        let reader_handle = tokio::task::spawn(async move {
            for counter in 0..10000 {
                let mut retries = 1000;

                while retries > 0 {
                    match reader.read_compute_input("client-id-1".to_string()).await {
                        Ok(_) => {
                            println!("consumed {} messages!", counter);
                            break;
                        }
                        Err(_e) => {
                            retries -= 1;
                        }
                    }
                }
            }
        });

        let scaler = tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(4)).await;

            let patch: HashMap<String, String> =
                [("desired_queue_count".to_string(), "1000".to_string())]
                    .into_iter()
                    .collect();

            cloned_reader
                .update_configuration(patch.clone())
                .await
                .unwrap();
            cloned_writer
                .update_configuration(patch.clone())
                .await
                .unwrap();

            let mut retries = 600;

            loop {
                let writer_dc = writer_muxer.get_desired_queue_count().await;
                let reader_obc = reader_muxer.get_observed_queue_count().await;

                dbg!("writer_muxer.desired_queue_count = {}", writer_dc);
                dbg!("reader_muxer.observed_queue_count = {}", reader_obc);

                if writer_dc == 1000 && reader_obc == 1000 {
                    break;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;

                retries -= 1;

                if retries == 0 {
                    panic!("maximum retries reached for scaling verification");
                }
            }

            assert_eq!(writer_muxer.get_desired_queue_count().await, 1000);
            assert_eq!(reader_muxer.get_observed_queue_count().await, 1000);
        });

        writer_handle.await.unwrap();
        reader_handle.await.unwrap();
        scaler.await.unwrap();
    }
}
