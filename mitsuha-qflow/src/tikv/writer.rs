use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mitsuha_core_types::channel::ComputeInput;

use crate::{util, System, Writer};

use super::{constants::Constants, muxer::TikvQueueMuxer};

pub struct TikvWriter {
    client: Arc<tikv_client::TransactionClient>,
    muxer: Arc<TikvQueueMuxer>,
}

impl TikvWriter {
    pub async fn new(
        client: Arc<tikv_client::TransactionClient>,
        muxer: Arc<TikvQueueMuxer>,
    ) -> anyhow::Result<Self> {
        Ok(Self { client, muxer })
    }

    async fn write_compute_input_tx(
        &self,
        tx: &mut tikv_client::Transaction,
        input: ComputeInput,
    ) -> anyhow::Result<()> {
        // If sticky, then redirect to sticky queue
        if self.process_sticky_element(tx, &input).await? {
            return Ok(());
        }

        let queue_index = self.muxer.get_producer_queue_index(tx).await?;
        let queue_length_handle = util::generate_queue_length_handle(queue_index);

        let length_data = tx.get_for_update(queue_length_handle.clone()).await?;
        let mut length = 0u64;

        if length_data.is_none() {
            tx.put(queue_length_handle.clone(), 0u64.to_le_bytes().to_vec())
                .await?;
        } else {
            length = util::vec_to_u64(length_data.unwrap())?;
        }

        let element_handle = util::generate_queue_element_handle(queue_index, length);
        let data: Vec<u8> = musubi_api::types::to_value(&input)?.try_into()?;

        tx.put(element_handle, data).await?;

        length += 1;

        tx.put(queue_length_handle, length.to_le_bytes()).await?;

        Ok(())
    }

    async fn process_sticky_element(
        &self,
        tx: &mut tikv_client::Transaction,
        input: &ComputeInput,
    ) -> anyhow::Result<bool> {
        match input {
            ComputeInput::Status { handle, .. }
            | ComputeInput::Abort { handle, .. }
            | ComputeInput::Extend { handle, .. } => {
                let input_trigger_handle =
                    util::generate_sticky_element_trigger_handle(handle.clone());
                let client_trigger_handle = tx.get(input_trigger_handle).await?;
                if client_trigger_handle.is_none() {
                    // TODO: Add warning that we are dropping the input operation here
                    return Ok(true);
                }

                let client_trigger_handle = String::from_utf8(client_trigger_handle.unwrap())?;
                let client_id = util::unwrap_sticky_element_trigger_handle(client_trigger_handle)?;

                let queue_length_handle =
                    util::generate_sticky_queue_length_handle(client_id.clone());

                let length_data = tx.get_for_update(queue_length_handle.clone()).await?;
                let mut length = 0u64;

                if length_data.is_none() {
                    tx.put(queue_length_handle.clone(), 0u64.to_le_bytes().to_vec())
                        .await?;
                } else {
                    length = util::vec_to_u64(length_data.unwrap())?;
                }

                let element_handle =
                    util::generate_sticky_element_handle(client_id.clone(), length);
                let data: Vec<u8> = musubi_api::types::to_value(input)?.try_into()?;

                tx.put(element_handle, data).await?;

                length += 1;

                tx.put(queue_length_handle, length.to_le_bytes()).await?;
            }
            _ => return Ok(false),
        }

        Ok(true)
    }
}

#[async_trait]
impl System for TikvWriter {
    async fn update_configuration(&self, patch: HashMap<String, String>) -> anyhow::Result<()> {
        for (key, value) in patch {
            match key {
                k if k == Constants::DesiredQueueCount.to_string() => {
                    let desired_queue_count: u64 = value.parse()?;

                    self.muxer
                        .update_desired_queue_count(desired_queue_count)
                        .await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Writer for TikvWriter {
    async fn write_compute_input(&self, input: ComputeInput) -> anyhow::Result<()> {
        let options = tikv_client::TransactionOptions::new_optimistic()
            .retry_options(tikv_client::RetryOptions::default_optimistic());

        let mut tx = self.client.begin_with_options(options).await?;

        match self.write_compute_input_tx(&mut tx, input).await {
            Ok(_) => {
                tx.commit().await?;
                Ok(())
            }
            Err(e) => {
                tx.rollback().await?;
                Err(e)
            }
        }
    }
}
