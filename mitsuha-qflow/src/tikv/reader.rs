use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use mitsuha_core::channel::ComputeInput;

use crate::{util, Reader};

use super::muxer::TikvQueueMuxer;

pub struct TikvReader {
    client: Arc<tikv_client::TransactionClient>,
    muxer: Arc<TikvQueueMuxer>,
}

impl TikvReader {
    pub async fn new(
        client: Arc<tikv_client::TransactionClient>,
        muxer: Arc<TikvQueueMuxer>,
    ) -> anyhow::Result<Self> {
        Ok(Self { client, muxer })
    }

    async fn read_compute_input_tx(
        &self,
        tx: &mut tikv_client::Transaction,
    ) -> anyhow::Result<ComputeInput> {
        let queue_index = self.muxer.get_consumer_queue_index(tx).await?;
        let queue_offset_handle = util::generate_queue_offset_handle(queue_index);

        let offset_data = tx.get_for_update(queue_offset_handle.clone()).await?;
        let mut offset = 0u64;

        if offset_data.is_none() {
            tx.put(queue_offset_handle.clone(), 0u64.to_le_bytes().to_vec())
                .await?;
        } else {
            offset = util::vec_to_u64(offset_data.unwrap())?;
        }

        let element_handle = util::generate_queue_element_handle(queue_index, offset);

        let data = tx.get(element_handle.clone()).await?;

        if data.is_none() {
            return Err(anyhow!("target queue was empty"));
        }

        tx.delete(element_handle).await?;

        offset += 1;

        tx.put(queue_offset_handle, offset.to_le_bytes()).await?;

        let value = musubi_api::types::Value::try_from(data.unwrap())?;

        Ok(musubi_api::types::from_value(value)?)
    }

    async fn process_sticky_element(
        &self,
        tx: &mut tikv_client::Transaction,
        client_id: String,
        input: &ComputeInput,
    ) -> anyhow::Result<()> {
        match input {
            ComputeInput::Run { spec } => {
                let handle = spec.handle.clone();

                let client_trigger_handle =
                    util::generate_sticky_element_trigger_handle(client_id.clone());
                let input_trigger_handle =
                    util::generate_sticky_element_trigger_handle(handle.clone());

                tx.put(client_trigger_handle, handle).await?;
                tx.put(input_trigger_handle, client_id).await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn read_sticky_element_tx(
        &self,
        tx: &mut tikv_client::Transaction,
        client_id: String,
    ) -> anyhow::Result<ComputeInput> {
        let queue_offset_handle = util::generate_sticky_queue_offset_handle(client_id.clone());

        let offset_data = tx.get_for_update(queue_offset_handle.clone()).await?;
        let mut offset = 0u64;

        if offset_data.is_none() {
            tx.put(queue_offset_handle.clone(), 0u64.to_le_bytes().to_vec())
                .await?;
        } else {
            offset = util::vec_to_u64(offset_data.unwrap())?;
        }

        let element_handle = util::generate_sticky_element_handle(client_id.clone(), offset);

        let data = tx.get(element_handle.clone()).await?;
        if data.is_none() {
            return Err(anyhow!("target sticky queue was empty"));
        }

        tx.delete(element_handle).await?;

        offset += 1;

        tx.put(queue_offset_handle, offset.to_le_bytes()).await?;
        let value = musubi_api::types::Value::try_from(data.unwrap())?;

        Ok(musubi_api::types::from_value(value)?)
    }
}

#[async_trait]
impl Reader for TikvReader {
    async fn read_compute_input(&self, client_id: String) -> anyhow::Result<ComputeInput> {
        let options = tikv_client::TransactionOptions::new_optimistic()
            .retry_options(tikv_client::RetryOptions::default_optimistic());

        let mut sticky_tx = self.client.begin_with_options(options.clone()).await?;

        match self
            .read_sticky_element_tx(&mut sticky_tx, client_id.clone())
            .await
        {
            Ok(input) => {
                sticky_tx.commit().await?;
                return Ok(input);
            }
            Err(_e) => {
                // log error
                sticky_tx.rollback().await?;
            }
        }

        let mut tx = self.client.begin_with_options(options.clone()).await?;

        match self.read_compute_input_tx(&mut tx).await {
            Ok(input) => {
                // log error
                _ = self
                    .process_sticky_element(&mut tx, client_id, &input)
                    .await;

                tx.commit().await?;
                Ok(input)
            }
            Err(e) => {
                tx.rollback().await?;
                Err(e)
            }
        }
    }
}
