use std::sync::Arc;

use rand::seq::SliceRandom;
use tokio::sync::RwLock;

use crate::util;

#[derive(Clone)]
pub struct QueueMuxer {
    index_space: Arc<RwLock<Vec<u64>>>,
    index: Arc<RwLock<u64>>,
    desired_queue_count: u64,
    observed_queue_count: Arc<RwLock<u64>>,
    client: Arc<tikv_client::TransactionClient>,
}

impl QueueMuxer {
    pub async fn new(
        client: Arc<tikv_client::TransactionClient>,
        desired_queue_count: u64,
    ) -> anyhow::Result<Self> {
        let queue_count = Self::load_queue_count(client.clone(), desired_queue_count).await?;

        Ok(Self {
            index_space: Arc::new(RwLock::new(vec![])),
            index: Arc::new(RwLock::new(0u64)),
            desired_queue_count,
            observed_queue_count: Arc::new(RwLock::new(queue_count)),
            client,
        })
    }

    async fn load_queue_count(
        client: Arc<tikv_client::TransactionClient>,
        desired_queue_count: u64,
    ) -> anyhow::Result<u64> {
        let handle = util::generate_queue_count_handle();
        let mut queue_count = 0u64;

        let options = tikv_client::TransactionOptions::new_pessimistic()
            .retry_options(tikv_client::RetryOptions::default_pessimistic());

        let mut tx = client.begin_with_options(options).await?;

        let data = tx.get(handle.clone()).await.unwrap();

        if data.is_none() {
            tx.put(handle.clone(), queue_count.to_le_bytes()).await?;
        } else {
            queue_count = util::vec_to_u64(data.unwrap())?;
        }

        // If the observed queue count is less than the desired queue count, then
        // scale it up as soon as possible. For scale down scenarios, we need to
        // gracefully decommision a queue and wait for the queue to become empty.

        if queue_count < desired_queue_count {
            tx.put(handle, desired_queue_count.to_le_bytes()).await?;
            queue_count = desired_queue_count;
        }

        tx.commit().await?;

        Ok(queue_count)
    }

    async fn get_random_queue_index(&self, queue_count: u64) -> anyhow::Result<u64> {
        let mut index = *self.index.read().await as usize;

        if self.index_space.read().await.len() as u64 != queue_count
            || index >= queue_count as usize
        {
            let mut v = vec![0u64; queue_count as usize];

            for i in 0..queue_count {
                v[i as usize] = i;
            }

            v.shuffle(&mut rand::thread_rng());

            *self.index_space.write().await = v;
            *self.index.write().await = 0u64;
            index = 0usize;
        }

        *self.index.write().await = (index + 1) as u64;

        Ok(self.index_space.read().await[index])
    }

    pub async fn get_producer_queue_index(
        &self,
        tx: &mut tikv_client::Transaction,
    ) -> anyhow::Result<u64> {
        let queue_count = self.desired_queue_count;

        self.get_next_queue_index(tx, queue_count).await
    }

    pub async fn get_consumer_queue_index(
        &self,
        tx: &mut tikv_client::Transaction,
    ) -> anyhow::Result<u64> {
        let queue_count = *self.observed_queue_count.read().await;

        self.get_next_queue_index(tx, queue_count).await
    }

    async fn get_next_queue_index(
        &self,
        tx: &mut tikv_client::Transaction,
        queue_count: u64,
    ) -> anyhow::Result<u64> {
        let mut queue_index = 0u64;

        loop {
            queue_index = self.get_random_queue_index(queue_count).await?;

            let lock_handle = util::generate_queue_lock_handle(queue_index);

            let lock_data = tx.get(lock_handle.clone()).await?;

            if lock_data.is_some() {
                continue;
            }

            break;
        }

        Ok(queue_index)
    }

    async fn refresh_counter(&self) -> anyhow::Result<()> {
        let queue_count =
            Self::load_queue_count(self.client.clone(), self.desired_queue_count).await?;
        *self.observed_queue_count.write().await = queue_count;

        Ok(())
    }
}
