use std::{sync::Arc, time::Duration};

use rand::seq::SliceRandom;
use tokio::{sync::RwLock, task::JoinHandle};

use crate::util;

#[derive(Clone)]
pub struct TikvQueueMuxer {
    index_space: Arc<RwLock<Vec<u64>>>,
    index: Arc<RwLock<u64>>,
    desired_queue_count: Arc<RwLock<u64>>,
    observed_queue_count: Arc<RwLock<u64>>,
    client: Arc<tikv_client::TransactionClient>,
    reconciler_task: Arc<RwLock<Option<JoinHandle<anyhow::Result<()>>>>>,
}

impl TikvQueueMuxer {
    pub async fn new(
        client: Arc<tikv_client::TransactionClient>,
        desired_queue_count: u64,
    ) -> anyhow::Result<Self> {
        let queue_count = Self::reconcile_queue_count(client.clone(), desired_queue_count).await?;

        Ok(Self {
            index_space: Arc::new(RwLock::new(vec![])),
            index: Arc::new(RwLock::new(0u64)),
            desired_queue_count: Arc::new(RwLock::new(desired_queue_count)),
            observed_queue_count: Arc::new(RwLock::new(queue_count)),
            client,
            reconciler_task: Arc::new(RwLock::new(None)),
        })
    }

    async fn reconcile_queue_count(
        client: Arc<tikv_client::TransactionClient>,
        desired_queue_count: u64,
    ) -> anyhow::Result<u64> {
        let queue_count_handle = util::generate_queue_count_handle();
        let mut queue_count = 0u64;

        let options = tikv_client::TransactionOptions::new_optimistic()
            .retry_options(tikv_client::RetryOptions::default_optimistic());

        let mut tx = client.begin_with_options(options).await?;

        let data = tx.get(queue_count_handle.clone()).await.unwrap();

        if data.is_none() {
            tx.put(queue_count_handle.clone(), queue_count.to_le_bytes()).await?;
        } else {
            queue_count = util::vec_to_u64(data.unwrap())?;
        }

        // If the observed queue count is less than the desired queue count, then
        // scale it up as soon as possible. For scale down scenarios, we need to
        // gracefully decommision a queue and wait for the queue to become empty.

        if queue_count < desired_queue_count {
            tx.put(queue_count_handle.clone(), desired_queue_count.to_le_bytes()).await?;
            queue_count = desired_queue_count;
        }

        let mut queue_count_changed = false;

        // Scale down if queues are empty
        while queue_count > desired_queue_count {
            let queue_index = queue_count - 1;

            let offset_handle = util::generate_queue_offset_handle(queue_index);
            let length_handle = util::generate_queue_length_handle(queue_index);

            let offset_result = tx.get(offset_handle.clone()).await?.map(util::vec_to_u64);
            let length_result = tx.get(length_handle).await?.map(util::vec_to_u64);

            match (offset_result, length_result) {
                (Some(Ok(offset)), Some(Ok(length))) => {
                    tracing::debug!("scaling down queue {}", queue_index);
    
                    if offset == length {
                        queue_count -= 1;
                        queue_count_changed = true;
                    } else {
                        break;
                    }
                },
                _ => {
                    tracing::debug!("scaling down queue {}", queue_index);

                    queue_count -= 1;
                    queue_count_changed = true;
                },
            }
        }

        if queue_count_changed {
            tx.put(queue_count_handle, queue_count.to_le_bytes()).await?;
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
        let queue_count = *self.desired_queue_count.read().await;

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
        let mut queue_index;

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

    pub async fn update_desired_queue_count(&self, desired_queue_count: u64) -> anyhow::Result<()> {
        *self.desired_queue_count.write().await = desired_queue_count;

        let desired_queue_count = self.desired_queue_count.clone();
        let observed_queue_count = self.observed_queue_count.clone();
        let client = self.client.clone();

        // If a reconciler task was running, make sure that it is completed before we start a new one.
        if let Some(join_handle) = self.reconciler_task.write().await.as_mut() {
            join_handle.await??;
        }

        let join_handle = tokio::task::spawn(async move {
            while *observed_queue_count.read().await != *desired_queue_count.read().await {
                let queue_count =
                Self::reconcile_queue_count(client.clone(), *desired_queue_count.read().await).await;

                if let Ok(queue_count) = queue_count {
                    *observed_queue_count.write().await = queue_count;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            Ok::<(), anyhow::Error>(())
        });

        *self.reconciler_task.write().await = Some(join_handle);

        Ok(())
    }

    pub async fn get_desired_queue_count(&self) -> u64 {
        *self.desired_queue_count.read().await
    }

    pub async fn get_observed_queue_count(&self) -> u64 {
        *self.observed_queue_count.read().await
    }
}

#[cfg(test)]
mod tests {
    use crate::tikv::muxer::TikvQueueMuxer;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_qmux_single_queue_producer() {
        let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
            .await
            .unwrap();
        let client = Arc::new(client);

        let muxer = TikvQueueMuxer::new(client.clone(), 1).await.unwrap();

        for _i in 0..10 {
            let mut tx = client.begin_pessimistic().await.unwrap();

            assert_eq!(0, muxer.get_producer_queue_index(&mut tx).await.unwrap());

            tx.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_qmux_multi_queue_producer() {
        let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
            .await
            .unwrap();
        let client = Arc::new(client);

        let muxer = TikvQueueMuxer::new(client.clone(), 10).await.unwrap();
        let mut visited = vec![false; 10];

        for _ in 0..10 {
            let mut tx = client.begin_pessimistic().await.unwrap();

            let index = muxer.get_producer_queue_index(&mut tx).await.unwrap();
            visited[index as usize] = true;

            tx.commit().await.unwrap();
        }

        for v in visited {
            assert!(v);
        }
    }
}
