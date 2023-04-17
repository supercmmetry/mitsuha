use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use mitsuha_core::{types, channel::{ComputeOutput, ComputeChannel, ComputeInput, ComputeHandle}, selector::Label};
use mitsuha_storage::Storage;
use rand::{Rng, distributions::Alphanumeric};
use tokio::task::JoinHandle;

pub struct SimpleComputeChannel {
    task_map: DashMap<String, JoinHandle<types::Result<ComputeOutput>>>,
    storage: Arc<Box<dyn Storage>>,
    storage_selector: Label,
    next: Option<Arc<Box<dyn ComputeChannel>>>,
    id: String,
}

#[async_trait]
impl ComputeChannel for SimpleComputeChannel {
    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())

        // let handle: String = rand::thread_rng()
        //     .sample_iter(&Alphanumeric)
        //     .take(16)
        //     .map(char::from)
        //     .collect();

        // Ok(format!("mitsuha/channel/simple/id/{}", handle))
    }

    async fn compute(&self, elem: ComputeInput) -> types::Result<ComputeHandle> {
        match elem {
            ComputeInput::Store { spec } => {
                let spec = spec.with_selector(&self.storage_selector);
                tokio::task::spawn(async move {
                    let 
                })
            },
            _ => {}
        }

        Ok(handle)
    }

    async fn connect(&mut self, next: Box<dyn ComputeChannel>)  {
        self.next = Some(Arc::new(next));
    }
}