use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeHandle, ComputeInput, ComputeOutput},
    errors::Error,
    selector::Label,
    storage::Storage,
    types,
};
use rand::{distributions::Alphanumeric, Rng};

pub struct LabeledStorageChannel {
    storage: Arc<Box<dyn Storage>>,
    storage_selector: Label,
    next: Option<Arc<Box<dyn ComputeChannel>>>,
    id: String,
}

#[async_trait]
impl ComputeChannel for LabeledStorageChannel {
    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())
    }

    async fn compute(&self, elem: ComputeInput) -> types::Result<ComputeHandle> {
        let storage = self.storage.clone();

        match elem {
            ComputeInput::Store { spec } => {
                let spec = spec.with_selector(&self.storage_selector);
                let handle = tokio::task::spawn(async move {
                    let result = storage.store(spec).await;

                    match result {
                        Ok(_) => Ok(ComputeOutput::Completed),
                        Err(e) => Err(e),
                    }
                });

                Ok(handle)
            }
            ComputeInput::Load { handle } => {
                let handle = tokio::task::spawn(async move {
                    let result = storage.load(handle).await;

                    match result {
                        Ok(data) => Ok(ComputeOutput::Loaded { data }),
                        Err(e) => Err(e),
                    }
                });

                Ok(handle)
            }
            ComputeInput::Persist { handle, ttl } => {
                let handle = tokio::task::spawn(async move {
                    let result = storage.persist(handle, ttl).await;

                    match result {
                        Ok(_) => Ok(ComputeOutput::Completed),
                        Err(e) => Err(e),
                    }
                });

                Ok(handle)
            }
            ComputeInput::Clear { handle } => {
                let handle = tokio::task::spawn(async move {
                    let result = storage.clear(handle).await;

                    match result {
                        Ok(_) => Ok(ComputeOutput::Completed),
                        Err(e) => Err(e),
                    }
                });

                Ok(handle)
            }
            _ => match self.next.clone() {
                Some(chan) => chan.compute(elem).await,
                None => Err(Error::ComputeChannelEOF),
            },
        }
    }

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel>>) {
        self.next = Some(next);
    }
}

impl LabeledStorageChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/labeled_storage"
    }

    pub fn new(storage: Arc<Box<dyn Storage>>, selector: Label) -> Self {
        let id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        let full_id = format!("{}/{}", Self::get_identifier_type(), id);

        Self {
            storage,
            storage_selector: selector,
            next: None,
            id: full_id,
        }
    }
}
