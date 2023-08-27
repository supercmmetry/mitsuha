use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    selector::Label,
    storage::Storage,
    types,
};
use tokio::sync::RwLock;

use crate::WrappedComputeChannel;

pub struct LabeledStorageChannel<Context: Send> {
    storage: Arc<Box<dyn Storage>>,
    storage_selector: Label,
    next: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>,
    id: String,
}

#[async_trait]
impl<Context> ComputeChannel for LabeledStorageChannel<Context>
where
    Context: Send,
{
    type Context = Context;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(&self, ctx: Context, elem: ComputeInput) -> types::Result<ComputeOutput> {
        let storage = self.storage.clone();

        match elem {
            ComputeInput::Store { spec } => {
                let spec = spec.with_selector(&self.storage_selector);
                let result = storage.store(spec).await;

                match result {
                    Ok(_) => Ok(ComputeOutput::Completed),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Load { handle, .. } => {
                let result = storage.load(handle).await;

                match result {
                    Ok(data) => Ok(ComputeOutput::Loaded { data }),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Persist { handle, ttl, .. } => {
                let result = storage.persist(handle, ttl).await;

                match result {
                    Ok(_) => Ok(ComputeOutput::Completed),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Clear { handle, .. } => {
                let result = storage.clear(handle).await;

                match result {
                    Ok(_) => Ok(ComputeOutput::Completed),
                    Err(e) => Err(e),
                }
            }
            _ => match self.next.read().await.clone() {
                Some(chan) => chan.compute(ctx, elem).await,
                None => Err(Error::ComputeChannelEOF),
            },
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Context>>>) {
        *self.next.write().await = Some(next);
    }
}

impl<Context> LabeledStorageChannel<Context>
where
    Context: Send,
{
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/labeled_storage"
    }

    pub fn new(storage: Arc<Box<dyn Storage>>, selector: Label) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            storage,
            storage_selector: selector,
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
