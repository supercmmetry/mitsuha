use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInputExt},
    errors::Error,
    kernel::LabelExtensionExt,
    selector::Label,
    storage::Storage,
    types,
};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use regex::Regex;
use tokio::sync::RwLock;

use crate::WrappedComputeChannel;

pub type Rule = (Regex, Label);

pub struct MuxedStorageChannel<Context: Send> {
    storage: Arc<Box<dyn Storage>>,
    rules: Vec<Rule>,
    next: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>,
    id: String,
}

#[async_trait]
impl<Context> ComputeChannel for MuxedStorageChannel<Context>
where
    Context: Send,
{
    type Context = Context;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(&self, ctx: Context, mut elem: ComputeInput) -> types::Result<ComputeOutput> {
        let storage = self.storage.clone();

        if elem.is_storage_input() {
            match self.get_first_match(&elem.get_original_handle()) {
                Some(label) => {
                    elem.get_extensions_mut().add_selector(label);
                }
                None => {
                    return Err(Error::StorageOperationFailed {
                        message: "failed to mux storage request".to_string(),
                        source: anyhow!(""),
                    })
                }
            }
        }

        match elem {
            ComputeInput::Store { spec } => {
                let result = storage.store(spec).await;

                match result {
                    Ok(_) => Ok(ComputeOutput::Completed),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Load {
                handle,
                extensions,
            } => {
                let result = storage.load(handle, extensions).await;

                match result {
                    Ok(data) => Ok(ComputeOutput::Loaded { data }),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Persist {
                handle,
                ttl,
                extensions,
            } => {
                let result = storage.persist(handle, ttl, extensions).await;

                match result {
                    Ok(_) => Ok(ComputeOutput::Completed),
                    Err(e) => Err(e),
                }
            }
            ComputeInput::Clear {
                handle,
                extensions,
            } => {
                let result = storage.clear(handle, extensions).await;

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

impl<Context> MuxedStorageChannel<Context>
where
    Context: Send,
{
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/muxed_storage"
    }

    pub fn new(storage: Arc<Box<dyn Storage>>, rules: Vec<Rule>) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            storage,
            rules,
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }

    fn get_first_match(&self, handle: &String) -> Option<&Label> {
        for rule in self.rules.iter() {
            let (regexp, label) = rule;
            if regexp.is_match(&handle) {
                return Some(label);
            }
        }

        None
    }
}
