use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::ComputeChannel,
    errors::Error,
    types,
};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use tokio::sync::RwLock;

use crate::WrappedComputeChannel;

pub struct QFlowWriterChannel<Context: Send> {
    writer: Arc<Box<dyn mitsuha_qflow::Writer>>,
    next: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>,
    id: String,
}

#[async_trait]
impl<Context> ComputeChannel for QFlowWriterChannel<Context>
where
    Context: Send,
{
    type Context = Context;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(&self, _ctx: Context, elem: ComputeInput) -> types::Result<ComputeOutput> {
        self.writer
            .write_compute_input(elem)
            .await
            .map_err(|e| Error::Unknown { source: e })?;

        Ok(ComputeOutput::Submitted)
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Context>>>) {
        *self.next.write().await = Some(next);
    }
}

impl<Context> QFlowWriterChannel<Context>
where
    Context: Send,
{
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/qflow_writer"
    }

    pub fn new(writer: Arc<Box<dyn mitsuha_qflow::Writer>>) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            writer,
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
