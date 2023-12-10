use std::sync::Arc;

use mitsuha_core::{channel::ComputeChannel, errors::Error, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_runtime_rpc::proto::channel::{interceptor_client::InterceptorClient, ComputeRequest};

use crate::{context::ChannelContext, WrappedComputeChannel};

use async_trait::async_trait;

pub struct InterceptorChannel {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    id: String,
    client: InterceptorClient<tonic::transport::Channel>,
}

#[async_trait]
impl ComputeChannel for InterceptorChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        let compute_request: ComputeRequest =
            elem.try_into().map_err(|e| Error::Unknown { source: e })?;

        let compute_request = self
            .client
            .clone()
            .intercept(compute_request)
            .await
            .map_err(|e| Error::UnknownWithMsgOnly {
                message: e.to_string(),
            })?
            .into_inner();

        let compute_input: ComputeInput = compute_request
            .try_into()
            .map_err(|e| Error::Unknown { source: e })?;

        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, compute_input).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl InterceptorChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/interceptor"
    }

    pub fn new(
        client: InterceptorClient<tonic::transport::Channel>,
    ) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
            client,
        })
    }
}
