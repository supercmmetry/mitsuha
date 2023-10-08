use std::sync::Arc;

use mitsuha_core::{
    channel::{ComputeChannel, ComputeInputExt},
    errors::Error,
    types,
};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_policy_engine::{PolicyEngine, engine::StandardPolicyEngine, Policy};

use crate::{context::ChannelContext, WrappedComputeChannel};

use async_trait::async_trait;

pub struct EnforcerChannel {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    id: String,
    policy_engine: Arc<Box<dyn PolicyEngine>>,
    policy_blob_key: String,
}

#[async_trait]
impl ComputeChannel for EnforcerChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        if elem.get_extensions().is_none() || elem.get_extensions().is_some_and(|x| !x.contains_key(&self.policy_blob_key)) {
            tracing::warn!("could not find policy blob key: '{}', bypassing enforcer", self.policy_blob_key);
            
            return self.forward_next(ctx, elem).await;
        }

        let policy_blob_handle = elem.get_extensions().unwrap().get(&self.policy_blob_key).unwrap();
        let policies: Vec<Policy>;

        let policy_blob_input = ComputeInput::Load { handle: policy_blob_handle.clone(), extensions: elem.get_extensions().unwrap().clone() };

        let policy_blob_output = self.forward_next(ctx.clone(), policy_blob_input).await?;

        if let ComputeOutput::Loaded { data } = policy_blob_output {
            let value = musubi_api::types::Value::try_from(data).map_err(|e| Error::Unknown { source: e })?;

            policies = musubi_api::types::from_value(value).map_err(|e| Error::Unknown { source: e.into() })?;
        } else {
            return Err(Error::UnknownWithMsgOnly { message: format!("expected to find data in policy blob compute output") });
        }

        let policy_eval = self.policy_engine.evaluate(&elem, &policies).await?;

        if !policy_eval {
            return Err(Error::InvalidOperation { message: format!("policies defined in blob '{}' forbids the compute operation", policy_blob_handle) });
        }

        tracing::info!("policies defined in blob '{}' allows the compute operation", policy_blob_handle);

        self.forward_next(ctx, elem).await
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl EnforcerChannel {
    async fn forward_next(&self, ctx: ChannelContext, elem: ComputeInput) -> types::Result<ComputeOutput> {
        match self.next.read().await.clone() {
            Some(chan) => chan.compute(ctx, elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/enforcer"
    }

    pub fn new(policy_blob_key: String) -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
            policy_engine: Arc::new(Box::new(StandardPolicyEngine)),
            policy_blob_key,
        })
    }
}