use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::channel::{ChannelContext, ChannelManager, ChannelUtilityProvider};
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::{channel::ComputeChannel, errors::Error, types};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use tokio::sync::RwLock;
use tracing::Instrument;

pub mod delegator;
pub mod enforcer;
pub mod interceptor;
pub mod labeled_storage;
pub mod muxed_storage;
pub mod namespacer;
pub mod scheduler;
pub mod system;
mod util;
pub mod wasmtime;

type NextComputeChannel<Context> =
    Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>>>;

pub struct WrappedComputeChannel<T: ComputeChannel> {
    inner: T,
    id: Option<String>,
    next: NextComputeChannel<T::Context>,
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel,
{
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            id: None,
            next: Default::default(),
        }
    }

    pub fn new_with_id(inner: T, id: String) -> Self {
        tracing::info!("initialized channel with id: '{}'", id);
        Self {
            inner,
            id: Some(id),
            next: Default::default(),
        }
    }
}

#[async_trait]
impl<T> ComputeChannel for WrappedComputeChannel<T>
where
    T: ComputeChannel<Context = ChannelContext>,
{
    type Context = ChannelContext;

    fn id(&self) -> String {
        match self.id.clone() {
            Some(id) => id,
            _ => panic!("id was not assigned to channel"),
        }
    }

    async fn compute(
        &self,
        ctx: Self::Context,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        if ctx.should_skip_channel(&self.id(), &elem).await {
            tracing::debug!("skipping compute on channel: '{}'", self.id());

            return self
                .next
                .read()
                .await
                .as_ref()
                .unwrap()
                .compute(ctx, elem)
                .await;
        }

        tracing::debug!("performing compute on channel: '{}'", self.id());
        self.inner.compute(ctx, elem).await
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>) {
        tracing::info!("connecting channel '{}' to '{}'", self.id(), next.id());
        *self.next.write().await = Some(next.clone());
        self.inner.connect(next).await
    }
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel<Context = ChannelContext>,
{
    pub fn with_id(mut self, id: String) -> Self {
        tracing::info!("initialized channel with id: '{}'", id);
        self.id = Some(id);
        self
    }
}

#[derive(Clone)]
pub struct EntrypointChannel {
    next: NextComputeChannel<ChannelContext>,
    id: String,
}

#[async_trait]
impl ComputeChannel for EntrypointChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        mut ctx: Self::Context,
        mut elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        let next_channel = self.next.read().await.clone();

        match next_channel {
            Some(chan) => {
                let new_start: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
                    Arc::new(Box::new(self.clone()));
                new_start.connect(chan.clone()).await;

                ctx.set_channel_start(new_start);

                let compute_input_span = util::make_compute_input_span(&elem);

                ctx.truncate_privileged_fields(&mut elem).await;

                let result = chan.compute(ctx, elem).instrument(compute_input_span).await;

                match result {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        tracing::error!("failed to process compute input, error={}", e);

                        Err(e)
                    }
                }
            }
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl EntrypointChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/init"
    }

    pub fn new() -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new_with_id(
            Self {
                next: Arc::new(RwLock::new(None)),
                id: Self::get_identifier_type().to_string(),
            },
            format!(
                "{}/{}",
                Self::get_identifier_type().to_string(),
                util::generate_random_id()
            ),
        )
    }
}

pub struct EofChannel {
    next: NextComputeChannel<ChannelContext>,
    id: String,
}

#[async_trait]
impl ComputeChannel for EofChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        _ctx: ChannelContext,
        _elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        Err(Error::ComputeChannelEOF)
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl EofChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/eof"
    }

    pub fn new() -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new(Self {
            next: Arc::new(RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
