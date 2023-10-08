use std::sync::Arc;

use async_trait::async_trait;
use context::ChannelContext;
use mitsuha_core::{
    channel::ComputeChannel,
    errors::Error,
    types,
};
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};

pub mod context;
pub mod delegator;
pub mod interceptor;
mod job_controller;
pub mod labeled_storage;
pub mod namespacer;
pub mod qflow;
pub mod system;
mod util;
pub mod wasmtime;
pub mod enforcer;

pub struct WrappedComputeChannel<T: ComputeChannel> {
    inner: T,
    id: Option<String>,
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel,
{
    pub fn new(inner: T) -> Self {
        Self { inner, id: None }
    }

    pub fn new_with_id(inner: T, id: String) -> Self {
        tracing::info!("initialized channel with id: '{}'", id);
        Self {
            inner,
            id: Some(id),
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
        tracing::debug!("performing compute on channel: '{}'", self.id());
        self.inner.compute(ctx, elem).await
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>) {
        tracing::info!("connecting channel '{}' to '{}'", self.id(), next.id());
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
pub struct InitChannel {
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    id: String,
}

#[async_trait]
impl ComputeChannel for InitChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        mut ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        let next_channel = self.next.read().await.clone();

        match next_channel {
            Some(chan) => {
                let new_start: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
                    Arc::new(Box::new(self.clone()));
                new_start.connect(chan.clone()).await;

                ctx.set_channel_start(new_start);
                chan.compute(ctx, elem).await
            }
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl InitChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/init"
    }

    pub fn new() -> WrappedComputeChannel<Self> {
        WrappedComputeChannel::new_with_id(
            Self {
                next: Arc::new(tokio::sync::RwLock::new(None)),
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
    next: Arc<tokio::sync::RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
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
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
    }
}
