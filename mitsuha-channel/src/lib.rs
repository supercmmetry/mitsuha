use std::sync::Arc;

use async_trait::async_trait;
use context::ChannelContext;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    types,
};

pub mod context;
pub mod delegator;
mod job_controller;
pub mod labeled_storage;
pub mod system;
mod util;
pub mod wasmtime;

pub struct WrappedComputeChannel<T: ComputeChannel> {
    inner: T,
    id: Option<String>,
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel,
{
    pub fn new(inner: T) -> Self {
        log::info!("initialized channel '{}'", inner.id());
        Self { inner, id: None }
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
            Some(id) => format!("{}/{}", self.inner.id(), id),
            _ => panic!("id was not assigned to channel"),
        }
    }

    async fn compute(
        &self,
        ctx: Self::Context,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        self.inner.compute(ctx, elem).await
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = Self::Context>>>) {
        log::info!("connecting channel '{}' to '{}'", self.id(), next.id());
        self.inner.connect(next).await
    }
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel<Context = ChannelContext>,
{
    pub fn with_id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
}

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
        match self.next.read().await.clone() {
            Some(chan) => {
                ctx.set_channel_start(chan.clone());
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
        WrappedComputeChannel::new(Self {
            next: Arc::new(tokio::sync::RwLock::new(None)),
            id: Self::get_identifier_type().to_string(),
        })
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