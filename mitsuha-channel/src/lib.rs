use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    types,
};

pub mod context;
mod job_controller;
pub mod labeled_storage;
pub mod system;
mod util;
pub mod wasmtime;

pub struct WrappedComputeChannel<T: ComputeChannel> {
    inner: T,
}

impl<T> WrappedComputeChannel<T>
where
    T: ComputeChannel,
{
    pub fn new(inner: T) -> Self {
        log::info!("initialized channel '{}'", inner.id());
        Self { inner }
    }
}

#[async_trait]
impl<T> ComputeChannel for WrappedComputeChannel<T>
where
    T: ComputeChannel,
    <T as ComputeChannel>::Context: Send + Sync,
{
    type Context = <T as ComputeChannel>::Context;

    fn id(&self) -> String {
        self.inner.id()
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
