use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_channel::{context::ChannelContext, wasmtime::WasmtimeChannel};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeKernel},
    kernel::Kernel,
    module::ModuleInfo,
    resolver::blob::BlobResolver,
    resolver::Resolver,
    types,
};

use super::{Plugin, PluginContext};

#[derive(Clone)]
pub struct WasmtimePlugin;

#[async_trait]
impl Plugin for WasmtimePlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.wasmtime"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let kernel: Arc<Box<dyn Kernel>> =
            Arc::new(Box::new(ComputeKernel::new(ctx.channel_start.clone())));
        let blob_resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>> =
            Arc::new(Box::new(BlobResolver::new(ctx.channel_start.clone())));
        let channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(WasmtimeChannel::new(kernel, blob_resolver)));

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
