use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_channel::wasmtime::WasmtimeChannel;
use mitsuha_core::{channel::ComputeKernel, kernel::Kernel, types};

use super::{initialize_channel, Plugin, PluginContext};

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

        let raw_channel = WasmtimeChannel::new(kernel);
        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
