use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_channel::{
    context::ChannelContext, labeled_storage::LabeledStorageChannel, wasmtime::WasmtimeChannel,
};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeKernel},
    kernel::Kernel,
    module::ModuleInfo,
    resolver::blob::BlobResolver,
    resolver::Resolver,
    types,
};
use mitsuha_storage::UnifiedStorage;

use super::{Plugin, PluginContext};

#[derive(Clone)]
pub struct OneStoragePlugin;

#[async_trait]
impl Plugin for OneStoragePlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.one_storage"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let storage = UnifiedStorage::new(&ctx.config.storage).unwrap();

        let channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(LabeledStorageChannel::new(
                storage,
                serde_json::from_str(ctx.extensions.get("selector").unwrap()).unwrap(),
            )));

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
