use async_trait::async_trait;
use mitsuha_channel::labeled_storage::LabeledStorageChannel;
use mitsuha_core::types;
use mitsuha_storage::UnifiedStorage;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct OneStoragePlugin;

#[async_trait]
impl Plugin for OneStoragePlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.one_storage"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let storage = UnifiedStorage::new(&ctx.config.storage).await?;

        let raw_channel = LabeledStorageChannel::new(
            storage,
            serde_json::from_str(ctx.current_properties.get("selector").unwrap()).unwrap(),
        );

        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}
