use async_trait::async_trait;
use mitsuha_channel::muxed_storage::MuxedStorageChannel;
use mitsuha_channel::muxed_storage::Rule;
use mitsuha_core::errors::ToUnknownErrorResult;
use mitsuha_core::selector::Label;
use mitsuha_core::{err_unknown, errors::Error, types};
use mitsuha_storage::UnifiedStorage;
use regex::Regex;
use serde::Deserialize;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MuxingRule {
    pub expression: String,
    pub label: Label,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MuxedStorageConfiguration {
    pub rules: Vec<MuxingRule>,
}

#[derive(Clone)]
pub struct MuxedStoragePlugin;

#[async_trait]
impl Plugin for MuxedStoragePlugin {
    fn kind(&self) -> &'static str {
        "mitsuha.plugin.muxed_storage"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let storage = UnifiedStorage::new(&ctx.global_configuration.storage).await?;

        let raw_channel = MuxedStorageChannel::new(storage, self.build_muxing_rules(&ctx)?);

        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}

impl MuxedStoragePlugin {
    fn build_muxing_rules(&self, ctx: &PluginContext) -> types::Result<Vec<Rule>> {
        let cfg: MuxedStorageConfiguration = ctx.plugin_configuration.get_spec().to_unknown_err_result()?;

        let mut rules = Vec::new();

        for rule in cfg.rules {
            let regexp = Regex::new(&rule.expression).to_unknown_err_result()?;
 
            rules.push((regexp, rule.label));
        }

        Ok(rules)
    }
}
