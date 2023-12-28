use async_trait::async_trait;
use mitsuha_channel::muxed_storage::MuxedStorageChannel;
use mitsuha_channel::muxed_storage::Rule;
use mitsuha_core::selector::Label;
use mitsuha_core::{err_unknown, errors::Error, types};
use mitsuha_storage::UnifiedStorage;
use regex::Regex;

use super::{initialize_channel, Plugin, PluginContext};

const RULE_PREFIX: &str = "rules.";
const RULE_EXPRESSION_SUFFIX: &str = ".expr";
const RULE_LABEL_KEY_SUFFIX: &str = ".label.key";
const RULE_LABEL_VALUE_SUFFIX: &str = ".label.value";

#[derive(Clone)]
pub struct MuxedStoragePlugin;

#[async_trait]
impl Plugin for MuxedStoragePlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.muxed_storage"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let storage = UnifiedStorage::new(&ctx.config.storage).await?;

        let raw_channel = MuxedStorageChannel::new(storage, self.build_muxing_rules(&ctx)?);

        let channel = initialize_channel(&ctx, raw_channel)?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        Ok(ctx)
    }
}

impl MuxedStoragePlugin {
    fn get_rule_expression_property(&self, index: u64) -> String {
        format!("{}{}{}", RULE_PREFIX, index, RULE_EXPRESSION_SUFFIX)
    }

    fn get_rule_label_key_property(&self, index: u64) -> String {
        format!("{}{}{}", RULE_PREFIX, index, RULE_LABEL_KEY_SUFFIX)
    }

    fn get_rule_label_value_property(&self, index: u64) -> String {
        format!("{}{}{}", RULE_PREFIX, index, RULE_LABEL_VALUE_SUFFIX)
    }

    fn build_muxing_rules(&self, ctx: &PluginContext) -> types::Result<Vec<Rule>> {
        let mut rules = Vec::new();
        let mut index = 0u64;

        loop {
            let expression_property = self.get_rule_expression_property(index);
            let label_key_property = self.get_rule_label_key_property(index);
            let label_value_property = self.get_rule_label_value_property(index);

            if !ctx.current_properties.contains_key(&expression_property) {
                break;
            }

            if !ctx.current_properties.contains_key(&label_key_property) {
                break;
            }

            if !ctx.current_properties.contains_key(&label_value_property) {
                break;
            }

            let regexp_str = ctx.current_properties.get(&expression_property).unwrap().clone();
            let label_key = ctx.current_properties.get(&label_key_property).unwrap().clone();
            let label_value = ctx.current_properties.get(&label_value_property).unwrap().clone();

            let regexp = Regex::new(&regexp_str).map_err(|e| err_unknown!(e))?;
            let label = Label {
                key: label_key,
                value: label_value,
            };

            rules.push((regexp, label));

            index += 1;
        }

        Ok(rules)
    }
}
