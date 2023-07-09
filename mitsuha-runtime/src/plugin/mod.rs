use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lazy_static::lazy_static;
use mitsuha_channel::context::ChannelContext;
use mitsuha_core::{channel::ComputeChannel, config::Config, types};

use self::{init::InitPlugin, one_storage::OneStoragePlugin, wasmtime::WasmtimePlugin};

pub mod init;
pub mod one_storage;
pub mod wasmtime;

#[derive(Clone)]
pub struct PluginContext {
    pub channel_start: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
    pub channel_end: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
    pub channel_context: ChannelContext,
    pub config: Config,
    pub extensions: HashMap<String, String>,
}

impl PluginContext {
    fn merge(&mut self, value: PluginContext) {
        self.channel_context = value.channel_context;
        self.channel_start = value.channel_start;
        self.channel_end = value.channel_end;
    }
}

#[async_trait]
pub trait Plugin: Send + Sync {
    fn name(&self) -> &'static str;

    async fn run(&self, ctx: PluginContext) -> types::Result<PluginContext>;
}

pub async fn load_plugins(mut ctx: PluginContext) -> PluginContext {
    let plugin_list: Vec<Box<dyn Plugin>> =
        vec![Box::new(OneStoragePlugin), Box::new(WasmtimePlugin)];

    let plugin_map: HashMap<&'static str, Box<dyn Plugin>> = plugin_list
        .into_iter()
        .map(|x| (x.name(), x))
        .into_iter()
        .collect();

    let plugin_configs = ctx.config.plugins.clone();

    for plugin_config in plugin_configs {
        let plugin = plugin_map.get(plugin_config.name.as_str()).unwrap();

        ctx.extensions = plugin_config.extensions;

        let new_ctx = plugin.run(ctx.clone()).await.unwrap();

        ctx.merge(new_ctx);
    }

    // Run the init plugin at the end to ensure that the InitChannel is always in the beginning
    let init_plugin = InitPlugin;
    let new_ctx = init_plugin.run(ctx.clone()).await.unwrap();

    ctx.merge(new_ctx);

    ctx
}
