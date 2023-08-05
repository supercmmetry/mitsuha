use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mitsuha_channel::{context::ChannelContext, InitChannel, WrappedComputeChannel};
use mitsuha_core::{
    channel::ComputeChannel, config::Config, constants::Constants, errors::Error, types,
};

use self::{
    common::{EofPlugin, InitPlugin, SystemPlugin},
    delegator::DelegatorPlugin,
    one_storage::OneStoragePlugin,
    qflow::QFlowPlugin,
    wasmtime::WasmtimePlugin,
};

pub mod common;
pub mod delegator;
pub mod one_storage;
pub mod qflow;
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
    pub fn new(config: Config, extensions: HashMap<String, String>) -> Self {
        let init_channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(InitChannel::new()));

        let mut channel_context = ChannelContext::default();
        channel_context.channel_start = Some(init_channel.clone());

        Self {
            channel_start: init_channel.clone(),
            channel_end: init_channel.clone(),
            channel_context,
            config,
            extensions,
        }
    }

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
    let plugin_list: Vec<Box<dyn Plugin>> = vec![
        Box::new(EofPlugin),
        Box::new(SystemPlugin),
        Box::new(OneStoragePlugin),
        Box::new(WasmtimePlugin),
        Box::new(DelegatorPlugin),
        Box::new(QFlowPlugin),
    ];

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
    // let init_plugin = InitPlugin;
    // let new_ctx = init_plugin.run(ctx.clone()).await.unwrap();

    // ctx.merge(new_ctx);

    ctx
}

pub fn initialize_channel<T>(
    ctx: &PluginContext,
    mut chan: WrappedComputeChannel<T>,
) -> types::Result<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>
where
    T: 'static + ComputeChannel<Context = ChannelContext>,
{
    chan = match ctx.extensions.get(&Constants::ChannelId.to_string()) {
        Some(id) => chan.with_id(id.clone()),
        None => {
            return Err(Error::UnknownWithMsgOnly {
                message: format!("channel id was not assigned"),
            })
        }
    };

    let boxed_chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
        Arc::new(Box::new(chan));

    ctx.channel_context
        .channel_map
        .insert(boxed_chan.id(), boxed_chan.clone());

    Ok(boxed_chan)
}
