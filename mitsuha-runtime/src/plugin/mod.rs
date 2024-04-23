use std::{collections::HashMap, sync::Arc};

use crate::plugin::muxed_storage::MuxedStoragePlugin;
use crate::plugin::scheduler::SchedulerPlugin;
use async_trait::async_trait;
use mitsuha_channel::{EntrypointChannel, WrappedComputeChannel};
use mitsuha_core::channel::{ChannelContext, ChannelManager};
use mitsuha_core::config::plugin::PluginConfiguration;
use mitsuha_core::errors::ToUnknownErrorResult;
use mitsuha_core::job::mgr::JobManager;
use mitsuha_core::{
    channel::ComputeChannel, config::Config, constants::Constants, err_unsupported_op,
    errors::Error, types,
};

use self::{
    common::{EofPlugin, SystemPlugin},
    delegator::DelegatorPlugin,
    enforcer::EnforcerPlugin,
    interceptor::InterceptorPlugin,
    namespacer::NamespacerPlugin,
    wasmtime::WasmtimePlugin,
};

pub mod common;
pub mod delegator;
pub mod enforcer;
pub mod interceptor;
pub mod muxed_storage;
pub mod namespacer;
mod scheduler;
pub mod wasmtime;

#[derive(Clone)]
pub struct PluginContext {
    pub channel_start: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
    pub channel_end: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>,
    pub global_configuration: Config,
    pub plugin_configuration: PluginConfiguration,
    pub plugin_name: String,
}

impl PluginContext {
    pub async fn new(config: Config, properties: HashMap<String, String>) -> types::Result<Self> {
        let init_channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
            Arc::new(Box::new(EntrypointChannel::new()));

        let mut channel_context = ChannelContext::default();

        let job_cost_evaluator = (&config).try_into()?;

        let job_manager = JobManager::new(
            init_channel.clone(),
            Arc::new(Box::new(channel_context.clone())),
            config.job.maximum_concurrent_cost.clone(),
            job_cost_evaluator,
            config.instance.id.clone(),
        )?;

        let channel_manager = ChannelManager::global_rw();

        channel_manager.write().await.channel_start = Some(init_channel.clone());
        channel_manager.write().await.job_manager = Some(job_manager);

        Ok(Self {
            channel_start: init_channel.clone(),
            channel_end: init_channel.clone(),
            global_configuration: config,
            plugin_configuration: Default::default(),
            plugin_name: String::new(),
        })
    }

    fn merge(&mut self, value: PluginContext) {
        self.channel_start = value.channel_start;
        self.channel_end = value.channel_end;
    }
}

#[async_trait]
pub trait Plugin: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn run(&self, ctx: PluginContext) -> types::Result<PluginContext>;
}

pub async fn load_plugins(mut ctx: PluginContext) -> types::Result<PluginContext> {
    let plugin_list: Vec<Box<dyn Plugin>> = vec![
        Box::new(EofPlugin),
        Box::new(SystemPlugin),
        Box::new(WasmtimePlugin),
        Box::new(DelegatorPlugin),
        Box::new(NamespacerPlugin),
        Box::new(InterceptorPlugin),
        Box::new(EnforcerPlugin),
        Box::new(MuxedStoragePlugin),
        Box::new(SchedulerPlugin),
    ];

    let plugin_map: HashMap<&'static str, Box<dyn Plugin>> = plugin_list
        .into_iter()
        .map(|x| (x.kind(), x))
        .into_iter()
        .collect();

    let plugins = ctx.global_configuration.plugins.clone();

    for mut plugin in plugins {
        let plugin_obj = plugin_map
            .get(plugin.kind.as_str())
            .ok_or(err_unsupported_op!("could not find plugin"))?;

        plugin = plugin.with_configuration().to_unknown_err_result()?;

        ctx.plugin_configuration = plugin.config;
        ctx.plugin_name = plugin.name;

        let new_ctx = plugin_obj.run(ctx.clone()).await?;

        ctx.merge(new_ctx);
    }

    Ok(ctx)
}

pub async fn initialize_channel<T>(
    ctx: &PluginContext,
    mut chan: WrappedComputeChannel<T>,
) -> types::Result<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>
where
    T: 'static + ComputeChannel<Context = ChannelContext>,
{
    chan = chan.with_id(ctx.plugin_name.clone());

    let boxed_chan: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> =
        Arc::new(Box::new(chan));

    ChannelManager::global_rw()
        .read()
        .await
        .channel_map
        .insert(boxed_chan.id(), boxed_chan.clone());

    Ok(boxed_chan)
}
