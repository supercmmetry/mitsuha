use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backoff::exponential::ExponentialBackoffBuilder;
use backoff::ExponentialBackoff;
use mitsuha_channel::qflow::QFlowWriterChannel;
use mitsuha_core::channel::ChannelContext;
use mitsuha_core::{config::Config, errors::Error, types};
use mitsuha_qflow::gate::StandardComputeInputGate;
use mitsuha_qflow::ComputeInputGate;
use tokio::runtime::Handle;

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct QFlowPlugin;

#[async_trait]
impl Plugin for QFlowPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.qflow"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let channel_id = ctx.current_properties.get("channel_id").unwrap().clone();

        let client_id = ctx.config.instance.id.clone();

        let writer = mitsuha_qflow::make_writer(&ctx.current_properties)
            .await
            .map_err(|e| Error::Unknown { source: e })?;

        let compute_gate: Arc<Box<dyn ComputeInputGate>> = Arc::new(Box::new(
            StandardComputeInputGate::new(ChannelContext::default()),
        ));
        let reader = mitsuha_qflow::make_reader(compute_gate, &ctx.current_properties)
            .await
            .map_err(|e| Error::Unknown { source: e })?;

        let retry_init_interval: u64 = ctx
            .current_properties
            .get("retry_initial_interval_millis")
            .cloned()
            .map(|x| x.parse().unwrap())
            .unwrap_or(1);

        let retry_max_interval: u64 = ctx
            .current_properties
            .get("retry_max_interval_millis")
            .cloned()
            .map(|x| x.parse().unwrap())
            .unwrap_or(3000);

        let retry_interval_randomization_factor: f64 = ctx
            .current_properties
            .get("retry_interval_randomization_factor")
            .cloned()
            .map(|x| x.parse().unwrap())
            .unwrap_or(0.5);

        let retry_interval_multiplier: f64 = ctx
            .current_properties
            .get("retry_interval_multiplier")
            .cloned()
            .map(|x| x.parse().unwrap())
            .unwrap_or(2.0);

        let retry_max_elapsed_millis: Option<Duration> = ctx
            .current_properties
            .get("retry_max_elapsed_millis")
            .cloned()
            .map(|x| Duration::from_millis(x.parse().unwrap()));

        let exponential_backoff: ExponentialBackoff = ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_millis(retry_init_interval))
            .with_max_interval(Duration::from_millis(retry_max_interval))
            .with_randomization_factor(retry_interval_randomization_factor)
            .with_multiplier(retry_interval_multiplier)
            .with_max_elapsed_time(retry_max_elapsed_millis)
            .build();

        let raw_channel = QFlowWriterChannel::new(writer.clone(), exponential_backoff);

        let channel = initialize_channel(&ctx, raw_channel).await?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        let channel_start = ctx.channel_start.clone();

        let cloned_reader = reader.clone();

        let reader_task = async move {
            loop {
                match reader.read_compute_input(client_id.clone()).await {
                    Ok(input) => {
                        tracing::info!("received qflow compute input");
                        // log error
                        _ = channel_start
                            .compute(ChannelContext::default(), input)
                            .await;
                    }
                    Err(e) => {
                        tracing::trace!("error occured during consumption: {}", e);
                    }
                }
            }
        };

        tokio::task::spawn_blocking(|| Handle::current().block_on(reader_task));

        let config_updater_task = async move {
            if let Ok(config) = Config::global().await {
                tracing::trace!("updating qflow configurations dynamically.");

                for plugin in config.plugins.iter() {
                    if plugin.properties.get("channel_id").unwrap().clone() == channel_id {
                        _ = writer.update_configuration(plugin.properties.clone()).await;
                        _ = cloned_reader
                            .update_configuration(plugin.properties.clone())
                            .await;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        };

        tokio::task::spawn_blocking(|| Handle::current().block_on(config_updater_task));

        Ok(ctx)
    }
}
