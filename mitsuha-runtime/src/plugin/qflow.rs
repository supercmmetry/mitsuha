use std::time::Duration;

use async_trait::async_trait;
use mitsuha_channel::qflow::QFlowWriterChannel;
use mitsuha_core::{errors::Error, types};

use super::{initialize_channel, Plugin, PluginContext};

#[derive(Clone)]
pub struct QFlowPlugin;

#[async_trait]
impl Plugin for QFlowPlugin {
    fn name(&self) -> &'static str {
        "mitsuha.plugin.qflow"
    }

    async fn run(&self, mut ctx: PluginContext) -> types::Result<PluginContext> {
        let client_id = ctx
            .extensions
            .get("client_id")
            .ok_or(Error::UnknownWithMsgOnly {
                message: "failed to get client_id".to_string(),
            })?
            .clone();

        let writer = mitsuha_qflow::make_writer(&ctx.extensions)
            .await
            .map_err(|e| Error::Unknown { source: e })?;
        let reader = mitsuha_qflow::make_reader(&ctx.extensions)
            .await
            .map_err(|e| Error::Unknown { source: e })?;

        let raw_channel = QFlowWriterChannel::new(writer);

        let channel = initialize_channel(&ctx, raw_channel)?;

        ctx.channel_end.connect(channel.clone()).await;
        ctx.channel_end = channel;

        let channel_start = ctx.channel_start.clone();
        let channel_context = ctx.channel_context.clone();

        tokio::task::spawn(async move {
            loop {
                match reader.read_compute_input(client_id.clone()).await {
                    Ok(input) => {
                        tracing::debug!("received qflow compute input!");
                        // log error
                        _ = channel_start.compute(channel_context.clone(), input).await;

                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(_e) => {
                        // log::error!("error occured during consumption: {}", e);
                        // log error
                    }
                }
            }
        });

        Ok(ctx)
    }
}
