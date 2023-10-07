use mitsuha_core::config::Config;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, Registry};

use self::otel::create_otel_layer;

pub mod otel;

pub fn setup(config: &Config) -> anyhow::Result<()> {
    tracing_log::LogTracer::init()?;

    let subscriber = Registry::default().with(create_otel_layer(&config)?);

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}
