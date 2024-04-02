use mitsuha_core::config::Config;
use tracing::Subscriber;
use tracing_subscriber::filter::{Filtered, LevelFilter};
use tracing_subscriber::Layer;

pub fn create_stdout_layer<S>(
    config: &Config,
) -> anyhow::Result<Option<Filtered<tracing_subscriber::fmt::Layer<S>, LevelFilter, S>>>
where
    S: Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    if config.telemetry.stdout.is_none() {
        return Ok(None);
    }

    let stdout_config = config.telemetry.stdout.as_ref().unwrap().clone();

    let stdout_log = tracing_subscriber::fmt::layer()
        .with_filter(LevelFilter::from_level(stdout_config.level.to_level()));

    Ok(Some(stdout_log))
}
