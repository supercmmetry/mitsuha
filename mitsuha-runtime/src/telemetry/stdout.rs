use tracing::{Level, Subscriber};
use tracing_subscriber::filter::{Filtered, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Pretty};
use tracing_subscriber::Layer;
use mitsuha_core::config::Config;

pub fn create_stdout_layer<S>(
    config: &Config,
) -> anyhow::Result<Option<Filtered<tracing_subscriber::fmt::Layer<S>, LevelFilter, S>>>
    where
        S: Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    if config.telemetry.stdout.is_none() {
        return Ok(None);
    }

    let stdout_log= tracing_subscriber::fmt::layer()
        .with_filter(LevelFilter::from_level(Level::TRACE));


    Ok(Some(stdout_log))
}
