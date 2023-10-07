use mitsuha_core::config::Config;
use opentelemetry::{
    sdk::{trace::Tracer, Resource},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::Registry;

pub fn create_otel_layer<S>(
    config: &Config,
) -> anyhow::Result<Option<OpenTelemetryLayer<S, Tracer>>>
where
    S: Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    if config.telemetry.opentelemetry.is_none() {
        return Ok(None);
    }

    let otel_config = config.telemetry.opentelemetry.clone().unwrap();

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otel_config.endpoint);

    let otlp_tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            opentelemetry::sdk::trace::config().with_resource(Resource::new(
                otel_config
                    .entity_attributes
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect::<Vec<KeyValue>>(),
            )),
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    let tracing_layer = tracing_opentelemetry::layer().with_tracer(otlp_tracer);

    Ok(Some(tracing_layer))
}
