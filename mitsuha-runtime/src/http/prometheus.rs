use actix_web::{get, web};
use lazy_static::lazy_static;
use prometheus::Registry;
use std::sync::Once;

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref REGISTER_ONCE: Once = Once::new();
}

#[get("/metrics")]
async fn get_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        tracing::error!("could not encode custom metrics: {}", e);
    };

    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("custom metrics could not be parsed with utf8: {}", e);
            String::default()
        }
    };

    buffer.clear();

    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        tracing::error!("could not encode prometheus metrics: {}", e);
    };

    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("prometheus metrics could not be parsed with utf8: {}", e);
            String::default()
        }
    };

    buffer.clear();

    res.push_str(&res_custom);

    res
}

fn register_metrics() {
    REGISTRY
        .register(Box::new(
            mitsuha_core::metric::job_request_count_metric().clone(),
        ))
        .expect("failed to register metric");

    REGISTRY
        .register(Box::new(
            mitsuha_core::metric::job_queued_compute_cost_metric().clone(),
        ))
        .expect("failed to register metric");

    REGISTRY
        .register(Box::new(
            mitsuha_scheduler::metric::partition_job_consumption_duration_metric().clone(),
        ))
        .expect("failed to register metric");

    REGISTRY
        .register(Box::new(
            mitsuha_scheduler::metric::partition_orphaned_job_consumption_duration_metric().clone(),
        ))
        .expect("failed to register metric");

    REGISTRY
        .register(Box::new(
            mitsuha_scheduler::metric::partition_job_command_consumption_duration_metric().clone(),
        ))
        .expect("failed to register metric");

    REGISTRY
        .register(Box::new(
            mitsuha_scheduler::metric::partition_lease_renewal_duration_metric().clone(),
        ))
        .expect("failed to register metric");
}

super::register_routes!(app, {
    REGISTER_ONCE.call_once(|| {
        register_metrics();
    });

    app.service(web::scope("/prometheus").service(get_metrics))
});
