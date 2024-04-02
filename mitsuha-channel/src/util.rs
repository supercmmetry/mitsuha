use mitsuha_core_types::channel::ComputeInput;
use mitsuha_core_types::kernel::{JobSpec, StorageSpec};

use mitsuha_core::channel::ComputeInputExt;
use mitsuha_core::{kernel::JobSpecExt, types};
use rand::{distributions::Alphanumeric, Rng};

pub fn generate_random_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}

pub fn make_output_storage_spec(job_spec: JobSpec, data: Vec<u8>) -> types::Result<StorageSpec> {
    let ttl = job_spec.get_output_ttl()?;

    let storage_spec = StorageSpec {
        handle: job_spec.output_handle,
        data,
        ttl,
        extensions: job_spec.extensions,
    };

    Ok(storage_spec)
}

pub fn make_compute_input_span(elem: &ComputeInput) -> tracing::span::Span {
    tracing::info_span!(
        "compute_input",
        handle = elem.get_handle(),
        kind = elem.get_opkind(),
        ttl = elem.get_optional_ttl(),
    )
}

pub fn make_job_span(executor: &str) -> tracing::span::Span {
    tracing::info_span!("job_run", job_executor = executor)
}
