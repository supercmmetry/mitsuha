use mitsuha_core_types::kernel::{JobSpec, StorageSpec};

use mitsuha_core::{types, kernel::JobSpecExt};
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
