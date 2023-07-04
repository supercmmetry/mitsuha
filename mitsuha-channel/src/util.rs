use mitsuha_core::errors::Error;
use mitsuha_core::{
    constants::Constants,
    kernel::{JobSpec, StorageSpec},
    types,
};
use rand::{distributions::Alphanumeric, Rng};

pub fn generate_random_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}

pub fn make_output_storage_spec(job_spec: JobSpec, data: Vec<u8>) -> types::Result<StorageSpec> {
    let mut ttl = job_spec.ttl;
    if let Some(v) = job_spec
        .extensions
        .get(&Constants::JobOutputTTL.to_string())
    {
        ttl = v
            .parse::<u64>()
            .map_err(|e| Error::Unknown { source: e.into() })?;
    }

    let storage_spec = StorageSpec {
        handle: job_spec.output_handle,
        data,
        ttl,
        extensions: Default::default(),
    };

    Ok(storage_spec)
}
