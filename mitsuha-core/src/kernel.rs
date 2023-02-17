use crate::{symbol::Symbol, types};
use std::collections::HashMap;

use musubi_api::types::Value;

use serde::{Deserialize, Serialize};


use async_trait::async_trait;

#[derive(Serialize, Deserialize)]
pub struct JobSpec {
    id: String,
    symbol: Symbol,
    input_handle: String,
    output_handle: String,
    status_handle: String,
    ttl: u64,
    extensions: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub enum JobStatusType {
    Pending,
    Running,
    Completed,
}

#[derive(Serialize, Deserialize)]
pub struct JobStatus {
    status: JobStatusType,
    extensions: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub struct StorageSpec {
    handle: String,
    data: Vec<u8>,
    ttl: u64,
    extensions: HashMap<String, Value>,
}

#[async_trait]
pub trait Kernel: Send + Sync {
    async fn run_job(&self, spec: &JobSpec) -> types::Result<()>;

    async fn extend_job(&self, id: String, time: u64) -> types::Result<()>;

    async fn abort_job(&self, id: String) -> types::Result<()>;

    async fn get_job_status(&self, id: String) -> types::Result<JobStatus>;

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>>;

    async fn persist_data(&self, handle: String, time: u64) -> types::Result<()>;

    async fn clear_data(&self, handle: String) -> types::Result<()>;
}

#[async_trait]
pub trait CoreStub: Send + Sync {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}
