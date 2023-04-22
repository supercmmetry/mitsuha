use crate::{constants::Constants, selector::Label, symbol::Symbol, types};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use async_trait::async_trait;

#[derive(Serialize, Deserialize)]
pub struct JobSpec {
    pub handle: String,
    pub symbol: Symbol,
    pub input_handle: String,
    pub output_handle: String,
    pub status_handle: String,
    pub ttl: u64,
    pub extensions: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub enum JobStatusType {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize)]
pub struct JobStatus {
    pub status: JobStatusType,
    pub extensions: HashMap<String, String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StorageSpec {
    pub handle: String,
    pub data: Vec<u8>,
    pub ttl: u64,
    pub extensions: HashMap<String, String>,
}

impl StorageSpec {
    pub fn with_selector(mut self, label: &Label) -> Self {
        self.extensions.insert(
            Constants::StorageSelectorQuery.to_string(),
            serde_json::to_string(label).unwrap(),
        );

        self
    }
}

#[async_trait]
pub trait Kernel: Send + Sync {
    async fn run_job(&self, spec: &JobSpec) -> types::Result<()>;

    async fn extend_job(&self, handle: String, time: u64) -> types::Result<()>;

    async fn abort_job(&self, handle: String) -> types::Result<()>;

    async fn get_job_status(&self, handle: String) -> types::Result<JobStatus>;

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>>;

    async fn persist_data(&self, handle: String, time: u64) -> types::Result<()>;

    async fn clear_data(&self, handle: String) -> types::Result<()>;
}

#[async_trait]
pub trait CoreStub: Send + Sync {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}
