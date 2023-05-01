use crate::{constants::Constants, selector::Label, symbol::Symbol, types};
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use async_trait::async_trait;
use uuid::Uuid;

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
    Running,
    Completed,
    Aborted,
    ExpiredAt { datetime: DateTime<Utc> },
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
    async fn run_job(&self, spec: JobSpec) -> types::Result<()>;

    async fn extend_job(&self, handle: String, ttl: u64) -> types::Result<()>;

    async fn abort_job(&self, handle: String) -> types::Result<()>;

    async fn get_job_status(&self, handle: String) -> types::Result<JobStatus>;

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load_data(&self, handle: String) -> types::Result<Vec<u8>>;

    async fn persist_data(&self, handle: String, ttl: u64) -> types::Result<()>;

    async fn clear_data(&self, handle: String) -> types::Result<()>;
}

#[async_trait]
pub trait CoreStub: Send + Sync {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}

pub struct StubbedKernel {
    kernel: Arc<Box<dyn Kernel>>
}

static CORE_SYMBOL_RUN: &str = "run";
static CORE_SYMBOL_EXTEND: &str = "extend";
static CORE_SYMBOL_ABORT: &str = "abort";
static CORE_SYMBOL_STATUS: &str = "status";
static CORE_SYMBOL_STORE: &str = "store";
static CORE_SYMBOL_LOAD: &str = "load";
static CORE_SYMBOL_PERSIST: &str = "persist";
static CORE_SYMBOL_CLEAR: &str = "clear";

lazy_static! {
    static ref CORE_SYMBOL_NAMES: Vec<&'static str> = vec![
        CORE_SYMBOL_RUN,
        CORE_SYMBOL_EXTEND,
        CORE_SYMBOL_ABORT,
        CORE_SYMBOL_STATUS,
        CORE_SYMBOL_STORE,
        CORE_SYMBOL_LOAD,
        CORE_SYMBOL_PERSIST,
        CORE_SYMBOL_CLEAR,
    ];
}

#[async_trait]
impl CoreStub for StubbedKernel {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        if self.is_core_symbol(symbol) {
            self.kernel_call(symbol, input).await
        } else {
            self.dispatch_job(symbol, input).await
        }
    }
}

impl StubbedKernel {
    pub fn new(kernel: Arc<Box<dyn Kernel>>) -> Self {
        Self { kernel }
    }

    fn is_core_symbol(&self, symbol: &Symbol) -> bool {
        if symbol.module_info.name != "mitsuha.core" {
            return false;
        }

        CORE_SYMBOL_NAMES.contains(&symbol.name.as_str())
    }

    async fn kernel_call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        todo!("direct kernel calls are not supported")
    }

    async fn dispatch_job(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        let input_handle = Uuid::new_v4().to_string();
        let output_handle = Uuid::new_v4().to_string();
        let status_handle = Uuid::new_v4().to_string();

        let input_spec = StorageSpec {
            handle: input_handle.clone(),
            data: input,
            ttl: u64::MAX,
            extensions: Default::default(),
        };

        self.kernel.store_data(input_spec).await?;

        let job_spec = JobSpec {
            handle: Uuid::new_v4().to_string(),
            symbol: symbol.clone(),
            input_handle,
            output_handle: output_handle.clone(),
            status_handle,
            ttl: u64::MAX,
            extensions: Default::default(),
        };

        self.kernel.run_job(job_spec).await?;

        self.kernel.load_data(output_handle).await
    }
}