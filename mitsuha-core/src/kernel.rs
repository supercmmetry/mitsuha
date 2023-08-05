use crate::{constants::Constants, errors::Error, selector::Label, symbol::Symbol, types};
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use musubi_api::{
    types::{Data, Value},
    DataBuilder,
};
use serde::{Deserialize, Serialize};

use async_trait::async_trait;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub handle: String,
    pub symbol: Symbol,
    pub input_handle: String,
    pub output_handle: String,
    pub status_handle: String,
    pub ttl: u64,
    pub extensions: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub trait KernelBinding: Send + Sync {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}

pub struct KernelBridge {
    kernel: Arc<Box<dyn Kernel>>,
}

const CORE_SYMBOL_RUN: &str = "run";
const CORE_SYMBOL_EXTEND: &str = "extend";
const CORE_SYMBOL_ABORT: &str = "abort";
const CORE_SYMBOL_STATUS: &str = "status";
const CORE_SYMBOL_STORE: &str = "store";
const CORE_SYMBOL_LOAD: &str = "load";
const CORE_SYMBOL_PERSIST: &str = "persist";
const CORE_SYMBOL_CLEAR: &str = "clear";

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
impl KernelBinding for KernelBridge {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        if self.is_core_symbol(symbol) {
            self.kernel_call(symbol, input).await
        } else {
            self.dispatch_job(symbol, input).await
        }
    }
}

impl KernelBridge {
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
        let data = Data::try_from(input).map_err(|e| Error::Unknown { source: e.into() })?;
        let mut data_builder = DataBuilder::new();

        match symbol.name.as_str() {
            CORE_SYMBOL_RUN => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_RUN,
                            data.values().len()
                        ),
                    });
                }

                let spec: JobSpec =
                    musubi_api::types::from_value(data.values().get(0).unwrap().clone())
                        .map_err(|e| Error::Unknown { source: e.into() })?;

                self.kernel.run_job(spec).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_EXTEND => {
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values found {}",
                            CORE_SYMBOL_EXTEND,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let ttl: u64;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_EXTEND
                        ),
                    });
                }

                if let Value::U64(x) = data.values().get(1).unwrap() {
                    ttl = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a u64",
                            CORE_SYMBOL_EXTEND
                        ),
                    });
                }

                self.kernel.extend_job(handle, ttl).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_ABORT => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_ABORT,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_ABORT
                        ),
                    });
                }

                self.kernel.abort_job(handle).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_STATUS => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_STATUS,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_STATUS
                        ),
                    });
                }

                let status = self.kernel.get_job_status(handle).await?;

                data_builder = data_builder.add(
                    musubi_api::types::to_value(status)
                        .map_err(|e| Error::Unknown { source: e.into() })?,
                );
            }
            CORE_SYMBOL_STORE => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_STORE,
                            data.values().len()
                        ),
                    });
                }

                let spec: StorageSpec =
                    musubi_api::types::from_value(data.values().get(0).unwrap().clone())
                        .map_err(|e| Error::Unknown { source: e.into() })?;

                self.kernel.store_data(spec).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_LOAD => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_LOAD,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_LOAD
                        ),
                    });
                }

                let data = self.kernel.load_data(handle).await?;

                data_builder = data_builder.add(Value::Bytes(data));
            }
            CORE_SYMBOL_PERSIST => {
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values found {}",
                            CORE_SYMBOL_PERSIST,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let ttl: u64;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_PERSIST
                        ),
                    });
                }

                if let Value::U64(x) = data.values().get(1).unwrap() {
                    ttl = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a u64",
                            CORE_SYMBOL_PERSIST
                        ),
                    });
                }

                self.kernel.persist_data(handle, ttl).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_CLEAR => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value found {}",
                            CORE_SYMBOL_CLEAR,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;

                if let Value::String(x) = data.values().get(0).unwrap() {
                    handle = x.clone();
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected first value to be a string",
                            CORE_SYMBOL_CLEAR
                        ),
                    });
                }

                self.kernel.clear_data(handle).await?;

                data_builder = data_builder.add(Value::Null);
            }
            _ => {}
        }

        Ok(TryInto::<Vec<u8>>::try_into(data_builder.build())
            .map_err(|e| Error::Unknown { source: e.into() })?)
    }

    async fn dispatch_job(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        let input_handle = Uuid::new_v4().to_string();
        let output_handle = Uuid::new_v4().to_string();
        let status_handle = Uuid::new_v4().to_string();

        // TODO: Set sane ttls here

        let input_spec = StorageSpec {
            handle: input_handle.clone(),
            data: input,
            ttl: 86400,
            extensions: Default::default(),
        };

        self.kernel.store_data(input_spec).await?;

        let job_spec = JobSpec {
            handle: Uuid::new_v4().to_string(),
            symbol: symbol.clone(),
            input_handle,
            output_handle: output_handle.clone(),
            status_handle,
            ttl: 86400,
            extensions: [
                (Constants::JobOutputTTL.to_string(), "120".to_string()),
                (Constants::JobChannelAwait.to_string(), "true".to_string()),
            ].into_iter().collect(),
        };

        self.kernel.run_job(job_spec).await?;

        self.kernel.load_data(output_handle).await
    }
}
