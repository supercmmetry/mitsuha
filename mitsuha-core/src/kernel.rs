use crate::{constants::Constants, errors::Error, selector::Label, types};
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use mitsuha_core_types::{symbol::Symbol, kernel::{JobSpec, StorageSpec, JobStatus}};
use musubi_api::{
    types::{Data, HashableValue, Value},
    DataBuilder,
};
use serde::{Deserialize, Serialize};

use async_trait::async_trait;
use uuid::Uuid;

pub trait JobSpecExt {
    fn get_output_ttl(&self) -> types::Result<u64>;

    fn get_kernel_bridge_metadata(&self) -> types::Result<Option<KernelBridgeMetadata>>;

    fn make_kernel_bridge_metadata(&self) -> types::Result<KernelBridgeMetadata>;

    fn load_kernel_bridge_metadata(&mut self, metadata: &KernelBridgeMetadata);
}

impl JobSpecExt for JobSpec {
    fn get_output_ttl(&self) -> types::Result<u64> {
        let mut ttl = self.ttl;
        if let Some(v) = self.extensions.get(&Constants::JobOutputTTL.to_string()) {
            ttl = v
                .parse::<u64>()
                .map_err(|e| Error::Unknown { source: e.into() })?;
        }

        Ok(ttl)
    }

    fn get_kernel_bridge_metadata(&self) -> types::Result<Option<KernelBridgeMetadata>> {
        let kernel_bridge_metadata_key = &Constants::JobKernelBridgeMetadata.to_string();

        match self.extensions.get(kernel_bridge_metadata_key) {
            Some(value) => {
                let obj: KernelBridgeMetadata = serde_json::from_str(value.as_str())
                    .map_err(|e| Error::Unknown { source: e.into() })?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    fn make_kernel_bridge_metadata(&self) -> types::Result<KernelBridgeMetadata> {
        let existing_metadata = self.get_kernel_bridge_metadata()?;

        let mut new_metadata = KernelBridgeMetadata {
            job_ttl: self.ttl,
            job_output_ttl: self.get_output_ttl()?,
            job_start_time: Utc::now(),
            extensions: Default::default(),
        };

        if let Some(metadata) = existing_metadata {
            new_metadata.extensions = metadata.extensions;
        }

        Ok(new_metadata)
    }

    fn load_kernel_bridge_metadata(&mut self, metadata: &KernelBridgeMetadata) {
        self.ttl = metadata.job_ttl;
        self.extensions.insert(
            Constants::JobOutputTTL.to_string(),
            metadata.job_output_ttl.to_string(),
        );
    }
}

pub trait StorageSpecExt {
    fn with_selector(self, label: &Label) -> Self;
}

impl StorageSpecExt for StorageSpec {
    fn with_selector(mut self, label: &Label) -> Self {
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

    async fn extend_job(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()>;

    async fn abort_job(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()>;

    async fn get_job_status(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<JobStatus>;

    async fn store_data(&self, spec: StorageSpec) -> types::Result<()>;

    async fn load_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<Vec<u8>>;

    async fn persist_data(
        &self,
        handle: String,
        ttl: u64,
        extensions: HashMap<String, String>,
    ) -> types::Result<()>;

    async fn clear_data(
        &self,
        handle: String,
        extensions: HashMap<String, String>,
    ) -> types::Result<()>;
}

#[async_trait]
pub trait KernelBinding: Send + Sync {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KernelBridgeMetadata {
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub job_ttl: u64,

    #[serde(skip_deserializing)]
    #[serde(default)]
    pub job_output_ttl: u64,

    #[serde(skip_deserializing)]
    #[serde(default)]
    pub job_start_time: DateTime<Utc>,

    pub extensions: HashMap<String, String>,
}

pub struct KernelBridge {
    kernel: Arc<Box<dyn Kernel>>,
    metadata: KernelBridgeMetadata,
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
    pub fn new(kernel: Arc<Box<dyn Kernel>>, metadata: KernelBridgeMetadata) -> Self {
        Self { kernel, metadata }
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
                            "attempted kernel call: {}, expected 1 value, found {}",
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
                if data.values().len() != 3 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 3 values, found {}",
                            CORE_SYMBOL_EXTEND,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let ttl: u64;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(2).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected third value to be a extension map",
                                        CORE_SYMBOL_EXTEND
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected third value to be a map",
                            CORE_SYMBOL_EXTEND
                        ),
                    });
                }

                self.kernel.extend_job(handle, ttl, extensions).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_ABORT => {
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values, found {}",
                            CORE_SYMBOL_ABORT,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(1).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected second value to be a extension map",
                                        CORE_SYMBOL_ABORT
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a map",
                            CORE_SYMBOL_ABORT
                        ),
                    });
                }

                self.kernel.abort_job(handle, extensions).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_STATUS => {
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values, found {}",
                            CORE_SYMBOL_STATUS,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(1).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected second value to be a extension map",
                                        CORE_SYMBOL_STATUS
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a map",
                            CORE_SYMBOL_STATUS
                        ),
                    });
                }

                let status = self.kernel.get_job_status(handle, extensions).await?;

                data_builder = data_builder.add(
                    musubi_api::types::to_value(status)
                        .map_err(|e| Error::Unknown { source: e.into() })?,
                );
            }
            CORE_SYMBOL_STORE => {
                if data.values().len() != 1 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 1 value, found {}",
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
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values, found {}",
                            CORE_SYMBOL_LOAD,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(1).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected second value to be a extension map",
                                        CORE_SYMBOL_LOAD
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a map",
                            CORE_SYMBOL_LOAD
                        ),
                    });
                }

                let data = self.kernel.load_data(handle, extensions).await?;

                data_builder = data_builder.add(Value::Bytes(data));
            }
            CORE_SYMBOL_PERSIST => {
                if data.values().len() != 3 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 3 values, found {}",
                            CORE_SYMBOL_PERSIST,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let ttl: u64;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(2).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected third value to be a extension map",
                                        CORE_SYMBOL_PERSIST
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected third value to be a map",
                            CORE_SYMBOL_PERSIST
                        ),
                    });
                }

                self.kernel.persist_data(handle, ttl, extensions).await?;

                data_builder = data_builder.add(Value::Null);
            }
            CORE_SYMBOL_CLEAR => {
                if data.values().len() != 2 {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected 2 values, found {}",
                            CORE_SYMBOL_CLEAR,
                            data.values().len()
                        ),
                    });
                }

                let handle: String;
                let mut extensions: HashMap<String, String> = Default::default();

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

                if let Value::Map(x) = data.values().get(1).unwrap() {
                    for (key, value) in x.iter() {
                        match (key, value) {
                            (HashableValue::String(key), Value::String(value)) => {
                                extensions.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(Error::InvalidOperation {
                                    message: format!(
                                        "attempted kernel call: {}, expected second value to be a extension map",
                                        CORE_SYMBOL_CLEAR
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidOperation {
                        message: format!(
                            "attempted kernel call: {}, expected second value to be a map",
                            CORE_SYMBOL_CLEAR
                        ),
                    });
                }

                self.kernel.clear_data(handle, extensions).await?;

                data_builder = data_builder.add(Value::Null);
            }
            _ => {}
        }

        Ok(TryInto::<Vec<u8>>::try_into(data_builder.build())
            .map_err(|e| Error::Unknown { source: e.into() })?)
    }

    // Deprecated as this can be handled in client side by musubi with kernel calls
    #[deprecated]
    async fn dispatch_job(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        let input_handle = Uuid::new_v4().to_string();
        let output_handle = Uuid::new_v4().to_string();

        let input_spec = StorageSpec {
            handle: input_handle.clone(),
            data: input,
            ttl: self.metadata.job_output_ttl,
            extensions: Default::default(),
        };

        self.kernel.store_data(input_spec).await?;

        let mut job_spec = JobSpec {
            handle: Uuid::new_v4().to_string(),
            symbol: symbol.clone(),
            input_handle,
            output_handle: output_handle.clone(),
            ttl: 0,
            extensions: [
                (Constants::JobOutputTTL.to_string(), "0".to_string()),
                (Constants::JobChannelAwait.to_string(), "true".to_string()),
            ]
            .into_iter()
            .collect(),
        };

        job_spec.load_kernel_bridge_metadata(&self.metadata);

        self.kernel.run_job(job_spec).await?;

        self.kernel
            .load_data(output_handle, Default::default())
            .await
    }
}
