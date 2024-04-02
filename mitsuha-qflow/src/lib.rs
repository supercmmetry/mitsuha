use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use mitsuha_core::types;
use mitsuha_core_types::channel::ComputeInput;
use tikv::make_tikv_reader;

use tikv::make_tikv_writer;

pub mod gate;
mod partition;
pub mod tikv;
pub mod util;

#[async_trait]
pub trait System {
    async fn update_configuration(&self, patch: HashMap<String, String>) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Reader: System + Send + Sync {
    async fn read_compute_input(&self, client_id: String) -> anyhow::Result<ComputeInput>;
}

#[async_trait]
pub trait Writer: System + Send + Sync {
    async fn write_compute_input(&self, input: ComputeInput) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ComputeInputGate: Send + Sync {
    async fn evaluate_compute_input(&self, input: &mut ComputeInput) -> types::Result<()>;
}

pub async fn make_writer(
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Writer>>> {
    match extensions.get("kind").unwrap().as_str() {
        "tikv" => make_tikv_writer(extensions).await,
        kind => Err(anyhow!("unknown qflow kind: '{}'", kind)),
    }
}

pub async fn make_reader(
    gate: Arc<Box<dyn ComputeInputGate>>,
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Reader>>> {
    match extensions.get("kind").unwrap().as_str() {
        "tikv" => make_tikv_reader(gate, extensions).await,
        kind => Err(anyhow!("unknown qflow kind: '{}'", kind)),
    }
}
