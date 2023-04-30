use async_trait::async_trait;

use crate::{kernel::JobSpec, types};

pub enum WasmRuntimeType {
    Wasmtime,
    Wasmer,
}

pub enum ContainerRuntimeType {
    Docker,
    Kubernetes,
}

pub enum RuntimeType {
    Wasm(WasmRuntimeType),
    Container(ContainerRuntimeType),
}

#[async_trait]
pub trait Runtime {
    fn runtime_type(&self) -> RuntimeType;

    async fn run(&self, spec: &JobSpec) -> types::Result<()>;

    async fn extend(&self, handle: String, time: u64) -> types::Result<()>;
}
