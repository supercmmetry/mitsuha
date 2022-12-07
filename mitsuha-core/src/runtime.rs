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

pub enum ExecutorType {
    Local,
    Delegated,
}

pub trait Runtime {
    fn runtime_type(&self) -> RuntimeType;
}
