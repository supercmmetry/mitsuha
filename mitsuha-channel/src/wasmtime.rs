use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    module::ModuleInfo,
    resolver::Resolver,
    types,
};
use mitsuha_wasm_runtime::{
    resolver::wasmtime::WasmtimeModuleResolver,
    wasmtime::{WasmtimeLinker, WasmtimeModule},
};

use crate::util;

pub struct WasmtimeChannel<Context> {
    id: String,
    next: Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>,
    linker: WasmtimeLinker,
}

#[async_trait]
impl<Context> ComputeChannel for WasmtimeChannel<Context>
where
    Context: Send,
{
    type Context = Context;

    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())
    }

    async fn compute(&self, ctx: Context, mut elem: ComputeInput) -> types::Result<ComputeOutput> {
        Err(Error::UnknownWithMsgOnly {
            message: format!("unimplemented"),
        })
    }

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel<Context = Context>>>) {
        self.next = Some(next);
    }
}

impl<Context> WasmtimeChannel<Context> {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/wasmtime"
    }

    pub fn new(resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>) -> Self {
        // TODO: Make this configurable

        let mut config = wasmtime::Config::default();
        config.async_support(true);
        config.epoch_interruption(true);

        let engine = wasmtime::Engine::new(&config).unwrap();
        let module_resolver: Arc<Box<dyn Resolver<ModuleInfo, WasmtimeModule>>> = Arc::new(
            Box::new(WasmtimeModuleResolver::new(engine.clone(), resolver)),
        );

        let linker = WasmtimeLinker::new(module_resolver, engine).unwrap();

        let id = format!(
            "{}/{}",
            Self::get_identifier_type(),
            util::generate_random_id()
        );

        Self {
            id,
            next: None,
            linker,
        }
    }
}
