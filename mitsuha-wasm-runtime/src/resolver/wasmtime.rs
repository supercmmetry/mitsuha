use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{module::ModuleInfo, resolver::Resolver, types};

use crate::wasmtime::WasmtimeModule;

pub struct WasmtimeModuleResolver {
    resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    engine: wasmtime::Engine,
}

impl WasmtimeModuleResolver {
    pub fn new(
        engine: wasmtime::Engine,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    ) -> Self {
        Self { resolver, engine }
    }
}

#[async_trait]
impl Resolver<ModuleInfo, WasmtimeModule> for WasmtimeModuleResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<WasmtimeModule> {
        let data = self.resolver.resolve(&key).await?;
        Ok(WasmtimeModule::new(data, key.clone(), &self.engine)?)
    }

    async fn register(&self, key: &ModuleInfo, value: &WasmtimeModule) -> types::Result<()> {
        unimplemented!()
    }
}
