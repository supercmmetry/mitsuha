use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{module::ModuleInfo, resolver::Resolver, types};

use crate::wasmtime::WasmtimeModule;

pub struct WasmtimeModuleResolver {
    resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    engine: wasmtime::Engine,
    cache: moka::future::Cache<ModuleInfo, WasmtimeModule>,
}

impl WasmtimeModuleResolver {
    pub fn new(
        engine: wasmtime::Engine,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    ) -> Self {
        let cache = moka::future::Cache::new(16);

        Self { resolver, engine, cache }
    }
}

#[async_trait]
impl Resolver<ModuleInfo, WasmtimeModule> for WasmtimeModuleResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<WasmtimeModule> {
        if let Some(v) = self.cache.get(key) {
            return Ok(v);
        }

        let data = self.resolver.resolve(&key).await?;

        let module = WasmtimeModule::new(data, key.clone(), &self.engine)?;

        self.cache.insert(key.clone(), module.clone()).await;

        Ok(module)
    }

    async fn register(&self, _key: &ModuleInfo, _value: &WasmtimeModule) -> types::Result<()> {
        unimplemented!()
    }
}
