use async_trait::async_trait;

use super::Resolver;
use crate::{
    errors::Error,
    module::ModuleInfo,
    types::{self, SharedMany},
};

pub struct ModuleResolver {
    pub wasm_resolver: SharedMany<dyn Resolver<ModuleInfo, Vec<u8>>>,
    pub service_resolver: SharedMany<dyn Resolver<ModuleInfo, Vec<u8>>>,
}

#[async_trait(?Send)]
impl Resolver<ModuleInfo, WasmerModule> for ModuleResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<WasmerModule> {
        let data = self.wasm_resolver.read().unwrap().resolve(key).await?;

        let module = WasmerModule::new(data, key.clone()).await?;
        Ok(module)
    }

    async fn register(&self, _key: &ModuleInfo, _value: &WasmerModule) -> types::Result<()> {
        unimplemented!()
    }

    async fn register_mut(
        &mut self,
        _key: &ModuleInfo,
        _value: &WasmerModule,
    ) -> types::Result<()> {
        unimplemented!()
    }
}

#[async_trait(?Send)]
impl Resolver<ModuleInfo, ServiceModule> for ModuleResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<ServiceModule> {
        let data = self.service_resolver.read().unwrap().resolve(key).await?;

        let module: ServiceDefinition = serde_json::from_slice(data.as_slice()).map_err(|e| {
            Error::ResolverModulePreprocessingFailed {
                message: "failed to load service definition".to_string(),
                inner: key.clone(),
                source: e.into(),
            }
        })?;

        Ok(module.into())
    }

    async fn register(&self, _key: &ModuleInfo, _value: &ServiceModule) -> types::Result<()> {
        unimplemented!()
    }
}
