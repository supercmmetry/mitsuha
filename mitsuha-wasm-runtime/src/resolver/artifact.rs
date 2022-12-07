use mitsuha_core::resolver::{
    redis::{RedisContextKey, RedisKey},
    Resolver,
};

use async_trait::async_trait;
use mitsuha_core::{
    errors::Error,
    module::ModuleInfo,
    provider::Provider,
    types::{self, SharedMany},
};

#[derive(Clone)]
pub struct ArtifactResolver {
    pub redis_resolver: SharedMany<dyn Resolver<RedisContextKey<ModuleInfo, RedisKey>, Vec<u8>>>,
    pub provider: SharedMany<dyn Provider>,
}

#[async_trait(?Send)]
impl Resolver<ModuleInfo, Vec<u8>> for ArtifactResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<Vec<u8>> {
        let redis_key = RedisContextKey::new(key.clone(), RedisKey::ModuleInfoToWasm);

        // Check if the file can be resolved from redis.
        if let Ok(v) = self
            .redis_resolver
            .read()
            .unwrap()
            .resolve(&redis_key)
            .await
        {
            return Ok(v);
        }

        let value = self
            .provider
            .read()
            .unwrap()
            .get_raw_module(key)
            .await
            .map_err(|e| Error::ResolverModuleNotFound {
                message: "failed to get resource from provider".to_string(),
                inner: key.clone(),
                source: e,
            })?;

        self.redis_resolver
            .read()
            .unwrap()
            .register(&redis_key, &value)
            .await?;

        Ok(value)
    }

    async fn register(&self, _key: &ModuleInfo, _value: &Vec<u8>) -> types::Result<()> {
        unimplemented!()
    }

    async fn register_mut(&mut self, _key: &ModuleInfo, _value: &Vec<u8>) -> types::Result<()> {
        unimplemented!()
    }
}
