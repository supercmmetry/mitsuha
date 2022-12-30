use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use mitsuha_core::{
    module::{ModuleInfo, ModuleType},
    provider::Provider,
    resolver::Resolver,
};
use mitsuha_wasm_runtime::resolver::artifact::ArtifactResolver;

pub struct TestProvider;

#[async_trait(?Send)]
impl Provider for TestProvider {
    async fn get_raw_module(&self, _module_info: &ModuleInfo) -> anyhow::Result<Vec<u8>> {
        Ok(vec![1u8, 2u8, 3u8])
    }
}

impl TestProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[tokio::test]
async fn resolve_artifact_1() -> anyhow::Result<()> {
    let provider = TestProvider::new();
    let artifact_resolver = ArtifactResolver {
        redis_resolver: None,
        provider: Arc::new(RwLock::new(provider)),
    };

    let value = artifact_resolver
        .resolve(&ModuleInfo {
            name: "".to_string(),
            version: "".to_string(),
            modtype: ModuleType::WASM,
        })
        .await?;

    assert_eq!(value, vec![1u8, 2u8, 3u8]);
    Ok(())
}
