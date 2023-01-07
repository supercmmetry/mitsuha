use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use mitsuha_core::{
    module::{ModuleInfo, ModuleType},
    provider::Provider,
    resolver::Resolver,
};
use mitsuha_wasm_runtime::resolver::artifact::ArtifactResolver;
use mitsuha_wasm_runtime::wasmtime::WasmMetadata;

pub struct InMemoryWasmProvider {
    wasm_echo: Vec<u8>,
    wasm_loop: Vec<u8>,
    wasm_main: Vec<u8>,
}

#[async_trait(?Send)]
impl Provider for InMemoryWasmProvider {
    async fn get_raw_module(&self, module_info: &ModuleInfo) -> anyhow::Result<Vec<u8>> {
        assert_eq!(module_info.modtype.clone(), ModuleType::WASM);
        match module_info.name.as_str() {
            "mitsuha.test.echo" => Ok(self.wasm_echo.clone()),
            "mitsuha.test.loop" => Ok(self.wasm_loop.clone()),
            "mitsuha.test.main" => Ok(self.wasm_main.clone()),
            x => Err(anyhow::anyhow!("cannot provide module: {}.", x)),
        }
    }
}

impl InMemoryWasmProvider {
    pub fn new() -> Self {
        let wasm_echo: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/mitsuha-wasm-echo/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm").to_vec();
        let wasm_loop: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/mitsuha-wasm-loop/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm").to_vec();
        let wasm_main: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/mitsuha-wasm-main/target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm").to_vec();

        Self {
            wasm_echo,
            wasm_loop,
            wasm_main,
        }
    }
}

#[tokio::test]
async fn load_wasm_metadata_1() -> anyhow::Result<()> {
    let resolver = ArtifactResolver {
        redis_resolver: None,
        provider: Arc::new(RwLock::new(InMemoryWasmProvider::new())),
    };

    let wasm_data = resolver
        .resolve(&ModuleInfo {
            name: "mitsuha.test.echo".to_string(),
            version: "".to_string(),
            modtype: ModuleType::WASM,
        })
        .await?;

    WasmMetadata::new(wasm_data.as_slice())?;

    Ok(())
}
