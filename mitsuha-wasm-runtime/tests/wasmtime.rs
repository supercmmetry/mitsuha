use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use mitsuha_core::{
    kernel::CoreStub,
    linker::{Linker, LinkerContext},
    module::{ModuleInfo, ModuleType},
    provider::Provider,
    resolver::Resolver,
    symbol::Symbol,
    types,
};
use mitsuha_wasm_runtime::wasmtime::WasmMetadata;
use mitsuha_wasm_runtime::{
    resolver::artifact::ArtifactResolver,
    wasmtime::{WasmtimeLinker, WasmtimeModule},
};
use musubi_api::{
    types::{Data, Value},
    DataBuilder,
};

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
        let wasm_echo: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm").to_vec();
        let wasm_loop: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm").to_vec();
        let wasm_main: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm").to_vec();

        Self {
            wasm_echo,
            wasm_loop,
            wasm_main,
        }
    }
}

fn get_artifact_resolver() -> ArtifactResolver {
    ArtifactResolver {
        redis_resolver: None,
        provider: Arc::new(RwLock::new(InMemoryWasmProvider::new())),
    }
}

struct TestCoreStub;

#[async_trait]
impl CoreStub for TestCoreStub {
    async fn run(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn load_wasm_metadata_1() -> anyhow::Result<()> {
    let resolver = get_artifact_resolver();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_no_dep_wasm() -> anyhow::Result<()> {
    let mut config = wasmtime::Config::default();
    config.async_support(true);
    config.epoch_interruption(true);

    let engine = wasmtime::Engine::new(&config)?;

    let resolver = Arc::new(RwLock::new(WasmtimeModuleResolver::new(engine.clone())));
    let mut wasmtime_linker = WasmtimeLinker::new(resolver.clone(), engine)?;
    let core_stub = Arc::new(tokio::sync::RwLock::new(TestCoreStub));
    let mut linker_context = LinkerContext::new(core_stub);

    println!("created linker context");

    let module_info = ModuleInfo {
        name: "mitsuha.test.echo".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    wasmtime_linker
        .load(&mut linker_context, &module_info)
        .await?;

    println!("linker load completed");

    let executor_context = wasmtime_linker
        .link(&mut linker_context, &module_info)
        .await?;

    println!("linker link completed");

    let symbol = Symbol {
        module_info,
        name: "echo".to_string(),
    };

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    dbg!("calling symbol ...");
    let output = executor_context.call(&symbol, input.try_into()?).await?;
    dbg!(Data::try_from(output));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wasm_timed_future() -> anyhow::Result<()> {
    let mut config = wasmtime::Config::default();
    config.async_support(true);
    config.epoch_interruption(true);

    let engine = wasmtime::Engine::new(&config)?;

    let resolver = Arc::new(RwLock::new(WasmtimeModuleResolver::new(engine.clone())));
    let mut wasmtime_linker = WasmtimeLinker::new(resolver.clone(), engine)?;
    let core_stub = Arc::new(tokio::sync::RwLock::new(TestCoreStub));
    let mut linker_context = LinkerContext::new(core_stub);

    println!("created linker context");

    let module_info = ModuleInfo {
        name: "mitsuha.test.loop".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    wasmtime_linker
        .load(&mut linker_context, &module_info)
        .await?;

    println!("linker load completed");

    let executor_context = wasmtime_linker
        .link(&mut linker_context, &module_info)
        .await?;

    println!("linker link completed");

    let symbol = Symbol {
        module_info,
        name: "run".to_string(),
    };

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    dbg!("calling symbol ...");

    let timed_fut = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        executor_context.call(&symbol, input.try_into()?),
    );
    dbg!(timed_fut.await);

    Ok(())
}
