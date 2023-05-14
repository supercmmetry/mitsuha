use std::sync::Arc;

mod setup;
use criterion::{Criterion, criterion_group, criterion_main};
use mitsuha_channel::context::ChannelContext;
use mitsuha_core::{channel::{ComputeChannel, ComputeInput, ComputeOutput}, module::{ModuleInfo, ModuleType}, kernel::{StorageSpec, JobSpec}, symbol::Symbol};
use musubi_api::{DataBuilder, types::{Value, Data}};
use rand::{distributions::Alphanumeric, Rng};
use setup::*;

pub async fn make_channel() -> Arc<Box<dyn ComputeChannel<Context = ChannelContext>>> {
    let system_channel = make_system_channel();
    let labeled_storage_channel = make_labeled_storage_channel();
    let wasmtime_channel = make_wasmtime_channel(system_channel.clone());

    labeled_storage_channel.connect(wasmtime_channel).await;

    system_channel
        .connect(labeled_storage_channel)
        .await;

    system_channel
}

pub async fn upload_artifacts(channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
    let wasm_echo: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm").to_vec();
    let wasm_loop: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm").to_vec();
    let wasm_main: Vec<u8> = include_bytes!("../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm").to_vec();

    let module_info_echo = ModuleInfo {
        name: "mitsuha.test.echo".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let module_info_loop = ModuleInfo {
        name: "mitsuha.test.loop".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let module_info_main = ModuleInfo {
        name: "mitsuha.test.main".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let spec_echo = StorageSpec {
        handle: module_info_echo.get_identifier(),
        data: wasm_echo,
        ttl: 0,
        extensions: Default::default(),
    };

    let spec_loop = StorageSpec {
        handle: module_info_loop.get_identifier(),
        data: wasm_loop,
        ttl: 0,
        extensions: Default::default(),
    };

    let spec_main = StorageSpec {
        handle: module_info_main.get_identifier(),
        data: wasm_main,
        ttl: 0,
        extensions: Default::default(),
    };

    channel.compute(ChannelContext::default(), ComputeInput::Store { spec: spec_echo }).await.unwrap();
    channel.compute(ChannelContext::default(), ComputeInput::Store { spec: spec_loop }).await.unwrap();
    channel.compute(ChannelContext::default(), ComputeInput::Store { spec: spec_main }).await.unwrap();
   
}

async fn run_hello_world(ctx: ChannelContext, channel: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>, mut spec: JobSpec) {
    let handle: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    spec.handle = handle.clone();
    spec.output_handle = handle.clone();

    channel.compute(ctx.clone(), ComputeInput::Run { spec }).await.unwrap();

    
    let output = channel.compute(ctx, ComputeInput::Load { handle }).await.unwrap();

    if let ComputeOutput::Loaded { data } = output {
        match Data::try_from(data).unwrap().values().get(0).unwrap() {
            Value::String(s) => {
                assert_eq!(s.as_str(), "Hello world!");
            },
            _ => panic!("expected string")
        }
    } else {
        panic!("expected ComputeOutput of type Loaded");
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    let channel = tokio_rt.block_on(make_channel());
    let ctx = ChannelContext::default();

    tokio_rt.block_on(upload_artifacts(channel.clone()));

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let job_handle = "job_1".to_string();
    let input_handle = "input_1".to_string();
    let status_handle = "status_1".to_string();
    let output_handle = "output_1".to_string();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 86400,
        extensions: Default::default(),
    };

    tokio_rt.block_on(channel.compute(ctx.clone(), ComputeInput::Store { spec: input_spec })).unwrap();

    let job_spec = JobSpec {
        handle: job_handle,
        symbol: Symbol {
            name: "echo".to_string(),
            module_info: ModuleInfo {
                name: "mitsuha.test.echo".to_string(),
                version: "0.1.0".to_string(),
                modtype: ModuleType::WASM,
            },
        },
        ttl: 120,
        input_handle,
        output_handle: output_handle.clone(),
        status_handle,
        extensions: Default::default(),
    };

    c.bench_function("run_hello_world", |b| {
        b.to_async(&tokio_rt)
            .iter(|| run_hello_world(ctx.clone(), channel.clone(), job_spec.clone()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
