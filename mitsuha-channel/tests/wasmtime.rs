use std::{sync::Arc, time::Duration};

mod setup;
use mitsuha_channel::context::ChannelContext;
use mitsuha_core::{channel::{ComputeChannel, ComputeInput, ComputeOutput}, module::{ModuleInfo, ModuleType}, kernel::{StorageSpec, JobSpec}, symbol::Symbol};
use musubi_api::{DataBuilder, types::{Value, Data}};
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

macro_rules! graceless_async_test {
    ($code: block) => {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            $code
        });

        runtime.shutdown_background();
    };
}

async fn internal_run_hello_world() {
    let channel = make_channel().await;
    let ctx = ChannelContext::default();

    upload_artifacts(channel.clone()).await;

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let job_handle = "run_hello_world_job_1".to_string();
    let input_handle = "run_hello_world_input_1".to_string();
    let status_handle = "run_hello_world_status_1".to_string();
    let output_handle = "run_hello_world_output_1".to_string();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 120,
        extensions: Default::default(),
    };

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

    channel.compute(ctx.clone(), ComputeInput::Store { spec: input_spec }).await.unwrap();

    channel.compute(ctx.clone(), ComputeInput::Run { spec: job_spec }).await.unwrap();


    let output = channel.compute(ctx.clone(), ComputeInput::Load { handle: output_handle }).await.unwrap();

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

async fn internal_run_mugen_loop() {
    let channel = make_channel().await;
    let ctx = ChannelContext::default();

    upload_artifacts(channel.clone()).await;

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let job_handle = "run_mugen_loop_job_1".to_string();
    let input_handle = "run_mugen_loop_input_1".to_string();
    let status_handle = "run_mugen_loop_status_1".to_string();
    let output_handle = "run_mugen_loop_output_1".to_string();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 120,
        extensions: Default::default(),
    };

    let job_spec = JobSpec {
        handle: job_handle,
        symbol: Symbol {
            name: "run".to_string(),
            module_info: ModuleInfo {
                name: "mitsuha.test.loop".to_string(),
                version: "0.1.0".to_string(),
                modtype: ModuleType::WASM,
            },
        },
        ttl: 1,
        input_handle,
        output_handle: output_handle.clone(),
        status_handle,
        extensions: Default::default(),
    };

    channel.compute(ctx.clone(), ComputeInput::Store { spec: input_spec }).await.unwrap();

    let result = channel.compute(ctx.clone(), ComputeInput::Run { spec: job_spec }).await;

    assert!(result.is_err());
}

async fn internal_run_and_abort_mugen_loop() {
    let channel = make_channel().await;
    let ctx = ChannelContext::default();

    upload_artifacts(channel.clone()).await;

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let job_handle = "run_and_abort_mugen_loop_job_1".to_string();
    let input_handle = "run_and_abort_mugen_loop_input_1".to_string();
    let status_handle = "run_and_abort_mugen_loop_status_1".to_string();
    let output_handle = "run_and_abort_mugen_loop_output_1".to_string();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 120,
        extensions: Default::default(),
    };

    let job_spec = JobSpec {
        handle: job_handle.clone(),
        symbol: Symbol {
            name: "run".to_string(),
            module_info: ModuleInfo {
                name: "mitsuha.test.loop".to_string(),
                version: "0.1.0".to_string(),
                modtype: ModuleType::WASM,
            },
        },
        ttl: 86400,
        input_handle,
        output_handle: output_handle.clone(),
        status_handle,
        extensions: Default::default(),
    };

    channel.compute(ctx.clone(), ComputeInput::Store { spec: input_spec }).await.unwrap();

    let cloned_channel = channel.clone();
    let cloned_ctx = ctx.clone();

    let join_handle = tokio::task::spawn(async move {
        let result = cloned_channel.compute(cloned_ctx, ComputeInput::Run { spec: job_spec }).await;
        dbg!(result.as_ref().err());
        assert!(result.is_err());
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    channel.compute(ctx.clone(), ComputeInput::Abort { handle: job_handle }).await.unwrap();

    join_handle.await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn internal_run_wasm_with_deps() {
    let channel = make_channel().await;
    let ctx = ChannelContext::default();

    upload_artifacts(channel.clone()).await;

    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let job_handle = "run_wasm_with_deps_job_1".to_string();
    let input_handle = "run_wasm_with_deps_input_1".to_string();
    let status_handle = "run_wasm_with_deps_status_1".to_string();
    let output_handle = "run_wasm_with_deps_output_1".to_string();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 120,
        extensions: Default::default(),
    };

    let job_spec = JobSpec {
        handle: job_handle,
        symbol: Symbol {
            name: "run".to_string(),
            module_info: ModuleInfo {
                name: "mitsuha.test.main".to_string(),
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

    channel.compute(ctx.clone(), ComputeInput::Store { spec: input_spec }).await.unwrap();

    channel.compute(ctx.clone(), ComputeInput::Run { spec: job_spec }).await.unwrap();


    let output = channel.compute(ctx.clone(), ComputeInput::Load { handle: output_handle }).await.unwrap();

    if let ComputeOutput::Loaded { data } = output {
        match Data::try_from(data).unwrap().values().get(0).unwrap() {
            Value::String(s) => {
                assert_eq!(s.as_str(), "Hello world!");
            },
            x => {
                dbg!(x);
                panic!("expected string")
            }
        }
    } else {
        panic!("expected ComputeOutput of type Loaded");
    }

}

#[test]
fn run_hello_world() {
    graceless_async_test! ({
       internal_run_hello_world().await;
    });
}

#[test]
fn run_wasm_with_deps() {
    graceless_async_test! ({
       internal_run_wasm_with_deps().await;
    });
}

#[test]
fn run_and_abort_mugen_loop() {
    graceless_async_test! ({
       internal_run_and_abort_mugen_loop().await;
    });
}

#[test]
fn run_mugen_loop() {
    graceless_async_test! ({
       internal_run_mugen_loop().await;
    });
}
