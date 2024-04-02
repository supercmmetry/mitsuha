use anyhow::anyhow;
use mitsuha_core::constants::Constants;
use mitsuha_core_types::channel::{ComputeInput, ComputeOutput};
use mitsuha_core_types::kernel::{JobSpec, StorageSpec};
use mitsuha_core_types::module::{ModuleInfo, ModuleType};
use mitsuha_core_types::symbol::Symbol;
use mitsuha_runtime_rpc::proto::channel::channel_client::ChannelClient;
use mitsuha_runtime_rpc::proto::channel::ComputeRequest;
use mitsuha_scheduler::constant::SchedulerConstants;
use musubi_api::types::Value;
use musubi_api::DataBuilder;
use prometheus::core::{Atomic, AtomicU64};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use uuid::Uuid;

async fn add_modules(client: &mut ChannelClient<Channel>) {
    let wasm_echo: Vec<u8> = include_bytes!(
        "../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm"
    )
    .to_vec();

    let module_info_echo = ModuleInfo {
        name: "mitsuha.test.echo".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let wasm_echo_input = ComputeInput::Store {
        spec: StorageSpec {
            handle: module_info_echo.get_identifier(),
            data: wasm_echo,
            ttl: 86400,
            extensions: Default::default(),
        },
    };

    let request: ComputeRequest = wasm_echo_input.try_into().unwrap();

    client.compute(request).await.unwrap();

    let wasm_loop: Vec<u8> = include_bytes!(
        "../../mitsuha-runtime-test/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm"
    )
    .to_vec();

    let module_info_echo = ModuleInfo {
        name: "mitsuha.test.loop".to_string(),
        version: "0.1.0".to_string(),
        modtype: ModuleType::WASM,
    };

    let wasm_loop_input = ComputeInput::Store {
        spec: StorageSpec {
            handle: module_info_echo.get_identifier(),
            data: wasm_loop,
            ttl: 86400,
            extensions: Default::default(),
        },
    };

    let request: ComputeRequest = wasm_loop_input.try_into().unwrap();

    client.compute(request).await.unwrap();
}

fn gen_job_handles() -> (String, String, String) {
    (
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
        Uuid::new_v4().to_string(),
    )
}

async fn dispatch_echo_job(client: &mut ChannelClient<Channel>) -> anyhow::Result<String> {
    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let (job_handle, input_handle, output_handle) = gen_job_handles();

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
        extensions: [
            (Constants::JobOutputTTL.to_string(), "120".to_string()),
            (Constants::JobChannelAwait.to_string(), "false".to_string()),
        ]
        .into_iter()
        .collect(),
    };

    let mut request: ComputeRequest = ComputeInput::Store { spec: input_spec }.try_into().unwrap();

    client.compute(request).await?;

    request = ComputeInput::Run { spec: job_spec }.try_into().unwrap();
    client.compute(request).await?;

    Ok(output_handle)
}

async fn dispatch_loop_job(client: &mut ChannelClient<Channel>) -> String {
    let input = DataBuilder::new()
        .add(Value::String("Hello world!".to_string()))
        .build();

    let (job_handle, input_handle, output_handle) = gen_job_handles();

    let input_spec = StorageSpec {
        handle: input_handle.clone(),
        data: input.clone().try_into().unwrap(),
        ttl: 240,
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
        ttl: 240,
        input_handle,
        output_handle: output_handle.clone(),
        extensions: [
            (Constants::JobOutputTTL.to_string(), "120".to_string()),
            (Constants::JobChannelAwait.to_string(), "false".to_string()),
        ]
        .into_iter()
        .collect(),
    };

    let mut request: ComputeRequest = ComputeInput::Store { spec: input_spec }.try_into().unwrap();

    client.compute(request).await.unwrap();

    request = ComputeInput::Run { spec: job_spec }.try_into().unwrap();
    client.compute(request).await.unwrap();

    output_handle
}

#[tokio::test]
async fn test_simple_execution() {
    let mut client = mitsuha_runtime_rpc::proto::channel::channel_client::ChannelClient::connect(
        "grpc://4.156.180.191:20000",
    )
    .await
    .unwrap();

    add_modules(&mut client).await;

    let output_handle = dispatch_echo_job(&mut client).await.unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let output_spec = ComputeInput::Load {
        handle: output_handle,
        extensions: Default::default(),
    };

    let request: ComputeRequest = output_spec.try_into().unwrap();
    let output = client.compute(request).await.unwrap();

    let output: ComputeOutput = output.into_inner().try_into().unwrap();

    if let ComputeOutput::Loaded { data } = output {
        let data = musubi_api::types::Data::try_from(data).unwrap();
        let out: String = musubi_api::types::from_value(&data.values().get(0).unwrap()).unwrap();

        assert_eq!(out, "Hello world!");
    } else {
        panic!("unexpected output type");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multi_execution() {
    let mut client = mitsuha_runtime_rpc::proto::channel::channel_client::ChannelClient::connect(
        "grpc://4.156.180.191:20000",
    )
    .await
    .unwrap();

    add_modules(&mut client).await;

    let mut output_handles = Vec::<String>::new();

    let count = 16384u64;

    let mut join_handles = Vec::new();
    let mut counter = Arc::new(AtomicU64::new(0));

    let other_counter = counter.clone();
    tokio::task::spawn(async move {
        loop {
            let prev = other_counter.get();
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!(
                "message produced per second = {}",
                other_counter.get() - prev
            );
        }
    });

    for _ in 0..count {
        let counter = counter.clone();
        let other_client = client.clone();

        let join_handle = tokio::task::spawn(async move {
            let mut retries = 1024u64;

            loop {
                let mut client = other_client.clone();

                match dispatch_echo_job(&mut client).await {
                    Ok(v) => {
                        counter.inc_by_with_ordering(1, Ordering::SeqCst);
                        return Ok(v);
                    }
                    Err(e) => {
                        if retries == 0 {
                            return Err(anyhow!("failed to submit job"));
                        }

                        eprintln!("error = {}", e);

                        retries -= 1;
                    }
                }
            }
        });

        join_handles.push(join_handle);
    }

    eprintln!("completed all job submissions!");

    for join_handle in join_handles {
        match join_handle.await {
            Ok(Ok(v)) => output_handles.push(v),
            Ok(Err(e)) => eprintln!("task submission failed: {}", e),
            _ => {}
        }
    }

    dbg!(output_handles.len());

    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut error_counter = 0u64;

    for output_handle in output_handles {
        let output_spec = ComputeInput::Load {
            handle: output_handle,
            extensions: Default::default(),
        };

        let request: ComputeRequest = output_spec.try_into().unwrap();
        let result = client.compute(request).await;

        if result.is_err() {
            error_counter += 1;
            continue;
        }

        let output: ComputeOutput = result.unwrap().into_inner().try_into().unwrap();

        if let ComputeOutput::Loaded { data } = output {
            let data = musubi_api::types::Data::try_from(data).unwrap();
            let out: String =
                musubi_api::types::from_value(&data.values().get(0).unwrap()).unwrap();

            assert_eq!(out, "Hello world!");
        } else {
            panic!("unexpected output type");
        }
    }

    dbg!(error_counter);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_heavy_multi_execution() {
    let mut client = mitsuha_runtime_rpc::proto::channel::channel_client::ChannelClient::connect(
        "grpc://0.0.0.0:20000",
    )
    .await
    .unwrap();

    add_modules(&mut client).await;

    let mut output_handles = Vec::<String>::new();

    let count = 1u64;

    let mut join_handles = Vec::<JoinHandle<String>>::new();

    for _ in 0..count {
        let mut client = client.clone();
        let join_handle = tokio::task::spawn(async move { dispatch_loop_job(&mut client).await });

        join_handles.push(join_handle);
    }

    for join_handle in join_handles {
        output_handles.push(join_handle.await.unwrap());
    }
}
