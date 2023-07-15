use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    kernel::{JobSpec, Kernel, KernelBinding, KernelBridge},
    linker::{Linker, LinkerContext},
    module::{ModuleInfo, ModuleType},
    resolver::Resolver,
    types,
};
use mitsuha_wasm_runtime::{
    resolver::wasmtime::WasmtimeModuleResolver,
    wasmtime::{WasmtimeLinker, WasmtimeModule},
};
use tokio::{sync::RwLock, task::JoinHandle};

use crate::{
    context::ChannelContext,
    job_controller::{JobController, JobState},
    system::JobContext,
    util::{self, make_output_storage_spec},
    WrappedComputeChannel,
};

pub struct WasmtimeChannel {
    id: String,
    next: Arc<RwLock<Option<Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>>>>,
    linker: Arc<WasmtimeLinker>,
    kernel: Arc<Box<dyn Kernel>>,
    stub: Arc<Box<dyn KernelBinding>>,
}

#[async_trait]
impl ComputeChannel for WasmtimeChannel {
    type Context = ChannelContext;

    fn id(&self) -> String {
        self.id.clone()
    }

    async fn compute(
        &self,
        ctx: ChannelContext,
        elem: ComputeInput,
    ) -> types::Result<ComputeOutput> {
        match elem {
            ComputeInput::Run { spec } if spec.symbol.module_info.modtype == ModuleType::WASM => {
                let handle = spec.handle.clone();

                let linker = self.linker.clone();
                let stub = self.stub.clone();
                let kernel = self.kernel.clone();

                let (updater, updation_target) = tokio::sync::mpsc::channel::<JobState>(16);
                let (status_updater, status_reader) = tokio::sync::mpsc::channel::<JobState>(16);

                let job_context = JobContext::new(
                    handle.clone(),
                    updater.clone(),
                    status_reader,
                    JobState::ExpireAt(Utc::now() + Duration::seconds(spec.ttl as i64)),
                )
                .await;

                ctx.register_job_context(handle.clone(), job_context);

                let job_task_spec = spec.clone();
                let job_task: JoinHandle<types::Result<()>> = tokio::task::spawn(async move {
                    Self::run(linker, stub, kernel, job_task_spec).await?;
                    Ok(())
                });

                let job_ctrl = JobController::new(spec.clone(), job_task, ctx.clone());

                let result = job_ctrl
                    .run(handle.clone(), updater, updation_target, status_updater)
                    .await;

                if result.is_err() {
                    log::error!(
                        "job execution failed with error: {:?}",
                        result.as_ref().err().unwrap()
                    );
                }

                // TODO: Store JobStatus in storage.

                // ctx.deregister_job_context(&handle);

                result
            }
            _ => match self.next.read().await.clone() {
                Some(chan) => chan.compute(ctx, elem).await,
                None => Err(Error::ComputeChannelEOF),
            },
        }
    }

    async fn connect(&self, next: Arc<Box<dyn ComputeChannel<Context = ChannelContext>>>) {
        *self.next.write().await = Some(next);
    }
}

impl WasmtimeChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/wasmtime"
    }

    pub fn new(
        kernel: Arc<Box<dyn Kernel>>,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    ) -> WrappedComputeChannel<Self> {
        // TODO: Make this configurable

        let mut config = wasmtime::Config::default();
        config.async_support(true);
        config.epoch_interruption(true);

        let engine = wasmtime::Engine::new(&config).unwrap();
        let module_resolver: Arc<Box<dyn Resolver<ModuleInfo, WasmtimeModule>>> = Arc::new(
            Box::new(WasmtimeModuleResolver::new(engine.clone(), resolver)),
        );

        let linker = Arc::new(WasmtimeLinker::new(module_resolver, engine).unwrap());

        let stub: Arc<Box<dyn KernelBinding>> =
            Arc::new(Box::new(KernelBridge::new(kernel.clone())));

        WrappedComputeChannel::new(Self {
            id: Self::get_identifier_type().to_string(),
            next: Arc::new(RwLock::new(None)),
            linker,
            kernel,
            stub,
        })
    }

    async fn run(
        linker: Arc<WasmtimeLinker>,
        stub: Arc<Box<dyn KernelBinding>>,
        kernel: Arc<Box<dyn Kernel>>,
        spec: JobSpec,
    ) -> types::Result<()> {
        let symbol = spec.symbol.clone();
        let module_info = symbol.module_info.clone();

        let mut linker_ctx = LinkerContext::new(stub);

        linker.load(&mut linker_ctx, &module_info).await?;

        let exec_ctx = Arc::new(linker.link(&mut linker_ctx, &module_info).await?);

        let input = kernel.load_data(spec.input_handle.clone()).await?;

        let output = exec_ctx.call(&symbol, input).await?;

        kernel
            .store_data(make_output_storage_spec(spec, output)?)
            .await?;

        Ok(())
    }
}
