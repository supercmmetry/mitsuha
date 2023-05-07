use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    errors::Error,
    executor::ExecutorContext,
    kernel::{CoreStub, JobSpec, Kernel, StorageSpec, StubbedKernel},
    linker::{Linker, LinkerContext},
    module::{ModuleInfo, ModuleType},
    resolver::Resolver,
    symbol::Symbol,
    types,
};
use mitsuha_wasm_runtime::{
    resolver::wasmtime::WasmtimeModuleResolver,
    wasmtime::{WasmtimeLinker, WasmtimeModule},
};

use crate::{
    job_future::{JobFuture, JobState},
    system::SystemContext,
    util,
};

pub struct WasmtimeChannel<Context> {
    id: String,
    next: Option<Arc<Box<dyn ComputeChannel<Context = Context>>>>,
    linker: Arc<WasmtimeLinker>,
    kernel: Arc<Box<dyn Kernel>>,
    stub: Arc<Box<dyn CoreStub>>,
}

#[async_trait]
impl<Context> ComputeChannel for WasmtimeChannel<Context>
where
    Context: Send + SystemContext,
{
    type Context = Context;

    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())
    }

    async fn compute(&self, ctx: Context, elem: ComputeInput) -> types::Result<ComputeOutput> {
        match elem {
            ComputeInput::Run { spec } if spec.symbol.module_info.modtype == ModuleType::WASM => {
                let handle = spec.handle.clone();

                let linker = self.linker.clone();
                let stub = self.stub.clone();
                let kernel = self.kernel.clone();

                let job_state = ctx.make_job_state(
                    spec.handle.clone(),
                    JobState::ExpireAt(Utc::now() + Duration::seconds(spec.ttl as i64)),
                );
                let job_task = tokio::task::spawn(async move {
                    Self::run(linker, stub, kernel, spec).await?;
                    Ok(ComputeOutput::Completed)
                });

                let fut = JobFuture::new(handle, job_task, job_state);
                fut.await
            }
            _ => match self.next.clone() {
                Some(chan) => chan.compute(ctx, elem).await,
                None => Err(Error::ComputeChannelEOF),
            },
        }
    }

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel<Context = Context>>>) {
        self.next = Some(next);
    }
}

impl<Context> WasmtimeChannel<Context> {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/wasmtime"
    }

    pub fn new(
        kernel: Arc<Box<dyn Kernel>>,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    ) -> Self {
        // TODO: Make this configurable

        let mut config = wasmtime::Config::default();
        config.async_support(true);
        config.epoch_interruption(true);

        let engine = wasmtime::Engine::new(&config).unwrap();
        let module_resolver: Arc<Box<dyn Resolver<ModuleInfo, WasmtimeModule>>> = Arc::new(
            Box::new(WasmtimeModuleResolver::new(engine.clone(), resolver)),
        );

        let linker = Arc::new(WasmtimeLinker::new(module_resolver, engine).unwrap());

        let id = format!(
            "{}/{}",
            Self::get_identifier_type(),
            util::generate_random_id()
        );

        let stub: Arc<Box<dyn CoreStub>> = Arc::new(Box::new(StubbedKernel::new(kernel.clone())));

        Self {
            id,
            next: None,
            linker,
            kernel,
            stub,
        }
    }

    async fn run(
        linker: Arc<WasmtimeLinker>,
        stub: Arc<Box<dyn CoreStub>>,
        kernel: Arc<Box<dyn Kernel>>,
        spec: JobSpec,
    ) -> types::Result<()> {
        let symbol = spec.symbol.clone();
        let module_info = symbol.module_info.clone();

        let mut linker_ctx = LinkerContext::new(stub);

        linker.load(&mut linker_ctx, &module_info).await?;

        let exec_ctx = Arc::new(linker.link(&mut linker_ctx, &module_info).await?);

        let input = kernel.load_data(spec.input_handle).await?;

        let output = exec_ctx.call(&symbol, input).await?;

        kernel
            .store_data(StorageSpec {
                handle: spec.output_handle,
                data: output,
                ttl: spec.ttl,
                extensions: Default::default(),
            })
            .await?;

        Ok(())
    }
}
