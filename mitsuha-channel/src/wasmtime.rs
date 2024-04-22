use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use futures::stream::{AbortHandle, Abortable};
use mitsuha_core::channel::ChannelContext;
use mitsuha_core::errors::ToUnknownErrorResult;
use mitsuha_core::job::ctrl::JobController;
use mitsuha_core::job::ctx::{JobContext, JobState};
use mitsuha_core::job::mgr::JobManagerProvider;
use mitsuha_core::{
    channel::ComputeChannel,
    constants::Constants,
    errors::Error,
    kernel::{JobSpecExt, Kernel, KernelBinding, KernelBridge},
    linker::{Linker, LinkerContext},
    resolver::{blob::BlobResolver, Resolver},
    types,
};
use mitsuha_core_types::{
    channel::{ComputeInput, ComputeOutput},
    kernel::JobSpec,
    module::{ModuleInfo, ModuleType},
};
use mitsuha_wasm_runtime::wasmtime::WasmtimeLinker;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::Instrument;

use crate::{
    util::{self, make_output_storage_spec},
    NextComputeChannel, WrappedComputeChannel,
};

pub struct WasmtimeChannel {
    id: String,
    next: NextComputeChannel<ChannelContext>,
    linker: Arc<WasmtimeLinker>,
    kernel: Arc<Box<dyn Kernel>>,
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
                let job_handle_span = util::make_job_span("wasmtime");
                let _job_handle_span_entered = job_handle_span.enter();

                let handle = spec.handle.clone();

                let raw_module_resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>> =
                    Arc::new(Box::new(
                        BlobResolver::new(ctx.get_channel_start())
                            .with_extensions(spec.extensions.clone()),
                    ));

                let linker = self.linker.clone();

                let kernel_binding: Arc<Box<dyn KernelBinding>> = Arc::new(Box::new(
                    KernelBridge::new(self.kernel.clone(), spec.make_kernel_bridge_metadata()?),
                ));

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

                ctx.get_job_mgr()
                    .await
                    .register_job_context(handle.clone(), job_context);

                let job_task_spec = spec.clone();

                let (abort_handle, abort_registration) = AbortHandle::new_pair();

                let job_task_future = async move {
                    let abortable_future = Abortable::new(
                        async move {
                            Self::run(
                                linker,
                                kernel_binding,
                                kernel,
                                raw_module_resolver,
                                job_task_spec,
                            )
                            .await?;
                            types::Result::Ok(())
                        },
                        abort_registration,
                    );

                    abortable_future.await.to_unknown_err_result()??;

                    Ok(())
                };

                let job_task: JoinHandle<types::Result<()>> =
                    tokio::task::spawn(job_task_future.instrument(tracing::Span::current()));

                let mut job_ctrl = JobController::new(
                    spec.clone(),
                    job_task,
                    abort_handle,
                    ctx.get_channel_start().clone(),
                    ctx.clone(),
                );

                ctx.get_job_mgr()
                    .await
                    .inject_post_job_hooks(&mut job_ctrl)
                    .await;

                let consolidated_task_future = async move {
                    let result = job_ctrl
                        .run(
                            handle.clone(),
                            &ctx,
                            updater,
                            updation_target,
                            status_updater,
                        )
                        .await;

                    if result.is_err() {
                        tracing::error!(
                            "job execution failed with error: {:?}",
                            result.as_ref().err().unwrap()
                        );
                    }

                    ctx.get_job_mgr().await.deregister_job_context(&handle);

                    result
                };

                let consolidated_task = tokio::task::spawn(
                    consolidated_task_future.instrument(tracing::Span::current()),
                );

                if let Some("true") = spec
                    .extensions
                    .get(&Constants::JobChannelAwait.to_string())
                    .map(|e| e.as_str())
                {
                    consolidated_task.await.to_unknown_err_result()?
                } else {
                    Ok(ComputeOutput::Submitted)
                }
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

    pub fn new(kernel: Arc<Box<dyn Kernel>>) -> WrappedComputeChannel<Self> {
        // TODO: Make this configurable

        let mut config = wasmtime::Config::default();
        config.async_support(true);
        config.epoch_interruption(true);

        let engine = wasmtime::Engine::new(&config).unwrap();

        let linker = Arc::new(WasmtimeLinker::new(engine).unwrap());

        WrappedComputeChannel::new(Self {
            id: Self::get_identifier_type().to_string(),
            next: Arc::new(RwLock::new(None)),
            linker,
            kernel,
        })
    }

    async fn run(
        linker: Arc<WasmtimeLinker>,
        kernel_binding: Arc<Box<dyn KernelBinding>>,
        kernel: Arc<Box<dyn Kernel>>,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
        spec: JobSpec,
    ) -> types::Result<()> {
        let symbol = spec.symbol.clone();
        let module_info = symbol.module_info.clone();

        let mut linker_ctx = LinkerContext::new(kernel_binding, resolver);

        linker_ctx.load_extensions_from_job(&spec);

        linker.load(&mut linker_ctx, &module_info).await?;

        let exec_ctx = Arc::new(linker.link(&mut linker_ctx, &module_info).await?);

        let input = kernel
            .load_data(spec.input_handle.clone(), spec.extensions.clone())
            .await?;

        let output = exec_ctx.call(&symbol, input).await?;

        kernel
            .store_data(make_output_storage_spec(spec, output)?)
            .await?;

        Ok(())
    }
}
