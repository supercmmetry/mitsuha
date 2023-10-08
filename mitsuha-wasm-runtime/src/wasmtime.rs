use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::FutureExt;
use mitsuha_core::{
    errors::Error,
    executor::ExecutorContext,
    kernel::KernelBinding,
    linker::{Linker, LinkerContext},
    module::Module,
    resolver::Resolver,
    types::{self, SharedAsyncMany}, symbol::SymbolExt,
};
use mitsuha_core_types::{module::{ModuleInfo, ModuleType}, symbol::Symbol};
use num_traits::cast::FromPrimitive;

use crate::{constants::Constants, resolver::wasmtime::WasmtimeModuleResolver};

#[derive(Clone)]
pub struct WasmMetadata {
    spec: musubi_api::types::Spec,
}

impl WasmMetadata {
    pub fn new(mut data: &[u8]) -> anyhow::Result<Self> {
        let mut musubi_info_sections: Vec<Vec<u8>> = vec![];
        let mut musubi_header_sections: Vec<Vec<u8>> = vec![];
        let mut parser = wasmparser::Parser::new(0);

        loop {
            match parser.parse(data, true)? {
                wasmparser::Chunk::NeedMoreData(_) => {
                    return Err(anyhow::anyhow!(
                        "unexpected error occured while parsing WASM"
                    ))
                }
                wasmparser::Chunk::Parsed { payload, consumed } => {
                    data = &data[consumed..];

                    match payload {
                        wasmparser::Payload::CustomSection(s) => match s.name() {
                            ".msbi" | "msbi,unstable" => {
                                musubi_info_sections.push(s.data().to_vec())
                            }
                            ".msbh" | "msbh,unstable" => {
                                musubi_header_sections.push(s.data().to_vec())
                            }
                            _ => {}
                        },
                        wasmparser::Payload::End { .. } => {
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        if musubi_info_sections.len() == 0 {
            return Err(anyhow::anyhow!(
                "could not find section for musubi_info in WASM binary"
            ));
        }

        if musubi_info_sections.len() > 1 {
            return Err(anyhow::anyhow!(
                "duplicate musubi_info sections found in WASM binary"
            ));
        }

        let raw = musubi_info_sections.get(0).unwrap().clone();
        let info: musubi_api::types::Info = serde_json::from_slice(raw.as_slice())?;

        let mut headers: HashMap<musubi_api::types::Header, Vec<u8>> = Default::default();
        for section in musubi_header_sections {
            if let Some(key) = section.get(0) {
                if let Some(header) = musubi_api::types::Header::from_u8(*key) {
                    headers.insert(header, section[1..].to_vec());
                }
            }
        }

        let spec = musubi_api::types::Spec { info, headers };

        Ok(Self { spec })
    }

    pub fn get_musubi_spec(&self) -> musubi_api::types::Spec {
        self.spec.clone()
    }
}

#[derive(Clone)]
pub struct WasmtimeModule {
    inner: wasmtime::Module,
    info: ModuleInfo,
    metadata: WasmMetadata,
}

impl Module<wasmtime::Module> for WasmtimeModule {
    fn get_info(&self) -> ModuleInfo {
        self.info.clone()
    }

    fn inner(&self) -> &wasmtime::Module {
        &self.inner
    }

    fn get_musubi_spec(&mut self) -> anyhow::Result<musubi_api::types::Spec> {
        Ok(self.metadata.get_musubi_spec())
    }
}

impl WasmtimeModule {
    pub fn new(
        data: Vec<u8>,
        module_info: ModuleInfo,
        engine: &wasmtime::Engine,
    ) -> types::Result<Self> {
        let metadata = WasmMetadata::new(data.as_slice()).map_err(|e| Error::WasmError {
            message: "failed to parse musubi metadata".to_string(),
            inner: module_info.clone(),
            source: e,
        })?;

        let module = wasmtime::Module::from_binary(&engine, data.as_slice()).map_err(|e| {
            Error::ModuleLoadFailed {
                message: "failed to load wasm module".to_string(),
                inner: module_info.clone(),
                source: e,
            }
        })?;

        Ok(Self {
            metadata,
            inner: module,
            info: module_info,
        })
    }
}

#[derive(Clone)]
pub struct WasmtimeContext {
    kernel_binding: Arc<Box<dyn KernelBinding>>,
    instance: SharedAsyncMany<Option<wasmtime::Instance>>,
}

impl WasmtimeContext {
    pub fn new(kernel_binding: Arc<Box<dyn KernelBinding>>) -> Self {
        Self {
            kernel_binding,
            instance: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    pub async fn set_instance(&self, instance: wasmtime::Instance) {
        *self.instance.write().await = Some(instance);
    }

    pub fn get_instance(&self) -> SharedAsyncMany<Option<wasmtime::Instance>> {
        self.instance.clone()
    }

    pub async fn call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        self.kernel_binding.run(symbol, input).await
    }
}

pub struct WasmtimeLinker {
    engine: wasmtime::Engine,
    module_cache: moka::future::Cache<(String, ModuleInfo), WasmtimeModule>,
    ticker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WasmtimeLinker {
    pub fn new(engine: wasmtime::Engine) -> types::Result<Self> {
        let mut obj = Self {
            module_cache: moka::future::Cache::new(16),
            engine,
            ticker_handle: None,
        };

        obj.start_ticker();
        Ok(obj)
    }

    fn start_ticker(&mut self) {
        let engine = self.engine.clone();

        self.ticker_handle = Some(tokio::task::spawn_blocking(move || loop {
            tracing::debug!("incrementing wasmtime engine epoch");
            engine.increment_epoch();
            std::thread::sleep(Duration::from_millis(1000));
        }));
    }

    async fn fetch_module(
        &self,
        ctx: &LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<WasmtimeModule> {
        let resolver_prefix = ctx
            .extensions
            .get(&mitsuha_core::constants::Constants::ModuleResolverPrefix.to_string())
            .cloned()
            .unwrap_or(module_info.get_identifier());

        let cache_key = (resolver_prefix, module_info.clone());
        if let Some(v) = self.module_cache.get(&cache_key) {
            return Ok(v);
        }

        let module_resolver =
            WasmtimeModuleResolver::new(self.engine.clone(), ctx.module_resolver.clone());

        let module = module_resolver.resolve(module_info).await?;

        self.module_cache.insert(cache_key, module.clone()).await;

        Ok(module)
    }

    fn construct_error(error: &str) -> musubi_api::types::Data {
        musubi_api::DataBuilder::new()
            .add(musubi_api::types::Value::Error {
                code: Constants::RuntimeModuleName.to_string(),
                message: error.to_string(),
            })
            .build()
    }

    async fn emit_wasm32_import_error<T>(
        error: &str,
        instance: &wasmtime::Instance,
        store: impl wasmtime::AsContextMut<Data = T>,
        output_ptr: i32,
    ) -> i64
    where
        T: Send,
    {
        let data = Self::construct_error(error);

        let result = musubi_wasmtime::import::run_imported_function32_write_output(
            instance,
            store,
            output_ptr as usize,
            data.try_into().unwrap(),
        )
        .await;

        if result.is_err() {
            tracing::error!(
                "failed to emit error back to wasm runtime. error: {}",
                result.err().unwrap()
            );
            return 0;
        }

        result.unwrap()
    }

    pub async fn run_wasm32_import<'a>(
        mut caller: wasmtime::Caller<'a, WasmtimeContext>,
        symbol: Symbol,
        input_ptr: i32,
        input_len: i64,
        output_ptr: i32,
    ) -> i64 {
        let instance = caller.data().get_instance();

        let read_result = musubi_wasmtime::import::run_imported_function_read_input(
            instance.read().await.as_ref().unwrap(),
            &mut caller,
            input_ptr as usize,
            input_len,
        )
        .await;

        if read_result.is_err() {
            return Self::emit_wasm32_import_error(
                "failed to read input from memory",
                instance.read().await.as_ref().unwrap(),
                &mut caller,
                output_ptr,
            )
            .await;
        }

        let result = caller.data().call(&symbol, read_result.unwrap()).await;

        if result.is_err() {
            return Self::emit_wasm32_import_error(
                format!(
                    "failed to call symbol: {:?}, error: {}",
                    symbol.clone(),
                    result.err().unwrap()
                )
                .as_str(),
                instance.read().await.as_ref().unwrap(),
                &mut caller,
                output_ptr,
            )
            .await;
        }

        let write_result = musubi_wasmtime::import::run_imported_function32_write_output(
            instance.read().await.as_ref().unwrap(),
            &mut caller,
            output_ptr as usize,
            result.unwrap(),
        )
        .await;

        if write_result.is_err() {
            return Self::emit_wasm32_import_error(
                "failed to write output from memory",
                instance.read().await.as_ref().unwrap(),
                &mut caller,
                output_ptr,
            )
            .await;
        }

        write_result.unwrap()
    }

    async fn run_wasm32_export(
        context: WasmtimeContext,
        shared_store: SharedAsyncMany<Option<wasmtime::Store<WasmtimeContext>>>,
        function: String,
        input: Vec<u8>,
    ) -> Vec<u8> {
        let mut guard = shared_store.write().await;

        let store = guard.as_mut().unwrap();

        let output_result = musubi_wasmtime::export::run_exported_function32_read_output(
            context.get_instance().read().await.as_ref().unwrap(),
            store,
            function.as_str(),
            input,
        )
        .await;

        if let Err(e) = output_result {
            return Self::construct_error(
                format!(
                    "failed to run exported function: {} with error: {}",
                    function, e
                )
                .as_str(),
            )
            .try_into()
            .unwrap();
        }

        output_result.unwrap()
    }
}

#[async_trait]
impl Linker for WasmtimeLinker {
    async fn load(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<()> {
        let mut module = self.fetch_module(&context, &module_info).await?;
        let mut spec = module
            .get_musubi_spec()
            .map_err(|e| Error::LinkerLoadFailed {
                message: "failed to load musubi specification".to_string(),
                target: module_info.clone(),
                source: e,
            })?;

        let mut dependencies = HashMap::new();

        for dependency in spec.info.deps.drain(..) {
            dependencies.insert(dependency.name.clone(), dependency.into());
        }

        context
            .dependency_graph
            .insert(module_info.clone(), dependencies);

        Ok(())
    }

    async fn link(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<ExecutorContext> {
        let mut module = self.fetch_module(&context, &module_info).await?;

        let wasmtime_context = WasmtimeContext::new(context.kernel_binding.clone());

        let mut store = wasmtime::Store::new(&self.engine, wasmtime_context.clone());

        store.epoch_deadline_async_yield_and_update(1);

        let mut linker = wasmtime::Linker::new(&self.engine);

        let dep_map =
            context
                .dependency_graph
                .get(&module_info)
                .ok_or(Error::LinkerLinkFailed {
                    message: "cannot find dependency in context".to_string(),
                    target: module_info.clone(),
                    source: anyhow::anyhow!(""),
                })?;

        let bitness = module
            .get_musubi_spec()
            .map_err(|e| Error::LinkerLinkFailed {
                message: "failed to load musubi specification".to_string(),
                target: module_info.clone(),
                source: e,
            })?
            .get_bitness()
            .map_err(|e| Error::LinkerLinkFailed {
                message: "failed to load WASM bitness".to_string(),
                target: module_info.clone(),
                source: e,
            })?;

        if bitness != musubi_api::types::Bitness::X32 {
            return Err(Error::LinkerLinkFailed {
                message: "only 32 bit wasm is supported".to_string(),
                target: module_info.clone(),
                source: anyhow::anyhow!(""),
            });
        }

        for import in module.inner().imports() {
            // Ignore if the import is not a musubi import
            let result = Symbol::parse_musubi_function_symbol(import.name());
            if result.is_err() {
                continue;
            }

            let (module_name, _) = result.unwrap();

            let child_info = dep_map.get(&module_name).ok_or(Error::LinkerLinkFailed {
                message: format!(
                    "cannot find dependency in context: {}",
                    module_name.as_str()
                ),
                target: module_info.clone(),
                source: anyhow::anyhow!(""),
            })?;

            // The import symbol which we will use to get the imported function from executor context
            let symbol = Symbol::from_imported_function_symbol(
                import.name(),
                ModuleType::WASM,
                child_info.version.as_str(),
            )
            .map_err(|e| Error::LinkerLinkFailed {
                message: format!("failed to import musubi symbol: {}", import.name()),
                target: module_info.clone(),
                source: e,
            })?;

            let imported_symbol = symbol.clone();
            let imported_func = wasmtime::Func::wrap3_async(
                &mut store,
                move |caller: wasmtime::Caller<'_, WasmtimeContext>,
                      input_ptr: i32,
                      input_len: i64,
                      output_ptr: i32| {
                    let fut = Self::run_wasm32_import(
                        caller,
                        imported_symbol.clone(),
                        input_ptr,
                        input_len,
                        output_ptr,
                    );

                    Box::new(fut)
                },
            );

            tracing::debug!("importing symbol {:?}", symbol.clone());

            linker
                .define(&store, module_name.as_str(), import.name(), imported_func)
                .map_err(|e| Error::LinkerLinkFailed {
                    message: "failed to define import in wasmtime module".to_string(),
                    target: module_info.clone(),
                    source: e,
                })?;
        }

        let instance = linker
            .instantiate_async(&mut store, module.inner())
            .await
            .map_err(|e| Error::LinkerLinkFailed {
                message: format!("failed to instantiate wasmtime module with imports"),
                target: module_info.clone(),
                source: e,
            })?;

        wasmtime_context.set_instance(instance).await;

        let shared_store = Arc::new(tokio::sync::RwLock::new(Some(store)));

        let mut executor_context = ExecutorContext::new();

        for export in module.inner().exports() {
            // Ignore if the export is not a musubi export
            let result = Symbol::parse_musubi_function_symbol(export.name());
            if result.is_err() {
                continue;
            }

            let (module_name, _) = result.unwrap();

            // Ignore export if spoofing was detected.
            if module_name != module_info.name {
                tracing::warn!(
                    "detected module spoofing! exported symbol: '{}', expected module name: '{}'",
                    export.name(),
                    module_info.name
                );
                continue;
            }

            // The export symbol which we will use to get the exported function from executor context
            let symbol = Symbol::from_exported_function_symbol(
                export.name(),
                ModuleType::WASM,
                module_info.version.as_str(),
            )
            .map_err(|e| Error::LinkerLinkFailed {
                message: format!("failed to export musubi symbol: {}", export.name()),
                target: module_info.clone(),
                source: e,
            })?;

            tracing::debug!("exporting symbol: {:?}", symbol.clone());

            let exported_store = shared_store.clone();
            let exported_context = wasmtime_context.clone();
            let function_name = export.name().to_string();

            let exported_func = move |input: Vec<u8>| {
                Self::run_wasm32_export(
                    exported_context.clone(),
                    exported_store.clone(),
                    function_name.clone(),
                    input,
                )
                .boxed()
            };

            executor_context
                .add_symbol(symbol, Arc::new(tokio::sync::RwLock::new(exported_func)))?;
        }

        Ok(executor_context)
    }
}

impl Drop for WasmtimeLinker {
    fn drop(&mut self) {
        if let Some(ticker_handle) = self.ticker_handle.as_mut() {
            ticker_handle.abort();
        }
    }
}
