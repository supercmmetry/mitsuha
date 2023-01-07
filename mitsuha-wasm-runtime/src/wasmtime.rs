use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use mitsuha_core::{
    errors::Error,
    executor::ExecutorContext,
    linker::{Linker, LinkerContext},
    module::{Module, ModuleInfo, ModuleType},
    resolver::Resolver,
    symbol::Symbol,
    types::{self, SharedMany, SharedAsyncMany}, kernel::Kernel,
};
use musubi_api::types::Data;
use num_traits::cast::FromPrimitive;

use lazy_static::lazy_static;

use crate::constants::Constants;

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
                            ".musubi_info" => musubi_info_sections.push(s.data().to_vec()),
                            ".musubi_header" => musubi_header_sections.push(s.data().to_vec()),
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
                "could not find section .musubi_info in WASM binary"
            ));
        }

        if musubi_info_sections.len() > 1 {
            return Err(anyhow::anyhow!(
                "duplicate .musubi_info sections found in WASM binary"
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
    pub fn new(data: Vec<u8>, module_info: ModuleInfo) -> types::Result<Self> {
        let metadata = WasmMetadata::new(data.as_slice()).map_err(|e| Error::WasmError {
            message: "failed to parse musubi metadata".to_string(),
            inner: module_info.clone(),
            source: e,
        })?;

        let engine = wasmtime::Engine::default();
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
    kernel: SharedAsyncMany<dyn Kernel>,
    instance: SharedMany<Option<wasmtime::Instance>>,
}

impl WasmtimeContext {
    pub fn new(kernel: SharedAsyncMany<dyn Kernel>) -> Self {
        Self {
            kernel,
            instance: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_instance(&self, instance: wasmtime::Instance) {
        *self.instance.write().unwrap() = Some(instance);
    }

    pub fn get_instance(&self) -> SharedMany<Option<wasmtime::Instance>> {
        self.instance.clone()
    }

    pub async fn call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        self.kernel.read().await.run_task(symbol, input).await
    }
}

pub struct WasmtimeLinker {
    engine: wasmtime::Engine,
    resolver: SharedMany<dyn Resolver<ModuleInfo, WasmtimeModule>>,
    has_ticker_started: AtomicBool,
}

impl WasmtimeLinker {
    pub fn new(
        resolver: SharedMany<dyn Resolver<ModuleInfo, WasmtimeModule>>,
    ) -> types::Result<Self> {
        let mut config = wasmtime::Config::default();
        config.async_support(true);
        config.epoch_interruption(true);

        let engine = wasmtime::Engine::new(&config).map_err(|e| Error::Unknown { source: e })?;

        Ok(Self {
            resolver,
            engine,
            has_ticker_started: AtomicBool::new(false),
        })
    }

    fn start_ticker(&mut self) {
        let value = self.has_ticker_started.get_mut();
        if *value {
            return;
        }

        let engine = self.engine.clone();

        tokio::task::spawn_blocking(move || loop {
            engine.increment_epoch();

            // TODO: Make this configurable
            tokio::time::sleep(Duration::from_secs(1));
        });

        *value = true;
    }

    async fn fetch_module(&self, module_info: &ModuleInfo) -> types::Result<WasmtimeModule> {
        self.resolver.read().unwrap().resolve(module_info).await
    }

    fn construct_error(error: &str) -> musubi_api::types::Data {
        musubi_api::DataBuilder::new()
            .add(musubi_api::types::Value::Error {
                code: Constants::RuntimeModuleName.to_string(),
                message: error.to_string(),
            })
            .build()
    }

    fn emit_wasm32_import_error(
        error: &str,
        instance: &wasmtime::Instance,
        store: impl wasmtime::AsContextMut,
        output_ptr: i32,
    ) -> i64 {
        let data = Self::construct_error(error);

        let result = musubi_wasmtime::import::run_imported_function32_write_output(
            instance,
            store,
            output_ptr as *mut u8,
            data.try_into().unwrap(),
        );

        if result.is_err() {
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

        let read_result = musubi_wasmtime::import::run_imported_function32_read_input(
            instance.read().unwrap().as_ref().unwrap(),
            &mut caller,
            input_ptr as *mut u8,
            input_len,
        );

        if read_result.is_err() {
            return Self::emit_wasm32_import_error(
                "failed to read input from memory",
                instance.read().unwrap().as_ref().unwrap(),
                &mut caller,
                output_ptr,
            );
        }

        let result = caller.data().call(&symbol, read_result.unwrap()).await;

        if result.is_err() {
            return Self::emit_wasm32_import_error(
                format!("failed to call symbol: {:?}", symbol.clone()).as_str(),
                instance.read().unwrap().as_ref().unwrap(),
                &mut caller,
                output_ptr,
            );
        }

        let write_result = musubi_wasmtime::import::run_imported_function32_write_output(
            instance.read().unwrap().as_ref().unwrap(),
            &mut caller,
            output_ptr as *mut u8,
            result.unwrap(),
        );

        if write_result.is_err() {
            return Self::emit_wasm32_import_error(
                "failed to write output from memory",
                instance.read().unwrap().as_ref().unwrap(),
                &mut caller,
                output_ptr,
            );
        }

        write_result.unwrap()
    }

    async fn run_wasm32_export(
        context: WasmtimeContext,
        shared_store: SharedMany<Option<wasmtime::Store<WasmtimeContext>>>,
        function: String,
        input: Vec<u8>,
    ) -> Vec<u8> {
        let mut guard = shared_store.write().unwrap();

        let store = guard.as_mut().unwrap();

        let output_result = musubi_wasmtime::export::run_exported_function32_read_output(
            context.get_instance().read().unwrap().as_ref().unwrap(), store, function.as_str(), input,
        );

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

#[async_trait(?Send)]
impl Linker for WasmtimeLinker {
    async fn load(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<()> {
        let mut module = self.fetch_module(&module_info).await?;
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
        &mut self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<ExecutorContext> {
        let mut module = self.fetch_module(&module_info).await?;

        let wasmtime_context = WasmtimeContext::new(context.kernel.clone());

        let mut store = wasmtime::Store::new(&self.engine, wasmtime_context.clone());

        store.epoch_deadline_async_yield_and_update(1);
        self.start_ticker();

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
                move |mut caller: wasmtime::Caller<'_, WasmtimeContext>,
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

            linker.define(module_name.as_str(), import.name(), imported_func);
        }

        let instance =
            linker
                .instantiate(&mut store, module.inner())
                .map_err(|e| Error::LinkerLinkFailed {
                    message: format!("failed to instantiate wasmtime module with imports"),
                    target: module_info.clone(),
                    source: e,
                })?;


        wasmtime_context.set_instance(instance);

        let mut shared_store = Arc::new(RwLock::new(Some(store)));

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
                // TODO: Add log statement
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

            executor_context.add_symbol(symbol, Arc::new(RwLock::new(exported_func)))?;
        }

        Ok(executor_context)
    }
}
