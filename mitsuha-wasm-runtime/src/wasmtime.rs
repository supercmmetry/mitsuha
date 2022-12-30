use std::{collections::HashMap, sync::{Arc, RwLock}};

use async_trait::async_trait;
use mitsuha_core::{
    errors::Error,
    module::{Module, ModuleInfo},
    types::{self, SharedMany}, linker::{Linker, LinkerContext}, resolver::Resolver, executor::ExecutorContext, symbol::Symbol,
};
use musubi_api::types::Data;
use num_traits::cast::FromPrimitive;

use lazy_static::lazy_static;

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
    executor_context: SharedMany<ExecutorContext>,
    instance: SharedMany<Option<wasmtime::Instance>>,
    store: SharedMany<Option<wasmtime::Store<Self>>>,
}

impl WasmtimeContext {
    pub fn new(executor_context: SharedMany<ExecutorContext>) -> Self {
        Self {
            executor_context,
            instance: Arc::new(RwLock::new(None)),
            store: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_instance(&self, instance: wasmtime::Instance) {
        *self.instance.write().unwrap() = Some(instance);
    }

    pub fn set_store(&self, store: wasmtime::Store<Self>) {
        *self.store.write().unwrap() = Some(store);
    }

    pub fn get_instance(&self) -> &wasmtime::Instance {
        self.instance.read().unwrap().as_ref().unwrap()
    }

    pub fn get_store(&self) -> &wasmtime::Store<Self> {
        self.store.read().unwrap().as_ref().unwrap()
    }

    pub fn get_store_mut(&self) -> &mut wasmtime::Store<Self> {
        self.store.write().unwrap().as_mut().unwrap()
    }

    pub async fn call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        self.executor_context.read().unwrap().call(symbol, input).await
    }
}

pub struct WasmtimeLinker {
    engine: wasmtime::Engine,
    resolver: SharedMany<dyn Resolver<ModuleInfo, WasmtimeModule>>,
}

impl WasmtimeLinker {
    pub fn new(resolver: SharedMany<dyn Resolver<ModuleInfo, WasmtimeModule>>) -> Self {
        Self { 
            resolver,
            engine: wasmtime::Engine::default(),
        }
    }

    async fn fetch_module(
        &self,
        module_info: &ModuleInfo,
    ) -> types::Result<WasmtimeModule> {
        self.resolver.read().unwrap().resolve(module_info).await
    }

    pub fn run_wasm32_import(
        mut caller: wasmtime::Caller<'_, WasmtimeContext>,
        input_ptr: i32,
        input_len: i64,
        output_ptr: i32,
    ) -> i64 {
        caller.data().executor_context.read().unwrap().get_kernel().run_task(symbol, input)
        wasmtime::Func::wrap

        // musubi_wasmtime::import::run_imported_function(
        //     caller.data().,
        //     store, 
        //     input_ptr, 
        //     input_len, 
        //     output_ptr, 
        //     inner_func
        // )
    }
}

#[async_trait(?Send)]
impl Linker for WasmtimeLinker {
    async fn load(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> anyhow::Result<()> {
        let mut module = self.fetch_module(&module_info).await?;
        let mut spec = module.get_musubi_spec()?;

        let mut dependencies = HashMap::new();

        for dependency in spec.info.deps.drain(..) {
            dependencies.insert(dependency.name.clone(), dependency.into());
        }

        context.dependency_graph.insert(module_info.clone(), dependencies);

        Ok(())
    }

    async fn link(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> anyhow::Result<()> {
        let mut module = self.fetch_module(&module_info).await?;
        let mut wasmtime_context = WasmtimeContext::new(context.executor_context.clone());
        let mut store = wasmtime::Store::new(&self.engine, wasmtime_context.clone());

        let dep_map =
            context
                .dependency_graph
                .get(&module_info)
                .ok_or(Error::LinkerLinkFailed {
                    message: "cannot find dependency in context".to_string(),
                    target: module_info.clone(),
                    source: anyhow::anyhow!(""),
                })?;

        let imports = vec![];

        for (module_name, module_info) in dep_map.iter() {
            let func = wasmtime::Func::wrap(store, |mut caller: wasmtime::Caller<'_, SharedMany<ExecutorContext>>, input: Vec<u8>| {
                vec![]
            });
        }
        Ok(())
    }
}

