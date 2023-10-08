use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use mitsuha_core_types::{module::ModuleInfo, kernel::JobSpec};

use crate::{
    constants::Constants,
    executor::ExecutorContext,
    kernel::KernelBinding,
    resolver::Resolver,
    types,
};

pub struct LinkerContext {
    pub dependency_graph: HashMap<ModuleInfo, HashMap<String, ModuleInfo>>,
    pub kernel_binding: Arc<Box<dyn KernelBinding>>,
    pub module_resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    pub extensions: HashMap<String, String>,
}

impl LinkerContext {
    pub fn new(
        kernel_binding: Arc<Box<dyn KernelBinding>>,
        resolver: Arc<Box<dyn Resolver<ModuleInfo, Vec<u8>>>>,
    ) -> Self {
        Self {
            dependency_graph: HashMap::new(),
            kernel_binding,
            module_resolver: resolver,
            extensions: Default::default(),
        }
    }

    pub fn with_extension(mut self, key: String, value: String) -> Self {
        self.extensions.insert(key, value);
        self
    }

    pub fn with_extensions(mut self, extensions: HashMap<String, String>) -> Self {
        self.extensions = extensions;
        self
    }

    pub fn load_extensions_from_job(&mut self, spec: &JobSpec) {
        self.extensions
            .insert(Constants::JobHandle.to_string(), spec.handle.clone());
    }
}

#[async_trait]
pub trait Linker {
    async fn load(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<()>;

    async fn link(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> types::Result<ExecutorContext>;
}
