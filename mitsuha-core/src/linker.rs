use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::{
    executor::ExecutorContext,
    kernel::CoreStub,
    module::ModuleInfo,
    types::{self, SharedAsyncMany},
};

pub struct LinkerContext {
    pub dependency_graph: HashMap<ModuleInfo, HashMap<String, ModuleInfo>>,
    pub core_stub: Arc<Box<dyn CoreStub>>,
}

impl LinkerContext {
    pub fn new(core_stub: Arc<Box<dyn CoreStub>>) -> Self {
        Self {
            dependency_graph: HashMap::new(),
            core_stub,
        }
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
