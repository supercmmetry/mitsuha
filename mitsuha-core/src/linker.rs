use std::collections::HashMap;

use async_trait::async_trait;

use crate::{module::ModuleInfo, types::SharedMany, executor::ExecutorContext, kernel::Kernel};

pub struct LinkerContext {
    pub dependency_graph: HashMap<ModuleInfo, HashMap<String, ModuleInfo>>,
    pub executor_context: SharedMany<ExecutorContext>,
    pub kernel: SharedMany<dyn Kernel>,
}

#[async_trait(?Send)]
pub trait Linker {
    async fn load(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> anyhow::Result<()>;

    async fn link(
        &self,
        context: &mut LinkerContext,
        module_info: &ModuleInfo,
    ) -> anyhow::Result<()>;
}