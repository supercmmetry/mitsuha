use async_trait::async_trait;

use crate::module::ModuleInfo;

pub struct LinkerContext {}

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
