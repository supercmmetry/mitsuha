use async_trait::async_trait;

use crate::module::ModuleInfo;

pub mod api;

#[async_trait(?Send)]
pub trait Provider: Send + Sync {
    async fn get_raw_module(&self, module_info: &ModuleInfo) -> anyhow::Result<Vec<u8>>;
}
