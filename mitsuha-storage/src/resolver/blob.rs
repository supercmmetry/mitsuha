use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{module::ModuleInfo, resolver::Resolver, types};

use crate::Storage;

pub struct BlobResolver {
    storage: Arc<Box<dyn Storage>>,
}

#[async_trait(?Send)]
impl Resolver<ModuleInfo, Vec<u8>> for BlobResolver {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<Vec<u8>> {
        self.storage.load(key.get_identifier()).await
    }

    async fn register(&self, key: &ModuleInfo, value: &Vec<u8>) -> types::Result<()> {
        Ok(())
    }
}
