use std::{collections::HashMap, sync::Arc};

use crate::{
    channel::ComputeChannel,
    resolver::Resolver,
    types,
};
use async_trait::async_trait;
use mitsuha_core_types::{module::ModuleInfo, kernel::StorageSpec, channel::{ComputeInput, ComputeOutput}};

use crate::errors::Error;

pub struct BlobResolver<Context> {
    extensions: HashMap<String, String>,
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
}

#[async_trait]
impl<Context> Resolver<ModuleInfo, Vec<u8>> for BlobResolver<Context>
where
    Context: Default + Send,
{
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<Vec<u8>> {
        let handle = self.get_handle(key);

        let input = ComputeInput::Load {
            handle,
            extensions: self.extensions.clone(),
        };
        let output = self.channel.compute(Context::default(), input).await?;

        match output {
            ComputeOutput::Loaded { data } => Ok(data),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected compute output with load type"),
            }),
        }
    }

    async fn register(&self, key: &ModuleInfo, value: &Vec<u8>) -> types::Result<()> {
        let handle = self.get_handle(key);
        let spec = StorageSpec {
            handle,
            data: value.clone(),
            ttl: 0,
            extensions: HashMap::new(),
        };

        let input = ComputeInput::Store { spec };
        let output = self.channel.compute(Context::default(), input).await?;

        match output {
            ComputeOutput::Completed => Ok(()),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected compute output with completion type"),
            }),
        }
    }
}

impl<Context> BlobResolver<Context>
where
    Context: Send + Default,
{
    pub fn new(channel: Arc<Box<dyn ComputeChannel<Context = Context>>>) -> Self {
        Self {
            channel,
            extensions: Default::default(),
        }
    }

    pub fn with_extensions(mut self, extensions: HashMap<String, String>) -> Self {
        self.extensions = extensions;
        self
    }

    fn get_handle(&self, module_info: &ModuleInfo) -> String {
        module_info.get_identifier()
    }
}
