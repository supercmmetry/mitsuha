use std::{collections::HashMap, sync::Arc};

use crate::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    kernel::StorageSpec,
    module::ModuleInfo,
    resolver::Resolver,
    types,
};
use async_trait::async_trait;

use crate::errors::Error;

pub struct BlobResolver<Context> {
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
}

#[async_trait]
impl<Context> Resolver<ModuleInfo, Vec<u8>> for BlobResolver<Context>
where
    Context: Default + Send,
{
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<Vec<u8>> {
        let handle = key.get_identifier();

        let input = ComputeInput::Load { handle };
        let output = self.channel.compute(Context::default(), input).await?;

        match output {
            ComputeOutput::Loaded { data } => Ok(data),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected compute output with load type"),
            }),
        }
    }

    async fn register(&self, key: &ModuleInfo, value: &Vec<u8>) -> types::Result<()> {
        let handle = key.get_identifier();
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

impl<Context> BlobResolver<Context> where Context: Send + Default {
    pub fn new(channel: Arc<Box<dyn ComputeChannel<Context = Context>>>) -> Self {
        Self { channel }
    }
}
