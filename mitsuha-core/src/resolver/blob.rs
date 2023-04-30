use std::{sync::Arc, collections::HashMap};

use async_trait::async_trait;
use crate::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    module::ModuleInfo,
    resolver::Resolver,
    types, kernel::StorageSpec,
};

use crate::errors::Error;

pub struct BlobResolver<Context> {
    channel: Arc<Box<dyn ComputeChannel<Context = Context>>>,
}

#[async_trait(?Send)]
impl<Context> Resolver<ModuleInfo, Vec<u8>> for BlobResolver<Context> where Context: Default {
    async fn resolve(&self, key: &ModuleInfo) -> types::Result<Vec<u8>> {
        let handle = key.get_identifier();

        let input = ComputeInput::Load { handle };
        let output = self
            .channel
            .compute(Context::default(), input)
            .await?
            .await
            .map_err(|e| Error::Unknown { source: e.into() })??;

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
        let output = self
            .channel
            .compute(Context::default(), input)
            .await?
            .await
            .map_err(|e| Error::Unknown { source: e.into() })??;

        match output {
            ComputeOutput::Completed => Ok(()),
            _ => Err(Error::UnknownWithMsgOnly {
                message: format!("expected compute output with completion type"),
            }),
        }
    }
}
