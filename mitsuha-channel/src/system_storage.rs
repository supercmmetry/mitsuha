use std::sync::Arc;

use async_trait::async_trait;
use mitsuha_core::{channel::{ComputeChannel, ComputeInput, ComputeHandle}, types, module::ModuleInfo, errors::Error};
use rand::{distributions::Alphanumeric, Rng};

pub struct SystemStorageChannel {
    next: Option<Arc<Box<dyn ComputeChannel>>>,
    id: String,
}


#[async_trait]
impl ComputeChannel for SystemStorageChannel {
    async fn id(&self) -> types::Result<String> {
        Ok(self.id.clone())
    }

    async fn compute(&self, mut elem: ComputeInput) -> types::Result<ComputeHandle> {
        if let ComputeInput::Store { mut spec } = elem {
            if ModuleInfo::equals_identifier_type(&spec.handle) {
                spec.ttl = u64::MAX;
            }

            elem = ComputeInput::Store { spec };
        }

        match self.next.clone() {
            Some(chan) => chan.compute(elem).await,
            None => Err(Error::ComputeChannelEOF),
        }
    }

    async fn connect(&mut self, next: Arc<Box<dyn ComputeChannel>>) {
        self.next = Some(next);
    }
}

impl SystemStorageChannel {
    pub fn get_identifier_type() -> &'static str {
        "mitsuha/channel/system_storage"
    }

    pub fn new() -> Self {
        let id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        let full_id = format!("{}/{}", Self::get_identifier_type(), id);

        Self {
            next: None,
            id: full_id,
        }
    }
}