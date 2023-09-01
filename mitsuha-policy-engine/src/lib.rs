use async_trait::async_trait;
use mitsuha_core::{channel::ComputeInput, types};
use serde::{Deserialize, Serialize};

pub mod engine;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub action: Action,
    pub subject: Subject,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub enum Action {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Subject {
    RunJob { handle: String, ttl: u64 },
    GetJobStatus { handle: String },
    ExtendJob { handle: String, ttl: u64 },
    AbortJob { handle: String },

    StoreBlob { handle: String, ttl: u64 },
    LoadBlob { handle: String },
    PersistBlob { handle: String, ttl: u64 },
    ClearBlob { handle: String },
}

#[async_trait]
pub trait PolicyEngine: Send + Sync {
    async fn evaluate(&self, input: &ComputeInput, policies: &Vec<Policy>) -> types::Result<bool>;

    async fn contains(&self, parent: &Vec<Policy>, child: &Vec<Policy>) -> types::Result<bool>;
}
