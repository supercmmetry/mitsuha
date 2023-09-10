use async_trait::async_trait;
use mitsuha_core::{channel::ComputeInput, types};
use serde::{Deserialize, Serialize};

pub mod engine;

/// A [Policy] defines a binding between a [Permission] and an [Action]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub permission: Permission,
    pub action: Action,
}

/// Defines the permission that needs to be enforced
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
pub enum Permission {
    /// Allow the action to be performed
    Allow,

    /// Deny the action from being performed
    Deny,
}

/// Defines the action that needs to be performed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    /// Run a job with a handle prefix and maximum ttl
    RunJob { handle: String, ttl: u64 },

    /// Get job status with a handle prefix
    GetJobStatus { handle: String },

    /// Extend a job with handle prefix and maximum ttl
    ExtendJob { handle: String, ttl: u64 },

    /// Abort a job with handle preix
    AbortJob { handle: String },

    /// Store a blob with handle prefix and maximum ttl
    StoreBlob { handle: String, ttl: u64 },

    /// Load a blob with handle prefix
    LoadBlob { handle: String },

    /// Persist a blob with handle prefix and maximum ttl
    PersistBlob { handle: String, ttl: u64 },

    /// Clear a blob with handle prefix
    ClearBlob { handle: String },
}


/// The [PolicyEngine] is responsible for performing evaluating policies against operations
#[async_trait]
pub trait PolicyEngine: Send + Sync {

    /// Evaluates whether a [ComputeInput] can be authorized for execution given a set of policies
    /// 
    /// ### Arguments
    /// 
    /// * `input` - The [ComputeInput] that needs to be evaluated
    /// * `policies` - A list of policies which needs to be used for evaluation
    /// 
    async fn evaluate(&self, input: &ComputeInput, policies: &Vec<Policy>) -> types::Result<bool>;

    /// Checks whether a list of policies is a semantic superset of another list of policies
    /// 
    /// ### Arguments
    /// 
    /// * `parent` - The list of policies which is a potential superset of `child`
    /// * `child` - The list of policies which is a potential subset of `parent`
    /// 
    async fn contains(&self, parent: &Vec<Policy>, child: &Vec<Policy>) -> types::Result<bool>;
}
