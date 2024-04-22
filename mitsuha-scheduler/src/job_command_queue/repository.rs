use async_trait::async_trait;
use mitsuha_core::types;
use mitsuha_core_types::channel::ComputeInput;
use mitsuha_persistence::scheduler_job_command_queue::Model;

#[async_trait]
pub trait Repository: Send + Sync {
    async fn create(&self, input: &ComputeInput, storage_handle: String) -> types::Result<Model>;

    async fn consume_from_partition(&self, partition_id: String) -> types::Result<Option<Model>>;

    async fn mark_as_completed(&self, command_id: i64) -> types::Result<()>;

    async fn is_job_aborted(&self, job_handle: &String) -> types::Result<bool>;
}
