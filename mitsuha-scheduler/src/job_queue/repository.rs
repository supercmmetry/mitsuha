use async_trait::async_trait;
use mitsuha_core::types;
use mitsuha_core_types::kernel::JobSpec;
use mitsuha_persistence::scheduler_job_queue::Model;

#[async_trait]
pub trait Repository: Send + Sync {
    /// Adds a job to the queue. We may perform partition assignment in this stage.
    async fn add_job_to_queue(
        &self,
        job_spec: &JobSpec,
        storage_handle: String,
    ) -> types::Result<Model>;

    /// Remove a job from the partition.
    /// This should be only called when the job execution was completed.
    async fn remove_from_partition(
        &self,
        partition_id: String,
        job_handle: String,
    ) -> types::Result<()>;

    /// Consumes a pending job from a partition. The job state is set to `Running`.
    async fn consume_from_partition(&self, partition_id: String) -> types::Result<Option<Model>>;

    /// Try to look for an orphaned job and assign it to the given partition.
    /// An orphaned job has no partition.
    async fn add_orphaned_job_to_partition(
        &self,
        partition_id: String,
    ) -> types::Result<Option<Model>>;

    async fn batch_event(
        &self,
        partition_id: String,
        batch_size: u64,
        remove_job_handles: Vec<String>,

    ) -> types::Result<usize>;
}
