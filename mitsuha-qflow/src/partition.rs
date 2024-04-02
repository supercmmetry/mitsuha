use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mitsuha_core::types;
use mitsuha_core_types::kernel::JobSpec;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub type PartitionId = String;

#[derive(Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub decommisioned: bool,
    pub last_alive_time: DateTime<Utc>,
    pub max_compute_units: u64,
    pub avl_compute_units: u64,
}

#[async_trait]
pub trait PartitionManager: Send + Sync {
    async fn decommision_partition(&self, partition_id: PartitionId) -> types::Result<()>;

    async fn assign_partition(&self, job_spec: &JobSpec) -> types::Result<PartitionId>;

    async fn set_partition_metadata(
        &self,
        partition_id: PartitionId,
        metadata: PartitionMetadata,
    ) -> types::Result<()>;

    async fn get_partition_metadata(
        &self,
        partition_id: PartitionId,
    ) -> types::Result<PartitionMetadata>;
}

#[async_trait]
pub trait PartitionStore: Send + Sync {
    async fn run_job_by_offset(
        &self,
        partition_id: PartitionId,
        offset: u64,
    ) -> types::Result<Option<JobSpec>>;

    async fn delete_job_from_running_pool(
        &self,
        partition_id: PartitionId,
        job_handle: String,
    ) -> types::Result<()>;

    async fn add_job_to_pending_pool(
        &self,
        partition_id: PartitionId,
        job_spec: JobSpec,
    ) -> types::Result<()>;

    async fn move_jobs_from_pending_pool(
        &self,
        partition_id: PartitionId,
        write_offset_supplier: fn() -> u64,
        limit: u64,
    ) -> types::Result<u64>;
}

pub trait PartitionBackend: PartitionStore + PartitionManager {}

#[derive(Clone)]
pub struct Partition {
    id: PartitionId,
    write_offset: Arc<AtomicU64>,
    read_offset: Arc<AtomicU64>,
    backend: Arc<Box<dyn PartitionBackend>>,
}

impl Partition {
    pub fn new(id: String, backend: Arc<Box<dyn PartitionBackend>>) -> Self {
        Self {
            id,
            write_offset: Arc::new(AtomicU64::new(0)),
            read_offset: Arc::new(AtomicU64::new(0)),
            backend,
        }
    }

    pub fn get_id(&self) -> PartitionId {
        self.id.clone()
    }
}
