use async_trait::async_trait;
use mitsuha_core::types;
use mitsuha_persistence::scheduler_partition::Model;

#[async_trait]
pub trait Repository: Send + Sync {
    async fn register_module(&self) -> types::Result<()>;

    /// Create a new partition.
    async fn create(&self) -> types::Result<Model>;

    /// Read a partition by ID.
    async fn read_by_id(&self, id: String) -> types::Result<Option<Model>>;

    /// Renew the lease on a partition.
    async fn renew_lease(&self, id: String) -> types::Result<()>;

    /// Remove a partition. Note that only partitions whose leases have expired may be removed.
    async fn remove(&self, id: String) -> types::Result<()>;

    /// Removes all partitions with expired leases.
    async fn remove_stale_partitions(&self) -> types::Result<()>;
}
