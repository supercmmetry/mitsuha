use crate::partition::repository::Repository;
use crate::partition::KIND;
use async_trait::async_trait;
use chrono::{Duration, NaiveDateTime, Utc};
use mitsuha_core::errors::Error;
use mitsuha_core::{err_unsupported_op, types};
use mitsuha_persistence::module::{
    ActiveModel as ModuleActiveModel, Column as ModuleColumn, Entity as ModuleEntity,
    Model as ModuleModel,
};
use mitsuha_persistence::scheduler_partition::{ActiveModel, Column, Entity, Model};
use mitsuha_persistence::scheduler_partition_resource::ActiveModel as PartitionResourceActiveModel;
use sea_orm::sea_query::LockType;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, EntityName, EntityTrait, QueryFilter, QuerySelect, Statement, TransactionTrait,
    TryIntoModel,
};
use std::sync::Arc;
use uuid::Uuid;

pub struct Service {
    connection: DatabaseConnection,
    partition_lease_duration: Duration,
    partition_lease_duration_skew: Duration,
    total_compute_units: i64,
    max_shards: i64,
}

impl Service {
    pub async fn new(
        partition_lease_duration: Duration,
        partition_lease_duration_skew: Duration,
        total_compute_units: i64,
        max_shards: i64,
    ) -> Arc<Box<dyn Repository>> {
        Arc::new(Box::new(Self {
            connection: mitsuha_persistence::database_connection(),
            partition_lease_duration,
            partition_lease_duration_skew,
            total_compute_units,
            max_shards,
        }))
    }

    async fn read_by_id_tx(
        &self,
        tx: &DatabaseTransaction,
        id: String,
    ) -> types::Result<Option<Model>> {
        Ok(Entity::find_by_id(id.clone()).one(tx).await?)
    }

    async fn reshard_partitions_by_tx(&self, tx: &DatabaseTransaction) -> types::Result<()> {
        let table_name = Entity.table_name();

        let sql = match tx.get_database_backend() {
            DbBackend::MySql => {
                format!(
                        "with numbered_rows as (
                        select id, row_number() over (order by id) as row_num
                        from {table_name})
                        update {table_name}
                        join numbered_rows on {table_name}.id = numbered_rows.id 
                        set shard_start = (row_num - 1) * ({max_shards} / (select count(*) from {table_name})),
                        shard_end = row_num * ({max_shards} / (select count(*) from {table_name})) - 1;",
                        table_name = table_name,
                        max_shards = self.max_shards
                    )
            }
            DbBackend::Postgres => {
                format!(
                    "WITH numbered_rows AS (
                        SELECT
                        id,
                        row_number() OVER (ORDER BY id) AS row_num,
                        COUNT(*) OVER () AS total_rows
                        FROM
                        {table_name}
                    ),
                    shard_ranges AS (
                        SELECT
                        id,
                        (row_num - 1) * ({max_shards} / total_rows) AS shard_start,
                        row_num * ({max_shards} / total_rows) - 1 AS shard_end
                        FROM
                        numbered_rows
                    )
                    UPDATE {table_name} AS t
                    SET
                    shard_start = sr.shard_start,
                    shard_end = sr.shard_end
                    FROM
                    shard_ranges AS sr
                    WHERE
                    t.id = sr.id",
                    table_name = table_name,
                    max_shards = self.max_shards
                )
            }
            _ => {
                return Err(err_unsupported_op!(
                    "only supported backends are MySql and Postgres!"
                ))
            }
        };

        let statement = Statement::from_string(tx.get_database_backend(), sql);

        tx.execute(statement).await?;

        Ok(())
    }

    async fn acquire_module_lock(&self, tx: &DatabaseTransaction) -> types::Result<()> {
        let module_name = crate::partition::KIND.to_string();

        ModuleEntity::find()
            .filter(ModuleColumn::Name.eq(module_name))
            .lock(LockType::Update)
            .one(tx)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Repository for Service {
    async fn register_module(&self) -> types::Result<()> {
        let tx = self.connection.begin().await?;
        let module_name = crate::partition::KIND.to_string();

        let model = ModuleEntity::find()
            .filter(ModuleColumn::Name.eq(module_name.to_string()))
            .one(&tx)
            .await?;

        if model.is_none() {
            ModuleActiveModel {
                name: Set(module_name),
                ..Default::default()
            }
            .save(&tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn create(&self) -> types::Result<Model> {
        let partition_id = Uuid::new_v4().to_string();
        let lease_expiry = Utc::now() + self.partition_lease_duration;

        let lease_expiry_naive = NaiveDateTime::from_timestamp_opt(lease_expiry.timestamp(), 0);

        let tx = self.connection.begin().await?;

        self.acquire_module_lock(&tx).await?;

        let obj = ActiveModel {
            id: Set(partition_id.clone()),
            lease_expiry: Set(lease_expiry_naive.unwrap()),
            shard_start: Set(0i64),
            shard_end: Set(self.max_shards - 1),
        }
        .insert(&tx)
        .await?;

        PartitionResourceActiveModel {
            id: Set(partition_id),
            available_compute_units: Set(self.total_compute_units),
            total_compute_units: Set(self.total_compute_units),
        }
        .insert(&tx)
        .await?;

        self.reshard_partitions_by_tx(&tx).await?;

        tx.commit().await?;

        Ok(obj.try_into_model()?)
    }

    async fn read_by_id(&self, id: String) -> types::Result<Option<Model>> {
        let tx = self.connection.begin().await?;

        let value = self.read_by_id_tx(&tx, id).await?;

        tx.commit().await?;

        Ok(value)
    }

    async fn renew_lease(&self, id: String) -> types::Result<()> {
        let tx = self.connection.begin().await?;

        let existing_partition = self.read_by_id_tx(&tx, id.clone()).await?;

        if existing_partition.is_none() {
            return Err(Error::EntityNotFoundError {
                name: id.clone(),
                kind: KIND.clone(),
            });
        }

        let existing_partition = existing_partition.unwrap();

        if existing_partition.lease_expiry.timestamp()
            <= Utc::now().timestamp() + self.partition_lease_duration_skew.num_seconds()
        {
            return Err(Error::EntityConflictError {
                name: id.clone(),
                kind: KIND.clone(),
                reason: "the partition has expired or is close to expiry, cannot renew further"
                    .to_string(),
            });
        }

        let lease_expiry = Utc::now() + self.partition_lease_duration;

        let lease_expiry_naive = NaiveDateTime::from_timestamp_opt(lease_expiry.timestamp(), 0);

        ActiveModel {
            id: Set(id),
            lease_expiry: Set(lease_expiry_naive.unwrap()),
            ..Default::default()
        }
        .update(&tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn remove(&self, id: String) -> types::Result<()> {
        let tx = self.connection.begin().await?;

        Entity::delete_by_id(id).exec(&tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn remove_stale_partitions(&self) -> types::Result<()> {
        let utc_now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let tx = self.connection.begin().await?;

        let result = Entity::delete_many()
            .filter(Column::LeaseExpiry.lte(utc_now))
            .exec(&tx)
            .await?;

        // If some partitions were deleted, then reshard all partitions.
        if result.rows_affected > 0 {
            self.acquire_module_lock(&tx).await?;

            self.reshard_partitions_by_tx(&tx).await?;
        }

        tx.commit().await?;

        Ok(())
    }
}
