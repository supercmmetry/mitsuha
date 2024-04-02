use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use mitsuha_core::config::Config;
use mitsuha_core::errors::Error;
use mitsuha_core::job::cost::{JobCost, JobCostEvaluator};
use mitsuha_core::{err_unsupported_op, types};
use mitsuha_core_types::kernel::JobSpec;
use mitsuha_persistence::scheduler_job_queue::{
    ActiveModel, Algorithm, Column, Entity, JobState, Model,
};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait, TryIntoModel,
};
use std::sync::Arc;

use mitsuha_persistence::scheduler_partition::Entity as PartitionEntity;
use mitsuha_persistence::scheduler_partition::Model as PartitionModel;

use mitsuha_persistence::scheduler_job_command_queue::Column as JobCommandQueueColumn;
use mitsuha_persistence::scheduler_job_command_queue::Entity as JobCommandQueueEntity;
use mitsuha_persistence::scheduler_job_command_queue::{
    ActiveModel as JobCommandQueueActiveModel, JobCommandState,
};
use mitsuha_persistence::scheduler_partition_resource::{
    ActiveModel as PartitionResourceActiveModel, Column as PartitionResourceColumn,
    Entity as PartitionResourceEntity,
};
use rand::Rng;
use sea_orm_migration::prelude::LockType;
use tokio::sync::RwLock;

use crate::job_queue::repository::Repository;
use crate::util;

pub struct Service {
    connection: DatabaseConnection,
    job_cost_evaluator: Arc<Box<dyn JobCostEvaluator>>,
}

impl Service {
    pub async fn new() -> Arc<Box<dyn Repository>> {
        let config = Config::global().await.unwrap();

        Arc::new(Box::new(Self {
            connection: mitsuha_persistence::database_connection(),
            job_cost_evaluator: (&config).try_into().unwrap(),
        }))
    }

    async fn find_partition_by_id_tx(
        &self,
        tx: &DatabaseTransaction,
        partition_id: String,
    ) -> types::Result<PartitionModel> {
        let partition = PartitionEntity::find_by_id(&partition_id).one(tx).await?;

        if partition.is_none() {
            return Err(Error::EntityNotFoundError {
                name: partition_id.clone(),
                kind: crate::partition::KIND.to_string(),
            });
        }

        Ok(partition.unwrap())
    }

    async fn try_assign_any_partition_tx(
        &self,
        tx: &DatabaseTransaction,
        job_handle: String,
        compute_units: u64,
        storage_handle: String,
        algorithm: Algorithm,
    ) -> types::Result<Model> {
        let shard_id: u64 = rand::thread_rng().gen();

        let utc_now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0).unwrap();

        let mut partition_id = None;

        if algorithm.is_quick_fit() {
            let partition_resource = PartitionResourceEntity::find()
                .filter(PartitionResourceColumn::AvailableComputeUnits.gte(compute_units))
                .lock(LockType::Update)
                .one(tx)
                .await?;

            if let Some(partition_resource) = partition_resource {
                PartitionResourceActiveModel {
                    id: Set(partition_resource.id.clone()),
                    available_compute_units: Set(
                        partition_resource.available_compute_units - compute_units
                    ),
                    ..Default::default()
                }
                .update(tx)
                .await?;

                partition_id = Some(partition_resource.id);
            }
        }

        let obj = ActiveModel {
            job_handle: Set(job_handle),
            partition_id: Set(partition_id),
            shard_id: Set(shard_id),
            job_state: Set(JobState::Pending),
            creation_timestamp: Set(utc_now),
            compute_units: Set(compute_units),
            storage_handle: Set(storage_handle),
            algorithm: Set(algorithm),
        }
        .insert(tx)
        .await?;

        Ok(obj.try_into_model()?)
    }

    async fn reassign_command_queue(
        &self,
        tx: &DatabaseTransaction,
        job_handle: &String,
        partition_id: &String,
    ) -> types::Result<()> {
        let updation_model = JobCommandQueueActiveModel {
            partition_id: Set(Some(partition_id.clone())),
            state: Set(JobCommandState::Pending),
            ..Default::default()
        };

        JobCommandQueueEntity::update_many()
            .filter(JobCommandQueueColumn::JobHandle.eq(job_handle))
            .filter(JobCommandQueueColumn::State.ne(JobCommandState::Pending))
            .set(updation_model)
            .exec(tx)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl Repository for Service {
    async fn add_job_to_queue(
        &self,
        job_spec: &JobSpec,
        storage_handle: String,
    ) -> types::Result<Model> {
        let cost = self.job_cost_evaluator.get_cost(job_spec)?;

        let tx = self.connection.begin().await?;

        let alg = util::get_scheduling_algorithm(job_spec);

        let obj = self
            .try_assign_any_partition_tx(
                &tx,
                job_spec.handle.clone(),
                cost.compute,
                storage_handle,
                alg,
            )
            .await?;

        tx.commit().await?;

        Ok(obj)
    }

    async fn remove_from_partition(
        &self,
        partition_id: String,
        job_handle: String,
    ) -> types::Result<()> {
        let tx = self.connection.begin().await?;

        let job = Entity::find_by_id(&job_handle).one(&tx).await?;

        if job.is_none() {
            tracing::error!("failed to reclaim compute units for job '{}'", &job_handle);
            return Err(Error::EntityNotFoundError {
                name: job_handle.clone(),
                kind: crate::job_queue::KIND.to_string(),
            });
        }

        let job = job.unwrap();

        if job.algorithm.uses_shared_tracking() {
            let partition_resource = PartitionResourceEntity::find_by_id(&partition_id)
                .lock(LockType::Update)
                .one(&tx)
                .await?;

            if partition_resource.is_none() {
                return Err(Error::EntityConflictError {
                    name: partition_id.clone(),
                    kind: crate::partition::KIND.to_string(),
                    reason: "partition not found".to_string(),
                });
            }

            let partition_resource = partition_resource.unwrap();

            PartitionResourceActiveModel {
                id: Set(partition_id),
                available_compute_units: Set(
                    partition_resource.available_compute_units + job.compute_units
                ),
                ..Default::default()
            }
            .update(&tx)
            .await?;
        }

        Entity::delete(ActiveModel {
            job_handle: Set(job_handle),
            ..Default::default()
        })
        .exec(&tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn consume_from_partition(&self, partition_id: String) -> types::Result<Option<Model>> {
        let tx = self.connection.begin().await?;

        let partition = self
            .find_partition_by_id_tx(&tx, partition_id.clone())
            .await?;

        if partition.is_lease_expired() {
            return Err(Error::EntityConflictError {
                name: partition_id,
                kind: crate::partition::KIND.to_string(),
                reason: "partition has expired".to_string(),
            });
        }

        let mut model = Entity::find()
            .filter(Column::PartitionId.eq(partition_id))
            .filter(Column::JobState.eq(JobState::Pending))
            .order_by_asc(Column::CreationTimestamp)
            .limit(1)
            .lock(LockType::Update)
            .one(&tx)
            .await?;

        if let Some(value) = model.as_ref() {
            let inner = ActiveModel {
                job_handle: Set(value.job_handle.clone()),
                job_state: Set(JobState::Running),
                ..Default::default()
            }
            .update(&tx)
            .await?;

            model = Some(inner.try_into_model()?);
        }

        tx.commit().await?;

        Ok(model)
    }

    async fn add_orphaned_job_to_partition(
        &self,
        partition_id: String,
    ) -> types::Result<Option<Model>> {
        let tx = self.connection.begin().await?;

        let partition = self
            .find_partition_by_id_tx(&tx, partition_id.clone())
            .await?;

        if partition.is_lease_expired() {
            return Err(Error::EntityConflictError {
                name: partition_id,
                kind: crate::partition::KIND.to_string(),
                reason: "partition has expired".to_string(),
            });
        }

        let partition_resource = PartitionResourceEntity::find_by_id(&partition_id)
            .lock(LockType::Update)
            .one(&tx)
            .await?;

        if partition_resource.is_none() {
            return Err(Error::EntityConflictError {
                name: partition_id,
                kind: crate::partition::KIND.to_string(),
                reason: "partition resource definitions not found".to_string(),
            });
        }

        let partition_resource = partition_resource.unwrap();

        let mut model = Entity::find()
            .filter(Column::ShardId.gte(partition.shard_start))
            .filter(Column::ShardId.lte(partition.shard_end))
            .filter(Column::PartitionId.is_null())
            .filter(Column::ComputeUnits.lte(partition_resource.available_compute_units))
            .order_by_asc(Column::CreationTimestamp)
            .limit(1)
            .lock(LockType::Update)
            .one(&tx)
            .await?;

        if let Some(job) = model.as_ref() {
            if job.algorithm.uses_shared_tracking() {
                PartitionResourceActiveModel {
                    id: Set(partition_id.clone()),
                    available_compute_units: Set(
                        partition_resource.available_compute_units - job.compute_units
                    ),
                    ..Default::default()
                }
                .update(&tx)
                .await?;
            }

            // If we got an orphaned job, then make sure to reassign the commond queue
            // for that job as well.
            self.reassign_command_queue(&tx, &job.job_handle, &partition_id)
                .await?;

            let inner = ActiveModel {
                job_handle: Set(job.job_handle.clone()),
                job_state: Set(JobState::Pending),
                partition_id: Set(Some(partition_id)),
                ..Default::default()
            }
            .update(&tx)
            .await?;

            model = Some(inner.try_into_model()?);
        }

        tx.commit().await?;

        Ok(model)
    }

    async fn batch_event(
        &self,
        partition_id: String,
        batch_size: u64,
        remove_job_handles: Vec<String>,
    ) -> types::Result<usize> {
        let tx = self.connection.begin().await?;

        let partition = self
            .find_partition_by_id_tx(&tx, partition_id.clone())
            .await?;

        if partition.is_lease_expired() {
            return Err(Error::EntityConflictError {
                name: partition_id,
                kind: crate::partition::KIND.to_string(),
                reason: "partition has expired".to_string(),
            });
        }

        let partition_resource = PartitionResourceEntity::find_by_id(&partition_id)
            .lock(LockType::Update)
            .one(&tx)
            .await?;

        if partition_resource.is_none() {
            return Err(Error::EntityConflictError {
                name: partition_id,
                kind: crate::partition::KIND.to_string(),
                reason: "partition resource definitions not found".to_string(),
            });
        }

        let partition_resource = partition_resource.unwrap();

        let mut available_compute_units = partition_resource.available_compute_units;

        // First let's remove all jobs to free up compute as much as possible

        for job_handle in remove_job_handles {
            let job = Entity::find_by_id(&job_handle).one(&tx).await?;

            if job.is_none() {
                tracing::error!("failed to reclaim compute units for job '{}'", &job_handle);
                return Err(Error::EntityNotFoundError {
                    name: job_handle.clone(),
                    kind: crate::job_queue::KIND.to_string(),
                });
            }

            let job = job.unwrap();

            available_compute_units += job.compute_units;

            Entity::delete(ActiveModel {
                job_handle: Set(job_handle),
                ..Default::default()
            })
            .exec(&tx)
            .await?;
        }

        // Now let's try to consume orphaned jobs based on available compute

        let mut orphaned_jobs = Entity::find()
            .filter(Column::ShardId.gte(partition.shard_start))
            .filter(Column::ShardId.lte(partition.shard_end))
            .filter(Column::PartitionId.is_null())
            .filter(Column::ComputeUnits.lte(available_compute_units))
            .order_by_asc(Column::CreationTimestamp)
            .limit(batch_size)
            .lock(LockType::Update)
            .all(&tx)
            .await?;

        let mut orphaned_jobs_added = 0usize;
        for orphaned_job in orphaned_jobs {
            if available_compute_units < orphaned_job.compute_units {
                break;
            }

            orphaned_jobs_added += 1;

            available_compute_units -= orphaned_job.compute_units;

            self.reassign_command_queue(&tx, &orphaned_job.job_handle, &partition_id)
                .await?;

            ActiveModel {
                job_handle: Set(orphaned_job.job_handle.clone()),
                job_state: Set(JobState::Pending),
                partition_id: Set(Some(partition_id.clone())),
                ..Default::default()
            }
            .update(&tx)
            .await?;
        }

        PartitionResourceActiveModel {
            id: Set(partition_id.clone()),
            available_compute_units: Set(available_compute_units),
            ..Default::default()
        }
        .update(&tx)
        .await?;

        tx.commit().await?;

        Ok(orphaned_jobs_added)
    }
}
