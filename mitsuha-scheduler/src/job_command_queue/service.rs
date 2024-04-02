use crate::job_command_queue::repository::Repository;
use async_trait::async_trait;
use mitsuha_core::channel::ComputeInputExt;
use mitsuha_core::errors::Error;
use mitsuha_core::{err_unsupported_op, types};
use mitsuha_core_types::channel::ComputeInput;
use mitsuha_persistence::scheduler_job_command_queue::{
    ActiveModel, Column, Entity, JobCommandState, JobCommandType, Model,
};

use mitsuha_persistence::scheduler_job_queue::Entity as JobQueueEntity;
use sea_orm::sea_query::LockType;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect,
    TransactionTrait, TryIntoModel,
};
use std::sync::Arc;

pub struct Service {
    connection: DatabaseConnection,
}

impl Service {
    pub async fn new() -> Arc<Box<dyn Repository>> {
        Arc::new(Box::new(Self {
            connection: mitsuha_persistence::database_connection(),
        }))
    }
}

#[async_trait]
impl Repository for Service {
    async fn create(&self, input: &ComputeInput, storage_handle: String) -> types::Result<Model> {
        let command: JobCommandType;

        match input {
            ComputeInput::Extend { .. } => {
                command = JobCommandType::Extend;
            }
            ComputeInput::Abort { .. } => {
                command = JobCommandType::Abort;
            }
            _ => {
                return Err(err_unsupported_op!(
                    "unhandled compute input cannot be added to job_command_queue"
                ));
            }
        }

        let tx = self.connection.begin().await?;

        let job_queue_item = JobQueueEntity::find_by_id(input.get_handle())
            .one(&tx)
            .await?;

        if job_queue_item.is_none() {
            return Err(err_unsupported_op!(
                "cannot find job_handle {} in job_queue",
                input.get_handle()
            ));
        }

        let obj = ActiveModel {
            job_handle: Set(input.get_handle()),
            partition_id: Set(job_queue_item.unwrap().partition_id),
            command: Set(command),
            state: Set(JobCommandState::Pending),
            storage_handle: Set(storage_handle),
            ..Default::default()
        }
        .insert(&tx)
        .await?;

        tx.commit().await?;

        Ok(obj.try_into_model()?)
    }

    async fn consume_from_partition(&self, partition_id: String) -> types::Result<Option<Model>> {
        let tx = self.connection.begin().await?;

        let model = Entity::find()
            .filter(Column::PartitionId.eq(partition_id))
            .filter(Column::State.eq(JobCommandState::Pending))
            .lock(LockType::Update)
            .one(&tx)
            .await?;

        if let Some(model) = &model {
            ActiveModel {
                id: Set(model.id),
                state: Set(JobCommandState::Running),
                ..Default::default()
            }
            .update(&tx)
            .await?;
        }

        tx.commit().await?;

        Ok(model)
    }

    async fn mark_as_completed(&self, command_id: u64) -> types::Result<()> {
        let tx = self.connection.begin().await?;

        ActiveModel {
            id: Set(command_id),
            state: Set(JobCommandState::Completed),
            ..Default::default()
        }
        .update(&tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn is_job_aborted(&self, job_handle: &String) -> types::Result<bool> {
        let tx = self.connection.begin().await?;

        let model = Entity::find()
            .filter(Column::JobHandle.eq(job_handle))
            .filter(Column::Command.eq(JobCommandType::Abort))
            .one(&tx)
            .await?;

        tx.commit().await?;

        Ok(model.is_some())
    }
}
