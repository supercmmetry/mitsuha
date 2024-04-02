use crate::m20240126_113029_create_mitsuha_scheduler_partition_table::MitsuhaSchedulerPartition;
use crate::m20240126_114150_create_mitsuha_scheduler_job_queue_table::MitsuhaSchedulerJobQueue;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(MitsuhaSchedulerJobCommandQueue::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobCommandQueue::Id)
                            .big_unsigned()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobCommandQueue::JobHandle)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(MitsuhaSchedulerJobCommandQueue::PartitionId).string())
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobCommandQueue::Command)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobCommandQueue::State)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobCommandQueue::StorageHandle)
                            .string()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_mitsuha_scheduler_jcq_x_mitsuha_scheduler_jq")
                            .from(
                                MitsuhaSchedulerJobCommandQueue::Table,
                                MitsuhaSchedulerJobCommandQueue::JobHandle,
                            )
                            .to(
                                MitsuhaSchedulerJobQueue::Table,
                                MitsuhaSchedulerJobQueue::JobHandle,
                            )
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_mitsuha_scheduler_jcq_x_mitsuha_scheduler_partition")
                            .from(
                                MitsuhaSchedulerJobCommandQueue::Table,
                                MitsuhaSchedulerJobCommandQueue::PartitionId,
                            )
                            .to(
                                MitsuhaSchedulerPartition::Table,
                                MitsuhaSchedulerPartition::Id,
                            )
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MitsuhaSchedulerJobQueue::Table)
                    .drop_foreign_key(Alias::new(
                        "fk_mitsuha_scheduler_job_command_queue_x_mitsuha_scheduler_job_queue_x_job_handle",
                    ))
                    .drop_foreign_key(Alias::new(
                        "fk_mitsuha_scheduler_job_command_queue_x_mitsuha_scheduler_partition_x_partition_id",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(MitsuhaSchedulerJobCommandQueue::Table)
                    .to_owned(),
            )
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
pub enum MitsuhaSchedulerJobCommandQueue {
    Table,
    Id,
    JobHandle,
    PartitionId,
    Command,
    State,
    StorageHandle,
}
