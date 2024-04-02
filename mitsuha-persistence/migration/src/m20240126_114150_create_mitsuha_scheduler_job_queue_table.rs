use crate::m20240126_113029_create_mitsuha_scheduler_partition_table::MitsuhaSchedulerPartition;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(MitsuhaSchedulerJobQueue::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::JobHandle)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(MitsuhaSchedulerJobQueue::PartitionId).string())
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::ShardId)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::JobState)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::CreationTimestamp)
                            .date_time()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::ComputeUnits)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::StorageHandle)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerJobQueue::Algorithm)
                            .string()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_mitsuha_scheduler_jq_x_mitsuha_scheduler_partition")
                            .from(
                                MitsuhaSchedulerJobQueue::Table,
                                MitsuhaSchedulerJobQueue::PartitionId,
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
                        "fk_mitsuha_scheduler_jq_x_mitsuha_scheduler_partition",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(MitsuhaSchedulerJobQueue::Table)
                    .to_owned(),
            )
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
pub enum MitsuhaSchedulerJobQueue {
    Table,
    JobHandle,
    PartitionId,
    ShardId,
    JobState,
    CreationTimestamp,
    ComputeUnits,
    StorageHandle,
    Algorithm,
}
