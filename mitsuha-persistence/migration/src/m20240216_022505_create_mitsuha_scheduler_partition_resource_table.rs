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
                    .table(MitsuhaSchedulerPartitionResource::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartitionResource::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartitionResource::AvailableComputeUnits)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartitionResource::TotalComputeUnits)
                            .big_unsigned()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_mitsuha_scheduler_part_x_mitsuha_scheduler_part_resx")
                            .from(
                                MitsuhaSchedulerPartitionResource::Table,
                                MitsuhaSchedulerPartitionResource::Id,
                            )
                            .to(
                                MitsuhaSchedulerPartition::Table,
                                MitsuhaSchedulerPartition::Id,
                            )
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(MitsuhaSchedulerPartitionResource::Table)
                    .drop_foreign_key(Alias::new(
                        "fk_mitsuha_scheduler_part_x_mitsuha_scheduler_part_resx",
                    ))
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(MitsuhaSchedulerPartitionResource::Table)
                    .to_owned(),
            )
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum MitsuhaSchedulerPartitionResource {
    Table,
    Id,
    AvailableComputeUnits,
    TotalComputeUnits,
}
