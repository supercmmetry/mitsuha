use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(MitsuhaSchedulerPartition::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartition::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartition::ShardStart)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(MitsuhaSchedulerPartition::ShardEnd)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(ColumnDef::new(MitsuhaSchedulerPartition::LeaseExpiry).date_time())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(MitsuhaSchedulerPartition::Table)
                    .to_owned(),
            )
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
pub enum MitsuhaSchedulerPartition {
    Table,
    Id,
    LeaseExpiry,
    ShardStart,
    ShardEnd,
}
