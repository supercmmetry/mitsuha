use chrono::Utc;
use sea_orm::entity::prelude::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Deserialize, Serialize)]
#[sea_orm(table_name = "mitsuha_scheduler_partition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub lease_expiry: chrono::NaiveDateTime,
    pub shard_start: u64,
    pub shard_end: u64,
}

impl Model {
    pub fn is_lease_expired(&self) -> bool {
        self.lease_expiry.timestamp() <= Utc::now().timestamp()
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::scheduler_job_queue::Entity",
        from = "crate::scheduler_partition::Column::Id",
        to = "crate::scheduler_job_queue::Column::PartitionId"
    )]
    Job,

    #[sea_orm(
        belongs_to = "crate::scheduler_job_command_queue::Entity",
        from = "crate::scheduler_partition::Column::Id",
        to = "crate::scheduler_job_command_queue::Column::PartitionId"
    )]
    JobCommand,

    #[sea_orm(
        belongs_to = "crate::scheduler_partition_resource::Entity",
        from = "crate::scheduler_partition::Column::Id",
        to = "crate::scheduler_partition_resource::Column::Id"
    )]
    PartitionResource,
}

impl Related<crate::scheduler_job_queue::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Job.def()
    }
}

impl Related<crate::scheduler_job_command_queue::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::JobCommand.def()
    }
}

impl Related<crate::scheduler_partition_resource::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PartitionResource.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
