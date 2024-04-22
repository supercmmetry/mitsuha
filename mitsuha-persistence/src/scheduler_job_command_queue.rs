use sea_orm::entity::prelude::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "String(Some(1))",
)]
pub enum JobCommandState {
    #[sea_orm(string_value = "Pending")]
    Pending,
    #[sea_orm(string_value = "Running")]
    Running,
    #[sea_orm(string_value = "Completed")]
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "String(Some(1))",
)]
pub enum JobCommandType {
    #[sea_orm(string_value = "Extend")]
    Extend,
    #[sea_orm(string_value = "Abort")]
    Abort,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Deserialize, Serialize)]
#[sea_orm(table_name = "mitsuha_scheduler_job_command_queue")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: i64,
    pub job_handle: String,
    pub partition_id: Option<String>,
    pub command: JobCommandType,
    pub state: JobCommandState,
    pub storage_handle: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "crate::scheduler_partition::Entity")]
    Partition,

    #[sea_orm(has_one = "crate::scheduler_job_queue::Entity")]
    JobQueueItem,
}

impl Related<crate::scheduler_partition::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Partition.def()
    }
}

impl Related<crate::scheduler_job_queue::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::JobQueueItem.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
