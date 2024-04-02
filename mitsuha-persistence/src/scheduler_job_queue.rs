use sea_orm::entity::prelude::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "mitsuha_scheduler_job_queue_job_state"
)]
pub enum JobState {
    #[sea_orm(string_value = "Pending")]
    Pending,
    #[sea_orm(string_value = "Running")]
    Running,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "mitsuha_scheduler_job_queue_algorithm"
)]
pub enum Algorithm {
    #[sea_orm(string_value = "LightRandom")]
    LightRandom,
    #[sea_orm(string_value = "Random")]
    Random,
    #[sea_orm(string_value = "QuickFit")]
    QuickFit,
}

impl From<&String> for Algorithm {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "LightRandom" => Self::LightRandom,
            "Random" => Self::Random,
            "QuickFit" => Self::QuickFit,
            _ => Self::Random,
        }
    }
}

impl Algorithm {
    pub fn is_quick_fit(&self) -> bool {
        match self {
            Algorithm::QuickFit => true,
            _ => false,
        }
    }

    pub fn uses_shared_tracking(&self) -> bool {
        match self {
            Algorithm::LightRandom => false,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Deserialize, Serialize)]
#[sea_orm(table_name = "mitsuha_scheduler_job_queue")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_handle: String,
    pub partition_id: Option<String>,
    pub shard_id: u64,
    pub job_state: JobState,
    pub creation_timestamp: chrono::NaiveDateTime,
    pub compute_units: u64,
    pub storage_handle: String,
    pub algorithm: Algorithm,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "crate::scheduler_partition::Entity")]
    Partition,

    #[sea_orm(
        belongs_to = "crate::scheduler_job_command_queue::Entity",
        from = "crate::scheduler_job_queue::Column::JobHandle",
        to = "crate::scheduler_job_command_queue::Column::JobHandle"
    )]
    JobCommand,
}

impl Related<crate::scheduler_partition::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Partition.def()
    }
}

impl Related<crate::scheduler_job_command_queue::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::JobCommand.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
