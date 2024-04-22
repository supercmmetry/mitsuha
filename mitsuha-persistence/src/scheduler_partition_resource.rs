use chrono::Utc;
use sea_orm::entity::prelude::*;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Deserialize, Serialize)]
#[sea_orm(table_name = "mitsuha_scheduler_partition_resource")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub available_compute_units: i64,
    pub total_compute_units: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "crate::scheduler_partition::Entity")]
    Partition,
}

impl Related<crate::scheduler_partition::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Partition.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
