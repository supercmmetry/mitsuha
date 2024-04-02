pub use sea_orm_migration::prelude::*;

mod m20240126_113029_create_mitsuha_scheduler_partition_table;
mod m20240126_114150_create_mitsuha_scheduler_job_queue_table;
mod m20240128_030024_create_mitsuha_scheduler_job_command_queue_table;
mod m20240216_022505_create_mitsuha_scheduler_partition_resource_table;
mod m20240312_024922_create_mitsuha_module_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20240126_113029_create_mitsuha_scheduler_partition_table::Migration),
            Box::new(m20240126_114150_create_mitsuha_scheduler_job_queue_table::Migration),
            Box::new(m20240128_030024_create_mitsuha_scheduler_job_command_queue_table::Migration),
            Box::new(m20240216_022505_create_mitsuha_scheduler_partition_resource_table::Migration),
            Box::new(m20240312_024922_create_mitsuha_module_table::Migration),
        ]
    }
}
