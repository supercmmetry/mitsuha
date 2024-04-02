use lazy_static::lazy_static;
use mitsuha_core::config::Config;
use mitsuha_persistence_migration::Migrator;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use sea_orm_migration::MigratorTrait;
use std::time::Duration;
use tokio::runtime::Handle;

pub mod module;
pub mod scheduler_job_command_queue;
pub mod scheduler_job_queue;
pub mod scheduler_partition;
pub mod scheduler_partition_resource;

fn create_database_connection() -> DatabaseConnection {
    let config = Config::new().unwrap();

    let mut connect_opts = ConnectOptions::from(&config.persistence.database_connection_string);

    connect_opts.sqlx_logging(true);
    connect_opts.sqlx_logging_level(log::LevelFilter::Trace);
    connect_opts
        .sqlx_slow_statements_logging_settings(log::LevelFilter::Warn, Duration::from_millis(100));

    tokio::task::block_in_place(|| Handle::current().block_on(Database::connect(connect_opts)))
        .unwrap()
}

lazy_static! {
    static ref DATABASE_CONNECTION: DatabaseConnection = create_database_connection();
}

pub fn database_connection() -> DatabaseConnection {
    DATABASE_CONNECTION.clone()
}

pub async fn apply_migrations() {
    let connection = database_connection();

    Migrator::up(&connection, None).await.unwrap();
}
