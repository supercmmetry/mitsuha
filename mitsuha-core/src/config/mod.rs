pub mod api;
pub mod plugin;
pub mod storage;
pub mod telemetry;

use anyhow::anyhow;
use lazy_static::lazy_static;
use serde::Deserialize;
use tokio::sync::RwLock;
use std::{env, path::Path, sync::{Arc, Once}, time::Duration};

use self::{api::Api, plugin::Plugin, storage::Storage, telemetry::Telemetry};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub api: Api,
    pub storage: Storage,
    pub plugins: Vec<Plugin>,
    pub telemetry: Telemetry,
}

lazy_static! {
    static ref GLOBAL_CONFIG: Arc<RwLock<Option<Config>>> = Arc::new(RwLock::new(Config::new().ok()));

    static ref GLOBAL_CONFIG_TRACKER_ONCE: Once = Once::new();
}

impl Config {
    fn get_local_config_dir() -> anyhow::Result<String> {
        let mut path = env::current_exe()?;
        path.pop();
        path.push("config");

        if !path.is_dir() {
            if Path::new("mitsuha_runtime/config").is_dir() {
                return Ok("mitsuha_runtime/config".into());
            }

            if Path::new("config").is_dir() {
                return Ok("config".into());
            }

            return Err(anyhow::anyhow!("default config dir was not found"));
        }

        match path.to_str() {
            Some(v) => Ok(v.into()),
            _ => Err(anyhow::anyhow!("failed to get default config dir")),
        }
    }

    fn custom(run_mode: String, config_dir: String) -> anyhow::Result<Self> {
        let config = config::Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(config::File::with_name(&format!("{}/default", config_dir)))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(
                config::File::with_name(&format!("{}/{}", config_dir, run_mode)).required(false),
            )
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            .add_source(config::File::with_name(&format!("{}/local", config_dir)).required(false))
            // Add in settings from the environment (with a prefix of MITSUHA)
            // Eg.. `MITSUHA_RUNTIME_DEBUG=1 ./target/mitsuha_runtime` would set the `debug` key
            .add_source(config::Environment::with_prefix("mitsuha_runtime"))
            .build()?;

        Ok(config.try_deserialize()?)
    }

    pub fn get_config_dir() -> anyhow::Result<String> {
        if let Ok(dir) = env::var("MITSUHA_RUNTIME_CONFIG_DIR") {
            return Ok(dir);
        } else {
            return Self::get_local_config_dir();
        }
    }

    pub fn custom_run_mode(run_mode: String) -> anyhow::Result<Self> {
        Self::custom(run_mode, Self::get_config_dir()?)
    }

    pub fn new() -> anyhow::Result<Self> {
        let run_mode =
            env::var("MITSUHA_RUNTIME_RUN_MODE").unwrap_or_else(|_| "development".into());
        Self::custom_run_mode(run_mode)
    }

    pub async fn global() -> anyhow::Result<Self> {
        let config = GLOBAL_CONFIG.read().await.clone();

        config.ok_or(anyhow!("failed to load configuration"))
    }

    pub fn start_global_tracker() {
        GLOBAL_CONFIG_TRACKER_ONCE.call_once(|| {
            let config = GLOBAL_CONFIG.clone();

            tokio::task::spawn(async move {
                *config.write().await = Config::new().ok();
                tokio::time::sleep(Duration::from_secs(1)).await;
            });
        });
    }
}
