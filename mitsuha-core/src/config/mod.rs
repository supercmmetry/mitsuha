pub mod api;
pub mod client;
pub mod executor;
pub mod provider;
pub mod storage;

use serde::Deserialize;
use std::{env, path::Path};

use self::{api::Api, client::Client, executor::Executor, provider::Provider, storage::Storage};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub client: Client,
    pub provider: Provider,
    pub executor: Executor,
    pub api: Api,
    pub storage: Storage,
}

impl Config {
    fn get_config_dir() -> anyhow::Result<String> {
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

    pub fn custom_run_mode(run_mode: String) -> anyhow::Result<Self> {
        let config_dir: String;

        if let Ok(dir) = env::var("MITSUHA_RUNTIME_CONFIG_DIR") {
            config_dir = dir;
        } else {
            config_dir = Self::get_config_dir()?;
        }

        Self::custom(run_mode, config_dir)
    }

    pub fn new() -> anyhow::Result<Self> {
        let run_mode =
            env::var("MITSUHA_RUNTIME_RUN_MODE").unwrap_or_else(|_| "development".into());
        Self::custom_run_mode(run_mode)
    }
}
