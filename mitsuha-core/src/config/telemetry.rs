use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::Level;

#[derive(Deserialize, Debug, Clone)]
pub struct Telemetry {
    pub stdout: Option<Stdout>,
    pub opentelemetry: Option<OpenTelemetry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    /// The "trace" level.
    ///
    /// Designates very low priority, often extremely verbose, information.
    Trace,
    /// The "debug" level.
    ///
    /// Designates lower priority information.
    Debug,
    /// The "info" level.
    ///
    /// Designates useful information.
    Info,
    /// The "warn" level.
    ///
    /// Designates hazardous situations.
    Warn,
    /// The "error" level.
    ///
    /// Designates very serious errors.
    Error,
}

impl LogLevel {
    pub fn to_level(self) -> Level {
        match self {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Stdout {
    pub level: LogLevel,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OpenTelemetry {
    pub endpoint: String,
    pub entity_attributes: HashMap<String, String>,
}
