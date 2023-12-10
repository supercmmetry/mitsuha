use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Telemetry {
    pub opentelemetry: Option<OpenTelemetry>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OpenTelemetry {
    pub endpoint: String,
    pub entity_attributes: HashMap<String, String>,
}
