use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Redis {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Client {
    pub redis: Redis,
}
