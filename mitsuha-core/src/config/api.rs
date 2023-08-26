use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Api {
    pub address: String,
    pub port: u64,
}
