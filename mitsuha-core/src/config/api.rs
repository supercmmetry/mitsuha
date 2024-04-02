use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Api {
    pub address: String,
    pub rpc_port: u64,
    pub http_port: u64,
}
