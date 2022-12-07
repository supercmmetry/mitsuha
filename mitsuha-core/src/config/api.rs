use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Security {
    pub enable_error_redaction: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Api {
    pub security: Security,
    pub port: Option<u16>,
    pub public_url: Option<String>,
    pub private_url: Option<String>,
}
