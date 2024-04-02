use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Persistence {
    pub database_connection_string: String,
}
