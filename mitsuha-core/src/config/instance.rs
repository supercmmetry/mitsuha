use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Instance {
    pub id: String,
}
