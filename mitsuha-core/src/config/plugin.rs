use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Plugin {
    pub name: String,
    pub properties: HashMap<String, String>,
}
