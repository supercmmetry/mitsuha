use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Plugin {
    pub name: String,
    pub extensions: HashMap<String, String>,
}
