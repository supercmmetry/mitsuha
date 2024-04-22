use serde::Deserialize;

use crate::storage::StorageClass;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Storage {
    pub classes: Vec<StorageClass>,
}
