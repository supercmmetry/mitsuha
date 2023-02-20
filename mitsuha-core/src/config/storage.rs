use serde::Deserialize;

use crate::storage::StorageClass;

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub classes: Vec<StorageClass>,
}
