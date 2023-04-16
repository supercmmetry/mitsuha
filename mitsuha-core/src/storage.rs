use std::collections::HashMap;

use serde::Deserialize;

use crate::selector::Label;

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum StorageLocality {
    Solid { cache_name: Option<String> },
    Cache { ttl: u64 },
}

impl StorageLocality {
    pub fn is_cache(&self) -> bool {
        match self {
            Self::Cache { .. } => true,
            _ => false,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum StorageKind {
    Memory,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageClass {
    pub kind: StorageKind,
    pub locality: StorageLocality,
    pub name: String,
    pub labels: Vec<Label>,
    pub extensions: HashMap<String, String>,
}
