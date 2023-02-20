use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum StorageLocality {
    Local,
    Cache,
    Shared,
}

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum StorageKind {
    Memory,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageClass {
    pub kind: StorageKind,
    pub locality: StorageLocality,
    pub index: usize,
    pub extensions: HashMap<String, String>,
}
