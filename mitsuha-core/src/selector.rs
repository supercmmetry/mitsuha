use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    pub key: String,
    pub value: String,
}

// TODO: Add selector DSL
