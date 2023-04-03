use serde::{Serialize, Deserialize};


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Label {
    pub name: String,
    pub value: String,
}

// TODO: Add selector DSL
