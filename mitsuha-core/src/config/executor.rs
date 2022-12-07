use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Executor {
    pub max_instances: usize,
}
