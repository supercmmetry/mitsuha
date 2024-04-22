use crate::job::cost::{JobCost, JobCostEvaluatorType};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub maximum_concurrent_cost: JobCost,
    pub cost_evaluator_type: JobCostEvaluatorType,
    pub scheduler: Scheduler,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Scheduler {
    #[serde(default)]
    pub enable_update_optimization: bool,

    pub core_scheduling_capacity: JobCost,
}
