use crate::config::Config;
use crate::types;
use mitsuha_core_types::kernel::JobSpec;
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, SubAssign};
use std::sync::Arc;

#[derive(Debug, Default, Clone, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct JobCost {
    pub compute: u64,
}

impl Add for JobCost {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            compute: self.compute + rhs.compute,
        }
    }
}

impl AddAssign for JobCost {
    fn add_assign(&mut self, rhs: Self) {
        self.compute += rhs.compute;
    }
}

impl SubAssign for JobCost {
    fn sub_assign(&mut self, rhs: Self) {
        self.compute -= rhs.compute;
    }
}

pub trait JobCostEvaluator: Send + Sync {
    fn get_cost(&self, job_spec: &JobSpec) -> types::Result<JobCost>;
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobCostEvaluatorType {
    Standard,
}

pub struct StandardJobCostEvaluator;

impl JobCostEvaluator for StandardJobCostEvaluator {
    fn get_cost(&self, job_spec: &JobSpec) -> types::Result<JobCost> {
        let compute = job_spec.ttl;

        Ok(JobCost { compute })
    }
}

impl TryInto<Arc<Box<dyn JobCostEvaluator>>> for &Config {
    type Error = crate::errors::Error;

    fn try_into(self) -> types::Result<Arc<Box<dyn JobCostEvaluator>>> {
        let cost_evaluator_type = self.job.cost_evaluator_type.clone();

        match cost_evaluator_type {
            JobCostEvaluatorType::Standard => Ok(Arc::new(Box::new(StandardJobCostEvaluator))),
        }
    }
}
