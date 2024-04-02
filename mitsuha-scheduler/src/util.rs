use crate::constant::SchedulerConstants;
use mitsuha_core_types::kernel::JobSpec;
use mitsuha_persistence::scheduler_job_queue::Algorithm;
use uuid::Uuid;

pub fn generate_scheduler_store_handle() -> String {
    format!("/mitsuha/scheduler/store/{}", Uuid::new_v4())
}

pub fn get_scheduling_algorithm(spec: &JobSpec) -> Algorithm {
    match spec
        .extensions
        .get(&SchedulerConstants::SchedulingAlgorithm.to_string())
    {
        Some(v) => Algorithm::from(v),
        None => Algorithm::Random,
    }
}
