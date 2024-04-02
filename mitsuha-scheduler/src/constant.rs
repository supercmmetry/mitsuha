#[derive(strum_macros::Display)]
pub enum SchedulerConstants {
    #[strum(serialize = "mitsuha.scheduler.param.job_handle")]
    JobHandleParameter,

    #[strum(serialize = "mitsuha.scheduler.param.storage_handle")]
    StorageHandleParameter,

    #[strum(serialize = "mitsuha.scheduler.param.job_command_id")]
    JobCommandIdParameter,

    #[strum(serialize = "mitsuha.scheduler.algorithm")]
    SchedulingAlgorithm,
}
