use lazy_static::lazy_static;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

lazy_static! {
    static ref JOB_QUEUED_COMPUTE_COST: HistogramVec = HistogramVec::new(
        HistogramOpts::new("job_queued_compute_cost", "Job Queued Compute Costs")
            .namespace("mitsuha_core"),
        &["instance"]
    )
    .expect("failed to initialize metric: JOB_QUEUED_COMPUTE_COST");
    static ref JOB_REQUEST_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new("job_request_count", "Job Request Count").namespace("mitsuha_core"),
        &["instance"]
    )
    .expect("failed to initialize metric: JOB_REQUEST_COUNT");
}

pub fn job_queued_compute_cost_metric() -> &'static HistogramVec {
    &JOB_QUEUED_COMPUTE_COST
}

pub fn job_request_count_metric() -> &'static IntCounterVec {
    &JOB_REQUEST_COUNT
}
