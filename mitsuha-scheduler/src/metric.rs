use lazy_static::lazy_static;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts};

lazy_static! {
    static ref PARTITION_LEASE_RENEWAL_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "partition_lease_renewal_duration",
            "Partition Lease Renewal Duration"
        )
        .namespace("mitsuha_scheduler"),
        &["instance"]
    )
    .expect("failed to initialize metric: PARTITION_LEASE_RENEWAL_DURATION");
    static ref PARTITION_JOB_CONSUMPTION_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "partition_job_consumption_duration",
            "Partition Job Consumption Duration"
        )
        .namespace("mitsuha_scheduler"),
        &["instance"]
    )
    .expect("failed to initialize metric: PARTITION_JOB_CONSUMPTION_DURATION");
    static ref PARTITION_ORPHANED_JOB_CONSUMPTION_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "partition_orphaned_job_consumption_duration",
            "Partition Orphaned Job Consumption Duration"
        )
        .namespace("mitsuha_scheduler"),
        &["instance"]
    )
    .expect("failed to initialize metric: PARTITION_ORPHANED_JOB_CONSUMPTION_DURATION");
    static ref PARTITION_JOB_COMMAND_CONSUMPTION_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "partition_job_command_consumption_duration",
            "Partition Job Command Consumption Duration"
        )
        .namespace("mitsuha_scheduler"),
        &["instance"]
    )
    .expect("failed to initialize metric: PARTITION_JOB_COMMAND_CONSUMPTION_DURATION");
}

pub fn partition_lease_renewal_duration_metric() -> &'static HistogramVec {
    &PARTITION_LEASE_RENEWAL_DURATION
}

pub fn partition_job_consumption_duration_metric() -> &'static HistogramVec {
    &PARTITION_JOB_CONSUMPTION_DURATION
}

pub fn partition_orphaned_job_consumption_duration_metric() -> &'static HistogramVec {
    &PARTITION_ORPHANED_JOB_CONSUMPTION_DURATION
}

pub fn partition_job_command_consumption_duration_metric() -> &'static HistogramVec {
    &PARTITION_JOB_COMMAND_CONSUMPTION_DURATION
}
