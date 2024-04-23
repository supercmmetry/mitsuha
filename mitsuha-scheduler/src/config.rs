use serde::Deserialize;

const fn default_partition_lease_duration_seconds() -> u64 {
    15
}

const fn default_partition_lease_skew_seconds() -> u64 {
    5
}

const fn default_partition_poll_interval_millis() -> u64 {
    1
}

const fn default_max_shards() -> u64 {
    i64::MAX as u64
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerConfiguration {
    #[serde(default = "default_partition_lease_duration_seconds")]
    pub partition_lease_duration_seconds: u64,

    #[serde(default = "default_partition_lease_skew_seconds")]
    pub partition_lease_skew_seconds: u64,

    #[serde(default = "default_partition_poll_interval_millis")]
    pub partition_poll_interval_millis: u64,

    #[serde(default)]
    pub exclude_plugins: Vec<String>,

    #[serde(default = "default_max_shards")]
    pub max_shards: u64,
}