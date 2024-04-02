#[derive(strum_macros::Display)]
pub enum ConfKey {
    #[strum(serialize = "partition_lease_duration_seconds")]
    PartitionLeaseDurationSeconds,

    #[strum(serialize = "partition_lease_skew_seconds")]
    PartitionLeaseSkewDurationSeconds,

    #[strum(serialize = "partition_poll_interval_millis")]
    PartitionPollIntervalMilliSeconds,

    #[strum(serialize = "bypass_channel_ids")]
    BypassChannelIds,

    #[strum(serialize = "max_shards")]
    MaxShards,
}
