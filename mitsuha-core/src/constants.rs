#[derive(strum_macros::Display)]
pub enum Constants {
    #[strum(serialize = "mitsuha.storage.request.timestamp")]
    StorageRequestTimestamp,

    #[strum(serialize = "mitsuha.storage.expiry.timestamp")]
    StorageExpiryTimestamp,

    #[strum(serialize = "mitsuha.storage.selector.query")]
    StorageSelectorQuery,

    #[strum(serialize = "mitsuha.storage")]
    StorageLabel,

    #[strum(serialize = "mitsuha.job.output.ttl")]
    JobOutputTTL,

    #[strum(serialize = "mitsuha.job.status.last_updated")]
    JobStatusLastUpdated,

    #[strum(serialize = "channel_id")]
    ChannelId,
}
