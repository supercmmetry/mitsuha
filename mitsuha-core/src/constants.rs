#[derive(strum_macros::Display)]
pub enum StorageControlConstants {
    #[strum(serialize = "mitsuha.storage.request.timestamp")]
    StorageRequestTimestamp,

    #[strum(serialize = "mitsuha.storage.expiry.timestamp")]
    StorageExpiryTimestamp,

    #[strum(serialize = "mitsuha.storage.selector.query")]
    StorageSelectorQuery,

    #[strum(serialize = "mitsuha.storage")]
    StorageLabel,

    #[strum(serialize = "mitsuha.storage.capability.matrix")]
    StorageCapabilityMatrix,
}

#[derive(strum_macros::Display)]
pub enum Constants {
    #[strum(serialize = "mitsuha.core.job.handle")]
    JobHandle,

    #[strum(serialize = "mitsuha.job.output.ttl")]
    JobOutputTTL,

    #[strum(serialize = "mitsuha.job.kernel_bridge.metadata")]
    JobKernelBridgeMetadata,

    #[strum(serialize = "mitsuha.job.status.last_updated")]
    JobStatusLastUpdated,

    #[strum(serialize = "mitsuha.channel.job.await")]
    JobChannelAwait,

    #[strum(serialize = "mitsuha.channel.namespace")]
    ChannelNamespace,

    #[strum(serialize = "mitsuha.channel.original.handle")]
    OriginalHandle,

    #[strum(serialize = "channel_id")]
    ChannelId,

    #[strum(serialize = "mitsuha.core.module.resolver.prefix")]
    ModuleResolverPrefix,
}
