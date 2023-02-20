#[derive(strum_macros::Display)]
pub enum Constants {
    #[strum(serialize = "mitsuha.storage.request.timestamp")]
    StorageRequestTimestamp,

    #[strum(serialize = "mitsuha.storage.expiry.timestamp")]
    StorageExpiryTimestamp,
}
