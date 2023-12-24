#[derive(strum_macros::Display)]
pub enum ConfKey {
    #[strum(serialize = "rootDir")]
    RootDir,

    #[strum(serialize = "enableGc")]
    EnableGC,

    #[strum(serialize = "pdEndpoints")]
    PdEndpoints,

    #[strum(serialize = "concurrencyMode")]
    ConcurrencyMode,
}
