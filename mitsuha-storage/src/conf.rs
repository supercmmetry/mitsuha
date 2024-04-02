#[derive(strum_macros::Display)]
pub enum ConfKey {
    #[strum(serialize = "root_dir")]
    RootDir,

    #[strum(serialize = "enable_gc")]
    EnableGC,

    #[strum(serialize = "pd_endpoints")]
    PdEndpoints,

    #[strum(serialize = "concurrency_mode")]
    ConcurrencyMode,
}
