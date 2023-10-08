use mitsuha_core_types::{module::ModuleInfo, symbol::Symbol};



#[derive(Debug, thiserror::Error)]
pub enum Error {
    // symbol errors
    #[error("ambiguous symbol found in symbol table: {symbol:?}")]
    AmbiguousSymbolError { symbol: Symbol },

    #[error("could not find symbol in context: {symbol:?}")]
    NotFoundSymbolError { symbol: Symbol },

    // wasm errors
    #[error("resolution failed for {inner:?}, {message}")]
    WasmError {
        message: String,
        inner: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    // resolver errors
    #[error("resolution failed for {inner:?}, {message}")]
    ResolverModuleNotFound {
        message: String,
        inner: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    #[error("preprocessing failed during resolution for {inner:?}, {message}")]
    ResolverModulePreprocessingFailed {
        message: String,
        inner: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    #[error("an unknown error occured during resource resolution")]
    ResolverUnknown(#[source] anyhow::Error),

    // module errors
    #[error("failed to load module: {inner:?}, {message}")]
    ModuleLoadFailed {
        message: String,
        inner: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    // linker errors
    #[error("failed to load module during linking, target: {target:?}, {message}")]
    LinkerLoadFailed {
        message: String,
        target: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to link module, target: {target:?}, {message}")]
    LinkerLinkFailed {
        message: String,
        target: ModuleInfo,

        #[source]
        source: anyhow::Error,
    },

    // executor errors
    #[error("execution failed, {message}")]
    ExecutorRunFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to allocate executor, {message}")]
    ExecutorAllocationFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("executor not found")]
    ExecutorNotFound {
        #[source]
        source: anyhow::Error,
    },

    // service errors
    #[error("failed to initialize service, {message}")]
    ServiceInitializationFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    // storage errors
    #[error("failed to perform storage operation, {message}")]
    StorageOperationFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to initialize storage, {message}")]
    StorageInitFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to perform storage store operation, {message}")]
    StorageStoreFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to perform storage load operation, {message}")]
    StorageLoadFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to perform storage persist operation, {message}")]
    StoragePersistFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("failed to perform storage clear operation, {message}")]
    StorageClearFailed {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    #[error("invalid operation, {message}")]
    InvalidOperation { message: String },

    // Kernel error
    #[error("failed to perform kernel call, {message}")]
    KernelError {
        message: String,

        #[source]
        source: anyhow::Error,
    },

    // Job errors
    #[error("job with handle '{handle}' was not found")]
    JobNotFound { handle: String },

    #[error("job with handle '{handle}' expired at '{expiry}'")]
    JobExpired { handle: String, expiry: String },

    #[error("job with handle '{handle}' was aborted")]
    JobAborted { handle: String },

    // Compute channel errors
    #[error("reached compute channel EOF")]
    ComputeChannelEOF,

    // unknown errors
    #[error("encountered unknown error, source: {source}")]
    Unknown {
        #[source]
        source: anyhow::Error,
    },

    #[error("encountered unknown error, {message}")]
    UnknownWithMsgOnly { message: String },
}
