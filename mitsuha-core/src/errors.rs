use crate::{module::ModuleInfo, symbol::Symbol};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // symbol errors

    #[error("ambiguous symbol found in symbol table: {symbol:?}")]
    AmbiguousSymbolError {
        symbol: Symbol,
    },

    #[error("could not find symbol in context: {symbol:?}")]
    NotFoundSymbolError {
        symbol: Symbol,
    },

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

    // unknown errors
    #[error("encountered unknown error")]
    Unknown {
        #[source]
        source: anyhow::Error,
    },
}
