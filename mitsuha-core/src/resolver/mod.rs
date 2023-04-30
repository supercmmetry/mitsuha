use async_trait::async_trait;

use crate::types;

// pub mod module;
pub mod redis;
pub mod wasm;
pub mod blob;

#[async_trait(?Send)]
pub trait Resolver<Key, Value>: Send + Sync {
    async fn resolve(&self, key: &Key) -> types::Result<Value>;

    async fn register(&self, key: &Key, value: &Value) -> types::Result<()>;
}
