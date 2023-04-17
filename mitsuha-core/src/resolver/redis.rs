use crate::{errors::Error, types};

use super::Resolver;
use async_trait::async_trait;
use redis::{FromRedisValue, ToRedisArgs};
use serde::Serialize;

#[derive(Clone)]
pub struct RedisResolver {
    client: redis::Client,
}

#[async_trait(?Send)]
impl<K, V> Resolver<K, V> for RedisResolver
where
    K: ToRedisArgs + Send + Sync,
    V: ToRedisArgs + FromRedisValue + Send + Sync,
{
    async fn resolve(&self, key: &K) -> types::Result<V> {
        let mut conn = self
            .client
            .get_connection()
            .map_err(|e| Error::ResolverUnknown(e.into()))?;

        if redis::cmd("EXISTS")
            .arg(key)
            .query::<i32>(&mut conn)
            .map_err(|e| Error::ResolverUnknown(e.into()))?
            == 0
        {
            return Err(anyhow::anyhow!("redis key was not found"))
                .map_err(|e| Error::ResolverUnknown(e));
        }

        Ok(redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .map_err(|e| Error::ResolverUnknown(e.into()))?)
    }

    async fn register(&self, key: &K, value: &V) -> types::Result<()> {
        let mut conn = self
            .client
            .get_connection()
            .map_err(|e| Error::ResolverUnknown(e.into()))?;

        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut conn)
            .map_err(|e| Error::ResolverUnknown(e.into()))?;

        Ok(())
    }
}

impl RedisResolver {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[derive(Serialize)]
pub enum RedisKey {
    ModuleInfoToWasm,
    ModuleInfoToServiceDefinition,
}

#[derive(Serialize)]
pub struct RedisContextKey<T, U> {
    pub data: T,
    pub context: U,
}

impl<T, U> ToRedisArgs for RedisContextKey<T, U>
where
    T: Serialize,
    U: Serialize,
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let data = serde_json::to_string(&self).unwrap();
        out.write_arg_fmt(data);
    }
}

impl<T, U> RedisContextKey<T, U> {
    pub fn new(data: T, context: U) -> Self {
        Self { data, context }
    }
}
