use chrono::{DateTime, Utc};
use dashmap::DashMap;

pub struct LocalFileStorage {
    root_dir: String,
    expiry_cache: moka::future::Cache<String, DateTime<Utc>>,
}
