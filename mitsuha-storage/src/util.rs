use std::{collections::HashMap, str::ParseBoolError};

use chrono::{DateTime, Duration, Utc};
use mitsuha_core::{
    constants::StorageControlConstants, err_unknown, errors::Error, storage::StorageClass, types,
};

use crate::conf::ConfKey;

pub fn get_expiry(
    handle: &String,
    ttl: u64,
    extensions: &HashMap<String, String>,
) -> types::Result<DateTime<Utc>> {
    match extensions.get(&StorageControlConstants::StorageExpiryTimestamp.to_string()) {
        Some(value) => {
            let date_time =
                value
                    .parse::<DateTime<Utc>>()
                    .map_err(|e| Error::StorageStoreFailed {
                        message: format!(
                            "failed to parse storage expiry time for storage handle: '{}'",
                            handle
                        ),
                        source: e.into(),
                    })?;

            if date_time <= Utc::now() {
                return Err(Error::StorageStoreFailed {
                    message: format!("storage handle has already expired: '{}'", handle.clone()),
                    source: anyhow::anyhow!(""),
                });
            }

            Ok(date_time)
        }
        None => Ok(Utc::now() + Duration::seconds(ttl as i64)),
    }
}

pub trait StorageClassExt {
    fn should_enable_gc(&self) -> types::Result<bool>;
}

impl StorageClassExt for StorageClass {
    fn should_enable_gc(&self) -> types::Result<bool> {
        if let Some(value) = self.extensions.get(&ConfKey::EnableGC.to_string()) {
            value.parse().map_err(|e: ParseBoolError| err_unknown!(e))
        } else {
            Ok(false)
        }
    }
}
