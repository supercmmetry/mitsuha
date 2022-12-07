use std::sync::{Arc, Mutex, RwLock};

use crate::errors::Error;

pub type SharedMany<T> = Arc<RwLock<T>>;

pub type SharedOne<T> = Arc<Mutex<T>>;

pub type Result<T> = core::result::Result<T, Error>;
