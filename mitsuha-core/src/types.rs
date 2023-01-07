use std::sync::{Arc, RwLock, Mutex};

use crate::errors::Error;

pub type SharedMany<T> = Arc<RwLock<T>>;

pub type SharedOne<T> = Arc<Mutex<T>>;

pub type SharedAsyncMany<T> = Arc<tokio::sync::RwLock<T>>;

pub type Result<T> = core::result::Result<T, Error>;
