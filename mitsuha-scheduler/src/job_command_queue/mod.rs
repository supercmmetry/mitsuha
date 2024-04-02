use std::sync::Arc;

mod repository;
pub mod service;

pub type Repository = Arc<Box<dyn repository::Repository>>;
