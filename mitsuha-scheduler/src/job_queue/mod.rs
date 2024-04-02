use lazy_static::lazy_static;
use std::string::ToString;
use std::sync::Arc;

mod repository;
pub mod service;

pub type Repository = Arc<Box<dyn repository::Repository>>;

lazy_static! {
    pub static ref KIND: String = "JobQueue".to_string();
}
