#![feature(async_iterator)]
#![feature(fs_try_exists)]

mod local;
mod memory;
pub mod unified;

pub use unified::UnifiedStorage;
