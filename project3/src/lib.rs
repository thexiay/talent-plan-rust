#![feature(error_generic_member_access)]
#![feature(let_chains)]

pub use kv::KvStore;
pub use error::Result;
pub use engine::KvsEngine;

pub mod error;
pub mod kv;
pub mod cli;
mod io;
mod engine;