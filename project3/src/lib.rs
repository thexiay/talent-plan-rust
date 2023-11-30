#![feature(error_generic_member_access)]
#![feature(let_chains)]

pub use engine::kvs::KvStore;
pub use engine::KvsEngine;
pub use engine::sled::SledStore;
pub use error::Result;

pub mod error;
pub mod cli;
mod io;
mod engine;