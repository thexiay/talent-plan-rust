#![feature(error_generic_member_access)]
#![feature(let_chains)]

pub use engine::kvs::KvStore;
pub use engine::KvsEngine;
pub use engine::sled::SledStore;
pub use error::Result;
pub use server::KvServer;
pub mod error;
pub mod common;

mod engine;
mod server;
mod client;