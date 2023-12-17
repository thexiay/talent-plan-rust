#![feature(error_generic_member_access)]
#![feature(let_chains)]

pub use client::KvClient;
pub use engine::kvs::KvStore;
pub use engine::sled::SledStore;
pub use engine::KvsEngine;
pub use error::Result;
pub use server::KvServer;
pub use server::ThreadHandle;
pub mod common;
pub mod error;
pub mod thread_pool;

mod client;
mod engine;
mod server;
