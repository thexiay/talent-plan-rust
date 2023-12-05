use std::path::Path;

use crate::Result;

pub trait KvsEngine {
    fn open(path: &Path) -> Result<Self>
    where
        Self: Sized;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}

pub mod kvs;
pub mod sled;
