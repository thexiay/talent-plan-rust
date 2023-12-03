
use std::path::Path;

use crate::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn open(path: &Path) -> Result<Self> where Self: Sized;

    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}

pub mod kvs;
pub mod sled;