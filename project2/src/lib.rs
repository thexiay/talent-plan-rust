#![feature(error_generic_member_access)]

use std::{collections::{BTreeMap, HashSet, HashMap}, io::{Write, Seek, SeekFrom, Read, BufReader}, fs::{File, OpenOptions}, path::Path, fmt::{Display, Formatter}, os::unix::fs::FileExt, backtrace::Backtrace};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErrorCode {
    #[error("internel error: {0}")]
    InternalError(String),
    #[error(transparent)]
    NetworkError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Error)]
#[error("{inner}")]
pub struct KvError {
    #[source]
    inner: Box<ErrorCode>,
    backtrace: Box<Backtrace>,
}

impl From<ErrorCode> for KvError {
    fn from(value: ErrorCode) -> Self {
        KvError{
            inner: Box::new(value),
            backtrace: Box::new(Backtrace::capture()),
        }
    }
}

impl core::fmt::Debug for KvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}",
            self.inner,
            // Use inner error's backtrace by default, otherwise use the generated one in `From`.
            std::error::request_ref::<Backtrace>(&self.inner).unwrap_or(&*self.backtrace)
        )
    }
}


impl From<std::env::VarError> for KvError {
    fn from(value: std::env::VarError) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<serde_json::error::Error> for KvError {
    fn from(value: serde_json::error::Error) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<std::str::Utf8Error> for KvError {
    fn from(value: std::str::Utf8Error) -> Self {
        ErrorCode::InternalError(value.to_string()).into()
    }
}

impl From<std::io::Error> for KvError {
    fn from(value: std::io::Error) -> Self {
        ErrorCode::NetworkError(value).into()
    }
}

/// serde command
#[derive(Serialize, Deserialize)]
pub enum Command {
    SetCommand{ key: String, value: String},
    RmCommand{ key: String },
}

#[derive(Serialize, Deserialize)]
struct KIndex {
    key: String,
    index: Index,    
}

#[derive(Serialize, Deserialize)]
struct Index {
    offset: u64,
    length: usize,
}

pub struct KvStore {
    // index的信息
    map: HashMap<String, Index>,
    // data file
    storage: File,
    // index file
    index: File,
}


/// 1.Is a buffer is needed?
/// 2.compact it or not ?
impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let storage_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("op.log"))?;
        let mut index_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("index.log"))?;
        let mut kv_store = KvStore {
            map: HashMap::default(),
            storage: storage_file,
            index: index_file,
        };
        kv_store.rebuild(path)?;
        Ok(kv_store)
    }
    
    fn load(&mut self) -> Result<()> {
        std::io::IoSlice
        std::io::Read
        storage_file.seek(SeekFrom::Start(0))?;
        let bufReader = BufReader::new(self.storage);
        let iter = serde_json::Deserializer::from_reader(bufReader).into_iter();
        while let Some(cmd) = iter.next() {

        }
        while bufReader.
        self.storage.read(buf)
        
        

    
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let value_buf = serde_json::to_vec(
            &Command::SetCommand { key: key.clone(), value}
        )?;
        
        let len = self.storage.metadata()?.len();
        self.storage.write(&value_buf)?;
        self.map.insert(key, Index { offset: len , length: value_buf.len() });
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(index) => {
                self.storage.seek(SeekFrom::Start(index.offset))?;
                let mut buffer = vec![0; index.length];
                self.storage.read_exact(&mut buffer)?;
                
                let cmd: serde_json::error::Result<Command> = serde_json::from_slice(buffer.as_slice());
                match cmd? {
                    Command::SetCommand{ value, .. } => Ok(Some(value)),
                    _ => Err(ErrorCode::InternalError(format!("invalid cmd at key {}", key)).into())
                }
            }
            None => Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let value_buf = serde_json::to_vec(
            &Command::RmCommand{ key: key.clone() }
        )?;
        
        let len = self.storage.metadata()?.len();
        self.storage.write(&value_buf)?;
        self.map.insert(key, Index { offset: len, length: value_buf.len() });
        Ok(())
    }
}

impl Write for KvStore {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        todo!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
