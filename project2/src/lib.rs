#![feature(error_generic_member_access)]

use std::{collections::{BTreeMap, HashSet, HashMap}, io::{Write, Seek, SeekFrom, Read, BufReader}, fs::{File, OpenOptions}, path::Path, fmt::{Display, Formatter}, os::unix::fs::FileExt, backtrace::Backtrace, ops::Deref};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErrorCode {
    #[error("internel error: {0}")]
    InternalError(String),
    #[error(transparent)]
    NetworkError(#[from] std::io::Error),
    #[error("delete not exists key: {0}")]
    RmError(String),
}

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Error)]
#[error("{inner}")]
pub struct KvError {
    #[source]
    inner: Box<ErrorCode>,
    backtrace: Box<Backtrace>,
}

impl Deref for KvError {
    type Target = ErrorCode;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
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
    length: u64,
}

pub struct KvStore {
    // memory index
    map: HashMap<String, Index>,
    // data file
    storage: File,
}


/// 1.How much memory do you need? a fixed memory 
/// 2.What is the minimum amout of copying necessary to compact the log? 
/// Only compact some key until threshold bytes are compacted. So there may be not only one file to read, maybe a lot of files to read a whole new range for current database key.
/// 3.Can the compaction be done in-place? 
/// No, it will break the file
/// 4.How do you maintain data-integrity if compaction fails? 
/// First replace memory index and second clean old log in one trafic 
impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let storage_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(path.join("op.log"))?;
        let mut kv_store = KvStore {
            map: HashMap::default(),
            storage: storage_file,
        };
        kv_store.load()?;
        Ok(kv_store)
    }
    
    fn load(&mut self) -> Result<()> {
        //println!("reload begin.");
        self.storage.seek(SeekFrom::Start(0))?;
        let buf_reader = BufReader::new(self.storage.try_clone()?);
        let mut iter = serde_json::Deserializer::from_reader(buf_reader).into_iter::<Command>();
        let mut last_offset = iter.byte_offset();
        while let Some(cmd) = iter.next() {
            match cmd? {
                Command::SetCommand{ key, ..} => {
                    // println!("reload insert for key {} for index (offset: {}, length: {}).", key, last_offset, iter.byte_offset());
                    self.map.insert(key, Index{ offset: last_offset as u64, length: (iter.byte_offset() - last_offset) as u64 });
                }
                Command::RmCommand { key } => {
                    self.map.remove(&key);
                },
            }
            last_offset = iter.byte_offset();
        }
        //println!("reload end.");
        Ok(())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let value_buf = serde_json::to_vec(
            &Command::SetCommand { key: key.clone(), value}
        )?;
        
        let len = self.storage.metadata()?.len();
        self.storage.write(&value_buf)?;
        //println!("insert for key {} for index (offset: {}, length: {}).", key, len, value_buf.len());
        self.map.insert(key, Index { offset: len , length: value_buf.len() as u64 });
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(index) => {
                self.storage.seek(SeekFrom::Start(index.offset))?;
                let mut buffer = vec![0; index.length as usize];
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
        
        self.storage.write(&value_buf)?;
        if let None = self.map.remove(&key) {
            Err(ErrorCode::RmError(key).into())
        } else {
            Ok(())
        }
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
