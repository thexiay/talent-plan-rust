use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{spawn, JoinHandle};

use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use super::KvsEngine;
use crate::error::ErrorCode;
use crate::Result;
use std::ffi::OsStr;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<RwLock<SharedKvStore>>,
}

pub struct SharedKvStore {
    // directory for the log and other data
    path: PathBuf,
    // map generation number to the file reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: BTreeMap<String, CommandPos>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction
    uncompacted: u64,
}

#[derive(Clone)]
pub struct ReadLockFreeKvStore {
    path: Arc<PathBuf>,
    reader: SharedReader,
    writer: Arc<Mutex<SharedWriter>>,
    index: Arc<HierarchicalIndex>,
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn rebuild_index(
    gen: u64,
    mut reader: BufReaderWithPos<File>,
    index: &HierarchicalIndex,
) -> Result<u64> {
    // To make sure we read from the beginning of the file
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

impl KvsEngine for ReadLockFreeKvStore {
    fn open(path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        fs::create_dir_all(path)?;

        // rebuild index
        let gen_list = sorted_gen_list(path)?;
        let mut uncompacted = 0;
        let index = Arc::new(HierarchicalIndex::default());
        for &gen in &gen_list {
            let reader = BufReaderWithPos::new(File::open(log_path(path, gen))?)?;
            uncompacted += rebuild_index(gen, reader, &index)?;
        }

        // all field
        let path = Arc::new(PathBuf::from(path));
        let reader = SharedReader {
            index: index.clone(),
            path: path.clone(),
            readers: RefCell::new(BTreeMap::new()),
        };
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(log_path(&path, current_gen))?
        )?;
        let writer = Arc::new(Mutex::new(SharedWriter {
            path: path.clone(),
            current_gen: 0,
            uncompacted: 0,
            total: 0,
            writer,
            index: index.clone(),
        }));

        Ok(ReadLockFreeKvStore {
            path,
            reader,
            writer,
            index,
        })
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

// SharedReader cannot sync in thread
struct SharedReader {
    // a index to get from it
    index: Arc<HierarchicalIndex>,
    // a path to get record from it.
    path: Arc<PathBuf>,
    // a seq of readers associated with different gen
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl Clone for SharedReader {
    fn clone(&self) -> Self {
        Self {
            index: Arc::clone(&self.index),
            path: Arc::clone(&self.path),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

impl SharedReader {
    fn get(&self, key: String) -> Result<Option<String>> {
        self.index
            .get(key)
            .map_or(Ok(None), |pos| -> Result<Option<String>> {
                if !self.readers.borrow().contains_key(&pos.gen) {
                    let reader = BufReaderWithPos::new(File::open(log_path(&self.path, pos.gen))?)?;
                    self.readers.borrow_mut().insert(pos.gen, reader);
                }

                let mut binding = self.readers.borrow_mut();
                let reader = binding.get_mut(&pos.gen).unwrap();
                // seek and read
                reader.seek(SeekFrom::Start(pos.pos))?;
                let value = serde_json::from_reader(reader.take(pos.len))?;
                Ok(Some(value))
            })
    }
}

struct SharedWriter {
    // base file path
    path: Arc<PathBuf>,
    // the current writer gen
    current_gen: u64,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction
    uncompacted: u64,
    // the number of bytes has been write total ly
    total: u64,
    // current writer
    writer: BufWriterWithPos<File>,
    // a index is needed for update index
    index: Arc<HierarchicalIndex>,
}

impl SharedWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // 1. write kv into current writer
        // 2. check if index has a key, if has, update it; if not insert it(index is thread safe)
        // 3. check uncompacted bytes > COMPACT_THREHOLD? scroll it and compact
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        self.total += self.writer.pos - pos;
        if let Command::Set { key, .. } = cmd {
            if let Some(cmd_pos) = self
                .index
                .insert(key, (self.current_gen, (pos..self.writer.pos)).into())
            {
                self.uncompacted += cmd_pos.len;
            }
        }        

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        // 1. write kv into current writer
        // 2. check if index has a key, if has, delte it; if not, return an err(index is thread safe)
        // 3. check uncompacted bytes > COMPACT_THREHOLD? scroll it and compact
        let cmd = Command::remove(key);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        self.total += self.writer.pos - pos;
        if let Command::Remove { key } = cmd {
            if let Some(cmd_pos) = self.index.remove(&key) {
                self.uncompacted += cmd_pos.len + self.writer.pos - pos;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    // NOTICE: it has limit that it can onlu compact before last compact finish
    fn compact(&mut self) -> Result<()> {
        // 1. snapshot the index
        // 2. keep gen sequential, the file gen during compaction is lager than the last file gen when snapshot,
        // the file gen in normal wirte after compaction trigger is lager than all gen in compaction
        // 3. run in another thread, so we must ensure what we need can send between thread:
        // index,  snapshot
        // 4. read all record in snapshot, write them into new file and generate new index
        // 5. merge new index into current index,

        fn compact_process(index: Arc<HierarchicalIndex>, gen: u64, path: PathBuf) -> Result<()> {
            let mut writer = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(log_compact_path(&path, gen))?;
            let mut readers = BTreeMap::<u64, File>::new();
            let res = index.snapshot_read(|key, cmd_pos| -> Result<()> {
                // rewrite it into new log
                if !readers.contains_key(&cmd_pos.gen) {
                    readers.insert(cmd_pos.gen, File::open(log_path(&path, cmd_pos.gen))?);
                }

                let reader = readers.get_mut(&cmd_pos.gen).unwrap();
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;

                io::copy(&mut reader.take(cmd_pos.len), &mut writer)?;
                writer.flush()?;
                Ok(())
            });

            // commit if compact success
            fs::rename(log_compact_path(&path, gen), log_path(&path, gen))?;
            // remove useless readers
            
            // update memory index 
            fs::remove_file(path); // delete useless file
            Ok(())
        }

        let index = self.index.clone();
        let gen = self.current_gen + 1;
        let path = (*self.path).clone();
        
        spawn(move || {
            compact_process(index, gen, path)
        });

        // after spawn compact
        self.uncompacted -= self.index.snapshot();
        self.current_gen += 2;
        self.writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(log_path(&self.path, self.current_gen))?,
        )?;
        Ok(())
    }
}

enum CommandIdx {
    Index(CommandPos),
    Tombstone,
}

/// A thread safe index, it can share between different thread safety
#[derive(Default)]
struct HierarchicalIndex {
    // snapshot is last level, so it can't be has a delete record
    snapshot: SkipMap<String, CommandPos>,
    active: SkipMap<String, CommandIdx>,
}

impl HierarchicalIndex {
    // return old record if replace a record, return none if not
    fn insert(&self, key: String, value: CommandPos) -> Option<CommandPos> {
        todo!()
    }

    // return pos if remove a record, return none if not
    fn remove(&self, key: &String) -> Option<CommandPos> {
        todo!()
    }

    // get from low level first
    // it may be resulting in read amplificatio
    fn get(&self, key: String) -> Option<CommandPos> {
        todo!()
    }

    // produce level snapshot, make level_write into level1_snapshot, return reduced bytes
    fn snapshot(&self) -> u64 {
        todo!()
    }

    fn snapshot_read<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&String, &CommandPos) -> Result<()>,
    {
        for item in (&self.snapshot).into_iter() {
            f(item.key(), item.value())?;
        }
        Ok(())
    }
}

impl SharedKvStore {
    /// - execute opportunity：a separate thread to execute it ，just rewrite those data into new file and lock free
    /// - index data race：lock index and replace those rewrite data key into current index when the compact is completed.
    ///  write operation and compact operation will both update index，so they should lock when they update index
    /// - compact fail tolerated: should keep  data consistency even if an error occurred during compact process
    /// （just do not replace those new compacted file key into index ）
    /// - Hierarchical index：it's a little bit like lsm index, but now it has only two level, one is for write, which
    ///   could be modify ;one is for compact, it's a snapshot and it cann't be modify.
    /// - Tombstone mechanism：now it is a lsm index,so delete record should be recored as a tombstone.
    pub fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        let mut new_pos = 0; // pos in the new log file
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files
        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        self.uncompacted = 0;

        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(ErrorCode::UnexpectedCommandType.into())
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Error
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(ErrorCode::RmKeyNotFound.into())
        }
    }
}

impl KvsEngine for KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    fn open(path: &Path) -> Result<KvStore> {
        fs::create_dir_all(path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let gen_list = sorted_gen_list(path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(path, current_gen, &mut readers)?;

        Ok(KvStore {
            inner: Arc::new(RwLock::new(SharedKvStore {
                path: path.to_path_buf(),
                readers,
                writer,
                current_gen,
                index,
                uncompacted,
            })),
        })
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner.write().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.inner.write().unwrap().get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.inner.write().unwrap().remove(key)
    }
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

/// Returns sorted generation numbers in the given directory
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    // To make sure we read from the beginning of the file
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn log_compact_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.tmp", gen))
}

/// Struct representing a command
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

/// Represents the position and length of a json-serialized command in the log
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
