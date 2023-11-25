use std::{collections::{BTreeMap, HashMap}, io::{Write, Seek, SeekFrom, Read}, fs::{OpenOptions, self}, path::{Path, PathBuf}, ops::Range, ffi::OsStr};
use serde_derive::{Deserialize, Serialize};
use crate::io::{Writer, Reader};
use crate::error::{Result, ErrorCode};

#[derive(Serialize, Deserialize)]
pub enum Command {
    Set{ key: String, value: String},
    Rm{ key: String },
}

impl Command {
    fn set(key: &String, value: String) -> Command {
        Command::Set{ key: key.clone(), value }
    }
    
    fn rm(key: &String) -> Command {
        Command::Rm { key: key.clone() }
    }
}

/// once uncompacted data increse to this threshold, trigger compact
pub const COMPACTABLE_THRESHOLD: u64 = 32 * 1024;  // 32KB
pub const COMPACTED_ONCE_BYTES: u64 = 16 * 1024;  // 16KB
pub const FILE_THRESHOLD: u64 = 32 * 1024;  // 32KB

#[derive(Serialize, Deserialize, Debug)]
struct Pointer {
    // data file version
    seq: u64,
    pos: u64,
    len: u64,
}



#[derive(Default)]
struct Statistics {
    // every uncompacted bytes in each file
    uncompacted: HashMap<u64, u64>,
    // total uncompacted bytes
    total_uncompacted: u64,
}

pub struct KvStore {
    // current version
    sequence_no: u64,
    // current path
    path: PathBuf,
    // all readers
    readers: BTreeMap<u64, Reader>,
    // only one writer, once compact 
    writer: Writer,
    // memory index
    index: HashMap<String, Pointer>,
    // uncompacted data
    stats: Statistics,
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
        let mut seq_list: Vec<u64> = fs::read_dir(&path)?
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
        seq_list.sort_unstable();
        //println!("all files is {:#?}", &seq_list);
        

        let mut index: HashMap<String, Pointer> = HashMap::new();
        let mut stats = Statistics::default();
        let mut readers: BTreeMap<u64, Reader> = BTreeMap::new();

        //println!("load from {:#?}", seq_list);
        for seq in seq_list.iter() {
            readers.insert(seq.clone(), Self::load(path, seq.clone(), &mut index, &mut stats)?);
        }
        let sequence_no = seq_list.pop().map_or(1, |seq| seq + 1 );
        //println!("open writer {}", sequence_no);
        let writer = Writer::new(
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(path.join(sequence_no.to_string() + ".log"))?
        );
        readers.insert(sequence_no, 
            Reader::new(OpenOptions::new()
                .read(true)
                .open(path.join(sequence_no.to_string() + ".log"))?
            )
        );
        Ok(KvStore{
            sequence_no,
            path: path.into(),
            readers,
            writer,
            index,
            stats,
        })
    }
    
    /// Reload all data into memory, build memory index
    fn load(path: &Path, seq: u64, index: &mut HashMap<String, Pointer>, stats: &mut Statistics) -> Result<Reader> {
        let mut reader = Reader::new(
            OpenOptions::new()
                .read(true)
                .open(path.join(seq.to_string() + ".log"))?
        );
        reader.seek(SeekFrom::Start(0))?;
        let mut iter = serde_json::Deserializer::from_reader(&mut reader).into_iter::<Command>();
        let mut last_offset = iter.byte_offset();
        while let Some(cmd) = iter.next() {
            match cmd? {
                Command::Set{ key, ..} => {
                    if let Some(old_record) = index.insert(key, Pointer {
                        seq,
                        pos: last_offset as u64, 
                        len: (iter.byte_offset() - last_offset) as u64,
                    }) {
                        stats.total_uncompacted += old_record.len;
                        stats.uncompacted.entry(seq)
                            .and_modify(|x| *x += old_record.len)
                            .or_insert(old_record.len);
                    }
                }
                Command::Rm { key } => {
                    if let Some(old_record) = index.remove(&key) {
                        stats.uncompacted
                                .entry(seq)
                                .and_modify(|x| *x += old_record.len)
                                .or_insert(old_record.len);    
                        stats.total_uncompacted += old_record.len;
                    }
                    stats.uncompacted.entry(seq)
                        .and_modify(|x| *x += (iter.byte_offset() - last_offset) as u64)
                        .or_insert((iter.byte_offset() - last_offset) as u64);
                    stats.total_uncompacted += (iter.byte_offset() - last_offset) as u64;
                },
            }
            last_offset = iter.byte_offset();
        }
        Ok(reader)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let set = Command::set(&key, value);
        let pos = self.writer.pos()?;
        serde_json::to_writer(&mut self.writer, &set)?;
        self.writer.flush()?;
        let new_pos = self.writer.pos()?;
        if let Some(old_record) = self.index.insert(key, Pointer{
            seq: self.sequence_no,
            pos,
            len: new_pos - pos,
        }) {
            self.stats.uncompacted.entry(old_record.seq)
                .and_modify(|v| *v += old_record.len)
                .or_insert(old_record.len);
            self.stats.total_uncompacted += old_record.len;
        }
        
        self.try_trigger_compact()?;
        self.try_trigger_scroll()?;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(index) => {
                let reader = self.readers
                    .get_mut(&index.seq)
                    .expect(&format!("Invalid seq {} for current readers", &index.seq));
                //println!("load from {} len {}", index.pos, index.len);
                reader.seek(SeekFrom::Start(index.pos))?;
                let cmd_reader = reader.take(index.len);
                match serde_json::from_reader(cmd_reader)? {
                    Command::Set{value, ..} => Ok(Some(value)),
                    _ => Err(ErrorCode::InternalError(format!("invalid cmd at key {}", key)).into())
                }
            }
            None => Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let rm = Command::rm(&key);
        let pos = self.writer.pos()?;
        serde_json::to_writer(&mut self.writer, &rm)?;
        let new_pos = self.writer.pos()?;
        match self.index.remove(&key) {
            Some(old_record) => {
                self.stats.uncompacted
                    .entry(self.sequence_no)
                    .and_modify(|x| *x += new_pos - pos)
                    .or_insert(new_pos - pos);
                self.stats.uncompacted
                    .entry(old_record.seq)
                    .and_modify(|f| *f += old_record.len)
                    .or_insert(old_record.len);
                self.stats.total_uncompacted += old_record.len + new_pos - pos
            } 
            None => return Err(ErrorCode::RmError(key).into())
        }
        
        self.try_trigger_compact()?;
        self.try_trigger_scroll()?;
        Ok(())
    }

    fn try_trigger_compact(&mut self) -> Result<()> {
        if self.stats.total_uncompacted >= COMPACTABLE_THRESHOLD {
            // sort it by uncompacted bytes
            let mut to_be_compacted_bytes = 0_u64;
            let mut to_be_compacted_seqs = Vec::new();
            
            let mut uncompacted_entrys: Vec<(&u64, &mut u64)> = self.stats.uncompacted.iter_mut().collect();
            uncompacted_entrys.sort_by(|a, b|  b.1.cmp(&a.1) );
            
            for entry in uncompacted_entrys.iter() {
                to_be_compacted_seqs.push(*entry.0);
                to_be_compacted_bytes += *entry.1;
                if to_be_compacted_bytes >= COMPACTED_ONCE_BYTES {
                    break;
                }    
            }
            //println!("compact seq is {:#?}", to_be_compacted_seqs);
            
            
            // begin compacted 
            
                let begin_compact_seq = self.sequence_no + 1;
                let mut compact_seq = self.sequence_no + 1;
                self.scroll((to_be_compacted_seqs.len() + 1) as u64)?;  // it must before compact
                let mut new_index: HashMap<String, Pointer> = HashMap::new();
                let mut compact_writer = Writer::new(
                    OpenOptions::new()
                        .append(true)
                        .create_new(true)
                        .open(self.path.join(compact_seq.to_string() + ".tmp"))?
                );
                //println!("aaaaa");
                //println!("all to be compacted seqs is {:#?}, new seqs is {:#?}", to_be_compacted_seqs, begin_compact_seq);

                // println!("all entrys is  {:#?}", self.index);
                for key in self.index.keys().into_iter() {
                    if let Some(pointer) = self.index.get(key) 
                        && to_be_compacted_seqs.contains(&pointer.seq) 
                    {
                        let reader = self.readers
                            .get_mut(&pointer.seq)
                            .expect(&format!("Invalid seq {} for current readers", &pointer.seq));
                        if reader.pos()? != pointer.pos {
                            reader.seek(SeekFrom::Start(pointer.pos))?;
                        }
                        reader.take(pointer.len);
                        let pos = compact_writer.pos()?;
                        new_index.insert(key.clone(), Pointer {
                            seq: compact_seq,
                            pos: pos,
                            len: pointer.len,
                        });
                        std::io::copy(reader, &mut compact_writer)?;
                        //println!("compact new record {} to {}", pos, pos+pointer.len);
                        compact_writer.seek(SeekFrom::Start(pos + pointer.len))?;
                        

                        // once writer over threshold, scroll it
                        if compact_writer.pos()? >= FILE_THRESHOLD {
                            compact_seq += 1;
                            compact_writer = Writer::new(
                                OpenOptions::new()
                                    .append(true)
                                    .create_new(true)
                                    .open(self.path.join(compact_seq.to_string() + ".tmp"))?
                            );
                        }
                    }
                }
                let end_compact_seq = compact_seq + 1;
                
                // commit compacte, any error happen in commit cannot impact eventual consistency
                self.commit_compact(
                    begin_compact_seq..end_compact_seq, 
                    to_be_compacted_seqs,
                    to_be_compacted_bytes,
                    new_index)?;
            
            
        }
        Ok(())
    }

    fn commit_compact(
        &mut self, 
        after_compact_seqs: Range<u64>, 
        to_be_compacted_seqs: Vec<u64>, 
        to_be_compacted_bytes: u64,
        new_index: HashMap<String, Pointer>,
    ) -> Result<()> {
        // rename file
        for after_compact_seq in after_compact_seqs {
            std::fs::rename(
                self.path.join(after_compact_seq.to_string() + ".tmp"),
                 self.path.join(after_compact_seq.to_string() + ".log"))?;
        }
        // delete file
        for seq in to_be_compacted_seqs.iter() {
            std::fs::remove_file(self.path.join(seq.to_string() + ".log"))?;
        }
        // remove stats
        for compacted_seq in to_be_compacted_seqs.iter() {
            self.stats.uncompacted.remove(compacted_seq).expect("remove invalid seq");
        }
        self.stats.total_uncompacted -= to_be_compacted_bytes;
        // update memory index
        self.index.extend(new_index);
        Ok(())
    }

    fn try_trigger_scroll(&mut self) -> Result<()> {
        if self.writer.pos()? >= FILE_THRESHOLD {
            self.scroll(1)?;    
        }
        Ok(())
    }
    
    fn scroll(&mut self, scroll_step: u64) -> Result<()> {
        self.sequence_no += scroll_step;
        self.writer = Writer::new(
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(self.path.join(self.sequence_no.to_string() + ".log"))?
        );
        let reader = Reader::new(
            OpenOptions::new()
                .read(true)
                .open(self.path.join(self.sequence_no.to_string() + ".log"))?
        );
        self.readers.insert(self.sequence_no, reader);
        Ok(())
    }
}
