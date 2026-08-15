use std::fs::{self, File, OpenOptions, read_dir};
use std::io::ErrorKind;
use std::marker::Sync;
use std::path::PathBuf;
use std::sync::{
    Arc, RwLock,
    mpsc::{Receiver, Sender, channel},
};
use std::thread::JoinHandle;
use std::{collections::HashMap, io};
use wal::segment::{OpType, RecoveryError, Segment, final_entry_after};
use wal::{Wal, segment::WalEntry};

use crate::storage_engine::memtable_flusher::MemTableFlush;
use crate::storage_engine::sstable::SsTableEntry;

mod wal;

#[derive(Clone)]
pub struct MemTableValue {
    pub value: Option<String>,
    pub sequence_number: u64,
}

enum WriteJob {
    Delete(String, Sender<io::Result<Option<MemTableValue>>>),
    Put(String, String, Sender<io::Result<Option<MemTableValue>>>),
}

pub struct MemTable {
    table: Arc<RwLock<HashMap<String, MemTableValue>>>,
    sender: Sender<WriteJob>,
    writer_handle: JoinHandle<()>,
}

struct WriteWorker<T>
where
    T: MemTableFlush + Send + Sync + Sized + 'static,
{
    table: Arc<RwLock<HashMap<String, MemTableValue>>>,
    receiver: Receiver<WriteJob>,
    wal: wal::Wal,
    flusher: T,
    current_size: u64,
    max_size: u64,
}

impl MemTable {
    pub fn start<T: MemTableFlush + Send + Sync + Sized + 'static>(
        dir: PathBuf,
        segment_size: u32,
        sequence_number: u64,
        max_size: u64,
        flusher: T,
    ) -> io::Result<Self> {
        let segments = MemTable::find_and_open_segments(&dir, &segment_size, &sequence_number)?;
        if !segments.is_empty() {
            let mut table = HashMap::new();
            match MemTable::recover(segments, sequence_number % segment_size as u64, &mut table) {
                Ok(curr_size) => {
                    let segment = MemTable::open_last_segment(&dir, &segment_size)?;
                    let wal = Wal::from_segment(dir.join("WAL"), segment, segment_size);
                    return Ok(MemTable::from(table, wal, flusher, curr_size, max_size));
                }
                Err((curr_size, e)) => match e {
                    // TODO: this needs to be logged
                    RecoveryError::Corrupted(fp, offset) => {
                        let segment =
                            MemTable::truncate_and_open_segment_file(fp, offset, segment_size)?;
                        let file_paths = MemTable::list_segment_files(&dir, &segment.next_lsn())?;
                        for fp in file_paths {
                            fs::remove_file(fp)?;
                        }
                        let wal = Wal::from_segment(dir.join("WAL"), segment, segment_size);
                        return Ok(MemTable::from(table, wal, flusher, curr_size, max_size));
                    }
                    RecoveryError::Io(err) => {
                        return Err(err);
                    }
                },
            }
        } else {
            let wal = Wal::create_new(dir, segment_size)?;
            Ok(MemTable::from(HashMap::new(), wal, flusher, 0, max_size))
        }
    }

    fn from<T: MemTableFlush + Send + Sync + Sized + 'static>(
        table: HashMap<String, MemTableValue>,
        wal: Wal,
        flusher: T,
        curr_size: u64,
        max_size: u64,
    ) -> Self {
        let table_ptr = Arc::new(RwLock::new(table));
        let (tx, rx) = channel();
        let mut worker = WriteWorker {
            receiver: rx,
            table: table_ptr.clone(),
            wal: wal,
            flusher: flusher,
            current_size: curr_size,
            max_size: max_size,
        };
        let handle = std::thread::spawn(move || worker.run());
        MemTable {
            table: table_ptr.clone(),
            sender: tx,
            writer_handle: handle,
        }
    }

    pub fn put(&self, key: String, value: String) -> io::Result<Option<MemTableValue>> {
        // TODO: panic when key empty?
        let (tx, rx) = channel();
        let job = WriteJob::Put(key, value, tx);
        // TODO: maybe the send and receive errors need to be handled differently
        self.sender
            .send(job)
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match rx.recv() {
            Err(_) => Err(io::Error::from(io::ErrorKind::NotConnected)),
            Ok(r) => r,
        }
    }

    pub fn get(&self, key: &String) -> Option<MemTableValue> {
        self.table.read().unwrap().get(key).cloned()
    }

    pub fn delete(&self, key: &String) -> io::Result<Option<MemTableValue>> {
        // TODO: panic when key empty?
        let (tx, rx) = channel();
        let job = WriteJob::Delete(key.clone(), tx);
        // TODO: maybe the send and receive errors need to be handled differently
        self.sender
            .send(job)
            .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
        match rx.recv() {
            Err(_) => Err(io::Error::from(io::ErrorKind::NotConnected)),
            Ok(r) => r,
        }
    }

    /* pub fn sync(&mut self) -> io::Result<()> {
        self.wal.sync()
    } */

    fn find_and_open_segments(
        dir: &PathBuf,
        segment_size: &u32,
        sequence_number: &u64,
    ) -> io::Result<Vec<Segment>> {
        let mut segments = Vec::<Segment>::new();
        let segment_file_paths = MemTable::list_segment_files(dir, sequence_number)?;
        for fp in segment_file_paths {
            let segment_file = File::open(&fp)?;
            let file_size = segment_file.metadata()?.len();
            if file_size > *segment_size as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "file size exceeded given max segment size",
                ));
            }
            segments.push(Segment::from(
                segment_file,
                fp,
                file_size as u32,
                segment_size.clone(),
            ));
        }
        Ok(segments)
    }

    fn open_last_segment(dir: &PathBuf, segment_size: &u32) -> io::Result<Segment> {
        MemTable::list_segment_files(dir, &0)?.last().map_or(
            Err(io::Error::from(io::ErrorKind::NotFound)),
            |fp| {
                let segment_file = OpenOptions::new().write(true).create(false).open(fp)?;
                let file_size = segment_file.metadata()?.len();
                if file_size > *segment_size as u64 {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "file size exceeded given max segment size",
                    ));
                }
                Ok(Segment::from(
                    segment_file,
                    fp.clone(),
                    file_size as u32,
                    segment_size.clone(),
                ))
            },
        )
    }

    fn truncate_and_open_segment_file(
        segment_fp: PathBuf,
        offset: u64,
        segment_size: u32,
    ) -> io::Result<Segment> {
        let dir = segment_fp.parent().unwrap();
        let mut segment_filepaths = MemTable::list_segment_files(&dir.to_path_buf(), &0)?;
        segment_filepaths.sort();
        let filtered_filepaths: Vec<&PathBuf> = segment_filepaths
            .iter()
            .filter(|p| segment_fp.cmp(p).is_le())
            .collect();
        if filtered_filepaths.is_empty() {
            return Err(io::Error::from(ErrorKind::NotFound));
        }
        let segment_file = OpenOptions::new()
            .write(true)
            .create(false)
            .open(&filtered_filepaths.first().unwrap())?;
        segment_file.set_len(offset)?;
        Ok(Segment::from(
            segment_file,
            filtered_filepaths.first().unwrap().to_path_buf(),
            offset as u32,
            segment_size,
        ))
    }

    fn list_segment_files(dir: &PathBuf, sequence_number: &u64) -> io::Result<Vec<PathBuf>> {
        let mut segment_file_paths = Vec::<PathBuf>::new();
        for entry in read_dir(dir)? {
            let d = entry?;
            if d.file_type()?.is_file()
                && final_entry_after(
                    d.file_name().to_str().unwrap(),
                    d.metadata()?.len(),
                    &sequence_number,
                )
            {
                segment_file_paths.push(d.path());
            }
        }
        segment_file_paths.sort();
        Ok(segment_file_paths)
    }

    pub fn recover(
        segments: Vec<Segment>,
        starting_offset: u64,
        table: &mut HashMap<String, MemTableValue>,
    ) -> Result<u64, (u64, RecoveryError)> {
        let mut curr_size: u64 = 0;
        let mut offset = starting_offset;
        let mut partial_entry: Option<(Vec<u8>, u64)> = None;
        let mut path: PathBuf = PathBuf::new();
        for mut segment in segments {
            path = segment.file_path();
            let mut entries = Vec::<WalEntry>::new();
            match partial_entry {
                None => {
                    let res = segment.read_parse_validate_from_offset(&mut entries, offset);
                    for entry in entries {
                        match entry.operation_type {
                            OpType::Delete => {
                                let value = MemTableValue {
                                    sequence_number: entry.sequence_number,
                                    value: entry.value,
                                };
                                curr_size += SsTableEntry::from(entry.key.clone(), value.clone())
                                    .len() as u64;
                                _ = table.insert(entry.key, value);
                            }
                            OpType::Put => {
                                let value = MemTableValue {
                                    sequence_number: entry.sequence_number,
                                    value: entry.value,
                                };
                                curr_size += SsTableEntry::from(entry.key.clone(), value.clone())
                                    .len() as u64;
                                _ = table.insert(entry.key, value);
                            }
                        }
                    }
                    offset = 0;
                    match res {
                        Ok(opt) => partial_entry = opt,
                        Err(e) => {
                            return Err((curr_size, e));
                        }
                    }
                }
                Some((bytes, _)) => {
                    let res = segment.read_parse_validate_from_partial_record(bytes, &mut entries);
                    for entry in entries {
                        match entry.operation_type {
                            OpType::Delete => {
                                let value = MemTableValue {
                                    sequence_number: entry.sequence_number,
                                    value: entry.value,
                                }; // TODO: curr_size is not correct, should subtract first if old value present
                                curr_size += SsTableEntry::from(entry.key.clone(), value.clone())
                                    .len() as u64;
                                _ = table.insert(entry.key, value);
                            }
                            OpType::Put => {
                                let value = MemTableValue {
                                    sequence_number: entry.sequence_number,
                                    value: entry.value,
                                }; // TODO: curr_size is not correct, should subtract first if old value present
                                curr_size += SsTableEntry::from(entry.key.clone(), value.clone())
                                    .len() as u64;
                                _ = table.insert(entry.key, value);
                            }
                        }
                    }
                    match res {
                        Ok(opt) => partial_entry = opt,
                        Err(e) => {
                            return Err((curr_size, e));
                        }
                    }
                }
            }
        }
        match partial_entry {
            Some((_, pos)) => Err((curr_size, RecoveryError::Corrupted(path, pos))),
            None => Ok(curr_size),
        }
    }
}

impl<T> WriteWorker<T>
where
    T: MemTableFlush + Send + Sync + Sized,
{
    fn put(&mut self, key: String, value: String) -> io::Result<Option<MemTableValue>> {
        let sequence_number = self.wal.next_sequence_number();
        let entry = WalEntry {
            operation_type: wal::segment::OpType::Put,
            key: key.clone(),
            value: Some(value.clone()),
            sequence_number: sequence_number.clone(),
        };
        self.wal.append(&entry)?;
        let mvalue = MemTableValue {
            value: Some(value),
            sequence_number: sequence_number,
        };
        let len = SsTableEntry::from(entry.key.clone(), mvalue.clone()).len() as u64;
        let val = self.table.write().unwrap().insert(key, mvalue);
        self.current_size += len; // TODO: This is not correct, have to subtract if there is an old value
        Ok(val) // TODO: handle locking error
    }

    fn delete(&mut self, key: &String) -> io::Result<Option<MemTableValue>> {
        let sequence_number = self.wal.next_sequence_number();
        let entry = WalEntry {
            operation_type: wal::segment::OpType::Delete,
            key: key.clone(),
            value: None,
            sequence_number: sequence_number.clone(),
        };
        self.wal.append(&entry)?;
        let mvalue = MemTableValue {
            sequence_number: sequence_number,
            value: None,
        };
        let len = SsTableEntry::from(entry.key.clone(), mvalue.clone()).len() as u64;
        let val = self.table.write().unwrap().insert(key.clone(), mvalue);
        self.current_size += len; // TODO: This is not correct, have to subtract if there is an old value
        Ok(val)
    }

    fn run(&mut self) {
        while let Ok(job) = self.receiver.recv() {
            if self.current_size >= self.max_size {
                let mut lock = self.table.write().unwrap();
                match self.flusher.flush(&*lock, self.wal.next_sequence_number()) {
                    Ok(_) => {
                        self.current_size = 0;
                        lock.clear();
                    }
                    Err(_) => {
                        todo!() // TODO: log error
                    }
                }
            }
            match job {
                WriteJob::Delete(key, sender) => {
                    let _ = sender.send(self.delete(&key));
                }
                WriteJob::Put(key, value, sender) => {
                    let _ = sender.send(self.put(key, value));
                }
            }
        }
        // TODO: regular sync of WAL
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engine::memtable_flusher::MemTableFlush;
    use std::fs::DirBuilder;
    use std::path::PathBuf;

    struct Cleanup {
        dir: PathBuf,
    }

    impl Cleanup {
        fn setup(&self) -> io::Result<()> {
            let _ = fs::remove_dir_all(&self.dir);
            DirBuilder::new().recursive(true).create(&self.dir)
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    struct FakeFlusher {}

    impl MemTableFlush for FakeFlusher {
        fn flush(&self, _: &HashMap<String, MemTableValue>, _: u64) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn basic_put_get() {
        let dir = PathBuf::from("./memtable_basic_put_get");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let res = memtable.put(String::from("key"), String::from("value"));
        assert!(matches!(res, Ok(None)));
        let v = memtable.get(&String::from("key")).unwrap();
        assert!(matches!(v.value, Some(value) if value == String::from("value")));
    }

    #[test]
    fn put_twice() {
        let dir = PathBuf::from("./memtable_put_twice");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let res = memtable.put(String::from("key"), String::from("value1"));
        assert!(matches!(res, Ok(None)));
        let old_value = memtable.put(String::from("key"), String::from("value2"));
        assert!(
            matches!(old_value, Ok(Some(value)) if value.clone().value.unwrap() == String::from("value1"))
        );
        let v = memtable.get(&String::from("key")).unwrap();
        assert!(matches!(v.value, Some(value) if value == String::from("value2")));
    }

    /* TODO: is this test still useful or do we want to allow empty keys? I am tending to allowing empty keys
    #[test]
    #[should_panic]
    fn put_empty_key() {
        let dir = PathBuf::from("./memtable_put_empty_key");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let _ = memtable.put(String::from(""), String::from("value"));
    } */

    #[test]
    fn get_empty_key() {
        let dir = PathBuf::from("./memtable_get_empty_key");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let res = memtable.get(&String::from(""));
        assert!(matches!(res, None));
    }

    #[test]
    fn delete() {
        let dir = PathBuf::from("./memtable_delete");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.delete(&String::from("key"));
        assert!(matches!(res, Ok(None)));
        res = memtable.put(String::from("key"), String::from("value"));
        // This is somewhat counterintuitive, but necessary if we delete keys that are in the sstables,
        // but not in the memtable
        assert!(matches!(res, Ok(Some(_))));
        let mvalue = res.unwrap().unwrap();
        assert!(matches!(mvalue.value, None));
        let v = memtable.get(&String::from("key"));
        assert!(
            matches!(v, Some(value) if value.clone().value.unwrap_or(String::from("")) == String::from("value"))
        );
        res = memtable.delete(&String::from("key"));
        assert!(
            matches!(res, Ok(Some(s)) if s.clone().value.unwrap_or(String::from("")) == String::from("value"))
        );
        let value = memtable.get(&String::from("key")).unwrap();
        assert!(matches!(value.value, None));
    }

    #[test]
    fn recover_single_segment() {
        let dir = PathBuf::from("./memtable_recover_single_segment");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, None));
        res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("value3")));
    }

    #[test]
    fn recover_twice_single_segment() {
        let dir = PathBuf::from("./memtable_recover_twice_single_segment");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }
        /*
        key1: new_value1
        key2: deleted
        key3: value3
        */

        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let mut res = memtable.get(&String::from("key1")).unwrap();
            assert!(matches!(res.value, Some(value) if value == String::from("new_value1")));
            res = memtable.get(&String::from("key2")).unwrap();
            assert!(matches!(res.value, None));
            res = memtable.get(&String::from("key3")).unwrap();
            assert!(matches!(res.value, Some(value) if value == String::from("value3")));
            let _ = memtable.put(String::from("key4"), String::from("value4"));
            let _ = memtable.put(String::from("key2"), String::from("new_value2"));
            let _ = memtable.delete(&String::from("key3"));
        }
        /*
        key1: new_value1
        key2: new_value2
        key3: deleted
        key4: value4
        */

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("new_value2")));
        res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, None));
        res = memtable.get(&String::from("key4")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("value4")));
    }

    #[test]
    fn recover_multiple_segments() {
        let dir = PathBuf::from("./memtable_recover_multiple_segments");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 64;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, None));
        res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("value3")));
    }

    #[test]
    fn recover_twice_multiple_segments() {
        let dir = PathBuf::from("./memtable_recover_twice_multiple_segments");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }
        /*
        key1: new_value1
        key2: deleted
        key3: value3
        */

        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let mut res = memtable.get(&String::from("key1")).unwrap();
            assert!(matches!(res.value, Some(value) if value == String::from("new_value1")));
            res = memtable.get(&String::from("key2")).unwrap();
            assert!(matches!(res.value, None));
            res = memtable.get(&String::from("key3")).unwrap();
            assert!(matches!(res.value, Some(value) if value == String::from("value3")));
            let _ = memtable.put(String::from("key4"), String::from("value4"));
            let _ = memtable.put(String::from("key2"), String::from("new_value2"));
            let _ = memtable.delete(&String::from("key3"));
        }
        /*
        key1: new_value1
        key2: new_value2
        key3: deleted
        key4: value4
        */

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("new_value2")));
        res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, None));
        res = memtable.get(&String::from("key4")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("value4")));
    }

    #[test]
    fn recover_multiple_segments_higher_sequence_no() {
        let dir = PathBuf::from("./memtable_recover_multiple_segments_higher_sequence_no");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 64;
        assert!(cl.setup().is_ok());
        let sequence_number: u64;
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1")); // 0 -> 35
            let _ = memtable.put(String::from("key1"), String::from("new_value1")); //35 -> 74
            let _ = memtable.put(String::from("key2"), String::from("value2")); // 74 -> 109
            let v = memtable.get(&String::from("key2")).unwrap();
            sequence_number = v.sequence_number;
            let _ = memtable.delete(&String::from("key2")); // 109 -> 134
            let _ = memtable.put(String::from("key3"), String::from("value3")); // 134 -> 169
        }

        let memtable = MemTable::start(
            dir,
            segment_size.clone(),
            sequence_number,
            2u64.pow(32),
            FakeFlusher {},
        )
        .unwrap();
        let res = memtable.get(&String::from("key1"));
        assert!(matches!(res, None));
        let res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, None));
        let res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, Some(value) if value == String::from("value3")));
    }

    #[test]
    fn recover_corrupted_wal() {
        let dir = PathBuf::from("./memtable_recover_corrupted_wal");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }

        {
            let file_paths = MemTable::list_segment_files(&dir, &0).unwrap();
            assert_eq!(file_paths.len(), 1);
            let file = OpenOptions::new()
                .create(false)
                .write(true)
                .open(file_paths.first().unwrap())
                .unwrap();
            let file_size = file.metadata().unwrap().len();
            file.set_len(file_size - 4).unwrap();
        }

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, None));
        let res = memtable.get(&String::from("key3"));
        assert!(matches!(res, None));
    }

    #[test]
    fn recover_corrupted_wal_twice() {
        let dir = PathBuf::from("./memtable_recover_corrupted_wal_twice");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }

        {
            let file_paths = MemTable::list_segment_files(&dir, &0).unwrap();
            assert_eq!(file_paths.len(), 1);
            let file = OpenOptions::new()
                .create(false)
                .write(true)
                .open(file_paths.first().unwrap())
                .unwrap();
            let file_size = file.metadata().unwrap().len();
            file.set_len(file_size - 4).unwrap();
        }

        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let mut res = memtable.get(&String::from("key1")).unwrap();
            assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
            res = memtable.get(&String::from("key2")).unwrap();
            assert!(matches!(res.value, None));
            let res = memtable.get(&String::from("key3"));
            assert!(matches!(res, None));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("new_value3"));
            let _ = memtable.put(String::from("key4"), String::from("value4"));
        }

        let memtable = MemTable::start(
            dir.clone(),
            segment_size.clone(),
            0,
            2u64.pow(32),
            FakeFlusher {},
        )
        .unwrap();
        let res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
        let res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, None));
        let res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value3")));
        let res = memtable.get(&String::from("key4")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("value4")));
    }

    #[test]
    fn recover_corrupted_wal_multiple_segments() {
        let dir = PathBuf::from("./memtable_recover_corrupted_wal_multiple_segments");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 64;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
            let _ = memtable.delete(&String::from("key3"));
        }

        {
            let file_paths = MemTable::list_segment_files(&dir, &0).unwrap();
            assert!(file_paths.len() > 1);
            let file = OpenOptions::new()
                .create(false)
                .write(true)
                .open(file_paths.last().unwrap())
                .unwrap();
            let file_size = file.metadata().unwrap().len();
            file.set_len(file_size - 4).unwrap();
        }

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let mut res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
        res = memtable.get(&String::from("key2")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("value2")));
        res = memtable.get(&String::from("key3")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("value3")));
    }

    #[test]
    fn recover_corrupted_wal_multiple_segments_middle() {
        let dir = PathBuf::from("./memtable_recover_corrupted_wal_multiple_segments_middle");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 64;
        assert!(cl.setup().is_ok());
        {
            let memtable = MemTable::start(
                dir.clone(),
                segment_size.clone(),
                0,
                2u64.pow(32),
                FakeFlusher {},
            )
            .unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1")); // 0 -> 35
            let _ = memtable.put(String::from("key1"), String::from("new_value1")); //35 -> 74
            let _ = memtable.put(String::from("key2"), String::from("value2")); // 74 -> 109
            let _ = memtable.delete(&String::from("key2")); // 109 -> 134
            let _ = memtable.put(String::from("key3"), String::from("value3")); // 134 -> 169
        }

        {
            let file_paths = MemTable::list_segment_files(&dir, &0).unwrap();
            assert!(file_paths.len() > 2);
            let file = OpenOptions::new()
                .create(false)
                .write(true)
                .open(file_paths[1].clone())
                .unwrap();
            let file_size = file.metadata().unwrap().len();
            file.set_len(file_size - 4).unwrap();
        }

        let memtable =
            MemTable::start(dir, segment_size.clone(), 0, 2u64.pow(32), FakeFlusher {}).unwrap();
        let res = memtable.get(&String::from("key1")).unwrap();
        assert!(matches!(res.value, Some(v) if v == String::from("new_value1")));
        let res = memtable.get(&String::from("key2"));
        assert!(matches!(res, None));
        let res = memtable.get(&String::from("key3"));
        assert!(matches!(res, None));
    }
    /*
    TODO: corrupted WAL tests -> make sure that recovering twice is correct
     */
}
