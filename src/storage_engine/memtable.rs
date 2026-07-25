use std::fs::{self, File, OpenOptions, read_dir};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::{collections::HashMap, io};
use wal::segment::{OpType, RecoveryError, Segment, final_entry_after};
use wal::{Wal, segment::WalEntry};

mod wal;

pub struct MemTable {
    // TODO: maybe need a custom data type, since we might want to store the LSN as well
    table: HashMap<String, Option<String>>,
    wal: wal::Wal,
}

impl MemTable {
    pub fn start(dir: PathBuf, segment_size: u32, sequence_number: u64) -> io::Result<Self> {
        let segments = MemTable::find_and_open_segments(&dir, &segment_size, &sequence_number)?;
        if !segments.is_empty() {
            let mut table = HashMap::new();
            match MemTable::recover(
                segments,
                (sequence_number % segment_size as u64).try_into().unwrap(),
                &mut table,
            ) {
                Ok(_) => {
                    let segment = MemTable::open_last_segment(&dir, &segment_size)?;
                    let wal = Wal::from_segment(dir, segment, segment_size);
                    return Ok(MemTable {
                        table: table,
                        wal: wal,
                    });
                }
                Err(e) => match e {
                    // TODO: this needs to be logged
                    RecoveryError::Corrupted(fp, offset) => {
                        let segment =
                            MemTable::truncate_and_open_segment_file(fp, offset, segment_size)?;
                        let file_paths = MemTable::list_segment_files(&dir, &segment.next_lsn())?;
                        for fp in file_paths {
                            fs::remove_file(fp)?;
                        }
                        let wal = Wal::from_segment(dir, segment, segment_size);
                        return Ok(MemTable {
                            table: table,
                            wal: wal,
                        });
                    }
                    RecoveryError::Io(err) => {
                        return Err(err);
                    }
                },
            }
        } else {
            let wal = Wal::create_new(dir, segment_size)?;
            Ok(MemTable {
                table: HashMap::new(),
                wal: wal,
            })
        }
    }

    pub fn put(&mut self, key: String, value: String) -> io::Result<Option<String>> {
        let entry = WalEntry {
            operation_type: wal::segment::OpType::Put,
            key: key.clone(),
            value: Some(value.clone()),
            sequence_number: self.wal.next_sequence_number(),
        };
        self.wal.append(&entry)?;
        Ok(self.table.insert(key, Some(value)).flatten())
    }

    pub fn get(&self, key: &String) -> Option<String> {
        self.table.get(key).cloned().flatten()
    }

    pub fn delete(&mut self, key: &String) -> io::Result<Option<String>> {
        let entry = WalEntry {
            operation_type: wal::segment::OpType::Delete,
            key: key.clone(),
            value: None,
            sequence_number: self.wal.next_sequence_number(),
        };
        self.wal.append(&entry)?;
        Ok(self.table.insert(key.clone(), None).flatten())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.wal.sync()
    }

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
        starting_offset: u32,
        table: &mut HashMap<String, Option<String>>,
    ) -> Result<(), RecoveryError> {
        let mut offset = starting_offset;
        let mut partial_entry: Option<Vec<u8>> = None;
        for mut segment in segments {
            let mut entries = Vec::<WalEntry>::new();
            match partial_entry {
                None => {
                    let res = segment.read_parse_validate_from_offset(&mut entries, offset);
                    for entry in entries {
                        match entry.operation_type {
                            OpType::Delete => _ = table.remove(&entry.key),
                            OpType::Put => _ = table.insert(entry.key, entry.value),
                        }
                    }
                    offset = 0;
                    match res {
                        Ok(opt) => partial_entry = opt,
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                Some(bytes) => {
                    let res = segment.read_parse_validate_from_partial_record(bytes, &mut entries);
                    for entry in entries {
                        match entry.operation_type {
                            OpType::Delete => _ = table.remove(&entry.key),
                            OpType::Put => _ = table.insert(entry.key, entry.value),
                        }
                    }
                    match res {
                        Ok(opt) => partial_entry = opt,
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn basic_put_get() {
        let dir = PathBuf::from("./memtable_basic_put_get");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let mut memtable = MemTable::start(dir, segment_size, 0).unwrap();
        let res = memtable.put(String::from("key"), String::from("value"));
        assert!(matches!(res, Ok(None)));
        let v = memtable.get(&String::from("key"));
        assert!(matches!(v, Some(value) if value == String::from("value")));
    }

    #[test]
    fn put_twice() {
        let dir = PathBuf::from("./memtable_put_twice");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let mut memtable = MemTable::start(dir, segment_size, 0).unwrap();
        let res = memtable.put(String::from("key"), String::from("value1"));
        assert!(matches!(res, Ok(None)));
        let old_value = memtable.put(String::from("key"), String::from("value2"));
        assert!(matches!(old_value, Ok(Some(value)) if value == String::from("value1")));
        let v = memtable.get(&String::from("key"));
        assert!(matches!(v, Some(value) if value == String::from("value2")));
    }

    #[test]
    #[should_panic]
    fn put_empty_key() {
        let dir = PathBuf::from("./memtable_put_empty_key");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let mut memtable = MemTable::start(dir, segment_size, 0).unwrap();
        let _ = memtable.put(String::from(""), String::from("value"));
    }

    #[test]
    fn get_empty_key() {
        let dir = PathBuf::from("./memtable_get_empty_key");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let memtable = MemTable::start(dir, segment_size, 0).unwrap();
        let res = memtable.get(&String::from(""));
        assert!(matches!(res, None));
    }

    #[test]
    fn delete() {
        let dir = PathBuf::from("./memtable_delete");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let mut memtable = MemTable::start(dir, segment_size, 0).unwrap();
        let mut res = memtable.delete(&String::from("key"));
        assert!(matches!(res, Ok(None)));
        res = memtable.put(String::from("key"), String::from("value"));
        assert!(matches!(res, Ok(None)));
        let v = memtable.get(&String::from("key"));
        assert!(matches!(v, Some(value) if value == String::from("value")));
        res = memtable.delete(&String::from("key"));
        assert!(matches!(res, Ok(Some(s)) if s == String::from("value")));
        let value = memtable.get(&String::from("key"));
        assert!(matches!(value, None));
    }

    #[test]
    fn recover_single_segment() {
        let dir = PathBuf::from("./memtable_recover_single_segment");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 4096;
        assert!(cl.setup().is_ok());
        {
            let mut memtable = MemTable::start(dir.clone(), segment_size.clone(), 0).unwrap();
            let _ = memtable.put(String::from("key1"), String::from("value1"));
            let _ = memtable.put(String::from("key1"), String::from("new_value1"));
            let _ = memtable.put(String::from("key2"), String::from("value2"));
            let _ = memtable.delete(&String::from("key2"));
            let _ = memtable.put(String::from("key3"), String::from("value3"));
        }

        let memtable = MemTable::start(dir, segment_size.clone(), 0).unwrap();
        let mut res = memtable.get(&String::from("key1"));
        assert!(matches!(res, Some(value) if value == String::from("new_value1")));
        res = memtable.get(&String::from("key2"));
        assert!(matches!(res, None));
        res = memtable.get(&String::from("key3"));
        assert!(matches!(res, Some(value) if value == String::from("value3")));
    }
}

/*
Implementation thoughts
1.
*/
