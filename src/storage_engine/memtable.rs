use std::fs::{File, read_dir};
use std::path::PathBuf;
use std::{collections::HashMap, io};
use wal::segment::{OpType, RecoveryError, Segment, final_entry_after};
use wal::{Wal, segment::WalEntry};

mod wal;

pub struct MemTable {
    table: HashMap<String, Option<String>>,
    wal: wal::Wal,
}

/* impl Clone for MemTable {
    fn clone(&self) -> Self {
        MemTable {
            table: self.table.clone(),
        }
    }
} */

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
                // TODO: still need to initialize the WAL from the final segment
                Ok(_) => {
                    let wal = Wal::create_new(dir, segment_size)?;
                    return Ok(MemTable {
                        table: table,
                        wal: wal,
                    });
                }
                Err(e) => match e {
                    // TODO: this needs to be logged
                    RecoveryError::Corrupted => {
                        let wal = Wal::create_new(dir, segment_size)?;
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
        match self.table.insert(key, Some(value)) {
            Some(opt) => Ok(opt),
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &String) -> Option<String> {
        match self.table.get(key) {
            Some(opt) => match opt {
                Some(v) => Some(v.clone()),
                None => None,
            },
            None => None,
        }
    }

    pub fn delete(&mut self, key: &String) -> io::Result<Option<String>> {
        let entry = WalEntry {
            operation_type: wal::segment::OpType::Delete,
            key: key.clone(),
            value: None,
            sequence_number: self.wal.next_sequence_number(),
        };
        self.wal.append(&entry)?;
        match self.table.insert(key.clone(), None) {
            Some(opt) => Ok(opt),
            None => Ok(None),
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
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
        let mut segment_file_paths = Vec::<PathBuf>::new();
        for entry in read_dir(dir)? {
            let d = entry?;
            if d.file_type()?.is_file()
                && final_entry_after(
                    d.path().to_str().unwrap(),
                    d.metadata()?.len(),
                    &sequence_number,
                )
            {
                segment_file_paths.push(d.path());
            }
        }
        segment_file_paths.sort();
        for fp in segment_file_paths {
            let segment_file = File::open(fp)?;
            let file_size = segment_file.metadata()?.len();
            if file_size <= *segment_size as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "file size exceeded given max segment size",
                ));
            }
            segments.push(Segment::from(
                segment_file,
                file_size as u32,
                segment_size.clone(),
            ));
        }
        Ok(segments)
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

/*
Implementation thoughts
1. Soft deletes
    1. Not sure about this, since it is only the MemTable, for SSTable I would indeed prefer it
    2. This probably depends on the the implementation. If we use a vector, then soft deletes are the better choice
       from a performance standpoint, but for a hash map probably not.
2. Metadata? I.e. use a custom struct instead of String as value?
3. Using vector instead of HashMap?
4. We would want to configure a maximum size for the MemTable so that we can flush to the SSTable when it is full

Testing TODO:
1. Table driven tests for crud operations
    1. Added value can be retrieved
    2. Absent value returns None both for get and delete
    3. Updating existing key returns previous value
2. Len method
*/

/* #[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn put_get() {
        let mut table = MemTable::new();
        let key = String::from("key");
        let key_read = key.clone();
        let value = String::from("value");
        let val_read = value.clone();
        let result = table.put(key, value);
        assert!(result.is_none());
        let len = table.len();
        assert_eq!(len, 1);
        let res = table.get(&key_read);
        assert!(res.is_some());
        assert_eq!(res.unwrap(), val_read);
    }
} */
