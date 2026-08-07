use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::storage_engine::{
    memtable::MemTableValue,
    sstable::{SsTable, SsTableEntry},
};

pub struct Flusher {
    sstable: Arc<SsTable>,
    dir: PathBuf,
}

impl Flusher {
    pub fn from(sstable: Arc<SsTable>, dir: PathBuf) -> Self {
        Flusher {
            sstable: sstable,
            dir: dir,
        }
    }

    fn write_segment_file(&self, table: HashMap<String, MemTableValue>) -> io::Result<PathBuf> {
        let mut keys: Vec<String> = table.keys().cloned().collect();
        let filename = String::from("segment"); // TODO: real filename
        let filepath = self.dir.join(filename);
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath.clone())?;
        keys.sort_unstable();
        for key in keys {
            let value = table.get(&key).unwrap().clone();
            let entry = SsTableEntry::from(key, value);
            file.write(&entry.to_bytes())?;
        }
        Ok(filepath)
        // TODO: write index and footer
    }

    pub fn flush(&self, table: HashMap<String, MemTableValue>) -> io::Result<()> {
        let fp = self.write_segment_file(table)?;
        self.sstable.find_and_add_segment(fp)
    }
}

impl SsTableEntry {
    fn from(key: String, value: MemTableValue) -> Self {
        SsTableEntry {
            key: key,
            value: value.value,
            sequence_number: value.sequence_number,
        }
    }
}
