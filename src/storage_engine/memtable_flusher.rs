use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::storage_engine::{
    memtable::MemTableValue,
    sstable::{
        SsTable, SsTableEntry,
        level::{SegmentFooter, SegmentIndex},
    },
};

pub trait MemTableFlush {
    fn flush(&self, table: &HashMap<String, MemTableValue>, sequence_number: u64)
    -> io::Result<()>;
}

pub struct Flusher {
    sstable: Arc<SsTable>,
    dir: PathBuf,
    index_sparsity_factor: Arc<u32>,
}

impl Flusher {
    pub fn from(sstable: Arc<SsTable>, dir: PathBuf, sparsity_factor: Arc<u32>) -> Self {
        Flusher {
            sstable: sstable,
            dir: dir,
            index_sparsity_factor: sparsity_factor,
        }
    }

    fn write_segment_file(
        &self,
        table: &HashMap<String, MemTableValue>,
        sequence_number: u64,
    ) -> io::Result<PathBuf> {
        let filename = Flusher::determine_segment_filename(sequence_number);
        let filepath = self.dir.join(filename);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath.clone())?;
        let mut writer = BufWriter::new(file);
        let mut keys: Vec<String> = table.keys().cloned().collect();
        keys.sort_unstable();
        let mut index = SegmentIndex::new();
        let mut counter: u32 = 0;
        let mut offset: u64 = 0;
        for key in keys {
            if counter % self.index_sparsity_factor.as_ref() == 0 {
                index.add_index(key.clone(), offset.clone());
            }
            let value = table.get(&key).unwrap().clone();
            let entry = SsTableEntry::from(key, value);
            writer.write_all(&entry.to_bytes())?;
            counter += 1;
            offset += entry.len() as u64;
        }
        writer.write_all(&index.to_bytes())?;
        let footer = SegmentFooter::from(offset);
        writer.write_all(&footer.to_bytes())?;
        writer.flush()?;
        Ok(filepath)
    }

    fn determine_segment_filename(sequence_number: u64) -> String {
        let mut padding_bytes = Vec::<u8>::new();
        let level_number_str = String::from("00000000");
        let mut sequence_number_hex_str = format!("{:x}", sequence_number);
        padding_bytes.resize(8 - sequence_number_hex_str.len(), 48); // "0" = 0x30 = 48
        sequence_number_hex_str.insert_str(0, &String::from_utf8(padding_bytes).unwrap());
        level_number_str + &sequence_number_hex_str
    }
}

impl MemTableFlush for Flusher {
    fn flush(
        &self,
        table: &HashMap<String, MemTableValue>,
        sequence_number: u64,
    ) -> io::Result<()> {
        let fp = self.write_segment_file(table, sequence_number)?;
        self.sstable.find_and_add_segment(fp)
    }
}

impl SsTableEntry {
    pub fn from(key: String, value: MemTableValue) -> Self {
        SsTableEntry {
            key: key,
            value: value.value,
            sequence_number: value.sequence_number,
        }
    }
}
