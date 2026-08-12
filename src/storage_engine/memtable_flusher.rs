use std::{collections::HashMap, io, path::PathBuf, sync::Arc};

use crate::storage_engine::memtable::MemTableValue;
use crate::storage_engine::sstable::{SsTable, level::Segment};

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
}

impl MemTableFlush for Flusher {
    fn flush(
        &self,
        table: &HashMap<String, MemTableValue>,
        sequence_number: u64,
    ) -> io::Result<()> {
        let fp = Segment::write_segment_file(
            &self.dir,
            table,
            &sequence_number,
            &self.index_sparsity_factor,
        )?;
        self.sstable.find_and_add_segment(fp)
    }
}
