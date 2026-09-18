use std::{collections::HashMap, io, path::PathBuf, sync::Arc};

use crate::storage_engine::memtable::MemTableValue;
use crate::storage_engine::sstable::SsTable;
use crate::storage_engine::sstable::level::segment::SegmentWriter;

pub trait MemTableFlush {
    fn flush(&self, table: &HashMap<String, MemTableValue>) -> io::Result<()>;
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
    fn flush(&self, table: &HashMap<String, MemTableValue>) -> io::Result<()> {
        let fp = SegmentWriter::segment_file_from_memtable(
            &self.dir,
            table,
            &(self.sstable.highest_segment_number(&0) + 1),
            &self.index_sparsity_factor,
        )?;
        self.sstable.find_and_add_segment(fp)
    }
}
