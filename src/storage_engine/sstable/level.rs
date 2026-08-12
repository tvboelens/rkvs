use super::SsTableEntry;
use crate::storage_engine::memtable::MemTableValue;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::Arc;

static MAGIC_BYTES: [u8; 4] = [0x72, 0x6B, 0x76, 0x73]; //rkvs

pub struct SegmentIndexEntry {
    key: String,
    offset: u64,
}

struct SegmentIndex {
    indices: Vec<SegmentIndexEntry>,
    len: usize,
}

pub struct Segment {
    file: File,
    filepath: PathBuf,
    index: SegmentIndex,
    highest_sequence_no: u64,
    size: u64,
    number: u64,
}

pub struct SegmentFooter {
    index_offset: u64,
    // bloom_filter_offset: u64,
    // bloom_filter_size: u64
}

pub trait Level {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>>;
    fn segments_to_merge(&self) -> Vec<Arc<Segment>>;
    fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)>;
    fn exceeds_target_size(&self) -> bool;
}

#[derive(Clone)]
pub struct OverlappingLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
}
pub struct PartitionedLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
    highest_segment_number: u64,
}

pub struct SsTableLevel<T>
where
    T: Level,
{
    inner: T,
}

pub struct LevelContainer {
    level_zero: Arc<SsTableLevel<OverlappingLevel>>,
    partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
}

impl LevelContainer {
    pub fn level_zero(&self) -> Arc<SsTableLevel<OverlappingLevel>> {
        self.level_zero.clone()
    }

    pub fn partitioned_levels(&self) -> Vec<Arc<SsTableLevel<PartitionedLevel>>> {
        self.partitioned_levels.clone()
    }

    pub fn swap_partitioned_levels(
        &mut self,
        new_partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) {
        self.partitioned_levels = new_partitioned_levels;
    }

    pub fn swap_all_levels(
        &mut self,
        new_partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) {
        let target_size = self.level_zero.inner.target_size;
        self.level_zero = Arc::new(SsTableLevel::<OverlappingLevel>::new(target_size));
        self.partitioned_levels = new_partitioned_levels;
    }

    pub fn add_to_level_zero(&mut self, path: PathBuf) -> io::Result<()> {
        let segment = Segment::from_file(path)?;
        let mut new_level_zero = self.level_zero.as_ref().clone();
        new_level_zero.add_segment(segment);
        self.level_zero = Arc::new(new_level_zero);
        Ok(())
    }

    pub fn do_compact(&self) -> bool {
        self.level_zero.exceeds_target_size()
    }
}

impl Segment {
    fn create(
        path: PathBuf,
        mut entries: Vec<SsTableEntry>,
        segment_number: u64,
    ) -> io::Result<Self> {
        let mut highest_sequence_no: u64 = 0;
        entries.sort_unstable_by(|entry1, entry2| entry1.key.cmp(&entry2.key));
        let mut wfile = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&path)?;
        for entry in entries {
            if entry.sequence_number > highest_sequence_no {
                highest_sequence_no = entry.sequence_number.clone();
            }
            wfile.write_all(&entry.to_bytes())?;
        }
        let rfile = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)?;
        // TODO: read index from file or use mmap
        let index = SegmentIndex {
            indices: Vec::new(),
            len: 0,
        };
        let metadata = rfile.metadata()?;
        Ok(Segment {
            file: rfile,
            filepath: path,
            highest_sequence_no: highest_sequence_no,
            index: index,
            size: metadata.size(),
            number: segment_number,
        })
    }

    fn from_file(_path: PathBuf) -> io::Result<Self> {
        todo!()
    }

    pub fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        /*
        This might be suboptimal. Maybe we can extract the offset of the next key as well,
        read the byte stream into memory and traverse that instead of the file.
        */
        let mut offset = self.index.find_closest_offset(key);
        let mut entry: SsTableEntry;
        loop {
            entry = self.read_table_entry(&offset)?;
            if entry.key >= *key {
                break;
            }
            offset += entry.len() as u64;
        }
        if entry.key == *key {
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    fn read_table_entry(&self, offset: &u64) -> io::Result<SsTableEntry> {
        let mut curr_offset = offset.clone();
        let mut buf_offset: usize = 0;
        let mut u32_buf: [u8; size_of::<u32>()] = [0, 0, 0, 0];
        let mut bytes_read: usize = 0;
        while bytes_read < size_of::<u32>() {
            bytes_read += self
                .file
                .read_at(&mut u32_buf[buf_offset..], curr_offset.clone())?;
            buf_offset += bytes_read;
            curr_offset += bytes_read as u64;
        }
        let entry_len = u32::from_le_bytes(u32_buf.clone());
        let mut entry_buf = Vec::<u8>::new();
        entry_buf.resize(entry_len as usize, 0);
        bytes_read = 0;
        buf_offset = 0;
        while bytes_read < entry_len as usize {
            bytes_read += self
                .file
                .read_at(&mut entry_buf[buf_offset..], curr_offset.clone())?;
            buf_offset += bytes_read;
            curr_offset += bytes_read as u64;
        }

        Ok(SsTableEntry::from_bytes(&entry_buf))
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn filepath(&self) -> PathBuf {
        self.filepath.clone()
    }

    pub fn write_segment_file(
        dir: &PathBuf,
        table: &HashMap<String, MemTableValue>,
        sequence_number: &u64,
        index_sparsity_factor: &u32,
    ) -> io::Result<PathBuf> {
        let filename = Segment::determine_segment_filename(sequence_number);
        let filepath = dir.join(filename);
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
            if counter % index_sparsity_factor == 0 {
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

    fn determine_segment_filename(sequence_number: &u64) -> String {
        let mut padding_bytes = Vec::<u8>::new();
        let level_number_str = String::from("00000000");
        let mut sequence_number_hex_str = format!("{:x}", sequence_number);
        padding_bytes.resize(8 - sequence_number_hex_str.len(), 48); // "0" = 0x30 = 48
        sequence_number_hex_str.insert_str(0, &String::from_utf8(padding_bytes).unwrap());
        level_number_str + &sequence_number_hex_str
    }
}

impl Level for OverlappingLevel {
    fn get(&self, _key: &String) -> io::Result<Option<SsTableEntry>> {
        todo!()
    }
    fn segments_to_merge(&self) -> Vec<Arc<Segment>> {
        self.segments.clone()
    }

    fn exceeds_target_size(&self) -> bool {
        let size: u64 = self.segments.iter().map(|segment| segment.size()).sum();
        size > self.target_size
    }

    fn merge(
        &self,
        _segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        todo!()
    }
}

impl Level for PartitionedLevel {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        self.find_containing_segment(key)
            .map(|segment| segment.get(key))
            .transpose()
            .map(|inner_opt| inner_opt.flatten())
    }

    fn segments_to_merge(&self) -> Vec<Arc<Segment>> {
        self.segments.clone()
    }

    fn exceeds_target_size(&self) -> bool {
        let size: u64 = self.segments.iter().map(|segment| segment.size()).sum();
        size > self.target_size
    }

    fn merge(
        &self,
        _segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        todo!()
    }
}

impl PartitionedLevel {
    fn find_containing_segment(&self, _key: &String) -> Option<Arc<Segment>> {
        if self.segments.is_empty() {
            None
        } else {
            todo!()
        }
    }
}

impl<T> SsTableLevel<T>
where
    T: Level,
{
    pub fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        self.inner.get(key)
    }

    pub fn from(level_type: T) -> Self {
        SsTableLevel { inner: level_type }
    }

    pub fn segments_to_merge(&self) -> Vec<Arc<Segment>> {
        self.inner.segments_to_merge()
    }

    pub fn exceeds_target_size(&self) -> bool {
        self.inner.exceeds_target_size()
    }

    pub fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        self.inner.merge(segments)
    }
}

impl SsTableLevel<PartitionedLevel> {
    pub fn new(target_size: u64, level_number: u64) -> Self {
        SsTableLevel {
            inner: PartitionedLevel {
                segments: Vec::new(),
                target_size: target_size,
                highest_segment_number: level_number,
            },
        }
    }

    pub fn highest_sequence_no(&self) -> u64 {
        let mut res = 0;
        for segment in &self.inner.segments {
            if segment.number > res {
                res = segment.number
            }
        }
        res
    }
}

impl SsTableLevel<OverlappingLevel> {
    pub fn new(target_size: u64) -> Self {
        SsTableLevel {
            inner: OverlappingLevel {
                segments: Vec::new(),
                target_size: target_size,
            },
        }
    }

    fn add_segment(&mut self, segment: Segment) -> () {
        self.inner.segments.push(Arc::new(segment));
    }
}

impl Clone for SsTableLevel<OverlappingLevel> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        SsTableLevel { inner: inner }
    }
}

impl SegmentIndex {
    pub fn new() -> Self {
        SegmentIndex {
            indices: Vec::new(),
            len: 0,
        }
    }
    fn find_closest_offset(&self, key: &String) -> u64 {
        let mut l: usize = 0;
        let mut r = self.indices.len() - 1;
        let mut m = (l + r) / 2;
        while l < r {
            if self.indices[l].key <= *key {
                l = m;
            } else {
                r = m;
            }
            m = (l + r) / 2
        }
        self.indices[l].offset
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.reserve(self.len);
        for index in &self.indices {
            buf.append(&mut index.to_bytes());
        }
        buf
    }

    pub fn add_index(&mut self, key: String, offset: u64) {
        self.len += key.len() + 2 * size_of::<u64>();
        self.indices.push(SegmentIndexEntry {
            key: key,
            offset: offset,
        });
    }
}

impl SegmentIndexEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut offset = 0;
        let mut buf = Vec::<u8>::new();
        buf.resize(self.key.len() + 2 * size_of::<u64>(), 0);
        let len = self.key.len() as u64;
        buf[offset..offset + size_of::<u64>()].copy_from_slice(&len.to_le_bytes());
        offset += size_of::<u64>();
        buf[offset..offset + self.key.len()].copy_from_slice(self.key.as_bytes());
        offset += self.key.len();
        buf[offset..].copy_from_slice(&self.offset.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &Vec<u8>) -> Self {
        let mut buf_offset = 0;
        let key_len = u64::from_le_bytes(
            buf[buf_offset..buf_offset + size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize;
        buf_offset += size_of::<u64>();
        let key = String::from_utf8(buf[buf_offset..buf_offset + key_len].to_vec()).unwrap();
        buf_offset += key_len;
        let offset = u64::from_le_bytes(
            buf[buf_offset..buf_offset + size_of::<u64>()]
                .try_into()
                .unwrap(),
        );
        SegmentIndexEntry {
            key: key,
            offset: offset,
        }
    }
}

impl SegmentFooter {
    pub fn from(offset: u64) -> Self {
        SegmentFooter {
            index_offset: offset,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.resize(size_of::<u64>() + MAGIC_BYTES.len(), 0);
        buf[0..size_of::<u64>()].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[size_of::<u64>()..].copy_from_slice(&MAGIC_BYTES);
        buf
    }
}
