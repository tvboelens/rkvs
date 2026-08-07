use std::fs::{File, OpenOptions, remove_file};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::{Arc, Condvar};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

struct SegmentIndex {}
struct Segment {
    file: File,
    filepath: PathBuf,
    index: SegmentIndex,
    highest_sequence_no: u64,
    size: u64,
}

trait Level {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>>;
    fn segments_to_merge(&self) -> Vec<Arc<Segment>>;
    fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)>;
    fn exceeds_target_size(&self) -> bool;
}

#[derive(Clone)]
struct OverlappingLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
}
struct PartitionedLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
}

struct SsTableLevel<T>
where
    T: Level,
{
    inner: T,
}

pub struct SsTableEntry {
    pub key: String,
    pub value: Option<String>,
    pub sequence_number: u64,
}

struct CompactionBt {
    container: Arc<RwLock<LevelContainer>>,
    segments_to_delete: Vec<Arc<Segment>>,
    cv: Arc<Condvar>,
    do_compact: Arc<Mutex<bool>>,
}

struct LevelContainer {
    level_zero: Arc<SsTableLevel<OverlappingLevel>>,
    partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
}

pub struct SsTable {
    inner: Arc<RwLock<LevelContainer>>,
    compaction_bt_cv: Arc<Condvar>,
    do_compact: Arc<Mutex<bool>>,
}

impl LevelContainer {
    fn level_zero(&self) -> Arc<SsTableLevel<OverlappingLevel>> {
        self.level_zero.clone()
    }

    fn partitioned_levels(&self) -> Vec<Arc<SsTableLevel<PartitionedLevel>>> {
        self.partitioned_levels.clone()
    }

    fn swap_partitioned_levels(
        &mut self,
        new_partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) {
        self.partitioned_levels = new_partitioned_levels;
    }

    fn swap_all_levels(
        &mut self,
        new_partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) {
        let target_size = self.level_zero.inner.target_size;
        self.level_zero = Arc::new(SsTableLevel::<OverlappingLevel>::new(target_size));
        self.partitioned_levels = new_partitioned_levels;
    }

    fn add_to_level_zero(&mut self, path: PathBuf) -> io::Result<()> {
        let segment = Segment::from_file(path)?;
        let mut new_level_zero = self.level_zero.as_ref().clone();
        new_level_zero.add_segment(segment);
        self.level_zero = Arc::new(new_level_zero);
        Ok(())
    }

    fn do_compact(&self) -> bool {
        self.level_zero.exceeds_target_size()
    }
}

impl SsTable {
    pub fn start() -> io::Result<Self> {
        todo!()
    }

    pub fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        let level_zero: Arc<SsTableLevel<OverlappingLevel>>;
        let partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>;
        {
            let lock = self.inner.read().unwrap();
            level_zero = lock.level_zero();
            partitioned_levels = lock.partitioned_levels();
        }

        match level_zero.get(key)? {
            Some(entry) => Ok(Some(entry)),
            None => {
                for level in &partitioned_levels {
                    match level.get(key)? {
                        Some(entry) => {
                            return Ok(Some(entry));
                        }
                        None => {
                            continue;
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn find_and_add_segment(&self, path: PathBuf) -> io::Result<()> {
        let do_compact: bool;
        {
            let mut lock = self.inner.write().unwrap();
            lock.add_to_level_zero(path)?;
            do_compact = lock.do_compact();
        }
        {
            let mut lock = self.do_compact.lock().unwrap();
            *lock = do_compact;
            self.compaction_bt_cv.notify_one();
        }
        Ok(())
    }
}

impl Segment {
    fn create(path: PathBuf, mut entries: Vec<SsTableEntry>) -> io::Result<Self> {
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
        let index = SegmentIndex {};
        let metadata = rfile.metadata()?;
        Ok(Segment {
            file: rfile,
            filepath: path,
            highest_sequence_no: highest_sequence_no,
            index: index,
            size: metadata.size(),
        })
    }

    fn from_file(path: PathBuf) -> io::Result<Self> {
        todo!()
    }

    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
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
}

impl SegmentIndex {
    fn find_closest_offset(&self, key: &String) -> u64 {
        return 0;
    }
}

impl SsTableEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut offset: usize = 0;
        buf.resize(self.len(), 0);
        let entry_len = self.len() as u32;
        buf[offset..offset + size_of::<u32>()].copy_from_slice(&entry_len.to_le_bytes());
        offset += size_of::<u32>();
        let key_len = self.key.len() as u32;
        buf[offset..offset + size_of::<u32>()].copy_from_slice(&key_len.to_le_bytes());
        offset += size_of::<u32>();
        buf[offset..offset + self.key.len()].copy_from_slice(&self.key.as_bytes());
        offset += self.key.len();
        match &self.value {
            Some(value) => {
                buf[offset] = 0;
                offset += 1;
                buf[offset..offset + value.len()].copy_from_slice(&value.as_bytes());
                offset += value.len();
            }
            None => {
                buf[offset] = 1;
                offset += 1;
            }
        }
        buf[offset..].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf
    }

    fn from_bytes(bytes: &Vec<u8>) -> Self {
        SsTableEntry {
            key: String::from("key"),
            value: None,
            sequence_number: 0,
        }
    }

    pub fn len(&self) -> usize {
        /*
        entry_len: u32
        key_len: u32
        key: variable len
        value_len: u32
        value (+byte for tombstone)
        lsn: u64
        */
        let value_len = match &self.value {
            None => 1,
            Some(v) => v.len() + 1,
        };
        3 * size_of::<u32>() + size_of::<u64>() + self.key.len() + value_len
    }
}

impl Level for OverlappingLevel {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        Ok(None)
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
        segments: &Vec<Arc<Segment>>,
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
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        todo!()
    }
}

impl PartitionedLevel {
    fn find_containing_segment(&self, key: &String) -> Option<Arc<Segment>> {
        if self.segments.is_empty() {
            None
        } else {
            Some(self.segments[0].clone())
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

    fn segments_to_merge(&self) -> Vec<Arc<Segment>> {
        self.inner.segments_to_merge()
    }

    fn exceeds_target_size(&self) -> bool {
        self.inner.exceeds_target_size()
    }

    fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        self.inner.merge(segments)
    }
}

impl SsTableLevel<PartitionedLevel> {
    fn new(target_size: u64) -> Self {
        SsTableLevel {
            inner: PartitionedLevel {
                segments: Vec::new(),
                target_size: target_size,
            },
        }
    }
}

impl SsTableLevel<OverlappingLevel> {
    fn new(target_size: u64) -> Self {
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

impl CompactionBt {
    fn compact_from_level_zero(&mut self) -> io::Result<Vec<Arc<SsTableLevel<PartitionedLevel>>>> {
        let level_zero: Arc<SsTableLevel<OverlappingLevel>>;
        let mut partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>;
        {
            let lock = self.container.read().unwrap();
            level_zero = lock.level_zero();
            partitioned_levels = lock.partitioned_levels();
        }

        let mut new_partitioned_levels = Vec::new();
        let mut new_partitioned_level: Arc<SsTableLevel<PartitionedLevel>>;
        let mut segments_to_merge = level_zero.segments_to_merge();
        let mut segments_to_delete: Vec<Arc<Segment>>;
        loop {
            match partitioned_levels.first() {
                Some(level) => {
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                    let _ = partitioned_levels.remove(0); // TODO: maybe VecDeque is better for this purpose
                }
                None => {
                    let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(0));
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                }
            }
            if !new_partitioned_level.exceeds_target_size() {
                break;
            }
            segments_to_merge = new_partitioned_level.segments_to_merge();
        }
        new_partitioned_levels.append(&mut partitioned_levels);
        Ok(new_partitioned_levels)
    }

    fn compact_from_partitioned_level(
        &mut self,
        mut levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) -> io::Result<Vec<Arc<SsTableLevel<PartitionedLevel>>>> {
        if levels.is_empty() {
            return Ok(Vec::new());
        }
        let mut segments_to_merge = levels.first().unwrap().segments_to_merge();
        let mut new_partitioned_levels = Vec::new();
        let mut new_partitioned_level: Arc<SsTableLevel<PartitionedLevel>>;
        let mut segments_to_delete: Vec<Arc<Segment>>;
        loop {
            match levels.first() {
                Some(level) => {
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                    let _ = levels.remove(0); // TODO: maybe VecDeque is better for this purpose
                }
                None => {
                    let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(0));
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                }
            }
            if !new_partitioned_level.exceeds_target_size() {
                break;
            }
            segments_to_merge = new_partitioned_level.segments_to_merge();
        }
        new_partitioned_levels.append(&mut levels);
        Ok(new_partitioned_levels)
    }

    fn find_partitioned_levels_to_compact(
        &self,
    ) -> (
        Vec<Arc<SsTableLevel<PartitionedLevel>>>,
        Vec<Arc<SsTableLevel<PartitionedLevel>>>,
    ) {
        let partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>;
        {
            let lock = self.container.read().unwrap();
            partitioned_levels = lock.partitioned_levels();
        }
        let mut idx: usize = 0;
        for _ in 0..partitioned_levels.len() {
            if partitioned_levels[idx].exceeds_target_size() {
                break;
            }
            idx += 1;
        }
        if idx == partitioned_levels.len() {
            return (partitioned_levels, Vec::new());
        } else {
            return (
                partitioned_levels[0..idx].to_vec(),
                partitioned_levels[idx..].to_vec(),
            );
        }
    }

    fn remove_segment_files(&mut self) {
        let mut indices = Vec::new();
        let mut files = Vec::new();
        for i in 0..self.segments_to_delete.len() {
            /* Since we swapped the segments the reference count can only go down,
            hence it is safe to delete as soon as reference count is 1 */
            if Arc::strong_count(&self.segments_to_delete[i]) == 1 {
                indices.push(i);
                files.push(self.segments_to_delete[i].filepath.clone());
            }
        }
        indices.reverse();
        for idx in indices {
            self.segments_to_delete.remove(idx);
        }
        for file in files {
            /* No need to handle error, since this can only fail if
            path does not exist, is a dir or we do not have permissions
            and this should never happen */
            let _ = remove_file(file);
        }
    }

    fn run(&mut self) {
        let mut do_compact: bool;
        loop {
            {
                let lock = self.do_compact.lock().unwrap();
                let res = self
                    .cv
                    .wait_timeout(lock, Duration::from_millis(5000))
                    .unwrap();
                do_compact = *res.0;
            }

            if do_compact {
                match self.compact_from_level_zero() {
                    Ok(new_levels) => {
                        {
                            let mut container = self.container.write().unwrap();
                            container.swap_all_levels(new_levels);
                        }
                        self.remove_segment_files();
                    }
                    Err(_) => {
                        break;
                    } // TODO: log instead of break
                }
            } else {
                self.remove_segment_files();
                let (mut new_partitioned_levels, levels_to_compact) =
                    self.find_partitioned_levels_to_compact();
                let mut new_levels = self
                    .compact_from_partitioned_level(levels_to_compact)
                    .unwrap();
                new_partitioned_levels.append(&mut new_levels);
                let mut container = self.container.write().unwrap();
                container.swap_partitioned_levels(new_partitioned_levels);
            }
        }
    }
}
