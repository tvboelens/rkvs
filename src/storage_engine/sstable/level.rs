use crate::storage_engine::sstable::level::segment::SegmentWriter;

use super::SsTableEntry;
pub use segment::Segment;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufWriter};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::bytes::buf;

pub mod segment;

static MERGE_BUF_MAX_TOTAL_SIZE: u64 = 1024 * 1024 * 10; // 10 MB

pub trait SstLevel {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>>;
    fn segments_to_merge(&self) -> Vec<Arc<Segment>>;
    fn exceeds_target_size(&self) -> bool;
    fn highest_segment_number(&self) -> u64;
}

#[derive(Clone)]
pub struct OverlappingLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
}
pub struct PartitionedLevel {
    segments: Vec<Arc<Segment>>,
    target_size: u64,
    index_sparsity_factor: Arc<u32>,
    level_number: u64,
}

pub struct SsTableLevel<T>
where
    T: SstLevel,
{
    inner: T,
}

pub struct LevelContainer {
    level_zero: Arc<SsTableLevel<OverlappingLevel>>,
    partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>,
}

struct SegmentReadBuf {
    segment: Arc<Segment>,
    buf: VecDeque<SsTableEntry>,
    curr_offset: u64,
    size: u64,
    max_size: u64,
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

impl SstLevel for OverlappingLevel {
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

    fn highest_segment_number(&self) -> u64 {
        let mut res = 0;
        for segment in &self.segments {
            if *segment.segment_number() > res {
                res = *segment.segment_number()
            }
        }
        res
    }
}

impl SstLevel for PartitionedLevel {
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

    fn highest_segment_number(&self) -> u64 {
        let mut res = 0;
        for segment in &self.segments {
            if *segment.segment_number() > res {
                res = *segment.segment_number()
            }
        }
        res
    }
}

impl PartitionedLevel {
    fn find_containing_segment(&self, key: &String) -> Option<Arc<Segment>> {
        if self.segments.is_empty() {
            None
        } else {
            for segment in &self.segments {
                if segment.first_key() <= key && key <= segment.last_key() {
                    return Some(segment.clone());
                }
            }
            None
        }
    }

    pub fn highest_segment_number(&self) -> u64 {
        let mut res = 0;
        for segment in &self.segments {
            if *segment.segment_number() > res {
                res = *segment.segment_number()
            }
        }
        res
    }

    /// Merges the incoming segments into the segments contained in self.
    /// If succesful, outputs the new level and the segments to be deleted.
    /// The current level is to be discarded in the background compaction task.
    fn merge(
        &self,
        incoming_segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        let mut segments = Vec::new();
        let mut untouched_segments = Vec::new();
        for segment in &self.segments {
            let mut overlaps = false;
            for incoming_segment in incoming_segments {
                overlaps = false;
                if segment.overlaps(incoming_segment) {
                    segments.push(segment.clone());
                    overlaps = true;
                    break;
                }
            }
            if !overlaps {
                untouched_segments.push(segment.clone());
            }
        }
        segments.extend(incoming_segments.iter().map(|s| s.clone()));
        let mut new_segments = self.merge_and_write(&segments)?;
        new_segments.append(&mut untouched_segments);
        new_segments.sort_unstable_by(|s, t| s.first_key().cmp(t.first_key()));
        let new_level = PartitionedLevel {
            target_size: self.target_size,
            segments: new_segments,
            index_sparsity_factor: self.index_sparsity_factor.clone(),
            level_number: self.level_number,
        };
        Ok((
            Arc::new(SsTableLevel::<PartitionedLevel>::from(new_level)),
            segments,
        ))
    }

    fn merge_and_write(&self, segments: &Vec<Arc<Segment>>) -> io::Result<Vec<Arc<Segment>>> {
        if segments.is_empty() {
            return Ok(Vec::new());
        }
        let read_buf_len = (MERGE_BUF_MAX_TOTAL_SIZE / segments.len() as u64) + 1;
        let mut res = Vec::new();
        let mut read_bufs: Vec<SegmentReadBuf> = Vec::new();
        let mut write_buf: VecDeque<SsTableEntry> = VecDeque::new();
        for segment in segments {
            read_bufs.push(SegmentReadBuf {
                segment: segment.clone(),
                buf: VecDeque::new(),
                curr_offset: 0,
                size: 0,
                max_size: read_buf_len,
            });
        }

        let mut new_segment_number = self.highest_segment_number() + 1;
        let dir: PathBuf = segments
            .first()
            .unwrap()
            .filepath()
            .parent()
            .unwrap()
            .into();
        let mut segment_writer =
            SegmentWriter::create_new_segment(&dir, &self.level_number, &new_segment_number)?;
        //new_segment_number += 1;
        /*
        1. Fill the read bufs (before starting loop)
        2. Discard empty read bufs
        3. merge into write buf
        4. while write buf not empty
            1. write
            2. If segment full, create new
        6. fill read bufs -> go to 2.
         */
        for buf in &mut read_bufs {
            buf.fill()?
        }
        read_bufs = read_bufs
            .into_iter()
            .filter(|buf| !buf.is_empty())
            .collect();

        while !read_bufs.is_empty() {
            let mut read_buf_refs = HashMap::new();
            let mut idx: usize = 0;
            for buf in &mut read_bufs {
                read_buf_refs.insert(idx.clone(), buf);
                idx += 1;
            }
            let mut write_buf = merge_sort(&mut read_buf_refs);
            while !write_buf.is_empty() {
                todo!()
            }

            for buf in &mut read_bufs {
                buf.fill()?
            }
            read_bufs = read_bufs
                .into_iter()
                .filter(|buf| !buf.is_empty())
                .collect();
        }
        Ok(res)
    }
}

fn merge_sort(read_bufs: &mut HashMap<usize, &mut SegmentReadBuf>) -> VecDeque<SsTableEntry> {
    if read_bufs.is_empty() {
        return VecDeque::new();
    } else if read_bufs.len() == 1 {
        let mut res = VecDeque::new();
        let buf = read_bufs.values_mut().next().unwrap();
        while let Some(entry) = buf.pop_front() {
            res.push_back(entry);
        }
        return res;
    }
    let mut res = VecDeque::new();
    let mut smallest_final_key = String::new();
    let mut curr_indices: Vec<usize> = Vec::new();
    let mut next_indices: Vec<usize> = Vec::new();
    for (idx, buf) in read_bufs.iter() {
        if smallest_final_key.is_empty() || buf.back().unwrap().key < smallest_final_key {
            smallest_final_key = buf.back().unwrap().key.clone();
        }
        let buf_key = &buf.front().unwrap().key;
        match curr_indices.first() {
            None => {
                curr_indices.push(*idx);
            }
            Some(curr_idx) => {
                let curr_key = &read_bufs.get(curr_idx).unwrap().front().unwrap().key;
                if *curr_key >= *buf_key {
                    if *curr_key != *buf_key {
                        std::mem::swap(&mut curr_indices, &mut next_indices);
                        curr_indices.clear();
                    }
                    curr_indices.push(*idx);
                } else if *buf_key <= smallest_final_key {
                    let push = match next_indices.first() {
                        None => true,
                        Some(next_idx) => {
                            read_bufs.get(next_idx).unwrap().front().unwrap().key >= *buf_key
                        }
                    };
                    let clear = match next_indices.first() {
                        None => false,
                        Some(next_idx) => {
                            read_bufs.get(next_idx).unwrap().front().unwrap().key > *buf_key
                        }
                    };
                    if clear {
                        next_indices.clear();
                    }
                    if push {
                        next_indices.push(*idx);
                    }
                }
            }
        }
    }
    read_bufs.retain(|_, buf| buf.front().unwrap().key <= smallest_final_key);
    while !read_bufs.is_empty() {
        let mut curr_bufs = Vec::new();
        for idx in &curr_indices {
            match read_bufs.remove(idx) {
                Some(buf) => {
                    curr_bufs.push(buf);
                }
                None => (),
            }
        }
        let next_key: Option<&String> = match next_indices.first() {
            None => None,
            Some(idx) => match read_bufs.get(idx).unwrap().front() {
                None => None,
                Some(entry) => Some(&entry.key),
            },
        };
        if curr_bufs.len() == 1 {
            let curr_buf = curr_bufs.pop().unwrap();
            while let Some(entry) = curr_buf.front() {
                let do_continue = match next_key {
                    None => entry.key <= smallest_final_key,
                    Some(key) => entry.key < *key,
                };
                if do_continue {
                    res.push_back(curr_buf.pop_front().unwrap());
                } else {
                    break;
                }
            }
        } else {
            let mut curr_entry: SsTableEntry = curr_bufs.first().unwrap().front().unwrap().clone();
            for buf in curr_bufs.iter_mut() {
                let entry = buf.pop_front().unwrap();
                if entry.sequence_number > curr_entry.sequence_number {
                    curr_entry = entry;
                }
            }
            res.push_back(curr_entry);
            loop {
                let mut pot_next_entry: Option<&SsTableEntry> = None;
                for buf in curr_bufs.iter() {
                    match buf.front() {
                        Some(entry) => {
                            let replace = match pot_next_entry {
                                Some(nentry) => {
                                    entry.key < nentry.key
                                        || (entry.key == nentry.key
                                            && entry.sequence_number > nentry.sequence_number)
                                }
                                None => entry.key <= smallest_final_key,
                            };
                            if replace {
                                pot_next_entry = Some(entry);
                            }
                        }
                        None => (),
                    }
                }
                let next_entry = pot_next_entry.cloned();
                match next_entry {
                    None => {
                        break;
                    }
                    Some(entry) => {
                        let insert: bool = match next_key {
                            Some(nkey) => entry.key < *nkey,
                            None => entry.key <= smallest_final_key,
                        };
                        if insert {
                            for buf in &mut curr_bufs {
                                match buf.front() {
                                    None => (),
                                    Some(e) => {
                                        if entry.key == e.key {
                                            let _ = buf.pop_front();
                                        }
                                    }
                                }
                            }
                            res.push_back(entry);
                        }
                    }
                }
            }
        }
        let mut idx: usize = 0;
        for buf in curr_bufs {
            match buf.front() {
                Some(entry) => {
                    if entry.key <= smallest_final_key {
                        let _ = read_bufs.insert(curr_indices[idx].clone(), buf);
                    }
                }
                None => (),
            }
            idx += 1;
        }
        std::mem::swap(&mut curr_indices, &mut next_indices);
        next_indices.clear();
        match curr_indices.first() {
            None => (),
            Some(curr_idx) => {
                let curr_key = &read_bufs.get(curr_idx).unwrap().front().unwrap().key;
                let mut next_key: Option<&String> = None;
                for (idx, buf) in read_bufs.iter() {
                    let buf_key = &buf.front().unwrap().key;
                    let push = match next_key {
                        None => *buf_key > *curr_key && *buf_key <= smallest_final_key,
                        Some(key) => *buf_key > *curr_key && *buf_key <= *key,
                    };
                    let replace_and_clear = match next_key {
                        None => *buf_key > *curr_key && *buf_key <= smallest_final_key,
                        Some(key) => *buf_key > *curr_key && *buf_key < *key,
                    };
                    if replace_and_clear {
                        next_key = Some(&buf.front().unwrap().key);
                        next_indices.clear();
                    }
                    if push {
                        next_indices.push(idx.clone());
                    }
                }
            }
        }
    }
    res
}

impl<T> SsTableLevel<T>
where
    T: SstLevel,
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

    pub fn highest_segment_number(&self) -> u64 {
        self.inner.highest_segment_number()
    }
}

impl SsTableLevel<PartitionedLevel> {
    pub fn new(target_size: u64, level_number: u64) -> Self {
        SsTableLevel {
            inner: PartitionedLevel {
                segments: Vec::new(),
                target_size: target_size,
                index_sparsity_factor: Arc::new(1), // TODO
                level_number: level_number,
            },
        }
    }

    pub fn highest_sequence_no(&self) -> u64 {
        let mut res = 0;
        for segment in &self.inner.segments {
            if segment.highest_sequence_number() > res {
                res = segment.highest_sequence_number()
            }
        }
        res
    }

    pub fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        self.inner.merge(segments)
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

    pub fn highest_sequence_no(&self) -> u64 {
        let mut res = 0;
        for segment in &self.inner.segments {
            if segment.highest_sequence_number() > res {
                res = segment.highest_sequence_number()
            }
        }
        res
    }
}

impl Clone for SsTableLevel<OverlappingLevel> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        SsTableLevel { inner: inner }
    }
}

impl SegmentReadBuf {
    fn fill(&mut self) -> io::Result<()> {
        let max_bytes = self.max_size - self.size;
        let new_entries = self.segment.read_at_most(&self.curr_offset, &max_bytes)?;
        for entry in new_entries {
            self.size += entry.len() as u64 + size_of::<u32>() as u64;
            self.curr_offset += entry.len() as u64 + size_of::<u32>() as u64;
            self.buf.push_back(entry);
        }
        Ok(())
    }

    fn back(&self) -> Option<&SsTableEntry> {
        self.buf.back()
    }

    fn front(&self) -> Option<&SsTableEntry> {
        self.buf.front()
    }

    fn pop_front(&mut self) -> Option<SsTableEntry> {
        match self.buf.front() {
            Some(entry) => {
                self.size -= (entry.len() + size_of::<u32>()) as u64;
            }
            None => (),
        }
        self.buf.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engine::memtable::MemTableValue;
    use crate::storage_engine::sstable::level::segment::SegmentWriter;
    use std::collections::HashMap;
    use std::fs::{self, DirBuilder};
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
    fn merge_two_non_overlapping_segments() {
        let dir = PathBuf::from("./sstable_merge_two_non_overlapping_segments");
        let cl = Cleanup { dir: dir.clone() };
        assert!(cl.setup().is_ok());
        let mut table = HashMap::new();
        let kv_pairs1 = vec![
            (
                String::from("key1"),
                MemTableValue {
                    value: Some(String::from("value1")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key2"),
                MemTableValue {
                    value: Some(String::from("value2")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key3"),
                MemTableValue {
                    value: Some(String::from("value3")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key4"),
                MemTableValue {
                    value: Some(String::from("value4")),
                    sequence_number: 2,
                },
            ),
        ];
        for (key, value) in &kv_pairs1 {
            table.insert(key.clone(), value.clone());
        }
        let sequence_number = 0;
        let sparsity_factor = 1;
        let fp =
            SegmentWriter::write_segment_file(&dir, &table, &sequence_number, &sparsity_factor)
                .unwrap();
        let segment1 = Segment::from_file(fp).unwrap();
        table.clear();
        let kv_pairs2 = vec![
            (
                String::from("key5"),
                MemTableValue {
                    value: Some(String::from("value5")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key6"),
                MemTableValue {
                    value: Some(String::from("value6")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key7"),
                MemTableValue {
                    value: Some(String::from("value7")),
                    sequence_number: 2,
                },
            ),
            (
                String::from("key8"),
                MemTableValue {
                    value: Some(String::from("value8")),
                    sequence_number: 2,
                },
            ),
        ];
        for (key, value) in &kv_pairs1 {
            table.insert(key.clone(), value.clone());
        }
        let sequence_number = 2;
        let sparsity_factor = 1;
        let fp =
            SegmentWriter::write_segment_file(&dir, &table, &sequence_number, &sparsity_factor)
                .unwrap();
        let segment2 = Segment::from_file(fp).unwrap();
        let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(0, 1));
        let segments = vec![Arc::new(segment1), Arc::new(segment2)];
        let (new_level, segments_to_delete) = level.merge(&segments).unwrap();
        assert!(segments_to_delete.is_empty());
        for (key, value) in kv_pairs1 {
            let entry = SsTableEntry::from(key.clone(), value);
            let read_entry = new_level.get(&key).unwrap().unwrap();
            assert_eq!(entry, read_entry);
        }
        for (key, value) in kv_pairs2 {
            let entry = SsTableEntry::from(key.clone(), value);
            let read_entry = new_level.get(&key).unwrap().unwrap();
            assert_eq!(entry, read_entry);
        }
    }
}

/*
TODO:
1. Probably write the merge functions first
    1. This would allow us in tests to create levels in line with later implementation
    2. The merge function itself should also be easy to test -> apply, read segment file and test the segment file
2. Testing
    1. Merge function
        1. The segments are OK
        2. The resulting level is indeed non-overlapping
    2. Reading when multiple levels are involved -> make sure newest version is read if the key is in multiple levels
3. We also want to be able to detect the key range of a segment somehow, i.e. first and last key
    1. This way we can find out which segment to read from within a level instead of trying to read them all
    2. question is how to do this
        1. In memory
            1. So just in the segment struct itself, read when creating the segment
            2. For first key this is one read
            3. For last key take the last key that is indexed, read until end of data block, parse and take last entry
            4. potential downside is that this will lead to higher memory consumption, since large dbs will have a lot of segments
        2. On-disk
            1. Filename -> seems like a bad idea, since key length can be variable
            2. Just read directly from disk

*/
