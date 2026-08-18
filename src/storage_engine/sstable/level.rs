use super::SsTableEntry;
pub use segment::Segment;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;

pub mod segment;

pub trait Level {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>>;
    fn segments_to_merge(&self) -> Vec<Arc<Segment>>;
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
}

impl PartitionedLevel {
    fn find_containing_segment(&self, _key: &String) -> Option<Arc<Segment>> {
        if self.segments.is_empty() {
            None
        } else {
            todo!()
        }
    }

    fn merge(
        &self,
        _segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        /*
        1. Break up into "intervals" that each don't overlap
        2. Then could do one thread per interval, I think here we could indeed spawn a thread
        3. For each interval, read a range from each segment, merge in memory and then pass to BufWriter
            1. So the relation is first compare by key and if equal, then compare by lsn. If equal take only the newest
            2. The question then becomes how to determine the range
                1. When we start, then simply take the first key of each segment
                2. So lets say we have segment 0,...,n and vec 0,..,n with start_i and end_i
                    1. Then sort until in some segment we hit a key that is larger then end_i for some i
                    2. After we have inserted or discarded a key from a vec, also delete it (use VecDeque)
                    3. Thus at start we must make sure that the vecs are large enough to contain the starts of the other vecs
                    4. And apart from that could just take a target size and divide it by n
                    5. What about this:
                        1. Have target size and n segments
                        2. Read approximately target size divided by n
                        3. Then determine "smallest" end key
                        4. When merging: if we write a key (or discard because equal but smaller lsn), then also delete it from vec
                        5. Continue until first key of each vec is larger than the "smallest" end key we determined before
                        6. Repeat
                    6. On a first glance this should work, but have to think more carefully about this
                    7. We also might want to think about the target size

         */
        todo!()
    }

    fn merge_and_write_slices(
        segment: &mut Segment,
        mut slices: Vec<VecDeque<SsTableEntry>>,
        buf_writer: &mut BufWriter<File>,
    ) -> io::Result<Option<Vec<SsTableEntry>>> {
        slices = slices
            .into_iter()
            .filter(|slice| !slice.is_empty())
            .collect();
        if slices.is_empty() {
            return Ok(None);
        }
        let mut smallest_final_key = slices[0].back().unwrap().key.clone();
        for slice in &slices {
            match slice.back() {
                None => {
                    continue;
                }
                Some(entry) => {
                    if entry.key < smallest_final_key {
                        smallest_final_key = entry.key.clone();
                    }
                }
            }
        }
        let mut candidate_slices: Vec<&mut VecDeque<SsTableEntry>> = slices
            .iter_mut()
            .filter(|slice| slice.front().unwrap().key <= smallest_final_key)
            .collect();
        let mut slice_to_write = VecDeque::<SsTableEntry>::new();
        while !candidate_slices.is_empty() {
            /*
            1. iterate over slices to determine which entry to write
            2. Then have to iterate again to determine which to remove
            3. Alternatively could save the indices
             */
            let mut slices_to_remove: Vec<usize> = Vec::new();
            let mut slices_containing_key: Vec<usize> = Vec::new();
            let mut curr_entry = candidate_slices[0].front().unwrap();
            slices_containing_key.push(0);
            for idx in 0..candidate_slices.len() {
                let candidate_entry = candidate_slices[idx].front().unwrap();
                if candidate_entry.key < curr_entry.key {
                    slices_containing_key.clear();
                    slices_containing_key.push(idx);
                    curr_entry = candidate_entry;
                } else if candidate_entry.key == curr_entry.key {
                    slices_containing_key.push(idx);
                    if curr_entry.sequence_number < candidate_entry.sequence_number {
                        curr_entry = candidate_entry;
                    }
                }
            }
            slice_to_write.push_back(curr_entry.clone());
            for idx in slices_containing_key {
                let _ = candidate_slices[idx].pop_front();
                if candidate_slices[idx].is_empty()
                    || candidate_slices[idx]
                        .front()
                        .is_some_and(|entry| entry.key > smallest_final_key)
                {
                    slices_to_remove.push(idx);
                }
            }
            slices_to_remove.sort();
            slices_to_remove.reverse();
            for idx in slices_to_remove {
                candidate_slices.remove(idx);
            }
        }
        Ok(None)
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
}

impl Clone for SsTableLevel<OverlappingLevel> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        SsTableLevel { inner: inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engine::memtable::MemTableValue;
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
            Segment::write_segment_file(&dir, &table, &sequence_number, &sparsity_factor).unwrap();
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
            Segment::write_segment_file(&dir, &table, &sequence_number, &sparsity_factor).unwrap();
        let segment2 = Segment::from_file(fp).unwrap();
        let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(0, 0));
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
