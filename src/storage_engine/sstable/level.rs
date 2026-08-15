use super::SsTableEntry;
pub use segment::Segment;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub mod segment;

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
            if segment.highest_sequence_number() > res {
                res = segment.highest_sequence_number()
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

/* #[cfg(test)]
mod tests {
    use super::*;
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
} */

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
