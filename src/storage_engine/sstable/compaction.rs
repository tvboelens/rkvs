use crate::storage_engine::sstable::level::segment::Segment;
use crate::storage_engine::sstable::level::{
    LevelContainer, OverlappingLevel, PartitionedLevel, SsTableLevel,
};
use std::fs::remove_file;
use std::io;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

struct CompactionBt {
    container: Arc<RwLock<LevelContainer>>,
    segments_to_delete: Vec<Arc<Segment>>,
    cv: Arc<Condvar>,
    do_compact: Arc<Mutex<bool>>,
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
        let mut level_number = 1;
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
                    let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(
                        0,
                        level_number.clone(),
                    ));
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                }
            }
            level_number += 1;
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
        let mut level_number = levels.first().unwrap().highest_sequence_no() + 1;
        let _ = levels.remove(0);
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
                    let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(0, level_number));
                    // maybe do a match here to clean up if error?
                    (new_partitioned_level, segments_to_delete) =
                        level.merge(&segments_to_merge)?;
                    self.segments_to_delete.append(&mut segments_to_delete);
                    new_partitioned_levels.push(new_partitioned_level.clone());
                }
            }
            level_number += 1;
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
                files.push(self.segments_to_delete[i].filepath());
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
