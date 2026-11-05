use crate::storage_engine::sstable::level::segment::{
    SegmentOpenError, SegmentReadBuf, SegmentWriter,
};

use super::SsTableEntry;
pub use segment::Segment;
use std::collections::{HashMap, VecDeque};
use std::fs::{read_dir, remove_file};
use std::io::{self};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

pub mod segment;

static MERGE_BUF_MAX_TOTAL_SIZE: u64 = 1024 * 1024 * 10; // 10 MB

pub trait SstLevel {
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>>;
    fn segments_to_merge(&self) -> Vec<Arc<Segment>>;
    fn exceeds_target_size(&self) -> bool;
    fn highest_segment_number(&self) -> u64;
    fn target_size(&self) -> u64;
    fn rollback_merge(&mut self) -> io::Result<()>;
    fn finish_merge(&mut self) -> io::Result<()>;
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

enum MergeStatus {
    Pending,
    Finish,
    Rollback,
}

impl LevelContainer {
    fn new(level_zero_target_size: u64) -> Self {
        let level = OverlappingLevel {
            segments: Vec::new(),
            target_size: level_zero_target_size,
        };
        let level_zero = SsTableLevel { inner: level };
        LevelContainer {
            level_zero: Arc::new(level_zero),
            partitioned_levels: Vec::new(),
        }
    }

    /// Method to create a LevelContainer from existing segment files.
    /// Looks in the directory specified by dir for the segment files,
    /// creates the segments from them and then performs recovery.
    /// Recovery entails deleting corrupted segment files (i.e. ones that
    /// are missing magic bytes at the end), checking for each level whether
    /// a merge was interrupted or failed to start and if yes, performs the merge
    /// (after cleaning up if necessary).
    pub fn from_dir(
        dir: &PathBuf,
        level_zero_target_size: u64,
        level_size_factor: u64,
        index_sparsity_factor: Arc<u32>,
    ) -> io::Result<Self> {
        // Create all the segments and delete the corrupted ones
        let mut filepaths = Vec::new();
        let mut segments = Vec::new();
        for entry in read_dir(dir)? {
            let e = entry?;
            if e.file_type()?.is_file() {
                filepaths.push(e.path());
            }
        }
        for fp in filepaths {
            match Segment::from_file(fp.clone()) {
                Ok(segment) => {
                    segments.push(segment);
                }
                Err(e) => match e {
                    SegmentOpenError::Corrupted => {
                        let _ = std::fs::remove_file(fp);
                    }
                    SegmentOpenError::IoError(err) => {
                        return Err(err);
                    }
                },
            }
        }
        if segments.is_empty() {
            return Ok(LevelContainer::new(level_zero_target_size));
        }
        // If there were fully written segment files, we create intermediate levels
        // on which we will perform recovery
        // TODO: We should probably check for gaps in the level numbers,
        // i.e. I think the level numbers should be 0, 1, ..., n
        segments.sort_by(|s1, s2| s1.level_number().cmp(&s2.level_number()));
        let mut level_zero_segments = Vec::new();
        let mut higher_level_segments = Vec::new();
        let mut higher_levels = Vec::new();
        let mut curr_level: u64 = 0;
        for segment in segments {
            if segment.level_number() == 0 {
                level_zero_segments.push(Arc::new(segment));
            } else if curr_level == segment.level_number() {
                higher_level_segments.push(Arc::new(segment));
            } else {
                higher_level_segments
                    .sort_by(|s1, s2| s1.segment_number().cmp(s2.segment_number()));
                let level = SsTableLevel::<PartitionedLevel>::from(PartitionedLevel::from(
                    higher_level_segments.clone(),
                    level_zero_target_size * curr_level * level_size_factor,
                    index_sparsity_factor.clone(),
                    curr_level,
                ));
                higher_levels.push(level);
                curr_level = segment.level_number();
                higher_level_segments.clear();
                higher_level_segments.push(Arc::new(segment));
            }
        }
        level_zero_segments.sort_by(|s1, s2| s1.segment_number().cmp(s2.segment_number()));
        let level_zero = SsTableLevel::<OverlappingLevel>::from(OverlappingLevel {
            segments: level_zero_segments,
            target_size: level_zero_target_size,
        });

        let mut recovered_higher_levels = Vec::new();
        let mut it = higher_levels.iter_mut().peekable();
        while let Some(level) = it.next() {
            if !level.exceeds_target_size() {
                continue;
            }
            match it.peek() {
                None => (), // Perform merge into a new top level later
                Some(next_level) => match level.check_merge_status(next_level)? {
                    MergeStatus::Pending => (),
                    MergeStatus::Finish => {
                        level.finish_merge()?;
                    }
                    MergeStatus::Rollback => {
                        level.rollback_merge()?;
                    }
                },
            }
        }
        // Execute merges if needed
        let mut segments_to_merge = level_zero.segments_to_merge();
        let mut it = higher_levels.into_iter();
        while let Some(level) = it.next() {
            let (new_level, segments_to_delete) = level.merge(&segments_to_merge)?;
            for segment in segments_to_delete {
                remove_file(segment.filepath())?;
            }
            segments_to_merge = new_level.segments_to_merge();
            recovered_higher_levels.push(new_level);
        }
        // keep merging until the top level does not exceed target size
        while !segments_to_merge.is_empty() {
            let level_number = (recovered_higher_levels.len() + 1) as u64;
            let empty_level = SsTableLevel::<PartitionedLevel>::new(
                level_zero_target_size * level_number * level_size_factor,
                level_number,
            );
            let (new_level, segments_to_delete) = empty_level.merge(&segments_to_merge)?;
            for segment in segments_to_delete {
                remove_file(segment.filepath())?;
            }
            segments_to_merge = new_level.segments_to_merge();
            recovered_higher_levels.push(new_level);
        }
        Ok(LevelContainer {
            level_zero: Arc::new(level_zero),
            partitioned_levels: recovered_higher_levels,
        })
    }

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
        let segment: Segment;
        match Segment::from_file(path) {
            Ok(s) => {
                segment = s;
            }
            Err(e) => match e {
                SegmentOpenError::Corrupted => {
                    panic!("memtable flusher wrote a corrupted sstable segment!")
                }
                SegmentOpenError::IoError(err) => {
                    return Err(err);
                }
            },
        }
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
    /// Searches all the segments in the level for the entry.
    /// If multiple entries are found with the key, returns the
    /// newest entry, i.e. the one with the highest sequence number.
    fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        let mut curr_entry = None;
        for segment in &self.segments {
            curr_entry = match segment.get(key)? {
                None => curr_entry,
                Some(entry) => match curr_entry {
                    None => Some(entry),
                    Some(curr_e) => {
                        if curr_e.sequence_number > entry.sequence_number {
                            Some(curr_e)
                        } else {
                            Some(entry)
                        }
                    }
                },
            };
        }
        Ok(curr_entry)
    }

    fn segments_to_merge(&self) -> Vec<Arc<Segment>> {
        let mut segments_to_merge = Vec::new();
        let mut size: u64 = self.segments.iter().map(|s| s.size()).sum();
        let mut curr_segment_number = self.highest_segment_number();
        while size > self.target_size {
            for segment in &self.segments {
                if *segment.segment_number() == curr_segment_number {
                    segments_to_merge.push(segment.clone());
                    size -= segment.size();
                }
                curr_segment_number -= 1;
            }
        }
        segments_to_merge
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

    fn target_size(&self) -> u64 {
        self.target_size
    }

    fn finish_merge(&mut self) -> io::Result<()> {
        todo!()
    }

    fn rollback_merge(&mut self) -> io::Result<()> {
        todo!()
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
        let mut segments_to_merge = Vec::new();
        let mut size: u64 = self.segments.iter().map(|s| s.size()).sum();
        let mut curr_segment_number = self.highest_segment_number();
        while size > self.target_size {
            for segment in &self.segments {
                if *segment.segment_number() == curr_segment_number {
                    segments_to_merge.push(segment.clone());
                    size -= segment.size();
                }
                curr_segment_number -= 1;
            }
        }
        segments_to_merge
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

    fn target_size(&self) -> u64 {
        self.target_size
    }

    fn finish_merge(&mut self) -> io::Result<()> {
        todo!()
    }

    fn rollback_merge(&mut self) -> io::Result<()> {
        todo!()
    }
}

impl PartitionedLevel {
    fn from(
        segments: Vec<Arc<Segment>>,
        target_size: u64,
        index_sparsity_factor: Arc<u32>,
        level_number: u64,
    ) -> Self {
        PartitionedLevel {
            segments: segments,
            target_size: target_size,
            index_sparsity_factor: index_sparsity_factor,
            level_number: level_number,
        }
    }

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
        for segment in incoming_segments {
            segments.push(segment.clone());
        }
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
        for segment in segments {
            read_bufs.push(SegmentReadBuf::new(segment.clone(), read_buf_len));
        }
        let mut new_segment_number = self.highest_segment_number() + 1;
        let dir: PathBuf = segments
            .first()
            .unwrap()
            .filepath()
            .parent()
            .unwrap()
            .into();
        let mut segment_writer = SegmentWriter::create_new_segment(
            &dir,
            &self.level_number,
            &new_segment_number,
            self.index_sparsity_factor.deref().clone(),
        )?;
        new_segment_number += 1;
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
            while let Some(entry) = write_buf.pop_front() {
                if segment_writer.reached_target_size() {
                    segment_writer.write_index_and_footer()?;
                    let new_segment =
                        Segment::from_file(segment_writer.filepath().clone()).unwrap();
                    res.push(Arc::new(new_segment));
                    segment_writer = SegmentWriter::create_new_segment(
                        &dir,
                        &self.level_number,
                        &new_segment_number,
                        self.index_sparsity_factor.deref().clone(),
                    )?;
                    new_segment_number += 1;
                }
                segment_writer.write_entry(&entry)?;
            }
            for buf in &mut read_bufs {
                buf.fill()?
            }

            // If a read buffer is still empty after filling,
            // it has reached the end of the segment, so it can be discarded
            read_bufs = read_bufs
                .into_iter()
                .filter(|buf| !buf.is_empty())
                .collect();
        }
        // We might have exited the loop without writing the index and footer
        // of the last segment. In that case do it here
        if !segment_writer.finished() {
            segment_writer.write_index_and_footer()?;
            let new_segment = Segment::from_file(segment_writer.filepath().clone()).unwrap();
            res.push(Arc::new(new_segment));
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
    // The next key could be in a buffer that is removed in the previous line,
    // therefore filter the next indices
    next_indices = next_indices
        .into_iter()
        .filter(|idx| read_bufs.contains_key(idx))
        .collect();
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

    pub fn target_size(&self) -> u64 {
        self.inner.target_size()
    }

    fn rollback_merge(&mut self) -> io::Result<()> {
        self.inner.rollback_merge()
    }

    fn finish_merge(&mut self) -> io::Result<()> {
        self.inner.finish_merge()
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

    /// Merges the incoming segments with the segments held by the current level.
    /// Outputs the replacement for the current level together with a list of all
    /// segments that have to be deleted.
    pub fn merge(
        &self,
        segments: &Vec<Arc<Segment>>,
    ) -> io::Result<(Arc<SsTableLevel<PartitionedLevel>>, Vec<Arc<Segment>>)> {
        self.inner.merge(segments)
    }

    pub fn level_number(&self) -> u64 {
        self.inner.level_number
    }

    fn check_merge_status(
        &self,
        _next_level: &SsTableLevel<PartitionedLevel>,
    ) -> io::Result<MergeStatus> {
        todo!()
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

    fn check_merge_status(
        &self,
        _next_level: &SsTableLevel<OverlappingLevel>,
    ) -> io::Result<MergeStatus> {
        todo!()
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
                    sequence_number: 1,
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
                    sequence_number: 3,
                },
            ),
            (
                String::from("key4"),
                MemTableValue {
                    value: Some(String::from("value4")),
                    sequence_number: 4,
                },
            ),
        ];
        for (key, value) in &kv_pairs1 {
            table.insert(key.clone(), value.clone());
        }
        let sequence_number = 0;
        let sparsity_factor = 1;
        let fp = SegmentWriter::segment_file_from_memtable(
            &dir,
            &table,
            &sequence_number,
            &sparsity_factor,
        )
        .unwrap();
        let segment1 = Segment::from_file(fp).unwrap();
        table.clear();
        let kv_pairs2 = vec![
            (
                String::from("key5"),
                MemTableValue {
                    value: Some(String::from("value5")),
                    sequence_number: 5,
                },
            ),
            (
                String::from("key6"),
                MemTableValue {
                    value: Some(String::from("value6")),
                    sequence_number: 6,
                },
            ),
            (
                String::from("key7"),
                MemTableValue {
                    value: Some(String::from("value7")),
                    sequence_number: 7,
                },
            ),
            (
                String::from("key8"),
                MemTableValue {
                    value: Some(String::from("value8")),
                    sequence_number: 8,
                },
            ),
        ];
        for (key, value) in &kv_pairs2 {
            table.insert(key.clone(), value.clone());
        }
        let sequence_number = 2;
        let sparsity_factor = 1;
        let fp = SegmentWriter::segment_file_from_memtable(
            &dir,
            &table,
            &sequence_number,
            &sparsity_factor,
        )
        .unwrap();
        let segment2 = Segment::from_file(fp).unwrap();
        let level = Arc::new(SsTableLevel::<PartitionedLevel>::new(1024, 1));
        let segments = vec![Arc::new(segment1), Arc::new(segment2)];
        let (new_level, segments_to_delete) = level.merge(&segments).unwrap();
        assert_eq!(segments_to_delete.len(), 2);
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

    #[test]
    fn merge_two_overlapping_segments_different_levels() {
        let dir = PathBuf::from("./sstable_merge_two_overlapping_segments");
        let cl = Cleanup { dir: dir.clone() };
        assert!(cl.setup().is_ok());
        let mut entries1: Vec<SsTableEntry> = vec![
            SsTableEntry {
                key: String::from("key5"),
                value: Some(String::from("value0")),
                sequence_number: 0,
            },
            SsTableEntry {
                key: String::from("key2"),
                value: Some(String::from("value")),
                sequence_number: 1,
            },
            SsTableEntry {
                key: String::from("key4"),
                value: Some(String::from("value5")),
                sequence_number: 2,
            },
            SsTableEntry {
                key: String::from("key9"),
                value: Some(String::from("value9")),
                sequence_number: 3,
            },
        ];
        let mut entries0: Vec<SsTableEntry> = vec![
            SsTableEntry {
                key: String::from("key8"),
                value: None,
                sequence_number: 4,
            },
            SsTableEntry {
                key: String::from("key3"),
                value: Some(String::from("value3")),
                sequence_number: 5,
            },
            SsTableEntry {
                key: String::from("key4"),
                value: None,
                sequence_number: 6,
            },
            SsTableEntry {
                key: String::from("key5"),
                value: Some(String::from("new_value")),
                sequence_number: 7,
            },
        ];
        let mut entries_after_merge: Vec<SsTableEntry> = vec![
            SsTableEntry {
                key: String::from("key2"),
                value: Some(String::from("value")),
                sequence_number: 1,
            },
            SsTableEntry {
                key: String::from("key3"),
                value: Some(String::from("value3")),
                sequence_number: 5,
            },
            SsTableEntry {
                key: String::from("key4"),
                value: None,
                sequence_number: 6,
            },
            SsTableEntry {
                key: String::from("key5"),
                value: Some(String::from("new_value")),
                sequence_number: 7,
            },
            SsTableEntry {
                key: String::from("key8"),
                value: None,
                sequence_number: 4,
            },
            SsTableEntry {
                key: String::from("key9"),
                value: Some(String::from("value9")),
                sequence_number: 3,
            },
        ];
        // The merge function expects the segments to be ordered by key
        entries0.sort_by(|e1, e2| e1.key.cmp(&e2.key));
        entries1.sort_by(|e1, e2| e1.key.cmp(&e2.key));
        entries_after_merge.sort_by(|e1, e2| e1.key.cmp(&e2.key));
        let mut writer0 = SegmentWriter::create_new_segment(&dir, &0, &0, 10).unwrap();
        let mut writer1 = SegmentWriter::create_new_segment(&dir, &1, &0, 10).unwrap();
        for entry in entries0 {
            writer0.write_entry(&entry).unwrap();
        }
        for entry in entries1 {
            writer1.write_entry(&entry).unwrap();
        }
        writer0.write_index_and_footer().unwrap();
        writer1.write_index_and_footer().unwrap();
        let segment0 = Segment::from_file(writer0.filepath().clone()).unwrap();
        let segment1 = Segment::from_file(writer1.filepath().clone()).unwrap();
        let fp0 = segment0.filepath();
        let fp1 = segment1.filepath();
        let partitioned_level =
            PartitionedLevel::from(vec![Arc::new(segment1)], 1024, Arc::new(10), 1);
        let level1 = SsTableLevel::<PartitionedLevel>::from(partitioned_level);
        let (new_level, segments_to_delete) = level1.merge(&vec![Arc::new(segment0)]).unwrap();
        assert_eq!(segments_to_delete.len(), 2);
        for entry in entries_after_merge {
            let read_entry = new_level.get(&entry.key).unwrap().unwrap();
            assert_eq!(entry, read_entry);
        }
        assert_eq!(new_level.inner.segments.len(), 1);
        assert_eq!(new_level.inner.segments[0].first_key(), "key2");
        assert_eq!(new_level.inner.segments[0].last_key(), "key9");
        assert!(
            segments_to_delete.first().unwrap().filepath() == fp0
                || segments_to_delete.first().unwrap().filepath() == fp1
        );
        assert!(
            segments_to_delete.last().unwrap().filepath() == fp0
                || segments_to_delete.last().unwrap().filepath() == fp1
        );
    }
}

/*
TODO:
Testing
    1. Merge function
        1. The segments are OK
        2. The resulting level is indeed non-overlapping
    2. Reading when multiple levels are involved -> make sure newest version is read if the key is in multiple levels
    3. Add impl for SsTable (or segment) so that we can input a vector of sstable entries
       (and some other data like level, segment number etc) to create an sstable segment
       or just a whole sstable. This will also come in handy once we want to test recovery
*/
