use ::std::io;
use segment::{Segment, WalEntry, determine_segment_filename};
use std::path::PathBuf;

pub mod segment;

/*
methods:
1. sync -> done
2. rotate
3. recover?
4. append
    1. checksum
    2. update file size
5. new()
6. Do I need synchronization or do we let the storage engine itself handle this?
*/

pub struct Wal {
    active_segment: Segment,
    segment_max_size: u32,
    dir: PathBuf,
}

impl Wal {
    pub fn create_new(dir: PathBuf, segment_max_size: u32) -> io::Result<Self> {
        /*
        1. If we start from scratch, just create the first segment, return and start rw operations
        2. If WAL files and SSTable present:
            1. Find highest LSN from SSTable
            2. Recover everything after this
                1. Find segment containing this lsn
                2. Then go to next lsn (possibly in next segment)
                3. Then call recover()
         */
        let filename = PathBuf::from(determine_segment_filename(&0, &0, &segment_max_size));
        let path = dir.join(filename);
        let file = std::fs::File::create(&path)?;
        let segment = Segment::new(file, path, segment_max_size);

        let wal = Wal {
            active_segment: segment,
            segment_max_size: segment_max_size,
            dir: dir,
        };
        Ok(wal)
    }

    pub fn from_segment(dir: PathBuf, segment: Segment, segment_size: u32) -> Self {
        Wal {
            active_segment: segment,
            segment_max_size: segment_size,
            dir: dir,
        }
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.active_segment.sync()
    }

    pub fn append(&mut self, entry: &WalEntry) -> std::io::Result<()> {
        let buf = entry.to_bytes();
        let buf_size = buf.len() as u32;
        let free_space = self.active_segment.remaining_space();
        if buf_size <= free_space {
            self.active_segment.append(buf.as_slice())?;
        } else if segment::HEADER_SIZE as u32 <= free_space {
            self.active_segment.append(&buf[0..segment::HEADER_SIZE])?;
            self.active_segment.pad()?;
            self.rotate(entry.sequence_number + buf_size as u64)?;
            self.active_segment.append(&buf[segment::HEADER_SIZE..])?;
        } else {
            self.active_segment.pad()?;
            self.rotate(entry.sequence_number + buf_size as u64)?;
            self.active_segment.append(buf.as_slice())?;
        }
        // TODO: Is there a possibility of partial writes? If so, truncate
        Ok(())
    }

    pub fn next_sequence_number(&self) -> u64 {
        self.active_segment.next_lsn()
    }

    fn rotate(&mut self, next_sequence_number: u64) -> io::Result<()> {
        let filename =
            determine_segment_filename(&0, &next_sequence_number, &self.segment_max_size);
        let path = self.dir.join(filename);
        let file = std::fs::File::create(&path)?;
        self.active_segment = Segment::new(file, path, self.segment_max_size);
        Ok(())
    }

    /*
    1. Check if SSTable files exist (separate function)
        1. Yes -> get largest LSN
        2. No -> largest LSN = 0
    2. Check if WAL files exist
        1. No -> new memtable, no recovery
        2. Yes -> check if segments exist containing newer LSNs
            1. No -> new memtable, no recovery
            2. Yes -> recover
     */
}
