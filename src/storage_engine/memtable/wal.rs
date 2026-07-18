pub mod segment;

use ::std::io;
use segment::{Segment, WalEntry, determine_segment_filename};
use std::path::PathBuf;

enum SerError {
    InvalidKey,
    InvalidValue,
}

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
    last_sequence_number: u64,
    last_no_of_bytes_written: u32,
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
        let filename = PathBuf::from(determine_segment_filename(0, 0, segment_max_size as u64));
        let path = dir.join(filename);
        let file = std::fs::File::create(path)?;
        let segment = Segment::new(file, segment_max_size);

        let wal = Wal {
            active_segment: segment,
            last_sequence_number: 0,
            last_no_of_bytes_written: 0,
            segment_max_size: segment_max_size,
            dir: dir,
        };
        Ok(wal)
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.active_segment.sync()
    }

    pub fn last_sequence_number(&self) -> &u64 {
        &self.last_sequence_number
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
            self.rotate(entry.sequence_number + buf_size as u64);
            self.active_segment.append(buf.as_slice())?;
        }
        self.last_sequence_number = entry.sequence_number;
        self.last_no_of_bytes_written = buf_size;
        // TODO: Is there a possibility of partial writes? If so, truncate
        Ok(())
    }

    pub fn next_sequence_number(&self) -> u64 {
        self.last_sequence_number + self.last_no_of_bytes_written as u64
    }

    fn rotate(&mut self, next_sequence_number: u64) -> io::Result<()> {
        let segment_size = self.segment_max_size as u64;
        let filename = determine_segment_filename(0, next_sequence_number, segment_size);
        let path = self.dir.join(filename);
        let file = std::fs::File::create(path)?;
        self.active_segment = Segment::new(file, self.segment_max_size);
        Ok(())
    }
}
/*
LSN:
64 bit unsigned int (16 hexadecimals), high part first 32 bits and low part the last 32 bits
This is the offset of bytes written from the start (i.e. if an entry has LSN x and consists of y bits,
then the LSN of next entry is x+y)
The high part signifies the logical log number (each logical log is 2^32 bytes = 4GB)
Each logical log is divided into segments (1 file per segment) (for example 16MB per file means 4GB/16MB = 256 segments)
WAL filename now consists of
timeline_id high_part segment_number
Each is a 32 bit uint, timeline id comes from the fact that checkpointing and recovery can lead to multiple
versions (timelines) of the db and we must track all of them (maybe not so relevant right now for me)

Other details:
segment size is exact, e.g. if configured size is 16 MB, then we write exactly 16 MB before we open the new segment

high part in filename should be the high part of lsn of first record that starts in the file (i.e. if the first bytes belong to
a record whose start is written into previous file, the next record)

there also seems to be a concept of pages, which seem to be kept in memory
apparently record headers do not get split across pages (but the payload can be split across),
instead there is some kind of padding
even if we don't want to implement this (at first) do we need to pad so that the headers are not split across files?
*/

/*
Implementation notes:
1. Segment is the direct interface to the FS, WAL manages the segments
2. Right now we need to solve two problems:
    1. Rollover
        1. Check before write if we will need to rollover
            1. rollover_necessary(len) -> returns enum with 2 values
                1. No
                2. Yes with no of bytes -> no of bytes we can write, if 0 then call pad()
        2. If have to rollover check whether we can write the header or have to pad -> see above
        3. Pad with zeros?
        4. Open new segment file
        5. Write bytes to old segment file
        5. Close the old segment file?
        6. Write remaining bytes to new segment file
    2. Do we need to keep open previous WAL files?
        1. I guess maybe not open, since we only use the WAL for recovering the memtable on restart
        2. So once the memtable is persisted to the sstable we don't need the old files anymore
        3. I guess here the question
*/
