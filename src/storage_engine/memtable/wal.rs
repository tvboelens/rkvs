use ::std::io;
use segment::{Segment, WalEntry, determine_segment_filename};
use std::path::PathBuf;

pub mod segment;

pub struct Wal {
    active_segment: Segment,
    segment_max_size: u32,
    dir: PathBuf,
}

impl Wal {
    pub fn create_new(dir: PathBuf, segment_max_size: u32) -> io::Result<Self> {
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
}
