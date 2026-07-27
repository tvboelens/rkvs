use std::fs::File;
use std::io;

struct SegmentIndex {}
struct Segment {
    file: File,
    index: SegmentIndex,
    highest_sequence_no: u64,
}

trait LevelSearch {
    fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>>;
}

struct OverlappingLevel {}
struct PartitionedLevel {}

struct SsTableLevel<T>
where
    T: LevelSearch,
{
    inner: T,
}

pub struct SsTableEntry {
    pub key: String,
    pub value: Option<String>,
    pub deleted: bool,
    pub sequence_number: u64,
}

pub struct SsTable {
    level_zero: SsTableLevel<OverlappingLevel>,
    higher_levels: Vec<SsTableLevel<PartitionedLevel>>,
}

impl SsTable {
    pub fn start() -> io::Result<Self> {
        Ok(SsTable {
            higher_levels: Vec::new(),
            level_zero: SsTableLevel {
                inner: OverlappingLevel {},
            },
        })
    }
    pub fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>> {
        let mut opt = self.level_zero.get(key)?;
        if opt.is_some() {
            Ok(opt)
        } else {
            for level in &mut self.higher_levels {
                opt = level.get(key)?;
                if opt.is_some() {
                    return Ok(opt);
                }
            }
            return Ok(None);
        }
    }
}

impl Segment {
    pub fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>> {
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

    fn read_table_entry(&mut self, offset: &u64) -> io::Result<SsTableEntry> {
        Ok(SsTableEntry {
            key: String::from("key"),
            value: None,
            deleted: false,
            sequence_number: 0,
        })
    }
}

impl SegmentIndex {
    fn find_closest_offset(&self, key: &String) -> u64 {
        return 0;
    }
}

impl SsTableEntry {
    /* pub fn to_bytes(&self) -> Vec<u8> {} */

    pub fn len(&self) -> usize {
        /*
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
        2 * size_of::<u32>() + size_of::<u64>() + self.key.len() + value_len
    }
}

impl LevelSearch for OverlappingLevel {
    fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>> {
        Ok(None)
    }
}

impl LevelSearch for PartitionedLevel {
    fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>> {
        Ok(None)
    }
}

impl<T> SsTableLevel<T>
where
    T: LevelSearch,
{
    pub fn get(&mut self, key: &String) -> io::Result<Option<SsTableEntry>> {
        self.inner.get(key)
    }
}
