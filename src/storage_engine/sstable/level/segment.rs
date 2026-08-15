use crate::storage_engine::memtable::MemTableValue;
use crate::storage_engine::sstable::SsTableEntry;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;

static MAGIC_BYTES: [u8; 4] = [0x72, 0x6B, 0x76, 0x73]; //rkvs
static SEGMENT_FOOTER_LEN: u64 = 12; // magic bytes 4, index offset 8

pub struct Segment {
    file: File,
    filepath: PathBuf,
    index: SegmentIndex,
    highest_sequence_no: u64,
    size: u64,
    data_block_end: u64,
    number: u64,
}

#[derive(Debug, PartialEq)]
struct SegmentIndex {
    indices: Vec<SegmentIndexEntry>,
    len: usize,
}

pub struct SegmentFooter {
    index_offset: u64,
    // bloom_filter_offset: u64,
    // bloom_filter_size: u64
}

#[derive(Debug, PartialEq)]
struct SegmentIndexEntry {
    key: String,
    offset: u64,
}

enum IndexSearchResult {
    Match(u64),
    Range(u64, Option<u64>),
}

impl Segment {
    pub fn from_file(path: PathBuf) -> io::Result<Self> {
        let mut rfile = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)?;
        let metadata = rfile.metadata()?;
        let mut footer_buf = Vec::<u8>::new();
        rfile.seek(io::SeekFrom::End(-(SEGMENT_FOOTER_LEN as i64)))?;
        rfile.read_to_end(&mut footer_buf)?;
        if footer_buf[footer_buf.len() - MAGIC_BYTES.len()..] != MAGIC_BYTES {
            todo!()
            //return Err(io::Error::from(io::ErrorKind::NotSeekable))
        }
        let footer = SegmentFooter::from_bytes(
            &footer_buf[0..SEGMENT_FOOTER_LEN as usize - MAGIC_BYTES.len()].to_vec(),
        );

        let mut index_buf = Vec::<u8>::new();
        let buf_len = metadata.size() - SEGMENT_FOOTER_LEN - footer.index_offset;
        index_buf.resize(buf_len as usize, 0);
        rfile.seek(io::SeekFrom::Start(footer.index_offset))?;
        rfile.read_exact(&mut index_buf)?;
        let index = SegmentIndex::from_bytes(&index_buf);
        let highest_sequence_no = 0;
        let segment_number = 0;
        Ok(Segment {
            file: rfile,
            filepath: path,
            highest_sequence_no: highest_sequence_no,
            index: index,
            size: metadata.size(),
            number: segment_number,
            data_block_end: footer.index_offset,
        })
    }

    pub fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        match self.index.find(key) {
            IndexSearchResult::Match(offset) => {
                self.read_table_entry(&offset).map(|entry| Some(entry))
            }
            IndexSearchResult::Range(start, opt) => {
                let end = opt.unwrap_or(self.data_block_end);
                let entries = self.read_table_entries(start, end)?;
                for entry in entries {
                    if entry.key == *key {
                        return Ok(Some(entry));
                    } else if entry.key > *key {
                        return Ok(None);
                    }
                }
                Ok(None)
            }
        }
    }

    fn read_table_entries(&self, start: u64, end: u64) -> io::Result<Vec<SsTableEntry>> {
        let mut curr_offset = start.clone();
        let mut buf = Vec::<u8>::new();
        buf.resize(end as usize - start as usize, 0);
        let mut buf_offset: usize = 0;
        let mut bytes_read: usize = 0;
        while bytes_read < buf.len() {
            bytes_read += self
                .file
                .read_at(&mut buf[buf_offset..], curr_offset.clone())?;
            buf_offset += bytes_read;
            curr_offset += bytes_read as u64;
        }
        Ok(Segment::parse_entries(buf))
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

    pub fn highest_sequence_number(&self) -> u64 {
        self.highest_sequence_no
    }

    pub fn filepath(&self) -> PathBuf {
        self.filepath.clone()
    }

    pub fn write_segment_file(
        dir: &PathBuf,
        table: &HashMap<String, MemTableValue>,
        sequence_number: &u64,
        index_sparsity_factor: &u32,
    ) -> io::Result<PathBuf> {
        let filename = Segment::determine_segment_filename(sequence_number);
        let filepath = dir.join(filename);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath.clone())?;
        let mut writer = BufWriter::new(file);
        let mut keys: Vec<String> = table.keys().cloned().collect();
        keys.sort_unstable();
        let mut index = SegmentIndex::new();
        let mut counter: u32 = 0;
        let mut offset: u64 = 0;
        for key in keys {
            if counter % index_sparsity_factor == 0 {
                index.add_index(key.clone(), offset.clone());
            }
            let value = table.get(&key).unwrap().clone();
            let entry = SsTableEntry::from(key, value);
            let bytes = entry.to_bytes();
            let entry_len = bytes.len() as u32;
            writer.write_all(&entry_len.to_le_bytes())?;
            offset += size_of::<u32>() as u64;
            writer.write_all(&bytes)?;
            offset += entry.len() as u64;
            counter += 1;
        }
        writer.write_all(&index.to_bytes())?;
        let footer = SegmentFooter {
            index_offset: offset,
        };
        writer.write_all(&footer.to_bytes())?;
        writer.flush()?;
        Ok(filepath)
    }

    fn determine_segment_filename(sequence_number: &u64) -> String {
        let mut padding_bytes = Vec::<u8>::new();
        let level_number_str = String::from("00000000");
        let mut sequence_number_hex_str = format!("{:x}", sequence_number);
        padding_bytes.resize(8 - sequence_number_hex_str.len(), 48); // "0" = 0x30 = 48
        sequence_number_hex_str.insert_str(0, &String::from_utf8(padding_bytes).unwrap());
        level_number_str + &sequence_number_hex_str
    }

    fn parse_entries(buf: Vec<u8>) -> Vec<SsTableEntry> {
        let mut res = Vec::new();
        let mut offset: usize = 0;
        let mut u64_buf: [u8; size_of::<u64>()] = [0, 0, 0, 0, 0, 0, 0, 0];
        while offset < buf.len() {
            u64_buf.copy_from_slice(&buf[offset..offset + size_of::<u64>()]);
            let record_len = u64::from_le_bytes(u64_buf);
            offset += size_of::<u64>();
            res.push(SsTableEntry::from_bytes(
                &buf[offset..offset + record_len as usize].to_vec(),
            ));
            offset += record_len as usize;
        }
        res
    }
}

impl SegmentIndex {
    fn new() -> Self {
        SegmentIndex {
            indices: Vec::new(),
            len: 0,
        }
    }

    fn find(&self, key: &String) -> IndexSearchResult {
        if self.indices.is_empty() {
            return IndexSearchResult::Range(0, None);
        } else if *key < self.indices[0].key {
            return IndexSearchResult::Range(0, Some(self.indices[0].offset));
        } else {
            let (entry, idx) = self.binary_search(key);
            if *key == entry.key {
                return IndexSearchResult::Match(entry.offset);
            } else {
                if idx < self.indices.len() - 1 {
                    return IndexSearchResult::Range(
                        entry.offset,
                        Some(self.indices[idx + 1].offset),
                    );
                } else {
                    return IndexSearchResult::Range(entry.offset, None);
                }
            }
        }
    }

    fn binary_search(&self, key: &String) -> (&SegmentIndexEntry, usize) {
        let mut l: usize = 0;
        let mut r = self.indices.len() - 1;
        let mut m = (l + r) / 2 + 1;
        while l < r {
            if self.indices[m].key <= *key {
                l = m;
            } else {
                r = m - 1;
            }
            m = (l + r) / 2 + 1;
        }
        (&self.indices[l], l)
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.reserve(self.len);
        for index in &self.indices {
            buf.append(&mut index.to_bytes());
        }
        buf
    }

    fn add_index(&mut self, key: String, offset: u64) {
        self.len += key.len() + size_of::<u64>() + size_of::<u32>();
        self.indices.push(SegmentIndexEntry {
            key: key,
            offset: offset,
        });
    }

    fn from_bytes(buf: &Vec<u8>) -> Self {
        let mut segment_index = SegmentIndex::new();
        let mut buf_offset: usize = 0;
        let mut u64_buf: [u8; size_of::<u64>()] = [0, 0, 0, 0, 0, 0, 0, 0];
        while buf_offset < buf.len() {
            u64_buf.copy_from_slice(&buf[buf_offset..buf_offset + size_of::<u64>()]);
            let key_len = u64::from_le_bytes(u64_buf);
            buf_offset += size_of::<u64>();
            let key =
                String::from_utf8(buf[buf_offset..buf_offset + key_len as usize].to_vec()).unwrap();
            buf_offset += key_len as usize;
            u64_buf.copy_from_slice(&buf[buf_offset..buf_offset + size_of::<u64>()]);
            segment_index.add_index(key, u64::from_le_bytes(u64_buf));
            buf_offset += size_of::<u64>();
        }
        segment_index
    }
}

impl SegmentIndexEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut offset = 0;
        let mut buf = Vec::<u8>::new();
        buf.resize(self.key.len() + 2 * size_of::<u64>(), 0);
        let len = self.key.len() as u32;
        buf[offset..offset + size_of::<u32>()].copy_from_slice(&len.to_le_bytes());
        offset += size_of::<u64>();
        buf[offset..offset + self.key.len()].copy_from_slice(self.key.as_bytes());
        offset += self.key.len();
        buf[offset..].copy_from_slice(&self.offset.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &Vec<u8>) -> Self {
        let mut buf_offset = 0;
        let key_len = u32::from_le_bytes(
            buf[buf_offset..buf_offset + size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        buf_offset += size_of::<u64>();
        let key = String::from_utf8(buf[buf_offset..buf_offset + key_len].to_vec()).unwrap();
        buf_offset += key_len;
        let offset = u64::from_le_bytes(
            buf[buf_offset..buf_offset + size_of::<u64>()]
                .try_into()
                .unwrap(),
        );
        SegmentIndexEntry {
            key: key,
            offset: offset,
        }
    }
}

impl SegmentFooter {
    pub fn from_bytes(buf: &Vec<u8>) -> Self {
        assert_eq!(buf.len(), SEGMENT_FOOTER_LEN as usize - MAGIC_BYTES.len());
        let index_offset = u64::from_le_bytes(buf[0..size_of::<u64>()].try_into().unwrap());
        SegmentFooter {
            index_offset: index_offset,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.resize(size_of::<u64>() + MAGIC_BYTES.len(), 0);
        buf[0..size_of::<u64>()].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[size_of::<u64>()..].copy_from_slice(&MAGIC_BYTES);
        buf
    }
}

#[cfg(test)]
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

    #[test]
    fn serde_index_entry() {
        let entry_write = SegmentIndexEntry {
            key: String::from("key"),
            offset: 20,
        };
        let entry_read = SegmentIndexEntry::from_bytes(&entry_write.to_bytes());
        assert_eq!(entry_write, entry_read);
    }

    #[test]
    fn serde_index_entry_empty_key() {
        let entry_write = SegmentIndexEntry {
            key: String::from(""),
            offset: 20,
        };
        let entry_read = SegmentIndexEntry::from_bytes(&entry_write.to_bytes());
        assert_eq!(entry_write, entry_read);
    }

    #[test]
    fn empty_index() {
        let segment_index = SegmentIndex::new();
        let res = segment_index.find(&String::from("key"));
        assert!(matches!(res, IndexSearchResult::Range(start, end) if start == 0 && end.is_none())); // TODO: is this actually the expected value? or do we have anything before the data block?
    }

    #[test]
    fn index_find_contains_key() {
        let mut segment_index = SegmentIndex::new();
        segment_index.add_index(String::from("key0"), 0);
        segment_index.add_index(String::from("key1"), 5);
        segment_index.add_index(String::from("key2"), 10);
        let res = segment_index.find(&String::from("key0"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 0));
        let res = segment_index.find(&String::from("key1"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 5));
        let res = segment_index.find(&String::from("key2"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 10));
    }

    #[test]
    fn index_find_contains_empty_key() {
        let mut segment_index = SegmentIndex::new();
        segment_index.add_index(String::from(""), 0);
        segment_index.add_index(String::from("key1"), 5);
        segment_index.add_index(String::from("key2"), 10);
        let res = segment_index.find(&String::from(""));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 0));
        let res = segment_index.find(&String::from("key1"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 5));
        let res = segment_index.find(&String::from("key2"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 10));
    }

    #[test]
    fn index_find_empty_key_sparse() {
        let mut segment_index = SegmentIndex::new();
        segment_index.add_index(String::from("key1"), 5);
        segment_index.add_index(String::from("key2"), 10);
        let res = segment_index.find(&String::from(""));
        assert!(
            matches!(res, IndexSearchResult::Range(start, end) if start == 0 && end.unwrap() == 5)
        );
        let res = segment_index.find(&String::from("key1"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 5));
        let res = segment_index.find(&String::from("key2"));
        assert!(matches!(res, IndexSearchResult::Match(offset) if offset == 10));
    }

    #[test]
    fn index_find_sparse() {
        let mut segment_index = SegmentIndex::new();
        segment_index.add_index(String::from("key0"), 0);
        segment_index.add_index(String::from("key2"), 5);
        segment_index.add_index(String::from("key4"), 10);
        let res = segment_index.find(&String::from("key1"));
        assert!(
            matches!(res, IndexSearchResult::Range(start, end) if start == 0 && end.unwrap() == 5)
        );
        let res = segment_index.find(&String::from("key3"));
        assert!(
            matches!(res, IndexSearchResult::Range(start, end) if start == 5 && end.unwrap() == 10)
        );
        let res = segment_index.find(&String::from("key5"));
        assert!(
            matches!(res, IndexSearchResult::Range(start, end) if start == 10 && end.is_none())
        );
    }

    #[test]
    fn index_to_bytes() {
        let mut segment_index_write = SegmentIndex::new();
        segment_index_write.add_index(String::from("key0"), 0);
        segment_index_write.add_index(String::from("key2"), 5);
        segment_index_write.add_index(String::from("key4"), 10);
        let segment_index_read = SegmentIndex::from_bytes(&segment_index_write.to_bytes());
        assert_eq!(segment_index_read, segment_index_write);
    }

    #[test]
    fn segment_basic_get() {
        let dir = PathBuf::from("./sstable_segment_basic_get");
        let cl = Cleanup { dir: dir.clone() };
        assert!(cl.setup().is_ok());
        let mut table = HashMap::new();
        table.insert(
            String::from("key"),
            MemTableValue {
                value: Some(String::from("value")),
                sequence_number: 2,
            },
        );
        let sequence_number = 0;
        let sparsity_factor = 1;
        let fp =
            Segment::write_segment_file(&dir, &table, &sequence_number, &sparsity_factor).unwrap();
        let segment = Segment::from_file(fp).unwrap();
        let value = segment.get(&String::from("key")).unwrap();
        assert!(value.is_some());
        let entry = SsTableEntry::from(
            String::from("key"),
            MemTableValue {
                value: Some(String::from("value")),
                sequence_number: 2,
            },
        );
        assert_eq!(value.unwrap(), entry);
    }
}

/*
TODO testing:
1. Key not in segment
    1. Key we are looking for would come before the first key in segment
    2. Key we are looking for is "between" two keys in the segment
    3. Key we are looking for would come after the last key in segment
2. Lookup in segment with sparse indexing
    1. Key present
    2. Key not present
*/
