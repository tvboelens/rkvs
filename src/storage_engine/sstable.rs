use crate::storage_engine::memtable::MemTableValue;
use level::{LevelContainer, OverlappingLevel, PartitionedLevel, SsTableLevel};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Condvar};
use std::sync::{Mutex, RwLock};

mod compaction;
pub mod level;

#[derive(PartialEq, Debug)]
pub struct SsTableEntry {
    pub key: String,
    pub value: Option<String>,
    pub sequence_number: u64,
}

pub struct SsTable {
    inner: Arc<RwLock<LevelContainer>>,
    compaction_bt_cv: Arc<Condvar>,
    do_compact: Arc<Mutex<bool>>,
}

impl SsTable {
    pub fn start() -> io::Result<Self> {
        todo!()
    }

    pub fn get(&self, key: &String) -> io::Result<Option<SsTableEntry>> {
        let level_zero: Arc<SsTableLevel<OverlappingLevel>>;
        let partitioned_levels: Vec<Arc<SsTableLevel<PartitionedLevel>>>;
        {
            let lock = self.inner.read().unwrap();
            level_zero = lock.level_zero();
            partitioned_levels = lock.partitioned_levels();
        }

        match level_zero.get(key)? {
            Some(entry) => Ok(Some(entry)),
            None => {
                for level in &partitioned_levels {
                    match level.get(key)? {
                        Some(entry) => {
                            return Ok(Some(entry));
                        }
                        None => {
                            continue;
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn find_and_add_segment(&self, path: PathBuf) -> io::Result<()> {
        let do_compact: bool;
        {
            let mut lock = self.inner.write().unwrap();
            lock.add_to_level_zero(path)?;
            do_compact = lock.do_compact();
        }
        {
            let mut lock = self.do_compact.lock().unwrap();
            *lock = do_compact;
            self.compaction_bt_cv.notify_one();
        }
        Ok(())
    }
}

impl SsTableEntry {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut offset: usize = 0;
        buf.resize(self.len(), 0);
        let key_len = self.key.len() as u32;
        buf[offset..offset + size_of::<u32>()].copy_from_slice(&key_len.to_le_bytes());
        offset += size_of::<u32>();
        buf[offset..offset + self.key.len()].copy_from_slice(&self.key.as_bytes());
        offset += self.key.len();
        match &self.value {
            Some(value) => {
                let value_len = value.len() as u32;
                buf[offset..offset + size_of::<u32>()].copy_from_slice(&value_len.to_le_bytes());
                offset += size_of::<u32>();
                buf[offset] = 0;
                offset += 1;
                buf[offset..offset + value.len()].copy_from_slice(&value.as_bytes());
                offset += value.len();
            }
            None => {
                let value_len: u32 = 0;
                buf[offset..offset + size_of::<u32>()].copy_from_slice(&value_len.to_le_bytes());
                offset += size_of::<u32>();
                buf[offset] = 1;
                offset += 1;
            }
        }
        buf[offset..].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf
    }

    fn from_bytes(bytes: &Vec<u8>) -> Self {
        let mut offset: usize = 0;
        let mut u32_buf: [u8; size_of::<u32>()] = [0, 0, 0, 0];
        u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
        let key_len = u32::from_le_bytes(u32_buf);
        offset += size_of::<u32>();
        let key = String::from_utf8(bytes[offset..offset + key_len as usize].to_vec()).unwrap();
        offset += key_len as usize;
        u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
        let value_len = u32::from_le_bytes(u32_buf);
        offset += size_of::<u32>();
        let tombstone_value = bytes[offset];
        offset += 1;
        let value: Option<String>;
        if tombstone_value == 1 {
            value = None;
        } else {
            value = Some(
                String::from_utf8(bytes[offset..offset + value_len as usize].to_vec()).unwrap(),
            );
        }
        offset += value_len as usize;
        let mut u64_buf: [u8; size_of::<u64>()] = [0, 0, 0, 0, 0, 0, 0, 0];
        u64_buf.copy_from_slice(&bytes[offset..offset + size_of::<u64>()]);
        let sequence_no = u64::from_le_bytes(u64_buf);
        SsTableEntry {
            key: key,
            value: value,
            sequence_number: sequence_no,
        }
    }

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

    pub fn from(key: String, value: MemTableValue) -> Self {
        SsTableEntry {
            key: key,
            value: value.value,
            sequence_number: value.sequence_number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SsTableEntry;

    #[test]
    fn entry_to_bytes_value() {
        let entry_write = SsTableEntry {
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 2,
        };
        let bytes = entry_write.to_bytes();
        let entry_read = SsTableEntry::from_bytes(&bytes);
        assert_eq!(entry_read, entry_write);
    }

    #[test]
    fn entry_to_bytes_deleted_value() {
        let entry_write = SsTableEntry {
            key: String::from("key"),
            value: None,
            sequence_number: 2,
        };
        let bytes = entry_write.to_bytes();
        let entry_read = SsTableEntry::from_bytes(&bytes);
        assert_eq!(entry_read, entry_write);
    }
}
