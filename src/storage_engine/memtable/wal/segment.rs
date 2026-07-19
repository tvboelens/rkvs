use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::string::FromUtf8Error;

// length 4 bytes checksum 4 bytes key_len 4 bytes and op 1 bytes
pub static HEADER_SIZE: usize = 3 * size_of::<u32>() + size_of::<u8>();
// crc32 with Castagnoli polynomial
static CHECKSUM_ALG: crc::Algorithm<u32> = crc::Algorithm {
    width: 32,
    poly: 0x1edc6f41,
    init: 0xffffffff,
    refin: true,
    refout: true,
    xorout: 0xffffffff,
    residue: 0xb798b438,
    check: 0xe3069283,
};

pub struct Segment {
    file: File,
    file_size: u32,
    max_size: u32,
}

#[derive(Debug, PartialEq)]
pub enum OpType {
    Put,
    Delete,
}

#[derive(Debug)]
pub enum RecoveryError {
    Io(io::Error),
    Corrupted,
}

#[derive(Debug, PartialEq)]
pub struct WalEntry {
    pub operation_type: OpType,
    pub key: String,
    pub value: Option<String>,
    pub sequence_number: u64,
}

pub fn calculate_checksum(buf: &[u8]) -> u32 {
    let crc32 = crc::Crc::<u32>::new(&CHECKSUM_ALG);
    let mut digest = crc32.digest();
    digest.update(buf);
    digest.finalize()
}

// timeline, logical_log_number, segment_number + ".wal"
// all hex strings of len 8 (u32)
// logical_log_number is first 4 bytes (u32) of segment_number
// segment number is last 4 bytes of segment_number (as u32) divided by segment_size
pub fn determine_segment_filename(
    timeline: &u32,
    sequence_number: &u64,
    segment_size: &u32,
) -> String {
    // The hex strings should have length 8 or 16, therefore we might need to pad them with leading zeros
    let mut padding_bytes = Vec::<u8>::new();
    let mut timeline_hex_str = format!("{:x}", timeline);
    padding_bytes.resize(8 - timeline_hex_str.len(), 48); // "0" = 0x30 = 48
    timeline_hex_str.insert_str(0, &String::from_utf8(padding_bytes.clone()).unwrap());
    // logical log is 4GB = 2^32 bytes, hence the number of the logical
    // log containing this sequence number is the first 8 bytes (32 bits)
    let mut sequence_number_hex_str = format!("{:x}", sequence_number);
    padding_bytes.resize(16 - sequence_number_hex_str.len(), 48);
    sequence_number_hex_str.insert_str(0, &String::from_utf8(padding_bytes.clone()).unwrap());
    let two: u64 = 2;
    let segment_no = sequence_number % two.pow(32) / *segment_size as u64;
    let mut segment_size_hex_str = format!("{:x}", segment_no);
    padding_bytes.resize(16 - segment_size_hex_str.len(), 48);
    segment_size_hex_str.insert_str(0, &String::from_utf8(padding_bytes).unwrap());
    timeline_hex_str + &sequence_number_hex_str[0..8] + &segment_size_hex_str[8..] + ".wal"
}

pub fn final_entry_after(filename: &str, file_size: u64, sequence_number: &u64) -> bool {
    let base_2: u64 = 2;
    let segment_no = u64::from_str_radix(&filename[16..24], 16).unwrap();
    let base_offset = u64::from_str_radix(&filename[8..16], 16).unwrap() * base_2.pow(32);
    *sequence_number <= base_offset + file_size * segment_no
}

impl Segment {
    pub fn append(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let buf_size = buf.len() as u32;
        assert!(self.file_size + buf_size <= self.max_size);
        self.file.write_all(buf).map(|_| self.file_size += buf_size)
    }

    pub fn pad(&mut self) -> std::io::Result<()> {
        let buf_size = self.max_size - self.file_size;
        let mut buf = Vec::new();
        buf.resize(buf_size as usize, 0);
        self.append(buf.as_slice())
    }

    pub fn new(file: File, max_size: u32) -> Self {
        let metadata = file.metadata().unwrap();
        let file_size = metadata.len() as u32;
        Segment {
            file: file,
            file_size: file_size,
            max_size: max_size,
        }
    }

    pub fn from(mut file: File, file_size: u32, max_size: u32) -> Self {
        file.seek(io::SeekFrom::End(0)).unwrap();
        Segment {
            file: file,
            file_size: file_size,
            max_size: max_size,
        }
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn remaining_space(&self) -> u32 {
        self.max_size - self.file_size
    }

    pub fn read_parse_validate_from_offset(
        &mut self,
        entries: &mut Vec<WalEntry>,
        offset: u32,
    ) -> Result<Option<Vec<u8>>, RecoveryError> {
        let mut bytes = Vec::<u8>::new();
        self.file.seek(io::SeekFrom::Start(offset as u64))?;
        self.file.read_to_end(&mut bytes)?;
        Segment::parse_validate_wal_entries(&bytes, entries)
    }

    pub fn read_parse_validate_from_partial_record(
        &mut self,
        mut bytes: Vec<u8>,
        entries: &mut Vec<WalEntry>,
    ) -> Result<Option<Vec<u8>>, RecoveryError> {
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.read_to_end(&mut bytes)?;
        Segment::parse_validate_wal_entries(&bytes, entries)
    }

    fn parse_validate_wal_entries(
        bytes: &Vec<u8>,
        entries: &mut Vec<WalEntry>,
    ) -> Result<Option<Vec<u8>>, RecoveryError> {
        let mut offset: usize = 0;
        let mut record_len: usize;
        let mut u32_buf: [u8; size_of::<u32>()] = [0, 0, 0, 0];
        loop {
            if size_of::<u32>() + offset > bytes.len() {
                break;
            }
            u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
            record_len = u32::from_le_bytes(u32_buf) as usize;
            if record_len < HEADER_SIZE - size_of::<u32>() {
                return Err(RecoveryError::Corrupted);
            }
            offset += size_of::<u32>();
            if offset + record_len > bytes.len() {
                break;
            }
            u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
            let read_checksum = u32::from_le_bytes(u32_buf);
            offset += size_of::<u32>();
            let calculated_checksum =
                calculate_checksum(&bytes[offset..offset + record_len - size_of::<u32>()]);
            if read_checksum != calculated_checksum {
                return Err(RecoveryError::Corrupted);
            }
            let entry =
                WalEntry::from_bytes(&bytes[offset..offset + record_len - size_of::<u32>()])?;
            entries.push(entry);
            offset += record_len - size_of::<u32>();
        }
        if offset == bytes.len() {
            Ok(None)
        } else {
            Ok(Some(bytes[offset..].to_vec()))
        }
    }
}

impl WalEntry {
    /*
    entry len: u32
    checksum: u32
    op: u8
    key_len: u32
    key: variable length
    value_len (optional): u32
    value (optional): variable length
    sequence_number: u64
    */
    pub fn to_bytes(&self) -> Vec<u8> {
        assert!(!self.key.is_empty());
        let buf_size: usize = match self.value.as_ref() {
            None => HEADER_SIZE + size_of::<u64>() + self.key.len(),
            Some(value) => {
                HEADER_SIZE + size_of::<u32>() + size_of::<u64>() + self.key.len() + value.len()
            }
        };
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(buf_size, 0);
        let mut offset = 2 * size_of::<u32>();
        match self.operation_type {
            OpType::Put => {
                buf[offset] = 1;
                assert!(self.value.is_some());
            }
            OpType::Delete => {
                buf[offset] = 2;
                assert!(self.value.is_none());
            }
        }
        offset += 1;
        let key_len = self.key.len() as u32;
        buf[offset..offset + size_of::<u32>()].copy_from_slice(&key_len.to_le_bytes());
        offset += size_of::<u32>();
        buf[offset..offset + self.key.len()].copy_from_slice(self.key.as_bytes());
        offset = offset + self.key.len();
        if let Some(value) = &self.value {
            let value_len = value.len() as u32;
            buf[offset..offset + size_of::<u32>()].copy_from_slice(&value_len.to_le_bytes());
            offset += size_of::<u32>();
            buf[offset..offset + value.len()].copy_from_slice(value.as_bytes());
        }
        buf[buf_size - size_of::<u64>()..buf_size]
            .copy_from_slice(&self.sequence_number.to_le_bytes());
        let checksum = calculate_checksum(&buf[2 * size_of::<u32>()..buf_size]);
        let entry_len = (buf_size - size_of::<u32>()) as u32;
        buf[0..size_of::<u32>()].copy_from_slice(&entry_len.to_le_bytes());
        buf[size_of::<u32>()..2 * size_of::<u32>()].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, RecoveryError> {
        if bytes.len() < 1 + size_of::<u32>() {
            return Err(RecoveryError::Corrupted);
        }
        let mut u32_buf: [u8; 4] = [0, 0, 0, 0];
        let mut offset: usize = 1;
        let op: OpType;
        if bytes[0] == 1 {
            op = OpType::Put;
        } else {
            op = OpType::Delete;
        }
        u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
        let key_len = u32::from_le_bytes(u32_buf);
        offset += size_of::<u32>();
        if bytes.len() < offset + key_len as usize {
            return Err(RecoveryError::Corrupted);
        }
        let key = String::from_utf8(bytes[offset..offset + key_len as usize].to_vec())?;
        offset += key_len as usize;
        let mut value = None;
        if offset + size_of::<u64>() < bytes.len() {
            u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
            offset += size_of::<u32>();
            let value_len = u32::from_le_bytes(u32_buf);
            if bytes.len() < offset + value_len as usize {
                return Err(RecoveryError::Corrupted);
            }
            value = Some(String::from_utf8(
                bytes[offset..offset + value_len as usize].to_vec(),
            )?);
            offset += value_len as usize;
        }
        if bytes.len() < offset + 8 {
            return Err(RecoveryError::Corrupted);
        }
        let mut u64_buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        u64_buf.copy_from_slice(&bytes[offset..]);
        let sequence_number = u64::from_le_bytes(u64_buf);
        Ok(WalEntry {
            operation_type: op,
            key: key,
            value: value,
            sequence_number: sequence_number,
        })
    }
}

impl From<io::Error> for RecoveryError {
    fn from(value: io::Error) -> Self {
        RecoveryError::Io(value)
    }
}

impl From<FromUtf8Error> for RecoveryError {
    fn from(_: FromUtf8Error) -> Self {
        RecoveryError::Corrupted
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, DirBuilder, File};
    use std::io;
    use std::path::PathBuf;

    use super::{
        HEADER_SIZE, OpType, RecoveryError, Segment, WalEntry, determine_segment_filename,
        final_entry_after,
    };

    struct Cleanup {
        dir: PathBuf,
    }

    impl Cleanup {
        fn setup(&self) -> io::Result<()> {
            let _ = fs::remove_dir_all(&self.dir);
            DirBuilder::new().create(&self.dir)
        }
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn serde_wal_entry_delete_ok() {
        let write_entry = WalEntry {
            operation_type: OpType::Delete,
            key: String::from("key"),
            value: None,
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..]).unwrap();
        assert_eq!(read_entry.key, write_entry.key);
        assert!(read_entry.value.is_none());
        assert_eq!(read_entry.sequence_number, write_entry.sequence_number);
        assert_eq!(read_entry.operation_type, write_entry.operation_type);
    }

    #[test]
    fn serde_wal_entry_put_ok() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..]).unwrap();
        assert_eq!(read_entry.key, write_entry.key);
        assert_eq!(read_entry.sequence_number, write_entry.sequence_number);
        assert_eq!(read_entry.operation_type, write_entry.operation_type);
        assert_eq!(read_entry.value, write_entry.value);
    }

    #[test]
    #[should_panic]
    fn serde_wal_entry_missing_key() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::new(),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let _ = write_entry.to_bytes();
    }

    #[test]
    #[should_panic]
    fn serde_wal_entry_put_missing_value() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: None,
            sequence_number: 1024,
        };
        let _ = write_entry.to_bytes();
    }

    #[test]
    #[should_panic]
    fn serde_wal_entry_delete_value_panic() {
        let write_entry = WalEntry {
            operation_type: OpType::Delete,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let _ = write_entry.to_bytes();
    }

    #[test]
    fn serde_wal_entry_put_truncated_key_len() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE - 2]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_put_truncated_key() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE + 1]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_put_truncated_value_len() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE + 4]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_put_truncated_value() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE + 8]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_put_truncated_sequence_no() {
        let write_entry = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            value: Some(String::from("value")),
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..bytes.len() - 2]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_del_truncated_key_len() {
        let write_entry = WalEntry {
            operation_type: OpType::Delete,
            key: String::from("key"),
            value: None,
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE - 2]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_del_truncated_key() {
        let write_entry = WalEntry {
            operation_type: OpType::Delete,
            key: String::from("key"),
            value: None,
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..HEADER_SIZE + 1]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_wal_entry_del_truncated_sequence_no() {
        let write_entry = WalEntry {
            operation_type: OpType::Delete,
            key: String::from("key"),
            value: None,
            sequence_number: 1024,
        };
        let bytes = write_entry.to_bytes();
        let read_entry = WalEntry::from_bytes(&bytes[2 * size_of::<u32>()..bytes.len() - 4]);
        assert!(matches!(read_entry, Err(RecoveryError::Corrupted)));
    }

    #[test]
    fn serde_multiple_wal_entries() {
        let mut wal_entries_write = Vec::<WalEntry>::new();
        for n in 0..1000 {
            if n % 7 == 0 {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Put,
                    key: String::from("key") + &n.to_string(),
                    value: Some(String::from("value") + &n.to_string()),
                    sequence_number: n as u64,
                });
            } else {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Delete,
                    key: String::from("key") + &n.to_string(),
                    value: None,
                    sequence_number: n as u64,
                });
            }
        }
        let mut bytes = Vec::<u8>::new();
        for entry in &wal_entries_write {
            bytes.append(&mut entry.to_bytes());
        }
        let mut wal_entries_read = Vec::<WalEntry>::new();
        let res = Segment::parse_validate_wal_entries(&bytes, &mut wal_entries_read);
        assert!(matches!(res, Ok(None)));
        assert_eq!(wal_entries_read, wal_entries_write);
    }

    #[test]
    fn serde_multiple_wal_entries_checksum_mismatch() {
        let mut wal_entries_write = Vec::<WalEntry>::new();
        for n in 0..1000 {
            if n % 7 == 0 {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Put,
                    key: String::from("key") + &n.to_string(),
                    value: Some(String::from("value") + &n.to_string()),
                    sequence_number: n as u64,
                });
            } else {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Delete,
                    key: String::from("key") + &n.to_string(),
                    value: None,
                    sequence_number: n as u64,
                });
            }
        }
        let mut bytes = Vec::<u8>::new();
        for entry in &wal_entries_write {
            if entry.sequence_number == 500 {
                let mut corrupted_bytes = entry.to_bytes();
                corrupted_bytes[7] += 1;
                bytes.append(&mut corrupted_bytes);
            } else {
                bytes.append(&mut entry.to_bytes());
            }
        }
        let mut wal_entries_read = Vec::<WalEntry>::new();
        let res = Segment::parse_validate_wal_entries(&bytes, &mut wal_entries_read);
        assert!(matches!(res, Err(RecoveryError::Corrupted)));
        assert_eq!(wal_entries_read.len(), 500);
        assert_eq!(wal_entries_read, wal_entries_write[0..500]);
    }

    #[test]
    fn serde_multiple_wal_entries_truncated_entry() {
        let mut wal_entries_write = Vec::<WalEntry>::new();
        for n in 0..1000 {
            if n % 7 == 0 {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Put,
                    key: String::from("key") + &n.to_string(),
                    value: Some(String::from("value") + &n.to_string()),
                    sequence_number: n as u64,
                });
            } else {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Delete,
                    key: String::from("key") + &n.to_string(),
                    value: None,
                    sequence_number: n as u64,
                });
            }
        }
        let mut bytes = Vec::<u8>::new();
        for entry in &wal_entries_write {
            if entry.sequence_number == 500 {
                let corrupted_bytes = entry.to_bytes();
                bytes.append(&mut corrupted_bytes[0..corrupted_bytes.len() - 5].to_vec());
            } else {
                bytes.append(&mut entry.to_bytes());
            }
        }
        let mut wal_entries_read = Vec::<WalEntry>::new();
        let res = Segment::parse_validate_wal_entries(&bytes, &mut wal_entries_read);
        assert!(matches!(res, Err(RecoveryError::Corrupted)));
        assert_eq!(wal_entries_read.len(), 500);
        assert_eq!(wal_entries_read, wal_entries_write[0..500]);
    }

    #[test]
    fn serde_multiple_wal_entries_zero_len_entry() {
        let mut wal_entries_write = Vec::<WalEntry>::new();
        for n in 0..10 {
            if n % 7 == 0 {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Put,
                    key: String::from("key") + &n.to_string(),
                    value: Some(String::from("value") + &n.to_string()),
                    sequence_number: n as u64,
                });
            } else {
                wal_entries_write.push(WalEntry {
                    operation_type: OpType::Delete,
                    key: String::from("key") + &n.to_string(),
                    value: None,
                    sequence_number: n as u64,
                });
            }
        }
        let mut bytes = Vec::<u8>::new();
        for entry in &wal_entries_write {
            if entry.sequence_number == 5 {
                let len: u32 = 0;
                let mut corrupted_bytes = entry.to_bytes();
                corrupted_bytes[0..size_of::<u32>()].copy_from_slice(&len.to_le_bytes());
                bytes.append(&mut corrupted_bytes);
            } else {
                bytes.append(&mut entry.to_bytes());
            }
        }
        let mut wal_entries_read = Vec::<WalEntry>::new();
        let res = Segment::parse_validate_wal_entries(&bytes, &mut wal_entries_read);
        assert!(matches!(res, Err(RecoveryError::Corrupted)));
        assert_eq!(wal_entries_read.len(), 5);
        assert_eq!(wal_entries_read, wal_entries_write[0..5]);
    }

    #[test]
    fn segment_append_ok() {
        let dir = PathBuf::from("./Segment_Append_Ok");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let filename = determine_segment_filename(&0, &0, &segment_size);
        let fp = dir.join(filename);
        let file = File::create(fp).unwrap();
        let mut segment = Segment::new(file, segment_size);
        segment.append(vec![0, 1, 2, 3].as_slice()).unwrap();
        assert_eq!(segment.file_size, 4);
    }

    #[test]
    fn segment_append_empty() {
        let dir = PathBuf::from("./Segment_Append_Empty");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let filename = determine_segment_filename(&0, &0, &segment_size);
        let fp = dir.join(filename);
        let file = File::create(fp).unwrap();
        let mut segment = Segment::new(file, segment_size);
        let buf = Vec::new();
        segment.append(&buf).unwrap();
        assert_eq!(segment.file_size, 0);
    }

    #[test]
    fn segment_append_one_entry() {
        let dir = PathBuf::from("./Segment_Append_One_Entry");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let filename = determine_segment_filename(&0, &0, &segment_size);
        let fp = dir.join(filename);
        let file = File::create(fp).unwrap();
        let mut segment = Segment::new(file, segment_size);
        let bytes = WalEntry {
            operation_type: OpType::Put,
            key: String::from("key"),
            sequence_number: 1024,
            value: Some(String::from("value")),
        }
        .to_bytes();
        segment.append(&bytes).unwrap();
        assert_eq!(segment.file_size as usize, bytes.len());
    }

    #[test]
    #[should_panic]
    fn segment_append_too_large() {
        let dir = PathBuf::from("./Segment_Append_Too_Large");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 2;
        assert!(cl.setup().is_ok());
        let filename = determine_segment_filename(&0, &0, &segment_size);
        let fp = dir.join(filename);
        let file = File::create(fp).unwrap();
        let mut segment = Segment::new(file, segment_size);
        let _ = segment.append(vec![0, 1, 2, 3].as_slice());
    }

    #[test]
    fn segment_pad() {
        let dir = PathBuf::from("./Segment_Pad");
        let cl = Cleanup { dir: dir.clone() };
        let segment_size = 256;
        assert!(cl.setup().is_ok());
        let filename = determine_segment_filename(&0, &0, &segment_size);
        let fp = dir.join(filename);
        let file = File::create(fp).unwrap();
        let segment_file = file.try_clone().unwrap();
        let mut segment = Segment::new(file, segment_size);
        segment.append(vec![0, 1, 2, 3].as_slice()).unwrap();
        assert_eq!(segment.file_size, 4);
        segment.pad().unwrap();
        let metadata = segment_file.metadata().unwrap();
        assert_eq!(segment.file_size, segment_size);
        assert_eq!(metadata.len(), segment_size as u64);
    }

    struct FilenameTest {
        pub timeline: u32,
        pub sequence_number: u64,
        pub segment_size: u32,
        pub expected: String,
    }

    #[test]
    fn segment_filename() {
        let base_two: u32 = 2;
        let base_two_u64: u64 = 2;
        let tests = vec![
            FilenameTest {
                timeline: 0,
                sequence_number: 0,
                segment_size: 2,
                expected: String::from("000000000000000000000000.wal"),
            },
            FilenameTest {
                timeline: 0,
                sequence_number: base_two_u64.pow(16) + 30, // 0x000000000001001e
                segment_size: base_two.pow(8),
                expected: String::from("000000000000000000000100.wal"),
            },
            FilenameTest {
                timeline: 0,
                sequence_number: base_two_u64.pow(40) + 8443, // 0x00000100000020fb
                segment_size: base_two.pow(16),
                expected: String::from("000000000000010000000000.wal"),
            },
            FilenameTest {
                timeline: 0,
                sequence_number: 56014641572749563, // 0x00c701050d2a20fb
                segment_size: base_two.pow(16),
                expected: String::from("0000000000c7010500000d2a.wal"),
            },
        ];
        for test in tests {
            assert_eq!(
                determine_segment_filename(
                    &test.timeline,
                    &test.sequence_number,
                    &test.segment_size
                ),
                test.expected
            );
        }
    }

    struct FinalEntryAfterTest {
        pub filename: String,
        pub sequence_number: u64,
        pub file_size: u64,
        pub expected: bool,
    }

    #[test]
    fn segment_final_entry_after() {
        let base_two_u64: u64 = 2;
        let tests = vec![
            FinalEntryAfterTest {
                filename: String::from("0000000000c7010500000d2a.wal"),
                sequence_number: 0,
                file_size: base_two_u64.pow(24),
                expected: true,
            },
            FinalEntryAfterTest {
                filename: String::from("000000000000000000000000.wal"),
                sequence_number: 65,
                file_size: base_two_u64.pow(6),
                expected: false,
            },
            FinalEntryAfterTest {
                filename: String::from("0000000000c7010500000d2a.wal"),
                sequence_number: 56014641572741120,
                file_size: base_two_u64.pow(24),
                expected: true,
            },
            FinalEntryAfterTest {
                filename: String::from("0000000000c7010500000d2a.wal"),
                sequence_number: 0x00c70105 * base_two_u64.pow(32) + 0xd2b * base_two_u64.pow(24),
                file_size: base_two_u64.pow(24),
                expected: false,
            },
        ];
        for test in tests {
            assert_eq!(
                final_entry_after(&test.filename, test.file_size, &test.sequence_number),
                test.expected
            );
        }
    }
}
// TODO: read_parse_validate both versions (from offset and partial entry)
