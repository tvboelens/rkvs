use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, Write};

// length 4 bytes checksum 4 bytes key_len 4 bytes and op 2 bytes
pub static HEADER_SIZE: usize = 3 * size_of::<u32>() + size_of::<u16>();
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

pub enum OpType {
    Put,
    Delete,
}

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

pub fn determine_segment_filename(
    timeline: u32,
    sequence_number: u64,
    segment_size: u64,
) -> String {
    let str1 = format!("{:x}", timeline);
    let str2 = &format!("{:x}", sequence_number)[0..8];
    let base: u64 = 2;
    let no_of_segments: u64 = base.pow(32) / segment_size;
    let segment_no = sequence_number / no_of_segments;
    let str3 = &format!("{:x}", segment_no)[8..];
    str1 + str2 + str3 + ".wal"
}

impl Segment {
    pub fn append(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let buf_size = buf.len() as u32;
        self.file.write_all(buf).map(|_| self.file_size += buf_size)
    }

    pub fn pad(&mut self) -> std::io::Result<()> {
        let buf_size = self.max_size - self.file_size;
        let mut buf = Vec::new();
        buf.resize(buf_size.try_into().unwrap(), 0);
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

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn remaining_space(&self) -> u32 {
        self.max_size - self.file_size
    }

    pub fn recover(
        &mut self,
        table: &mut HashMap<String, Option<String>>,
        offset: u32,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut wal_entries = Vec::<WalEntry>::new();
        let res = self.read_parse_validate(&mut wal_entries, offset);
        for entry in wal_entries {
            match entry.operation_type {
                OpType::Put => {
                    table.insert(entry.key, entry.value);
                }
                OpType::Delete => {
                    table.insert(entry.key, None);
                }
            }
        }
        res
    }

    fn read_parse_validate(
        &mut self,
        entries: &mut Vec<WalEntry>,
        offset: u32,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut bytes = Vec::<u8>::new();
        self.file.seek(io::SeekFrom::Start(offset as u64))?;
        self.file.read_to_end(&mut bytes)?;
        Segment::parse_wal_entries(&bytes, entries)
    }

    fn parse_wal_entries(
        bytes: &Vec<u8>,
        entries: &mut Vec<WalEntry>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut offset: usize = 0;
        let mut record_len: usize = 0;
        let mut u32_buf: [u8; 4] = [0, 0, 0, 0];
        while offset + record_len < bytes.len() {
            u32_buf.copy_from_slice(&bytes[offset..offset + 4]);
            record_len = u32::from_le_bytes(u32_buf) as usize;
            offset += 4;
            if offset + record_len <= bytes.len() {
                let entry = WalEntry::from_bytes(&bytes[offset..offset + record_len]);
                entries.push(entry);
                offset += record_len;
            }
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
            }
            OpType::Delete => {
                buf[offset] = 2;
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

    // Return Result?
    // Op bytes could be out of range (>2)
    // mismatching lengths
    // invalid utf8
    // Checksum mismatch
    fn from_bytes(bytes: &[u8]) -> Self {
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
        let key = String::from_utf8(bytes[offset..offset + key_len as usize].to_vec()).unwrap();
        offset += key_len as usize;
        let mut value = None;
        if offset < bytes.len() {
            u32_buf.copy_from_slice(&bytes[offset..offset + size_of::<u32>()]);
            offset += size_of::<u32>();
            let value_len = u32::from_le_bytes(u32_buf);
            value =
                Some(String::from_utf8(bytes[offset..offset + key_len as usize].to_vec()).unwrap());
            offset += value_len as usize;
        }
        let mut u64_buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        u64_buf.copy_from_slice(&bytes[offset..]);
        let sequence_number = u64::from_le_bytes(u64_buf);
        WalEntry {
            operation_type: op,
            key: key,
            value: value,
            sequence_number: sequence_number,
        }
    }
}
