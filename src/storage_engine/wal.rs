use std::fs::File;
use std::io::{Error, ErrorKind, Write};

pub enum OpType {
    Put,
    Delete,
}

enum SerError {
    InvalidKey,
    InvalidValue,
}

static HEADER_SIZE: usize = 12;

/*
methods:
1. sync
2. rotate
3. recover?
4. append
    1. checksum
    2. update file size
5. new()
6. Do I need synchronization or do we let the storage engine itself handle this?
*/

pub struct WalEntry {
    operation_type: OpType,
    key: String,
    value: String,
    sequence_number: u64,
}

pub struct Wal {
    file: File,
    file_size: u64, // might need to convert to atomic
    last_sequence_number: u64,
}

impl Wal {
    pub fn sync(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub fn size(&self) -> &u64 {
        &self.file_size
    }

    pub fn last_sequence_number(&self) -> &u64 {
        &self.last_sequence_number
    }

    pub fn append(&mut self, entry: &WalEntry) -> std::io::Result<()> {
        match Wal::serialize_entry(entry) {
            Ok(buf) => self
                .file
                .write_all(buf.as_slice())
                .map(|_| self.last_sequence_number = entry.sequence_number),
            Err(e) => match e {
                SerError::InvalidKey => Err(Error::new(
                    ErrorKind::Other,
                    "Serialization error, invalid key",
                )),
                SerError::InvalidValue => Err(Error::new(
                    ErrorKind::Other,
                    "Serialization error, invalid value",
                )),
            },
        }
    }

    fn serialize_entry(entry: &WalEntry) -> Result<Vec<u8>, SerError> {
        if entry.key.is_empty() {
            return Err(SerError::InvalidKey);
        }
        // type 2 bytes and sequence number 4 bytes
        let buf_size: usize = HEADER_SIZE + 6 + entry.key.len() + entry.value.len();
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(buf_size, 0);
        let mut offset = HEADER_SIZE;
        match entry.operation_type {
            OpType::Put => {
                let i: u16 = 1;
                buf[HEADER_SIZE..HEADER_SIZE + 2].copy_from_slice(&i.to_le_bytes());
            }
            OpType::Delete => {
                let i: u16 = 2;
                buf[HEADER_SIZE..HEADER_SIZE + 2].copy_from_slice(&i.to_le_bytes());
            }
        }
        offset = offset + 2;
        buf[offset..offset + entry.key.len()].copy_from_slice(entry.key.as_bytes());
        offset = offset + entry.key.len();
        buf[offset..offset + entry.value.len()].copy_from_slice(entry.value.as_bytes());
        buf[buf_size - 4..buf_size].copy_from_slice(&entry.sequence_number.to_le_bytes());

        let checksum = Wal::calculate_checksum(&buf[HEADER_SIZE..buf_size]);
        buf[0..3].copy_from_slice(&buf_size.to_le_bytes());
        buf[4..HEADER_SIZE].copy_from_slice(&checksum.to_le_bytes());

        Ok(buf)
    }

    fn calculate_checksum(_buf: &[u8]) -> u64 {
        0
    }
}
