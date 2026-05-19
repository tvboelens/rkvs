use std::io;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::oneshot::Sender;

pub mod memtable;
pub mod wal;

pub trait Store {
    fn get(&self, key: &String) -> Option<String>;
    fn submit_put(&self, job: WriteJob) -> Result<(), SendError<WriteJob>>;
    fn submit_delete(&self, job: WriteJob) -> Result<(), SendError<WriteJob>>;
}

pub struct PutData {
    pub key: String,
    pub value: String,
}
pub enum WriteData {
    Delete(String),
    Put(PutData),
}

pub struct WriteJob {
    pub data: WriteData,
    pub sender: Sender<io::Result<Option<String>>>,
}

pub struct StorageEngine {
    memtable: Arc<AtomicPtr<memtable::MemTable>>,
    sender: mpsc::Sender<WriteJob>,
    join_handle: std::thread::JoinHandle<()>,
}

struct Worker {
    memtable: Arc<AtomicPtr<memtable::MemTable>>,
    wal: wal::Wal,
    receiver: mpsc::Receiver<WriteJob>,
    timeout: Duration,
}

pub struct StorageEngineConf {
    timeout: Duration,
}

impl Store for StorageEngine {
    fn get(&self, key: &String) -> Option<String> {
        /* This will have to get more sophisticated once we move to on-disk persistence
           Also the unwrap should be replaced later by something that returns None
           so that the server can return a not found error or similar
        */
        let memtable: &memtable::MemTable = unsafe { &*self.memtable.load(Ordering::Acquire) };
        memtable.get(key)
    }

    fn submit_put(&self, job: WriteJob) -> Result<(), SendError<WriteJob>> {
        self.sender.send(job)
    }

    fn submit_delete(&self, job: WriteJob) -> Result<(), SendError<WriteJob>> {
        self.sender.send(job)
    }
}

impl StorageEngine {
    pub fn new(config: StorageEngineConf) -> Self {
        let mut memtable = memtable::MemTable::new();
        let memtable_ptr = Arc::new(AtomicPtr::new(&mut memtable));
        let wal = wal::Wal::from(0);
        let (tx, rx) = mpsc::channel();

        let mut worker = Worker {
            memtable: memtable_ptr.clone(),
            wal: wal,
            receiver: rx,
            timeout: config.timeout,
        };
        let handle = std::thread::spawn(move || worker.run());
        StorageEngine {
            memtable: memtable_ptr.clone(),
            sender: tx,
            join_handle: handle,
        }
    }

    pub fn shutdown(self) {
        drop(self.sender);
        self.join_handle.join().unwrap_or(());
    }
}

impl Worker {
    fn run(&mut self) {
        let mut last_sync = Instant::now();
        let mut elapsed = self.timeout;
        loop {
            match self.receiver.recv_timeout(elapsed) {
                Err(error) => match error {
                    RecvTimeoutError::Disconnected => break,
                    RecvTimeoutError::Timeout => {
                        self.wal.sync().unwrap();
                        last_sync = Instant::now();
                        elapsed = self.timeout;
                    }
                },
                Ok(job) => {
                    self.do_write(job);
                    elapsed -= Instant::now() - last_sync;
                    if elapsed.is_zero() {
                        self.wal.sync().unwrap();
                        last_sync = Instant::now();
                        elapsed = self.timeout;
                    }
                }
            }
        }
    }

    fn do_write(&mut self, job: WriteJob) {
        match job.data {
            WriteData::Delete(key) => job.sender.send(self.do_delete(key)).unwrap_or(()),
            WriteData::Put(data) => job.sender.send(self.do_put(data)).unwrap_or(()),
        }
    }

    fn do_delete(&mut self, key: String) -> Result<Option<String>, std::io::Error> {
        let entry = wal::WalEntry {
            operation_type: wal::OpType::Delete,
            key: key.clone(),
            value: String::new(),
            sequence_number: self.wal.last_sequence_number() + 1,
        };
        self.wal.append(&entry)?;
        let mut memtable = unsafe { (*self.memtable.load(Ordering::Relaxed)).clone() };
        match memtable.delete(&key) {
            Some(value) => {
                let _ = self.memtable.swap(&mut memtable, Ordering::Release);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn do_put(&mut self, data: PutData) -> Result<Option<String>, std::io::Error> {
        let entry = wal::WalEntry {
            operation_type: wal::OpType::Put,
            key: data.key.clone(),
            value: data.value.clone(),
            sequence_number: self.wal.last_sequence_number() + 1,
        };
        self.wal.append(&entry)?;
        let mut memtable = unsafe { (*self.memtable.load(Ordering::Relaxed)).clone() };
        match memtable.put(data.key, data.value) {
            Some(value) => {
                let _ = self.memtable.swap(&mut memtable, Ordering::Release);
                Ok(Some(value))
            }
            None => {
                let _ = self.memtable.swap(&mut memtable, Ordering::Release);
                Ok(None)
            }
        }
    }
}
