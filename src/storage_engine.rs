use std::io;
use std::sync::mpsc;
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::time::{Duration, Instant};
use tokio::sync::oneshot::{Sender, channel};

pub mod memtable;
pub mod wal;

pub trait Store {
    fn get(
        &self,
        key: &String,
    ) -> impl std::future::Future<Output = Result<Option<String>, StorageEngineError>> + Send;
    fn put(
        &self,
        key: &String,
        value: &String,
    ) -> impl std::future::Future<Output = Result<Option<String>, StorageEngineError>> + Send;
    fn delete(
        &self,
        key: &String,
    ) -> impl std::future::Future<Output = Result<Option<String>, StorageEngineError>> + Send;
}

enum Command {
    Delete(String),
    Get(String),
    Put(String, String),
}

struct Job {
    pub command: Command,
    pub sender: Sender<io::Result<Option<String>>>,
}

pub struct StorageEngine {
    //memtable: Arc<AtomicPtr<memtable::MemTable>>,
    sender: mpsc::Sender<Job>,
    join_handle: std::thread::JoinHandle<()>,
}

struct Worker {
    memtable: memtable::MemTable,
    wal: wal::Wal,
    receiver: mpsc::Receiver<Job>,
    timeout: Duration,
}

pub struct StorageEngineConf {
    timeout: Duration,
}

#[derive(Debug)]
pub enum StorageEngineError {
    IoError,
    NotFound,
    Shutdown,
}

impl Store for StorageEngine {
    async fn get(&self, key: &String) -> Result<Option<String>, StorageEngineError> {
        let (tx, rx) = channel();
        let cmd = Command::Get(key.clone());
        let job = Job {
            command: cmd,
            sender: tx,
        };
        self.sender.send(job)?;
        match rx.await {
            Ok(res) => res.map_err(|_| StorageEngineError::IoError),
            Err(_) => Err(StorageEngineError::Shutdown),
        }
    }

    async fn put(
        &self,
        key: &String,
        value: &String,
    ) -> Result<Option<String>, StorageEngineError> {
        let (tx, rx) = channel();
        let cmd = Command::Put(key.clone(), value.clone());
        let job = Job {
            command: cmd,
            sender: tx,
        };
        self.sender.send(job)?;
        match rx.await {
            Ok(res) => res.map_err(|_| StorageEngineError::IoError),
            Err(_) => Err(StorageEngineError::Shutdown),
        }
    }

    async fn delete(&self, key: &String) -> Result<Option<String>, StorageEngineError> {
        let (tx, rx) = channel();
        let cmd = Command::Delete(key.clone());
        let job = Job {
            command: cmd,
            sender: tx,
        };
        self.sender.send(job)?;
        match rx.await {
            Ok(res) => res.map_err(|_| StorageEngineError::IoError),
            Err(_) => Err(StorageEngineError::Shutdown),
        }
    }
}

impl StorageEngine {
    pub fn new(config: StorageEngineConf) -> Self {
        let memtable = memtable::MemTable::new();
        let wal = wal::Wal::from(0);
        let (tx, rx) = mpsc::channel();

        let mut worker = Worker {
            memtable: memtable,
            wal: wal,
            receiver: rx,
            timeout: config.timeout,
        };
        let handle = std::thread::spawn(move || worker.run());
        StorageEngine {
            //memtable: memtable_ptr.clone(),
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
                    let res = match job.command {
                        Command::Delete(key) => self.do_delete(key),
                        Command::Get(key) => Ok(self.do_get(&key)),
                        Command::Put(key, value) => self.do_put(&key, &value),
                    };
                    elapsed -= Instant::now() - last_sync;
                    if elapsed.is_zero() {
                        self.wal.sync().unwrap();
                        last_sync = Instant::now();
                        elapsed = self.timeout;
                    }
                    job.sender.send(res).unwrap_or(())
                }
            }
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
        Ok(self.memtable.delete(&key))
    }

    fn do_put(&mut self, key: &String, value: &String) -> Result<Option<String>, std::io::Error> {
        let entry = wal::WalEntry {
            operation_type: wal::OpType::Put,
            key: key.clone(),
            value: value.clone(),
            sequence_number: self.wal.last_sequence_number() + 1,
        };
        self.wal.append(&entry)?;
        Ok(self.memtable.put(key.clone(), value.clone()))
    }

    fn do_get(&self, key: &String) -> Option<String> {
        self.memtable.get(key)
    }
}

impl From<SendError<Job>> for StorageEngineError {
    fn from(_: SendError<Job>) -> Self {
        StorageEngineError::Shutdown
    }
}
