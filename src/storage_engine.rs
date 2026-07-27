use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::{RecvTimeoutError, SendError};
use std::time::{Duration, Instant};
use tokio::sync::oneshot::{Sender, channel};

use sstable::SsTable;

mod memtable;
mod sstable;
pub trait Store {
    fn get(
        &self,
        key: &String,
    ) -> impl std::future::Future<Output = Result<Option<String>, StorageEngineError>> + Send;
    fn put(
        &self,
        key: &String,
        value: String,
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
    sstable: sstable::SsTable,
    receiver: mpsc::Receiver<Job>,
    timeout: Duration,
}

pub struct StorageEngineConf {
    timeout: Duration,
    dir: PathBuf,
    segment_size: u32,
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
        rx.await
            .map_err(|_| StorageEngineError::Shutdown)
            .and_then(|res| res.map_err(|_| StorageEngineError::IoError))
    }

    async fn put(&self, key: &String, value: String) -> Result<Option<String>, StorageEngineError> {
        let (tx, rx) = channel();
        let cmd = Command::Put(key.clone(), value);
        let job = Job {
            command: cmd,
            sender: tx,
        };
        self.sender.send(job)?;
        rx.await
            .map_err(|_| StorageEngineError::Shutdown)
            .and_then(|res| res.map_err(|_| StorageEngineError::IoError))
    }

    async fn delete(&self, key: &String) -> Result<Option<String>, StorageEngineError> {
        let (tx, rx) = channel();
        let cmd = Command::Delete(key.clone());
        let job = Job {
            command: cmd,
            sender: tx,
        };
        self.sender.send(job)?;
        rx.await
            .map_err(|_| StorageEngineError::Shutdown)
            .and_then(|res| res.map_err(|_| StorageEngineError::IoError))
    }
}

impl StorageEngine {
    pub fn new(config: StorageEngineConf) -> io::Result<Self> {
        let memtable = memtable::MemTable::start(
            config.dir.join(PathBuf::from("WAL")),
            config.segment_size,
            0,
        )?;
        let (tx, rx) = mpsc::channel();
        let sstable = SsTable::start()?;
        let mut worker = Worker {
            memtable: memtable,
            sstable: sstable,
            receiver: rx,
            timeout: config.timeout,
        };
        let handle = std::thread::spawn(move || worker.run());
        Ok(StorageEngine {
            //memtable: memtable_ptr.clone(),
            sender: tx,
            join_handle: handle,
        })
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
                        self.memtable.sync().unwrap();
                        last_sync = Instant::now();
                        elapsed = self.timeout;
                    }
                },
                Ok(job) => {
                    let res = match job.command {
                        Command::Delete(key) => self.delete(&key),
                        Command::Get(key) => self.get(&key),
                        Command::Put(key, value) => self.put(key, value),
                    };
                    elapsed -= Instant::now() - last_sync;
                    if elapsed.is_zero() {
                        self.memtable.sync().unwrap();
                        last_sync = Instant::now();
                        elapsed = self.timeout;
                    }
                    job.sender.send(res).unwrap_or(())
                }
            }
        }
    }

    fn get(&mut self, key: &String) -> io::Result<Option<String>> {
        match self.memtable.get(key) {
            Some(mvalue) => {
                if mvalue.deleted {
                    Ok(None)
                } else {
                    Ok(mvalue.value)
                }
            }
            None => self.sstable.get(key).map(|opt| {
                opt.map(|v| {
                    if v.deleted {
                        return None;
                    } else {
                        return v.value;
                    }
                })
                .flatten()
            }),
        }
    }

    fn put(&mut self, key: String, value: String) -> io::Result<Option<String>> {
        self.memtable.put(key, value).map(|opt| {
            opt.map(|mvalue| {
                if mvalue.deleted {
                    return None;
                } else {
                    return mvalue.value;
                }
            })
            .flatten()
        })
    }

    fn delete(&mut self, key: &String) -> io::Result<Option<String>> {
        self.memtable.delete(key).map(|opt| {
            opt.map(|mvalue| {
                if mvalue.deleted {
                    return None;
                } else {
                    return mvalue.value;
                }
            })
            .flatten()
        })
    }
}

impl From<SendError<Job>> for StorageEngineError {
    fn from(_: SendError<Job>) -> Self {
        StorageEngineError::Shutdown
    }
}

impl StorageEngineError {
    pub fn to_rc(&self) -> u8 {
        match self {
            StorageEngineError::IoError => 1,
            StorageEngineError::NotFound => 2,
            StorageEngineError::Shutdown => 3,
        }
    }
}
