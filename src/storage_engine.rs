use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc::SendError;
use std::sync::{Arc, Mutex, mpsc};
use tokio::sync::oneshot::{Sender, channel};

use memtable_flusher::Flusher;
use sstable::SsTable;

mod memtable;
mod memtable_flusher;
mod sstable;
pub trait Store {
    fn get(
        &self,
        key: &String,
    ) -> impl Future<Output = Result<Option<String>, StorageEngineError>> + Send;
    fn put(
        &self,
        key: &String,
        value: String,
    ) -> impl Future<Output = Result<Option<String>, StorageEngineError>> + Send;
    fn delete(
        &self,
        key: &String,
    ) -> impl Future<Output = Result<Option<String>, StorageEngineError>> + Send;
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
    join_handles: Vec<std::thread::JoinHandle<()>>,
}

struct Worker {
    memtable: Arc<memtable::MemTable>,
    sstable: Arc<sstable::SsTable>,
    receiver: Arc<Mutex<mpsc::Receiver<Job>>>,
}

pub struct StorageEngineConf {
    //timeout: Duration,
    dir: PathBuf,
    segment_size: u32,
    memtable_max_size: u64,
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
        let sstable = SsTable::start()?; // TODO: sstable also needs to know about the dir
        let sstable_ptr = Arc::new(sstable);
        let flusher = Flusher::from(sstable_ptr.clone(), config.dir.clone());
        let memtable = memtable::MemTable::start(
            config.dir,
            config.segment_size,
            0,
            config.memtable_max_size,
            flusher,
        )?;
        let (tx, rx) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(rx));
        let worker = Worker {
            memtable: Arc::new(memtable),
            sstable: sstable_ptr,
            receiver: receiver,
        };
        let mut join_handles = Vec::new();
        for _ in 0..6 {
            let new_worker = worker.clone();
            let handle = std::thread::spawn(move || new_worker.run());
            join_handles.push(handle);
        }
        Ok(StorageEngine {
            sender: tx,
            join_handles: join_handles,
        })
    }

    pub fn shutdown(self) {
        drop(self.sender);
        for handle in self.join_handles {
            // TODO: handle panics?
            handle.join().unwrap_or(());
        }
    }
}

impl Worker {
    fn run(&self) {
        let mut job;
        loop {
            {
                match self.receiver.lock() {
                    Ok(receiver) => match receiver.recv() {
                        Ok(j) => job = j,
                        Err(_) => {
                            break;
                        }
                    },
                    Err(_) => {
                        // TODO: handle poison error
                        break;
                    }
                }
            }
            let res = match job.command {
                Command::Delete(key) => self.delete(&key),
                Command::Get(key) => self.get(&key),
                Command::Put(key, value) => self.put(key, value),
            };
            job.sender.send(res).unwrap_or(())
        }
    }

    fn get(&self, key: &String) -> io::Result<Option<String>> {
        match self.memtable.get(key) {
            Some(mvalue) => Ok(mvalue.value),
            None => self
                .sstable
                .get(key)
                .map(|opt| opt.map(|entry| entry.value).flatten()),
        }
    }

    fn put(&self, key: String, value: String) -> io::Result<Option<String>> {
        self.memtable
            .put(key, value)
            .map(|opt| opt.map(|mvalue| mvalue.value).flatten())
    }

    fn delete(&self, key: &String) -> io::Result<Option<String>> {
        self.memtable
            .delete(key)
            .map(|opt| opt.map(|mvalue| mvalue.value).flatten())
    }
}

impl Clone for Worker {
    fn clone(&self) -> Self {
        Worker {
            memtable: self.memtable.clone(),
            sstable: self.sstable.clone(),
            receiver: self.receiver.clone(),
        }
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
