use std::sync::{Arc, RwLock, mpsc};
use tokio::sync::oneshot;

pub mod memtable;
pub mod wal;

pub enum WriteType {
    Put,
    Delete,
}
pub struct WriteData {
    op: WriteType,
    key: String,
    value: String,
}

pub struct WriteJob {
    data: WriteData,
    sender: oneshot::Sender<Result<(), String>>,
}

enum Command {
    Write(WriteJob),
}

pub struct StorageEngine {
    memtable: Arc<RwLock<memtable::MemTable>>,
    sender: mpsc::Sender<Command>,
    join_handle: std::thread::JoinHandle<()>,
}

struct Worker {
    memtable: Arc<RwLock<memtable::MemTable>>,
    wal: wal::Wal,
    receiver: mpsc::Receiver<Command>,
}

impl StorageEngine {
    pub fn new() -> Self {
        let memtable = Arc::new(RwLock::new(memtable::MemTable::new()));
        let wal = wal::Wal {};
        let (tx, rx) = mpsc::channel();

        let worker = Worker {
            memtable: memtable.clone(),
            wal: wal,
            receiver: rx,
        };
        let handle = std::thread::spawn(move || worker.run());
        StorageEngine {
            memtable: memtable,
            sender: tx,
            join_handle: handle,
        }
    }

    pub fn get(&self, key: &String) -> Option<String> {
        /* This will have to get more sophisticated once we move to on-disk persistence
           Also the unwrap should be replaced later by something that returns None
           so that the server can return a not found error or similar
        */
        let memtable = self.memtable.read().unwrap();
        memtable.get(key)
    }

    pub fn submit_put(&self, data: WriteData) -> Result<oneshot::Receiver<Result<(), String>>, ()> {
        let (tx, rx) = oneshot::channel();
        let job = WriteJob {
            data: data,
            sender: tx,
        };
        match self.sender.send(Command::Write(job)) {
            Ok(_) => Ok(rx),
            Err(_) => Err(()),
        }
    }

    pub fn submit_delete(
        &self,
        data: WriteData,
    ) -> Result<oneshot::Receiver<Result<(), String>>, ()> {
        let (tx, rx) = oneshot::channel();
        let job = WriteJob {
            data: data,
            sender: tx,
        };
        match self.sender.send(Command::Write(job)) {
            Ok(_) => Ok(rx),
            Err(_) => Err(()),
        }
    }

    pub fn shutdown(self) {
        drop(self.sender);
        let _ = self.join_handle.join();
    }
}

impl Worker {
    fn run(&mut self) {
        while let Ok(cmd) = self.receiver.recv() {
            // TODO: periodic sync for WAL
            match cmd {
                Command::Write(job) => {
                    let mut memtable = self.memtable.write().unwrap();
                    match job.data.op {
                        WriteType::Put => {
                            let entry = wal::WalEntry {
                                operation_type: wal::OpType::Put,
                                key: job.data.key,
                                value: job.data.value,
                                sequence_number: self.wal.last_sequence_number() + 1,
                            };
                            match self.wal.append(&entry) {
                                Ok(_) => {
                                    memtable.put(entry.key, entry.value);
                                    job.sender.send(Ok(())).unwrap()
                                }
                                Err(e) => job.sender.send(Err(e.to_string())).unwrap(),
                            }
                        }
                        WriteType::Delete => {
                            let entry = wal::WalEntry {
                                operation_type: wal::OpType::Put,
                                key: job.data.key,
                                value: String::from(""),
                                sequence_number: self.wal.last_sequence_number() + 1,
                            };
                            match self.wal.append(&entry) {
                                Ok(_) => {
                                    memtable.delete(&entry.key);
                                    job.sender.send(Ok(())).unwrap()
                                }
                                Err(e) => job.sender.send(Err(e.to_string())).unwrap(),
                            }
                        }
                    }
                }
            }
        }
        self.wal.sync();
    }
}
