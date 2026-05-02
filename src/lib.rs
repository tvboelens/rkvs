pub mod storage_engine;

use std::io;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::{Duration, sleep};

use crate::storage_engine::{StorageEngine, WriteData, WriteJob};

pub struct Server {
    storage_engine: Arc<StorageEngine>,
}

struct PutData {
    key: String,
    value: String,
}
enum Request {
    Get(String),
    Put(PutData),
    Delete(String),
}

#[derive(Debug)]
enum StorageEngineError {
    IoError,
    NotFound,
    Shutdown,
}

fn read_headers() {}

fn parse_tcp_request(bytes: &Vec<u8>) -> Request {
    Request::Get(String::from("value"))
}

async fn call_storage_engine(
    sender: Sender<Result<String, StorageEngineError>>,
    request: Request,
    storage_engine: Arc<StorageEngine>,
) {
    match request {
        Request::Delete(key) => {
            let (tx, rx) = oneshot::channel();
            let job = WriteJob {
                data: WriteData::Delete(key),
                sender: tx,
            };
            match storage_engine.submit_delete(job) {
                Ok(_) => match rx.await.unwrap() {
                    Ok(res) => match res {
                        Some(value) => sender.send(Ok(value)).unwrap(),
                        None => sender.send(Ok(String::new())).unwrap(), // Do I really want to do this? Maybe return option instead
                    },
                    Err(_) => sender.send(Err(StorageEngineError::IoError)).unwrap(),
                },
                Err(_) => sender.send(Err(StorageEngineError::Shutdown)).unwrap(),
            }
        }
        Request::Get(key) => match storage_engine.get(&key) {
            Some(value) => sender.send(Ok(value)).unwrap(),
            None => sender.send(Err(StorageEngineError::NotFound)).unwrap(),
        },
        Request::Put(data) => {
            let (tx, rx) = oneshot::channel();
            let job = WriteJob {
                data: WriteData::Put(storage_engine::PutData {
                    key: data.key,
                    value: data.value,
                }),
                sender: tx,
            };
            match storage_engine.submit_put(job) {
                Ok(_) => match rx.await.unwrap() {
                    Ok(res) => match res {
                        Some(value) => sender.send(Ok(value)).unwrap(),
                        None => sender.send(Ok(String::new())).unwrap(),
                    },
                    Err(_) => sender.send(Err(StorageEngineError::IoError)).unwrap(),
                },
                Err(_) => sender.send(Err(StorageEngineError::Shutdown)).unwrap(),
            }
        }
    }
}

async fn send_response_with_value(socket: &mut TcpStream, value: &String) -> io::Result<()> {
    let bytes = Vec::<u8>::new();
    socket.write_all(&bytes).await
}

async fn send_error_response(socket: &mut TcpStream, error: &StorageEngineError) -> io::Result<()> {
    let bytes = Vec::<u8>::new();
    socket.write_all(&bytes).await
}

async fn process_socket(mut socket: TcpStream, storage_engine_ptr: Arc<StorageEngine>) {
    let (tx, rx) = oneshot::channel();
    let bytes = Vec::new();
    let request = parse_tcp_request(&bytes);
    std::thread::spawn(move || {
        call_storage_engine(tx, request, storage_engine_ptr);
    });
    match rx.await.unwrap() {
        Ok(value) => {
            while let Err(_) = send_response_with_value(&mut socket, &value).await {
                sleep(Duration::from_millis(50)); // TODO: limit retries and log failure
            }
        }
        Err(error) => {
            while let Err(_) = send_error_response(&mut socket, &error).await {
                sleep(Duration::from_millis(50)); // TODO: limit retries and log failure
            }
        }
    }

    /*
       So what exactly do I want to send back:
           1. Get: Value if found, error if not found
           2. Put: Success or error
           3. Delete: success or error
           4. In the latter two cases possibly the previous value, but the error is io error
           5. And maybe also have shutdown
    */
}

fn parse_request(buf: &Vec<u8>) -> io::Result {}
impl Server {
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let storage_engine_ptr = self.storage_engine.clone();
            tokio::spawn(async move { process_socket(socket, storage_engine_ptr).await });
        }
        Ok(())
    }
}
