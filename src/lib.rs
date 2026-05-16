pub mod storage_engine;
pub mod tcp_protocol;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tcp_protocol::{TcpRequest, recv_tcp_request};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::storage_engine::{StorageEngine, WriteData, WriteJob};

static RESPONSE_RETRY_COUNT: u8 = 5;

pub struct Server {
    storage_engine: Arc<StorageEngine>,
    address: SocketAddr,
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
enum ServerError {
    StorageEngine(StorageEngineError),
    TcpError(tcp_protocol::TcpError),
}

#[derive(Debug)]
enum StorageEngineError {
    IoError,
    NotFound,
    Shutdown,
}

async fn handle_tcp_request(
    tcp_request: TcpRequest,
    storage_engine: Arc<StorageEngine>,
) -> Result<Option<String>, ServerError> {
    Ok(None)
}

async fn call_storage_engine(
    request: Request,
    storage_engine: Arc<StorageEngine>,
) -> Result<Option<String>, StorageEngineError> {
    match request {
        Request::Delete(key) => {
            let (tx, rx) = oneshot::channel();
            let job = WriteJob {
                data: WriteData::Delete(key),
                sender: tx,
            };
            match storage_engine.submit_delete(job) {
                Ok(_) => match rx.await {
                    Ok(res) => res.map_err(|_| StorageEngineError::IoError),
                    Err(_) => Err(StorageEngineError::Shutdown),
                },
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }
        Request::Get(key) => match storage_engine.get(&key) {
            Some(value) => Ok(Some(value)),
            None => Err(StorageEngineError::NotFound),
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
                Ok(_) => match rx.await {
                    Ok(res) => res.map_err(|_| StorageEngineError::IoError),
                    Err(_) => Err(StorageEngineError::Shutdown),
                },
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }
    }
}

async fn send_response(
    socket: &mut TcpStream,
    value: &Option<String>,
    correlation_id: &Uuid,
) -> io::Result<()> {
    let bytes = Vec::<u8>::new();
    socket.write_all(&bytes).await
}

async fn send_error_response(
    socket: &mut TcpStream,
    error: &ServerError,
    correlation_id: &Uuid,
) -> io::Result<()> {
    let bytes = Vec::<u8>::new();
    socket.write_all(&bytes).await
}

async fn process_socket(mut socket: TcpStream, storage_engine_ptr: Arc<StorageEngine>) {
    match recv_tcp_request(&mut socket).await {
        Ok(request) => {
            let correlation_id = request.headers.correlation_id;
            match handle_tcp_request(request, storage_engine_ptr).await {
                Ok(res) => {
                    let mut retry_count = 0;
                    while let Err(_) = send_response(&mut socket, &res, &correlation_id).await {
                        if retry_count > RESPONSE_RETRY_COUNT {
                            break;
                        }
                        retry_count += 1;
                        sleep(Duration::from_millis(50)); // TODO: log failure
                    }
                }
                Err(e) => {
                    let mut retry_count = 0;
                    while let Err(_) = send_error_response(&mut socket, &e, &correlation_id).await {
                        if retry_count > RESPONSE_RETRY_COUNT {
                            break;
                        }
                        retry_count += 1;
                        sleep(Duration::from_millis(50)); // TODO: log failure
                    }
                }
            }
        }
        Err(e) => {
            match e {
                tcp_protocol::TcpError::IoError(_) | tcp_protocol::TcpError::WrongMagicBytes => (),
                tcp_protocol::TcpError::InvalidKey(id)
                | tcp_protocol::TcpError::InvalidRequestType(id)
                | tcp_protocol::TcpError::InvalidValue(id)
                | tcp_protocol::TcpError::MalformedPayload(id)
                | tcp_protocol::TcpError::MissingValue(id)
                | tcp_protocol::TcpError::UnknownFlags(id)
                | tcp_protocol::TcpError::UnsupportedVersion(id) => {
                    let error = ServerError::TcpError(e);
                    let mut retry_count = 0;
                    while let Err(_) = send_error_response(&mut socket, &error, &id).await {
                        if retry_count > RESPONSE_RETRY_COUNT {
                            break;
                        }
                        retry_count += 1;
                        sleep(Duration::from_millis(50)); // TODO: log failure
                    }
                }
            }
        } // Log error?
    }
}

impl Server {
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(self.address).await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let storage_engine_ptr = self.storage_engine.clone();
            tokio::spawn(async move { process_socket(socket, storage_engine_ptr).await });
        }
        Ok(())
    }
}

impl From<StorageEngineError> for ServerError {
    fn from(value: StorageEngineError) -> Self {
        Self::StorageEngine(value)
    }
}

impl From<tcp_protocol::TcpError> for ServerError {
    fn from(value: tcp_protocol::TcpError) -> Self {
        Self::TcpError(value)
    }
}

impl From<tcp_protocol::TcpRequest> for Request {
    fn from(request: tcp_protocol::TcpRequest) -> Self {
        match request.headers.request_type {
            tcp_protocol::RequestType::Delete => Self::Delete(request.payload.key),
            tcp_protocol::RequestType::Get => Self::Get(request.payload.key),
            tcp_protocol::RequestType::Put => Self::Put(PutData {
                key: request.payload.key,
                value: request.payload.value.unwrap(),
            }),
        }
    }
}
