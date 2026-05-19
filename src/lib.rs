pub mod storage_engine;
pub mod tcp_protocol;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tcp_protocol::{TcpError, TcpRequest, TcpResponse, recv_tcp_request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::storage_engine::{StorageEngine, Store, WriteData, WriteJob};

pub struct Server {
    storage_engine: Arc<StorageEngine>,
    address: SocketAddr,
}

struct PutData {
    key: String,
    value: String,
}
enum Operation {
    Get(String),
    Put(PutData),
    Delete(String),
}

#[derive(Debug)]
pub enum ServerError {
    StorageEngine(StorageEngineError),
    TcpError(TcpError),
}

#[derive(Debug)]
pub enum StorageEngineError {
    IoError,
    NotFound,
    Shutdown,
}

async fn handle_tcp_request<T>(
    tcp_request: TcpRequest,
    storage_engine: Arc<T>,
) -> Result<Option<String>, ServerError>
where
    T: Store,
{
    let op = Operation::from(tcp_request);
    Ok(call_storage_engine(op, storage_engine).await?)
}

async fn call_storage_engine<T>(
    op: Operation,
    storage_engine: Arc<T>,
) -> Result<Option<String>, StorageEngineError>
where
    T: Store,
{
    match op {
        Operation::Delete(key) => {
            let (tx, rx) = oneshot::channel();
            let job = WriteJob {
                data: WriteData::Delete(key),
                sender: tx,
            };
            storage_engine
                .submit_delete(job)
                .map_err(|_| StorageEngineError::Shutdown)?;
            match rx.await {
                Ok(res) => res.map_err(|_| StorageEngineError::IoError),
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }
        Operation::Get(key) => match storage_engine.get(&key) {
            Some(value) => Ok(Some(value)),
            None => Err(StorageEngineError::NotFound),
        },
        Operation::Put(data) => {
            let (tx, rx) = oneshot::channel();
            let job = WriteJob {
                data: WriteData::Put(storage_engine::PutData {
                    key: data.key,
                    value: data.value,
                }),
                sender: tx,
            };
            storage_engine
                .submit_put(job)
                .map_err(|_| StorageEngineError::Shutdown)?;
            match rx.await {
                Ok(res) => res.map_err(|_| StorageEngineError::IoError),
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }
    }
}

async fn send_response<T>(writer: &mut T, value: Option<String>, correlation_id: &Uuid)
where
    T: AsyncWriteExt + Unpin,
{
    let bytes = TcpResponse::from(correlation_id, value).to_bytes();
    writer.write_all(&bytes).await.unwrap_or(())
}

async fn send_error_response<T>(writer: &mut T, error: &ServerError, correlation_id: &Uuid)
where
    T: AsyncWriteExt + Unpin,
{
    let bytes = TcpResponse::from_error(correlation_id, error).to_bytes();
    writer.write_all(&bytes).await.unwrap_or(())
}

async fn process_socket<T, U, V>(mut reader: T, mut writer: U, storage_engine_ptr: Arc<V>)
where
    T: AsyncReadExt + Unpin,
    U: AsyncWriteExt + Unpin,
    V: Store,
{
    match recv_tcp_request(&mut reader).await {
        Ok(request) => {
            let correlation_id = request.headers.correlation_id;
            match handle_tcp_request(request, storage_engine_ptr).await {
                Ok(res) => send_response(&mut writer, res, &correlation_id).await,
                Err(e) => send_error_response(&mut writer, &e, &correlation_id).await,
            }
        }
        Err(e) => match e {
            tcp_protocol::TcpError::IoError(_) | tcp_protocol::TcpError::WrongMagicBytes => (),
            tcp_protocol::TcpError::InvalidKey(id)
            | tcp_protocol::TcpError::InvalidRequestType(id)
            | tcp_protocol::TcpError::InvalidValue(id)
            | tcp_protocol::TcpError::MalformedPayload(id)
            | tcp_protocol::TcpError::MissingValue(id)
            | tcp_protocol::TcpError::UnknownFlags(id)
            | tcp_protocol::TcpError::UnsupportedVersion(id) => {
                let error = ServerError::TcpError(e);
                send_error_response(&mut writer, &error, &id).await
            }
        }, // Log error?
    }
}

impl Server {
    pub async fn run(&self) -> io::Result<()> {
        let listener: TcpListener = TcpListener::bind(self.address).await?;
        loop {
            let (mut socket, _) = listener.accept().await?;
            let storage_engine_ptr = self.storage_engine.clone();
            tokio::spawn(async move {
                let (reader, writer) = socket.split();
                process_socket(reader, writer, storage_engine_ptr).await
            });
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

impl From<TcpRequest> for Operation {
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

impl ServerError {
    pub fn to_rc(&self) -> u8 {
        match self {
            Self::StorageEngine(e) => match e {
                StorageEngineError::IoError => 1,
                StorageEngineError::NotFound => 2,
                StorageEngineError::Shutdown => 3,
            },
            Self::TcpError(e) => match e {
                TcpError::InvalidKey(_) => 4,
                TcpError::InvalidValue(_) => 5,
                TcpError::MissingValue(_) => 6,
                TcpError::MalformedPayload(_) => 7,
                TcpError::InvalidRequestType(_) => 8,
                TcpError::UnknownFlags(_) => 9,
                TcpError::UnsupportedVersion(_) => 10,
                _ => 255,
            },
        }
    }
}
