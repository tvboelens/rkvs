pub mod storage_engine;
pub mod tcp_protocol;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tcp_protocol::{TcpError, TcpRequest, TcpResponse, recv_tcp_request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::storage_engine::{StorageEngine, StorageEngineError, Store};

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
        Operation::Delete(key) => storage_engine.delete(&key).await,
        Operation::Get(key) => match storage_engine.get(&key).await {
            Ok(res) => match res {
                Some(value) => Ok(Some(value)),
                None => Err(StorageEngineError::NotFound),
            },
            Err(e) => Err(e),
        },
        Operation::Put(data) => storage_engine.put(&data.key, &data.value).await,
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
            println!("Received TcpRequest, handling...");
            let correlation_id = request.headers.correlation_id;
            match handle_tcp_request(request, storage_engine_ptr).await {
                Ok(res) => send_response(&mut writer, res, &correlation_id).await,
                Err(e) => send_error_response(&mut writer, &e, &correlation_id).await,
            }
        }
        Err(e) => {
            print!("TcpError!");
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
                    send_error_response(&mut writer, &error, &id).await
                }
            }
        } // Log error?
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

#[cfg(test)]
mod tests {
    use crate::storage_engine::{StorageEngineError, Store};
    use crate::tcp_protocol::{Payload, RequestType, TcpHeaders, TcpRequest, TcpResponse};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use tokio::sync::oneshot;
    use tokio_test::io::Builder;
    use uuid::Uuid;

    #[tokio::test]
    async fn handle_put_request() {
        let storage_engine = FakeStorageEngine::new();
        let storage_engine_ptr = Arc::new(storage_engine);
        let payload = Payload {
            key: String::from("key"),
            value: Some(String::from("value")),
        };
        let headers = TcpHeaders {
            correlation_id: Uuid::from_u128(1024),
            request_type: RequestType::Put,
            protocol_version: 0,
            flags: 0,
        };
        let request = TcpRequest {
            headers: headers,
            payload: payload,
        };
        let response = TcpResponse::from(&Uuid::from_u128(1024), None);
        let reader = Builder::new().read(&request.to_bytes()).build();
        let writer = Builder::new().write(&response.to_bytes()).build();
        let _ = super::process_socket(reader, writer, storage_engine_ptr).await;
    }

    #[tokio::test]
    async fn handle_put_get_request() {
        let storage_engine = FakeStorageEngine::new();
        let storage_engine_ptr = Arc::new(storage_engine);
        let put_payload = Payload {
            key: String::from("key"),
            value: Some(String::from("value")),
        };
        let put_headers = TcpHeaders {
            correlation_id: Uuid::from_u128(1024),
            request_type: RequestType::Put,
            protocol_version: 0,
            flags: 0,
        };
        let put_request = TcpRequest {
            headers: put_headers,
            payload: put_payload,
        };

        let get_payload = Payload {
            key: String::from("key"),
            value: None,
        };
        let get_headers = TcpHeaders {
            correlation_id: Uuid::from_u128(512),
            request_type: RequestType::Get,
            protocol_version: 0,
            flags: 0,
        };
        let get_request = TcpRequest {
            headers: get_headers,
            payload: get_payload,
        };
        let put_response = TcpResponse::from(&Uuid::from_u128(1024), None);
        let get_response = TcpResponse::from(&Uuid::from_u128(512), Some(String::from("value")));
        let put_reader = Builder::new().read(&put_request.to_bytes()).build();
        let put_writer = Builder::new().write(&put_response.to_bytes()).build();
        let _ = super::process_socket(put_reader, put_writer, storage_engine_ptr.clone()).await;
        let get_reader = Builder::new().read(&get_request.to_bytes()).build();
        let get_writer = Builder::new().write(&get_response.to_bytes()).build();
        let _ = super::process_socket(get_reader, get_writer, storage_engine_ptr).await;
    }

    enum Command {
        Delete(String),
        Get(String),
        Put(String, String),
    }

    struct Job {
        cmd: Command,
        sender: oneshot::Sender<Option<String>>,
    }

    struct FakeStorageEngine {
        sender: Sender<Job>,
    }

    struct FakeWorker {
        receiver: Receiver<Job>,
        map: HashMap<String, String>,
    }

    impl FakeStorageEngine {
        fn new() -> Self {
            let (tx, rx) = channel();
            let mut worker = FakeWorker::new(rx);
            std::thread::spawn(move || worker.run());
            FakeStorageEngine { sender: tx }
        }
    }

    impl FakeWorker {
        pub fn new(rx: Receiver<Job>) -> Self {
            let map = HashMap::new();
            FakeWorker {
                receiver: rx,
                map: map,
            }
        }
        pub fn run(&mut self) {
            while let Ok(job) = self.receiver.recv() {
                let res = match job.cmd {
                    Command::Delete(key) => self.map.remove(&key),
                    Command::Get(key) => self.map.get(&key).cloned(),
                    Command::Put(key, value) => self.map.insert(key, value),
                };
                job.sender.send(res).unwrap_or(())
            }
        }
    }

    impl Store for FakeStorageEngine {
        async fn get(&self, key: &String) -> Result<Option<String>, StorageEngineError> {
            let (tx, rx) = oneshot::channel();
            let cmd = Command::Get(key.clone());
            let job = Job {
                cmd: cmd,
                sender: tx,
            };
            match self.sender.send(job) {
                Ok(_) => rx.await.map_err(|_| StorageEngineError::Shutdown),
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }

        async fn delete(&self, key: &String) -> Result<Option<String>, StorageEngineError> {
            let (tx, rx) = oneshot::channel();
            let cmd = Command::Delete(key.clone());
            let job = Job {
                cmd: cmd,
                sender: tx,
            };
            match self.sender.send(job) {
                Ok(_) => rx.await.map_err(|_| StorageEngineError::Shutdown),
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }

        async fn put(
            &self,
            key: &String,
            value: &String,
        ) -> Result<Option<String>, StorageEngineError> {
            let (tx, rx) = oneshot::channel();
            let cmd = Command::Put(key.clone(), value.clone());
            let job = Job {
                cmd: cmd,
                sender: tx,
            };
            match self.sender.send(job) {
                Ok(_) => rx.await.map_err(|_| StorageEngineError::Shutdown),
                Err(_) => Err(StorageEngineError::Shutdown),
            }
        }
    }
}
