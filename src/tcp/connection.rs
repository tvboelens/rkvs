use super::protocol::{RequestType, TcpError, TcpRequest, TcpResponse, recv_tcp_request};
use crate::storage_engine::{StorageEngineError, Store};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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

pub struct ConnectionHandle {
    pub task_handle: JoinHandle<()>,
    pub cancellation_token: CancellationToken,
}

pub struct ConnectionHandleData {
    pub conn_handle: ConnectionHandle,
    pub id: u64,
}

pub enum ConnectionData {
    StartSignal(ConnectionHandleData),
    CompletionSignal(u64),
}
pub struct TcpConnection<T, U, V>
where
    T: AsyncReadExt + Unpin,
    U: AsyncWriteExt + Unpin,
    V: Store,
{
    id: u64,
    sender: Sender<ConnectionData>,
    reader: T,
    writer: U,
    storage_engine: Arc<V>,
    cancellation_token: CancellationToken,
}

impl<T, U, V> TcpConnection<T, U, V>
where
    T: AsyncReadExt + Unpin,
    U: AsyncWriteExt + Unpin,
    V: Store,
{
    pub fn new(
        id: u64,
        sender: Sender<ConnectionData>,
        reader: T,
        writer: U,
        storage_engine: Arc<V>,
        token: CancellationToken,
    ) -> Self {
        TcpConnection {
            id: id,
            sender: sender,
            reader: reader,
            writer: writer,
            storage_engine: storage_engine,
            cancellation_token: token,
        }
    }

    async fn handle_tcp_request(
        &mut self,
        tcp_request: TcpRequest,
    ) -> Result<Option<String>, ServerError> {
        let op = Operation::from(tcp_request);
        Ok(self.call_storage_engine(op).await?)
    }

    async fn call_storage_engine(
        &mut self,
        op: Operation,
    ) -> Result<Option<String>, StorageEngineError> {
        match op {
            Operation::Delete(key) => self.storage_engine.delete(&key).await,
            Operation::Get(key) => match self.storage_engine.get(&key).await {
                Ok(res) => match res {
                    Some(value) => Ok(Some(value)),
                    None => Err(StorageEngineError::NotFound),
                },
                Err(e) => Err(e),
            },
            Operation::Put(data) => self.storage_engine.put(&data.key, &data.value).await,
        }
    }

    async fn send_response(&mut self, value: Option<String>, correlation_id: &Uuid) {
        let bytes = TcpResponse::from(correlation_id, value).to_bytes();
        self.writer.write_all(&bytes).await.unwrap_or(())
    }

    async fn send_error_response(&mut self, error: &ServerError, correlation_id: &Uuid) {
        let bytes = TcpResponse::from_error(correlation_id, error).to_bytes();
        self.writer.write_all(&bytes).await.unwrap_or(())
    }

    pub async fn start(&mut self)
    where
        T: AsyncReadExt + Unpin,
        U: AsyncWriteExt + Unpin,
        V: Store,
    {
        while !self.cancellation_token.is_cancelled() {
            match recv_tcp_request(&mut self.reader).await {
                Ok(request) => {
                    println!("Received TcpRequest, handling...");
                    let correlation_id = request.headers.correlation_id;
                    match self.handle_tcp_request(request).await {
                        Ok(res) => self.send_response(res, &correlation_id).await,
                        Err(e) => self.send_error_response(&e, &correlation_id).await,
                    }
                }
                Err(e) => {
                    print!("TcpError!");
                    match e {
                        TcpError::IoError(_) | TcpError::WrongMagicBytes => break,
                        TcpError::InvalidKey(id)
                        | TcpError::InvalidRequestType(id)
                        | TcpError::InvalidValue(id)
                        | TcpError::MalformedPayload(id)
                        | TcpError::MissingValue(id)
                        | TcpError::UnknownFlags(id)
                        | TcpError::UnsupportedVersion(id) => {
                            let error = ServerError::TcpError(e);
                            self.send_error_response(&error, &id).await
                        }
                    }
                } // Log error?
            }
        }
        let _ = self.sender.send(ConnectionData::CompletionSignal(self.id));
    }
}

impl From<StorageEngineError> for ServerError {
    fn from(value: StorageEngineError) -> Self {
        Self::StorageEngine(value)
    }
}

impl From<TcpError> for ServerError {
    fn from(value: TcpError) -> Self {
        Self::TcpError(value)
    }
}

impl From<TcpRequest> for Operation {
    fn from(request: TcpRequest) -> Self {
        match request.headers.request_type {
            RequestType::Delete => Self::Delete(request.payload.key),
            RequestType::Get => Self::Get(request.payload.key),
            RequestType::Put => Self::Put(PutData {
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
    use super::TcpConnection;
    use crate::storage_engine::{StorageEngineError, Store};
    use crate::tcp::protocol::{Payload, RequestType, TcpHeaders, TcpRequest, TcpResponse};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use tokio::sync::oneshot;
    use tokio_test::io::Builder;
    use tokio_util::sync::CancellationToken;
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
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut connection = TcpConnection::new(1, tx, reader, writer, storage_engine_ptr, token);
        connection.start().await;
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
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut put_connection = TcpConnection::new(
            1,
            tx.clone(),
            put_reader,
            put_writer,
            storage_engine_ptr.clone(),
            token.clone(),
        );
        put_connection.start().await;
        let get_reader = Builder::new().read(&get_request.to_bytes()).build();
        let get_writer = Builder::new().write(&get_response.to_bytes()).build();
        let mut get_connection = TcpConnection::new(
            1,
            tx.clone(),
            get_reader,
            get_writer,
            storage_engine_ptr.clone(),
            token.clone(),
        );
        get_connection.start().await;
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
