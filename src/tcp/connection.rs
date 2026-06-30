use super::protocol::{
    ConnectionError, ParseError, RequestType, TcpError, TcpRequest, TcpResponse, recv_tcp_request,
};
use crate::storage_engine::{StorageEngineError, Store};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
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

enum KeepAliveStatus {
    KeepAlive,
    CloseNotify,
    CloseSilent,
}

/*
TODO
1. Timeout for receiving headers and payload
2. Can we expect the yield_now calls etc to reliably work in the tests?
*/

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
    do_cancel: CancellationToken,
}

enum CloseMode {
    Notify,
    Silent,
}

async fn recv_header_len<T>(reader: &mut T, connection_timeout: Duration) -> Result<u32, CloseMode>
where
    T: AsyncReadExt + Unpin,
{
    match timeout(connection_timeout, reader.read_u32()).await {
        Ok(res) => res.map_err(|_| CloseMode::Silent),
        Err(_) => Err(CloseMode::Notify),
    }
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
            do_cancel: token,
        }
    }

    async fn handle_tcp_request(
        &mut self,
        tcp_request: TcpRequest,
    ) -> Result<Option<String>, StorageEngineError> {
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
            Operation::Put(data) => self.storage_engine.put(&data.key, data.value).await,
        }
    }

    async fn send_response(&mut self, value: Option<String>, correlation_id: &Uuid) {
        let bytes = TcpResponse::from(correlation_id, value).to_bytes();
        self.writer.write_all(&bytes).await.unwrap_or(())
    }

    async fn send_internal_error_response(
        &mut self,
        error: &StorageEngineError,
        correlation_id: &Uuid,
    ) -> io::Result<()> {
        let bytes = TcpResponse::from_internal_error(correlation_id, error).to_bytes();
        self.writer.write_all(&bytes).await
    }

    async fn send_tcp_error_response(&mut self, error: ParseError) -> io::Result<()> {
        let bytes = TcpResponse::from_tcp_parse_error(error).to_bytes();
        self.writer.write_all(&bytes).await
    }

    async fn send_close_notification(&mut self) {
        let bytes = TcpResponse::create_close_notification().to_bytes();
        self.writer.write_all(&bytes).await.unwrap_or(())
    }

    async fn recv_and_handle_tcp_request(&mut self, header_len: u32) -> KeepAliveStatus {
        match recv_tcp_request(&mut self.reader, header_len).await {
            Ok(request) => {
                let correlation_id = request.headers.correlation_id.clone();
                match self.handle_tcp_request(request).await {
                    Ok(res) => {
                        self.send_response(res, &correlation_id).await;
                        KeepAliveStatus::KeepAlive
                    }
                    Err(e) => match self.send_internal_error_response(&e, &correlation_id).await {
                        Ok(_) => KeepAliveStatus::KeepAlive,
                        Err(_) => KeepAliveStatus::CloseSilent,
                    },
                }
            }
            Err(e) => self.handle_tcp_error(e).await,
        }
    }

    async fn handle_tcp_error(&mut self, error: TcpError) -> KeepAliveStatus {
        match error {
            TcpError::Connection(e) => match e {
                ConnectionError::TimedOut => KeepAliveStatus::CloseNotify,
                ConnectionError::IoError(_) => KeepAliveStatus::CloseSilent,
                ConnectionError::WrongMagicBytes => KeepAliveStatus::CloseSilent,
            },
            TcpError::Parse(e) => match self.send_tcp_error_response(e).await {
                Ok(_) => KeepAliveStatus::KeepAlive,
                Err(_) => KeepAliveStatus::CloseSilent,
            },
        }
    }

    /*
    1. cancelled -> break loop
    2. recv header len
        1. timeout -> send notification and break loop
        2. tcp error -> break loop without notification
        3. received -> recv and handle
            1. recv
                1. tcp error when receiving request -> break loop without notification
                2. parsing error
                    1. send error response
                        1. succesful -> continue
                        2. unsuccessful -> break loop
            2. handle request
                1. ok -> continue
                2. error -> send error response
                    1. succesful -> continue
                    2. unsuccessful -> break loop
            3.

    so something of an enum with 3 values
    1. keep alive -> continue loop
    2. close without notification
    3. close with notification
    */

    pub async fn start(&mut self, conn_timeout: Duration)
    where
        T: AsyncReadExt + Unpin,
        U: AsyncWriteExt + Unpin,
        V: Store,
    {
        loop {
            let status = tokio::select! {
                 _ = self.do_cancel.cancelled() => {
                    KeepAliveStatus::CloseNotify},
                 res = recv_header_len(&mut self.reader, conn_timeout) =>  {
                    match res {
                        Ok(header_len) => {
                            self.recv_and_handle_tcp_request(header_len).await
                        },
                        Err(mode) => {
                            match mode {
                                CloseMode::Notify => KeepAliveStatus::CloseNotify,
                                CloseMode::Silent => KeepAliveStatus::CloseSilent
                            }
                        }
                    }
                 }
            };
            match status {
                KeepAliveStatus::CloseNotify => {
                    self.send_close_notification().await;
                    break;
                }
                KeepAliveStatus::CloseSilent => {
                    break;
                }
                KeepAliveStatus::KeepAlive => {
                    continue;
                }
            }
        }
        let _ = self.sender.send(ConnectionData::CompletionSignal(self.id));
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

#[cfg(test)]
mod tests {
    use super::TcpConnection;
    use crate::storage_engine::{StorageEngineError, Store};
    use crate::tcp::protocol::{Payload, RequestType, TcpHeaders, TcpRequest, TcpResponse};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
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
        let mut connection =
            TcpConnection::new(1, tx, reader, writer, storage_engine_ptr.clone(), token);
        connection.start(Duration::from_millis(10)).await;
        let res: Result<Option<String>, StorageEngineError> =
            storage_engine_ptr.get(&String::from("key")).await;
        assert!(res.is_ok());
        let value = res.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value");
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
        put_connection.start(Duration::from_millis(10)).await;
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
        get_connection.start(Duration::from_millis(10)).await;
        let res: Result<Option<String>, StorageEngineError> =
            storage_engine_ptr.get(&String::from("key")).await;
        assert!(res.is_ok());
        let value = res.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value");
    }

    #[tokio::test]
    async fn handle_put_get_request_reuse_conn() {
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
        let reader = Builder::new()
            .read(&put_request.to_bytes())
            .read(&get_request.to_bytes())
            .build();
        let writer = Builder::new()
            .write(&put_response.to_bytes())
            .write(&get_response.to_bytes())
            .build();
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut connection = TcpConnection::new(
            1,
            tx.clone(),
            reader,
            writer,
            storage_engine_ptr.clone(),
            token.clone(),
        );
        connection.start(Duration::from_millis(50)).await;
        let res: Result<Option<String>, StorageEngineError> =
            storage_engine_ptr.get(&String::from("key")).await;
        assert!(res.is_ok());
        let value = res.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value");
    }

    #[tokio::test]
    async fn connection_cancel_before_start() {
        let storage_engine = Arc::new(FakeStorageEngine::new());
        let put_res = storage_engine
            .put(&String::from("key"), String::from("value1"))
            .await;
        assert!(put_res.is_ok());
        let payload = Payload {
            key: String::from("key"),
            value: Some(String::from("value2")),
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

        let response = TcpResponse::create_close_notification();
        let (reader, mut reader_handle) = Builder::new()
            .wait(Duration::from_millis(10))
            .build_with_handle();
        let (writer, mut writer_handle) = Builder::new().build_with_handle();
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut connection = TcpConnection::new(
            1,
            tx.clone(),
            reader,
            writer,
            storage_engine.clone(),
            token.clone(),
        );
        token.cancel();
        let task_handle = tokio::spawn(async move {
            connection.start(Duration::from_millis(10)).await;
        });
        reader_handle.read(&request.to_bytes());
        writer_handle.write(&response.to_bytes());
        let task_res = task_handle.await;
        assert!(task_res.is_ok());
        let res = storage_engine.get(&String::from("key")).await;
        assert!(res.is_ok());
        let value = res.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value1");
    }

    #[tokio::test]
    async fn connection_cancel_after_start() {
        let storage_engine = Arc::new(FakeStorageEngine::new());
        let put_res = storage_engine
            .put(&String::from("key"), String::from("value1"))
            .await;
        assert!(put_res.is_ok());
        let payload = Payload {
            key: String::from("key"),
            value: Some(String::from("value2")),
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

        let response = TcpResponse::from(&Uuid::from_u128(1024), Some(String::from("value1")));
        let reader = Builder::new().read(&request.to_bytes()).build();
        let (writer, mut writer_handle) = Builder::new()
            .write(&response.to_bytes())
            .build_with_handle();
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut connection = TcpConnection::new(
            1,
            tx.clone(),
            reader,
            writer,
            storage_engine.clone(),
            token.clone(),
        );
        let task_handle = tokio::spawn(async move {
            connection.start(Duration::from_millis(50)).await;
        });

        tokio::task::yield_now().await;
        token.cancel();
        writer_handle.write(&TcpResponse::create_close_notification().to_bytes());
        let task_timout_res = timeout(Duration::from_millis(10), task_handle).await;
        assert!(task_timout_res.is_ok());
        let task_res = task_timout_res.unwrap();
        assert!(task_res.is_ok());
        let res = storage_engine.get(&String::from("key")).await;
        assert!(res.is_ok());
        let value = res.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), "value2");
    }

    #[tokio::test]
    async fn connection_timout_stops() {
        let storage_engine = Arc::new(FakeStorageEngine::new());

        let reader = Builder::new().wait(Duration::from_millis(500)).build();
        let writer = Builder::new()
            .write(&TcpResponse::create_close_notification().to_bytes())
            .build();
        let token = CancellationToken::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut connection = TcpConnection::new(
            1,
            tx.clone(),
            reader,
            writer,
            storage_engine.clone(),
            token.clone(),
        );
        let task_handle = tokio::spawn(async move {
            connection.start(Duration::from_millis(1)).await;
        });

        let task_timout_res = timeout(Duration::from_millis(50), task_handle).await;
        assert!(task_timout_res.is_ok());
        let task_res = task_timout_res.unwrap();
        assert!(task_res.is_ok());
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
            value: String,
        ) -> Result<Option<String>, StorageEngineError> {
            let (tx, rx) = oneshot::channel();
            let cmd = Command::Put(key.clone(), value);
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

/*
test cases:
1. Cancel really cancels
2. error responses are triggered when faulty request or incomplete request comes in
3. Writing requests should be atomic, i.e. either the write happens or the key is not updated -> so if connection is severed,
storage engine not modified
4. Can handle multiple requests -> OK
*/
