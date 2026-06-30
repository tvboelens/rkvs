pub mod storage_engine;
pub mod tcp;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tcp::connection::{ConnectionData, ConnectionHandle, TcpConnection};
use tcp::connection_manager::ConnectionManager;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::storage_engine::StorageEngine;
use crate::tcp::connection::ConnectionHandleData;

/*
TODO:
1. Determine a good duration for connection timeouts or get it from config
2. set maximum request size and reject if exceeded
*/

pub struct Server {
    storage_engine: Arc<StorageEngine>,
    address: SocketAddr,
    do_cancel: CancellationToken,
    sender: Sender<ConnectionData>,
    connection_manager_handle: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

async fn start_connection(
    conn_id: u64,
    mut socket: TcpStream,
    storage_engine: Arc<StorageEngine>,
    sender: Sender<ConnectionData>,
    token: CancellationToken,
) {
    let (reader, writer) = socket.split();
    let mut connection = TcpConnection::new(conn_id, sender, reader, writer, storage_engine, token);
    connection.start(Duration::from_secs(120)).await
}

impl Server {
    pub fn new(
        storage_engine: Arc<StorageEngine>,
        addr: SocketAddr,
        token: CancellationToken,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let connection_manager = ConnectionManager::new(rx, token.clone());
        let handle = tokio::spawn(async move {
            connection_manager.run().await;
        });
        Server {
            storage_engine: storage_engine,
            address: addr,
            do_cancel: token,
            sender: tx,
            connection_manager_handle: handle,
            cancellation_token: CancellationToken::new(),
        }
    }
    pub async fn run(self) -> io::Result<()> {
        let mut last_conn_id: u64 = 0;
        let listener: TcpListener = TcpListener::bind(self.address).await?;
        while !self.do_cancel.is_cancelled() {
            let (socket, _) = listener.accept().await?;
            let storage_engine_ptr = self.storage_engine.clone();
            let sender = self.sender.clone();
            let conn_id = last_conn_id.clone();
            let token = self.cancellation_token.clone();
            let task_handle = tokio::spawn(async move {
                let _ = start_connection(conn_id, socket, storage_engine_ptr, sender, token).await;
            });
            let conn_handle = ConnectionHandle {
                task_handle: task_handle,
                cancellation_token: self.cancellation_token.clone(),
            };
            let _ = self
                .sender
                .send(ConnectionData::StartSignal(ConnectionHandleData {
                    conn_handle: conn_handle,
                    id: conn_id.clone(),
                }))
                .await;
            last_conn_id = last_conn_id + 1;
        }
        self.cancellation_token.cancel();
        let _ = self.connection_manager_handle.await;
        Ok(())
    }
}
