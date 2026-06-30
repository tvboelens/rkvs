use super::connection::{ConnectionData, ConnectionHandle};
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

pub struct ConnectionManager {
    handles: HashMap<u64, ConnectionHandle>,
    receiver: Receiver<ConnectionData>,
    cancellation_token: CancellationToken,
}

impl ConnectionManager {
    pub fn new(rx: Receiver<ConnectionData>, token: CancellationToken) -> Self {
        ConnectionManager {
            handles: HashMap::new(),
            receiver: rx,
            cancellation_token: token,
        }
    }
    pub async fn run(mut self) {
        while let Some(data) = self.receiver.recv().await {
            if self.cancellation_token.is_cancelled() {
                break;
            }
            match data {
                ConnectionData::CompletionSignal(id) => match self.handles.remove(&id) {
                    None => (),
                    Some(handle) => {
                        let _ = handle.task_handle.await; // TODO: should probably do something in case of error (logging)
                    }
                },
                ConnectionData::StartSignal(handle) => {
                    self.handles.insert(handle.id, handle.conn_handle);
                }
            }
        }
        for (_, handle) in self.handles {
            let _ = handle.task_handle.await;
        }
    }
}
/*
test cases
1. request completes when cancel happens after starting recv
2. when starting multiple requests in tasks, they are not killed until connection manager run completes
3. panics in the request tasks
*/
