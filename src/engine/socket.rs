//! Read-only length-prefixed JSON state socket (TCP localhost).
//!
//! Protocol: u32 big-endian length + UTF-8 JSON body. Ops only — no mutation
//! or order authority. Trust Ceiling remains L3.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::health::EngineHealth;
use super::published::{PublishedEngineState, PublishedStateStore};

/// Client → server request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SocketRequest {
    Ping,
    GetPublished,
    GetHealth,
}

/// Server → client response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ok", content = "data", rename_all = "snake_case")]
pub enum SocketResponse {
    Pong { engine_pid: u32 },
    Published(PublishedEngineState),
    Health(EngineHealth),
    Error { message: String },
}

async fn write_msg<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    value: &impl Serialize,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let len = (bytes.len() as u32).to_be_bytes();
    w.write_all(&len).await?;
    w.write_all(&bytes).await?;
    w.flush().await
}

async fn read_msg<R: AsyncReadExt + Unpin, T: for<'de> Deserialize<'de>>(
    r: &mut R,
) -> std::io::Result<T> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "socket message too large",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

/// Read-only engine socket server bound to a local TCP address.
pub struct EngineSocketServer {
    bind: String,
}

impl EngineSocketServer {
    pub fn new(bind: impl Into<String>) -> Self {
        Self { bind: bind.into() }
    }

    pub fn bind_addr(&self) -> &str {
        &self.bind
    }

    /// Serve until `shutdown` becomes true.
    pub async fn serve(
        self,
        store: PublishedStateStore,
        mut shutdown: watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.bind).await?;
        tracing::info!(bind = %self.bind, "engine.state_socket.listening");
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let store = store.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle_client(stream, store).await {
                            tracing::debug!(%peer, error = %err, "engine.state_socket.client_closed");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

async fn handle_client(mut stream: TcpStream, store: PublishedStateStore) -> std::io::Result<()> {
    loop {
        let req: SocketRequest = match read_msg(&mut stream).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let resp = match req {
            SocketRequest::Ping => SocketResponse::Pong {
                engine_pid: std::process::id(),
            },
            SocketRequest::GetPublished => {
                let state = store.load();
                SocketResponse::Published((*state).clone())
            }
            SocketRequest::GetHealth => {
                let state = store.load();
                SocketResponse::Health(state.health.clone())
            }
        };
        write_msg(&mut stream, &resp).await?;
    }
}

/// MCP-side client for the engine state socket.
#[derive(Clone)]
pub struct EngineClient {
    addr: String,
    /// Last successful generation — used to detect silent stale reads.
    last_generation: Arc<std::sync::atomic::AtomicU64>,
    last_error: Arc<std::sync::Mutex<Option<String>>>,
}

impl EngineClient {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            last_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_error: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().ok().and_then(|g| g.clone())
    }

    async fn call(&self, req: SocketRequest) -> Result<SocketResponse, String> {
        let connect = TcpStream::connect(&self.addr);
        let mut stream = tokio::time::timeout(Duration::from_secs(2), connect)
            .await
            .map_err(|_| "engine socket connect timed out".to_string())?
            .map_err(|e| format!("engine socket connect failed: {e}"))?;
        write_msg(&mut stream, &req)
            .await
            .map_err(|e| format!("engine socket write failed: {e}"))?;
        let resp: SocketResponse = read_msg(&mut stream)
            .await
            .map_err(|e| format!("engine socket read failed: {e}"))?;
        if let Ok(mut g) = self.last_error.lock() {
            *g = None;
        }
        Ok(resp)
    }

    pub async fn ping(&self) -> Result<u32, String> {
        match self.call(SocketRequest::Ping).await? {
            SocketResponse::Pong { engine_pid } => Ok(engine_pid),
            SocketResponse::Error { message } => Err(message),
            other => Err(format!("unexpected ping response: {other:?}")),
        }
    }

    /// Fetch published state; on transport failure returns a degraded placeholder
    /// (never silent stale success).
    pub async fn get_published(&self) -> PublishedEngineState {
        match self.call(SocketRequest::GetPublished).await {
            Ok(SocketResponse::Published(state)) => {
                self.last_generation
                    .store(state.generation, std::sync::atomic::Ordering::Release);
                state
            }
            Ok(SocketResponse::Error { message }) => {
                self.record_error(&message);
                PublishedEngineState::unavailable(message)
            }
            Ok(other) => {
                let msg = format!("unexpected published response: {other:?}");
                self.record_error(&msg);
                PublishedEngineState::unavailable(msg)
            }
            Err(err) => {
                self.record_error(&err);
                PublishedEngineState::unavailable(err)
            }
        }
    }

    pub async fn get_health(&self) -> EngineHealth {
        match self.call(SocketRequest::GetHealth).await {
            Ok(SocketResponse::Health(h)) => h,
            Ok(SocketResponse::Error { message }) => {
                self.record_error(&message);
                EngineHealth::unavailable(message)
            }
            Ok(other) => {
                let msg = format!("unexpected health response: {other:?}");
                self.record_error(&msg);
                EngineHealth::unavailable(msg)
            }
            Err(err) => {
                self.record_error(&err);
                EngineHealth::unavailable(err)
            }
        }
    }

    fn record_error(&self, msg: &str) {
        if let Ok(mut g) = self.last_error.lock() {
            *g = Some(msg.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::source::SourceProviderKind;
    use tokio::sync::watch;

    #[tokio::test]
    async fn socket_serves_published_and_recovers_after_restart() {
        let store = PublishedStateStore::new();
        store.store(PublishedEngineState {
            generation: 7,
            engine_pid: 1,
            published_at_ms: 1.0,
            data_time_ms: Some(1.0),
            source_provider: SourceProviderKind::File,
            market_state: serde_json::json!({"lastPrice": 42.0}),
            recent_events: vec![],
            health: EngineHealth::unavailable("boot"),
            degraded: false,
            degraded_note: None,
        });
        let (tx, rx) = watch::channel(false);
        // Pick a free port, then re-bind inside serve().
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = probe.local_addr().unwrap().to_string();
        drop(probe);
        let store_bg = store.clone();
        let server = EngineSocketServer::new(bind.clone());
        let handle = tokio::spawn(async move { server.serve(store_bg, rx).await });

        let client = EngineClient::new(bind.clone());
        // Wait briefly for listen
        for _ in 0..50 {
            if client.ping().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let published = client.get_published().await;
        assert_eq!(published.generation, 7);
        assert_eq!(published.market_state["lastPrice"], 42.0);

        // Kill server
        let _ = tx.send(true);
        let _ = handle.await;

        let degraded = client.get_published().await;
        assert!(degraded.degraded);
        assert!(client.last_error().is_some());

        // Recover: new server on same bind
        let (tx2, rx2) = watch::channel(false);
        store.store(PublishedEngineState {
            generation: 8,
            engine_pid: 2,
            published_at_ms: 2.0,
            data_time_ms: Some(2.0),
            source_provider: SourceProviderKind::File,
            market_state: serde_json::json!({"lastPrice": 43.0}),
            recent_events: vec![],
            health: EngineHealth::unavailable("recovered"),
            degraded: false,
            degraded_note: None,
        });
        let store_bg = store.clone();
        let server = EngineSocketServer::new(bind.clone());
        let handle2 = tokio::spawn(async move { server.serve(store_bg, rx2).await });
        for _ in 0..50 {
            if client.ping().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let recovered = client.get_published().await;
        assert!(!recovered.degraded);
        assert_eq!(recovered.generation, 8);
        assert_eq!(recovered.market_state["lastPrice"], 43.0);
        let _ = tx2.send(true);
        let _ = handle2.await;
    }
}
