mod messages;
mod mock_server;
mod parser;

pub use messages::{decode_trade_side, DtcMessageType, RawDtcMessage};
pub use mock_server::run_mock_dtc_server;
pub use parser::DtcFrameParser;

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Side of a trade execution (buy, sell, or unclassified).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

/// Events emitted by the DTC client to downstream consumers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DtcEvent {
    /// DTC connection established and ready.
    Connected,
    /// DTC connection lost or closed.
    Disconnected,
    /// A trade execution received from the data feed.
    Trade {
        symbol_id: u32,
        price: f64,
        volume: f64,
        side: TradeSide,
        timestamp: f64,
    },
    /// A bid/ask quote update received from the data feed.
    Quote {
        symbol_id: u32,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
        timestamp: f64,
    },
    /// An error encountered during DTC communication.
    Error { message: String },
}

/// DTC connection lifecycle states for the handshake state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    EncodingNegotiated,
    Authenticated,
    Subscribed,
}

impl ConnectionState {
    fn as_u8(&self) -> u8 {
        match self {
            Self::Disconnected => 0,
            Self::Connecting => 1,
            Self::EncodingNegotiated => 2,
            Self::Authenticated => 3,
            Self::Subscribed => 4,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Connecting,
            2 => Self::EncodingNegotiated,
            3 => Self::Authenticated,
            4 => Self::Subscribed,
            _ => Self::Disconnected,
        }
    }
}

/// Errors that can occur during DTC client operations.
#[derive(Debug, Error)]
pub enum DtcError {
    #[error("already connected")]
    AlreadyConnected,
    #[error("not connected")]
    NotConnected,
    #[error("invalid transition from {0:?} to {1:?}")]
    InvalidTransition(ConnectionState, ConnectionState),
    #[error("connection error: {0}")]
    Connection(String),
}

/// Manages a DTC protocol connection with handshake, heartbeat, and reconnect logic.
pub struct DtcClient {
    state: Arc<AtomicU8>,
    tx: broadcast::Sender<DtcEvent>,
    feed_task: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl DtcClient {
    /// Create a new DTC client abstraction.
    pub fn new(tx: broadcast::Sender<DtcEvent>) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(ConnectionState::Disconnected.as_u8())),
            tx,
            feed_task: None,
            shutdown_tx: None,
        }
    }

    /// Simulate DTC handshake transitions for deterministic tests.
    pub async fn connect(
        &mut self,
        _host: &str,
        _port: u16,
        _symbol: &str,
    ) -> Result<(), DtcError> {
        self.transition(ConnectionState::Connecting)?;
        self.transition(ConnectionState::EncodingNegotiated)?;
        self.transition(ConnectionState::Authenticated)?;
        self.transition(ConnectionState::Subscribed)?;
        let _ = self.tx.send(DtcEvent::Connected);
        Ok(())
    }

    /// Disconnect from DTC endpoint.
    pub async fn disconnect(&mut self) -> Result<(), DtcError> {
        if matches!(self.state(), ConnectionState::Disconnected) {
            return Err(DtcError::NotConnected);
        }
        if let Some(shutdown) = self.shutdown_tx.take() {
            let _ = shutdown.send(true);
        }
        if let Some(handle) = self.feed_task.take() {
            handle.abort();
            let _ = handle.await;
        }
        self.set_state(ConnectionState::Disconnected);
        let _ = self.tx.send(DtcEvent::Disconnected);
        Ok(())
    }

    /// Connect to a DTC server via TCP and start a managed feed task
    /// with handshake, heartbeat checks, and reconnect backoff.
    pub async fn start_live_feed(
        &mut self,
        host: &str,
        port: u16,
        symbol: &str,
    ) -> Result<(), DtcError> {
        if !matches!(self.state(), ConnectionState::Disconnected) {
            return Err(DtcError::AlreadyConnected);
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);
        self.set_state(ConnectionState::Connecting);
        let addr = format!("{host}:{port}");
        let tx = self.tx.clone();
        let state = Arc::clone(&self.state);
        let symbol = symbol.to_string();
        let handle = tokio::spawn(async move {
            run_connection_manager(addr, symbol, tx, state, shutdown_rx).await;
        });
        self.feed_task = Some(handle);
        Ok(())
    }

    /// Publish a parsed event to downstream consumers.
    pub fn publish(&self, event: DtcEvent) {
        let _ = self.tx.send(event);
    }

    /// Return current connection state.
    pub fn state(&self) -> ConnectionState {
        ConnectionState::from_u8(self.state.load(Ordering::Relaxed))
    }

    fn transition(&mut self, next: ConnectionState) -> Result<(), DtcError> {
        let allowed = matches!(
            (&self.state(), &next),
            (ConnectionState::Disconnected, ConnectionState::Connecting)
                | (
                    ConnectionState::Connecting,
                    ConnectionState::EncodingNegotiated
                )
                | (
                    ConnectionState::EncodingNegotiated,
                    ConnectionState::Authenticated
                )
                | (ConnectionState::Authenticated, ConnectionState::Subscribed)
        );
        if !allowed {
            return Err(DtcError::InvalidTransition(self.state(), next));
        }
        self.set_state(next);
        Ok(())
    }

    fn set_state(&self, state: ConnectionState) {
        self.state.store(state.as_u8(), Ordering::Relaxed);
    }
}

fn parse_dtc_frame(msg: &RawDtcMessage) -> Option<DtcEvent> {
    match msg.message_type {
        DtcMessageType::MarketDataUpdateTrade => parse_trade_payload(&msg.payload),
        DtcMessageType::MarketDataUpdateBidAsk => parse_quote_payload(&msg.payload),
        _ => None,
    }
}

fn build_frame(message_type: u16, payload: &[u8]) -> Vec<u8> {
    let size = (4 + payload.len()) as u16;
    let mut frame = Vec::with_capacity(size as usize);
    frame.extend_from_slice(&size.to_le_bytes());
    frame.extend_from_slice(&message_type.to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn set_atomic_state(state: &Arc<AtomicU8>, next: ConnectionState) {
    state.store(next.as_u8(), Ordering::Relaxed);
}

async fn perform_handshake(stream: &mut TcpStream) -> Result<(), DtcError> {
    stream
        .write_all(&build_frame(
            DtcMessageType::EncodingRequest as u16,
            &0i32.to_le_bytes(),
        ))
        .await
        .map_err(|e| DtcError::Connection(format!("Failed to write encoding request: {e}")))?;
    stream
        .write_all(&build_frame(
            DtcMessageType::LogonRequest as u16,
            &[1, 0, 0, 0],
        ))
        .await
        .map_err(|e| DtcError::Connection(format!("Failed to write logon request: {e}")))?;

    let mut parser = DtcFrameParser::default();
    let mut buf = [0u8; 4096];
    let mut got_encoding = false;
    let mut got_logon = false;
    let deadline = Instant::now() + Duration::from_secs(5);

    while !(got_encoding && got_logon) {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::from_millis(1));
        let read_result = tokio::time::timeout(remaining, stream.read(&mut buf))
            .await
            .map_err(|_| DtcError::Connection("Handshake timed out".to_string()))?
            .map_err(|e| DtcError::Connection(format!("Handshake read failed: {e}")))?;
        if read_result == 0 {
            return Err(DtcError::Connection(
                "Server closed during handshake".to_string(),
            ));
        }
        parser.push_bytes(&buf[..read_result]);
        while let Some(msg) = parser.next_message() {
            match msg.message_type {
                DtcMessageType::EncodingResponse => got_encoding = true,
                DtcMessageType::LogonResponse => got_logon = true,
                _ => {}
            }
        }
    }
    Ok(())
}

async fn subscribe_symbol(stream: &mut TcpStream, symbol: &str) -> Result<(), DtcError> {
    stream
        .write_all(&build_frame(
            DtcMessageType::MarketDataRequest as u16,
            symbol.as_bytes(),
        ))
        .await
        .map_err(|e| DtcError::Connection(format!("Failed to write market data request: {e}")))?;
    Ok(())
}

fn reconnect_delay(attempt: u32) -> Duration {
    let base = 2u64.saturating_pow(attempt.min(5));
    let capped = base.min(30);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter_ms = nanos % 300;
    Duration::from_secs(capped) + Duration::from_millis(jitter_ms)
}

async fn run_connection_manager(
    addr: String,
    symbol: String,
    tx: broadcast::Sender<DtcEvent>,
    state: Arc<AtomicU8>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut attempt: u32 = 0;
    loop {
        if *shutdown_rx.borrow() {
            set_atomic_state(&state, ConnectionState::Disconnected);
            let _ = tx.send(DtcEvent::Disconnected);
            break;
        }

        set_atomic_state(&state, ConnectionState::Connecting);
        match TcpStream::connect(&addr).await {
            Ok(mut stream) => {
                let handshake_result = async {
                    perform_handshake(&mut stream).await?;
                    set_atomic_state(&state, ConnectionState::EncodingNegotiated);
                    set_atomic_state(&state, ConnectionState::Authenticated);
                    subscribe_symbol(&mut stream, &symbol).await?;
                    set_atomic_state(&state, ConnectionState::Subscribed);
                    Ok::<(), DtcError>(())
                }
                .await;

                match handshake_result {
                    Ok(()) => {
                        attempt = 0;
                        let _ = tx.send(DtcEvent::Connected);
                        if let Err(err) = run_feed_loop(stream, tx.clone(), &mut shutdown_rx).await
                        {
                            set_atomic_state(&state, ConnectionState::Disconnected);
                            let _ = tx.send(DtcEvent::Disconnected);
                            let _ = tx.send(DtcEvent::Error {
                                message: err.to_string(),
                            });
                        } else {
                            set_atomic_state(&state, ConnectionState::Disconnected);
                            let _ = tx.send(DtcEvent::Disconnected);
                            break;
                        }
                    }
                    Err(err) => {
                        set_atomic_state(&state, ConnectionState::Disconnected);
                        let _ = tx.send(DtcEvent::Error {
                            message: err.to_string(),
                        });
                    }
                }
            }
            Err(err) => {
                set_atomic_state(&state, ConnectionState::Disconnected);
                let _ = tx.send(DtcEvent::Error {
                    message: format!("Connection failed: {err}"),
                });
            }
        }

        attempt = attempt.saturating_add(1);
        let delay = reconnect_delay(attempt);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown_rx.changed() => {
                set_atomic_state(&state, ConnectionState::Disconnected);
                let _ = tx.send(DtcEvent::Disconnected);
                break;
            }
        }
    }
}

async fn run_feed_loop(
    stream: TcpStream,
    tx: broadcast::Sender<DtcEvent>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), DtcError> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut parser = DtcFrameParser::default();
    let mut buf = [0u8; 8192];
    let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(10));
    let mut last_incoming = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                return Ok(());
            }
            _ = heartbeat_tick.tick() => {
                write_half
                    .write_all(&build_frame(DtcMessageType::Heartbeat as u16, &[]))
                    .await
                    .map_err(|e| DtcError::Connection(format!("Heartbeat write failed: {e}")))?;
                if last_incoming.elapsed() > Duration::from_secs(30) {
                    return Err(DtcError::Connection("Heartbeat timeout (no inbound data)".to_string()));
                }
            }
            read_result = reader.read(&mut buf) => {
                let n = read_result
                    .map_err(|e| DtcError::Connection(format!("Feed read failed: {e}")))?;
                if n == 0 {
                    return Err(DtcError::Connection("Connection closed by server".to_string()));
                }
                last_incoming = Instant::now();
                parser.push_bytes(&buf[..n]);
                while let Some(msg) = parser.next_message() {
                    if matches!(msg.message_type, DtcMessageType::Heartbeat) {
                        continue;
                    }
                    if let Some(event) = parse_dtc_frame(&msg) {
                        let _ = tx.send(event);
                    }
                }
            }
        }
    }
}

fn parse_trade_payload(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 29 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let side = decode_trade_side(payload[4]);
    let price = f64::from_le_bytes(payload[5..13].try_into().ok()?);
    let volume = f64::from_le_bytes(payload[13..21].try_into().ok()?);
    let timestamp = f64::from_le_bytes(payload[21..29].try_into().ok()?);
    Some(DtcEvent::Trade {
        symbol_id,
        price,
        volume,
        side,
        timestamp,
    })
}

fn parse_quote_payload(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 36 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let bid = f64::from_le_bytes(payload[4..12].try_into().ok()?);
    let ask = f64::from_le_bytes(payload[12..20].try_into().ok()?);
    let bid_size = f64::from_le_bytes(payload[20..28].try_into().ok()?);
    let ask_size = f64::from_le_bytes(payload[28..36].try_into().ok()?);
    let timestamp = if payload.len() >= 44 {
        f64::from_le_bytes(payload[36..44].try_into().ok()?)
    } else {
        0.0
    };
    Some(DtcEvent::Quote {
        symbol_id,
        bid,
        ask,
        bid_size,
        ask_size,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtc::messages::DtcMessageType;
    use crate::dtc::run_mock_dtc_server;
    use tokio::time::timeout;

    #[tokio::test]
    async fn client_connect_disconnect() {
        let (tx, mut rx) = broadcast::channel(16);
        let mut client = DtcClient::new(tx);
        client
            .connect("localhost", 11099, "NQ")
            .await
            .expect("connect");
        assert!(matches!(client.state(), ConnectionState::Subscribed));
        let event = rx.recv().await.expect("receive connected");
        assert!(matches!(event, DtcEvent::Connected));
        client.disconnect().await.expect("disconnect");
        let event = rx.recv().await.expect("receive disconnected");
        assert!(matches!(event, DtcEvent::Disconnected));
    }

    #[test]
    fn parser_decodes_framed_message() {
        let mut parser = DtcFrameParser::default();
        // size=8 (u16 LE), type=107 (trade), then 4-byte payload dummy
        parser.push_bytes(&[8, 0, 107, 0, 1, 2, 3, 4]);
        let next = parser.next_message().expect("message");
        assert_eq!(next.message_type, DtcMessageType::MarketDataUpdateTrade);
        assert_eq!(next.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn reconnect_delay_bounded() {
        assert!(reconnect_delay(0) >= Duration::from_secs(1));
        assert!(reconnect_delay(10) <= Duration::from_secs(31));
    }

    #[tokio::test]
    async fn live_feed_receives_mock_trade() {
        let bind = "127.0.0.1:12199";
        tokio::spawn(async move {
            let _ = run_mock_dtc_server(bind).await;
        });
        tokio::time::sleep(Duration::from_millis(150)).await;

        let (tx, mut rx) = broadcast::channel(64);
        let mut client = DtcClient::new(tx);
        client
            .start_live_feed("127.0.0.1", 12199, "NQ")
            .await
            .expect("start live feed");

        let connected = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(DtcEvent::Connected) = rx.recv().await {
                    break;
                }
            }
        })
        .await;
        assert!(connected.is_ok(), "expected connected event");

        let trade = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(DtcEvent::Trade { .. }) = rx.recv().await {
                    break;
                }
            }
        })
        .await;
        assert!(trade.is_ok(), "expected trade event");

        client.disconnect().await.expect("disconnect");
    }
}
