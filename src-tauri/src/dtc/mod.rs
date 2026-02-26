mod messages;
mod mock_server;
mod parser;

pub use messages::{decode_trade_side, DtcMessageType, RawDtcMessage};
pub use mock_server::run_mock_dtc_server;
pub use parser::DtcFrameParser;

use crate::feed::FeedEvent;
pub use crate::feed::TradeSide;
use messages::{
    build_encoding_request_payload, build_heartbeat_payload, build_logon_request_payload,
    build_market_data_request_payload, decode_trade_side_u16, parse_reject_text, LogonResult,
    DEFAULT_HEARTBEAT_INTERVAL,
};

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

pub type DtcEvent = FeedEvent;

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
    #[error("server rejected logon: {0}")]
    Rejected(String),
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
        DtcMessageType::MarketDataUpdateTrade => parse_trade_107(&msg.payload),
        DtcMessageType::MarketDataUpdateBidAsk => parse_quote_108(&msg.payload),
        DtcMessageType::MarketDataUpdateTradeCompact => parse_trade_compact_112(&msg.payload),
        DtcMessageType::MarketDataUpdateBidAskCompact => parse_quote_compact_117(&msg.payload),
        DtcMessageType::MarketDataUpdateTradeWithUnbundledIndicator => {
            parse_trade_unbundled_137(&msg.payload)
        }
        DtcMessageType::MarketDataUpdateTradeNoTimestamp => parse_trade_no_ts_142(&msg.payload),
        DtcMessageType::MarketDataUpdateBidAskNoTimestamp => parse_quote_no_ts_143(&msg.payload),
        DtcMessageType::MarketDataUpdateBidAskFloatWithMilliseconds => {
            parse_quote_float_ms_144(&msg.payload)
        }
        DtcMessageType::MarketDataReject => {
            let (sym_id, text) = parse_reject_text(&msg.payload);
            eprintln!("[DTC] MARKET_DATA_REJECT sym_id={sym_id}: {text}");
            Some(DtcEvent::Error {
                message: format!("Market data rejected for symbol {sym_id}: {text}"),
            })
        }
        DtcMessageType::MarketDataSnapshot => {
            eprintln!("[DTC] Received MARKET_DATA_SNAPSHOT — subscription accepted");
            None
        }
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
    let mut parser = DtcFrameParser::default();
    let mut buf = [0u8; 4096];
    let mut got_encoding = false;
    let mut early_logon: Option<LogonResult> = None;

    // Step 1: Send ENCODING_REQUEST with protocol version 8, binary encoding.
    stream
        .write_all(&build_frame(
            DtcMessageType::EncodingRequest as u16,
            &build_encoding_request_payload(),
        ))
        .await
        .map_err(|e| DtcError::Connection(format!("Failed to write encoding request: {e}")))?;

    // Step 2: Wait for ENCODING_RESPONSE (buffer any early LOGON_RESPONSE).
    let deadline = Instant::now() + Duration::from_secs(5);
    while !got_encoding {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::from_millis(1));
        let n = tokio::time::timeout(remaining, stream.read(&mut buf))
            .await
            .map_err(|_| {
                DtcError::Connection("Timed out waiting for encoding response".to_string())
            })?
            .map_err(|e| DtcError::Connection(format!("Read failed during encoding: {e}")))?;
        if n == 0 {
            return Err(DtcError::Connection(
                "Server closed during encoding negotiation".to_string(),
            ));
        }
        parser.push_bytes(&buf[..n]);
        while let Some(msg) = parser.next_message() {
            match msg.message_type {
                DtcMessageType::EncodingResponse => got_encoding = true,
                DtcMessageType::LogonResponse => {
                    early_logon = Some(LogonResult::from_payload(&msg.payload));
                }
                _ => {}
            }
        }
    }

    // Step 3: Send LOGON_REQUEST with protocol version 8, heartbeat, client name.
    stream
        .write_all(&build_frame(
            DtcMessageType::LogonRequest as u16,
            &build_logon_request_payload(DEFAULT_HEARTBEAT_INTERVAL),
        ))
        .await
        .map_err(|e| DtcError::Connection(format!("Failed to write logon request: {e}")))?;

    // Step 4: Check for early logon response or wait for one.
    if let Some(logon) = early_logon {
        return validate_logon(logon);
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::from_millis(1));
        let n = tokio::time::timeout(remaining, stream.read(&mut buf))
            .await
            .map_err(|_| DtcError::Connection("Timed out waiting for logon response".to_string()))?
            .map_err(|e| DtcError::Connection(format!("Read failed during logon: {e}")))?;
        if n == 0 {
            return Err(DtcError::Connection(
                "Server closed during logon".to_string(),
            ));
        }
        parser.push_bytes(&buf[..n]);
        while let Some(msg) = parser.next_message() {
            if matches!(msg.message_type, DtcMessageType::LogonResponse) {
                return validate_logon(LogonResult::from_payload(&msg.payload));
            }
        }
    }
}

fn validate_logon(logon: LogonResult) -> Result<(), DtcError> {
    if logon.is_success() {
        return Ok(());
    }
    let reason = if logon.result_text.is_empty() {
        format!("Logon rejected (result code {})", logon.result)
    } else {
        logon.result_text
    };
    Err(DtcError::Rejected(reason))
}

async fn subscribe_symbol(stream: &mut TcpStream, symbol: &str) -> Result<(), DtcError> {
    stream
        .write_all(&build_frame(
            DtcMessageType::MarketDataRequest as u16,
            &build_market_data_request_payload(1, symbol),
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
                    Err(DtcError::Rejected(reason)) => {
                        set_atomic_state(&state, ConnectionState::Disconnected);
                        let _ = tx.send(DtcEvent::Error {
                            message: format!("Server rejected logon: {reason}"),
                        });
                        let _ = tx.send(DtcEvent::Disconnected);
                        break; // deterministic failure — do not retry
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
                    .write_all(&build_frame(DtcMessageType::Heartbeat as u16, &build_heartbeat_payload()))
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
                    if matches!(msg.message_type, DtcMessageType::Unknown) {
                        eprintln!(
                            "[DTC] Unhandled message type={} payload_len={}",
                            msg.raw_type,
                            msg.payload.len()
                        );
                    }
                    if let Some(event) = parse_dtc_frame(&msg) {
                        let _ = tx.send(event);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Trade parsers
// ---------------------------------------------------------------------------

/// Type 107 — `s_MarketDataUpdateTrade` (pack(8))
///
/// Payload offsets:
///   0..4   SymbolID        (u32)
///   4..6   AtBidOrAsk      (u16)
///   6..12  _padding_
///   12..20 Price            (f64)
///   20..28 Volume           (f64)
///   28..36 DateTime         (f64 / t_DateTimeWithMilliseconds)
fn parse_trade_107(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 36 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let side = decode_trade_side_u16(u16::from_le_bytes(payload[4..6].try_into().ok()?));
    let price = f64::from_le_bytes(payload[12..20].try_into().ok()?);
    let volume = f64::from_le_bytes(payload[20..28].try_into().ok()?);
    let timestamp = f64::from_le_bytes(payload[28..36].try_into().ok()?);
    Some(DtcEvent::Trade {
        symbol_id,
        price,
        volume,
        side,
        timestamp,
    })
}

/// Type 112 — `s_MarketDataUpdateTradeCompact` (pack(8))
///
/// Payload offsets:
///   0..4   Price       (f32)
///   4..8   Volume      (f32)
///   8..12  DateTime    (u32 / t_DateTime4Byte)
///   12..16 SymbolID    (u32)
///   16..18 AtBidOrAsk  (u16)
fn parse_trade_compact_112(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 18 {
        return None;
    }
    let price = f32::from_le_bytes(payload[0..4].try_into().ok()?) as f64;
    let volume = f32::from_le_bytes(payload[4..8].try_into().ok()?) as f64;
    let dt_raw = u32::from_le_bytes(payload[8..12].try_into().ok()?);
    let symbol_id = u32::from_le_bytes(payload[12..16].try_into().ok()?);
    let side = decode_trade_side_u16(u16::from_le_bytes(payload[16..18].try_into().ok()?));
    Some(DtcEvent::Trade {
        symbol_id,
        price,
        volume,
        side,
        timestamp: dt_raw as f64,
    })
}

/// Type 137 — `s_MarketDataUpdateTradeWithUnbundledIndicator` (pack(8))
///
/// Payload offsets:
///   0..4   SymbolID                    (u32)
///   4      AtBidOrAsk                  (u8 / AtBidOrAskEnum8)
///   5      UnbundledTradeIndicator     (u8)
///   6      SaleCondition               (u8)
///   7      Reserve_1                   (u8)
///   8..12  Reserve_2                   (u32)
///   12..16 _padding_ (align Price to 8)
///   16..24 Price                       (f64)
///   24..28 Volume                      (u32)
///   28..32 Reserve_3                   (u32)
///   32..40 DateTime                    (f64 / t_DateTimeWithMilliseconds)
fn parse_trade_unbundled_137(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 40 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let side = decode_trade_side(payload[4]);
    let price = f64::from_le_bytes(payload[16..24].try_into().ok()?);
    let volume = u32::from_le_bytes(payload[24..28].try_into().ok()?) as f64;
    let timestamp = f64::from_le_bytes(payload[32..40].try_into().ok()?);
    Some(DtcEvent::Trade {
        symbol_id,
        price,
        volume,
        side,
        timestamp,
    })
}

/// Type 142 — `s_MarketDataUpdateTradeNoTimestamp` (pack(1))
///
/// Payload offsets:
///   0..4  SymbolID    (u32)
///   4..8  Price       (f32)
///   8..12 Volume      (u32)
///   12    AtBidOrAsk  (u8 / AtBidOrAskEnum8)
fn parse_trade_no_ts_142(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 13 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let price = f32::from_le_bytes(payload[4..8].try_into().ok()?) as f64;
    let volume = u32::from_le_bytes(payload[8..12].try_into().ok()?) as f64;
    let side = decode_trade_side(payload[12]);
    Some(DtcEvent::Trade {
        symbol_id,
        price,
        volume,
        side,
        timestamp: 0.0,
    })
}

// ---------------------------------------------------------------------------
// Quote parsers
// ---------------------------------------------------------------------------

/// Type 108 — `s_MarketDataUpdateBidAsk` (pack(8))
///
/// Payload offsets:
///   0..4   SymbolID      (u32)
///   4..12  BidPrice      (f64)
///   12..16 BidQuantity   (f32)
///   16..20 _padding_
///   20..28 AskPrice      (f64)
///   28..32 AskQuantity   (f32)
///   32..36 DateTime      (u32 / t_DateTime4Byte)
fn parse_quote_108(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 32 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let bid = f64::from_le_bytes(payload[4..12].try_into().ok()?);
    let bid_size = f32::from_le_bytes(payload[12..16].try_into().ok()?) as f64;
    let ask = f64::from_le_bytes(payload[20..28].try_into().ok()?);
    let ask_size = f32::from_le_bytes(payload[28..32].try_into().ok()?) as f64;
    let timestamp = if payload.len() >= 36 {
        u32::from_le_bytes(payload[32..36].try_into().ok()?) as f64
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

/// Type 117 — `s_MarketDataUpdateBidAskCompact` (pack(8), all float)
///
/// Payload offsets:
///   0..4   BidPrice      (f32)
///   4..8   BidQuantity   (f32)
///   8..12  AskPrice      (f32)
///   12..16 AskQuantity   (f32)
///   16..20 DateTime      (u32)
///   20..24 SymbolID      (u32)
fn parse_quote_compact_117(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 24 {
        return None;
    }
    let bid = f32::from_le_bytes(payload[0..4].try_into().ok()?) as f64;
    let bid_size = f32::from_le_bytes(payload[4..8].try_into().ok()?) as f64;
    let ask = f32::from_le_bytes(payload[8..12].try_into().ok()?) as f64;
    let ask_size = f32::from_le_bytes(payload[12..16].try_into().ok()?) as f64;
    let dt_raw = u32::from_le_bytes(payload[16..20].try_into().ok()?);
    let symbol_id = u32::from_le_bytes(payload[20..24].try_into().ok()?);
    Some(DtcEvent::Quote {
        symbol_id,
        bid,
        ask,
        bid_size,
        ask_size,
        timestamp: dt_raw as f64,
    })
}

/// Type 143 — `s_MarketDataUpdateBidAskNoTimeStamp` (pack(1))
///
/// Payload offsets:
///   0..4   SymbolID      (u32)
///   4..8   BidPrice      (f32)
///   8..12  BidQuantity   (u32)
///   12..16 AskPrice      (f32)
///   16..20 AskQuantity   (u32)
fn parse_quote_no_ts_143(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 20 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let bid = f32::from_le_bytes(payload[4..8].try_into().ok()?) as f64;
    let bid_size = u32::from_le_bytes(payload[8..12].try_into().ok()?) as f64;
    let ask = f32::from_le_bytes(payload[12..16].try_into().ok()?) as f64;
    let ask_size = u32::from_le_bytes(payload[16..20].try_into().ok()?) as f64;
    Some(DtcEvent::Quote {
        symbol_id,
        bid,
        ask,
        bid_size,
        ask_size,
        timestamp: 0.0,
    })
}

/// Type 144 — `s_MarketDataUpdateBidAskFloatWithMilliseconds` (pack(1))
///
/// Payload offsets:
///   0..4   SymbolID      (u32)
///   4..8   BidPrice      (f32)
///   8..12  BidQuantity   (f32)
///   12..16 AskPrice      (f32)
///   16..20 AskQuantity   (f32)
///   20..28 DateTime      (i64 / t_DateTimeWithMillisecondsInt)
fn parse_quote_float_ms_144(payload: &[u8]) -> Option<DtcEvent> {
    if payload.len() < 28 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let bid = f32::from_le_bytes(payload[4..8].try_into().ok()?) as f64;
    let bid_size = f32::from_le_bytes(payload[8..12].try_into().ok()?) as f64;
    let ask = f32::from_le_bytes(payload[12..16].try_into().ok()?) as f64;
    let ask_size = f32::from_le_bytes(payload[16..20].try_into().ok()?) as f64;
    let ts_raw = i64::from_le_bytes(payload[20..28].try_into().ok()?);
    Some(DtcEvent::Quote {
        symbol_id,
        bid,
        ask,
        bid_size,
        ask_size,
        timestamp: ts_raw as f64,
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
