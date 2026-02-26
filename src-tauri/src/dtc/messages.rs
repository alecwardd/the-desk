use super::TradeSide;

/// DTC protocol version 8 — required by Sierra Chart.
pub const DTC_PROTOCOL_VERSION: i32 = 8;

/// Binary fixed-length-string encoding (EncodingEnum value 0).
pub const DTC_ENCODING_BINARY: i32 = 0;

/// Recommended heartbeat interval in seconds.
pub const DEFAULT_HEARTBEAT_INTERVAL: i32 = 10;

// Field sizes from DTCProtocol.h
const USERNAME_PASSWORD_LENGTH: usize = 32;
const GENERAL_IDENTIFIER_LENGTH: usize = 64;
const TRADE_ACCOUNT_LENGTH: usize = 32;
const CLIENT_NAME_LENGTH: usize = 32;
const SYMBOL_LENGTH: usize = 64;
const EXCHANGE_LENGTH: usize = 16;
const TEXT_DESCRIPTION_LENGTH: usize = 96;

/// DTC protocol message type identifiers mapped to their wire values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtcMessageType {
    /// Request binary encoding mode (type 6).
    EncodingRequest = 6,
    /// Server acknowledgement of encoding mode (type 7).
    EncodingResponse = 7,
    /// Client logon request (type 1).
    LogonRequest = 1,
    /// Server logon acknowledgement (type 2).
    LogonResponse = 2,
    /// Keepalive heartbeat message (type 3).
    Heartbeat = 3,
    /// Subscribe to market data for a symbol (type 101).
    MarketDataRequest = 101,
    /// Server rejection of a market data subscription (type 103).
    MarketDataReject = 103,
    /// Initial snapshot after subscription accepted (type 104).
    MarketDataSnapshot = 104,
    /// Trade execution update — double precision (type 107, pack(8)).
    MarketDataUpdateTrade = 107,
    /// Bid/ask quote update — mixed double/float (type 108, pack(8)).
    MarketDataUpdateBidAsk = 108,
    /// Trade update — float compact (type 112, pack(8)).
    MarketDataUpdateTradeCompact = 112,
    /// Bid/ask update — float compact (type 117, pack(8)).
    MarketDataUpdateBidAskCompact = 117,
    /// Trade with unbundled indicator (type 137, pack(8)).
    MarketDataUpdateTradeWithUnbundledIndicator = 137,
    /// Trade update — float, no timestamp (type 142, pack(1)).
    MarketDataUpdateTradeNoTimestamp = 142,
    /// Bid/ask update — float, no timestamp (type 143, pack(1)).
    MarketDataUpdateBidAskNoTimestamp = 143,
    /// Bid/ask update — float with millisecond timestamp (type 144, pack(1)).
    MarketDataUpdateBidAskFloatWithMilliseconds = 144,
    /// Unrecognized or unsupported message type.
    Unknown = 0,
}

impl DtcMessageType {
    /// Return the raw wire value even for Unknown types.
    pub fn wire_value(&self) -> u16 {
        *self as u16
    }
}

impl From<u16> for DtcMessageType {
    fn from(value: u16) -> Self {
        match value {
            1 => Self::LogonRequest,
            2 => Self::LogonResponse,
            3 => Self::Heartbeat,
            6 => Self::EncodingRequest,
            7 => Self::EncodingResponse,
            101 => Self::MarketDataRequest,
            103 => Self::MarketDataReject,
            104 => Self::MarketDataSnapshot,
            107 => Self::MarketDataUpdateTrade,
            108 => Self::MarketDataUpdateBidAsk,
            112 => Self::MarketDataUpdateTradeCompact,
            117 => Self::MarketDataUpdateBidAskCompact,
            137 => Self::MarketDataUpdateTradeWithUnbundledIndicator,
            142 => Self::MarketDataUpdateTradeNoTimestamp,
            143 => Self::MarketDataUpdateBidAskNoTimestamp,
            144 => Self::MarketDataUpdateBidAskFloatWithMilliseconds,
            _ => Self::Unknown,
        }
    }
}

/// A decoded DTC frame containing the message type and raw payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawDtcMessage {
    /// The DTC message type parsed from the frame header.
    pub message_type: DtcMessageType,
    /// Original wire type value (preserved for logging unknown types).
    pub raw_type: u16,
    /// Raw payload bytes following the 4-byte frame header.
    pub payload: Vec<u8>,
}

/// Convert a DTC `AtBidOrAskEnum8` (u8) into a [`TradeSide`].
pub fn decode_trade_side(at_bid_or_ask: u8) -> TradeSide {
    match at_bid_or_ask {
        1 => TradeSide::Sell,
        2 => TradeSide::Buy,
        _ => TradeSide::Unknown,
    }
}

/// Convert a DTC `AtBidOrAskEnum` (u16) into a [`TradeSide`].
pub fn decode_trade_side_u16(at_bid_or_ask: u16) -> TradeSide {
    match at_bid_or_ask {
        1 => TradeSide::Sell,
        2 => TradeSide::Buy,
        _ => TradeSide::Unknown,
    }
}

/// Extract reject text from a `MARKET_DATA_REJECT` payload (type 103).
///
/// Layout (after 4-byte header): SymbolID(u32) | RejectText(char[96])
pub fn parse_reject_text(payload: &[u8]) -> (u32, String) {
    let symbol_id = payload
        .get(0..4)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_le_bytes)
        .unwrap_or(0);
    let text = if payload.len() > 4 {
        let end = payload.len().min(4 + TEXT_DESCRIPTION_LENGTH);
        let text_bytes = &payload[4..end];
        let nul = text_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(text_bytes.len());
        String::from_utf8_lossy(&text_bytes[..nul]).to_string()
    } else {
        String::new()
    };
    (symbol_id, text)
}

// ---------------------------------------------------------------------------
// Wire-format message builders (payload only; caller wraps with build_frame)
// ---------------------------------------------------------------------------

/// Build the payload for `ENCODING_REQUEST` (type 6).
///
/// Layout: ProtocolVersion(i32) | Encoding(i32) | ProtocolType(char[4])
pub fn build_encoding_request_payload() -> Vec<u8> {
    let mut p = Vec::with_capacity(12);
    p.extend_from_slice(&DTC_PROTOCOL_VERSION.to_le_bytes());
    p.extend_from_slice(&DTC_ENCODING_BINARY.to_le_bytes());
    p.extend_from_slice(b"DTC\0");
    p
}

/// Build the payload for `LOGON_REQUEST` (type 1).
///
/// Matches `s_LogonRequest` from DTCProtocol.h (version 8).
pub fn build_logon_request_payload(heartbeat_seconds: i32) -> Vec<u8> {
    // Payload size = total struct size (276) minus the 4-byte header
    let mut p = Vec::with_capacity(272);
    p.extend_from_slice(&DTC_PROTOCOL_VERSION.to_le_bytes()); // ProtocolVersion
    p.extend_from_slice(&[0u8; USERNAME_PASSWORD_LENGTH]); // Username
    p.extend_from_slice(&[0u8; USERNAME_PASSWORD_LENGTH]); // Password
    p.extend_from_slice(&[0u8; GENERAL_IDENTIFIER_LENGTH]); // GeneralTextData
    p.extend_from_slice(&0i32.to_le_bytes()); // Integer_1
    p.extend_from_slice(&0i32.to_le_bytes()); // Integer_2
    p.extend_from_slice(&heartbeat_seconds.to_le_bytes()); // HeartbeatIntervalInSeconds
    p.extend_from_slice(&0i32.to_le_bytes()); // TradeMode (0 = unset / no trading)
    p.extend_from_slice(&[0u8; TRADE_ACCOUNT_LENGTH]); // TradeAccount
    p.extend_from_slice(&[0u8; GENERAL_IDENTIFIER_LENGTH]); // HardwareIdentifier

    let mut client_name = [0u8; CLIENT_NAME_LENGTH];
    let name = b"The Desk";
    client_name[..name.len()].copy_from_slice(name);
    p.extend_from_slice(&client_name); // ClientName

    p
}

/// Build the payload for `MARKET_DATA_REQUEST` (type 101, subscribe).
///
/// Matches `s_MarketDataRequest` from DTCProtocol.h.
pub fn build_market_data_request_payload(symbol_id: u32, symbol: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(92);
    p.extend_from_slice(&1i32.to_le_bytes()); // RequestAction = SUBSCRIBE
    p.extend_from_slice(&symbol_id.to_le_bytes()); // SymbolID

    let mut sym_buf = [0u8; SYMBOL_LENGTH];
    let sym_bytes = symbol.as_bytes();
    let len = sym_bytes.len().min(SYMBOL_LENGTH - 1);
    sym_buf[..len].copy_from_slice(&sym_bytes[..len]);
    p.extend_from_slice(&sym_buf); // Symbol

    p.extend_from_slice(&[0u8; EXCHANGE_LENGTH]); // Exchange
    p.extend_from_slice(&0u32.to_le_bytes()); // IntervalForSnapshotUpdatesInMilliseconds

    p
}

/// Build the payload for a `HEARTBEAT` message (type 3).
///
/// Layout: NumDroppedMessages(u32) | CurrentDateTime(i64, DTC t_DateTime)
pub fn build_heartbeat_payload() -> Vec<u8> {
    let mut p = Vec::with_capacity(12);
    p.extend_from_slice(&0u32.to_le_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    p.extend_from_slice(&now.to_le_bytes());
    p
}

// ---------------------------------------------------------------------------
// Response parsers
// ---------------------------------------------------------------------------

/// Parsed fields from a `LOGON_RESPONSE` message (type 2).
#[derive(Debug)]
#[allow(dead_code)]
pub struct LogonResult {
    pub protocol_version: i32,
    /// 1 = success, 2 = error, 3 = error-no-reconnect.
    pub result: i32,
    pub result_text: String,
}

impl LogonResult {
    /// Parse from the raw payload bytes (everything after the 4-byte frame header).
    pub fn from_payload(payload: &[u8]) -> Self {
        let protocol_version = payload
            .get(0..4)
            .and_then(|b| b.try_into().ok())
            .map(i32::from_le_bytes)
            .unwrap_or(0);

        let result = payload
            .get(4..8)
            .and_then(|b| b.try_into().ok())
            .map(i32::from_le_bytes)
            .unwrap_or(0);

        let result_text = if payload.len() > 8 {
            let end = payload.len().min(8 + TEXT_DESCRIPTION_LENGTH);
            let text_bytes = &payload[8..end];
            let nul = text_bytes
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(text_bytes.len());
            String::from_utf8_lossy(&text_bytes[..nul]).to_string()
        } else {
            String::new()
        };

        Self {
            protocol_version,
            result,
            result_text,
        }
    }

    pub fn is_success(&self) -> bool {
        self.result == 1 // LOGON_SUCCESS
    }

    /// Server explicitly said "do not reconnect".
    #[allow(dead_code)]
    pub fn is_no_reconnect(&self) -> bool {
        self.result == 3 // LOGON_ERROR_NO_RECONNECT
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_request_payload_size() {
        let p = build_encoding_request_payload();
        assert_eq!(p.len(), 12); // 4 + 4 + 4
        let version = i32::from_le_bytes(p[0..4].try_into().unwrap());
        assert_eq!(version, DTC_PROTOCOL_VERSION);
        assert_eq!(&p[8..12], b"DTC\0");
    }

    #[test]
    fn logon_request_payload_has_correct_version() {
        let p = build_logon_request_payload(10);
        assert!(p.len() >= 4);
        let version = i32::from_le_bytes(p[0..4].try_into().unwrap());
        assert_eq!(version, DTC_PROTOCOL_VERSION);
    }

    #[test]
    fn logon_request_heartbeat_at_correct_offset() {
        let p = build_logon_request_payload(15);
        // Offset: 4 (version) + 32 (user) + 32 (pass) + 64 (text) + 4 + 4 = 140
        let hb = i32::from_le_bytes(p[140..144].try_into().unwrap());
        assert_eq!(hb, 15);
    }

    #[test]
    fn logon_request_client_name_at_correct_offset() {
        let p = build_logon_request_payload(10);
        // Offset: 140 (hb) + 4 (hb) + 4 (trade_mode) + 32 (account) + 64 (hw) = 244
        let name_bytes = &p[244..244 + 8];
        assert_eq!(name_bytes, b"The Desk");
    }

    #[test]
    fn market_data_request_payload_structure() {
        let p = build_market_data_request_payload(42, "NQ");
        let action = i32::from_le_bytes(p[0..4].try_into().unwrap());
        assert_eq!(action, 1); // SUBSCRIBE
        let sym_id = u32::from_le_bytes(p[4..8].try_into().unwrap());
        assert_eq!(sym_id, 42);
        assert_eq!(p[8], b'N');
        assert_eq!(p[9], b'Q');
        assert_eq!(p[10], 0); // null terminated
    }

    #[test]
    fn heartbeat_payload_structure() {
        let p = build_heartbeat_payload();
        assert_eq!(p.len(), 12); // 4 (dropped) + 8 (datetime)
        let dropped = u32::from_le_bytes(p[0..4].try_into().unwrap());
        assert_eq!(dropped, 0);
    }

    #[test]
    fn logon_result_parses_success() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&8i32.to_le_bytes()); // version
        payload.extend_from_slice(&1i32.to_le_bytes()); // LOGON_SUCCESS
        let r = LogonResult::from_payload(&payload);
        assert!(r.is_success());
        assert_eq!(r.protocol_version, 8);
    }

    #[test]
    fn logon_result_parses_rejection() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&8i32.to_le_bytes());
        payload.extend_from_slice(&2i32.to_le_bytes()); // LOGON_ERROR
        payload.extend_from_slice(b"Version mismatch\0");
        let r = LogonResult::from_payload(&payload);
        assert!(!r.is_success());
        assert_eq!(r.result_text, "Version mismatch");
    }

    #[test]
    fn logon_result_no_reconnect() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&8i32.to_le_bytes());
        payload.extend_from_slice(&3i32.to_le_bytes()); // LOGON_ERROR_NO_RECONNECT
        let r = LogonResult::from_payload(&payload);
        assert!(r.is_no_reconnect());
    }
}
