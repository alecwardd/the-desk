use super::TradeSide;

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
    /// Trade execution update from the data feed (type 107).
    MarketDataUpdateTrade = 107,
    /// Bid/ask quote update from the data feed (type 108).
    MarketDataUpdateBidAsk = 108,
    /// Unrecognized or unsupported message type.
    Unknown = 0,
}

impl From<u16> for DtcMessageType {
    fn from(value: u16) -> Self {
        match value {
            6 => Self::EncodingRequest,
            7 => Self::EncodingResponse,
            1 => Self::LogonRequest,
            2 => Self::LogonResponse,
            3 => Self::Heartbeat,
            101 => Self::MarketDataRequest,
            107 => Self::MarketDataUpdateTrade,
            108 => Self::MarketDataUpdateBidAsk,
            _ => Self::Unknown,
        }
    }
}

/// A decoded DTC frame containing the message type and raw payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawDtcMessage {
    /// The DTC message type parsed from the frame header.
    pub message_type: DtcMessageType,
    /// Raw payload bytes following the 4-byte frame header.
    pub payload: Vec<u8>,
}

/// Convert a DTC at-bid-or-ask byte into a [`TradeSide`] enum value.
pub fn decode_trade_side(at_bid_or_ask: u8) -> TradeSide {
    match at_bid_or_ask {
        1 => TradeSide::Sell,
        2 => TradeSide::Buy,
        _ => TradeSide::Unknown,
    }
}
