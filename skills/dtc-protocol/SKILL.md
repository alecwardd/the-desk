---
name: DTCProtocol
description: DTC (Data and Trading Communication) protocol reference for The Desk. USE WHEN implementing or debugging the DTC client, troubleshooting Sierra Chart connectivity, parsing market data messages, or working on the data feed layer.
---

# DTC Protocol Reference

Technical reference for implementing The Desk's DTC protocol client. Based on the official DTC specification and Sierra Chart's implementation.

**Official documentation:** https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php

---

## Connection Flow

```
Client                          Sierra Chart DTC Server
  │                                      │
  │──── TCP Connect ────────────────────>│  (IP:port, default localhost:11099)
  │                                      │
  │──── ENCODING_REQUEST ──────────────>│  (always binary encoded)
  │<─── ENCODING_RESPONSE ─────────────│  (confirms encoding)
  │                                      │
  │──── LOGON_REQUEST ─────────────────>│  (heartbeat interval, client name)
  │<─── LOGON_RESPONSE ────────────────│  (server capabilities, status)
  │                                      │
  │──── MARKET_DATA_REQUEST ───────────>│  (subscribe to NQ, assign SymbolID)
  │<─── MARKET_DATA_SNAPSHOT ──────────│  (initial state)
  │<─── MARKET_DATA_UPDATE_TRADE ──────│  (streaming trades)
  │<─── MARKET_DATA_UPDATE_BID_ASK ────│  (streaming quotes)
  │                                      │
  │──── MARKET_DEPTH_REQUEST ──────────>│  (subscribe to DOM)
  │<─── MARKET_DEPTH_SNAPSHOT_LEVEL ───│  (initial DOM levels, one per level)
  │<─── MARKET_DEPTH_UPDATE_LEVEL ─────│  (streaming DOM changes)
  │                                      │
  │<──> HEARTBEAT ─────────────────────>│  (bidirectional, periodic)
  │                                      │
```

---

## Message Format

### Message Structure

Every DTC message begins with a 2-byte size header:

```
[2 bytes: message size (uint16, little-endian)] [2 bytes: message type (uint16)] [payload...]
```

**Critical:** TCP is a stream protocol. Messages may arrive split across TCP reads, or multiple messages may arrive in a single read. The client MUST:
1. Buffer incoming data
2. Read the 2-byte size header
3. Wait until `size` bytes are available
4. Parse the complete message
5. Repeat

### Encoding Types

| Encoding | Value | Description |
|----------|-------|-------------|
| Binary (fixed-length strings) | 0 | Default, most efficient |
| Binary (variable-length strings) | 1 | More flexible string handling |
| JSON | 2 | Human-readable, larger |
| JSON compact | 3 | Compressed JSON |
| Protocol Buffers | 4 | Google protobuf |

**Recommendation for The Desk:** Use binary encoding (type 0) for performance. The ENCODING_REQUEST itself is always sent in binary encoding regardless of the requested encoding.

### Data Types

| Type | Size | Description |
|------|------|-------------|
| Price | 8 bytes | 64-bit double (IEEE 754) |
| Volume | 8 bytes | 64-bit double |
| DateTime | 8 bytes | DTC DateTime (seconds since Unix epoch as double) |
| String (fixed) | varies | Null-terminated, fixed buffer size |
| int32 | 4 bytes | 32-bit signed integer |
| uint16 | 2 bytes | 16-bit unsigned integer (message type, size) |
| uint32 | 4 bytes | 32-bit unsigned integer (SymbolID) |
| byte | 1 byte | Boolean or flag |

---

## Key Message Types

### ENCODING_REQUEST (Type 6)

Sent by client to negotiate encoding. Always binary encoded.

```rust
struct EncodingRequest {
    size: u16,           // = 8
    msg_type: u16,       // = 6
    protocol_version: i32, // Current DTC protocol version
    encoding: i32,       // Requested encoding (0 = binary)
}
```

### ENCODING_RESPONSE (Type 7)

Server confirms encoding.

### LOGON_REQUEST (Type 1)

```rust
struct LogonRequest {
    size: u16,
    msg_type: u16,                // = 1
    protocol_version: i32,        // DTC protocol version
    username: [u8; 32],           // Optional
    password: [u8; 32],           // Optional
    general_text_data: [u8; 64],  // Optional
    integer_1: i32,               // Client-specific
    integer_2: i32,               // Client-specific
    heartbeat_interval_seconds: i32, // Suggest 10-30 seconds
    trade_mode: i32,              // 0 = no trading, 1 = simulated, 2 = live
    trade_account: [u8; 32],      // Optional
    hardware_identifier: [u8; 64], // Optional
    client_name: [u8; 32],        // "The Desk"
}
```

**Notes:**
- Set `trade_mode = 0` (no trading). The Desk never places orders.
- `heartbeat_interval_seconds`: 10-30 seconds is typical. Both sides must send heartbeats.

### LOGON_RESPONSE (Type 2)

Server responds with capabilities and status.

Key fields:
- `result`: 1 = success, 0 = failure
- `security_definitions_supported`: boolean
- `market_depth_supported`: boolean
- `trading_supported`: boolean

### MARKET_DATA_REQUEST (Type 101)

Subscribe to real-time data for a symbol.

```rust
struct MarketDataRequest {
    size: u16,
    msg_type: u16,          // = 101
    request_action: i32,    // 1 = subscribe, 2 = unsubscribe, 3 = snapshot
    symbol_id: u32,         // Client-assigned unique ID for this subscription
    symbol: [u8; 64],       // e.g., "NQ" or "NQH26"
    exchange: [u8; 16],     // e.g., "CME"
}
```

**Symbol format for Sierra Chart:**
- Continuous contract: use the symbol as configured in SC (e.g., "NQ")
- Specific month: "NQH26" (H = March, 26 = 2026)
- Check Sierra Chart's symbol settings for exact format used by your data feed

### MARKET_DATA_SNAPSHOT (Type 104)

Initial snapshot of current market state. Sent once after subscription.

Key fields: `symbol_id`, `session_settlement_price`, `session_open_price`, `session_high_price`, `session_low_price`, `session_volume`, `session_num_trades`, `bid_price`, `ask_price`, `bid_quantity`, `ask_quantity`, `last_trade_price`, `last_trade_volume`, `last_trade_date_time`

### MARKET_DATA_UPDATE_TRADE (Type 107)

Streaming trade updates. Sent for every trade execution.

```rust
struct MarketDataUpdateTrade {
    size: u16,
    msg_type: u16,         // = 107
    symbol_id: u32,        // Matches your subscription
    at_bid_or_ask: u8,     // 0 = unknown, 1 = at bid, 2 = at ask
    price: f64,            // Trade price
    volume: f64,           // Trade volume (contracts)
    date_time: f64,        // DTC DateTime
}
```

**Critical for delta calculation:** The `at_bid_or_ask` field tells you whether the trade was at the bid (sell, negative delta) or ask (buy, positive delta). When this field is 0 (unknown), classify by comparing trade price to current bid/ask.

### MARKET_DATA_UPDATE_BID_ASK (Type 108)

Streaming bid/ask quote updates.

```rust
struct MarketDataUpdateBidAsk {
    size: u16,
    msg_type: u16,         // = 108
    symbol_id: u32,
    bid_price: f64,
    bid_quantity: f32,
    ask_price: f64,
    ask_quantity: f32,
    date_time: f64,        // May be 0 if not provided
}
```

### MARKET_DEPTH_REQUEST (Type 301)

Subscribe to market depth (DOM) data.

```rust
struct MarketDepthRequest {
    size: u16,
    msg_type: u16,         // = 301
    request_action: i32,   // 1 = subscribe, 2 = unsubscribe
    symbol_id: u32,        // Same SymbolID as market data subscription
    symbol: [u8; 64],
    exchange: [u8; 16],
    num_levels: i32,       // Number of DOM levels requested (e.g., 10)
}
```

### MARKET_DEPTH_UPDATE_LEVEL (Type 303)

Streaming DOM level updates.

```rust
struct MarketDepthUpdateLevel {
    size: u16,
    msg_type: u16,         // = 303
    symbol_id: u32,
    side: u16,             // 1 = bid, 2 = ask
    price: f64,
    quantity: f64,
    update_type: u8,       // 1 = insert, 2 = update, 3 = delete
    date_time: f64,
    num_orders: u32,       // Number of orders at this level (if available)
}
```

### HEARTBEAT (Type 3)

Bidirectional keepalive. Both client and server send at the negotiated interval.

```rust
struct Heartbeat {
    size: u16,
    msg_type: u16,         // = 3
    num_dropped_messages: u32, // Usually 0
    current_date_time: f64,
}
```

**If no heartbeat received within 2x the interval, consider the connection dead and reconnect.**

---

## Implementation Architecture for The Desk

### Recommended Rust Structure

```
src-tauri/src/dtc/
├── mod.rs              // Public API: connect(), subscribe(), on_trade(), on_depth()
├── connection.rs       // TCP connection management, reconnection logic
├── encoding.rs         // Binary message encoding/decoding
├── messages.rs         // Message type definitions (structs)
├── parser.rs           // Message buffering and parsing from TCP stream
├── client.rs           // High-level DTC client (orchestrates connection + subscriptions)
└── types.rs            // Shared types (SymbolId, Price, Volume, etc.)
```

### Connection Management

```
[Idle] ──connect──> [Connecting] ──success──> [Encoding] ──response──> [Authenticating]
                         │                                                    │
                         │                                              ──success──>
                         │                                                    │
                    ──failure──> [Reconnecting] ──backoff──> [Connecting]     │
                         ▲                                                    │
                         │                                              [Subscribing]
                         │                                                    │
                    ──timeout──                                          ──success──>
                         │                                                    │
                    [Connected] <──────────────────────────────────────────────┘
                         │
                    ──disconnect──> [Reconnecting]
```

**Reconnection strategy:**
1. First retry: immediate
2. Second retry: 1 second
3. Third retry: 5 seconds
4. Subsequent: 10 seconds
5. Max retries: unlimited (the session is running — keep trying)
6. On successful reconnect: re-subscribe to all symbols and depth

### Data Flow

```rust
// The DTC client emits structured events through channels
enum DtcEvent {
    Trade { symbol_id: u32, price: f64, volume: f64, side: TradeSide, timestamp: f64 },
    Quote { symbol_id: u32, bid: f64, bid_size: f32, ask: f64, ask_size: f32 },
    Depth { symbol_id: u32, side: DepthSide, price: f64, quantity: f64, update: DepthUpdate },
    Connected,
    Disconnected,
    Error(String),
}

enum TradeSide {
    Buy,    // at_bid_or_ask == 2 (at ask)
    Sell,   // at_bid_or_ask == 1 (at bid)
    Unknown // at_bid_or_ask == 0
}
```

The DTC client sends events through a Rust channel (`tokio::sync::broadcast` or `mpsc`). Pipeline consumers subscribe to the channel and process events independently.

---

## Sierra Chart-Specific Notes

### Enabling the DTC Server in Sierra Chart

1. Global Settings → Data/Trade Service Settings
2. Enable "SC DTC Server"
3. Default port: 11099 (configurable)
4. The server starts when SC starts and stops when SC closes

### Symbol Naming

Sierra Chart may use different symbol formats depending on the data feed:
- **Rithmic:** Typically uses the base symbol (e.g., "NQ") which SC maps to the current front month
- **Denali:** May use specific contract symbols
- Check SC's "Find Symbol" dialog for the exact symbol string

### Data Feed Considerations (Rithmic)

- Rithmic provides Level II data (DOM) — the DTC depth messages will be populated
- Trade messages include the `at_bid_or_ask` field — essential for delta calculation
- Rithmic has connection limits — The Desk's DTC connection goes through SC's server, not directly to Rithmic, so it doesn't count against Rithmic connection limits
- SC aggregates and forwards data — there may be minimal additional latency (typically <10ms)

### Common Issues

1. **SC DTC server not running:** The Desk can't connect if SC isn't running or if the DTC server is disabled in SC settings.
2. **Symbol not found:** If the subscription returns no data, check the symbol name matches SC's internal naming.
3. **No DOM data:** Depth data requires a data feed that supports Level II (Rithmic does, some Denali plans may not).
4. **Stale data after SC restart:** When SC restarts, the DTC server restarts. The Desk must detect the disconnect and re-establish everything.
5. **Contract rollover:** When NQ rolls to a new month, the symbol may change. The Desk should handle this gracefully (detect no data on old symbol, prompt user, or auto-detect front month).

---

## Testing the DTC Client

### Without Sierra Chart

For development/testing without a running SC instance, build a simple DTC mock server in Rust that:
1. Accepts TCP connections on localhost:11099
2. Responds to ENCODING_REQUEST and LOGON_REQUEST
3. Streams synthetic NQ trade data at realistic rates (50-100 trades/minute during RTH)
4. Supports MARKET_DEPTH_REQUEST with synthetic DOM data

### With Sierra Chart

1. Enable DTC server in SC settings
2. Open an NQ chart in SC (ensures SC is receiving NQ data)
3. Connect The Desk to localhost:11099
4. Verify: trades appearing in The Desk should match SC's time & sales window

### Data Validation

When validating the DTC client:
- Compare VWAP calculated from DTC trades to SC's built-in VWAP study
- Compare TPO profile to SC's Market Profile chart
- Compare cumulative delta to SC's delta study
- All should match within rounding tolerance

---

## Performance Considerations

- NQ generates ~50,000-100,000 trades per RTH session
- DOM updates are more frequent — potentially 10-50 per second
- The DTC client must handle sustained throughput of ~100-500 messages/second during active periods
- Message parsing should be zero-copy where possible (parse directly from buffer)
- Use ring buffers for the TCP read buffer to avoid memory allocation
- All timestamp conversion (DTC DateTime → system time) should be computed once at parse time
