# DTC Protocol Reference

Extended implementation notes for the DTC protocol skill. Use this file when you need wire-level details beyond `SKILL.md`.

## Connection Sequence

Canonical startup order:

1. TCP connect to Sierra Chart DTC endpoint (commonly `localhost:11099`)
2. Send `ENCODING_REQUEST` (wire-encoded as binary)
3. Receive `ENCODING_RESPONSE`
4. Send `LOGON_REQUEST`
5. Receive `LOGON_RESPONSE` and verify success
6. Send market data / depth subscriptions
7. Enter steady-state processing with heartbeat watchdog

If any required response is missing or invalid, treat startup as failed and reconnect with backoff.

## Frame Format

All DTC messages are framed over TCP:

```
[2-byte size: uint16 little-endian][2-byte type: uint16][payload...]
```

Parser requirements:

- maintain a persistent byte buffer
- parse only when full frame bytes are present
- support partial frames and multi-frame reads
- reject impossible sizes and recover cleanly

## Core Message Map

Use these message types as minimum viable coverage:

- `1` `LOGON_REQUEST`
- `2` `LOGON_RESPONSE`
- `3` `HEARTBEAT`
- `6` `ENCODING_REQUEST`
- `7` `ENCODING_RESPONSE`
- `101` `MARKET_DATA_REQUEST`
- `104` `MARKET_DATA_SNAPSHOT`
- `107` `MARKET_DATA_UPDATE_TRADE`
- `108` `MARKET_DATA_UPDATE_BID_ASK`
- `301` `MARKET_DEPTH_REQUEST`
- `303` `MARKET_DEPTH_UPDATE_LEVEL`

## Field Notes by Message

### `ENCODING_REQUEST`

- send first after TCP connect
- set requested encoding to binary fixed strings (`0`) unless intentionally testing alternatives

### `LOGON_REQUEST`

- `trade_mode` must remain no-trading for The Desk
- include a practical heartbeat interval (for example 10-30s)
- identify client name consistently (`The Desk`)

### `LOGON_RESPONSE`

- validate success result before subscribing
- treat negative/failed logon as terminal for that attempt

### `MARKET_DATA_REQUEST`

- use stable client-side `symbol_id` per symbol stream
- actions should be explicit subscribe/unsubscribe (no implicit state)

### `MARKET_DATA_UPDATE_TRADE`

- `at_bid_or_ask = 2` => buy-side trade
- `at_bid_or_ask = 1` => sell-side trade
- `at_bid_or_ask = 0` => unknown; apply deterministic fallback classification policy

### `MARKET_DATA_UPDATE_BID_ASK`

- quote updates can arrive frequently and outnumber trades during some periods
- keep latest bid/ask state per `symbol_id` for fallback trade classification

### `MARKET_DEPTH_UPDATE_LEVEL`

- interpret side and update type deterministically
- maintain depth book by price level keyed by `symbol_id` + side + price

### `HEARTBEAT`

- both directions matter
- if no heartbeat or data is received within watchdog threshold, treat as stale/dead connection

## Date/Time and Numeric Handling

- DTC timestamps are floating-point seconds from Unix epoch
- parse once and convert to internal timestamp type at ingest boundary
- avoid repeated conversion downstream
- prices and volumes should preserve precision through parse and event emission

## Suggested Rust Module Layout

```
src-tauri/src/dtc/
  mod.rs
  connection.rs
  parser.rs
  encoding.rs
  messages.rs
  client.rs
  types.rs
```

Recommended role split:

- `connection.rs`: socket lifecycle + reconnect loop
- `parser.rs`: byte buffering + frame extraction + decode dispatch
- `messages.rs`: message structs/enums and field-level parsing
- `client.rs`: orchestration (handshake, subscriptions, watchdog, emit events)

## Event Contract Example

Emit typed events to downstream consumers instead of leaking wire structs:

```rust
enum DtcEvent {
    Trade { symbol_id: u32, price: f64, volume: f64, side: TradeSide, timestamp: f64 },
    Quote { symbol_id: u32, bid: f64, bid_size: f32, ask: f64, ask_size: f32 },
    Depth { symbol_id: u32, side: DepthSide, price: f64, quantity: f64, update: DepthUpdate },
    Connected,
    Disconnected,
    Error(String),
}
```

## Reconnect and Recovery Contract

Backoff sequence:

1. immediate
2. 1 second
3. 5 seconds
4. 10 seconds (repeat)

On successful reconnect, always:

1. redo encoding negotiation
2. redo logon
3. restore subscriptions (market + depth)
4. clear and restart heartbeat watchdog

## Sierra Chart Specifics

- ensure Sierra Chart DTC server is enabled in settings
- ensure symbol names match Sierra Chart's configured feed mapping
- handle Sierra Chart restarts as full endpoint resets

## Test Vectors and Failure Cases

Minimum parser tests:

1. frame split across reads (size prefix in first read, payload in second)
2. multiple full frames in a single read buffer
3. malformed frame size smaller than header
4. malformed frame size larger than configured maximum
5. unknown message type with valid frame envelope

Minimum integration tests:

1. successful handshake and subscription
2. heartbeat timeout triggers reconnect
3. reconnect restores all subscriptions
4. trade-side mapping remains correct before and after reconnect

## Performance Notes

- expect high message bursts during active market windows
- avoid unnecessary allocations in parse loop
- prefer reuse of buffers and structures where practical
- keep parser and socket operations off UI/main thread

## Source Links

- Sierra Chart DTC documentation: `https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php`
- Project deep reference: `skills/dtc-protocol/SKILL.md`
