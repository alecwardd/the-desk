//! Headless engine host for SIL-M2a.
//!
//! `the-desk-engine` owns ingest, pipelines, and event detection so intelligence
//! survives MCP/agent disconnect. The MCP server is a thin adapter over a
//! read-only local state socket, with an **embedded-engine fallback** that keeps
//! today's MCP-owns-ingest topology as a true rollback.
//!
//! Published state uses a lock-free swap (addresses the stdio-stall failure mode
//! from contended pipeline mutexes). Trust Ceiling stays **L3** — no path here
//! reaches order placement. MarketRouter multi-symbol is later (#7).

pub mod config;
pub mod health;
pub mod host;
pub mod published;
pub mod socket;
pub mod source;

pub use crate::catalog::EngineMode;
pub use config::{load_engine_bind_addr, ENGINE_DEFAULT_BIND};
pub use health::{EngineHealth, FeedStallState};
pub use host::{coaching_parity_fingerprint, EngineHost, IngestOutcome};
pub use published::{PublishedEngineState, PublishedStateStore};
pub use socket::{EngineClient, EngineSocketServer, SocketRequest, SocketResponse};
pub use source::{
    FileProvider, SierraProvider, SourceError, SourceProvider, SourceProviderKind, SourceTick,
};
