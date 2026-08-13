//! Headless engine host for SIL-M2a / MarketRouter v0 (SIL-M2b).
//!
//! `the-desk-engine` owns ingest, pipelines, and event detection so intelligence
//! survives MCP/agent disconnect. The MCP server is a thin adapter over a
//! read-only local state socket, with an **embedded-engine fallback** that keeps
//! today's MCP-owns-ingest topology as a true rollback.
//!
//! **MarketRouter** hosts concurrent NQ + ES pipeline sets on one clock.
//! Published state uses a lock-free swap. Trust Ceiling stays **L3** — no path
//! here reaches order placement.

pub mod capsule;
pub mod config;
pub mod health;
pub mod host;
pub mod journal;
pub mod published;
pub mod root;
pub mod router;
pub mod socket;
pub mod source;

pub use crate::catalog::EngineMode;
pub use capsule::{
    capsule_window_bounds, CAPSULE_AFTER_MS, CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS,
};
pub use config::{default_engine_database_path, load_engine_bind_addr, ENGINE_DEFAULT_BIND};
pub use health::{EngineHealth, FeedStallState};
pub use host::{coaching_parity_fingerprint, EngineHost, IngestOutcome};
pub use journal::{
    journal_frame_second, journal_frames_as_of, persist_journal_observation, JournalPersistStats,
    JOURNAL_FRAME_INTERVAL_MS,
};
pub use published::{PublishedEngineState, PublishedStateStore};
pub use root::{parse_requested_roots, primary_root_from_config, RouterRoot, RouterRootError};
pub use router::{sort_ticks_one_clock, MarketRouter};
pub use socket::{EngineClient, EngineSocketServer, SocketRequest, SocketResponse};
pub use source::{
    FileProvider, SierraProvider, SourceError, SourceProvider, SourceProviderKind, SourceTick,
};
