//! Lock-free published engine state (ArcSwap).
//!
//! Readers (MCP adapter / socket) never take the pipeline mutex. Writers swap
//! a complete snapshot after each ingest/publish cycle.

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::health::EngineHealth;
use super::source::SourceProviderKind;

/// Coaching-path snapshot published by the engine host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PublishedEngineState {
    /// Generation monotonic counter (detects stalls / reconnect).
    pub generation: u64,
    /// Engine process identity (pid) for kill/recover diagnostics.
    pub engine_pid: u32,
    /// Wall-clock publish time (epoch ms).
    pub published_at_ms: f64,
    /// Market-data time of the last applied tick (epoch ms), when known.
    pub data_time_ms: Option<f64>,
    pub source_provider: SourceProviderKind,
    /// Full MarketState JSON (camelCase) for the live coaching path.
    pub market_state: Value,
    /// Recent event identity rows (type + timestamp + identityId).
    pub recent_events: Vec<Value>,
    pub health: EngineHealth,
    /// When true, consumers must treat market_state as unavailable/degraded.
    pub degraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_note: Option<String>,
    /// Per-root MarketState JSON (NQ and/or ES). Empty on pre-M2b placeholders.
    #[serde(default)]
    pub by_symbol: BTreeMap<String, Value>,
    /// Aligned MarketRouter clock (max applied market timestamp across lanes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_ms: Option<f64>,
    /// Coaching-path primary root (`NQ` unless configured otherwise).
    #[serde(default)]
    pub primary_root: String,
}

impl PublishedEngineState {
    /// Empty degraded placeholder used before the first successful publish or
    /// when the MCP adapter loses the engine connection.
    pub fn unavailable(note: impl Into<String>) -> Self {
        Self {
            generation: 0,
            engine_pid: std::process::id(),
            published_at_ms: chrono::Utc::now().timestamp_millis() as f64,
            data_time_ms: None,
            source_provider: SourceProviderKind::File,
            market_state: Value::Null,
            recent_events: Vec::new(),
            health: EngineHealth::unavailable(note),
            degraded: true,
            degraded_note: Some(
                "engine published state unavailable — your playbook reads should treat live structure as degraded"
                    .into(),
            ),
            by_symbol: BTreeMap::new(),
            clock_ms: None,
            primary_root: "NQ".into(),
        }
    }

    /// Snapshot for one MarketRouter root, falling back to primary `market_state`.
    pub fn snapshot_for_root(&self, root: &str) -> Option<&Value> {
        if let Some(v) = self.by_symbol.get(root) {
            if !v.is_null() {
                return Some(v);
            }
        }
        let primary = if self.primary_root.is_empty() {
            "NQ"
        } else {
            self.primary_root.as_str()
        };
        if root.eq_ignore_ascii_case(primary) && !self.market_state.is_null() {
            Some(&self.market_state)
        } else {
            None
        }
    }
}

/// Lock-free published-state handle shared between the ingest loop and readers.
#[derive(Clone)]
pub struct PublishedStateStore {
    inner: Arc<ArcSwap<PublishedEngineState>>,
}

impl Default for PublishedStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PublishedStateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(PublishedEngineState::unavailable(
                "engine starting",
            ))),
        }
    }

    /// Lock-free load of the latest published snapshot.
    pub fn load(&self) -> Arc<PublishedEngineState> {
        self.inner.load_full()
    }

    /// Lock-free swap of a complete published snapshot.
    pub fn store(&self, state: PublishedEngineState) {
        self.inner.store(Arc::new(state));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_store_swaps_without_blocking_readers() {
        let store = PublishedStateStore::new();
        let first = store.load();
        assert!(first.degraded);
        store.store(PublishedEngineState {
            generation: 1,
            engine_pid: 1,
            published_at_ms: 1.0,
            data_time_ms: Some(1.0),
            source_provider: SourceProviderKind::File,
            market_state: serde_json::json!({"lastPrice": 1.0}),
            recent_events: vec![],
            health: EngineHealth::unavailable("ok"),
            degraded: false,
            degraded_note: None,
            by_symbol: BTreeMap::new(),
            clock_ms: Some(1.0),
            primary_root: "NQ".into(),
        });
        let second = store.load();
        assert_eq!(second.generation, 1);
        assert!(!second.degraded);
        // Prior Arc remains valid (readers never observe a torn snapshot).
        assert!(first.degraded);
    }
}
