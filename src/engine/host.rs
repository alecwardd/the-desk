//! Engine host: ingest + pipelines + event detection + publish.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::feed::{ContractMetadata, TradeSide};
use crate::minute_of_session_from_timestamp;
use crate::pipelines::{EventDetector, FlowEventEmitter, MarketEvent, MarketState, PipelineEngine};

use super::health::{EngineHealth, FeedStallState};
use super::published::{PublishedEngineState, PublishedStateStore};
use super::root::RouterRoot;
use super::source::{SourceProvider, SourceProviderKind, SourceTick};

const STALL_THRESHOLD_MS: u64 = 10_000;
const RECENT_EVENTS_CAP: usize = 64;

/// Result of applying one tick through the engine host.
#[derive(Debug)]
pub struct IngestOutcome {
    pub snapshot: MarketState,
    pub new_events: Vec<MarketEvent>,
}

/// Headless (or in-process) engine host owning ingest → pipelines → events → publish.
pub struct EngineHost {
    pipelines: Arc<Mutex<PipelineEngine>>,
    detector: Arc<Mutex<EventDetector>>,
    flow_emitter: Arc<Mutex<FlowEventEmitter>>,
    published: PublishedStateStore,
    provider_kind: SourceProviderKind,
    generation: AtomicU64,
    ticks_ingested: AtomicU64,
    events_detected: AtomicU64,
    last_ingest_wall_ms: AtomicU64,
    last_publish_wall_ms: AtomicU64,
    last_tick_timestamp_bits: AtomicU64,
    last_bid: Arc<Mutex<f64>>,
    last_ask: Arc<Mutex<f64>>,
    recent_events: Mutex<Vec<Value>>,
    running: AtomicBool,
    mode_label: String,
}

impl EngineHost {
    /// Create a host with empty pipelines (tests / embedded publish path).
    pub fn new(provider_kind: SourceProviderKind, mode_label: impl Into<String>) -> Self {
        Self::from_shared(
            Arc::new(Mutex::new(PipelineEngine::new())),
            Arc::new(Mutex::new(EventDetector::new())),
            Arc::new(Mutex::new(FlowEventEmitter::new())),
            Arc::new(Mutex::new(0.0)),
            Arc::new(Mutex::new(0.0)),
            provider_kind,
            mode_label,
        )
    }

    /// Lane constructor: empty pipelines with `rootSymbol` already bound.
    pub fn new_for_root(
        root: RouterRoot,
        provider_kind: SourceProviderKind,
        mode_label: impl Into<String>,
    ) -> Self {
        let host = Self::new(provider_kind, mode_label);
        host.set_contract_metadata(root.placeholder_metadata());
        host
    }

    /// Share existing NQ pipelines / quotes (embedded-engine fallback).
    pub fn from_shared(
        pipelines: Arc<Mutex<PipelineEngine>>,
        detector: Arc<Mutex<EventDetector>>,
        flow_emitter: Arc<Mutex<FlowEventEmitter>>,
        last_bid: Arc<Mutex<f64>>,
        last_ask: Arc<Mutex<f64>>,
        provider_kind: SourceProviderKind,
        mode_label: impl Into<String>,
    ) -> Self {
        Self {
            pipelines,
            detector,
            flow_emitter,
            published: PublishedStateStore::new(),
            provider_kind,
            generation: AtomicU64::new(0),
            ticks_ingested: AtomicU64::new(0),
            events_detected: AtomicU64::new(0),
            last_ingest_wall_ms: AtomicU64::new(0),
            last_publish_wall_ms: AtomicU64::new(0),
            last_tick_timestamp_bits: AtomicU64::new(0),
            last_bid,
            last_ask,
            recent_events: Mutex::new(Vec::new()),
            running: AtomicBool::new(true),
            mode_label: mode_label.into(),
        }
    }

    /// Bind contract identity so snapshots carry `rootSymbol`.
    pub fn set_contract_metadata(&self, metadata: ContractMetadata) {
        if let Ok(mut p) = self.pipelines.lock() {
            p.set_contract_metadata(metadata);
        }
    }

    pub fn ticks_ingested(&self) -> u64 {
        self.ticks_ingested.load(Ordering::Acquire)
    }

    pub fn events_detected(&self) -> u64 {
        self.events_detected.load(Ordering::Acquire)
    }

    pub fn last_ingest_wall_ms(&self) -> u64 {
        self.last_ingest_wall_ms.load(Ordering::Acquire)
    }

    fn last_tick_timestamp_ms(&self) -> Option<f64> {
        let bits = self.last_tick_timestamp_bits.load(Ordering::Acquire);
        if bits == 0 {
            None
        } else {
            Some(f64::from_bits(bits))
        }
    }

    /// Market-data time for this lane: last `apply_tick`, else tape last trade.
    ///
    /// The tape fallback covers the embedded-engine path where MCP ingest
    /// updates the shared NQ `PipelineEngine` without calling `apply_tick`.
    pub fn market_time_ms(&self) -> Option<f64> {
        if let Some(ts) = self.last_tick_timestamp_ms() {
            return Some(ts);
        }
        self.pipelines
            .try_lock()
            .ok()
            .and_then(|p| p.tape_pace.last_trade_timestamp_ms())
    }

    /// Current MarketState JSON from this lane (Null when no quotes yet).
    ///
    /// Uses the last applied tick timestamp (or tape last trade) so session
    /// scope (RTH/Globex) is classified from market time, not the wall clock.
    pub fn snapshot_market_state(&self) -> Value {
        let bid = self.last_bid.lock().ok().map(|g| *g).unwrap_or(0.0);
        let ask = self.last_ask.lock().ok().map(|g| *g).unwrap_or(0.0);
        if bid <= 0.0 && ask <= 0.0 {
            return Value::Null;
        }
        match self.pipelines.lock() {
            Ok(p) => {
                let ts = self
                    .last_tick_timestamp_ms()
                    .or_else(|| p.tape_pace.last_trade_timestamp_ms());
                let snap = if let Some(ts) = ts {
                    p.snapshot_at(bid.max(1e-9), ask.max(1e-9), ts)
                } else {
                    p.snapshot(bid.max(1e-9), ask.max(1e-9))
                };
                serde_json::to_value(snap).unwrap_or(Value::Null)
            }
            Err(_) => Value::Null,
        }
    }

    /// Bump the host generation (used by MarketRouter combined publish).
    pub fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn recent_events_json(&self) -> Vec<Value> {
        self.recent_events
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    pub fn published_store(&self) -> PublishedStateStore {
        self.published.clone()
    }

    pub fn pipelines(&self) -> Arc<Mutex<PipelineEngine>> {
        Arc::clone(&self.pipelines)
    }

    pub fn mark_stopped(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Apply a single normalized trade (shared by embedded + headless paths).
    pub fn apply_tick(&self, tick: &SourceTick) -> Option<IngestOutcome> {
        let is_buy = matches!(tick.side, TradeSide::Buy);
        let minute = minute_of_session_from_timestamp(tick.timestamp_ms);
        let mut event_buffer = Vec::new();
        let snapshot = {
            let mut p = self.pipelines.lock().ok()?;
            p.on_trade_with_timestamp(tick.price, tick.volume, is_buy, minute, tick.timestamp_ms);
            let bid = if tick.bid > 0.0 {
                tick.bid
            } else {
                tick.price - 0.25
            };
            let ask = if tick.ask > 0.0 {
                tick.ask
            } else {
                tick.price + 0.25
            };
            if let Ok(mut b) = self.last_bid.lock() {
                *b = bid;
            }
            if let Ok(mut a) = self.last_ask.lock() {
                *a = ask;
            }
            let snapshot = p.snapshot_at(bid, ask, tick.timestamp_ms);
            let session_date = crate::session_date_from_timestamp_ms(tick.timestamp_ms);
            if let Ok(mut det) = self.detector.lock() {
                det.detect_into(
                    &snapshot,
                    tick.timestamp_ms,
                    &session_date,
                    minute,
                    &mut event_buffer,
                );
            }
            if let Ok(mut fe) = self.flow_emitter.lock() {
                fe.detect_into(
                    &p,
                    tick.timestamp_ms,
                    &session_date,
                    tick.price,
                    &mut event_buffer,
                );
            }
            snapshot
        };
        let new_events = event_buffer;
        self.ticks_ingested.fetch_add(1, Ordering::Release);
        if tick.timestamp_ms.is_finite() && tick.timestamp_ms > 0.0 {
            self.last_tick_timestamp_bits
                .store(tick.timestamp_ms.to_bits(), Ordering::Release);
        }
        self.events_detected
            .fetch_add(new_events.len() as u64, Ordering::Release);
        self.last_ingest_wall_ms.store(
            chrono::Utc::now().timestamp_millis().max(0) as u64,
            Ordering::Release,
        );
        if !new_events.is_empty() {
            if let Ok(mut recent) = self.recent_events.lock() {
                for ev in &new_events {
                    if let Ok(v) = serde_json::to_value(ev) {
                        recent.push(v);
                    }
                }
                let excess = recent.len().saturating_sub(RECENT_EVENTS_CAP);
                if excess > 0 {
                    recent.drain(0..excess);
                }
            }
        }
        Some(IngestOutcome {
            snapshot,
            new_events,
        })
    }

    /// Apply a compact book snapshot to the DOM-cluster detectors and emit events.
    ///
    /// Used by the live `.depth` path so Capsule-mandatory types are noted
    /// without waiting for the next trade. Fail-closed when the book is empty.
    /// Only [`FlowEventEmitter::detect_dom_into`] runs here — a full
    /// [`FlowEventEmitter::detect_into`] would steal tick-loop watermarks.
    pub fn apply_dom_update(
        &self,
        snapshot: &crate::depth::DomSnapshot,
        activity: &crate::depth::PullStackActivitySummary,
        timestamp_ms: f64,
    ) -> Option<IngestOutcome> {
        let mut event_buffer = Vec::new();
        let market_snapshot = {
            let mut p = self.pipelines.lock().ok()?;
            p.on_dom_feature(snapshot, activity, timestamp_ms);
            p.set_dom_summary(Some(crate::depth::build_dom_summary(snapshot, activity)));
            let bid = snapshot.best_bid.unwrap_or(0.0);
            let ask = snapshot.best_ask.unwrap_or(0.0);
            if bid > 0.0 {
                if let Ok(mut b) = self.last_bid.lock() {
                    *b = bid;
                }
            }
            if ask > 0.0 {
                if let Ok(mut a) = self.last_ask.lock() {
                    *a = ask;
                }
            }
            let market_snapshot = p.snapshot_at(bid, ask, timestamp_ms);
            let session_date = crate::session_date_from_timestamp_ms(timestamp_ms);
            if let Ok(mut fe) = self.flow_emitter.lock() {
                fe.detect_dom_into(&p, timestamp_ms, &session_date, &mut event_buffer);
            }
            market_snapshot
        };
        if timestamp_ms.is_finite() && timestamp_ms > 0.0 {
            self.last_tick_timestamp_bits
                .store(timestamp_ms.to_bits(), Ordering::Release);
        }
        self.events_detected
            .fetch_add(event_buffer.len() as u64, Ordering::Release);
        self.last_ingest_wall_ms.store(
            chrono::Utc::now().timestamp_millis().max(0) as u64,
            Ordering::Release,
        );
        if !event_buffer.is_empty() {
            if let Ok(mut recent) = self.recent_events.lock() {
                for ev in &event_buffer {
                    if let Ok(v) = serde_json::to_value(ev) {
                        recent.push(v);
                    }
                }
                let excess = recent.len().saturating_sub(RECENT_EVENTS_CAP);
                if excess > 0 {
                    recent.drain(0..excess);
                }
            }
        }
        Some(IngestOutcome {
            snapshot: market_snapshot,
            new_events: event_buffer,
        })
    }

    /// Poll a [`SourceProvider`], apply ticks, and publish lock-free state.
    pub fn poll_once(
        &self,
        provider: &mut dyn SourceProvider,
        max_ticks: usize,
    ) -> Result<usize, super::source::SourceError> {
        let ticks = provider.poll_ticks(max_ticks)?;
        let mut last_snap = None;
        for tick in &ticks {
            if let Some(out) = self.apply_tick(tick) {
                last_snap = Some(out.snapshot);
            }
        }
        let source_health = provider.health();
        self.publish(last_snap, &source_health);
        Ok(ticks.len())
    }

    /// Publish current coaching state (also callable after external apply).
    pub fn publish(
        &self,
        snapshot: Option<MarketState>,
        source_health: &super::source::SourceHealth,
    ) {
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let bid = self.last_bid.lock().ok().map(|g| *g).unwrap_or(0.0);
        let ask = self.last_ask.lock().ok().map(|g| *g).unwrap_or(0.0);
        let market_state = if let Some(snap) = snapshot {
            serde_json::to_value(snap).unwrap_or(Value::Null)
        } else if let Ok(p) = self.pipelines.lock() {
            if bid > 0.0 || ask > 0.0 {
                let snap = if let Some(ts) = self.last_tick_timestamp_ms() {
                    p.snapshot_at(bid.max(1e-9), ask.max(1e-9), ts)
                } else {
                    p.snapshot(bid.max(1e-9), ask.max(1e-9))
                };
                serde_json::to_value(snap).unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        } else {
            Value::Null
        };
        let data_time_ms = market_state
            .get("tapeLastTradeTimestampMs")
            .and_then(|v| v.as_f64())
            .or(source_health.last_tick_timestamp_ms);
        let backlog = source_health
            .scid_file_len_bytes
            .saturating_sub(source_health.scid_read_offset_bytes);
        let last_ingest = self.last_ingest_wall_ms.load(Ordering::Acquire);
        let stall_state = if !source_health.scid_exists {
            FeedStallState::Unavailable
        } else {
            EngineHealth::classify_stall(backlog, last_ingest, now_ms, STALL_THRESHOLD_MS)
        };
        let recent_events = self
            .recent_events
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        let degraded = market_state.is_null()
            || matches!(
                stall_state,
                FeedStallState::Stalled | FeedStallState::Unavailable
            );
        let health = EngineHealth {
            engine_alive: self.is_running(),
            engine_pid: std::process::id(),
            mode: self.mode_label.clone(),
            stall_state,
            scid_backlog_bytes: backlog,
            last_ingest_wall_ms: if last_ingest > 0 {
                Some(last_ingest)
            } else {
                None
            },
            last_publish_wall_ms: Some(now_ms),
            ticks_ingested: self.ticks_ingested.load(Ordering::Acquire),
            events_detected: self.events_detected.load(Ordering::Acquire),
            generation,
            source: source_health.clone(),
            note: match stall_state {
                FeedStallState::Stalled => Some(
                    "SCID ingest stalled (file growing, processed offset not advancing)".into(),
                ),
                FeedStallState::Behind => {
                    Some("SCID ingest behind (backlog present, still progressing)".into())
                }
                FeedStallState::Unavailable => {
                    Some("engine source unavailable — live coaching path is degraded".into())
                }
                FeedStallState::Ok => None,
            },
        };
        let primary_root = market_state
            .get("rootSymbol")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("NQ")
            .to_string();
        let mut by_symbol = std::collections::BTreeMap::new();
        if !market_state.is_null() {
            by_symbol.insert(primary_root.clone(), market_state.clone());
        }
        let state = PublishedEngineState {
            generation,
            engine_pid: std::process::id(),
            published_at_ms: now_ms as f64,
            data_time_ms,
            source_provider: self.provider_kind,
            market_state,
            recent_events,
            health,
            degraded,
            degraded_note: if degraded {
                Some(
                    "published engine state degraded — your rules/playbook evaluation should treat live structure as incomplete"
                        .into(),
                )
            } else {
                None
            },
            by_symbol,
            clock_ms: data_time_ms,
            primary_root,
        };
        self.last_publish_wall_ms.store(now_ms, Ordering::Release);
        self.published.store(state);
    }

    pub fn load_published(&self) -> Arc<PublishedEngineState> {
        self.published.load()
    }
}

/// Stable coaching-path fingerprint for embedded-vs-socket parity tests.
///
/// Compares the live coaching fields agents rely on (price/structure/delta),
/// not wall-clock publish metadata.
pub fn coaching_parity_fingerprint(state: &PublishedEngineState) -> Value {
    let ms = &state.market_state;
    serde_json::json!({
        "lastPrice": ms.get("lastPrice"),
        "vwap": ms.get("vwap"),
        "vaHigh": ms.get("vaHigh"),
        "vaLow": ms.get("vaLow"),
        "poc": ms.get("poc"),
        "dnvaHigh": ms.get("dnvaHigh"),
        "dnvaLow": ms.get("dnvaLow"),
        "dnp": ms.get("dnp"),
        "sessionDelta": ms.get("sessionDelta"),
        "cumulativeDelta": ms.get("cumulativeDelta"),
        "ibHigh": ms.get("ibHigh"),
        "ibLow": ms.get("ibLow"),
        "orHigh": ms.get("orHigh"),
        "orLow": ms.get("orLow"),
        "sessionHigh": ms.get("sessionHigh"),
        "sessionLow": ms.get("sessionLow"),
        "degraded": state.degraded,
        "eventCount": state.recent_events.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::source::FileProvider;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SCID_HEADER_SIZE: usize = 56;
    const SCID_RECORD_SIZE: usize = 40;
    const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;

    fn write_scid(ticks: &[(f64, f64, f64)]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        let mut header = vec![0u8; SCID_HEADER_SIZE];
        header[0..4].copy_from_slice(b"SCID");
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).unwrap();
        for &(ts_ms, price, volume) in ticks {
            let mut rec = [0u8; SCID_RECORD_SIZE];
            let sc_us = (ts_ms * 1_000.0) as i64 + SC_TO_UNIX_EPOCH_US;
            rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
            let ask = (price + 0.25) as f32;
            let bid = (price - 0.25) as f32;
            rec[12..16].copy_from_slice(&ask.to_le_bytes());
            rec[16..20].copy_from_slice(&bid.to_le_bytes());
            rec[20..24].copy_from_slice(&(price as f32).to_le_bytes());
            rec[28..32].copy_from_slice(&(volume as u32).to_le_bytes());
            rec[36..40].copy_from_slice(&1u32.to_le_bytes());
            file.write_all(&rec).unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[test]
    fn host_ingests_and_publishes_coaching_state() {
        // RTH window sample: 2024-01-02 10:00 ET ≈ 1704207600000 ms UTC
        let ts = 1_704_207_600_000.0;
        let scid = write_scid(&[
            (ts, 20_000.0, 1.0),
            (ts + 250.0, 20_000.25, 1.0),
            (ts + 500.0, 20_000.50, 1.0),
        ]);
        let mut provider = FileProvider::from_paths(scid.path(), vec![], 1.0);
        let host = EngineHost::new(SourceProviderKind::File, "embedded");
        let n = host.poll_once(&mut provider, 100).expect("poll");
        assert_eq!(n, 3);
        let pub_state = host.load_published();
        assert!(!pub_state.degraded);
        assert_eq!(pub_state.market_state["lastPrice"], 20_000.50);
        assert!(pub_state.generation >= 1);
    }

    #[test]
    fn embedded_and_direct_apply_share_coaching_fingerprint() {
        let ts = 1_704_207_600_000.0;
        let ticks = [
            SourceTick {
                timestamp_ms: ts,
                price: 20_000.0,
                volume: 1.0,
                bid: 19_999.75,
                ask: 20_000.25,
                side: TradeSide::Buy,
                root_symbol: None,
            },
            SourceTick {
                timestamp_ms: ts + 250.0,
                price: 20_000.25,
                volume: 2.0,
                bid: 20_000.0,
                ask: 20_000.50,
                side: TradeSide::Sell,
                root_symbol: None,
            },
        ];
        let a = EngineHost::new(SourceProviderKind::File, "embedded");
        let b = EngineHost::new(SourceProviderKind::File, "socket");
        for t in &ticks {
            a.apply_tick(t);
            b.apply_tick(t);
        }
        let health = super::super::source::SourceHealth {
            provider_kind: SourceProviderKind::File,
            scid_path: Some("fixture".into()),
            scid_exists: true,
            scid_file_len_bytes: 0,
            scid_read_offset_bytes: 0,
            depth_file_count: 0,
            depth_paths: vec![],
            last_tick_timestamp_ms: Some(ts + 250.0),
            ticks_emitted: 2,
            stubbed: false,
            note: None,
        };
        a.publish(None, &health);
        b.publish(None, &health);
        assert_eq!(
            coaching_parity_fingerprint(&a.load_published()),
            coaching_parity_fingerprint(&b.load_published())
        );
    }
}
