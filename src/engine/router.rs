//! MarketRouter v0: concurrent NQ + ES pipeline hosts on **one clock**.
//!
//! Each root owns a separate [`EngineHost`] (pipelines, detectors, session
//! state). Ticks from both FileProviders are merge-sorted by
//! `(timestamp_ms, root)` and applied in that order so cross-market reads are
//! co-recorded from the first row. Session classification (RTH / Globex) stays
//! per-tick on the owning lane — NQ Globex never contaminates ES RTH.
//!
//! Trust Ceiling stays **L3**. MarketRouter never places orders.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::feed::ContractMetadata;
use crate::pipelines::{EventDetector, FlowEventEmitter, PipelineEngine};

use super::health::{EngineHealth, FeedStallState};
use super::host::{EngineHost, IngestOutcome};
use super::published::{PublishedEngineState, PublishedStateStore};
use super::root::RouterRoot;
use super::source::{SourceError, SourceHealth, SourceProvider, SourceProviderKind, SourceTick};

/// Concurrent per-symbol pipeline host (NQ + ES) sharing one market-data clock.
pub struct MarketRouter {
    nq: EngineHost,
    es: EngineHost,
    primary: RouterRoot,
    published: PublishedStateStore,
    provider_kind: SourceProviderKind,
    mode_label: String,
    clock_ms_bits: AtomicU64,
}

impl MarketRouter {
    /// Two fresh lanes with placeholder contract metadata (`rootSymbol` set).
    pub fn new(
        primary: RouterRoot,
        provider_kind: SourceProviderKind,
        mode_label: impl Into<String>,
    ) -> Self {
        let nq = EngineHost::new_for_root(RouterRoot::Nq, provider_kind, "nq");
        let es = EngineHost::new_for_root(RouterRoot::Es, provider_kind, "es");
        Self {
            nq,
            es,
            primary,
            published: PublishedStateStore::new(),
            provider_kind,
            mode_label: mode_label.into(),
            clock_ms_bits: AtomicU64::new(0),
        }
    }

    /// Embed MarketRouter around MCP's existing NQ pipelines (embedded-engine fallback).
    ///
    /// The NQ lane shares the live coaching `PipelineEngine` so rollback stays
    /// behavior-parity with SIL-M2a. ES gets an isolated host.
    #[allow(clippy::too_many_arguments)]
    pub fn with_shared_nq(
        nq_pipelines: Arc<Mutex<PipelineEngine>>,
        nq_detector: Arc<Mutex<EventDetector>>,
        nq_flow_emitter: Arc<Mutex<FlowEventEmitter>>,
        nq_bid: Arc<Mutex<f64>>,
        nq_ask: Arc<Mutex<f64>>,
        primary: RouterRoot,
        provider_kind: SourceProviderKind,
        mode_label: impl Into<String>,
    ) -> Self {
        let nq = EngineHost::from_shared(
            nq_pipelines,
            nq_detector,
            nq_flow_emitter,
            nq_bid,
            nq_ask,
            provider_kind,
            "nq",
        );
        let es = EngineHost::new_for_root(RouterRoot::Es, provider_kind, "es");
        Self {
            nq,
            es,
            primary,
            published: PublishedStateStore::new(),
            provider_kind,
            mode_label: mode_label.into(),
            clock_ms_bits: AtomicU64::new(0),
        }
    }

    pub fn published_store(&self) -> PublishedStateStore {
        self.published.clone()
    }

    pub fn primary_root(&self) -> RouterRoot {
        self.primary
    }

    pub fn lane(&self, root: RouterRoot) -> &EngineHost {
        match root {
            RouterRoot::Nq => &self.nq,
            RouterRoot::Es => &self.es,
        }
    }

    pub fn nq_host(&self) -> &EngineHost {
        &self.nq
    }

    pub fn es_host(&self) -> &EngineHost {
        &self.es
    }

    pub fn set_contract_metadata(&self, root: RouterRoot, metadata: ContractMetadata) {
        self.lane(root).set_contract_metadata(metadata);
    }

    pub fn mark_stopped(&self) {
        self.nq.mark_stopped();
        self.es.mark_stopped();
    }

    /// Aligned clock: max market timestamp across lanes (epoch ms).
    ///
    /// Prefers the merge-sorted apply clock, then each lane's last tick / tape
    /// last trade so the embedded shared-NQ path still contributes.
    pub fn clock_ms(&self) -> Option<f64> {
        let bits = self.clock_ms_bits.load(Ordering::Acquire);
        let stored = if bits == 0 {
            None
        } else {
            Some(f64::from_bits(bits))
        };
        max_opt_ts(
            max_opt_ts(stored, self.nq.market_time_ms()),
            self.es.market_time_ms(),
        )
    }

    /// Apply one tick to the owning symbol's host only (no cross-lane session mix).
    pub fn apply_tick(&self, root: RouterRoot, tick: &SourceTick) -> Option<IngestOutcome> {
        let out = self.lane(root).apply_tick(tick)?;
        self.advance_clock(tick.timestamp_ms);
        Some(out)
    }

    /// Deterministic one-clock apply: merge-sort by `(timestamp_ms, root)` then apply.
    pub fn apply_merged(&self, mut ticks: Vec<(RouterRoot, SourceTick)>) -> usize {
        sort_ticks_one_clock(&mut ticks);
        let mut n = 0usize;
        for (root, tick) in &ticks {
            if self.apply_tick(*root, tick).is_some() {
                n += 1;
            }
        }
        n
    }

    /// Poll each provider, merge-sort onto one clock, apply, publish combined state.
    pub fn poll_once(
        &self,
        providers: &mut BTreeMap<RouterRoot, Box<dyn SourceProvider>>,
        max_ticks_per_root: usize,
    ) -> Result<usize, SourceError> {
        let mut batch = Vec::new();
        let mut health = BTreeMap::new();
        for root in RouterRoot::ALL {
            let Some(provider) = providers.get_mut(&root) else {
                continue;
            };
            match provider.poll_ticks(max_ticks_per_root.max(1)) {
                Ok(ticks) => {
                    for tick in ticks {
                        batch.push((root, tick));
                    }
                    health.insert(root, provider.health());
                }
                Err(err) => {
                    tracing::warn!(
                        root = root.as_str(),
                        error = %err,
                        "market_router.poll_error"
                    );
                    health.insert(root, provider.health());
                }
            }
        }
        let n = batch.len();
        self.apply_merged(batch);
        self.publish(&health);
        Ok(n)
    }

    /// Lock-free combined publish: primary `market_state` for coaching parity,
    /// `by_symbol` for both roots, `clock_ms` for the aligned clock.
    pub fn publish(&self, health_by_root: &BTreeMap<RouterRoot, SourceHealth>) {
        let mut by_symbol = BTreeMap::new();
        let mut recent_events = Vec::new();
        for root in RouterRoot::ALL {
            let snap = self.lane(root).snapshot_market_state();
            if !snap.is_null() {
                by_symbol.insert(root.as_str().to_string(), snap);
            }
            for mut ev in self.lane(root).recent_events_json() {
                if let Some(obj) = ev.as_object_mut() {
                    obj.entry("rootSymbol")
                        .or_insert_with(|| Value::String(root.as_str().to_string()));
                }
                recent_events.push(ev);
            }
        }
        let primary_state = by_symbol
            .get(self.primary.as_str())
            .cloned()
            .or_else(|| by_symbol.values().next().cloned())
            .unwrap_or(Value::Null);
        let clock_ms = self.clock_ms();
        let data_time_ms = clock_ms.or_else(|| {
            primary_state
                .get("tapeLastTradeTimestampMs")
                .and_then(|v| v.as_f64())
        });
        let primary_health = health_by_root.get(&self.primary);
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let generation = self.nq.next_generation();
        let last_ingest = self
            .nq
            .last_ingest_wall_ms()
            .max(self.es.last_ingest_wall_ms());
        let backlog = primary_health
            .map(|h| {
                h.scid_file_len_bytes
                    .saturating_sub(h.scid_read_offset_bytes)
            })
            .unwrap_or(0);
        let scid_exists = primary_health.map(|h| h.scid_exists).unwrap_or(false);
        let stall_state = if !scid_exists && primary_health.is_some() {
            FeedStallState::Unavailable
        } else {
            EngineHealth::classify_stall(backlog, last_ingest, now_ms, 10_000)
        };
        let es_missing = !by_symbol.contains_key(RouterRoot::Es.as_str());
        let primary_null = primary_state.is_null();
        let degraded = primary_null
            || matches!(
                stall_state,
                FeedStallState::Stalled | FeedStallState::Unavailable
            );
        let mut note = match stall_state {
            FeedStallState::Stalled => {
                Some("SCID ingest stalled (file growing, processed offset not advancing)".into())
            }
            FeedStallState::Behind => {
                Some("SCID ingest behind (backlog present, still progressing)".into())
            }
            FeedStallState::Unavailable => {
                Some("engine source unavailable — live coaching path is degraded".into())
            }
            FeedStallState::Ok => None,
        };
        if es_missing && note.is_none() {
            note = Some(
                "MarketRouter ES lane has no snapshot yet; NQ coaching path is unchanged".into(),
            );
        }
        let source = primary_health.cloned().unwrap_or_else(|| SourceHealth {
            provider_kind: self.provider_kind,
            scid_path: None,
            scid_exists: false,
            scid_file_len_bytes: 0,
            scid_read_offset_bytes: 0,
            depth_file_count: 0,
            depth_paths: Vec::new(),
            last_tick_timestamp_ms: clock_ms,
            ticks_emitted: 0,
            stubbed: false,
            note: Some("MarketRouter primary source health unavailable".into()),
        });
        let health = EngineHealth {
            engine_alive: self.nq.is_running() && self.es.is_running(),
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
            ticks_ingested: self.nq.ticks_ingested() + self.es.ticks_ingested(),
            events_detected: self.nq.events_detected() + self.es.events_detected(),
            generation,
            source,
            note: note.clone(),
        };
        let state = PublishedEngineState {
            generation,
            engine_pid: std::process::id(),
            published_at_ms: now_ms as f64,
            data_time_ms,
            source_provider: self.provider_kind,
            market_state: primary_state,
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
            clock_ms,
            primary_root: self.primary.as_str().to_string(),
        };
        self.published.store(state);
    }

    /// Current per-root MarketState JSON (null omitted).
    pub fn snapshots_by_symbol(&self) -> BTreeMap<String, Value> {
        let mut out = BTreeMap::new();
        for root in RouterRoot::ALL {
            let snap = self.lane(root).snapshot_market_state();
            if !snap.is_null() {
                out.insert(root.as_str().to_string(), snap);
            }
        }
        out
    }

    fn advance_clock(&self, timestamp_ms: f64) {
        if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 {
            return;
        }
        let new_bits = timestamp_ms.to_bits();
        let mut current = self.clock_ms_bits.load(Ordering::Acquire);
        loop {
            let current_ts = f64::from_bits(current);
            if current != 0 && current_ts >= timestamp_ms {
                return;
            }
            match self.clock_ms_bits.compare_exchange_weak(
                current,
                new_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(seen) => current = seen,
            }
        }
    }
}

fn max_opt_ts(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) if x.is_finite() && y.is_finite() => Some(x.max(y)),
        (Some(x), _) if x.is_finite() => Some(x),
        (_, Some(y)) if y.is_finite() => Some(y),
        _ => None,
    }
}

/// Stable one-clock order: market time, then NQ before ES on a timestamp tie.
pub fn sort_ticks_one_clock(ticks: &mut [(RouterRoot, SourceTick)]) {
    ticks.sort_by(|a, b| {
        a.1.timestamp_ms
            .partial_cmp(&b.1.timestamp_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::source::FileProvider;
    use crate::feed::TradeSide;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SCID_HEADER_SIZE: usize = 56;
    const SCID_RECORD_SIZE: usize = 40;
    const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;

    /// 2024-01-02 10:00 ET (RTH).
    const RTH_TS: f64 = 1_704_207_600_000.0;
    /// 2024-01-02 20:00 ET (Globex).
    const GLOBEX_TS: f64 = 1_704_243_600_000.0;

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

    fn tick(ts: f64, price: f64) -> SourceTick {
        SourceTick {
            timestamp_ms: ts,
            price,
            volume: 1.0,
            bid: price - 0.25,
            ask: price + 0.25,
            side: TradeSide::Buy,
            root_symbol: None,
        }
    }

    fn aligned_fixture() -> Vec<(RouterRoot, SourceTick)> {
        vec![
            (RouterRoot::Nq, tick(RTH_TS, 20_000.0)),
            (RouterRoot::Es, tick(RTH_TS + 100.0, 5_000.0)),
            (RouterRoot::Nq, tick(RTH_TS + 250.0, 20_000.25)),
            (RouterRoot::Es, tick(RTH_TS + 400.0, 5_000.25)),
            (RouterRoot::Nq, tick(RTH_TS + 500.0, 20_000.50)),
        ]
    }

    #[test]
    fn concurrent_matches_sequential_same_clock() {
        let ticks = aligned_fixture();
        let concurrent = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "test");
        concurrent.apply_merged(ticks.clone());

        let sequential = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "test");
        let mut ordered = ticks;
        sort_ticks_one_clock(&mut ordered);
        for (root, tick) in &ordered {
            sequential.apply_tick(*root, tick);
        }

        let c_nq = concurrent.nq_host().snapshot_market_state();
        let s_nq = sequential.nq_host().snapshot_market_state();
        let c_es = concurrent.es_host().snapshot_market_state();
        let s_es = sequential.es_host().snapshot_market_state();
        assert_eq!(c_nq["lastPrice"], s_nq["lastPrice"]);
        assert_eq!(c_es["lastPrice"], s_es["lastPrice"]);
        assert_eq!(c_nq["sessionType"], s_nq["sessionType"]);
        assert_eq!(c_es["sessionType"], s_es["sessionType"]);
        assert_eq!(concurrent.clock_ms(), sequential.clock_ms());
        assert_eq!(concurrent.clock_ms(), Some(RTH_TS + 500.0));
    }

    #[test]
    fn session_scopes_do_not_mix_across_symbols() {
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "test");
        router.apply_merged(vec![
            (RouterRoot::Nq, tick(RTH_TS, 20_000.0)),
            (RouterRoot::Nq, tick(RTH_TS + 250.0, 20_001.0)),
            (RouterRoot::Es, tick(GLOBEX_TS, 5_000.0)),
            (RouterRoot::Es, tick(GLOBEX_TS + 250.0, 5_001.0)),
        ]);
        let nq = router.nq_host().snapshot_market_state();
        let es = router.es_host().snapshot_market_state();
        assert_eq!(nq["rootSymbol"], "NQ");
        assert_eq!(es["rootSymbol"], "ES");
        assert_eq!(nq["sessionType"], "RTH");
        assert_eq!(es["sessionType"], "Globex");
        assert_eq!(nq["lastPrice"], 20_001.0);
        assert_eq!(es["lastPrice"], 5_001.0);
        // Prices must not leak across lanes.
        assert_ne!(nq["lastPrice"], es["lastPrice"]);
        assert!(nq["sessionHigh"].as_f64().unwrap() >= 20_000.0);
        assert!(es["sessionHigh"].as_f64().unwrap() < 6_000.0);
    }

    #[test]
    fn poll_once_publishes_both_symbols_on_one_clock() {
        let nq_file = write_scid(&[(RTH_TS, 20_000.0, 1.0), (RTH_TS + 500.0, 20_000.50, 1.0)]);
        let es_file = write_scid(&[
            (RTH_TS + 100.0, 5_000.0, 1.0),
            (RTH_TS + 400.0, 5_000.25, 1.0),
        ]);
        let mut providers: BTreeMap<RouterRoot, Box<dyn SourceProvider>> = BTreeMap::new();
        providers.insert(
            RouterRoot::Nq,
            Box::new(FileProvider::from_paths(nq_file.path(), vec![], 1.0)),
        );
        providers.insert(
            RouterRoot::Es,
            Box::new(FileProvider::from_paths(es_file.path(), vec![], 1.0)),
        );
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "embedded");
        let n = router.poll_once(&mut providers, 100).expect("poll");
        assert_eq!(n, 4);
        let published = router.published_store().load();
        assert!(!published.degraded);
        assert_eq!(published.primary_root, "NQ");
        assert_eq!(published.market_state["lastPrice"], 20_000.50);
        assert_eq!(published.by_symbol["NQ"]["lastPrice"], 20_000.50);
        assert_eq!(published.by_symbol["ES"]["lastPrice"], 5_000.25);
        assert_eq!(published.clock_ms, Some(RTH_TS + 500.0));
        assert_eq!(published.by_symbol["NQ"]["sessionType"], "RTH");
        assert_eq!(published.by_symbol["ES"]["sessionType"], "RTH");
    }

    #[test]
    fn missing_es_does_not_degrade_nq_coaching_path() {
        let nq_file = write_scid(&[(RTH_TS, 20_000.0, 1.0)]);
        let mut providers: BTreeMap<RouterRoot, Box<dyn SourceProvider>> = BTreeMap::new();
        providers.insert(
            RouterRoot::Nq,
            Box::new(FileProvider::from_paths(nq_file.path(), vec![], 1.0)),
        );
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "embedded");
        router.poll_once(&mut providers, 100).unwrap();
        let published = router.published_store().load();
        assert!(!published.degraded);
        assert_eq!(published.market_state["lastPrice"], 20_000.0);
        assert!(!published.by_symbol.contains_key("ES"));
    }

    #[test]
    fn shared_nq_tape_contributes_to_aligned_clock() {
        let nq_pipelines = Arc::new(Mutex::new(PipelineEngine::new()));
        let router = MarketRouter::with_shared_nq(
            Arc::clone(&nq_pipelines),
            Arc::new(Mutex::new(EventDetector::new())),
            Arc::new(Mutex::new(FlowEventEmitter::new())),
            Arc::new(Mutex::new(20_000.0)),
            Arc::new(Mutex::new(20_000.25)),
            RouterRoot::Nq,
            SourceProviderKind::File,
            "embedded",
        );
        {
            let mut p = nq_pipelines.lock().unwrap();
            p.on_trade_with_timestamp(20_000.0, 1.0, true, 30, RTH_TS + 800.0);
        }
        router.apply_tick(RouterRoot::Es, &tick(RTH_TS + 100.0, 5_000.0));
        assert_eq!(router.clock_ms(), Some(RTH_TS + 800.0));
        let nq = router.nq_host().snapshot_market_state();
        assert_eq!(nq["sessionType"], "RTH");
        assert_eq!(nq["lastPrice"], 20_000.0);
    }
}
