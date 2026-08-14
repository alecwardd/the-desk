//! MarketRouter v0: concurrent NQ + ES pipeline hosts on **one clock**.
//!
//! Each root owns a separate [`EngineHost`] (pipelines, detectors, session
//! state). Ticks from both FileProviders are merge-sorted by
//! `(timestamp_ms, root)` and applied in that order so cross-market reads are
//! co-recorded from the first row. Session classification (RTH / Globex) stays
//! per-tick on the owning lane — NQ Globex never contaminates ES RTH.
//!
//! Trust Ceiling stays **L3**. MarketRouter never places orders.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::attention::persist_event_stream_attention;
use crate::catalog::{
    collapse_events_latest_per_dedup, kernel_event_from_db_row, EVENT_LIFECYCLE_TTL_MS,
};
use crate::db::{market_event_dedup_id, market_event_id, Database, DbError, JournalFrameRecord};
use crate::feed::ContractMetadata;
use crate::pipelines::{EventDetector, FlowEventEmitter, MarketEvent, MarketState, PipelineEngine};
use serde_json::Value;

use super::capsule::{
    compact_capsule_sample, event_may_open_capsule, should_open_capsule, snapshot_session_type,
    CapsuleRing, PendingCapsule,
};
use super::cold_frames::ColdFrameStore;

use super::health::{EngineHealth, FeedStallState};
use super::host::{EngineHost, IngestOutcome};
use super::journal::{
    journal_frame_from_snapshot, journal_frame_second, persist_journal_observation,
    JournalPersistStats,
};
use super::published::{PublishedEngineState, PublishedStateStore};
use super::root::RouterRoot;
use super::source::{SourceError, SourceHealth, SourceProvider, SourceProviderKind, SourceTick};

/// Bound in-memory transition-event queue when persist is delayed.
const PENDING_JOURNAL_MAX_EVENTS: usize = 8_192;
/// Bound in-memory Journal Frames waiting to persist (SQLite failure or cold retry).
pub const PENDING_JOURNAL_MAX_FRAMES: usize = 8_192;
/// Bound in-memory Capsules waiting for an after-window or a trigger row.
const PENDING_CAPSULES_MAX: usize = 256;

/// Concurrent per-symbol pipeline host (NQ + ES) sharing one market-data clock.
pub struct MarketRouter {
    nq: EngineHost,
    es: EngineHost,
    primary: RouterRoot,
    published: PublishedStateStore,
    provider_kind: SourceProviderKind,
    mode_label: String,
    clock_ms_bits: AtomicU64,
    journal_enabled: AtomicBool,
    pending_journal_events: Mutex<Vec<(RouterRoot, MarketEvent)>>,
    pending_journal_frames: Mutex<BTreeMap<(i64, RouterRoot), JournalFrameRecord>>,
    journal_second_clock: Mutex<BTreeMap<i64, f64>>,
    capsule_rings: Mutex<BTreeMap<RouterRoot, CapsuleRing>>,
    pending_capsules: Mutex<Vec<PendingCapsule>>,
    /// Optional SIL-M3d cold dump target. `None` in unit tests that only
    /// exercise the SQLite hot window.
    cold_frames: Mutex<Option<ColdFrameStore>>,
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
            journal_enabled: AtomicBool::new(true),
            pending_journal_events: Mutex::new(Vec::new()),
            pending_journal_frames: Mutex::new(BTreeMap::new()),
            journal_second_clock: Mutex::new(BTreeMap::new()),
            capsule_rings: Mutex::new(BTreeMap::new()),
            pending_capsules: Mutex::new(Vec::new()),
            cold_frames: Mutex::new(None),
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
            journal_enabled: AtomicBool::new(true),
            pending_journal_events: Mutex::new(Vec::new()),
            pending_journal_frames: Mutex::new(BTreeMap::new()),
            journal_second_clock: Mutex::new(BTreeMap::new()),
            capsule_rings: Mutex::new(BTreeMap::new()),
            pending_capsules: Mutex::new(Vec::new()),
            cold_frames: Mutex::new(None),
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

    /// Queue Journal Frames / transition rows. Off when the host has no journal sink.
    pub fn set_journal_enabled(&self, enabled: bool) {
        self.journal_enabled.store(enabled, Ordering::Release);
    }

    /// Attach the SIL-M3d cold dump store. Hot SQLite writes continue regardless.
    pub fn set_cold_frame_store(&self, store: ColdFrameStore) {
        if let Ok(mut slot) = self.cold_frames.lock() {
            *slot = Some(store);
        }
    }

    pub fn set_contract_metadata(&self, root: RouterRoot, metadata: ContractMetadata) {
        self.lane(root).set_contract_metadata(metadata);
    }

    pub fn mark_stopped(&self) {
        self.nq.mark_stopped();
        self.es.mark_stopped();
    }

    /// Mark lanes stopped and persist Journal Frames + Capsules (session/feed end).
    pub fn flush_journal_on_stop(&self, db: &Database) -> Result<JournalPersistStats, DbError> {
        self.mark_stopped();
        let stats = self.persist_journal(db)?;
        let store = self.cold_frames.lock().ok().and_then(|slot| slot.clone());
        if let Some(store) = store {
            if let Err(err) = store.compact() {
                tracing::warn!(error = %err, "market_router.cold_frame_compact");
            }
        }
        Ok(stats)
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
        if !out.new_events.is_empty() {
            self.note_transition_events(root, &out.new_events);
        }
        self.queue_journal_frames();
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
        sort_recent_events_one_clock(&mut recent_events);
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

    /// Current per-root MarketState keyed by [`RouterRoot`].
    pub fn snapshots_by_root(&self) -> BTreeMap<RouterRoot, Value> {
        let mut out = BTreeMap::new();
        for root in RouterRoot::ALL {
            let snap = self.lane(root).snapshot_market_state();
            if !snap.is_null() {
                out.insert(root, snap);
            }
        }
        out
    }

    /// Queue transition event rows (embedded NQ ingest that does not call `apply_tick`).
    pub fn note_transition_events(&self, root: RouterRoot, events: &[MarketEvent]) {
        if events.is_empty() || !self.journal_enabled.load(Ordering::Acquire) {
            return;
        }
        self.sample_capsule_rings();
        if let Ok(mut pending) = self.pending_journal_events.lock() {
            pending.extend(events.iter().cloned().map(|event| (root, event)));
            let excess = pending.len().saturating_sub(PENDING_JOURNAL_MAX_EVENTS);
            if excess > 0 {
                pending.drain(0..excess);
            }
        }
        self.maybe_open_pending_capsules(root, events);
    }

    /// Capture 1 Hz Journal Frames for every root that printed in its lane second.
    ///
    /// Each root is keyed by `floor(lane_market_time_ms / 1000)`, not the max
    /// MarketRouter clock — a later ES print must not copy last-known NQ state
    /// onto a second NQ did not print. Roots that print in the same second share
    /// the first pinned `clock_ms` of that second. Later 250 ms ticks do not
    /// replace the frame (`or_insert` + DB `INSERT OR IGNORE`).
    ///
    /// Snapshots are taken without holding `pending_journal_frames` so embedded
    /// NQ ingest (`pipelines` lock) and ES `apply_tick` cannot deadlock.
    pub fn queue_journal_frames(&self) {
        if !self.journal_enabled.load(Ordering::Acquire) {
            return;
        }
        self.sample_capsule_rings();
        let mut needed: Vec<(i64, RouterRoot, f64)> = Vec::new();
        for root in RouterRoot::ALL {
            let Some(mt) = self.lane(root).market_time_ms() else {
                continue;
            };
            let Some(frame_second) = journal_frame_second(mt) else {
                continue;
            };
            needed.push((frame_second, root, mt));
        }
        if needed.is_empty() {
            return;
        }

        let missing = {
            let Ok(pending) = self.pending_journal_frames.lock() else {
                return;
            };
            needed
                .into_iter()
                .filter(|(sec, root, _)| !pending.contains_key(&(*sec, *root)))
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return;
        }

        let mut built = Vec::with_capacity(missing.len());
        for (frame_second, root, mt) in missing {
            let snap = self.lane(root).snapshot_market_state();
            if snap.is_null() {
                continue;
            }
            built.push((frame_second, root, mt, snap));
        }
        if built.is_empty() {
            return;
        }

        let Ok(mut clocks) = self.journal_second_clock.lock() else {
            return;
        };
        let Ok(mut pending) = self.pending_journal_frames.lock() else {
            return;
        };
        for (frame_second, root, mt, snap) in built {
            let aligned = *clocks.entry(frame_second).or_insert(mt);
            pending
                .entry((frame_second, root))
                .or_insert_with(|| journal_frame_from_snapshot(aligned, frame_second, root, &snap));
        }
        if let Some(max_sec) = clocks.keys().copied().max() {
            clocks.retain(|&sec, _| sec + 1 >= max_sec);
        }
    }

    /// Persist queued Journal Frames + transition events (INSERT OR IGNORE).
    ///
    /// Cold dumps are best-effort: a filesystem error re-queues frames (bounded)
    /// and this still returns `Ok` so attention and Capsules run.
    pub fn persist_journal(&self, db: &Database) -> Result<JournalPersistStats, DbError> {
        if !self.journal_enabled.load(Ordering::Acquire) {
            return Ok(JournalPersistStats::default());
        }
        self.queue_journal_frames();
        let frames: Vec<JournalFrameRecord> = self
            .pending_journal_frames
            .lock()
            .map(|mut g| std::mem::take(&mut *g).into_values().collect())
            .unwrap_or_default();
        let events: Vec<(RouterRoot, MarketEvent)> = self
            .pending_journal_events
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        match persist_journal_observation(db, &frames, &events) {
            Ok(mut stats) => {
                let cold_store = self.cold_frames.lock().ok().and_then(|slot| slot.clone());
                if let Some(store) = cold_store {
                    match store.upsert_frames(&frames) {
                        Ok(n) => stats.cold_frames_written = n,
                        Err(err) => {
                            tracing::warn!(error = %err, "market_router.cold_frame_dump");
                            self.restore_pending_frames(frames);
                        }
                    }
                }
                let clock = self.clock_ms().unwrap_or(0.0);
                if clock > 0.0 {
                    if let Err(err) = db.expire_event_lifecycles(clock, EVENT_LIFECYCLE_TTL_MS) {
                        tracing::warn!(error = %err, "market_router.event_lifecycle_expire");
                    }
                }
                if !events.is_empty() {
                    persist_router_event_attention(db, self, &events, clock);
                }
                match self.persist_capsules(db, clock) {
                    Ok((opened, finalized)) => {
                        stats.capsules_opened = opened;
                        stats.capsules_finalized = finalized;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "market_router.capsule_persist");
                    }
                }
                Ok(stats)
            }
            Err(err) => {
                self.restore_pending_journal(frames, events);
                Err(err)
            }
        }
    }

    fn sample_capsule_rings(&self) {
        if !self.journal_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut pushed = false;
        for root in RouterRoot::ALL {
            let Some(mt) = self.lane(root).market_time_ms() else {
                continue;
            };
            let need = {
                let Ok(rings) = self.capsule_rings.lock() else {
                    continue;
                };
                rings.get(&root).map(|r| r.needs_sample(mt)).unwrap_or(true)
            };
            if !need {
                continue;
            }
            let snap = self.lane(root).snapshot_market_state();
            if snap.is_null() {
                continue;
            }
            let session = snapshot_session_type(&snap);
            let compact = compact_capsule_sample(mt, root, &snap);
            if let Ok(mut rings) = self.capsule_rings.lock() {
                if rings.entry(root).or_default().push(mt, &session, compact) {
                    pushed = true;
                }
            }
        }
        if pushed {
            self.ingest_pending_capsules_from_rings();
        }
    }

    fn ingest_pending_capsules_from_rings(&self) {
        let Ok(rings) = self.capsule_rings.lock() else {
            return;
        };
        let Ok(mut pending) = self.pending_capsules.lock() else {
            return;
        };
        for cap in pending.iter_mut() {
            if let Some(ring) = rings.get(&cap.root) {
                cap.ingest_ring(ring);
            }
        }
    }

    fn maybe_open_pending_capsules(&self, root: RouterRoot, events: &[MarketEvent]) {
        if events.is_empty() || !self.journal_enabled.load(Ordering::Acquire) {
            return;
        }
        if !events.iter().any(event_may_open_capsule) {
            return;
        }
        let Ok(rings) = self.capsule_rings.lock() else {
            return;
        };
        let empty = CapsuleRing::default();
        let ring = rings.get(&root).unwrap_or(&empty);
        let Ok(mut pending) = self.pending_capsules.lock() else {
            return;
        };
        for event in events {
            if !event_may_open_capsule(event) {
                continue;
            }
            let trigger = market_event_id(event);
            if pending.iter().any(|p| p.trigger_identity_id == trigger) {
                continue;
            }
            let dedup = market_event_dedup_id(event, Some(root.as_str()));
            if pending
                .iter()
                .any(|p| p.dedup_identity_id == dedup && p.is_pending())
            {
                continue;
            }
            pending.push(PendingCapsule::open_from_ring(root, event, ring));
        }
    }

    fn persist_capsules(&self, db: &Database, clock_ms: f64) -> Result<(usize, usize), DbError> {
        self.sample_capsule_rings();
        self.ingest_pending_capsules_from_rings();
        let stopped = !self.nq.is_running() && !self.es.is_running();
        let pending = match self.pending_capsules.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => return Ok((0, 0)),
        };
        let keep_ids: Vec<String> = pending.iter().map(|cap| cap.id.clone()).collect();
        if let Err(err) = db.finalize_orphaned_pending_capsules(&keep_ids, clock_ms) {
            if let Ok(mut g) = self.pending_capsules.lock() {
                let mut restore = pending;
                restore.append(&mut *g);
                *g = merge_pending_capsules(restore);
            }
            return Err(err);
        }
        let mut opened = 0usize;
        let mut finalized = 0usize;
        let mut keep = Vec::new();
        let mut persist_err: Option<DbError> = None;
        let mut remaining = pending.into_iter();
        for mut cap in remaining.by_ref() {
            let lane_clock = self.lane(cap.root).market_time_ms().unwrap_or(0.0);
            let finalize_clock = if lane_clock > 0.0 {
                lane_clock.max(cap.observed_clock_ms())
            } else {
                clock_ms.max(cap.observed_clock_ms())
            };
            cap.finalize(finalize_clock, stopped);
            let lifecycle = match db.market_event_lifecycle_for_identity(&cap.trigger_identity_id) {
                Ok(v) => v,
                Err(err) => {
                    keep.push(cap);
                    persist_err = Some(err);
                    break;
                }
            };
            let existed = match db.capsule_exists(&cap.id) {
                Ok(v) => v,
                Err(err) => {
                    keep.push(cap);
                    persist_err = Some(err);
                    break;
                }
            };
            // `open` writes the first dump. `updated` with no row yet still writes
            // when this condition has no Capsule (backfill / same-batch stamp).
            // A later occurrence of the same dedup must not spawn another Capsule.
            let should_write = match lifecycle.as_deref() {
                Some(state) if should_open_capsule(&cap.event_type, state) => true,
                Some("updated") if !existed => {
                    match db.capsule_exists_for_dedup(&cap.dedup_identity_id) {
                        Ok(has_dedup) => !has_dedup,
                        Err(err) => {
                            keep.push(cap);
                            persist_err = Some(err);
                            break;
                        }
                    }
                }
                None if !existed && !cap.is_terminal() => {
                    keep.push(cap);
                    continue;
                }
                _ => true,
            };
            if !should_write {
                tracing::warn!(
                    trigger = %cap.trigger_identity_id,
                    lifecycle = lifecycle.as_deref().unwrap_or(""),
                    "market_router.capsule_skipped_not_open"
                );
                continue;
            }
            if !existed || cap.is_terminal() {
                let rec = cap.to_record(finalize_clock.max(cap.event_timestamp_ms));
                if let Err(err) = db.upsert_capsule(&rec) {
                    keep.push(cap);
                    persist_err = Some(err);
                    break;
                }
                if !existed {
                    opened += 1;
                }
            }
            if cap.is_terminal() {
                finalized += 1;
            } else {
                keep.push(cap);
            }
        }
        // Drain+break would drop unyielded Capsules; keep them for the next persist.
        keep.extend(remaining);
        if let Ok(mut g) = self.pending_capsules.lock() {
            keep.append(&mut *g);
            *g = merge_pending_capsules(keep);
        }
        match persist_err {
            Some(err) => Err(err),
            None => Ok((opened, finalized)),
        }
    }

    fn restore_pending_journal(
        &self,
        frames: Vec<JournalFrameRecord>,
        events: Vec<(RouterRoot, MarketEvent)>,
    ) {
        self.restore_pending_frames(frames);
        if let Ok(mut pending) = self.pending_journal_events.lock() {
            let mut restored = events;
            restored.append(&mut *pending);
            *pending = restored;
        }
    }

    fn restore_pending_frames(&self, frames: Vec<JournalFrameRecord>) {
        if let Ok(mut pending) = self.pending_journal_frames.lock() {
            for frame in frames {
                if let Ok(root) = RouterRoot::parse(&frame.root_symbol) {
                    pending.entry((frame.frame_second, root)).or_insert(frame);
                }
            }
            cap_pending_journal_map(&mut pending);
        }
    }

    /// Frames waiting to persist (tests / bounded-retry visibility).
    pub fn pending_journal_frame_count(&self) -> usize {
        self.pending_journal_frames
            .lock()
            .map(|g| g.len())
            .unwrap_or(0)
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

fn cap_pending_journal_map(pending: &mut BTreeMap<(i64, RouterRoot), JournalFrameRecord>) {
    while pending.len() > PENDING_JOURNAL_MAX_FRAMES {
        pending.pop_first();
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

fn persist_router_event_attention(
    db: &Database,
    router: &MarketRouter,
    events: &[(RouterRoot, MarketEvent)],
    clock_ms: f64,
) {
    if events.is_empty() {
        return;
    }
    let timestamp_ms = if clock_ms > 0.0 {
        clock_ms
    } else {
        events
            .iter()
            .map(|(_, e)| e.timestamp_ms)
            .fold(0.0_f64, f64::max)
    };
    let pending_ids: Vec<String> = events
        .iter()
        .map(|(root, event)| crate::db::market_event_dedup_id(event, Some(root.as_str())))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let rows = match db.list_market_events_by_dedup_ids(&pending_ids) {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "market_router.attention_event_read");
            return;
        }
    };
    let kernel_events =
        collapse_events_latest_per_dedup(rows.iter().map(kernel_event_from_db_row).collect());
    let mut by_root: BTreeMap<Option<RouterRoot>, Vec<crate::catalog::KernelEvent>> =
        BTreeMap::new();
    for event in kernel_events {
        let root = event
            .root_symbol
            .as_deref()
            .or(event.frame_ref.root_symbol.as_deref())
            .and_then(|s| match RouterRoot::parse(s) {
                Ok(root) => Some(root),
                Err(_) => {
                    tracing::warn!(root = s, "market_router.attention_unknown_root");
                    None
                }
            });
        by_root.entry(root).or_default().push(event);
    }
    if by_root.is_empty() {
        return;
    }
    for (root, kernel_events) in by_root {
        let snapshot = root.and_then(|root| {
            serde_json::from_value::<MarketState>(router.lane(root).snapshot_market_state()).ok()
        });
        if let Err(err) = persist_event_stream_attention(
            db,
            &kernel_events,
            snapshot.as_ref(),
            timestamp_ms,
            "live",
            None,
        ) {
            tracing::warn!(
                root = root.map(RouterRoot::as_str).unwrap_or("unknown"),
                error = %err,
                "market_router.attention_upsert"
            );
        }
    }
}

/// Keep the fuller Capsule when persist raced an open of the same trigger.
fn merge_pending_capsules(caps: Vec<PendingCapsule>) -> Vec<PendingCapsule> {
    bound_pending_capsules(collapse_pending_by_trigger(caps), PENDING_CAPSULES_MAX)
}

fn collapse_pending_by_trigger(mut caps: Vec<PendingCapsule>) -> Vec<PendingCapsule> {
    caps.sort_by(|a, b| a.trigger_identity_id.cmp(&b.trigger_identity_id));
    let mut out: Vec<PendingCapsule> = Vec::with_capacity(caps.len());
    for cap in caps {
        if let Some(prev) = out.last_mut() {
            if prev.trigger_identity_id == cap.trigger_identity_id {
                if cap.samples.len() > prev.samples.len()
                    || (cap.samples.len() == prev.samples.len()
                        && cap.observed_clock_ms() > prev.observed_clock_ms())
                {
                    *prev = cap;
                }
                continue;
            }
        }
        out.push(cap);
    }
    out
}

/// Evict oldest, lowest-sample, non-terminal Capsules first — not hash order.
fn bound_pending_capsules(mut caps: Vec<PendingCapsule>, max: usize) -> Vec<PendingCapsule> {
    if caps.len() <= max {
        return caps;
    }
    caps.sort_by(|a, b| {
        a.is_terminal()
            .cmp(&b.is_terminal())
            .then_with(|| a.samples.len().cmp(&b.samples.len()))
            .then_with(|| {
                a.created_at_ms
                    .partial_cmp(&b.created_at_ms)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.trigger_identity_id.cmp(&b.trigger_identity_id))
    });
    let excess = caps.len() - max;
    caps.drain(0..excess);
    caps
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

/// Published event identity rows: ascending `timestampMs`, NQ before ES on a tie.
fn sort_recent_events_one_clock(events: &mut [Value]) {
    events.sort_by(|a, b| {
        let ta = a.get("timestampMs").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let tb = b.get("timestampMs").and_then(|v| v.as_f64()).unwrap_or(0.0);
        ta.partial_cmp(&tb)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                let ra = a
                    .get("rootSymbol")
                    .and_then(|v| v.as_str())
                    .and_then(|s| RouterRoot::parse(s).ok());
                let rb = b
                    .get("rootSymbol")
                    .and_then(|v| v.as_str())
                    .and_then(|s| RouterRoot::parse(s).ok());
                ra.cmp(&rb)
            })
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

    #[test]
    fn journal_queue_does_not_deadlock_with_shared_nq_ingest() {
        let nq_pipelines = Arc::new(Mutex::new(PipelineEngine::new()));
        let last_bid = Arc::new(Mutex::new(20_000.0));
        let last_ask = Arc::new(Mutex::new(20_000.25));
        let router = Arc::new(MarketRouter::with_shared_nq(
            Arc::clone(&nq_pipelines),
            Arc::new(Mutex::new(EventDetector::new())),
            Arc::new(Mutex::new(FlowEventEmitter::new())),
            Arc::clone(&last_bid),
            Arc::clone(&last_ask),
            RouterRoot::Nq,
            SourceProviderKind::File,
            "embedded",
        ));
        std::thread::scope(|s| {
            s.spawn(|| {
                for i in 0..80 {
                    {
                        let mut p = nq_pipelines.lock().unwrap();
                        p.on_trade_with_timestamp(
                            20_000.0 + i as f64 * 0.25,
                            1.0,
                            true,
                            30,
                            RTH_TS + i as f64 * 250.0,
                        );
                    }
                    router.queue_journal_frames();
                }
            });
            s.spawn(|| {
                for i in 0..80 {
                    router.apply_tick(
                        RouterRoot::Es,
                        &tick(RTH_TS + i as f64 * 250.0 + 10.0, 5_000.0),
                    );
                }
            });
        });
        let db = Database::open(":memory:").expect("db");
        let stats = router.persist_journal(&db).expect("persist");
        assert!(stats.frames_written >= 2);
        assert!(db.count_journal_frames().expect("count") >= 2);
    }

    #[test]
    fn pending_journal_frames_drop_oldest_past_cap() {
        let mut pending = BTreeMap::new();
        for i in 0..(PENDING_JOURNAL_MAX_FRAMES + 10) {
            pending.insert(
                (i as i64, RouterRoot::Nq),
                JournalFrameRecord {
                    clock_ms: i as f64 * 1_000.0,
                    frame_second: i as i64,
                    root_symbol: "NQ".into(),
                    session_type: "RTH".into(),
                    session_segment: "None".into(),
                    trading_day: "2024-01-02".into(),
                    payload: serde_json::json!({ "lastPrice": 20_000.0 }),
                },
            );
        }
        cap_pending_journal_map(&mut pending);
        assert_eq!(pending.len(), PENDING_JOURNAL_MAX_FRAMES);
        assert!(
            !pending.contains_key(&(0, RouterRoot::Nq)),
            "oldest keys must drop first"
        );
        assert!(pending.contains_key(&(10, RouterRoot::Nq)));
        assert!(pending.contains_key(&((PENDING_JOURNAL_MAX_FRAMES + 9) as i64, RouterRoot::Nq)));
    }

    #[test]
    fn published_events_sort_by_one_clock() {
        let mut events = vec![
            serde_json::json!({"timestampMs": 200.0, "rootSymbol": "NQ", "eventType": "b"}),
            serde_json::json!({"timestampMs": 100.0, "rootSymbol": "ES", "eventType": "a"}),
            serde_json::json!({"timestampMs": 200.0, "rootSymbol": "ES", "eventType": "c"}),
        ];
        sort_recent_events_one_clock(&mut events);
        assert_eq!(events[0]["rootSymbol"], "ES");
        assert_eq!(events[0]["timestampMs"], 100.0);
        assert_eq!(events[1]["rootSymbol"], "NQ");
        assert_eq!(events[1]["timestampMs"], 200.0);
        assert_eq!(events[2]["rootSymbol"], "ES");
        assert_eq!(events[2]["timestampMs"], 200.0);
    }

    fn pending_capsule(trigger: &str, created_ms: f64, samples: usize) -> PendingCapsule {
        let event = MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: created_ms,
            event_type: "stop_run".into(),
            level_name: None,
            price: 20_000.0,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        };
        let mut cap =
            PendingCapsule::open_from_ring(RouterRoot::Nq, &event, &CapsuleRing::default());
        cap.trigger_identity_id = trigger.into();
        cap.created_at_ms = created_ms;
        cap.samples = (0..samples).map(|i| serde_json::json!({"i": i})).collect();
        cap
    }

    #[test]
    fn pending_capsule_bound_keeps_fuller_and_newer() {
        let old_thin = pending_capsule("aaa_old_thin", 1.0, 2);
        let fuller = pending_capsule("zzz_fuller", 2.0, 40);
        let newer_thin = pending_capsule("mmm_newer_thin", 3.0, 2);
        let kept = bound_pending_capsules(vec![fuller, old_thin, newer_thin], 2);
        let ids: Vec<_> = kept
            .iter()
            .map(|c| c.trigger_identity_id.as_str())
            .collect();
        assert!(ids.contains(&"zzz_fuller"));
        assert!(ids.contains(&"mmm_newer_thin"));
        assert!(!ids.contains(&"aaa_old_thin"));
    }
}
