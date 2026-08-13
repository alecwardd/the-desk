//! SIL-M3b Capsules: high-resolution dumps around DOM-family Events.
//!
//! A rolling ~250 ms in-memory ring per root covers lookback. When a
//! DOM-family Event is noted, a Capsule is opened from that ring
//! (~30 s before) and kept pending on the MarketRouter clock until ~60 s
//! after (or the session/feed ends, in which case the dump is marked
//! incomplete/degraded). The forever journal stays 1 Hz Journal Frames —
//! ring samples are never persisted as a 250 ms table.
//!
//! Capsules are **not** trader-memory markdown files under `~/.the-desk`.

use std::collections::VecDeque;

use serde_json::{json, Value};

use crate::catalog::{is_dom_family_event_type, is_invalidation_event_type, requires_capsule};

pub use crate::catalog::{CAPSULE_AFTER_MS, CAPSULE_LOOKBACK_MS};
use crate::db::{
    journal_frame_second_from_ts, market_event_dedup_id, market_event_id, stable_hash_hex,
    CapsuleRecord, CAPSULE_COMPLETENESS_COMPLETE, CAPSULE_COMPLETENESS_INCOMPLETE,
    CAPSULE_COMPLETENESS_PENDING,
};
use crate::pipelines::MarketEvent;

use super::root::RouterRoot;

/// In-memory ring cadence. Not a persist interval.
pub const CAPSULE_RING_STEP_MS: f64 = 250.0;
/// Extra ring retention beyond lookback so persist jitter cannot drop the window.
pub const CAPSULE_RING_MARGIN_MS: f64 = 2_000.0;

/// Intended Capsule window on the MarketRouter clock: 30 s before → 60 s after.
pub fn capsule_window_bounds(event_timestamp_ms: f64) -> (f64, f64) {
    (
        event_timestamp_ms - CAPSULE_LOOKBACK_MS,
        event_timestamp_ms + CAPSULE_AFTER_MS,
    )
}

/// One Capsule per triggering occurrence (`open`), never on every repeat.
pub fn should_open_capsule(event_type: &str, stored_lifecycle: &str) -> bool {
    requires_capsule(event_type)
        && !is_invalidation_event_type(event_type)
        && stored_lifecycle == "open"
}

/// Deterministic Capsule id keyed to the triggering occurrence identity.
pub fn capsule_id_for_trigger(trigger_identity_id: &str) -> String {
    format!("cap_{}", stable_hash_hex(trigger_identity_id))
}

/// Bounded per-root 250 ms ring. Rebuildable in spirit from `.scid`/`.depth`
/// through the same MarketRouter snapshot as Journal Frames.
#[derive(Debug, Clone, Default)]
pub struct CapsuleRing {
    samples: VecDeque<CapsuleRingSample>,
    last_bucket: Option<i64>,
}

/// One in-memory ring sample (never a SQLite row by itself).
#[derive(Debug, Clone)]
pub struct CapsuleRingSample {
    pub clock_ms: f64,
    pub session_type: String,
    pub payload: Value,
}

impl CapsuleRing {
    /// Samples retained: lookback plus a small margin, at 250 ms cadence.
    pub fn capacity() -> usize {
        ((CAPSULE_LOOKBACK_MS + CAPSULE_RING_MARGIN_MS) / CAPSULE_RING_STEP_MS).ceil() as usize + 1
    }

    /// Push a snapshot if the MarketRouter clock advanced into a new 250 ms bucket.
    ///
    /// The first sample in a bucket is kept (same spirit as 1 Hz frame pinning).
    pub fn push(&mut self, clock_ms: f64, session_type: &str, payload: Value) -> bool {
        if !clock_ms.is_finite() || clock_ms <= 0.0 || payload.is_null() {
            return false;
        }
        let bucket = (clock_ms / CAPSULE_RING_STEP_MS).floor() as i64;
        if self.last_bucket == Some(bucket) {
            return false;
        }
        if let Some(prev) = self.last_bucket {
            let max_jump = Self::capacity() as i64;
            if bucket < prev {
                if prev - bucket > max_jump {
                    // Large backward jump: a latched glitch; rebuild from here.
                    self.samples.clear();
                    self.last_bucket = None;
                } else {
                    return false;
                }
            } else if bucket > prev + max_jump {
                // Session gap or future glitch: drop stale lookback, accept this tick.
                self.samples.clear();
                self.last_bucket = None;
            }
        }
        self.samples.push_back(CapsuleRingSample {
            clock_ms,
            session_type: session_type.to_string(),
            payload,
        });
        self.last_bucket = Some(bucket);
        self.evict();
        true
    }

    fn evict(&mut self) {
        let cap = Self::capacity();
        while self.samples.len() > cap {
            self.samples.pop_front();
        }
        if let Some(newest) = self.samples.back().map(|s| s.clock_ms) {
            let cutoff = newest - CAPSULE_LOOKBACK_MS - CAPSULE_RING_MARGIN_MS;
            while self.samples.front().is_some_and(|s| s.clock_ms < cutoff) {
                self.samples.pop_front();
            }
        }
    }

    /// Samples in `[event_ts - lookback, event_ts]` on the MarketRouter clock.
    pub fn lookback(&self, event_timestamp_ms: f64) -> Vec<CapsuleRingSample> {
        let start = event_timestamp_ms - CAPSULE_LOOKBACK_MS;
        self.samples
            .iter()
            .filter(|s| s.clock_ms >= start && s.clock_ms <= event_timestamp_ms + 0.5)
            .cloned()
            .collect()
    }

    /// Samples strictly after `after_ms` up to `until_ms` (after-window collection).
    pub fn samples_after(&self, after_ms: f64, until_ms: f64) -> Vec<CapsuleRingSample> {
        self.samples
            .iter()
            .filter(|s| s.clock_ms > after_ms && s.clock_ms <= until_ms + 0.5)
            .cloned()
            .collect()
    }

    /// Number of ring samples currently retained.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// True when the ring holds no samples.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// True when `clock_ms` falls in a 250 ms bucket not yet stored.
    pub fn needs_sample(&self, clock_ms: f64) -> bool {
        if !clock_ms.is_finite() || clock_ms <= 0.0 {
            return false;
        }
        let bucket = (clock_ms / CAPSULE_RING_STEP_MS).floor() as i64;
        self.last_bucket != Some(bucket)
    }
}

/// In-memory Capsule until the after-window closes (or the session/feed ends).
#[derive(Debug, Clone)]
pub struct PendingCapsule {
    pub id: String,
    pub trigger_identity_id: String,
    pub dedup_identity_id: String,
    pub root: RouterRoot,
    pub event_type: String,
    pub event_timestamp_ms: f64,
    pub window_start_ms: f64,
    pub window_end_ms: f64,
    pub session_type: String,
    pub trading_day: String,
    pub samples: Vec<Value>,
    pub completeness: String,
    pub degraded: bool,
    pub created_at_ms: f64,
    last_sample_ms: f64,
}

impl PendingCapsule {
    /// Open from the ring at trigger time (lookback only; after-window fills later).
    pub fn open_from_ring(root: RouterRoot, event: &MarketEvent, ring: &CapsuleRing) -> Self {
        let trigger_identity_id = market_event_id(event);
        let (window_start_ms, window_end_ms) = capsule_window_bounds(event.timestamp_ms);
        let lookback = ring.lookback(event.timestamp_ms);
        // Advance the after-window cursor across the full lookback span so
        // filtered other-session samples are not re-ingested and mixed.
        let last_sample_ms = lookback
            .last()
            .map(|s| s.clock_ms)
            .unwrap_or(event.timestamp_ms);
        let mut session_truncated = false;
        let samples: Vec<Value> = lookback
            .into_iter()
            .filter(|s| {
                if s.session_type == event.session_type {
                    true
                } else {
                    session_truncated = true;
                    false
                }
            })
            .map(|s| s.payload)
            .collect();
        Self {
            id: capsule_id_for_trigger(&trigger_identity_id),
            trigger_identity_id,
            dedup_identity_id: market_event_dedup_id(event, Some(root.as_str())),
            root,
            event_type: event.event_type.clone(),
            event_timestamp_ms: event.timestamp_ms,
            window_start_ms,
            window_end_ms,
            session_type: event.session_type.clone(),
            trading_day: event.trading_day.clone(),
            samples,
            completeness: CAPSULE_COMPLETENESS_PENDING.to_string(),
            degraded: session_truncated,
            created_at_ms: event.timestamp_ms,
            last_sample_ms,
        }
    }

    /// Append after-window ring samples. Session mix marks incomplete/degraded.
    pub fn ingest_ring(&mut self, ring: &CapsuleRing) {
        if self.is_terminal() {
            return;
        }
        for sample in ring.samples_after(self.last_sample_ms, self.window_end_ms) {
            if sample.session_type != self.session_type {
                self.completeness = CAPSULE_COMPLETENESS_INCOMPLETE.to_string();
                self.degraded = true;
                return;
            }
            self.samples.push(sample.payload);
            self.last_sample_ms = sample.clock_ms;
        }
    }

    /// Close on MarketRouter clock (complete) or session/feed end (incomplete).
    pub fn finalize(&mut self, clock_ms: f64, session_or_feed_ended: bool) {
        if self.is_terminal() {
            return;
        }
        if clock_ms.is_finite() && clock_ms + 0.5 >= self.window_end_ms {
            self.completeness = CAPSULE_COMPLETENESS_COMPLETE.to_string();
            return;
        }
        if session_or_feed_ended {
            self.completeness = CAPSULE_COMPLETENESS_INCOMPLETE.to_string();
            self.degraded = true;
        }
    }

    /// True when the Capsule reached `complete` or `incomplete`.
    pub fn is_terminal(&self) -> bool {
        self.completeness == CAPSULE_COMPLETENESS_COMPLETE
            || self.completeness == CAPSULE_COMPLETENESS_INCOMPLETE
    }

    /// True while the after-window is still open.
    pub fn is_pending(&self) -> bool {
        self.completeness == CAPSULE_COMPLETENESS_PENDING
    }

    /// Latest ring sample on the MarketRouter clock (lookback or after-window).
    pub fn observed_clock_ms(&self) -> f64 {
        self.last_sample_ms
    }

    /// SQLite row (payload is the dump, not a 250 ms frame store).
    pub fn to_record(&self, updated_at_ms: f64) -> CapsuleRecord {
        let observed_start_ms = self
            .samples
            .first()
            .and_then(|s| s.get("clockMs").and_then(|v| v.as_f64()));
        let observed_end_ms = self
            .samples
            .last()
            .and_then(|s| s.get("clockMs").and_then(|v| v.as_f64()));
        let start_frame_second = journal_frame_second_from_ts(self.window_start_ms);
        let end_frame_second = if self.completeness == CAPSULE_COMPLETENESS_INCOMPLETE {
            observed_end_ms
                .and_then(journal_frame_second_from_ts)
                .or_else(|| journal_frame_second_from_ts(self.window_end_ms))
        } else {
            journal_frame_second_from_ts(self.window_end_ms)
        };
        CapsuleRecord {
            id: self.id.clone(),
            trigger_identity_id: self.trigger_identity_id.clone(),
            dedup_identity_id: self.dedup_identity_id.clone(),
            root_symbol: self.root.as_str().to_string(),
            event_type: self.event_type.clone(),
            event_timestamp_ms: self.event_timestamp_ms,
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            observed_start_ms,
            observed_end_ms,
            start_frame_second,
            end_frame_second,
            completeness: self.completeness.clone(),
            degraded: self.degraded,
            sample_count: self.samples.len() as i64,
            payload: json!({
                "samples": self.samples,
                "lookbackMs": CAPSULE_LOOKBACK_MS,
                "afterMs": CAPSULE_AFTER_MS,
                "ringStepMs": CAPSULE_RING_STEP_MS,
                "sessionType": self.session_type,
                "tradingDay": self.trading_day,
            }),
            created_at_ms: self.created_at_ms,
            updated_at_ms,
        }
    }
}

/// Compact forensic sample from a MarketRouter snapshot (same source as Journal Frames).
pub fn compact_capsule_sample(clock_ms: f64, root: RouterRoot, snapshot: &Value) -> Value {
    let pick = |key: &str| snapshot.get(key).cloned().unwrap_or(Value::Null);
    json!({
        "clockMs": clock_ms,
        "rootSymbol": root.as_str(),
        "sessionType": pick("sessionType"),
        "sessionSegment": pick("sessionSegment"),
        "tradingDay": pick("tradingDay"),
        "lastPrice": pick("lastPrice"),
        "bid": pick("bid"),
        "ask": pick("ask"),
        "sessionHigh": pick("sessionHigh"),
        "sessionLow": pick("sessionLow"),
        "cumulativeDelta": pick("cumulativeDelta"),
    })
}

/// Session type from a snapshot, falling back to `"Unknown"`.
pub fn snapshot_session_type(snapshot: &Value) -> String {
    snapshot
        .get("sessionType")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("Unknown")
        .to_string()
}

/// True when this detector row is a DOM-family type that may open a Capsule.
pub fn event_may_open_capsule(event: &MarketEvent) -> bool {
    is_dom_family_event_type(&event.event_type) && !is_invalidation_event_type(&event.event_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DOM_FAMILY_EVENT_TYPES;

    fn sample_event(event_type: &str, ts: f64) -> MarketEvent {
        MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: ts,
            event_type: event_type.into(),
            level_name: None,
            price: 20_000.0,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        }
    }

    fn fill_ring(ring: &mut CapsuleRing, start_ms: f64, n: usize) {
        for i in 0..n {
            let ts = start_ms + i as f64 * CAPSULE_RING_STEP_MS;
            ring.push(
                ts,
                "RTH",
                compact_capsule_sample(
                    ts,
                    RouterRoot::Nq,
                    &json!({"sessionType": "RTH", "lastPrice": 20_000.0}),
                ),
            );
        }
    }

    #[test]
    fn window_defaults_match_adr_024() {
        assert_eq!(CAPSULE_LOOKBACK_MS, 30_000.0);
        assert_eq!(CAPSULE_AFTER_MS, 60_000.0);
        assert_eq!(CAPSULE_RING_STEP_MS, 250.0);
        let ts = 1_704_207_600_000.0;
        let (start, end) = capsule_window_bounds(ts);
        assert!((start - (ts - 30_000.0)).abs() < f64::EPSILON);
        assert!((end - (ts + 60_000.0)).abs() < f64::EPSILON);
        assert_eq!(
            journal_frame_second_from_ts(end).unwrap()
                - journal_frame_second_from_ts(start).unwrap(),
            90
        );
    }

    #[test]
    fn trigger_is_dom_family_open_only() {
        for name in DOM_FAMILY_EVENT_TYPES {
            assert!(should_open_capsule(name, "open"), "{name}");
            assert!(
                !should_open_capsule(name, "updated"),
                "{name} repeat must not spawn another Capsule"
            );
            assert!(!should_open_capsule(name, "resolved"));
            assert!(!should_open_capsule(name, "expired"));
        }
        assert!(!should_open_capsule("pinch_detected", "open"));
        assert!(!should_open_capsule("absorption_confirmed", "open"));
        assert!(!should_open_capsule("ib_extension_hit", "open"));
        assert!(!should_open_capsule("stop_run_invalidated", "open"));
    }

    #[test]
    fn ring_keeps_lookback_not_a_permanent_store() {
        let mut ring = CapsuleRing::default();
        let start = 1_704_207_600_000.0;
        fill_ring(&mut ring, start, 200);
        assert!(ring.len() <= CapsuleRing::capacity());
        assert!(ring.len() < 200, "older than lookback+margin must evict");
        let event_ts = start + 199.0 * CAPSULE_RING_STEP_MS;
        let lookback = ring.lookback(event_ts);
        assert!(!lookback.is_empty());
        let first = lookback.first().unwrap().clock_ms;
        assert!(
            first + 1.0 >= event_ts - CAPSULE_LOOKBACK_MS - CAPSULE_RING_MARGIN_MS,
            "lookback must not retain a dense forever buffer"
        );
    }

    #[test]
    fn lookback_and_after_window_bounds_on_market_clock() {
        let mut ring = CapsuleRing::default();
        let event_ts = 1_704_207_630_000.0;
        fill_ring(
            &mut ring,
            event_ts - CAPSULE_LOOKBACK_MS,
            (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as usize + 1,
        );
        let pending = PendingCapsule::open_from_ring(
            RouterRoot::Nq,
            &sample_event("stop_run", event_ts),
            &ring,
        );
        assert_eq!(pending.window_start_ms, event_ts - 30_000.0);
        assert_eq!(pending.window_end_ms, event_ts + 60_000.0);
        assert!(pending.is_pending());
        assert!(!pending.samples.is_empty());
        let first = pending.samples[0]["clockMs"].as_f64().unwrap();
        assert!(first + 1.0 >= event_ts - 30_000.0);
        assert!(first <= event_ts);

        let mut pending = pending;
        let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as usize;
        for i in 1..=after_n {
            let ts = event_ts + i as f64 * CAPSULE_RING_STEP_MS;
            ring.push(
                ts,
                "RTH",
                compact_capsule_sample(
                    ts,
                    RouterRoot::Nq,
                    &json!({"sessionType": "RTH", "lastPrice": 20_000.0}),
                ),
            );
            pending.ingest_ring(&ring);
        }
        pending.finalize(event_ts + CAPSULE_AFTER_MS, false);
        assert_eq!(pending.completeness, CAPSULE_COMPLETENESS_COMPLETE);
        let last = pending.samples.last().unwrap()["clockMs"].as_f64().unwrap();
        assert!(last + 1.0 >= event_ts + CAPSULE_AFTER_MS - CAPSULE_RING_STEP_MS);
        assert!(last <= event_ts + CAPSULE_AFTER_MS + 1.0);
        let rec = pending.to_record(event_ts + CAPSULE_AFTER_MS);
        assert_eq!(
            rec.start_frame_second,
            journal_frame_second_from_ts(event_ts - 30_000.0)
        );
        assert_eq!(
            rec.end_frame_second,
            journal_frame_second_from_ts(event_ts + 60_000.0)
        );
    }

    #[test]
    fn incomplete_when_session_or_feed_ends_before_after_window() {
        let mut ring = CapsuleRing::default();
        let event_ts = 1_704_207_630_000.0;
        fill_ring(&mut ring, event_ts - 5_000.0, 20);
        let mut pending = PendingCapsule::open_from_ring(
            RouterRoot::Nq,
            &sample_event("iceberg_reload", event_ts),
            &ring,
        );
        pending.finalize(event_ts + 1_000.0, true);
        assert_eq!(pending.completeness, CAPSULE_COMPLETENESS_INCOMPLETE);
        assert!(pending.degraded);
        let rec = pending.to_record(event_ts + 1_000.0);
        assert_eq!(rec.completeness, CAPSULE_COMPLETENESS_INCOMPLETE);
        assert!(rec.degraded);
        assert!(
            rec.end_frame_second.unwrap()
                < journal_frame_second_from_ts(event_ts + CAPSULE_AFTER_MS).unwrap()
                || rec.observed_end_ms.unwrap() < event_ts + CAPSULE_AFTER_MS
        );
    }

    #[test]
    fn lookback_excludes_other_session_samples() {
        let mut ring = CapsuleRing::default();
        let event_ts = 1_704_207_630_000.0;
        for i in 0..4 {
            let ts = event_ts - 2_000.0 + i as f64 * CAPSULE_RING_STEP_MS;
            ring.push(
                ts,
                "Globex",
                compact_capsule_sample(
                    ts,
                    RouterRoot::Nq,
                    &json!({"sessionType": "Globex", "lastPrice": 19_999.0}),
                ),
            );
        }
        for i in 0..4 {
            let ts = event_ts - 750.0 + i as f64 * CAPSULE_RING_STEP_MS;
            ring.push(
                ts,
                "RTH",
                compact_capsule_sample(
                    ts,
                    RouterRoot::Nq,
                    &json!({"sessionType": "RTH", "lastPrice": 20_000.0}),
                ),
            );
        }
        let pending = PendingCapsule::open_from_ring(
            RouterRoot::Nq,
            &sample_event("stop_run", event_ts),
            &ring,
        );
        assert!(!pending.samples.is_empty());
        assert!(pending.is_pending());
        assert!(
            pending.degraded,
            "dropping other-session lookback must mark degraded"
        );
        for sample in &pending.samples {
            assert_eq!(sample["sessionType"], "RTH");
            assert_eq!(sample["lastPrice"].as_f64(), Some(20_000.0));
        }
    }

    #[test]
    fn ring_resets_on_discontinuity_instead_of_latching() {
        let mut ring = CapsuleRing::default();
        let start = 1_704_207_600_000.0;
        fill_ring(&mut ring, start, 8);
        let before = ring.len();
        assert!(before > 0);
        let glitch = start + 2.0 * 3_600_000.0;
        assert!(ring.push(
            glitch,
            "RTH",
            compact_capsule_sample(glitch, RouterRoot::Nq, &json!({"sessionType": "RTH"})),
        ));
        assert_eq!(ring.len(), 1, "large forward jump must drop stale lookback");
        let resume = start + 9.0 * CAPSULE_RING_STEP_MS;
        assert!(ring.push(
            resume,
            "RTH",
            compact_capsule_sample(resume, RouterRoot::Nq, &json!({"sessionType": "RTH"})),
        ));
        assert_eq!(
            ring.len(),
            1,
            "large backward jump after a glitch must rebuild"
        );
        let next = resume + CAPSULE_RING_STEP_MS;
        assert!(ring.push(
            next,
            "RTH",
            compact_capsule_sample(next, RouterRoot::Nq, &json!({"sessionType": "RTH"})),
        ));
        assert_eq!(ring.len(), 2);
    }

    #[test]
    fn session_mix_marks_incomplete_degraded() {
        let mut ring = CapsuleRing::default();
        let event_ts = 1_704_207_630_000.0;
        fill_ring(&mut ring, event_ts - 1_000.0, 4);
        let mut pending = PendingCapsule::open_from_ring(
            RouterRoot::Nq,
            &sample_event("pull_intent", event_ts),
            &ring,
        );
        ring.push(
            event_ts + 250.0,
            "Globex",
            compact_capsule_sample(
                event_ts + 250.0,
                RouterRoot::Nq,
                &json!({"sessionType": "Globex", "lastPrice": 20_000.0}),
            ),
        );
        pending.ingest_ring(&ring);
        assert_eq!(pending.completeness, CAPSULE_COMPLETENESS_INCOMPLETE);
        assert!(pending.degraded);
    }

    #[test]
    fn every_dom_family_type_is_capsule_mandatory() {
        for name in DOM_FAMILY_EVENT_TYPES {
            assert!(event_may_open_capsule(&sample_event(name, 1.0)), "{name}");
            assert!(requires_capsule(name), "{name}");
        }
        assert_eq!(DOM_FAMILY_EVENT_TYPES.len(), 4);
    }
}
