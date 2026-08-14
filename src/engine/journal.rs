//! Market State Journal (SIL-M3a): 1 Hz Journal Frames + transition event rows.
//!
//! Compute/publish may stay on the ~250 ms analysis cadence (ADR-019). This
//! module persists **Journal Frames** only when the MarketRouter clock advances
//! into a new Unix second — never every 250 ms snapshot. Capsules (SIL-M3b) are
//! high-resolution dumps around DOM-family Events, stored separately — never as
//! a permanent 250 ms frame table. SIL-M3d writes the same 1 Hz frames to
//! session-partitioned cold dumps when a [`crate::engine::ColdFrameStore`] is
//! attached — SQLite stays the hot window. Cold IO is append-only and
//! best-effort; a dump failure must not abort the persist cycle.
//!
//! MFE/MAE / R-result stay tick-driven via [`crate::outcome_tracker`] /
//! `PendingOutcomeSet`. This writer persists already-computed state only and
//! has no order authority (Trust Ceiling remains L3).

use std::collections::BTreeMap;

use serde_json::Value;

use crate::db::{
    journal_frame_second_from_ts, Database, DbError, JournalAsOfSnapshot, JournalFrameRecord,
};
use crate::pipelines::MarketEvent;

use super::root::RouterRoot;

/// Journal Frame persistence interval on the MarketRouter clock (1 Hz).
pub const JOURNAL_FRAME_INTERVAL_MS: f64 = 1000.0;

/// Result of one journal observation (frames + transition rows + Capsules).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JournalPersistStats {
    pub frames_written: usize,
    pub events_written: usize,
    pub capsules_opened: usize,
    pub capsules_finalized: usize,
    /// Newly inserted cold-dump rows (0 when no cold store is attached).
    pub cold_frames_written: usize,
    pub frame_second: Option<i64>,
}

/// Floor a MarketRouter clock timestamp onto the 1 Hz Journal Frame second.
pub fn journal_frame_second(clock_ms: f64) -> Option<i64> {
    journal_frame_second_from_ts(clock_ms)
}

/// Build a Journal Frame row for one root at a shared MarketRouter clock second.
pub fn journal_frame_from_snapshot(
    clock_ms: f64,
    frame_second: i64,
    root: RouterRoot,
    payload: &Value,
) -> JournalFrameRecord {
    JournalFrameRecord {
        clock_ms,
        frame_second,
        root_symbol: root.as_str().to_string(),
        session_type: json_str(payload, "sessionType").unwrap_or_else(|| "Unknown".into()),
        session_segment: json_str(payload, "sessionSegment").unwrap_or_else(|| "None".into()),
        trading_day: json_str(payload, "tradingDay").unwrap_or_default(),
        payload: payload.clone(),
    }
}

/// Flush queued 1 Hz frames plus joinable transition event rows.
///
/// Duplicate `(frame_second, root_symbol)` rows are ignored so a 250 ms publish
/// loop cannot store 4 Hz frames.
pub fn persist_journal_observation(
    db: &Database,
    frames: &[JournalFrameRecord],
    events: &[(RouterRoot, MarketEvent)],
) -> Result<JournalPersistStats, DbError> {
    let mut stats = JournalPersistStats::default();
    if !frames.is_empty() {
        stats.frames_written = db.insert_journal_frames(frames)?;
        stats.frame_second = frames.last().map(|f| f.frame_second);
    }
    stats.events_written = insert_transition_events(db, events)?;
    Ok(stats)
}

/// `get_state(as_of=…)` lookup — Journal Frames only (never `pipeline_snapshots`).
pub fn journal_frames_as_of(
    db: &Database,
    as_of_ms: f64,
) -> Result<Option<JournalAsOfSnapshot>, DbError> {
    db.get_journal_frames_as_of(as_of_ms)
}

fn insert_transition_events(
    db: &Database,
    events: &[(RouterRoot, MarketEvent)],
) -> Result<usize, DbError> {
    if events.is_empty() {
        return Ok(0);
    }
    let mut by_root: BTreeMap<RouterRoot, Vec<MarketEvent>> = BTreeMap::new();
    for (root, event) in events {
        by_root.entry(*root).or_default().push(event.clone());
    }
    let mut written = 0usize;
    for (root, group) in by_root {
        written += db.insert_market_events_batch_scoped(Some(root.as_str()), &group)?;
    }
    Ok(written)
}

fn json_str(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::engine::{MarketRouter, SourceProviderKind, SourceTick};
    use crate::feed::TradeSide;
    use crate::outcome_tracker;
    use crate::outcomes;
    use crate::pipelines::MarketEvent;

    const RTH_TS: f64 = 1_704_207_600_000.0;
    const GLOBEX_TS: f64 = 1_704_243_600_000.0;

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

    fn pending_outcome(signal_id: &str, fired_at_ms: f64) -> crate::db::SignalOutcome {
        crate::db::SignalOutcome {
            signal_id: signal_id.into(),
            setup_id: "test".into(),
            setup_name: Some("test".into()),
            session_date: crate::session_date_from_timestamp_ms(fired_at_ms),
            root_symbol: Some("NQ".into()),
            contract_symbol: None,
            source: "live".into(),
            job_id: None,
            fired_at_ms,
            fired_price: 100.0,
            target_price: Some(102.0),
            stop_price: Some(99.0),
            outcome: "pending".into(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
            direction: Some("long".into()),
            entry_price: Some(100.0),
            risk_points: Some(1.0),
            exit_price: None,
            outcome_quality: outcomes::QUALITY_VERIFIED.to_string(),
            quality_flags: Vec::new(),
            outcome_engine_version: Some(outcomes::OUTCOME_ENGINE_VERSION.into()),
            rules_schema_version: None,
            setup_template_hash: None,
            last_observed_price: Some(100.0),
            last_observed_at_ms: Some(fired_at_ms),
        }
    }

    #[test]
    fn one_hz_frames_not_every_250ms_for_nq_and_es() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "journal");
        let mut ticks = Vec::new();
        for i in 0..13 {
            let ts = RTH_TS + (i as f64) * 250.0;
            ticks.push((RouterRoot::Nq, tick(ts, 20_000.0 + i as f64 * 0.25)));
            ticks.push((RouterRoot::Es, tick(ts + 10.0, 5_000.0 + i as f64 * 0.25)));
        }
        router.apply_merged(ticks);
        let stats = router.persist_journal(&db).expect("persist");
        // 13 ticks × 250 ms spans seconds 0..=3 → 4 seconds × 2 roots = 8 frames,
        // not 13×2 (250 ms) publishes.
        assert_eq!(db.count_journal_frames().expect("count"), 8);
        assert_eq!(stats.frames_written, 8);
        let frames = db.list_journal_frames().expect("list");
        let seconds: std::collections::BTreeSet<_> =
            frames.iter().map(|f| f.frame_second).collect();
        assert_eq!(seconds.len(), 4);
        assert!(frames.iter().any(|f| f.root_symbol == "NQ"));
        assert!(frames.iter().any(|f| f.root_symbol == "ES"));
        let clock = frames.iter().map(|f| f.clock_ms).fold(0.0, f64::max);
        let nq_at = frames
            .iter()
            .filter(|f| f.root_symbol == "NQ" && (f.clock_ms - clock).abs() < 1.0)
            .count();
        let es_at = frames
            .iter()
            .filter(|f| f.root_symbol == "ES" && (f.clock_ms - clock).abs() < 1.0)
            .count();
        // Last persist writes both roots on the same clock.
        assert_eq!(nq_at, 1);
        assert_eq!(es_at, 1);
    }

    #[test]
    fn second_observe_same_second_does_not_write_another_frame() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "journal");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.apply_tick(RouterRoot::Es, &tick(RTH_TS + 50.0, 5_000.0));
        assert_eq!(
            router.persist_journal(&db).expect("first").frames_written,
            2
        );
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS + 250.0, 20_000.25));
        assert_eq!(
            router.persist_journal(&db).expect("second").frames_written,
            0
        );
        assert_eq!(db.count_journal_frames().expect("count"), 2);
    }

    #[test]
    fn mfe_mae_r_result_unchanged_when_journal_persists() {
        let without = Database::open(":memory:").expect("without");
        let with_journal = Database::open(":memory:").expect("with");
        let fired = RTH_TS;
        let row = pending_outcome("sig-journal-parity", fired);
        without.insert_signal_outcome(&row).expect("w");
        with_journal.insert_signal_outcome(&row).expect("j");

        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "parity");
        for (price, ts) in [(101.0, fired + 1_000.0), (99.5, fired + 2_000.0)] {
            outcome_tracker::on_tick(&without, price, ts).expect("tick w");
            outcome_tracker::on_tick(&with_journal, price, ts).expect("tick j");
            router.apply_tick(RouterRoot::Nq, &tick(ts, 20_000.0));
            router.apply_tick(RouterRoot::Es, &tick(ts + 1.0, 5_000.0));
            router.persist_journal(&with_journal).expect("journal");
        }

        let a = without
            .list_signal_outcomes_for_replay(Some("live"), None)
            .expect("a")
            .pop()
            .expect("a row");
        let b = with_journal
            .list_signal_outcomes_for_replay(Some("live"), None)
            .expect("b")
            .pop()
            .expect("b row");
        assert_eq!(a.outcome, b.outcome);
        assert_eq!(a.max_favorable_excursion, b.max_favorable_excursion);
        assert_eq!(a.max_adverse_excursion, b.max_adverse_excursion);
        assert_eq!(a.r_result, b.r_result);
        assert_eq!(a.outcome_at_ms, b.outcome_at_ms);
        assert!(
            with_journal.count_journal_frames().expect("frames") >= 2,
            "journal writer must have persisted frames during the parity run"
        );
    }

    #[test]
    fn journal_frame_interval_is_one_second() {
        assert_eq!(JOURNAL_FRAME_INTERVAL_MS, 1000.0);
        assert_eq!(
            journal_frame_second(1_704_207_600_250.0),
            Some(1_704_207_600)
        );
        assert_eq!(
            journal_frame_second(1_704_207_600_250.0),
            journal_frame_second_from_ts(1_704_207_600_250.0)
        );
    }

    #[test]
    fn transition_events_join_to_frames_on_shared_clock() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "join");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.apply_tick(RouterRoot::Es, &tick(RTH_TS + 50.0, 5_000.0));
        let event = MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: RTH_TS + 100.0,
            event_type: "new_session_high".into(),
            level_name: None,
            price: 20_000.25,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        };
        router.note_transition_events(RouterRoot::Nq, &[event]);
        router.persist_journal(&db).expect("persist");
        let second = journal_frame_second(RTH_TS).expect("second");
        let joined = db
            .list_events_joined_to_journal_frame(second)
            .expect("join");
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0]["eventType"], "new_session_high");
        assert_eq!(joined[0]["rootSymbol"], "NQ");
        assert_eq!(joined[0]["frameRootSymbol"], "NQ");
    }

    #[test]
    fn transition_event_joins_when_other_root_clock_is_ahead() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "join-ahead");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.apply_tick(RouterRoot::Es, &tick(RTH_TS + 1_500.0, 5_000.0));
        let event = MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: RTH_TS + 100.0,
            event_type: "new_session_high".into(),
            level_name: None,
            price: 20_000.25,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        };
        router.note_transition_events(RouterRoot::Nq, &[event]);
        router.persist_journal(&db).expect("persist");
        let nq_second = journal_frame_second(RTH_TS).expect("nq second");
        let es_second = journal_frame_second(RTH_TS + 1_500.0).expect("es second");
        assert_ne!(nq_second, es_second);
        let joined = db
            .list_events_joined_to_journal_frame(nq_second)
            .expect("join");
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0]["rootSymbol"], "NQ");
        let frames = db.list_journal_frames().expect("list");
        assert!(frames
            .iter()
            .any(|f| f.root_symbol == "NQ" && f.frame_second == nq_second));
        assert!(!frames
            .iter()
            .any(|f| f.root_symbol == "NQ" && f.frame_second == es_second));
    }

    #[test]
    fn later_root_does_not_copy_stale_session_onto_new_second() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "stale");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.apply_tick(RouterRoot::Es, &tick(GLOBEX_TS, 5_000.0));
        router.persist_journal(&db).expect("persist");
        let rth_second = journal_frame_second(RTH_TS).expect("rth");
        let globex_second = journal_frame_second(GLOBEX_TS).expect("globex");
        let frames = db.list_journal_frames().expect("list");
        let nq = frames
            .iter()
            .find(|f| f.root_symbol == "NQ")
            .expect("nq frame");
        let es = frames
            .iter()
            .find(|f| f.root_symbol == "ES")
            .expect("es frame");
        assert_eq!(nq.frame_second, rth_second);
        assert_eq!(nq.session_type, "RTH");
        assert_eq!(es.frame_second, globex_second);
        assert_eq!(es.session_type, "Globex");
        assert!(!frames
            .iter()
            .any(|f| f.root_symbol == "NQ" && f.frame_second == globex_second));
        let as_of = db
            .get_journal_frames_as_of(GLOBEX_TS)
            .expect("as_of")
            .expect("present");
        assert!(as_of.by_root.contains_key("ES"));
        assert!(
            !as_of.by_root.contains_key("NQ"),
            "NQ RTH must not appear co-recorded on the Globex second"
        );
    }

    #[test]
    fn persist_journal_writes_lifecycle_and_attention_without_mcp() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "overnight");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        let event = MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: RTH_TS + 100.0,
            event_type: "ib_extension_hit".into(),
            level_name: Some("ib_high".into()),
            price: 20_000.25,
            direction: Some("from_below".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        };
        router.note_transition_events(RouterRoot::Nq, &[event]);
        router.persist_journal(&db).expect("persist");
        let rows = db
            .list_recent_market_events(10, None, Some("ib_extension_hit"))
            .expect("events");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["lifecycle"], "open");
        assert!(rows[0].get("frameRef").is_none());
        assert!(rows[0]["journalFrameSecond"].as_i64().is_some());
        assert!(rows[0]["identityId"].as_str().unwrap().starts_with("evt_"));
        assert!(rows[0]["dedupIdentityId"]
            .as_str()
            .unwrap()
            .starts_with("dedup_"));
        let signals = db
            .query_attention_signals(&crate::db::AttentionSignalQuery {
                include_expired: true,
                limit: 50,
                ..crate::db::AttentionSignalQuery::default()
            })
            .expect("attention");
        assert!(
            !signals.is_empty(),
            "engine persist must write attention without MCP attached"
        );
        assert_eq!(signals[0].payload["viewOf"], "eventStream");
        assert!(signals[0].summary.contains("Your playbook"));
        assert!(!signals[0]
            .summary
            .to_ascii_lowercase()
            .contains("you should buy"));
    }

    fn stop_run_at(ts: f64) -> MarketEvent {
        MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: ts,
            event_type: "stop_run".into(),
            level_name: None,
            price: 20_000.25,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        }
    }

    #[test]
    fn persist_journal_writes_dom_capsules_without_mcp() {
        use crate::engine::{CAPSULE_AFTER_MS, CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS};
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "capsule");
        let event_ts = RTH_TS + CAPSULE_LOOKBACK_MS;
        let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 0..=lookback_n {
            let ts = RTH_TS + i as f64 * CAPSULE_RING_STEP_MS;
            router.apply_tick(RouterRoot::Nq, &tick(ts, 20_000.0));
        }
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(event_ts)]);
        let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 1..=after_n {
            let ts = event_ts + i as f64 * CAPSULE_RING_STEP_MS;
            router.apply_tick(RouterRoot::Nq, &tick(ts, 20_000.25));
        }
        let stats = router.persist_journal(&db).expect("persist");
        assert!(stats.events_written >= 1);
        assert!(stats.capsules_opened >= 1);
        assert!(stats.capsules_finalized >= 1);
        let capsules = db.list_capsules().expect("capsules");
        assert_eq!(capsules.len(), 1);
        let cap = &capsules[0];
        assert_eq!(cap.event_type, "stop_run");
        assert_eq!(cap.completeness, "complete");
        assert!((cap.window_start_ms - (event_ts - CAPSULE_LOOKBACK_MS)).abs() < 1.0);
        assert!((cap.window_end_ms - (event_ts + CAPSULE_AFTER_MS)).abs() < 1.0);
        assert_eq!(
            cap.start_frame_second,
            crate::db::journal_frame_second_from_ts(event_ts - CAPSULE_LOOKBACK_MS)
        );
        assert_eq!(
            cap.end_frame_second,
            crate::db::journal_frame_second_from_ts(event_ts + CAPSULE_AFTER_MS)
        );
        let events = db
            .list_recent_market_events(10, None, Some("stop_run"))
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            cap.trigger_identity_id,
            events[0]["identityId"].as_str().unwrap()
        );
        let frames = db
            .list_journal_frames_for_capsule(
                "NQ",
                cap.start_frame_second.unwrap(),
                cap.end_frame_second.unwrap(),
            )
            .expect("frames");
        assert!(
            !frames.is_empty(),
            "Capsule must join surrounding Journal Frames"
        );
        let frame_count = db.count_journal_frames().expect("frames");
        let span_seconds = cap.end_frame_second.unwrap() - cap.start_frame_second.unwrap() + 1;
        assert!(
            frame_count <= span_seconds + 2,
            "1 Hz frames only, not 250 ms store: frames={frame_count} span={span_seconds}"
        );
    }

    #[test]
    fn persist_keeps_capsule_when_es_clock_expires_nq_trigger() {
        use crate::engine::{CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS};
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "expire-cap");
        let event_ts = RTH_TS + CAPSULE_LOOKBACK_MS;
        let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 0..=lookback_n {
            let ts = RTH_TS + i as f64 * CAPSULE_RING_STEP_MS;
            router.apply_tick(RouterRoot::Nq, &tick(ts, 20_000.0));
        }
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(event_ts)]);
        router.persist_journal(&db).expect("open");
        assert_eq!(db.count_capsules().expect("opened"), 1);
        router.apply_tick(RouterRoot::Es, &tick(event_ts + 40.0 * 60_000.0, 5_000.0));
        router.persist_journal(&db).expect("after es jump");
        assert_eq!(
            db.count_capsules().expect("kept"),
            1,
            "expired trigger must not drop an in-flight Capsule"
        );
        let cap = &db.list_capsules().expect("list")[0];
        assert_eq!(cap.event_type, "stop_run");
        assert_ne!(
            cap.completeness, "complete",
            "NQ Capsule must not complete off the ES lane clock"
        );
    }

    #[test]
    fn non_dom_events_do_not_open_capsules() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "no-cap");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        let event = MarketEvent {
            session_date: "2024-01-02".into(),
            timestamp_ms: RTH_TS + 100.0,
            event_type: "pinch_detected".into(),
            level_name: None,
            price: 20_000.25,
            direction: Some("up".into()),
            sequence_num: Some(1),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2024-01-02".into(),
        };
        router.note_transition_events(RouterRoot::Nq, &[event]);
        router.persist_journal(&db).expect("persist");
        assert_eq!(db.count_capsules().expect("count"), 0);
        assert_eq!(
            db.list_recent_market_events(10, None, Some("pinch_detected"))
                .expect("ev")
                .len(),
            1
        );
    }

    #[test]
    fn repeat_dedup_does_not_spawn_unbounded_capsules() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "dedup-cap");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        let first = stop_run_at(RTH_TS + 100.0);
        let mut repeat = first.clone();
        repeat.timestamp_ms = RTH_TS + 1_100.0;
        router.note_transition_events(RouterRoot::Nq, &[first]);
        router.persist_journal(&db).expect("open");
        router.note_transition_events(RouterRoot::Nq, &[repeat]);
        router.persist_journal(&db).expect("repeat");
        assert_eq!(db.count_capsules().expect("count"), 1);
        let occ = db
            .list_recent_market_events(10, None, Some("stop_run"))
            .expect("occ");
        assert_eq!(occ.len(), 2);
        assert_eq!(occ[0]["lifecycle"], "updated");
    }

    #[test]
    fn updated_lifecycle_still_writes_first_capsule_when_none_exists() {
        let db = Database::open(":memory:").expect("db");
        db.insert_market_events_batch_scoped(Some("NQ"), &[stop_run_at(RTH_TS + 50.0)])
            .expect("backfill");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "updated-first");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(RTH_TS + 1_100.0)]);
        router.persist_journal(&db).expect("persist");
        assert_eq!(
            db.count_capsules().expect("count"),
            1,
            "first live dump must persist even when the occurrence row is already updated"
        );
        let events = db
            .list_recent_market_events(10, None, Some("stop_run"))
            .expect("events");
        assert_eq!(events.len(), 2);
        assert!(
            events.iter().any(|e| e["lifecycle"] == "updated"),
            "prior backfill row must stamp the live occurrence updated"
        );
    }

    #[test]
    fn completed_dedup_repeat_does_not_open_second_capsule() {
        use crate::engine::{CAPSULE_AFTER_MS, CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS};
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "dedup-complete");
        let event_ts = RTH_TS + CAPSULE_LOOKBACK_MS;
        let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 0..=lookback_n {
            router.apply_tick(
                RouterRoot::Nq,
                &tick(RTH_TS + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
            );
        }
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(event_ts)]);
        let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 1..=after_n {
            router.apply_tick(
                RouterRoot::Nq,
                &tick(event_ts + i as f64 * CAPSULE_RING_STEP_MS, 20_000.25),
            );
        }
        router.persist_journal(&db).expect("complete");
        assert_eq!(db.count_capsules().expect("first"), 1);
        assert_eq!(db.list_capsules().unwrap()[0].completeness, "complete");
        let mut repeat = stop_run_at(event_ts + 90_000.0);
        repeat.sequence_num = Some(1);
        router.note_transition_events(RouterRoot::Nq, &[repeat]);
        router.persist_journal(&db).expect("repeat");
        assert_eq!(
            db.count_capsules().expect("still one"),
            1,
            "repeat of the same dedup must not spawn a second Capsule after complete"
        );
    }

    #[test]
    fn restart_finalizes_orphaned_pending_capsules() {
        use crate::engine::CAPSULE_AFTER_MS;
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "orphan-open");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(RTH_TS + 100.0)]);
        router.persist_journal(&db).expect("open pending");
        assert_eq!(db.list_capsules().unwrap()[0].completeness, "pending");
        drop(router);

        let restarted =
            MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "orphan-restart");
        restarted.apply_tick(RouterRoot::Nq, &tick(RTH_TS + 1_000.0, 20_000.25));
        restarted.persist_journal(&db).expect("mid-window restart");
        assert_eq!(
            db.list_capsules().unwrap()[0].completeness,
            "pending",
            "another process must not stamp a still-open after-window as abandoned"
        );

        restarted.apply_tick(
            RouterRoot::Nq,
            &tick(RTH_TS + 100.0 + CAPSULE_AFTER_MS + 5_000.0, 20_000.5),
        );
        restarted.persist_journal(&db).expect("past window");
        let cap = db.list_capsules().expect("caps").pop().expect("one");
        assert_eq!(cap.completeness, "incomplete");
        assert!(cap.degraded);
    }

    #[test]
    fn after_window_feed_gap_marks_incomplete_degraded() {
        use crate::engine::{CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS};
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "gap");
        let event_ts = RTH_TS + CAPSULE_LOOKBACK_MS;
        let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
        for i in 0..=lookback_n {
            router.apply_tick(
                RouterRoot::Nq,
                &tick(RTH_TS + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
            );
        }
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(event_ts)]);
        for i in 1..=20 {
            router.apply_tick(
                RouterRoot::Nq,
                &tick(event_ts + i as f64 * CAPSULE_RING_STEP_MS, 20_000.25),
            );
        }
        router.apply_tick(RouterRoot::Nq, &tick(event_ts + 40.0 * 60_000.0, 20_001.0));
        router.persist_journal(&db).expect("persist");
        let cap = db.list_capsules().expect("caps").pop().expect("one");
        assert_eq!(cap.completeness, "incomplete");
        assert!(cap.degraded);
    }

    #[test]
    fn in_flight_pending_capsule_survives_later_persist() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "inflight");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(RTH_TS + 100.0)]);
        router.persist_journal(&db).expect("open");
        router.persist_journal(&db).expect("again");
        let cap = db.list_capsules().expect("caps").pop().expect("one");
        assert_eq!(cap.completeness, "pending");
        assert!(!cap.degraded);
    }

    #[test]
    fn incomplete_capsule_on_session_end_before_after_window() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "incomplete");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        router.note_transition_events(RouterRoot::Nq, &[stop_run_at(RTH_TS + 100.0)]);
        router.persist_journal(&db).expect("open pending");
        assert_eq!(db.count_capsules().expect("pending count"), 1);
        assert_eq!(db.list_capsules().unwrap()[0].completeness, "pending");
        router.mark_stopped();
        router.persist_journal(&db).expect("flush");
        let cap = db.list_capsules().expect("caps").pop().expect("one");
        assert_eq!(cap.completeness, "incomplete");
        assert!(cap.degraded);
    }

    #[test]
    fn every_dom_family_type_emits_a_capsule() {
        let db = Database::open(":memory:").expect("db");
        let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "all-dom");
        router.apply_tick(RouterRoot::Nq, &tick(RTH_TS, 20_000.0));
        for (i, name) in crate::catalog::DOM_FAMILY_EVENT_TYPES.iter().enumerate() {
            let mut event = stop_run_at(RTH_TS + 100.0 + i as f64);
            event.event_type = (*name).into();
            event.sequence_num = Some(i as i32 + 1);
            router.note_transition_events(RouterRoot::Nq, &[event]);
        }
        router.persist_journal(&db).expect("persist");
        assert_eq!(db.count_capsules().expect("count"), 4);
    }
}
