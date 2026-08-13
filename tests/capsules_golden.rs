//! Golden/fixture: Capsules around an injected DOM-family Event.
//!
//! Detectors do not emit `stop_run` yet (SIL-M5e). This fixture injects a
//! synthetic `stop_run` MarketEvent so Capsule policy can be asserted on the
//! MarketRouter clock: ~30 s before → ~60 s after, joinable to the event and
//! 1 Hz Journal Frames. No 250 ms forever-store.

use the_desk_backend::db::{journal_frame_second_from_ts, Database};
use the_desk_backend::engine::{
    MarketRouter, RouterRoot, SourceProviderKind, SourceTick, CAPSULE_AFTER_MS,
    CAPSULE_LOOKBACK_MS, CAPSULE_RING_STEP_MS,
};
use the_desk_backend::feed::TradeSide;
use the_desk_backend::pipelines::MarketEvent;

/// 2024-01-02 10:00 ET (RTH).
const RTH_TS: f64 = 1_704_207_600_000.0;

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

fn stop_run(ts: f64) -> MarketEvent {
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
fn golden_stop_run_capsule_window_on_market_clock() {
    let db = Database::open(":memory:").expect("db");
    let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "golden-capsule");
    let event_ts = RTH_TS + CAPSULE_LOOKBACK_MS;
    let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 0..=lookback_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(RTH_TS + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
        );
        router.apply_tick(
            RouterRoot::Es,
            &tick(RTH_TS + i as f64 * CAPSULE_RING_STEP_MS + 10.0, 5_000.0),
        );
    }
    router.note_transition_events(RouterRoot::Nq, &[stop_run(event_ts)]);
    let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 1..=after_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(event_ts + i as f64 * CAPSULE_RING_STEP_MS, 20_000.25),
        );
    }
    router.persist_journal(&db).expect("persist");

    let capsules = db.list_capsules().expect("capsules");
    assert_eq!(
        capsules.len(),
        1,
        "exactly one Capsule for the injected stop_run"
    );
    let cap = &capsules[0];
    assert_eq!(cap.event_type, "stop_run");
    assert_eq!(cap.root_symbol, "NQ");
    assert_eq!(cap.completeness, "complete");
    assert!(!cap.degraded);
    assert!((cap.window_start_ms - (event_ts - 30_000.0)).abs() < 1.0);
    assert!((cap.window_end_ms - (event_ts + 60_000.0)).abs() < 1.0);
    assert_eq!(
        cap.start_frame_second,
        journal_frame_second_from_ts(event_ts - 30_000.0)
    );
    assert_eq!(
        cap.end_frame_second,
        journal_frame_second_from_ts(event_ts + 60_000.0)
    );
    assert!(
        cap.sample_count >= 2,
        "dump must include ring samples, got {}",
        cap.sample_count
    );

    let events = db
        .list_recent_market_events(5, None, Some("stop_run"))
        .expect("event");
    assert_eq!(events.len(), 1);
    assert_eq!(
        cap.trigger_identity_id,
        events[0]["identityId"].as_str().unwrap()
    );
    assert_eq!(events[0]["rootSymbol"], "NQ");

    let frames = db
        .list_journal_frames_for_capsule(
            &cap.root_symbol,
            cap.start_frame_second.expect("start"),
            cap.end_frame_second.expect("end"),
        )
        .expect("join frames");
    assert!(
        frames.iter().any(|f| f.root_symbol == "NQ"),
        "Capsule must join NQ Journal Frames on frame_second × root"
    );

    let total_frames = db.count_journal_frames().expect("frame count");
    let ticks = (lookback_n + after_n + 1) as i64;
    assert!(
        total_frames < ticks,
        "must not persist the 250 ms ring: frames={total_frames} ticks={ticks}"
    );
}
