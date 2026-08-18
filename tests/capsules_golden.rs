//! Golden/fixture: Capsules around DOM-family Events.
//!
//! The injected `stop_run` row keeps the original window invariant (30s before /
//! 60s after, one Capsule per `open`). A second fixture drives a real
//! `DomClusterPipeline` stop-run so Capsules also open on detector-emitted
//! types, not only injected rows. No 250 ms forever-store.

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
    let min_samples =
        ((CAPSULE_LOOKBACK_MS + CAPSULE_AFTER_MS) / CAPSULE_RING_STEP_MS * 0.9) as i64;
    assert!(
        cap.sample_count >= min_samples,
        "250 ms ring must populate the window: sample_count={} min={}",
        cap.sample_count,
        min_samples
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

fn book_snapshot(
    ts: f64,
    bid_qty: u32,
    ask_qty: u32,
    bid: f64,
    ask: f64,
) -> the_desk_backend::depth::DomSnapshot {
    use the_desk_backend::depth::{DepthBook, DepthCommand, DepthRecord, DepthSide};
    let mut book = DepthBook::default();
    book.apply(&DepthRecord {
        timestamp_ms: ts,
        command: DepthCommand::AddBidLevel,
        side: Some(DepthSide::Bid),
        end_of_batch: true,
        num_orders: 1,
        price: bid,
        quantity: bid_qty,
    });
    book.apply(&DepthRecord {
        timestamp_ms: ts,
        command: DepthCommand::AddAskLevel,
        side: Some(DepthSide::Ask),
        end_of_batch: true,
        num_orders: 1,
        price: ask,
        quantity: ask_qty,
    });
    book.snapshot("golden.depth", ts, 10)
}

#[test]
fn golden_detector_driven_stop_run_opens_capsule() {
    use the_desk_backend::depth::PullStackActivitySummary;

    let db = Database::open(":memory:").expect("db");
    let router = MarketRouter::new(
        RouterRoot::Nq,
        SourceProviderKind::File,
        "golden-detector-capsule",
    );
    let start = RTH_TS;
    let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 0..=lookback_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(start + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
        );
        router.apply_tick(
            RouterRoot::Es,
            &tick(start + i as f64 * CAPSULE_RING_STEP_MS + 10.0, 5_000.0),
        );
    }

    let seed_ts = start + CAPSULE_LOOKBACK_MS;
    let activity = PullStackActivitySummary::default();
    router
        .apply_dom_update(
            RouterRoot::Nq,
            &book_snapshot(seed_ts, 80, 80, 20_000.0, 20_000.25),
            &activity,
            seed_ts,
        )
        .expect("seed book");

    let mut last_ts = seed_ts;
    for i in 1..=10 {
        last_ts = seed_ts + i as f64 * 80.0;
        let price = 20_000.0 + i as f64 * 0.25;
        router.apply_tick(RouterRoot::Nq, &tick(last_ts, price));
        let ask_qty = if i >= 6 { 20 } else { 80 };
        router
            .apply_dom_update(
                RouterRoot::Nq,
                &book_snapshot(last_ts, 80, ask_qty, price - 0.25, price),
                &activity,
                last_ts,
            )
            .expect("thin book");
    }

    let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 1..=after_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(last_ts + i as f64 * CAPSULE_RING_STEP_MS, 20_002.5),
        );
    }
    router.persist_journal(&db).expect("persist");

    let events = db
        .list_recent_market_events(20, None, Some("stop_run"))
        .expect("events");
    assert!(
        !events.is_empty(),
        "detector-driven stop_run must persist (not only injected rows)"
    );
    let capsules = db.list_capsules().expect("capsules");
    assert!(
        capsules.iter().any(|c| c.event_type == "stop_run"),
        "every detector-driven stop_run must open a Capsule; capsules={capsules:?}"
    );
    let cap = capsules
        .iter()
        .find(|c| c.event_type == "stop_run")
        .expect("stop_run capsule");
    assert!((cap.window_end_ms - cap.window_start_ms - 90_000.0).abs() < 1.0);
    assert_eq!(
        cap.trigger_identity_id,
        events[0]["identityId"].as_str().unwrap()
    );
    let total_frames = db.count_journal_frames().expect("frame count");
    assert!(
        total_frames < (lookback_n + after_n + 20) as i64,
        "must not persist the 250 ms ring"
    );
}

fn pull_heavy_bid() -> the_desk_backend::depth::PullStackActivitySummary {
    use the_desk_backend::depth::{PullStackActivitySummary, SideActivitySummary};
    PullStackActivitySummary {
        source_file: "golden.depth".into(),
        bid: SideActivitySummary {
            add_events: 0,
            modify_up_events: 0,
            modify_down_events: 4,
            delete_events: 4,
            stacked_quantity: 0.0,
            removed_quantity: 80.0,
            estimated_filled_quantity: 10.0,
            estimated_pulled_quantity: 70.0,
        },
        ask: SideActivitySummary {
            add_events: 2,
            modify_up_events: 2,
            modify_down_events: 0,
            delete_events: 0,
            stacked_quantity: 40.0,
            removed_quantity: 5.0,
            estimated_filled_quantity: 5.0,
            estimated_pulled_quantity: 0.0,
        },
        ..Default::default()
    }
}

#[test]
fn golden_detector_driven_pull_intent_opens_capsule() {
    let db = Database::open(":memory:").expect("db");
    let router = MarketRouter::new(
        RouterRoot::Nq,
        SourceProviderKind::File,
        "golden-detector-pull",
    );
    let start = RTH_TS;
    let lookback_n = (CAPSULE_LOOKBACK_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 0..=lookback_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(start + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
        );
        router.apply_tick(
            RouterRoot::Es,
            &tick(start + i as f64 * CAPSULE_RING_STEP_MS + 10.0, 5_000.0),
        );
    }

    let event_ts = start + CAPSULE_LOOKBACK_MS;
    router
        .apply_dom_update(
            RouterRoot::Nq,
            &book_snapshot(event_ts, 10, 40, 20_000.0, 20_000.25),
            &pull_heavy_bid(),
            event_ts,
        )
        .expect("pull-intent book");

    let after_n = (CAPSULE_AFTER_MS / CAPSULE_RING_STEP_MS) as i32;
    for i in 1..=after_n {
        router.apply_tick(
            RouterRoot::Nq,
            &tick(event_ts + i as f64 * CAPSULE_RING_STEP_MS, 20_000.0),
        );
    }
    router.persist_journal(&db).expect("persist");

    let events = db
        .list_recent_market_events(20, None, Some("pull_intent"))
        .expect("events");
    assert!(
        !events.is_empty(),
        "detector-driven pull_intent must persist (not only injected rows)"
    );
    let capsules = db.list_capsules().expect("capsules");
    assert!(
        capsules.iter().any(|c| c.event_type == "pull_intent"),
        "every detector-driven pull_intent must open a Capsule; capsules={capsules:?}"
    );
    let cap = capsules
        .iter()
        .find(|c| c.event_type == "pull_intent")
        .expect("pull_intent capsule");
    assert!((cap.window_end_ms - cap.window_start_ms - 90_000.0).abs() < 1.0);
    assert_eq!(
        cap.trigger_identity_id,
        events[0]["identityId"].as_str().unwrap()
    );
}
