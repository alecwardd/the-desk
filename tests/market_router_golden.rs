//! Golden replay seam: MarketRouter v0 one-clock NQ+ES alignment.
//!
//! Fixture ticks in → deterministic per-root snapshots + aligned clock.
//! Concurrent merge-sort must match sequential same-clock apply.

use serde_json::{json, Value};
use the_desk_backend::engine::{
    sort_ticks_one_clock, MarketRouter, RouterRoot, SourceProviderKind, SourceTick,
};
use the_desk_backend::feed::TradeSide;

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

fn aligned_ticks() -> Vec<(RouterRoot, SourceTick)> {
    vec![
        (RouterRoot::Nq, tick(RTH_TS, 20_000.0)),
        (RouterRoot::Es, tick(RTH_TS + 100.0, 5_000.0)),
        (RouterRoot::Nq, tick(RTH_TS + 250.0, 20_000.25)),
        (RouterRoot::Es, tick(RTH_TS + 400.0, 5_000.25)),
        (RouterRoot::Nq, tick(RTH_TS + 500.0, 20_000.50)),
    ]
}

fn lane_fingerprint(snap: &Value) -> Value {
    json!({
        "lastPrice": snap.get("lastPrice"),
        "rootSymbol": snap.get("rootSymbol"),
        "sessionType": snap.get("sessionType"),
        "sessionHigh": snap.get("sessionHigh"),
        "sessionLow": snap.get("sessionLow"),
    })
}

fn router_golden(router: &MarketRouter) -> Value {
    json!({
        "clockMs": router.clock_ms(),
        "nq": lane_fingerprint(&router.nq_host().snapshot_market_state()),
        "es": lane_fingerprint(&router.es_host().snapshot_market_state()),
    })
}

#[test]
fn golden_nq_es_aligned_clock_matches_blessed_artifact() {
    let mut ticks = aligned_ticks();
    // Intentionally unsorted input — MarketRouter must order by one clock.
    ticks.swap(0, 3);
    let router = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "golden");
    router.apply_merged(ticks);

    let got = router_golden(&router);
    let blessed = json!({
        "clockMs": RTH_TS + 500.0,
        "nq": {
            "lastPrice": 20_000.50,
            "rootSymbol": "NQ",
            "sessionType": "RTH",
            "sessionHigh": 20_000.50,
            "sessionLow": 20_000.0,
        },
        "es": {
            "lastPrice": 5_000.25,
            "rootSymbol": "ES",
            "sessionType": "RTH",
            "sessionHigh": 5_000.25,
            "sessionLow": 5_000.0,
        },
    });
    assert_eq!(got, blessed);
}

#[test]
fn golden_concurrent_equals_sequential_same_clock() {
    let ticks = aligned_ticks();
    let concurrent = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "concurrent");
    concurrent.apply_merged(ticks.clone());

    let sequential = MarketRouter::new(RouterRoot::Nq, SourceProviderKind::File, "sequential");
    let mut ordered = ticks;
    sort_ticks_one_clock(&mut ordered);
    for (root, tick) in &ordered {
        sequential.apply_tick(*root, tick);
    }
    assert_eq!(router_golden(&concurrent), router_golden(&sequential));
}
