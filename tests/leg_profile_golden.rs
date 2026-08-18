//! Focused golden for SIL-M5d: a known up-swing then reversal through
//! `PipelineEngine` (not the two-session SCID replay).

use serde_json::{json, Value};
use the_desk_backend::pipelines::{
    EVENT_LEG_COMPLETED, EVENT_LEG_STARTED, STATUS_ACTIVE, STATUS_INSUFFICIENT,
};

fn swing_sequence_actual() -> Value {
    let mut engine = the_desk_backend::pipelines::PipelineEngine::new();
    engine
        .leg_profile
        .on_trade(0.0, 21_000.0, 10.0, true, false);
    engine
        .leg_profile
        .on_trade(5_000.0, 21_008.0, 50.0, true, false);
    engine
        .leg_profile
        .on_trade(16_000.0, 21_016.0, 10.0, true, false);
    let mature = engine
        .leg_profile
        .snapshot(16_000.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    assert_eq!(mature.status, STATUS_ACTIVE);
    assert_eq!(mature.direction.as_deref(), Some("up"));
    assert_eq!(mature.poc, 21_008.0);

    engine
        .leg_profile
        .on_trade(20_000.0, 21_008.0, 8.0, false, false);
    let after = engine
        .leg_profile
        .snapshot(20_000.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    let event_types: Vec<&str> = engine
        .leg_profile
        .recent_events()
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();
    json!({
        "fixtureVersion": 1,
        "sequence": "up-swing then 8-point reversal",
        "afterReversal": {
            "status": after.status,
            "direction": after.direction,
            "anchorPrice": after.anchor_price,
            "volume": after.volume,
            "netDelta": after.net_delta,
            "poc": after.poc,
            "lastDirection": after.last_direction,
            "lastVolume": after.last_volume,
            "lastNetDelta": after.last_net_delta,
            "lastPoc": after.last_poc
        },
        "eventTypes": event_types
    })
}

#[test]
fn known_up_swing_then_reversal_matches_leg_profile_golden() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/leg_profile/expected_swing_sequence.json"
    );
    let expected: Value = serde_json::from_str(
        &std::fs::read_to_string(path).expect("read expected_swing_sequence.json"),
    )
    .expect("parse expected_swing_sequence.json");
    let actual = swing_sequence_actual();
    assert_eq!(actual, expected);
    assert_eq!(actual["afterReversal"]["status"], STATUS_INSUFFICIENT);
    assert_eq!(
        actual["eventTypes"],
        json!([EVENT_LEG_STARTED, EVENT_LEG_COMPLETED, EVENT_LEG_STARTED])
    );
}
