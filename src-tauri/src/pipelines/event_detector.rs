use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::MarketState;
#[allow(unused_imports)]
use crate::pipelines::RvolClassification;

const NQ_TICK: f64 = 0.25;
const PROXIMITY_TICKS: f64 = 2.0;
const PROXIMITY: f64 = PROXIMITY_TICKS * NQ_TICK;
const MIN_EVENT_GAP_MS: f64 = 60_000.0;

/// A structured market event detected during pipeline processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketEvent {
    pub session_date: String,
    pub timestamp_ms: f64,
    pub event_type: String,
    pub level_name: Option<String>,
    pub price: f64,
    pub direction: Option<String>,
    pub sequence_num: Option<i32>,
    pub metadata: Option<serde_json::Value>,
}

fn crossed_level(prev_price: f64, cur_price: f64, level: f64) -> Option<String> {
    if level <= 0.0 || prev_price <= 0.0 {
        return None;
    }
    if prev_price < level - PROXIMITY && cur_price >= level - PROXIMITY {
        Some("from_below".to_string())
    } else if prev_price > level + PROXIMITY && cur_price <= level + PROXIMITY {
        Some("from_above".to_string())
    } else {
        None
    }
}

/// Detects structured events by comparing consecutive MarketState snapshots.
///
/// Maintains minimal internal state: previous price, previous side of each
/// tracked level, event dedup timestamps, and per-session sequence counters.
pub struct EventDetector {
    prev_price: f64,
    prev_session_high: f64,
    prev_session_low: f64,
    prev_day_type: String,
    prev_poor_high: bool,
    prev_poor_low: bool,
    prev_excess_high: bool,
    prev_excess_low: bool,
    prev_or5_mid_retested: bool,
    ib_formed: bool,
    or_formed: bool,
    last_event_ts: HashMap<String, f64>,
    sequence_counts: HashMap<String, i32>,
    session_date: String,
}

impl Default for EventDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl EventDetector {
    pub fn new() -> Self {
        Self {
            prev_price: 0.0,
            prev_session_high: 0.0,
            prev_session_low: 0.0,
            prev_day_type: String::new(),
            prev_poor_high: false,
            prev_poor_low: false,
            prev_excess_high: false,
            prev_excess_low: false,
            prev_or5_mid_retested: false,
            ib_formed: false,
            or_formed: false,
            last_event_ts: HashMap::new(),
            sequence_counts: HashMap::new(),
            session_date: String::new(),
        }
    }

    /// Reset for a new trading session.
    pub fn reset(&mut self) {
        self.prev_price = 0.0;
        self.prev_session_high = 0.0;
        self.prev_session_low = 0.0;
        self.prev_day_type.clear();
        self.prev_poor_high = false;
        self.prev_poor_low = false;
        self.prev_excess_high = false;
        self.prev_excess_low = false;
        self.prev_or5_mid_retested = false;
        self.ib_formed = false;
        self.or_formed = false;
        self.last_event_ts.clear();
        self.sequence_counts.clear();
        self.session_date.clear();
    }

    /// Detect events from the current market state after a trade.
    ///
    /// `minute_of_session` is 0-based from RTH open; negative for Globex.
    pub fn detect(
        &mut self,
        state: &MarketState,
        timestamp_ms: f64,
        session_date: &str,
        minute_of_session: i32,
    ) -> Vec<MarketEvent> {
        if self.session_date != session_date && !session_date.is_empty() {
            self.session_date = session_date.to_string();
        }
        let mut events = Vec::new();
        let price = state.last_price;

        if self.prev_price <= 0.0 {
            self.prev_price = price;
            self.prev_session_high = price;
            self.prev_session_low = price;
            self.prev_day_type = format!("{:?}", state.day_type);
            self.prev_poor_high = state.poor_high;
            self.prev_poor_low = state.poor_low;
            self.prev_excess_high = state.excess_high;
            self.prev_excess_low = state.excess_low;
            self.prev_or5_mid_retested = state.or5_mid_retested;

            // Check one-shot structure events on init
            if !self.ib_formed
                && minute_of_session >= 60
                && state.ib_high > 0.0
                && state.ib_low > 0.0
            {
                self.ib_formed = true;
                let ib_range = state.ib_high - state.ib_low;
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "ib_formed".to_string(),
                    level_name: None,
                    price,
                    direction: None,
                    sequence_num: None,
                    metadata: Some(serde_json::json!({
                        "ibHigh": state.ib_high,
                        "ibLow": state.ib_low,
                        "ibRange": ib_range,
                        "ibMid": ib_mid(state),
                    })),
                });
            }
            if !self.or_formed
                && minute_of_session >= 30
                && state.or_high > 0.0
                && state.or_low > 0.0
            {
                self.or_formed = true;
            }
            return events;
        }

        // --- Level interaction events ---
        let levels: Vec<(&str, f64)> = vec![
            ("ib_high", state.ib_high),
            ("ib_low", state.ib_low),
            ("ib_mid", ib_mid(state)),
            ("vah", state.va_high),
            ("val", state.va_low),
            ("poc", state.poc),
            ("prior_day_high", state.prior_day_high),
            ("prior_day_low", state.prior_day_low),
            ("prior_close", state.prior_day_close),
            ("prior_vah", state.prior_va_high),
            ("prior_val", state.prior_va_low),
            ("prior_poc", state.prior_poc),
            ("overnight_high", state.overnight_high),
            ("overnight_low", state.overnight_low),
            ("vwap", state.vwap),
            ("vwap_1sd_upper", state.vwap_1sd_upper),
            ("vwap_1sd_lower", state.vwap_1sd_lower),
            ("vwap_2sd_upper", state.vwap_2sd_upper),
            ("vwap_2sd_lower", state.vwap_2sd_lower),
            ("dnp", state.dnp),
            ("dnva_high", state.dnva_high),
            ("dnva_low", state.dnva_low),
        ];

        for (name, level) in &levels {
            if *level <= 0.0 {
                continue;
            }
            if let Some(direction) = crossed_level(self.prev_price, price, *level) {
                let event_key = format!("{name}_test");
                if self.should_emit(&event_key, timestamp_ms) {
                    let seq = self.next_sequence(&event_key);
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key,
                        level_name: Some(name.to_string()),
                        price,
                        direction: Some(direction),
                        sequence_num: Some(seq),
                        metadata: Some(serde_json::json!({"levelPrice": level})),
                    });
                }
            }
        }

        // --- IB extension events ---
        if state.ib_high > 0.0 && state.ib_low > 0.0 {
            let ib_range = state.ib_high - state.ib_low;
            if ib_range > 0.0 {
                let extensions: Vec<(&str, f64, f64, &str)> = vec![
                    (
                        "ib_ext_0.5x_high",
                        state.ib_high + ib_range * 0.5,
                        0.5,
                        "up",
                    ),
                    (
                        "ib_ext_0.5x_low",
                        state.ib_low - ib_range * 0.5,
                        0.5,
                        "down",
                    ),
                    ("ib_ext_1.0x_high", state.ib_high + ib_range, 1.0, "up"),
                    ("ib_ext_1.0x_low", state.ib_low - ib_range, 1.0, "down"),
                    (
                        "ib_ext_1.5x_high",
                        state.ib_high + ib_range * 1.5,
                        1.5,
                        "up",
                    ),
                    (
                        "ib_ext_1.5x_low",
                        state.ib_low - ib_range * 1.5,
                        1.5,
                        "down",
                    ),
                ];
                for (name, ext_level, multiplier, dir) in &extensions {
                    if let Some(direction) = crossed_level(self.prev_price, price, *ext_level) {
                        let event_key = format!("{name}_hit");
                        if self.should_emit(&event_key, timestamp_ms) {
                            events.push(MarketEvent {
                                session_date: session_date.to_string(),
                                timestamp_ms,
                                event_type: "ib_extension_hit".to_string(),
                                level_name: Some(name.to_string()),
                                price,
                                direction: Some(direction),
                                sequence_num: None,
                                metadata: Some(serde_json::json!({
                                    "multiplier": multiplier,
                                    "extensionDirection": dir,
                                    "extensionPrice": ext_level,
                                    "ibRange": ib_range,
                                })),
                            });
                        }
                    }
                }
            }
        }

        // --- Structure events ---

        // IB formed (minute 60 of RTH, fire once)
        if !self.ib_formed && minute_of_session >= 60 && state.ib_high > 0.0 && state.ib_low > 0.0 {
            self.ib_formed = true;
            let ib_range = state.ib_high - state.ib_low;
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "ib_formed".to_string(),
                level_name: None,
                price,
                direction: None,
                sequence_num: None,
                metadata: Some(serde_json::json!({
                    "ibHigh": state.ib_high,
                    "ibLow": state.ib_low,
                    "ibRange": ib_range,
                    "ibMid": ib_mid(state),
                })),
            });
        }

        // OR formed (minute 30 of RTH, fire once)
        if !self.or_formed && minute_of_session >= 30 && state.or_high > 0.0 && state.or_low > 0.0 {
            self.or_formed = true;
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "or_formed".to_string(),
                level_name: None,
                price,
                direction: None,
                sequence_num: None,
                metadata: Some(serde_json::json!({
                    "orHigh": state.or_high,
                    "orLow": state.or_low,
                    "orRange": state.or_high - state.or_low,
                })),
            });
        }

        // New session high/low
        if price > self.prev_session_high && self.prev_session_high > 0.0 {
            let event_key = "new_session_high";
            if self.should_emit(event_key, timestamp_ms) {
                let seq = self.next_sequence(event_key);
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: event_key.to_string(),
                    level_name: None,
                    price,
                    direction: Some("up".to_string()),
                    sequence_num: Some(seq),
                    metadata: Some(serde_json::json!({"prevHigh": self.prev_session_high})),
                });
            }
        }
        if price < self.prev_session_low && self.prev_session_low > 0.0 {
            let event_key = "new_session_low";
            if self.should_emit(event_key, timestamp_ms) {
                let seq = self.next_sequence(event_key);
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: event_key.to_string(),
                    level_name: None,
                    price,
                    direction: Some("down".to_string()),
                    sequence_num: Some(seq),
                    metadata: Some(serde_json::json!({"prevLow": self.prev_session_low})),
                });
            }
        }
        self.prev_session_high = self.prev_session_high.max(price);
        self.prev_session_low = if self.prev_session_low <= 0.0 {
            price
        } else {
            self.prev_session_low.min(price)
        };

        // Day type change
        let current_day_type = format!("{:?}", state.day_type);
        if !self.prev_day_type.is_empty() && current_day_type != self.prev_day_type {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "day_type_change".to_string(),
                level_name: None,
                price,
                direction: None,
                sequence_num: None,
                metadata: Some(serde_json::json!({
                    "from": self.prev_day_type,
                    "to": current_day_type,
                })),
            });
        }
        self.prev_day_type = current_day_type;

        // Poor high/low detected
        if state.poor_high && !self.prev_poor_high {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "poor_high_detected".to_string(),
                level_name: None,
                price,
                direction: None,
                sequence_num: None,
                metadata: None,
            });
        }
        self.prev_poor_high = state.poor_high;

        if state.poor_low && !self.prev_poor_low {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "poor_low_detected".to_string(),
                level_name: None,
                price,
                direction: None,
                sequence_num: None,
                metadata: None,
            });
        }
        self.prev_poor_low = state.poor_low;

        // Excess detected
        if state.excess_high && !self.prev_excess_high {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "excess_high_detected".to_string(),
                level_name: None,
                price,
                direction: Some("up".to_string()),
                sequence_num: None,
                metadata: None,
            });
        }
        self.prev_excess_high = state.excess_high;

        if state.excess_low && !self.prev_excess_low {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "excess_low_detected".to_string(),
                level_name: None,
                price,
                direction: Some("down".to_string()),
                sequence_num: None,
                metadata: None,
            });
        }
        self.prev_excess_low = state.excess_low;

        // OR5 mid retest
        if state.or5_mid_retested && !self.prev_or5_mid_retested {
            events.push(MarketEvent {
                session_date: session_date.to_string(),
                timestamp_ms,
                event_type: "or5_mid_retest".to_string(),
                level_name: Some("or5_mid".to_string()),
                price,
                direction: Some(format!("{:?}", state.or5_break_direction)),
                sequence_num: None,
                metadata: Some(serde_json::json!({
                    "or5High": state.or5_high,
                    "or5Low": state.or5_low,
                    "or5Mid": state.or5_mid,
                })),
            });
        }
        self.prev_or5_mid_retested = state.or5_mid_retested;

        // --- Delta/flow events ---

        // DNP cross (price crosses delta neutral pivot)
        if state.dnp > 0.0 {
            if let Some(direction) = crossed_level(self.prev_price, price, state.dnp) {
                let event_key = "dnp_cross";
                if self.should_emit(event_key, timestamp_ms) {
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key.to_string(),
                        level_name: Some("dnp".to_string()),
                        price,
                        direction: Some(direction),
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "dnp": state.dnp,
                            "sessionDelta": state.session_delta,
                        })),
                    });
                }
            }
        }

        // RVOL spike (transition to High classification)
        if matches!(
            state.rvol_classification,
            crate::pipelines::RvolClassification::High
        ) && state.rvol_ratio > 1.15
        {
            let event_key = "rvol_spike";
            if self.should_emit(event_key, timestamp_ms) {
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: event_key.to_string(),
                    level_name: None,
                    price,
                    direction: None,
                    sequence_num: None,
                    metadata: Some(serde_json::json!({
                        "rvolRatio": state.rvol_ratio,
                    })),
                });
            }
        }

        self.prev_price = price;
        events
    }

    /// Check if enough time has passed since the last event of this type.
    fn should_emit(&mut self, event_key: &str, timestamp_ms: f64) -> bool {
        if let Some(&last_ts) = self.last_event_ts.get(event_key) {
            if timestamp_ms - last_ts < MIN_EVENT_GAP_MS {
                return false;
            }
        }
        self.last_event_ts
            .insert(event_key.to_string(), timestamp_ms);
        true
    }

    fn next_sequence(&mut self, event_key: &str) -> i32 {
        let count = self
            .sequence_counts
            .entry(event_key.to_string())
            .or_insert(0);
        *count += 1;
        *count
    }
}

fn ib_mid(state: &MarketState) -> f64 {
    if state.ib_high > 0.0 && state.ib_low > 0.0 {
        (state.ib_high + state.ib_low) / 2.0
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipelines::{
        BalanceState, DayType, InventoryDirection, InventoryState, Or5BreakDirection, ProfileShape,
        RvolClassification, SinglePrintsDirection,
    };

    fn base_state() -> MarketState {
        MarketState {
            last_price: 21000.0,
            bid: 20999.75,
            ask: 21000.25,
            vwap: 21000.0,
            vwap_1sd_upper: 21010.0,
            vwap_1sd_lower: 20990.0,
            vwap_2sd_upper: 21020.0,
            vwap_2sd_lower: 20980.0,
            vwap_3sd_upper: 21030.0,
            vwap_3sd_lower: 20970.0,
            va_high: 21010.0,
            va_low: 20990.0,
            poc: 21000.0,
            dnva_high: 21005.0,
            dnva_low: 20995.0,
            dnp: 21000.0,
            session_delta: 100.0,
            cumulative_delta: 500.0,
            prior_day_high: 21050.0,
            prior_day_low: 20950.0,
            prior_day_close: 21020.0,
            prior_va_high: 21040.0,
            prior_va_low: 20960.0,
            prior_poc: 21010.0,
            overnight_high: 21030.0,
            overnight_low: 20970.0,
            session_high: 21025.0,
            session_low: 20975.0,
            rth_close_price: 21000.0,
            or_high: 21015.0,
            or_low: 20985.0,
            ib_high: 21020.0,
            ib_low: 20980.0,
            tape_pace_5s: 5.0,
            tape_pace_30s: 4.0,
            tape_pace_5m: 3.0,
            tape_acceleration: 1.0,
            pace_percentile: 0.5,
            imbalance_count: 0,
            absorption_event_count: 0,
            avg_trade_size: 2.0,
            or5_high: 21010.0,
            or5_low: 20990.0,
            or5_mid: 21000.0,
            or5_locked: true,
            or5_break_direction: Or5BreakDirection::None,
            or5_mid_retested: false,
            rvol_ratio: 1.0,
            rvol_classification: RvolClassification::Normal,
            day_type: DayType::Normal,
            profile_shape: ProfileShape::Gaussian,
            balance_state: BalanceState::Balanced,
            single_prints_direction: SinglePrintsDirection::None,
            pinch_event_count: 0,
            inventory_state: InventoryState::Neutral,
            inventory_direction: InventoryDirection::Flat,
            sessions_in_trend: 0,
            active_zone_count: 0,
            poor_high: false,
            poor_low: false,
            excess_high: false,
            excess_low: false,
        }
    }

    #[test]
    fn detects_ib_mid_test() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        // IB mid = (21020 + 20980) / 2 = 21000
        let mut s1 = base_state();
        s1.last_price = 20998.0; // below IB mid
        detector.detect(&s1, 1000.0, date, 65);

        let mut s2 = base_state();
        s2.last_price = 21001.0; // crossed above IB mid
        let events = detector.detect(&s2, 2000.0, date, 65);

        assert!(
            events.iter().any(|e| e.event_type == "ib_mid_test"),
            "should detect IB mid test crossing"
        );
    }

    #[test]
    fn deduplicates_events() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let mut s1 = base_state();
        s1.last_price = 20998.0;
        detector.detect(&s1, 1000.0, date, 65);

        let mut s2 = base_state();
        s2.last_price = 21001.0;
        let events1 = detector.detect(&s2, 2000.0, date, 65);
        assert!(events1.iter().any(|e| e.event_type == "ib_mid_test"));

        // Cross back quickly -- should be deduplicated
        let mut s3 = base_state();
        s3.last_price = 20998.0;
        let events2 = detector.detect(&s3, 3000.0, date, 65);
        assert!(
            !events2.iter().any(|e| e.event_type == "ib_mid_test"),
            "should dedup within 60s window"
        );

        // Cross again after gap -- should fire
        let mut s4 = base_state();
        s4.last_price = 21001.0;
        let events3 = detector.detect(&s4, 70_000.0, date, 66);
        assert!(
            events3.iter().any(|e| e.event_type == "ib_mid_test"),
            "should fire after dedup window"
        );
    }

    #[test]
    fn detects_ib_formed() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let s = base_state();
        // Minute 59 -- IB not yet formed
        detector.detect(&s, 1000.0, date, 59);
        // Minute 60 -- IB should form
        let events = detector.detect(&s, 2000.0, date, 60);
        assert!(
            events.iter().any(|e| e.event_type == "ib_formed"),
            "should detect IB formation at minute 60"
        );

        // Should not fire again
        let events2 = detector.detect(&s, 3000.0, date, 61);
        assert!(
            !events2.iter().any(|e| e.event_type == "ib_formed"),
            "should only fire once"
        );
    }

    #[test]
    fn detects_new_session_high() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let mut s1 = base_state();
        s1.last_price = 21000.0;
        detector.detect(&s1, 1000.0, date, 5);

        let mut s2 = base_state();
        s2.last_price = 21001.0;
        let events = detector.detect(&s2, 2000.0, date, 5);
        assert!(events.iter().any(|e| e.event_type == "new_session_high"));
    }

    #[test]
    fn detects_day_type_change() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let s1 = base_state();
        detector.detect(&s1, 1000.0, date, 30);

        let mut s2 = base_state();
        s2.day_type = DayType::Trend;
        let events = detector.detect(&s2, 2000.0, date, 60);
        assert!(events.iter().any(|e| e.event_type == "day_type_change"));
    }

    #[test]
    fn detects_poor_high() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let s1 = base_state();
        detector.detect(&s1, 1000.0, date, 30);

        let mut s2 = base_state();
        s2.poor_high = true;
        let events = detector.detect(&s2, 2000.0, date, 60);
        assert!(events.iter().any(|e| e.event_type == "poor_high_detected"));
    }

    #[test]
    fn reset_clears_state() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        let s = base_state();
        detector.detect(&s, 1000.0, date, 60);
        assert!(detector.ib_formed);

        detector.reset();
        assert!(!detector.ib_formed);
        assert!(detector.sequence_counts.is_empty());
    }
}
