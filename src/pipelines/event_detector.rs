use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{DayType, MarketState};
#[allow(unused_imports)]
use crate::pipelines::RvolClassification;

const NQ_TICK: f64 = 0.25;
const PROXIMITY_TICKS: f64 = 2.0;
const PROXIMITY: f64 = PROXIMITY_TICKS * NQ_TICK;
const MIN_EVENT_GAP_MS: f64 = 60_000.0;
pub const IB_EXTENSION_RATIO: f64 = 0.5;
pub const IB_EXTENSION_DIRECTION_METADATA_KEY: &str = "extensionDirection";
pub const IB_EXTENSION_DIRECTION_UP: &str = "up";
pub const IB_EXTENSION_DIRECTION_DOWN: &str = "down";

pub fn ib_extension_direction_from_metadata(metadata: Option<&serde_json::Value>) -> Option<&str> {
    metadata?
        .get(IB_EXTENSION_DIRECTION_METADATA_KEY)?
        .as_str()
        .filter(|direction| {
            *direction == IB_EXTENSION_DIRECTION_UP || *direction == IB_EXTENSION_DIRECTION_DOWN
        })
}

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
    pub session_type: String,
    pub session_segment: String,
    pub trading_day: String,
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

fn normalize_session_type(value: &str) -> String {
    if value.eq_ignore_ascii_case("rth") {
        "RTH".to_string()
    } else if value.eq_ignore_ascii_case("globex") {
        "Globex".to_string()
    } else {
        "Unknown".to_string()
    }
}

fn normalize_session_segment(value: &str, session_type: &str) -> String {
    if session_type != "Globex" {
        return "None".to_string();
    }
    if value.eq_ignore_ascii_case("asia") {
        "Asia".to_string()
    } else if value.eq_ignore_ascii_case("london") {
        "London".to_string()
    } else {
        "None".to_string()
    }
}

fn event_allowed_in_session(
    event_type: &str,
    level_name: Option<&str>,
    session_type: &str,
) -> bool {
    if session_type == "RTH" {
        return true;
    }
    match event_type {
        "ib_formed"
        | "or_formed"
        | "or5_mid_retest"
        | "ib_extension_hit"
        | "day_type_change"
        | "ib_reentry"
        | "ib_reentry_hit_mid"
        | "ib_reentry_full_traverse" => {
            return false;
        }
        _ => {}
    }
    if event_type.ends_with("_test")
        && level_name
            .map(|name| name.starts_with("ib_"))
            .unwrap_or(false)
    {
        return false;
    }
    true
}

/// Tracks price position relative to IB range.
#[derive(Debug, Clone, Copy, PartialEq)]
enum IbPosition {
    Unknown,
    Inside,
    Above,
    Below,
}

/// Detects structured events by comparing consecutive MarketState snapshots.
///
/// Maintains minimal internal state: previous price, previous side of each
/// tracked level, event dedup timestamps, and per-session sequence counters.
pub struct EventDetector {
    prev_price: f64,
    prev_session_high: f64,
    prev_session_low: f64,
    prev_day_type: DayType,
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
    // IB reentry tracking
    ib_position: IbPosition,
    /// Which side price re-entered from: "high" or "low". None when not tracking.
    ib_reentry_from: Option<String>,
    ib_reentry_hit_mid: bool,
    ib_reentry_traversed: bool,
    /// Max excursion into IB (in points) during active reentry tracking.
    ib_reentry_max_penetration: f64,
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
            prev_day_type: DayType::default(),
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
            ib_position: IbPosition::Unknown,
            ib_reentry_from: None,
            ib_reentry_hit_mid: false,
            ib_reentry_traversed: false,
            ib_reentry_max_penetration: 0.0,
        }
    }

    fn push_event_with_context(
        &self,
        state: &MarketState,
        session_date: &str,
        events: &mut Vec<MarketEvent>,
        mut event: MarketEvent,
    ) {
        let session_type = normalize_session_type(&state.session_type);
        let session_segment = normalize_session_segment(&state.session_segment, &session_type);
        if !event_allowed_in_session(
            &event.event_type,
            event.level_name.as_deref(),
            &session_type,
        ) {
            return;
        }
        event.session_type = session_type;
        event.session_segment = session_segment;
        event.trading_day = if state.trading_day.is_empty() {
            session_date.to_string()
        } else {
            state.trading_day.clone()
        };
        events.push(event);
    }

    /// Reset for a new trading session.
    pub fn reset(&mut self) {
        self.prev_price = 0.0;
        self.prev_session_high = 0.0;
        self.prev_session_low = 0.0;
        self.prev_day_type = DayType::default();
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
        self.ib_position = IbPosition::Unknown;
        self.ib_reentry_from = None;
        self.ib_reentry_hit_mid = false;
        self.ib_reentry_traversed = false;
        self.ib_reentry_max_penetration = 0.0;
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
        let mut events = Vec::new();
        self.detect_into(
            state,
            timestamp_ms,
            session_date,
            minute_of_session,
            &mut events,
        );
        events
    }

    pub fn detect_into(
        &mut self,
        state: &MarketState,
        timestamp_ms: f64,
        session_date: &str,
        minute_of_session: i32,
        events: &mut Vec<MarketEvent>,
    ) {
        if self.session_date != session_date && !session_date.is_empty() {
            self.session_date = session_date.to_string();
        }
        let price = state.last_price;

        if self.prev_price <= 0.0 {
            self.prev_price = price;
            self.prev_session_high = price;
            self.prev_session_low = price;
            self.prev_day_type = state.day_type;
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
                self.push_event_with_context(
                    state,
                    session_date,
                    events,
                    MarketEvent {
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
                        session_type: String::new(),
                        session_segment: String::new(),
                        trading_day: String::new(),
                    },
                );
            }
            if !self.or_formed
                && minute_of_session >= 30
                && state.or_high > 0.0
                && state.or_low > 0.0
            {
                self.or_formed = true;
            }
            return;
        }

        // --- Level interaction events ---
        let levels = [
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

        for (name, level) in levels {
            if level <= 0.0 {
                continue;
            }
            if let Some(direction) = crossed_level(self.prev_price, price, level) {
                let event_key = format!("{name}_test");
                if self.should_emit(&event_key, timestamp_ms) {
                    let seq = self.next_sequence(&event_key);
                    self.push_event_with_context(
                        state,
                        session_date,
                        events,
                        MarketEvent {
                            session_date: session_date.to_string(),
                            timestamp_ms,
                            event_type: event_key,
                            level_name: Some(name.to_string()),
                            price,
                            direction: Some(direction),
                            sequence_num: Some(seq),
                            metadata: Some(serde_json::json!({"levelPrice": level})),
                            session_type: String::new(),
                            session_segment: String::new(),
                            trading_day: String::new(),
                        },
                    );
                }
            }
        }

        // --- IB extension events ---
        if state.ib_high > 0.0 && state.ib_low > 0.0 {
            let ib_range = state.ib_high - state.ib_low;
            if ib_range > 0.0 {
                let extensions = [
                    (
                        "ib_ext_0.5x_high",
                        state.ib_high + ib_range * IB_EXTENSION_RATIO,
                        IB_EXTENSION_RATIO,
                        IB_EXTENSION_DIRECTION_UP,
                    ),
                    (
                        "ib_ext_0.5x_low",
                        state.ib_low - ib_range * IB_EXTENSION_RATIO,
                        IB_EXTENSION_RATIO,
                        IB_EXTENSION_DIRECTION_DOWN,
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
                for (name, ext_level, multiplier, dir) in extensions {
                    if let Some(direction) = crossed_level(self.prev_price, price, ext_level) {
                        let event_key = format!("{name}_hit");
                        if self.should_emit(&event_key, timestamp_ms) {
                            self.push_event_with_context(
                                state,
                                session_date,
                                events,
                                MarketEvent {
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
                                    session_type: String::new(),
                                    session_segment: String::new(),
                                    trading_day: String::new(),
                                },
                            );
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
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
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
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }

        // OR formed (minute 30 of RTH, fire once)
        if !self.or_formed && minute_of_session >= 30 && state.or_high > 0.0 && state.or_low > 0.0 {
            self.or_formed = true;
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
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
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }

        // New session high/low
        if price > self.prev_session_high && self.prev_session_high > 0.0 {
            let event_key = "new_session_high";
            if self.should_emit(event_key, timestamp_ms) {
                let seq = self.next_sequence(event_key);
                self.push_event_with_context(
                    state,
                    session_date,
                    events,
                    MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key.to_string(),
                        level_name: None,
                        price,
                        direction: Some("up".to_string()),
                        sequence_num: Some(seq),
                        metadata: Some(serde_json::json!({"prevHigh": self.prev_session_high})),
                        session_type: String::new(),
                        session_segment: String::new(),
                        trading_day: String::new(),
                    },
                );
            }
        }
        if price < self.prev_session_low && self.prev_session_low > 0.0 {
            let event_key = "new_session_low";
            if self.should_emit(event_key, timestamp_ms) {
                let seq = self.next_sequence(event_key);
                self.push_event_with_context(
                    state,
                    session_date,
                    events,
                    MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key.to_string(),
                        level_name: None,
                        price,
                        direction: Some("down".to_string()),
                        sequence_num: Some(seq),
                        metadata: Some(serde_json::json!({"prevLow": self.prev_session_low})),
                        session_type: String::new(),
                        session_segment: String::new(),
                        trading_day: String::new(),
                    },
                );
            }
        }
        self.prev_session_high = self.prev_session_high.max(price);
        self.prev_session_low = if self.prev_session_low <= 0.0 {
            price
        } else {
            self.prev_session_low.min(price)
        };

        // Day type change
        let current_day_type = state.day_type;
        if current_day_type != self.prev_day_type {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "day_type_change".to_string(),
                    level_name: None,
                    price,
                    direction: None,
                    sequence_num: None,
                    metadata: Some(serde_json::json!({
                        "from": format!("{:?}", self.prev_day_type),
                        "to": format!("{:?}", current_day_type),
                    })),
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_day_type = current_day_type;

        // Poor high/low detected
        if state.poor_high && !self.prev_poor_high {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "poor_high_detected".to_string(),
                    level_name: None,
                    price,
                    direction: None,
                    sequence_num: None,
                    metadata: None,
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_poor_high = state.poor_high;

        if state.poor_low && !self.prev_poor_low {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "poor_low_detected".to_string(),
                    level_name: None,
                    price,
                    direction: None,
                    sequence_num: None,
                    metadata: None,
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_poor_low = state.poor_low;

        // Excess detected
        if state.excess_high && !self.prev_excess_high {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "excess_high_detected".to_string(),
                    level_name: None,
                    price,
                    direction: Some("up".to_string()),
                    sequence_num: None,
                    metadata: None,
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_excess_high = state.excess_high;

        if state.excess_low && !self.prev_excess_low {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms,
                    event_type: "excess_low_detected".to_string(),
                    level_name: None,
                    price,
                    direction: Some("down".to_string()),
                    sequence_num: None,
                    metadata: None,
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_excess_low = state.excess_low;

        // OR5 mid retest
        if state.or5_mid_retested && !self.prev_or5_mid_retested {
            self.push_event_with_context(
                state,
                session_date,
                events,
                MarketEvent {
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
                    session_type: String::new(),
                    session_segment: String::new(),
                    trading_day: String::new(),
                },
            );
        }
        self.prev_or5_mid_retested = state.or5_mid_retested;

        // --- Delta/flow events ---

        // DNP cross (price crosses delta neutral pivot — midpoint of DNVA)
        if state.dnp > 0.0 {
            if let Some(direction) = crossed_level(self.prev_price, price, state.dnp) {
                let event_key = "dnp_cross";
                if self.should_emit(event_key, timestamp_ms) {
                    self.push_event_with_context(
                        state,
                        session_date,
                        events,
                        MarketEvent {
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
                            session_type: String::new(),
                            session_segment: String::new(),
                            trading_day: String::new(),
                        },
                    );
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
                self.push_event_with_context(
                    state,
                    session_date,
                    events,
                    MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key.to_string(),
                        level_name: None,
                        price,
                        direction: None,
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "rvolRatio": state.rvol_ratio,
                            "rvolVelocity": state.rvol_velocity,
                        })),
                        session_type: String::new(),
                        session_segment: String::new(),
                        trading_day: String::new(),
                    },
                );
            }
        }

        // RVOL snapshot at IB close (bucket 12 = minute 60, end of Initial Balance period).
        // Fires once per session to record the RVOL regime context at the IB close.
        if state.rvol_bucket_index == 12 {
            let event_key = "rvol_at_ib_close";
            if self.should_emit(event_key, timestamp_ms) {
                self.push_event_with_context(
                    state,
                    session_date,
                    events,
                    MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: event_key.to_string(),
                        level_name: None,
                        price,
                        direction: None,
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "rvolRatio": state.rvol_ratio,
                            "rvolClassification": format!("{:?}", state.rvol_classification),
                            "rvolPercentile": state.rvol_percentile,
                            "rvolVelocity": state.rvol_velocity,
                            "bucket": state.rvol_bucket_index,
                        })),
                        session_type: String::new(),
                        session_segment: String::new(),
                        trading_day: String::new(),
                    },
                );
            }
        }

        // --- IB reentry tracking ---
        // Only track after IB is formed and we have valid IB levels.
        if self.ib_formed && state.ib_high > 0.0 && state.ib_low > 0.0 {
            let ib_high = state.ib_high;
            let ib_low = state.ib_low;
            let ib_range = ib_high - ib_low;
            let mid = (ib_high + ib_low) / 2.0;

            // Determine current position relative to IB
            let cur_pos = if price > ib_high {
                IbPosition::Above
            } else if price < ib_low {
                IbPosition::Below
            } else {
                IbPosition::Inside
            };

            // Detect reentry: transition from Above/Below → Inside
            if self.ib_position == IbPosition::Above && cur_pos == IbPosition::Inside {
                // Price re-entered IB from above (was extended above IB high)
                let event_key = "ib_reentry";
                if self.should_emit(event_key, timestamp_ms) {
                    let seq = self.next_sequence(event_key);
                    self.push_event_with_context(
                        state,
                        session_date,
                        events,
                        MarketEvent {
                            session_date: session_date.to_string(),
                            timestamp_ms,
                            event_type: event_key.to_string(),
                            level_name: Some("ib_high".to_string()),
                            price,
                            direction: Some("from_above".to_string()),
                            sequence_num: Some(seq),
                            metadata: Some(serde_json::json!({
                                "reentrySide": "high",
                                "ibHigh": ib_high,
                                "ibLow": ib_low,
                                "ibMid": mid,
                                "ibRange": ib_range,
                                "dayType": format!("{:?}", state.day_type),
                                "sessionDelta": state.session_delta,
                            })),
                            session_type: String::new(),
                            session_segment: String::new(),
                            trading_day: String::new(),
                        },
                    );
                }
                self.ib_reentry_from = Some("high".to_string());
                self.ib_reentry_hit_mid = false;
                self.ib_reentry_traversed = false;
                self.ib_reentry_max_penetration = ib_high - price;
            } else if self.ib_position == IbPosition::Below && cur_pos == IbPosition::Inside {
                // Price re-entered IB from below (was extended below IB low)
                let event_key = "ib_reentry";
                if self.should_emit(event_key, timestamp_ms) {
                    let seq = self.next_sequence(event_key);
                    self.push_event_with_context(
                        state,
                        session_date,
                        events,
                        MarketEvent {
                            session_date: session_date.to_string(),
                            timestamp_ms,
                            event_type: event_key.to_string(),
                            level_name: Some("ib_low".to_string()),
                            price,
                            direction: Some("from_below".to_string()),
                            sequence_num: Some(seq),
                            metadata: Some(serde_json::json!({
                                "reentrySide": "low",
                                "ibHigh": ib_high,
                                "ibLow": ib_low,
                                "ibMid": mid,
                                "ibRange": ib_range,
                                "dayType": format!("{:?}", state.day_type),
                                "sessionDelta": state.session_delta,
                            })),
                            session_type: String::new(),
                            session_segment: String::new(),
                            trading_day: String::new(),
                        },
                    );
                }
                self.ib_reentry_from = Some("low".to_string());
                self.ib_reentry_hit_mid = false;
                self.ib_reentry_traversed = false;
                self.ib_reentry_max_penetration = price - ib_low;
            }

            // Track outcomes during active reentry
            if let Some(ref side) = self.ib_reentry_from.clone() {
                match side.as_str() {
                    "high" => {
                        // Re-entered from above: tracking travel toward IB low
                        let penetration = ib_high - price;
                        if penetration > self.ib_reentry_max_penetration {
                            self.ib_reentry_max_penetration = penetration;
                        }

                        // Hit IB mid?
                        if !self.ib_reentry_hit_mid && price <= mid {
                            self.ib_reentry_hit_mid = true;
                            self.push_event_with_context(
                                state,
                                session_date,
                                events,
                                MarketEvent {
                                    session_date: session_date.to_string(),
                                    timestamp_ms,
                                    event_type: "ib_reentry_hit_mid".to_string(),
                                    level_name: Some("ib_mid".to_string()),
                                    price,
                                    direction: Some("from_above".to_string()),
                                    sequence_num: None,
                                    metadata: Some(serde_json::json!({
                                        "reentrySide": "high",
                                        "ibMid": mid,
                                        "ibRange": ib_range,
                                        "penetrationPoints": self.ib_reentry_max_penetration,
                                    })),
                                    session_type: String::new(),
                                    session_segment: String::new(),
                                    trading_day: String::new(),
                                },
                            );
                        }

                        // Full traverse to opposite side?
                        if !self.ib_reentry_traversed && price <= ib_low {
                            self.ib_reentry_traversed = true;
                            self.push_event_with_context(
                                state,
                                session_date,
                                events,
                                MarketEvent {
                                    session_date: session_date.to_string(),
                                    timestamp_ms,
                                    event_type: "ib_reentry_full_traverse".to_string(),
                                    level_name: Some("ib_low".to_string()),
                                    price,
                                    direction: Some("from_above".to_string()),
                                    sequence_num: None,
                                    metadata: Some(serde_json::json!({
                                        "reentrySide": "high",
                                        "ibRange": ib_range,
                                        "traversePoints": ib_range,
                                    })),
                                    session_type: String::new(),
                                    session_segment: String::new(),
                                    trading_day: String::new(),
                                },
                            );
                            // Done tracking — full traverse completed
                            self.ib_reentry_from = None;
                        }

                        // Price exited back above IB — reentry failed, stop tracking
                        if cur_pos == IbPosition::Above {
                            self.ib_reentry_from = None;
                        }
                    }
                    "low" => {
                        // Re-entered from below: tracking travel toward IB high
                        let penetration = price - ib_low;
                        if penetration > self.ib_reentry_max_penetration {
                            self.ib_reentry_max_penetration = penetration;
                        }

                        // Hit IB mid?
                        if !self.ib_reentry_hit_mid && price >= mid {
                            self.ib_reentry_hit_mid = true;
                            self.push_event_with_context(
                                state,
                                session_date,
                                events,
                                MarketEvent {
                                    session_date: session_date.to_string(),
                                    timestamp_ms,
                                    event_type: "ib_reentry_hit_mid".to_string(),
                                    level_name: Some("ib_mid".to_string()),
                                    price,
                                    direction: Some("from_below".to_string()),
                                    sequence_num: None,
                                    metadata: Some(serde_json::json!({
                                        "reentrySide": "low",
                                        "ibMid": mid,
                                        "ibRange": ib_range,
                                        "penetrationPoints": self.ib_reentry_max_penetration,
                                    })),
                                    session_type: String::new(),
                                    session_segment: String::new(),
                                    trading_day: String::new(),
                                },
                            );
                        }

                        // Full traverse to opposite side?
                        if !self.ib_reentry_traversed && price >= ib_high {
                            self.ib_reentry_traversed = true;
                            self.push_event_with_context(
                                state,
                                session_date,
                                events,
                                MarketEvent {
                                    session_date: session_date.to_string(),
                                    timestamp_ms,
                                    event_type: "ib_reentry_full_traverse".to_string(),
                                    level_name: Some("ib_high".to_string()),
                                    price,
                                    direction: Some("from_below".to_string()),
                                    sequence_num: None,
                                    metadata: Some(serde_json::json!({
                                        "reentrySide": "low",
                                        "ibRange": ib_range,
                                        "traversePoints": ib_range,
                                    })),
                                    session_type: String::new(),
                                    session_segment: String::new(),
                                    trading_day: String::new(),
                                },
                            );
                            self.ib_reentry_from = None;
                        }

                        // Price exited back below IB — reentry failed, stop tracking
                        if cur_pos == IbPosition::Below {
                            self.ib_reentry_from = None;
                        }
                    }
                    _ => {}
                }
            }

            self.ib_position = cur_pos;
        }

        self.prev_price = price;
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
            globex_delta: None,
            cumulative_delta: 500.0,
            prior_day_high: 21050.0,
            prior_day_low: 20950.0,
            prior_day_close: 21020.0,
            prior_va_high: 21040.0,
            prior_va_low: 20960.0,
            prior_poc: 21010.0,
            prior_dnva_high: 21035.0,
            prior_dnva_low: 20965.0,
            prior_dnp: 21000.0,
            overnight_high: 21030.0,
            overnight_low: 20970.0,
            session_high: 21025.0,
            session_low: 20975.0,
            rth_close_price: 21000.0,
            globex_or30_high: 21010.0,
            globex_or30_low: 20990.0,
            london_or60_high: 21005.0,
            london_or60_low: 20995.0,
            or_high: 21015.0,
            or_low: 20985.0,
            ib_high: 21020.0,
            ib_low: 20980.0,
            tape_pace_5s: Some(5.0),
            tape_pace_30s: Some(4.0),
            tape_pace_5m: Some(3.0),
            tape_acceleration: Some(1.0),
            tape_raw_acceleration: Some(1.0),
            pace_percentile: 0.5,
            tape_rolling_percentile: 0.5,
            tape_volume_per_sec_5s: Some(10.0),
            tape_volume_per_sec_30s: Some(8.0),
            tape_volume_per_sec_5m: Some(6.0),
            tape_regime_ticks_per_sec_30m_ema: Some(4.0),
            tape_regime_volume_per_sec_30m_ema: Some(8.0),
            tape_coverage_5s: 1.0,
            tape_coverage_30s: 1.0,
            tape_coverage_5m: 1.0,
            tape_valid_5s: true,
            tape_valid_30s: true,
            tape_valid_5m: true,
            tape_window_anchor_timestamp_ms: Some(1_000.0),
            tape_last_trade_timestamp_ms: Some(1_000.0),
            tape_event_time_lag_ms: Some(0.0),
            tape_dwell_at_current_price_ms: Some(2_000.0),
            imbalance_count: 0,
            absorption_event_count: 0,
            confirmed_absorption_event_count: 0,
            confirmed_exhaustion_event_count: 0,
            confirmed_delta_divergence_event_count: 0,
            has_recent_confirmed_absorption: false,
            recent_confirmed_absorption_price: None,
            recent_confirmed_absorption_direction: None,
            recent_confirmed_absorption_age_ms: None,
            recent_confirmed_absorption_distance_ticks: None,
            has_recent_confirmed_exhaustion: false,
            recent_confirmed_exhaustion_price: None,
            recent_confirmed_exhaustion_direction: None,
            recent_confirmed_exhaustion_age_ms: None,
            avg_trade_size: 2.0,
            or5_high: 21010.0,
            or5_low: 20990.0,
            or5_mid: 21000.0,
            or5_locked: true,
            or5_break_direction: Or5BreakDirection::None,
            or5_mid_retested: false,
            rvol_ratio: 1.0,
            rvol_classification: RvolClassification::Normal,
            rvol_velocity: 0.0,
            rvol_acceleration: 0.0,
            rvol_percentile: 50.0,
            rvol_bucket_index: 0,
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
            session_type: "RTH".to_string(),
            session_segment: "None".to_string(),
            trading_day: "2026-02-26".to_string(),
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            contract_month: Some("2026-03".to_string()),
            symbol_resolution_mode: "hybrid".to_string(),
            symbol_resolution_source: "manual_override".to_string(),
            rollover_warning: None,
            carry_forward_levels_valid: true,
            prior_day_contract_symbol: Some("NQH26.CME".to_string()),
            dom_summary: None,
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
    fn globex_suppresses_rth_only_events() {
        let mut detector = EventDetector::new();
        let date = "2026-02-27";

        let mut s = base_state();
        s.session_type = "Globex".to_string();
        s.session_segment = "Asia".to_string();
        s.trading_day = "2026-02-27".to_string();
        let events = detector.detect(&s, 1000.0, date, 60);
        assert!(
            !events.iter().any(|e| e.event_type == "ib_formed"),
            "IB-formed must remain RTH-only"
        );
    }

    #[test]
    fn globex_keeps_session_agnostic_events_with_context() {
        let mut detector = EventDetector::new();
        let date = "2026-02-27";

        let mut s1 = base_state();
        s1.session_type = "Globex".to_string();
        s1.session_segment = "London".to_string();
        s1.trading_day = "2026-02-27".to_string();
        s1.last_price = 20999.0;
        detector.detect(&s1, 1000.0, date, -30);

        let mut s2 = s1.clone();
        s2.last_price = 21001.0;
        let events = detector.detect(&s2, 2000.0, date, -29);
        let evt = events
            .into_iter()
            .find(|e| e.event_type == "dnp_cross")
            .expect("expected dnp_cross in globex");
        assert_eq!(evt.session_type, "Globex");
        assert_eq!(evt.session_segment, "London");
        assert_eq!(evt.trading_day, "2026-02-27");
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
        assert_eq!(detector.ib_position, IbPosition::Unknown);
        assert!(detector.ib_reentry_from.is_none());
    }

    // --- IB reentry tests ---

    /// Helper: walk the detector through IB formation, then position price outside IB.
    fn setup_outside_ib_high(detector: &mut EventDetector, date: &str) {
        // IB = 20980..21020, mid = 21000
        let mut s = base_state();
        s.last_price = 21000.0;
        detector.detect(&s, 1000.0, date, 60); // IB forms

        // Move price above IB high
        s.last_price = 21025.0;
        detector.detect(&s, 120_000.0, date, 62);
    }

    fn setup_outside_ib_low(detector: &mut EventDetector, date: &str) {
        let mut s = base_state();
        s.last_price = 21000.0;
        detector.detect(&s, 1000.0, date, 60);

        // Move price below IB low
        s.last_price = 20975.0;
        detector.detect(&s, 120_000.0, date, 62);
    }

    #[test]
    fn detects_ib_reentry_from_above() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_high(&mut detector, date);

        // Price re-enters IB (drops back below IB high of 21020)
        let mut s = base_state();
        s.last_price = 21015.0;
        let events = detector.detect(&s, 240_000.0, date, 64);

        let reentry = events.iter().find(|e| e.event_type == "ib_reentry");
        assert!(reentry.is_some(), "should detect IB reentry from above");
        let re = reentry.unwrap();
        assert_eq!(re.direction.as_deref(), Some("from_above"));
        let meta = re.metadata.as_ref().unwrap();
        assert_eq!(meta["reentrySide"], "high");
    }

    #[test]
    fn detects_ib_reentry_from_below() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_low(&mut detector, date);

        // Price re-enters IB (rises back above IB low of 20980)
        let mut s = base_state();
        s.last_price = 20985.0;
        let events = detector.detect(&s, 240_000.0, date, 64);

        let reentry = events.iter().find(|e| e.event_type == "ib_reentry");
        assert!(reentry.is_some(), "should detect IB reentry from below");
        assert_eq!(reentry.unwrap().direction.as_deref(), Some("from_below"));
    }

    #[test]
    fn detects_ib_reentry_hit_mid_from_above() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_high(&mut detector, date);

        // Re-enter IB
        let mut s = base_state();
        s.last_price = 21015.0;
        detector.detect(&s, 240_000.0, date, 64);

        // Price continues down to IB mid (21000)
        s.last_price = 20999.0;
        let events = detector.detect(&s, 300_000.0, date, 65);

        assert!(
            events.iter().any(|e| e.event_type == "ib_reentry_hit_mid"),
            "should fire ib_reentry_hit_mid when price reaches IB mid"
        );
    }

    #[test]
    fn detects_ib_reentry_full_traverse_from_above() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_high(&mut detector, date);

        // Re-enter IB
        let mut s = base_state();
        s.last_price = 21015.0;
        detector.detect(&s, 240_000.0, date, 64);

        // Price reaches mid
        s.last_price = 20999.0;
        detector.detect(&s, 300_000.0, date, 65);

        // Price traverses to opposite side (IB low = 20980)
        s.last_price = 20979.0;
        let events = detector.detect(&s, 360_000.0, date, 66);

        assert!(
            events
                .iter()
                .any(|e| e.event_type == "ib_reentry_full_traverse"),
            "should fire full traverse when price reaches opposite IB boundary"
        );
    }

    #[test]
    fn ib_reentry_cancelled_when_price_exits_same_side() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_high(&mut detector, date);

        // Re-enter IB
        let mut s = base_state();
        s.last_price = 21015.0;
        detector.detect(&s, 240_000.0, date, 64);

        // Price exits back above IB — reentry failed
        s.last_price = 21025.0;
        detector.detect(&s, 300_000.0, date, 65);

        // Price re-enters again — should get a new reentry event (after dedup window)
        s.last_price = 21015.0;
        let events = detector.detect(&s, 360_000.0, date, 66);
        assert!(
            events.iter().any(|e| e.event_type == "ib_reentry"),
            "should fire new reentry after previous was cancelled"
        );
    }

    #[test]
    fn no_ib_reentry_before_ib_formed() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";

        // Before minute 60 — IB not formed
        let mut s = base_state();
        s.last_price = 21025.0; // above IB high
        detector.detect(&s, 1000.0, date, 55);

        s.last_price = 21015.0; // back inside IB range
        let events = detector.detect(&s, 2000.0, date, 56);
        assert!(
            !events.iter().any(|e| e.event_type == "ib_reentry"),
            "should not detect reentry before IB is formed"
        );
    }

    #[test]
    fn ib_reentry_hit_mid_from_below() {
        let mut detector = EventDetector::new();
        let date = "2026-02-26";
        setup_outside_ib_low(&mut detector, date);

        // Re-enter IB
        let mut s = base_state();
        s.last_price = 20985.0;
        detector.detect(&s, 240_000.0, date, 64);

        // Price continues up to IB mid (21000)
        s.last_price = 21001.0;
        let events = detector.detect(&s, 300_000.0, date, 65);

        assert!(
            events.iter().any(|e| e.event_type == "ib_reentry_hit_mid"),
            "should fire hit_mid when re-entering from below and reaching mid"
        );
        let hit_mid = events
            .iter()
            .find(|e| e.event_type == "ib_reentry_hit_mid")
            .unwrap();
        assert_eq!(hit_mid.direction.as_deref(), Some("from_below"));
    }
}
