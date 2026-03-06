mod absorption;
mod day_type;
mod delta;
pub mod event_detector;
pub mod flow_event_emitter;
mod footprint;
mod levels;
mod opening_range_5min;
mod pinch;
mod rebid_reoffer;
mod rvol;
mod session_inventory;
mod tape_pace;
mod tpo;
mod trade_size;
mod vwap;

pub use absorption::{AbsorptionEvent, AbsorptionPipeline};
pub use day_type::{BalanceState, DayType, DayTypeClassifier, ProfileShape, SinglePrintsDirection};
pub use delta::DeltaPipeline;
pub use event_detector::{EventDetector, MarketEvent};
pub use flow_event_emitter::FlowEventEmitter;
pub use footprint::{FootprintLevel, FootprintPipeline};
pub use levels::{KeyLevel, KeyLevelType, LevelsPipeline, ProximityLevel};
pub use opening_range_5min::{OpeningRange5MinPipeline, Or5BreakDirection};
pub use pinch::{PinchEvent, PinchPipeline};
pub use rebid_reoffer::{AccelerationZone, RebidReofferPipeline, ZoneStatus, ZoneType};
pub use rvol::{RvolClassification, RvolPipeline};
pub use session_inventory::{
    InventoryDirection, InventoryState, PriorSessionData, SessionInventoryPipeline,
};
pub use tape_pace::{TapePacePipeline, TapePaceSnapshot};
pub use tpo::{SinglePrint, SinglePrintPeriod, TpoPipeline};
pub use trade_size::{TradeSizePipeline, TradeSizeSnapshot};
pub use vwap::VwapPipeline;

use serde::{Deserialize, Serialize};

use crate::depth::DomSummary;
use crate::{
    classify_session, et_minutes_from_timestamp, tick_time_context_from_timestamp_ms, DeltaSegment,
    SessionType,
};

/// Snapshot of session-ending data for prior-day level archival.
#[derive(Debug, Clone)]
pub struct SessionEndState {
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub va_high: f64,
    pub va_low: f64,
    pub poc: f64,
    pub dnva_high: f64,
    pub dnva_low: f64,
    pub dnp: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_state_serializes_dom_summary() {
        let state = MarketState {
            last_price: 21000.0,
            dom_summary: Some(DomSummary {
                source_file: "NQ.depth".into(),
                timestamp_ms: 1_000.0,
                spread_ticks: Some(1),
                touch_imbalance_ratio: Some(1.2),
                near_touch_bid_depth: 30.0,
                near_touch_ask_depth: 20.0,
                near_touch_depth_ratio: Some(1.5),
                bid_pull_rate: 0.1,
                ask_pull_rate: 0.4,
                stack_bias: 0.3,
                pull_stack_bias: 12.0,
                liquidity_bias: "bid_support".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let json = serde_json::to_value(&state).expect("serialize");
        assert_eq!(json["domSummary"]["liquidityBias"], "bid_support");
        assert_eq!(json["domSummary"]["nearTouchBidDepth"], 30.0);
    }
}

/// Consolidated snapshot of all pipeline outputs for the current session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketState {
    /// Most recent trade price.
    pub last_price: f64,
    /// Current best bid price.
    pub bid: f64,
    /// Current best ask price.
    pub ask: f64,
    /// Session volume-weighted average price.
    pub vwap: f64,
    /// VWAP plus one standard deviation.
    pub vwap_1sd_upper: f64,
    /// VWAP minus one standard deviation.
    pub vwap_1sd_lower: f64,
    /// VWAP plus two standard deviations.
    pub vwap_2sd_upper: f64,
    /// VWAP minus two standard deviations.
    pub vwap_2sd_lower: f64,
    /// VWAP plus three standard deviations.
    pub vwap_3sd_upper: f64,
    /// VWAP minus three standard deviations.
    pub vwap_3sd_lower: f64,
    /// TPO value area high (70% of TPOs).
    pub va_high: f64,
    /// TPO value area low (70% of TPOs).
    pub va_low: f64,
    /// Point of control — price with highest TPO count.
    pub poc: f64,
    /// Delta neutral value area high (70% of absolute delta).
    pub dnva_high: f64,
    /// Delta neutral value area low (70% of absolute delta).
    pub dnva_low: f64,
    /// Delta Neutral Pivot — midpoint of DNVA high and low.
    pub dnp: f64,
    /// Segment delta: Asia-only, London-only, or RTH-only. Resets at Asia→London (2 AM) and RTH↔Globex.
    pub session_delta: f64,
    /// Combined Globex delta (Asia + London) from 6 PM ET. Only present during Globex; null during RTH.
    pub globex_delta: Option<f64>,
    /// Running cumulative delta across sessions.
    pub cumulative_delta: f64,
    /// Previous RTH session high.
    pub prior_day_high: f64,
    /// Previous RTH session low.
    pub prior_day_low: f64,
    /// Previous RTH session closing price.
    pub prior_day_close: f64,
    /// Previous session value area high.
    pub prior_va_high: f64,
    /// Previous session value area low.
    pub prior_va_low: f64,
    /// Previous session point of control.
    pub prior_poc: f64,
    /// Prior RTH session DNVA high.
    pub prior_dnva_high: f64,
    /// Prior RTH session DNVA low.
    pub prior_dnva_low: f64,
    /// Prior RTH session DNP.
    pub prior_dnp: f64,
    /// Overnight (Globex) session high.
    pub overnight_high: f64,
    /// Overnight (Globex) session low.
    pub overnight_low: f64,
    /// Current RTH session high.
    pub session_high: f64,
    /// Current RTH session low.
    pub session_low: f64,
    /// Last RTH trade price used as session close.
    pub rth_close_price: f64,
    /// Globex OR30 high (18:00-18:30 ET).
    pub globex_or30_high: f64,
    /// Globex OR30 low (18:00-18:30 ET).
    pub globex_or30_low: f64,
    /// London OR60 high (02:00-03:00 ET).
    pub london_or60_high: f64,
    /// London OR60 low (02:00-03:00 ET).
    pub london_or60_low: f64,
    /// Opening range high (first 30 minutes of RTH).
    pub or_high: f64,
    /// Opening range low (first 30 minutes of RTH).
    pub or_low: f64,
    /// Initial balance high (first 60 minutes of RTH).
    pub ib_high: f64,
    /// Initial balance low (first 60 minutes of RTH).
    pub ib_low: f64,
    /// Rolling 5-second tape pace (ticks/sec).
    pub tape_pace_5s: f64,
    /// Rolling 30-second tape pace (ticks/sec).
    pub tape_pace_30s: f64,
    /// Rolling 5-minute tape pace (ticks/sec).
    pub tape_pace_5m: f64,
    /// Tape pace acceleration proxy (5s minus 30s).
    pub tape_acceleration: f64,
    /// Current pace percentile vs session distribution (0.0-1.0).
    pub pace_percentile: f64,
    /// Recent stacked imbalance count.
    pub imbalance_count: usize,
    /// Number of recent absorption events.
    pub absorption_event_count: usize,
    /// Average trade size for current session.
    pub avg_trade_size: f64,

    // --- 5-Min Opening Range (Leo's setup) ---
    /// 5-min OR high.
    pub or5_high: f64,
    /// 5-min OR low.
    pub or5_low: f64,
    /// 5-min OR midpoint (key level).
    pub or5_mid: f64,
    /// Whether OR5 range is locked (past 5 minutes).
    pub or5_locked: bool,
    /// Break direction from OR5.
    pub or5_break_direction: Or5BreakDirection,
    /// Whether price has retested the OR5 midpoint after a break.
    pub or5_mid_retested: bool,

    // --- Relative Volume ---
    /// RVOL ratio (1.0 = tracking average).
    pub rvol_ratio: f64,
    /// RVOL classification.
    pub rvol_classification: RvolClassification,
    /// RVOL velocity: rate of change of ratio per 5-min bucket.
    pub rvol_velocity: f64,
    /// RVOL acceleration: second derivative of ratio.
    pub rvol_acceleration: f64,
    /// RVOL percentile rank vs historical days at same time-of-day (0-100).
    pub rvol_percentile: f64,
    /// Current 5-minute bucket index.
    pub rvol_bucket_index: usize,

    // --- Day Type ---
    /// Current day type classification.
    pub day_type: DayType,
    /// Profile shape.
    pub profile_shape: ProfileShape,
    /// Balance state.
    pub balance_state: BalanceState,
    /// Single prints direction relative to POC.
    pub single_prints_direction: SinglePrintsDirection,

    // --- Pinch Events ---
    /// Number of recent pinch events.
    pub pinch_event_count: usize,

    // --- Session Inventory ---
    /// Cross-session inventory state.
    pub inventory_state: InventoryState,
    /// Cross-session inventory direction.
    pub inventory_direction: InventoryDirection,
    /// Consecutive sessions with same-direction delta.
    pub sessions_in_trend: usize,

    // --- Rebid/Reoffer ---
    /// Number of active acceleration zones.
    pub active_zone_count: usize,

    // --- TPO Enhancements ---
    /// Whether the session high is a "poor high" (multiple prints at extreme).
    pub poor_high: bool,
    /// Whether the session low is a "poor low" (multiple prints at extreme).
    pub poor_low: bool,
    /// Excess at top of profile.
    pub excess_high: bool,
    /// Excess at bottom of profile.
    pub excess_low: bool,

    /// Current session type from last tick / snapshot time: "RTH", "Globex", or "Unknown".
    /// During Globex, use overnightHigh/overnightLow as session range; sessionHigh/sessionLow and IB/OR/OR5 are RTH-only.
    pub session_type: String,
    /// Globex sub-session segment: "Asia", "London", or "None".
    pub session_segment: String,
    /// Trading day (YYYY-MM-DD) with a 6:00 PM ET roll.
    pub trading_day: String,
    /// Compact delayed DOM summary when historical depth context is available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dom_summary: Option<DomSummary>,
}

pub struct PipelineEngine {
    pub vwap: VwapPipeline,
    pub tpo: TpoPipeline,
    pub delta: DeltaPipeline,
    pub levels: LevelsPipeline,
    pub tape_pace: TapePacePipeline,
    pub footprint: FootprintPipeline,
    pub absorption: AbsorptionPipeline,
    pub trade_size: TradeSizePipeline,
    pub or5: OpeningRange5MinPipeline,
    pub rvol: RvolPipeline,
    pub day_type: DayTypeClassifier,
    pub rebid_reoffer: RebidReofferPipeline,
    pub pinch: PinchPipeline,
    pub session_inventory: SessionInventoryPipeline,
    last_trade_price: Option<f64>,
    cumulative_delta: f64,
    /// Combined Globex delta (Asia + London) from 6 PM ET. Only accumulates during Globex; resets at 6 PM and 9:30 AM.
    globex_delta: f64,
    dom_summary: Option<DomSummary>,
}

impl Default for PipelineEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineEngine {
    /// Create all deterministic pipelines with NQ tick size defaults.
    pub fn new() -> Self {
        Self {
            vwap: VwapPipeline::new(),
            tpo: TpoPipeline::new(0.25),
            delta: DeltaPipeline::new(0.25),
            levels: LevelsPipeline::default(),
            tape_pace: TapePacePipeline::new(),
            footprint: FootprintPipeline::new(0.25),
            absorption: AbsorptionPipeline::new(0.25),
            trade_size: TradeSizePipeline::new(),
            or5: OpeningRange5MinPipeline::new(),
            rvol: RvolPipeline::new(),
            day_type: DayTypeClassifier::new(),
            rebid_reoffer: RebidReofferPipeline::new(),
            pinch: PinchPipeline::new(),
            session_inventory: SessionInventoryPipeline::new(),
            last_trade_price: None,
            cumulative_delta: 0.0,
            globex_delta: 0.0,
            dom_summary: None,
        }
    }

    /// Reset all pipelines for a new trading session.
    /// Accumulates outgoing session delta into the cross-session cumulative total
    /// before clearing. Defaults to RTH session type for RVOL.
    pub fn reset_session(&mut self) {
        self.reset_session_with_type(false);
    }

    /// Reset all pipelines for a new trading session with explicit session type.
    pub fn reset_session_with_type(&mut self, is_globex: bool) {
        self.reset_segment(if is_globex {
            DeltaSegment::Asia
        } else {
            DeltaSegment::Rth
        });
        self.rvol.start_session(is_globex);
    }

    /// Reset at segment boundary. Asia and RTH get full reset; London gets delta-only reset
    /// (keeps Globex range/levels, resets segment delta for London-only tracking).
    pub fn reset_segment(&mut self, to_segment: DeltaSegment) {
        self.cumulative_delta += self.delta.session_delta();
        self.delta.reset();

        match to_segment {
            DeltaSegment::Asia | DeltaSegment::Rth => {
                self.globex_delta = 0.0;
                self.levels.reset_session();
                self.vwap.reset();
                self.tpo.reset();
                self.tape_pace.reset();
                self.footprint.reset();
                self.absorption.reset();
                self.trade_size.reset();
                self.or5.reset();
                self.rvol.reset();
                self.day_type.reset();
                self.rebid_reoffer.reset();
                self.pinch.reset();
                self.session_inventory.reset();
                self.last_trade_price = None;
                self.dom_summary = None;
            }
            DeltaSegment::London => {
                // Delta-only reset: keep levels, VWAP, TPO, etc. for full Globex; only segment delta resets.
            }
            DeltaSegment::Unknown => {}
        }
    }

    pub fn set_dom_summary(&mut self, dom_summary: Option<DomSummary>) {
        self.dom_summary = dom_summary;
    }

    /// Current session's ending state for archival into prior-day levels.
    pub fn session_end_state(&self) -> SessionEndState {
        let close = if self.levels.rth_close_price > 0.0 {
            self.levels.rth_close_price
        } else {
            self.levels.last_price
        };
        SessionEndState {
            high: self.levels.session_high,
            low: self.levels.session_low,
            close,
            va_high: self.tpo.va_high(),
            va_low: self.tpo.va_low(),
            poc: self.tpo.poc(),
            dnva_high: self.delta.dnva_high(),
            dnva_low: self.delta.dnva_low(),
            dnp: self.delta.dnp(),
        }
    }

    /// Apply a single trade incrementally to all pipelines.
    pub fn on_trade(&mut self, price: f64, volume: f64, is_buy: bool, minute_of_session: i32) {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        self.on_trade_with_timestamp(price, volume, is_buy, minute_of_session, now_ms);
    }

    /// Apply one trade with explicit timestamp from feed source.
    pub fn on_trade_with_timestamp(
        &mut self,
        price: f64,
        volume: f64,
        is_buy: bool,
        minute_of_session: i32,
        timestamp_ms: f64,
    ) {
        let (et_minutes, session_type) = et_minutes_from_timestamp(timestamp_ms)
            .map(|et_min| (et_min, classify_session(et_min)))
            .unwrap_or_else(|| {
                let fallback = minute_of_session + crate::RTH_OPEN_ET;
                (
                    fallback,
                    if minute_of_session < 0 {
                        SessionType::Globex
                    } else {
                        SessionType::Rth
                    },
                )
            });
        // Ignore 16:00-18:00 ET transition/noise window.
        if session_type == SessionType::Unknown {
            return;
        }
        let is_overnight = session_type == SessionType::Globex;
        self.on_trade_with_session_flag(
            price,
            volume,
            is_buy,
            minute_of_session,
            timestamp_ms,
            is_overnight,
            et_minutes,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_trade_with_session_flag(
        &mut self,
        price: f64,
        volume: f64,
        is_buy: bool,
        minute_of_session: i32,
        timestamp_ms: f64,
        is_overnight: bool,
        et_minutes: i32,
    ) {
        self.vwap.add_trade(price, volume);
        self.tpo.add_trade(price, minute_of_session);
        self.delta.add_trade(price, volume, is_buy);
        if is_overnight {
            let signed = if is_buy { volume } else { -volume };
            self.globex_delta += signed;
        }
        self.levels.on_trade(price, is_overnight, et_minutes);
        self.tape_pace.on_trade(timestamp_ms, volume, price);
        self.footprint.on_trade(price, volume, is_buy, timestamp_ms);
        let move_ticks = if let Some(prev) = self.last_trade_price {
            (price - prev) / 0.25
        } else {
            0.0
        };
        self.absorption
            .on_trade(timestamp_ms, price, volume, move_ticks, is_buy);
        self.trade_size.on_trade(volume, price);
        self.or5.on_trade(price, minute_of_session);
        self.rvol.on_trade(volume, minute_of_session);
        self.rebid_reoffer
            .on_trade(price, volume, is_buy, timestamp_ms);
        self.pinch.on_trade(timestamp_ms, price, volume, is_buy);
        self.session_inventory
            .update(self.delta.session_delta(), self.delta.dnp());

        // Periodically update day type classifier (every ~30 trades to avoid overhead)
        if self.vwap.trade_count().is_multiple_of(30) {
            let tpo_counts = self.tpo.tpo_count_by_price();
            let single_prints = self.tpo.single_print_prices();
            self.day_type.update(
                &tpo_counts,
                self.tpo.va_high(),
                self.tpo.va_low(),
                self.tpo.poc(),
                self.tpo.ib_high(),
                self.tpo.ib_low(),
                self.levels.session_high,
                self.levels.session_low,
                &single_prints,
            );
        }
        self.last_trade_price = Some(price);
    }

    /// Build current market state snapshot.
    pub fn snapshot(&self, bid: f64, ask: f64) -> MarketState {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        self.snapshot_at(bid, ask, now_ms)
    }

    pub fn snapshot_at(&self, bid: f64, ask: f64, timestamp_ms: f64) -> MarketState {
        self.build_snapshot(bid, ask, timestamp_ms, true)
    }

    pub fn snapshot_for_detection(&self, bid: f64, ask: f64, timestamp_ms: f64) -> MarketState {
        self.build_snapshot(bid, ask, timestamp_ms, false)
    }

    fn build_snapshot(
        &self,
        bid: f64,
        ask: f64,
        timestamp_ms: f64,
        include_extended_metrics: bool,
    ) -> MarketState {
        let sd = self.vwap.std_dev();
        let vwap = self.vwap.vwap();
        let (session_type, session_segment, trading_day) =
            if let Some(ctx) = tick_time_context_from_timestamp_ms(timestamp_ms) {
                let st = match ctx.session_type {
                    SessionType::Rth => "RTH",
                    SessionType::Globex => "Globex",
                    SessionType::Unknown => "Unknown",
                }
                .to_string();
                let ss = match ctx.session_segment {
                    crate::SessionSegment::Asia => "Asia",
                    crate::SessionSegment::London => "London",
                    crate::SessionSegment::None => "None",
                }
                .to_string();
                (st, ss, ctx.trading_day)
            } else {
                ("Unknown".to_string(), "None".to_string(), String::new())
            };
        let tape = if include_extended_metrics {
            self.tape_pace.snapshot(timestamp_ms)
        } else {
            TapePaceSnapshot::default()
        };
        let size = if include_extended_metrics {
            self.trade_size.snapshot()
        } else {
            TradeSizeSnapshot::default()
        };
        let imbalance_count = if include_extended_metrics {
            self.footprint.stacked_imbalances(2.0, 3).len()
        } else {
            0
        };
        let absorption_event_count = if include_extended_metrics {
            self.absorption.recent_events().len()
        } else {
            0
        };
        let pinch_event_count = if include_extended_metrics {
            self.pinch.recent_events().len()
        } else {
            0
        };
        let active_zone_count = if include_extended_metrics {
            self.rebid_reoffer.active_zones().len()
        } else {
            0
        };
        MarketState {
            last_price: self.levels.last_price,
            bid,
            ask,
            vwap,
            vwap_1sd_upper: vwap + sd,
            vwap_1sd_lower: vwap - sd,
            vwap_2sd_upper: vwap + 2.0 * sd,
            vwap_2sd_lower: vwap - 2.0 * sd,
            vwap_3sd_upper: vwap + 3.0 * sd,
            vwap_3sd_lower: vwap - 3.0 * sd,
            va_high: self.tpo.va_high(),
            va_low: self.tpo.va_low(),
            poc: self.tpo.poc(),
            dnva_high: self.delta.dnva_high(),
            dnva_low: self.delta.dnva_low(),
            dnp: self.delta.dnp(),
            session_delta: self.delta.session_delta(),
            globex_delta: if session_type == "Globex" {
                Some(self.globex_delta)
            } else {
                None
            },
            cumulative_delta: self.cumulative_delta + self.delta.session_delta(),
            prior_day_high: self.levels.prior_day_high,
            prior_day_low: self.levels.prior_day_low,
            prior_day_close: self.levels.prior_day_close,
            prior_va_high: self.levels.prior_va_high,
            prior_va_low: self.levels.prior_va_low,
            prior_poc: self.levels.prior_poc,
            prior_dnva_high: self.levels.prior_dnva_high,
            prior_dnva_low: self.levels.prior_dnva_low,
            prior_dnp: self.levels.prior_dnp,
            overnight_high: self.levels.overnight_high,
            overnight_low: self.levels.overnight_low,
            session_high: self.levels.session_high,
            session_low: self.levels.session_low,
            rth_close_price: self.levels.rth_close_price,
            globex_or30_high: self.levels.globex_or30_high,
            globex_or30_low: self.levels.globex_or30_low,
            london_or60_high: self.levels.london_or60_high,
            london_or60_low: self.levels.london_or60_low,
            or_high: self.tpo.or_high(),
            or_low: self.tpo.or_low(),
            ib_high: self.tpo.ib_high(),
            ib_low: self.tpo.ib_low(),
            tape_pace_5s: tape.ticks_per_sec_5s,
            tape_pace_30s: tape.ticks_per_sec_30s,
            tape_pace_5m: tape.ticks_per_sec_5m,
            tape_acceleration: tape.acceleration,
            pace_percentile: tape.pace_percentile,
            imbalance_count,
            absorption_event_count,
            avg_trade_size: size.avg_trade_size,

            or5_high: self.or5.or5_high(),
            or5_low: self.or5.or5_low(),
            or5_mid: self.or5.or5_mid(),
            or5_locked: self.or5.is_locked(),
            or5_break_direction: self.or5.break_direction(),
            or5_mid_retested: self.or5.mid_retested(),

            rvol_ratio: self.rvol.rvol_ratio(),
            rvol_classification: self.rvol.classification(),
            rvol_velocity: self.rvol.rvol_velocity(),
            rvol_acceleration: self.rvol.rvol_acceleration(),
            rvol_percentile: self.rvol.rvol_percentile(),
            rvol_bucket_index: self.rvol.bucket_index(),

            day_type: self.day_type.day_type(),
            profile_shape: self.day_type.profile_shape(),
            balance_state: self.day_type.balance_state(),
            single_prints_direction: self.day_type.single_prints_direction(),

            pinch_event_count,

            inventory_state: self.session_inventory.state(),
            inventory_direction: self.session_inventory.direction(),
            sessions_in_trend: self.session_inventory.sessions_in_trend(),

            active_zone_count,

            poor_high: self.tpo.poor_high(),
            poor_low: self.tpo.poor_low(),
            excess_high: {
                let (top, _) = self.tpo.excess();
                top
            },
            excess_low: {
                let (_, bottom) = self.tpo.excess();
                bottom
            },
            session_type,
            session_segment,
            trading_day,
            dom_summary: self.dom_summary.clone(),
        }
    }

    /// Run pipeline consistency invariant checks. Returns a list of (check_name, passed, detail).
    pub fn validate_invariants(&self) -> Vec<(String, bool, String)> {
        let mut checks = Vec::new();

        let poc = self.tpo.poc();
        let va_high = self.tpo.va_high();
        let va_low = self.tpo.va_low();

        // POC should be within the value area
        let poc_in_va = va_low <= poc && poc <= va_high;
        checks.push((
            "poc_within_va".to_string(),
            poc_in_va,
            format!("POC={poc:.2} VA=[{va_low:.2}, {va_high:.2}]"),
        ));

        // Value area should contain approximately 70% of TPOs
        let tpo_counts = self.tpo.tpo_count_by_price();
        let total_tpos: usize = tpo_counts.iter().map(|(_, c)| c).sum();
        if total_tpos > 0 {
            let va_tpos: usize = tpo_counts
                .iter()
                .filter(|(p, _)| *p >= va_low && *p <= va_high)
                .map(|(_, c)| c)
                .sum();
            let va_pct = va_tpos as f64 / total_tpos as f64;
            let va_valid = (0.60..=0.85).contains(&va_pct);
            checks.push((
                "va_contains_70pct_tpos".to_string(),
                va_valid,
                format!(
                    "VA contains {:.1}% of TPOs ({va_tpos}/{total_tpos})",
                    va_pct * 100.0
                ),
            ));
        }

        // Sum of delta-by-price should equal session delta (within tolerance)
        let delta_profile = self.delta.profile();
        let profile_sum: f64 = delta_profile.iter().map(|(_, d)| d).sum();
        let session_delta = self.delta.session_delta();
        let delta_diff = (profile_sum - session_delta).abs();
        let delta_consistent = delta_diff < 0.01;
        checks.push((
            "delta_sum_consistency".to_string(),
            delta_consistent,
            format!(
                "profile_sum={profile_sum:.2} session_delta={session_delta:.2} diff={delta_diff:.4}"
            ),
        ));

        // DNVA should be within overall price range
        let dnva_high = self.delta.dnva_high();
        let dnva_low = self.delta.dnva_low();
        if !delta_profile.is_empty() {
            let price_low = delta_profile.first().map(|(p, _)| *p).unwrap_or(0.0);
            let price_high = delta_profile.last().map(|(p, _)| *p).unwrap_or(0.0);
            let dnva_valid = dnva_low >= price_low && dnva_high <= price_high;
            checks.push((
                "dnva_within_range".to_string(),
                dnva_valid,
                format!(
                    "DNVA=[{dnva_low:.2}, {dnva_high:.2}] range=[{price_low:.2}, {price_high:.2}]"
                ),
            ));
        }

        checks
    }
}
