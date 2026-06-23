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
mod regime;
mod rvol;
mod session_inventory;
mod tape_pace;
mod tpo;
mod trade_size;
mod vwap;

pub use absorption::{AbsorptionEvent, AbsorptionPipeline, RecentSignalSnapshot};
pub use day_type::{
    day_type_label_aliases, normalize_day_type_label, normalize_profile_shape_label, BalanceState,
    DayType, DayTypeClassifier, ProfileShape, SinglePrintsDirection,
};
pub use delta::DeltaPipeline;
pub use event_detector::{EventDetector, MarketEvent, IB_EXTENSION_RATIO};
pub use flow_event_emitter::FlowEventEmitter;
pub use footprint::{FootprintLevel, FootprintPipeline};
pub use levels::{KeyLevel, KeyLevelType, LevelsPipeline, ProximityLevel};
pub use opening_range_5min::{OpeningRange5MinPipeline, Or5BreakDirection};
pub use pinch::{PinchEvent, PinchPipeline};
pub use rebid_reoffer::{AccelerationZone, RebidReofferPipeline, ZoneStatus, ZoneType};
pub use regime::{classify_regime, ib_extension_state_from_range, Regime, RegimeInputs};
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
use crate::feed::ContractMetadata;
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
    /// Rolling 5-second tape pace (ticks/sec) when the window has sufficient coverage.
    pub tape_pace_5s: Option<f64>,
    /// Rolling 30-second tape pace (ticks/sec) when the window has sufficient coverage.
    pub tape_pace_30s: Option<f64>,
    /// Rolling 5-minute tape pace (ticks/sec) when the window has sufficient coverage.
    pub tape_pace_5m: Option<f64>,
    /// Smoothed and normalized tape acceleration.
    pub tape_acceleration: Option<f64>,
    /// Raw 5s minus 30s pace spread for debugging and calibration.
    pub tape_raw_acceleration: Option<f64>,
    /// Current pace percentile vs session distribution (0.0-1.0).
    pub pace_percentile: f64,
    /// Current pace percentile vs the recent rolling intraday distribution (0.0-1.0).
    pub tape_rolling_percentile: f64,
    /// Rolling 5-second tape volume pace (contracts/sec).
    pub tape_volume_per_sec_5s: Option<f64>,
    /// Rolling 30-second tape volume pace (contracts/sec).
    pub tape_volume_per_sec_30s: Option<f64>,
    /// Rolling 5-minute tape volume pace (contracts/sec).
    pub tape_volume_per_sec_5m: Option<f64>,
    /// Longer-horizon tape regime baseline.
    pub tape_regime_ticks_per_sec_30m_ema: Option<f64>,
    pub tape_regime_volume_per_sec_30m_ema: Option<f64>,
    /// Window coverage ratios (0.0-1.0).
    pub tape_coverage_5s: f64,
    pub tape_coverage_30s: f64,
    pub tape_coverage_5m: f64,
    /// Whether each tape window has enough event-time coverage to trust.
    pub tape_valid_5s: bool,
    pub tape_valid_30s: bool,
    pub tape_valid_5m: bool,
    /// Event-time anchor and freshness metadata for the tape snapshot.
    pub tape_window_anchor_timestamp_ms: Option<f64>,
    pub tape_last_trade_timestamp_ms: Option<f64>,
    pub tape_event_time_lag_ms: Option<f64>,
    /// Dwell time at the current price level using the event-time anchor.
    pub tape_dwell_at_current_price_ms: Option<f64>,
    /// Recent stacked imbalance count.
    pub imbalance_count: usize,
    /// Number of recent absorption events.
    pub absorption_event_count: usize,
    /// Number of confirmed absorption events.
    pub confirmed_absorption_event_count: usize,
    /// Number of confirmed exhaustion events.
    pub confirmed_exhaustion_event_count: usize,
    /// Number of confirmed delta divergence events.
    pub confirmed_delta_divergence_event_count: usize,
    /// Whether there is a still-live confirmed absorption near current price.
    pub has_recent_confirmed_absorption: bool,
    /// Price of the most recent confirmed absorption considered live for evaluation.
    pub recent_confirmed_absorption_price: Option<f64>,
    /// Direction implied by the most recent confirmed absorption.
    pub recent_confirmed_absorption_direction: Option<String>,
    /// Age of the most recent confirmed absorption in milliseconds.
    pub recent_confirmed_absorption_age_ms: Option<f64>,
    /// Distance from current price to the most recent confirmed absorption, in ticks.
    pub recent_confirmed_absorption_distance_ticks: Option<f64>,
    /// Whether a recently *invalidated* absorption is live (IDEA-012 failure / vacuum).
    pub has_recent_invalidated_absorption: bool,
    /// Price of the most recent invalidated absorption.
    pub recent_invalidated_absorption_price: Option<f64>,
    /// Direction implied by the most recent invalidated absorption.
    pub recent_invalidated_absorption_direction: Option<String>,
    /// Age of the most recent invalidated absorption (since it failed), in milliseconds.
    pub recent_invalidated_absorption_age_ms: Option<f64>,
    /// Distance from current price to the most recent invalidated absorption, in ticks.
    pub recent_invalidated_absorption_distance_ticks: Option<f64>,
    /// Whether there is a still-live confirmed exhaustion signal.
    pub has_recent_confirmed_exhaustion: bool,
    /// Price of the most recent confirmed exhaustion considered live for evaluation.
    pub recent_confirmed_exhaustion_price: Option<f64>,
    /// Direction implied by the most recent confirmed exhaustion.
    pub recent_confirmed_exhaustion_direction: Option<String>,
    /// Age of the most recent confirmed exhaustion in milliseconds.
    pub recent_confirmed_exhaustion_age_ms: Option<f64>,
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

    // --- Regime (IDEA-000) ---
    /// Live 0.5x IB extension state: "None" | "UpOnly" | "DownOnly" | "BothSides".
    pub ib_extension_state: String,
    /// Computed session regime used to gate setup-family eligibility.
    pub regime: Regime,

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
    /// Root symbol for the active instrument family (e.g. NQ).
    pub root_symbol: String,
    /// Resolved active contract symbol (e.g. NQM26.CME).
    pub contract_symbol: String,
    /// Contract expiry month in YYYY-MM when available.
    pub contract_month: Option<String>,
    /// Configured symbol-resolution mode.
    pub symbol_resolution_mode: String,
    /// How the contract was resolved for the active feed.
    pub symbol_resolution_source: String,
    /// Roll/carry-forward warning surfaced to MCP callers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollover_warning: Option<String>,
    /// Whether prior-day carry-forward levels are safe for the active contract.
    pub carry_forward_levels_valid: bool,
    /// Contract symbol used for the current prior-day references, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prior_day_contract_symbol: Option<String>,
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
    contract_metadata: ContractMetadata,
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
            contract_metadata: ContractMetadata::default(),
        }
    }

    pub fn set_contract_metadata(&mut self, metadata: ContractMetadata) {
        let prior_day_contract_symbol = self.levels.prior_day_contract_symbol.clone();
        self.levels.set_prior_day_contract_context(
            Some(metadata.root_symbol.as_str()),
            prior_day_contract_symbol.as_deref(),
            Some(metadata.contract_symbol.as_str()),
        );
        self.contract_metadata = metadata;
    }

    /// Return the contract metadata currently bound to this live pipeline engine.
    pub fn contract_metadata(&self) -> &ContractMetadata {
        &self.contract_metadata
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
        let tape = self.tape_pace.snapshot(timestamp_ms);
        let rvol_ratio = self.rvol.rvol_ratio();
        let key_levels = self.levels.key_levels();
        self.absorption.on_trade(
            timestamp_ms,
            price,
            volume,
            move_ticks,
            is_buy,
            minute_of_session,
            tape.pace_percentile,
            rvol_ratio,
            &key_levels,
        );
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
            self.refresh_day_type_classification();
        }
        self.last_trade_price = Some(price);
    }

    /// Recompute day-type classification from the latest TPO state.
    ///
    /// The live path updates this periodically for low overhead, but final RTH
    /// close/backfill snapshots should force a fresh read before persistence.
    pub fn refresh_day_type_classification(&mut self) {
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
            self.levels.last_price,
            &single_prints,
        );
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
        let confirmed_absorption_event_count = if include_extended_metrics {
            self.absorption.count_confirmed("absorption")
        } else {
            0
        };
        let confirmed_exhaustion_event_count = if include_extended_metrics {
            self.absorption.count_confirmed("exhaustion")
        } else {
            0
        };
        let confirmed_delta_divergence_event_count = if include_extended_metrics {
            self.absorption.count_confirmed("delta_divergence")
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
        let recent_absorption = if include_extended_metrics {
            self.absorption
                .recent_confirmed_absorption_state(timestamp_ms, self.levels.last_price)
        } else {
            RecentSignalSnapshot::default()
        };
        let recent_exhaustion = if include_extended_metrics {
            self.absorption
                .recent_confirmed_exhaustion_state(timestamp_ms)
        } else {
            RecentSignalSnapshot::default()
        };
        let recent_invalidated_absorption = if include_extended_metrics {
            self.absorption
                .recent_invalidated_absorption_state(timestamp_ms, self.levels.last_price)
        } else {
            RecentSignalSnapshot::default()
        };
        let ib_extension_state = ib_extension_state_from_range(
            self.tpo.ib_high(),
            self.tpo.ib_low(),
            self.levels.session_high,
            self.levels.session_low,
        );
        let regime = classify_regime(&RegimeInputs {
            ib_extension_state: &ib_extension_state,
            day_type: self.day_type.day_type(),
            balance_state: self.day_type.balance_state(),
            last_price: self.levels.last_price,
            vwap,
            dnp: self.delta.dnp(),
            rvol_ratio: self.rvol.rvol_ratio(),
            pace_percentile: tape.pace_percentile,
        });
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
            tape_raw_acceleration: tape.raw_acceleration,
            pace_percentile: tape.pace_percentile,
            tape_rolling_percentile: tape.rolling_pace_percentile,
            tape_volume_per_sec_5s: tape.volume_per_sec_5s,
            tape_volume_per_sec_30s: tape.volume_per_sec_30s,
            tape_volume_per_sec_5m: tape.volume_per_sec_5m,
            tape_regime_ticks_per_sec_30m_ema: tape.regime_ticks_per_sec_30m_ema,
            tape_regime_volume_per_sec_30m_ema: tape.regime_volume_per_sec_30m_ema,
            tape_coverage_5s: tape.coverage_5s,
            tape_coverage_30s: tape.coverage_30s,
            tape_coverage_5m: tape.coverage_5m,
            tape_valid_5s: tape.valid_5s,
            tape_valid_30s: tape.valid_30s,
            tape_valid_5m: tape.valid_5m,
            tape_window_anchor_timestamp_ms: tape.window_anchor_timestamp_ms,
            tape_last_trade_timestamp_ms: tape.last_trade_timestamp_ms,
            tape_event_time_lag_ms: tape.event_time_lag_ms,
            tape_dwell_at_current_price_ms: if self.levels.last_price > 0.0 {
                tape.window_anchor_timestamp_ms.and_then(|anchor_ms| {
                    self.tape_pace
                        .dwell_at_price(self.levels.last_price, anchor_ms)
                })
            } else {
                None
            },
            imbalance_count,
            absorption_event_count,
            confirmed_absorption_event_count,
            confirmed_exhaustion_event_count,
            confirmed_delta_divergence_event_count,
            has_recent_confirmed_absorption: recent_absorption.is_active,
            recent_confirmed_absorption_price: recent_absorption.price,
            recent_confirmed_absorption_direction: recent_absorption.direction,
            recent_confirmed_absorption_age_ms: recent_absorption.age_ms,
            recent_confirmed_absorption_distance_ticks: recent_absorption.distance_ticks,
            has_recent_invalidated_absorption: recent_invalidated_absorption.is_active,
            recent_invalidated_absorption_price: recent_invalidated_absorption.price,
            recent_invalidated_absorption_direction: recent_invalidated_absorption
                .direction
                .clone(),
            recent_invalidated_absorption_age_ms: recent_invalidated_absorption.age_ms,
            recent_invalidated_absorption_distance_ticks: recent_invalidated_absorption
                .distance_ticks,
            has_recent_confirmed_exhaustion: recent_exhaustion.is_active,
            recent_confirmed_exhaustion_price: recent_exhaustion.price,
            recent_confirmed_exhaustion_direction: recent_exhaustion.direction,
            recent_confirmed_exhaustion_age_ms: recent_exhaustion.age_ms,
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

            ib_extension_state,
            regime,

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
            root_symbol: self.contract_metadata.root_symbol.clone(),
            contract_symbol: self.contract_metadata.contract_symbol.clone(),
            contract_month: self.contract_metadata.contract_month.clone(),
            symbol_resolution_mode: self.contract_metadata.symbol_resolution_mode.clone(),
            symbol_resolution_source: self.contract_metadata.symbol_resolution_source.clone(),
            rollover_warning: self
                .levels
                .carry_forward_warning
                .clone()
                .or_else(|| self.contract_metadata.warnings.first().cloned()),
            carry_forward_levels_valid: self.levels.carry_forward_levels_valid,
            prior_day_contract_symbol: self.levels.prior_day_contract_symbol.clone(),
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
