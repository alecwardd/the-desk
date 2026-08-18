//! DOM-cluster Tier-1 Base Detectors (SIL-M5e).
//!
//! Reviewed-Rust detectors for book velocity, market-by-price iceberg/reload
//! inference, stop-run, pull-intent, and a compact market-maker flow
//! composite. Math stays in this module. The four DOM-family event types
//! (`book_velocity_regime_shift`, `iceberg_reload`, `stop_run`,
//! `pull_intent`) are Capsule-mandatory. The composite (`detector.mm_flow`)
//! is catalog state only — it does not invent a fifth Capsule type.
//!
//! `.depth` is market-by-price. Reload inference only; no MBO / Databento.
//! Fail-closed when the book is missing. Session-scoped: full reset on Asia
//! and RTH; London is a Globex continuation. RTH and Globex never mix.
//!
//! Compact current-state fields ride `get_state`. Event emission uses a
//! monotonic `event_seq` (not ring length). Thinning vs thickening and
//! stop-cluster maps are context for these detectors, not a sixth family.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::depth::{
    DomSnapshot, PullStackActivitySummary, SideActivitySummary, DOM_SHORT_HORIZON_MS,
};

/// NQ minimum tick (0.25 points = $5.00/contract).
pub const NQ_TICK: f64 = 0.25;

/// Near-touch levels used for thinning / reload inference (not a full book dump).
pub const NEAR_TOUCH_LEVELS: usize = 3;

/// Event ring for [`crate::pipelines::FlowEventEmitter`].
const MAX_EVENTS: usize = 64;

/// Rolling trade / depth samples retained for stop-run windows.
const MAX_SAMPLES: usize = 256;

/// Short window for book-update velocity.
pub const VELOCITY_WINDOW_MS: f64 = 1_000.0;
/// Baseline window velocity is compared against.
pub const VELOCITY_BASELINE_MS: f64 = 10_000.0;
/// Fast regime: short rate ≥ this multiple of baseline (and the absolute floor).
pub const VELOCITY_FAST_MULT: f64 = 2.5;
/// Stay in fast until short rate falls below this multiple (hysteresis).
pub const VELOCITY_FAST_EXIT_MULT: f64 = 1.8;
/// Quiet regime: short rate ≤ this multiple of baseline.
pub const VELOCITY_QUIET_MULT: f64 = 0.4;
/// Stay in quiet until short rate rises above this multiple (hysteresis).
pub const VELOCITY_QUIET_EXIT_MULT: f64 = 0.6;
/// Absolute events/sec floor for "fast" so a dead book cannot spike on one print.
pub const VELOCITY_FAST_ABS_PER_SEC: f64 = 30.0;
/// Minimum baseline events/sec before a regime is classified.
pub const VELOCITY_MIN_BASELINE_PER_SEC: f64 = 2.0;

/// MbP reload must reappear within this window after a fill-classified decrease.
pub const RELOAD_WINDOW_MS: f64 = 2_000.0;
/// Minimum displayed-size recovery (contracts) to count as a reload.
pub const MIN_RELOAD_QTY: f64 = 10.0;
/// Per-price cooldown so flickering size does not spam Capsules.
pub const ICEBERG_EMIT_GAP_MS: f64 = 5_000.0;

/// Stop-run lookback.
pub const STOP_RUN_WINDOW_MS: f64 = 1_500.0;
/// Minimum one-way displacement (ticks) for a stop-run.
pub const STOP_RUN_TICKS: i64 = 8;
/// Near-touch depth on the consumed side must drop by at least this fraction.
pub const STOP_RUN_THIN_RATIO: f64 = 0.5;
/// Retrace / quiet window after which an open stop-run is invalidated.
pub const STOP_RUN_CLEAR_MS: f64 = 5_000.0;

/// Pull-intent: share of removed quantity classified as pull (not fill).
pub const PULL_INTENT_RATE: f64 = 0.55;
/// Side-to-side edge required so both sides pulling is not a signal.
pub const PULL_INTENT_EDGE: f64 = 0.15;
/// Minimum removed quantity (contracts) in the activity window.
pub const MIN_PULL_REMOVED: f64 = 25.0;
/// Pull rate at or below this clears an open pull-intent.
pub const PULL_INTENT_CLEAR_RATE: f64 = 0.35;
/// Minimum dwell before an open pull-intent may flip sides.
pub const PULL_INTENT_MIN_DWELL_MS: f64 = 2_000.0;

/// Wire types — must match [`crate::catalog::DOM_FAMILY_EVENT_TYPES`].
pub const EVENT_BOOK_VELOCITY_REGIME_SHIFT: &str = "book_velocity_regime_shift";
pub const EVENT_ICEBERG_RELOAD: &str = "iceberg_reload";
pub const EVENT_STOP_RUN: &str = "stop_run";
pub const EVENT_PULL_INTENT: &str = "pull_intent";

/// Compact snapshot for `MarketState` / `get_state` (no per-price maps).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomClusterSnapshot {
    /// True when a usable bid/ask book has been observed this session.
    pub book_present: bool,
    /// Classified book-update regime: `quiet` / `normal` / `fast`.
    pub book_velocity_regime: Option<String>,
    /// Short-window book update rate (events/sec).
    pub book_velocity_per_sec: Option<f64>,
    /// Side of the most recent MbP reload (`bid` / `ask`).
    pub iceberg_reload_side: Option<String>,
    /// Price of the most recent MbP reload.
    pub iceberg_reload_price: Option<f64>,
    /// Direction of an open stop-run (`up` / `down`).
    pub stop_run_direction: Option<String>,
    /// Compact stop-cluster marker: first thinned price of the open (or last) run.
    pub stop_cluster_price: Option<f64>,
    /// Side showing pull-intent (`bid` / `ask`).
    pub pull_intent_side: Option<String>,
    /// Pull rate on the intent side (0–1).
    pub pull_intent_rate: Option<f64>,
    /// Composite MM-flow status: `insufficient` / `none` / `bid_dominant` / `ask_dominant` / `mixed`.
    pub mm_flow_status: Option<String>,
    /// Composite score in `[-1, 1]` (positive = bid-dominant / ask-pulling).
    pub mm_flow_score: Option<f64>,
}

/// Lifecycle row stored in the event ring (not a specialty MCP payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomClusterEvent {
    pub timestamp_ms: f64,
    pub event_type: String,
    pub price: f64,
    pub direction: Option<String>,
    pub sequence_num: Option<i32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VelocityRegime {
    Quiet,
    Normal,
    Fast,
}

impl VelocityRegime {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quiet => "quiet",
            Self::Normal => "normal",
            Self::Fast => "fast",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LevelWatch {
    qty: f64,
    depleted_at_ms: Option<f64>,
    filled_since_deplete: f64,
}

#[derive(Debug, Clone, Copy)]
struct DepthSample {
    ts: f64,
    bid_near: f64,
    ask_near: f64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

/// Incremental DOM-cluster detector pipeline.
#[derive(Debug)]
pub struct DomClusterPipeline {
    event_seq: u64,
    events: VecDeque<DomClusterEvent>,
    book_present: bool,
    overnight: Option<bool>,
    last_price: f64,
    last_trade_is_buy: Option<bool>,
    last_trade_time_ms: Option<f64>,
    velocity_samples: VecDeque<(f64, f64)>,
    velocity_regime: Option<VelocityRegime>,
    open_velocity_direction: Option<&'static str>,
    levels: HashMap<(char, i64), LevelWatch>,
    last_iceberg_side: Option<String>,
    last_iceberg_price: Option<f64>,
    last_iceberg_emit_ms: HashMap<(char, i64), f64>,
    trades: VecDeque<(f64, f64)>,
    depth_samples: VecDeque<DepthSample>,
    stop_run_open: Option<&'static str>,
    stop_cluster_price: Option<f64>,
    stop_run_last_ts: Option<f64>,
    pull_intent_open: Option<&'static str>,
    pull_intent_opened_ms: Option<f64>,
    pull_intent_rate: Option<f64>,
    mm_flow_status: Option<String>,
    mm_flow_score: Option<f64>,
}

impl Default for DomClusterPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl DomClusterPipeline {
    /// Empty detector (no book — fail-closed).
    pub fn new() -> Self {
        Self {
            event_seq: 0,
            events: VecDeque::new(),
            book_present: false,
            overnight: None,
            last_price: 0.0,
            last_trade_is_buy: None,
            last_trade_time_ms: None,
            velocity_samples: VecDeque::new(),
            velocity_regime: None,
            open_velocity_direction: None,
            levels: HashMap::new(),
            last_iceberg_side: None,
            last_iceberg_price: None,
            last_iceberg_emit_ms: HashMap::new(),
            trades: VecDeque::new(),
            depth_samples: VecDeque::new(),
            stop_run_open: None,
            stop_cluster_price: None,
            stop_run_last_ts: None,
            pull_intent_open: None,
            pull_intent_opened_ms: None,
            pull_intent_rate: None,
            mm_flow_status: None,
            mm_flow_score: None,
        }
    }

    /// Full reset (Asia / RTH / session-kind flip).
    ///
    /// Detector state and the event ring clear. [`Self::event_seq`] stays
    /// monotonic so [`crate::pipelines::FlowEventEmitter`] recovers after a
    /// pipeline reset without an emitter reset (SIL-M5d Opus P2-1).
    pub fn reset(&mut self) {
        let event_seq = self.event_seq;
        *self = Self::new();
        self.event_seq = event_seq;
    }

    /// Monotonic emission watermark (not ring length).
    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    /// Recent detector events (bounded ring).
    pub fn recent_events(&self) -> &VecDeque<DomClusterEvent> {
        &self.events
    }

    /// Compact current-state fields. All detector fields are `None` without a book.
    pub fn snapshot(&self) -> DomClusterSnapshot {
        if !self.book_present {
            return DomClusterSnapshot {
                book_present: false,
                ..DomClusterSnapshot::default()
            };
        }
        DomClusterSnapshot {
            book_present: true,
            book_velocity_regime: self.velocity_regime.map(|r| r.as_str().to_string()),
            book_velocity_per_sec: short_velocity(&self.velocity_samples),
            iceberg_reload_side: self.last_iceberg_side.clone(),
            iceberg_reload_price: self.last_iceberg_price,
            stop_run_direction: self.stop_run_open.map(|d| d.to_string()),
            stop_cluster_price: self.stop_cluster_price,
            pull_intent_side: self.pull_intent_open.map(|s| s.to_string()),
            pull_intent_rate: self.pull_intent_rate,
            mm_flow_status: self.mm_flow_status.clone(),
            mm_flow_score: self.mm_flow_score,
        }
    }

    /// Observe a tape print. Stop-run still requires a book (thinning).
    pub fn on_trade(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        is_buy: bool,
        is_overnight: bool,
    ) {
        if !timestamp_ms.is_finite() || !price.is_finite() {
            return;
        }
        if self.session_kind_flip(is_overnight) {
            self.reset();
        }
        self.overnight = Some(is_overnight);
        self.last_price = price;
        self.last_trade_is_buy = Some(is_buy);
        self.last_trade_time_ms = Some(timestamp_ms);
        self.trades.push_back((timestamp_ms, price));
        evict_older_than(&mut self.trades, timestamp_ms, STOP_RUN_WINDOW_MS * 2.0);
        if volume > 0.0 && self.book_present {
            let side = if is_buy { 'A' } else { 'B' };
            let key = (side, price_key(price));
            if let Some(watch) = self.levels.get_mut(&key) {
                if watch.depleted_at_ms.is_some() {
                    watch.filled_since_deplete += volume;
                }
            }
        }
        if self.book_present {
            self.eval_stop_run(timestamp_ms, price);
            self.refresh_composite();
        }
    }

    /// Observe a compact book snapshot plus pull/stack activity (MbP, not MBO).
    pub fn on_book_update(
        &mut self,
        timestamp_ms: f64,
        snapshot: &DomSnapshot,
        activity: &PullStackActivitySummary,
        is_overnight: bool,
        last_trade_price: f64,
        last_trade_is_buy: Option<bool>,
    ) {
        if !timestamp_ms.is_finite() {
            return;
        }
        if self.session_kind_flip(is_overnight) {
            self.reset();
        }
        self.overnight = Some(is_overnight);
        if last_trade_price > 0.0 {
            self.last_price = last_trade_price;
        }
        if last_trade_is_buy.is_some() {
            self.last_trade_is_buy = last_trade_is_buy;
        }

        let bid_near = near_touch_qty(&snapshot.bids);
        let ask_near = near_touch_qty(&snapshot.asks);
        let has_book = snapshot.best_bid.is_some() && snapshot.best_ask.is_some();
        if !has_book {
            self.fail_closed_missing_book(timestamp_ms);
            return;
        }
        self.book_present = true;

        self.depth_samples.push_back(DepthSample {
            ts: timestamp_ms,
            bid_near,
            ask_near,
            best_bid: snapshot.best_bid,
            best_ask: snapshot.best_ask,
        });
        evict_front(&mut self.depth_samples);

        self.eval_velocity(timestamp_ms, activity);
        self.eval_iceberg(timestamp_ms, snapshot, last_trade_price, last_trade_is_buy);
        self.eval_pull_intent(timestamp_ms, activity);
        if self.last_price > 0.0 {
            self.eval_stop_run(timestamp_ms, self.last_price);
        }
        self.refresh_composite();
    }

    fn session_kind_flip(&self, is_overnight: bool) -> bool {
        self.overnight.is_some_and(|prev| prev != is_overnight)
    }

    fn fail_closed_missing_book(&mut self, timestamp_ms: f64) {
        if self.book_present {
            self.invalidate_open_conditions(timestamp_ms);
        }
        self.book_present = false;
        self.velocity_regime = None;
        self.mm_flow_status = Some("insufficient".into());
        self.mm_flow_score = None;
        self.pull_intent_rate = None;
    }

    fn invalidate_open_conditions(&mut self, timestamp_ms: f64) {
        if let Some(dir) = self.open_velocity_direction.take() {
            self.push_event(
                timestamp_ms,
                format!("{EVENT_BOOK_VELOCITY_REGIME_SHIFT}_invalidated"),
                self.last_price,
                Some(dir),
                None,
                serde_json::json!({ "reason": "book_missing" }),
            );
        }
        if let Some(dir) = self.stop_run_open.take() {
            self.push_event(
                timestamp_ms,
                format!("{EVENT_STOP_RUN}_invalidated"),
                self.last_price,
                Some(dir),
                None,
                serde_json::json!({ "reason": "book_missing" }),
            );
        }
        if let Some(side) = self.pull_intent_open.take() {
            self.pull_intent_opened_ms = None;
            self.push_event(
                timestamp_ms,
                format!("{EVENT_PULL_INTENT}_invalidated"),
                self.last_price,
                Some(side),
                None,
                serde_json::json!({ "reason": "book_missing" }),
            );
        }
    }

    fn eval_velocity(&mut self, timestamp_ms: f64, activity: &PullStackActivitySummary) {
        let events = side_event_count(&activity.bid) + side_event_count(&activity.ask);
        let window_secs = activity_window_secs(activity);
        let rate = events as f64 / window_secs;
        self.velocity_samples.push_back((timestamp_ms, rate));
        while self
            .velocity_samples
            .front()
            .is_some_and(|(ts, _)| timestamp_ms - *ts > VELOCITY_BASELINE_MS)
        {
            self.velocity_samples.pop_front();
        }
        let Some(short) = short_velocity(&self.velocity_samples) else {
            return;
        };
        let Some(baseline) = baseline_velocity(&self.velocity_samples) else {
            return;
        };
        if baseline < VELOCITY_MIN_BASELINE_PER_SEC {
            return;
        }
        let next = classify_velocity_regime(self.velocity_regime, short, baseline);
        let prev = self.velocity_regime;
        self.velocity_regime = Some(next);
        if prev == Some(next) {
            return;
        }
        if let Some(open) = self.open_velocity_direction {
            if next.as_str() != open {
                self.push_event(
                    timestamp_ms,
                    format!("{EVENT_BOOK_VELOCITY_REGIME_SHIFT}_invalidated"),
                    self.last_price,
                    Some(open),
                    None,
                    serde_json::json!({
                        "from": open,
                        "to": next.as_str(),
                        "shortPerSec": short,
                        "baselinePerSec": baseline,
                    }),
                );
                self.open_velocity_direction = None;
            }
        }
        if matches!(next, VelocityRegime::Fast | VelocityRegime::Quiet) {
            self.open_velocity_direction = Some(next.as_str());
            self.push_event(
                timestamp_ms,
                EVENT_BOOK_VELOCITY_REGIME_SHIFT.to_string(),
                self.last_price,
                Some(next.as_str()),
                None,
                serde_json::json!({
                    "from": prev.map(|p| p.as_str()),
                    "to": next.as_str(),
                    "shortPerSec": short,
                    "baselinePerSec": baseline,
                }),
            );
        }
    }

    fn eval_iceberg(
        &mut self,
        timestamp_ms: f64,
        snapshot: &DomSnapshot,
        last_trade_price: f64,
        last_trade_is_buy: Option<bool>,
    ) {
        update_side_watches(
            self,
            timestamp_ms,
            'B',
            snapshot.bids.iter().take(NEAR_TOUCH_LEVELS).map(|l| {
                (
                    l.price,
                    l.quantity as f64,
                    self.trade_at_level(timestamp_ms, last_trade_price, last_trade_is_buy, false)
                        && prices_equal(l.price, last_trade_price),
                )
            }),
        );
        update_side_watches(
            self,
            timestamp_ms,
            'A',
            snapshot.asks.iter().take(NEAR_TOUCH_LEVELS).map(|l| {
                (
                    l.price,
                    l.quantity as f64,
                    self.trade_at_level(timestamp_ms, last_trade_price, last_trade_is_buy, true)
                        && prices_equal(l.price, last_trade_price),
                )
            }),
        );
    }

    /// Last-print fill classification is only honoured inside [`RELOAD_WINDOW_MS`].
    fn trade_at_level(
        &self,
        timestamp_ms: f64,
        last_trade_price: f64,
        last_trade_is_buy: Option<bool>,
        want_buy: bool,
    ) -> bool {
        if last_trade_price <= 0.0 || last_trade_is_buy != Some(want_buy) {
            return false;
        }
        self.last_trade_time_ms
            .is_some_and(|ts| timestamp_ms - ts <= RELOAD_WINDOW_MS)
    }

    fn eval_pull_intent(&mut self, timestamp_ms: f64, activity: &PullStackActivitySummary) {
        let bid_rate = pull_rate(&activity.bid);
        let ask_rate = pull_rate(&activity.ask);
        let bid_removed = activity.bid.removed_quantity;
        let ask_removed = activity.ask.removed_quantity;
        let mut side: Option<&'static str> = None;
        let mut rate = 0.0;
        if bid_removed >= MIN_PULL_REMOVED
            && bid_rate >= PULL_INTENT_RATE
            && bid_rate >= ask_rate + PULL_INTENT_EDGE
        {
            side = Some("bid");
            rate = bid_rate;
        } else if ask_removed >= MIN_PULL_REMOVED
            && ask_rate >= PULL_INTENT_RATE
            && ask_rate >= bid_rate + PULL_INTENT_EDGE
        {
            side = Some("ask");
            rate = ask_rate;
        }
        match (self.pull_intent_open, side) {
            (None, Some(s)) => {
                self.pull_intent_open = Some(s);
                self.pull_intent_opened_ms = Some(timestamp_ms);
                self.pull_intent_rate = Some(rate);
                self.push_event(
                    timestamp_ms,
                    EVENT_PULL_INTENT.to_string(),
                    self.last_price,
                    Some(s),
                    None,
                    serde_json::json!({
                        "side": s,
                        "pullRate": rate,
                        "bidPullRate": bid_rate,
                        "askPullRate": ask_rate,
                        "bidRemoved": bid_removed,
                        "askRemoved": ask_removed,
                    }),
                );
            }
            (Some(open), None) => {
                let open_rate = if open == "bid" { bid_rate } else { ask_rate };
                if open_rate <= PULL_INTENT_CLEAR_RATE {
                    self.pull_intent_open = None;
                    self.pull_intent_opened_ms = None;
                    self.pull_intent_rate = None;
                    self.push_event(
                        timestamp_ms,
                        format!("{EVENT_PULL_INTENT}_invalidated"),
                        self.last_price,
                        Some(open),
                        None,
                        serde_json::json!({ "side": open, "pullRate": open_rate }),
                    );
                } else {
                    self.pull_intent_rate = Some(open_rate);
                }
            }
            (Some(open), Some(s)) if open != s => {
                let dwell_ok = self
                    .pull_intent_opened_ms
                    .is_none_or(|opened| timestamp_ms - opened >= PULL_INTENT_MIN_DWELL_MS);
                if !dwell_ok {
                    self.pull_intent_rate = Some(rate);
                    return;
                }
                self.push_event(
                    timestamp_ms,
                    format!("{EVENT_PULL_INTENT}_invalidated"),
                    self.last_price,
                    Some(open),
                    None,
                    serde_json::json!({ "side": open, "reason": "side_flip" }),
                );
                self.pull_intent_open = Some(s);
                self.pull_intent_opened_ms = Some(timestamp_ms);
                self.pull_intent_rate = Some(rate);
                self.push_event(
                    timestamp_ms,
                    EVENT_PULL_INTENT.to_string(),
                    self.last_price,
                    Some(s),
                    None,
                    serde_json::json!({
                        "side": s,
                        "pullRate": rate,
                        "bidPullRate": bid_rate,
                        "askPullRate": ask_rate,
                    }),
                );
            }
            (Some(_), Some(_)) => {
                self.pull_intent_rate = Some(rate);
            }
            (None, None) => {
                self.pull_intent_rate = None;
            }
        }
    }

    fn eval_stop_run(&mut self, timestamp_ms: f64, price: f64) {
        let window_start = timestamp_ms - STOP_RUN_WINDOW_MS;
        let origin = self
            .trades
            .iter()
            .find(|(ts, _)| *ts >= window_start)
            .map(|(_, p)| *p);
        let Some(origin) = origin else {
            self.maybe_clear_stop_run(timestamp_ms, price);
            return;
        };
        let ticks = ((price - origin) / NQ_TICK).round() as i64;
        if ticks.abs() < STOP_RUN_TICKS {
            self.maybe_clear_stop_run(timestamp_ms, price);
            return;
        }
        let up = ticks > 0;
        let Some(start_depth) = depth_at(&self.depth_samples, window_start, up) else {
            self.maybe_clear_stop_run(timestamp_ms, price);
            return;
        };
        let Some(now) = self.depth_samples.back() else {
            return;
        };
        let now_depth = if up { now.ask_near } else { now.bid_near };
        if start_depth <= 0.0 {
            return;
        }
        let thinned = now_depth <= start_depth * (1.0 - STOP_RUN_THIN_RATIO);
        if !thinned {
            self.maybe_clear_stop_run(timestamp_ms, price);
            return;
        }
        let dir: &'static str = if up { "up" } else { "down" };
        let cluster = if up { now.best_ask } else { now.best_bid };
        self.stop_run_last_ts = Some(timestamp_ms);
        if self.stop_run_open == Some(dir) {
            return;
        }
        self.stop_cluster_price = cluster.or(Some(origin));
        if let Some(prev) = self.stop_run_open {
            self.push_event(
                timestamp_ms,
                format!("{EVENT_STOP_RUN}_invalidated"),
                price,
                Some(prev),
                None,
                serde_json::json!({ "reason": "direction_flip" }),
            );
        }
        self.stop_run_open = Some(dir);
        self.push_event(
            timestamp_ms,
            EVENT_STOP_RUN.to_string(),
            price,
            Some(dir),
            None,
            serde_json::json!({
                "origin": origin,
                "ticks": ticks.abs(),
                "startDepth": start_depth,
                "nowDepth": now_depth,
                "clusterPrice": self.stop_cluster_price,
            }),
        );
    }

    fn maybe_clear_stop_run(&mut self, timestamp_ms: f64, price: f64) {
        let Some(dir) = self.stop_run_open else {
            return;
        };
        let last = self.stop_run_last_ts.unwrap_or(timestamp_ms);
        if timestamp_ms - last < STOP_RUN_CLEAR_MS {
            return;
        }
        self.stop_run_open = None;
        self.stop_run_last_ts = None;
        self.push_event(
            timestamp_ms,
            format!("{EVENT_STOP_RUN}_invalidated"),
            price,
            Some(dir),
            None,
            serde_json::json!({ "reason": "cleared" }),
        );
    }

    fn refresh_composite(&mut self) {
        if !self.book_present {
            self.mm_flow_status = Some("insufficient".into());
            self.mm_flow_score = None;
            return;
        }
        let mut score: f64 = 0.0;
        let mut parts = 0u32;
        if self.pull_intent_open == Some("ask") {
            score += 0.35;
            parts += 1;
        } else if self.pull_intent_open == Some("bid") {
            score -= 0.35;
            parts += 1;
        }
        if self.last_iceberg_side.as_deref() == Some("bid") {
            score += 0.30;
            parts += 1;
        } else if self.last_iceberg_side.as_deref() == Some("ask") {
            score -= 0.30;
            parts += 1;
        }
        if self.stop_run_open == Some("up") {
            score += 0.20;
            parts += 1;
        } else if self.stop_run_open == Some("down") {
            score -= 0.20;
            parts += 1;
        }
        if self.velocity_regime == Some(VelocityRegime::Fast) && parts > 0 {
            score += 0.15 * score.signum();
        }
        score = score.clamp(-1.0, 1.0);
        let status = if parts == 0 {
            "none"
        } else if self.pull_intent_open.is_some()
            && self.stop_run_open.is_some()
            && ((self.pull_intent_open == Some("bid") && self.stop_run_open == Some("up"))
                || (self.pull_intent_open == Some("ask") && self.stop_run_open == Some("down")))
        {
            "mixed"
        } else if score > 0.2 {
            "bid_dominant"
        } else if score < -0.2 {
            "ask_dominant"
        } else {
            "none"
        };
        self.mm_flow_score = Some(score);
        self.mm_flow_status = Some(status.into());
    }

    fn push_event(
        &mut self,
        timestamp_ms: f64,
        event_type: String,
        price: f64,
        direction: Option<&str>,
        sequence_num: Option<i32>,
        metadata: serde_json::Value,
    ) {
        self.event_seq = self.event_seq.saturating_add(1);
        let seq = sequence_num.or_else(|| {
            if event_type == EVENT_ICEBERG_RELOAD {
                Some(self.event_seq as i32)
            } else {
                None
            }
        });
        self.events.push_back(DomClusterEvent {
            timestamp_ms,
            event_type,
            price,
            direction: direction.map(str::to_string),
            sequence_num: seq,
            metadata,
        });
        while self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
    }
}

fn update_side_watches(
    pipeline: &mut DomClusterPipeline,
    timestamp_ms: f64,
    side: char,
    levels: impl Iterator<Item = (f64, f64, bool)>,
) {
    for (price, qty, trade_at_level) in levels {
        let key = (side, price_key(price));
        let prev = pipeline.levels.get(&key).copied();
        let mut watch = prev.unwrap_or(LevelWatch {
            qty,
            depleted_at_ms: None,
            filled_since_deplete: 0.0,
        });
        if qty + 1e-9 < watch.qty {
            watch.depleted_at_ms = Some(timestamp_ms);
            if trade_at_level {
                watch.filled_since_deplete += (watch.qty - qty).max(0.0);
            }
        } else if qty > watch.qty + 1e-9 {
            if let Some(depleted_at) = watch.depleted_at_ms {
                let recovered = qty - watch.qty;
                let fill_ok = watch.filled_since_deplete >= MIN_RELOAD_QTY * 0.5 || trade_at_level;
                if timestamp_ms - depleted_at <= RELOAD_WINDOW_MS
                    && recovered >= MIN_RELOAD_QTY
                    && fill_ok
                {
                    let gap_ok = pipeline
                        .last_iceberg_emit_ms
                        .get(&key)
                        .is_none_or(|last| timestamp_ms - *last >= ICEBERG_EMIT_GAP_MS);
                    if gap_ok {
                        let side_label: &'static str = if side == 'B' { "bid" } else { "ask" };
                        pipeline.last_iceberg_side = Some(side_label.into());
                        pipeline.last_iceberg_price = Some(price);
                        pipeline.last_iceberg_emit_ms.insert(key, timestamp_ms);
                        pipeline.push_event(
                            timestamp_ms,
                            EVENT_ICEBERG_RELOAD.to_string(),
                            price,
                            Some(side_label),
                            None,
                            serde_json::json!({
                                "side": side_label,
                                "recovered": recovered,
                                "filledSinceDeplete": watch.filled_since_deplete,
                                "reloadWindowMs": RELOAD_WINDOW_MS,
                                "inference": "market_by_price",
                            }),
                        );
                    }
                }
                watch.depleted_at_ms = None;
                watch.filled_since_deplete = 0.0;
            }
        }
        watch.qty = qty;
        pipeline.levels.insert(key, watch);
    }
}

fn side_event_count(side: &SideActivitySummary) -> u64 {
    side.add_events + side.modify_up_events + side.modify_down_events + side.delete_events
}

fn pull_rate(side: &SideActivitySummary) -> f64 {
    if side.removed_quantity > 0.0 {
        side.estimated_pulled_quantity / side.removed_quantity
    } else {
        0.0
    }
}

fn near_touch_qty(levels: &[crate::depth::DomLevel]) -> f64 {
    levels
        .iter()
        .take(NEAR_TOUCH_LEVELS)
        .map(|l| l.quantity as f64)
        .sum()
}

fn price_key(price: f64) -> i64 {
    (price / NQ_TICK).round() as i64
}

fn prices_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < NQ_TICK * 0.5
}

fn short_velocity(samples: &VecDeque<(f64, f64)>) -> Option<f64> {
    mean_rate_in_window(samples, VELOCITY_WINDOW_MS)
}

fn baseline_velocity(samples: &VecDeque<(f64, f64)>) -> Option<f64> {
    mean_rate_in_window(samples, VELOCITY_BASELINE_MS)
}

/// Live `.depth` summaries are a rolling 60 s window, not a per-poll delta.
/// Divide by that window so `bookVelocityPerSec` is an events/sec rate.
fn activity_window_secs(activity: &PullStackActivitySummary) -> f64 {
    let dt = activity.end_time_ms - activity.start_time_ms;
    if dt.is_finite() && dt > 1.0 {
        dt / 1_000.0
    } else {
        1.0
    }
}

fn mean_rate_in_window(samples: &VecDeque<(f64, f64)>, window_ms: f64) -> Option<f64> {
    let last = samples.back()?.0;
    let mut sum = 0.0;
    let mut n = 0u32;
    for (ts, rate) in samples.iter().rev() {
        if last - *ts > window_ms {
            break;
        }
        sum += rate;
        n += 1;
    }
    if n == 0 {
        None
    } else {
        Some(sum / f64::from(n))
    }
}

fn classify_velocity_regime(
    prev: Option<VelocityRegime>,
    short: f64,
    baseline: f64,
) -> VelocityRegime {
    let enter_fast = short >= VELOCITY_FAST_ABS_PER_SEC.max(baseline * VELOCITY_FAST_MULT);
    let stay_fast = short >= VELOCITY_FAST_ABS_PER_SEC.max(baseline * VELOCITY_FAST_EXIT_MULT);
    let enter_quiet = short <= baseline * VELOCITY_QUIET_MULT;
    let stay_quiet = short <= baseline * VELOCITY_QUIET_EXIT_MULT;
    match prev {
        Some(VelocityRegime::Fast) if stay_fast => VelocityRegime::Fast,
        Some(VelocityRegime::Quiet) if stay_quiet && !enter_fast => VelocityRegime::Quiet,
        _ if enter_fast => VelocityRegime::Fast,
        _ if enter_quiet => VelocityRegime::Quiet,
        _ => VelocityRegime::Normal,
    }
}

fn depth_at(samples: &VecDeque<DepthSample>, ts: f64, ask_side: bool) -> Option<f64> {
    samples
        .iter()
        .rev()
        .find(|s| s.ts <= ts + 1.0)
        .or_else(|| samples.front())
        .map(|s| if ask_side { s.ask_near } else { s.bid_near })
}

fn evict_older_than(buf: &mut VecDeque<(f64, f64)>, now: f64, keep_ms: f64) {
    while buf.front().is_some_and(|(ts, _)| now - *ts > keep_ms) {
        buf.pop_front();
    }
    while buf.len() > MAX_SAMPLES {
        buf.pop_front();
    }
}

fn evict_front(buf: &mut VecDeque<DepthSample>) {
    while buf.len() > MAX_SAMPLES {
        buf.pop_front();
    }
}

const _: () = assert!(DOM_SHORT_HORIZON_MS >= VELOCITY_WINDOW_MS);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::depth::{
        DepthBook, DepthCommand, DepthRecord, DepthSide, DomLevel, PullStackActivitySummary,
        SideActivitySummary,
    };

    /// 2024-01-02 10:00 ET (RTH).
    const RTH: f64 = 1_704_207_600_000.0;
    /// 2024-01-01 18:00 ET (Globex).
    const GLOBEX: f64 = 1_704_150_000_000.0;

    fn rec(ts: f64, cmd: DepthCommand, price: f64, qty: u32) -> DepthRecord {
        let side = match cmd {
            DepthCommand::AddBidLevel
            | DepthCommand::ModifyBidLevel
            | DepthCommand::DeleteBidLevel => Some(DepthSide::Bid),
            DepthCommand::AddAskLevel
            | DepthCommand::ModifyAskLevel
            | DepthCommand::DeleteAskLevel => Some(DepthSide::Ask),
            _ => None,
        };
        DepthRecord {
            timestamp_ms: ts,
            command: cmd,
            side,
            end_of_batch: true,
            num_orders: 1,
            price,
            quantity: qty,
        }
    }

    fn snapshot_from_qty(ts: f64, bid_qty: u32, ask_qty: u32) -> crate::depth::DomSnapshot {
        snapshot_book(ts, 21_000.0, bid_qty, 21_000.25, ask_qty)
    }

    fn snapshot_book(
        ts: f64,
        bid: f64,
        bid_qty: u32,
        ask: f64,
        ask_qty: u32,
    ) -> crate::depth::DomSnapshot {
        let mut book = DepthBook::default();
        book.apply(&rec(ts, DepthCommand::AddBidLevel, bid, bid_qty));
        book.apply(&rec(ts, DepthCommand::AddAskLevel, ask, ask_qty));
        book.snapshot("test.depth", ts, 10)
    }

    fn activity(events: u64, pulled: f64, removed: f64, stacked: f64) -> PullStackActivitySummary {
        let side = SideActivitySummary {
            add_events: events / 4,
            modify_up_events: events / 4,
            modify_down_events: events / 4,
            delete_events: events - 3 * (events / 4),
            stacked_quantity: stacked,
            removed_quantity: removed,
            estimated_filled_quantity: (removed - pulled).max(0.0),
            estimated_pulled_quantity: pulled,
        };
        PullStackActivitySummary {
            source_file: "test.depth".into(),
            start_time_ms: 0.0,
            end_time_ms: 1_000.0,
            session_date: "2024-01-02".into(),
            record_count: events as usize,
            batch_count: 1,
            bid: side.clone(),
            ask: side,
            top_pull_levels: Vec::new(),
            top_stack_levels: Vec::new(),
        }
    }

    fn pull_heavy_bid() -> PullStackActivitySummary {
        let mut a = activity(8, 0.0, 0.0, 0.0);
        a.bid = SideActivitySummary {
            add_events: 0,
            modify_up_events: 0,
            modify_down_events: 4,
            delete_events: 4,
            stacked_quantity: 0.0,
            removed_quantity: 80.0,
            estimated_filled_quantity: 10.0,
            estimated_pulled_quantity: 70.0,
        };
        a.ask = SideActivitySummary {
            add_events: 2,
            modify_up_events: 2,
            modify_down_events: 0,
            delete_events: 0,
            stacked_quantity: 40.0,
            removed_quantity: 5.0,
            estimated_filled_quantity: 5.0,
            estimated_pulled_quantity: 0.0,
        };
        a
    }

    fn pull_heavy_ask() -> PullStackActivitySummary {
        let mut a = pull_heavy_bid();
        std::mem::swap(&mut a.bid, &mut a.ask);
        a
    }

    fn activity_window(
        start_ms: f64,
        end_ms: f64,
        events: u64,
        pulled: f64,
        removed: f64,
        stacked: f64,
    ) -> PullStackActivitySummary {
        let mut a = activity(events, pulled, removed, stacked);
        a.start_time_ms = start_ms;
        a.end_time_ms = end_ms;
        a
    }

    #[test]
    fn fail_closed_without_book() {
        let p = DomClusterPipeline::new();
        let snap = p.snapshot();
        assert!(!snap.book_present);
        assert!(snap.mm_flow_status.is_none());
        assert!(snap.book_velocity_regime.is_none());
        assert!(p.recent_events().is_empty());
        assert_eq!(p.event_seq(), 0);
    }

    #[test]
    fn empty_snapshot_is_not_a_book() {
        let mut p = DomClusterPipeline::new();
        let empty = crate::depth::DomSnapshot {
            source_file: "missing.depth".into(),
            snapshot_timestamp_ms: RTH,
            session_date: "2024-01-02".into(),
            best_bid: None,
            best_ask: None,
            spread_ticks: None,
            touch_imbalance_ratio: None,
            total_bid_levels: 0,
            total_ask_levels: 0,
            bids: Vec::<DomLevel>::new(),
            asks: Vec::new(),
        };
        p.on_book_update(RTH, &empty, &activity(0, 0.0, 0.0, 0.0), false, 0.0, None);
        assert!(!p.snapshot().book_present);
        assert!(p.recent_events().is_empty());
    }

    #[test]
    fn book_velocity_regime_shifts_quiet_to_fast() {
        let mut p = DomClusterPipeline::new();
        let snap = snapshot_from_qty(RTH, 40, 40);
        let quiet = activity(4, 0.0, 10.0, 10.0);
        for i in 0..12 {
            let ts = RTH + i as f64 * 1_000.0;
            p.on_book_update(ts, &snap, &quiet, false, 21_000.0, Some(true));
        }
        let burst = activity(80, 0.0, 10.0, 10.0);
        p.on_book_update(RTH + 12_000.0, &snap, &burst, false, 21_000.0, Some(true));
        assert!(
            p.recent_events()
                .iter()
                .any(|e| e.event_type == EVENT_BOOK_VELOCITY_REGIME_SHIFT
                    && e.direction.as_deref() == Some("fast")),
            "events={:?}",
            p.recent_events()
                .iter()
                .map(|e| (e.event_type.as_str(), e.direction.clone()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn iceberg_reload_requires_fill_then_size_return() {
        let mut p = DomClusterPipeline::new();
        let t0 = RTH;
        p.on_book_update(
            t0,
            &snapshot_from_qty(t0, 40, 50),
            &activity(4, 0.0, 0.0, 50.0),
            false,
            21_000.25,
            Some(true),
        );
        p.on_trade(t0 + 100.0, 21_000.25, 40.0, true, false);
        p.on_book_update(
            t0 + 200.0,
            &snapshot_from_qty(t0 + 200.0, 40, 10),
            &activity(4, 0.0, 40.0, 0.0),
            false,
            21_000.25,
            Some(true),
        );
        p.on_book_update(
            t0 + 400.0,
            &snapshot_from_qty(t0 + 400.0, 40, 50),
            &activity(4, 0.0, 0.0, 40.0),
            false,
            21_000.25,
            Some(true),
        );
        assert!(
            p.recent_events()
                .iter()
                .any(|e| e.event_type == EVENT_ICEBERG_RELOAD
                    && e.direction.as_deref() == Some("ask")),
            "expected iceberg_reload, got {:?}",
            p.recent_events()
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(p.snapshot().iceberg_reload_price, Some(21_000.25));
    }

    #[test]
    fn pull_without_fill_is_not_iceberg_reload() {
        let mut p = DomClusterPipeline::new();
        let t0 = RTH;
        p.on_book_update(
            t0,
            &snapshot_from_qty(t0, 40, 50),
            &activity(4, 0.0, 0.0, 50.0),
            false,
            21_000.0,
            Some(false),
        );
        p.on_book_update(
            t0 + 200.0,
            &snapshot_from_qty(t0 + 200.0, 40, 10),
            &activity(4, 40.0, 40.0, 0.0),
            false,
            21_000.0,
            Some(false),
        );
        p.on_book_update(
            t0 + 400.0,
            &snapshot_from_qty(t0 + 400.0, 40, 50),
            &activity(4, 0.0, 0.0, 40.0),
            false,
            21_000.0,
            Some(false),
        );
        assert!(
            p.recent_events()
                .iter()
                .all(|e| e.event_type != EVENT_ICEBERG_RELOAD),
            "pull-then-repost must not infer iceberg"
        );
    }

    #[test]
    fn stop_run_requires_price_displacement_and_thinning() {
        let mut p = DomClusterPipeline::new();
        let t0 = RTH;
        p.on_book_update(
            t0,
            &snapshot_from_qty(t0, 80, 80),
            &activity(4, 0.0, 0.0, 80.0),
            false,
            21_000.0,
            Some(true),
        );
        p.on_trade(t0, 21_000.0, 10.0, true, false);
        for i in 1..=10 {
            let ts = t0 + i as f64 * 100.0;
            let price = 21_000.0 + i as f64 * NQ_TICK;
            p.on_trade(ts, price, 8.0, true, false);
            let ask = if i >= 6 { 20 } else { 80 };
            p.on_book_update(
                ts,
                &snapshot_from_qty(ts, 80, ask),
                &activity(8, 20.0, 40.0, 0.0),
                false,
                price,
                Some(true),
            );
        }
        assert!(
            p.recent_events()
                .iter()
                .any(|e| e.event_type == EVENT_STOP_RUN && e.direction.as_deref() == Some("up")),
            "events={:?}",
            p.recent_events()
                .iter()
                .map(|e| (e.event_type.as_str(), e.direction.clone()))
                .collect::<Vec<_>>()
        );
        assert!(p.snapshot().stop_cluster_price.is_some());
    }

    #[test]
    fn pull_intent_fires_on_one_sided_pull() {
        let mut p = DomClusterPipeline::new();
        let snap = snapshot_from_qty(RTH, 10, 40);
        p.on_book_update(RTH, &snap, &pull_heavy_bid(), false, 21_000.0, Some(false));
        assert!(
            p.recent_events()
                .iter()
                .any(|e| e.event_type == EVENT_PULL_INTENT && e.direction.as_deref() == Some("bid")),
            "events={:?}",
            p.recent_events()
                .iter()
                .map(|e| (e.event_type.as_str(), e.direction.clone()))
                .collect::<Vec<_>>()
        );
        let snap_s = p.snapshot();
        assert_eq!(snap_s.pull_intent_side.as_deref(), Some("bid"));
        assert!(snap_s.mm_flow_status.is_some());
    }

    #[test]
    fn composite_is_state_only_not_a_dom_family_event() {
        let mut p = DomClusterPipeline::new();
        p.on_book_update(
            RTH,
            &snapshot_from_qty(RTH, 10, 40),
            &pull_heavy_bid(),
            false,
            21_000.0,
            Some(false),
        );
        assert!(p
            .recent_events()
            .iter()
            .all(|e| e.event_type != "mm_flow" && e.event_type != "mm_flow_shift"));
        assert_eq!(p.snapshot().mm_flow_status.as_deref(), Some("ask_dominant"));
    }

    #[test]
    fn event_seq_survives_ring_saturation() {
        let mut p = DomClusterPipeline::new();
        let snap = snapshot_from_qty(RTH, 10, 40);
        for i in 0..80 {
            p.on_book_update(
                RTH + i as f64 * 6_000.0,
                &snap,
                &pull_heavy_bid(),
                false,
                21_000.0,
                Some(false),
            );
            // Clear so the next update is a fresh open.
            let mut quiet = activity(4, 0.0, 30.0, 0.0);
            quiet.bid.estimated_pulled_quantity = 5.0;
            quiet.bid.removed_quantity = 30.0;
            quiet.ask.estimated_pulled_quantity = 5.0;
            quiet.ask.removed_quantity = 30.0;
            p.on_book_update(
                RTH + i as f64 * 6_000.0 + 5_000.0,
                &snap,
                &quiet,
                false,
                21_000.0,
                Some(false),
            );
        }
        assert!(p.event_seq() > 64);
        assert!(p.recent_events().len() <= 64);
    }

    #[test]
    fn rth_and_globex_do_not_mix() {
        let mut p = DomClusterPipeline::new();
        p.on_book_update(
            GLOBEX,
            &snapshot_from_qty(GLOBEX, 10, 40),
            &pull_heavy_bid(),
            true,
            21_000.0,
            Some(false),
        );
        assert!(!p.recent_events().is_empty());
        p.on_book_update(
            RTH,
            &snapshot_from_qty(RTH, 40, 40),
            &activity(4, 0.0, 0.0, 10.0),
            false,
            21_000.0,
            Some(true),
        );
        assert!(
            p.recent_events()
                .iter()
                .all(|e| e.event_type != EVENT_PULL_INTENT),
            "session-kind flip must reset detector state"
        );
    }

    #[test]
    fn reset_clears_book_and_ring_but_keeps_seq() {
        let mut p = DomClusterPipeline::new();
        p.on_book_update(
            RTH,
            &snapshot_from_qty(RTH, 10, 40),
            &pull_heavy_bid(),
            false,
            21_000.0,
            Some(false),
        );
        let seq = p.event_seq();
        assert!(seq > 0);
        p.reset();
        assert_eq!(p.event_seq(), seq);
        assert!(p.recent_events().is_empty());
        assert!(!p.snapshot().book_present);
    }

    #[test]
    fn overlapping_60s_activity_uses_window_rate_not_sum() {
        let mut p = DomClusterPipeline::new();
        let snap = snapshot_from_qty(RTH, 40, 40);
        for i in 0..120 {
            let ts = RTH + i as f64 * 1_000.0;
            // 3000 events/side over a rolling 60s window → 100 events/sec, not 6000.
            let act = activity_window(ts - 60_000.0, ts, 3_000, 0.0, 10.0, 10.0);
            p.on_book_update(ts, &snap, &act, false, 21_000.0, Some(true));
        }
        let vel = p
            .snapshot()
            .book_velocity_per_sec
            .expect("rate after overlapping polls");
        assert!(
            vel > 50.0 && vel < 200.0,
            "expected ~100 events/sec from a 60s window, got {vel}"
        );
        assert_eq!(p.snapshot().book_velocity_regime.as_deref(), Some("normal"));
        assert!(
            p.recent_events()
                .iter()
                .all(|e| e.event_type != EVENT_BOOK_VELOCITY_REGIME_SHIFT
                    || e.direction.as_deref() != Some("fast")),
            "summing overlapping 60s totals must not classify as fast"
        );
    }

    #[test]
    fn classify_velocity_fast_and_quiet_hysteresis() {
        let baseline = 40.0;
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Normal), 100.0, baseline),
            VelocityRegime::Fast
        );
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Fast), baseline * 2.1, baseline),
            VelocityRegime::Fast,
            "stay fast while short ≥ 1.8× baseline"
        );
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Fast), baseline * 1.5, baseline),
            VelocityRegime::Normal,
            "exit fast when short < 1.8× baseline"
        );
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Normal), baseline * 0.4, baseline),
            VelocityRegime::Quiet
        );
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Quiet), baseline * 0.5, baseline),
            VelocityRegime::Quiet,
            "stay quiet while short ≤ 0.6× baseline"
        );
        assert_eq!(
            classify_velocity_regime(Some(VelocityRegime::Quiet), baseline * 0.7, baseline),
            VelocityRegime::Normal,
            "exit quiet when short > 0.6× baseline"
        );
    }

    #[test]
    fn pull_intent_does_not_flip_side_before_dwell() {
        let mut p = DomClusterPipeline::new();
        let snap = snapshot_from_qty(RTH, 10, 40);
        p.on_book_update(RTH, &snap, &pull_heavy_bid(), false, 21_000.0, Some(false));
        assert_eq!(p.snapshot().pull_intent_side.as_deref(), Some("bid"));

        p.on_book_update(
            RTH + 500.0,
            &snap,
            &pull_heavy_ask(),
            false,
            21_000.0,
            Some(true),
        );
        assert_eq!(
            p.snapshot().pull_intent_side.as_deref(),
            Some("bid"),
            "must not flip sides before {PULL_INTENT_MIN_DWELL_MS} ms"
        );

        p.on_book_update(
            RTH + 2_500.0,
            &snap,
            &pull_heavy_ask(),
            false,
            21_000.0,
            Some(true),
        );
        assert_eq!(p.snapshot().pull_intent_side.as_deref(), Some("ask"));
        assert!(p.recent_events().iter().any(|e| e.event_type
            == format!("{EVENT_PULL_INTENT}_invalidated")
            && e.direction.as_deref() == Some("bid")));
        assert!(
            p.recent_events()
                .iter()
                .filter(
                    |e| e.event_type == EVENT_PULL_INTENT && e.direction.as_deref() == Some("ask")
                )
                .count()
                >= 1
        );
    }

    #[test]
    fn stop_cluster_price_latches_at_first_open() {
        let mut p = DomClusterPipeline::new();
        let t0 = RTH;
        p.on_book_update(
            t0,
            &snapshot_book(t0, 21_000.0, 80, 21_000.25, 80),
            &activity(4, 0.0, 0.0, 80.0),
            false,
            21_000.0,
            Some(true),
        );
        p.on_trade(t0, 21_000.0, 10.0, true, false);
        let mut first_cluster = None;
        for i in 1..=12 {
            let ts = t0 + i as f64 * 100.0;
            let price = 21_000.0 + i as f64 * NQ_TICK;
            p.on_trade(ts, price, 8.0, true, false);
            let ask_qty = if i >= 6 { 20 } else { 80 };
            p.on_book_update(
                ts,
                &snapshot_book(ts, price - NQ_TICK, 80, price, ask_qty),
                &activity(8, 20.0, 40.0, 0.0),
                false,
                price,
                Some(true),
            );
            if first_cluster.is_none() {
                first_cluster = p.snapshot().stop_cluster_price;
            }
        }
        let first = first_cluster.expect("stop-run must open");
        let later = p
            .snapshot()
            .stop_cluster_price
            .expect("stop-run stays open");
        assert_eq!(
            first, later,
            "stopClusterPrice must latch at first open, not follow a moving ask"
        );
        assert!(p
            .recent_events()
            .iter()
            .any(|e| e.event_type == EVENT_STOP_RUN));
    }

    #[test]
    fn iceberg_reload_does_not_emit_when_same_side_print_is_stale() {
        let mut p = DomClusterPipeline::new();
        let t0 = RTH;
        p.on_book_update(
            t0,
            &snapshot_from_qty(t0, 40, 50),
            &activity(4, 0.0, 0.0, 50.0),
            false,
            21_000.0,
            Some(false),
        );
        p.on_trade(t0, 21_000.0, 40.0, false, false);
        let t_stale = t0 + 240_000.0;
        p.on_book_update(
            t_stale,
            &snapshot_from_qty(t_stale, 10, 50),
            &activity(4, 0.0, 30.0, 0.0),
            false,
            21_000.0,
            Some(false),
        );
        p.on_book_update(
            t_stale + 200.0,
            &snapshot_from_qty(t_stale + 200.0, 50, 50),
            &activity(4, 0.0, 0.0, 40.0),
            false,
            21_000.0,
            Some(false),
        );
        assert!(
            p.recent_events()
                .iter()
                .all(|e| e.event_type != EVENT_ICEBERG_RELOAD),
            "same-side print older than RELOAD_WINDOW_MS must not infer iceberg"
        );
    }
}
