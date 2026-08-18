//! Swing-anchored leg-to-leg volume/delta profile (SIL-M5d Base Detector).
//!
//! Track C (IDEA-029): OFL-style rotational profiles anchored to swing
//! highs/lows, reset when a new rotation forms. This is **not** a copy of a
//! vendor chart study — it is a deterministic swing boundary plus incremental
//! volume-at-price / delta-at-price using the same tick primitives as
//! footprint/delta.
//!
//! Session scope: **Session**. Full reset on Asia (Globex open) and RTH, same
//! as footprint/pinch/absorption. London is a Globex continuation — a live
//! overnight rotation is **not** wiped at 02:00 ET. RTH and Globex never mix
//! (a session-kind flip resets even if `reset_segment` was skipped).
//!
//! Profile geometry is fail-closed: POC / HVN / LVN / VA / delta POC are
//! reported only when the active rotation is mature (`active`). A new or
//! unstable rotation is labeled `insufficient`.
//!
//! **Swing vs confirmation:** `legAnchorPrice` / `legAnchorTimeMs` / `legAgeMs`
//! are the last confirmed swing extreme (price and the time that price last
//! printed). Maturity still uses confirmation time (`start_ms`) so geometry
//! does not publish during the 15s/40-contract gate.
//!
//! **Volume attribution:** prints from a rotation's own swing through its
//! extreme belong to that leg. Counter-move prints after a new extreme (the
//! pending map) transfer to the new rotation at confirmation — they are not
//! left on the closing leg. Confirmation itself is booked to the new leg.
//!
//! False friends: [`crate::outcomes`] MFE/MAE "legs" and
//! `research::ib_campaign` "confluence leg" are unrelated.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};

/// NQ minimum tick (0.25 points = $5.00/contract). Never 0.01 or 1.0.
pub const NQ_TICK: f64 = 0.25;

/// Minimum price reversal, in ticks, to confirm a swing and close the active leg.
///
/// 32 ticks = 8.0 NQ points. Chop smaller than this does not form a new rotation.
pub const MIN_REVERSAL_TICKS: i64 = 32;

/// [`MIN_REVERSAL_TICKS`] expressed in NQ points (`32 * 0.25 = 8.0`).
pub const MIN_REVERSAL_POINTS: f64 = MIN_REVERSAL_TICKS as f64 * NQ_TICK;

/// Minimum elapsed time in the active rotation before a reversal can confirm
/// and before the profile is labeled mature.
pub const MIN_ELAPSED_MS: f64 = 15_000.0;

/// Minimum traded volume in the active rotation before a reversal can confirm
/// and before the profile is labeled mature.
pub const MIN_VOLUME: f64 = 40.0;

/// Confluence band vs same-session POC / VA / DNVA (2 ticks, matching EventDetector).
pub const CONFLUENCE_TICKS: i64 = 2;

/// Completed-leg ring buffer (not dumped on `get_state`).
const MAX_COMPLETED_LEGS: usize = 16;

/// Event ring buffer for [`crate::pipelines::FlowEventEmitter`].
const MAX_EVENTS: usize = 64;

/// Wire event type: a rotation direction was confirmed (bootstrap or new swing).
pub const EVENT_LEG_STARTED: &str = "leg_started";

/// Wire event type: a qualifying reversal closed the prior rotation.
pub const EVENT_LEG_COMPLETED: &str = "leg_completed";

/// No trades in the current session segment.
pub const STATUS_NONE: &str = "none";

/// Active rotation is too new or has no confirmed direction yet.
pub const STATUS_INSUFFICIENT: &str = "insufficient";

/// Confirmed direction and min age/volume — profile geometry is trustworthy.
pub const STATUS_ACTIVE: &str = "active";

/// Direction of a rotational leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LegDirection {
    Up,
    Down,
}

impl LegDirection {
    /// Wire label (`up` / `down`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }

    fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

/// Compact current-leg (and last completed-leg) snapshot for `MarketState`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegProfileSnapshot {
    pub status: String,
    pub direction: Option<String>,
    pub anchor_time_ms: Option<f64>,
    pub anchor_price: f64,
    pub age_ms: f64,
    pub volume: f64,
    pub net_delta: f64,
    pub poc: f64,
    pub hvn: f64,
    pub lvn: f64,
    pub va_high: f64,
    pub va_low: f64,
    pub delta_poc: f64,
    pub poc_at_session_poc: bool,
    pub poc_in_session_va: bool,
    pub va_overlaps_session_va: bool,
    pub poc_in_session_dnva: bool,
    pub last_direction: Option<String>,
    pub last_volume: f64,
    pub last_net_delta: f64,
    pub last_poc: f64,
}

/// Lifecycle event stored in the pipeline ring buffer (not a specialty MCP payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegProfileEvent {
    pub timestamp_ms: f64,
    pub event_type: String,
    pub direction: String,
    pub anchor_price: f64,
    pub extreme_price: f64,
    pub volume: f64,
    pub net_delta: f64,
    pub poc: f64,
    pub age_ms: f64,
}

#[derive(Debug, Clone)]
struct CompletedLeg {
    direction: LegDirection,
    volume: f64,
    net_delta: f64,
    poc: f64,
}

#[derive(Debug, Clone)]
struct ActiveRotation {
    direction: Option<LegDirection>,
    is_overnight: bool,
    /// Confirmation (or first-print) time — maturity gate only.
    start_ms: f64,
    /// Time the swing extreme that anchored this rotation last printed.
    anchor_ms: f64,
    anchor_price: f64,
    extreme_price: f64,
    /// Time `extreme_price` last printed (becomes the next rotation's `anchor_ms`).
    extreme_ms: f64,
    forming_high: f64,
    forming_low: f64,
    forming_high_ms: f64,
    forming_low_ms: f64,
    volume: f64,
    net_delta: f64,
    volume_by_price: HashMap<i64, f64>,
    delta_by_price: HashMap<i64, f64>,
    /// Volume/delta after the current extreme; transferred to the next leg.
    pending_volume: f64,
    pending_net_delta: f64,
    pending_volume_by_price: HashMap<i64, f64>,
    pending_delta_by_price: HashMap<i64, f64>,
}

/// Counter-move maps stripped from a closing rotation and given to the next one.
struct PendingMaps {
    volume: f64,
    net_delta: f64,
    volume_by_price: HashMap<i64, f64>,
    delta_by_price: HashMap<i64, f64>,
}

/// Incremental swing-anchored volume/delta profile engine.
#[derive(Debug)]
pub struct LegProfilePipeline {
    tick_size: f64,
    active: Option<ActiveRotation>,
    completed: VecDeque<CompletedLeg>,
    events: VecDeque<LegProfileEvent>,
    /// Monotonic lifecycle-event count for this segment (not the ring length).
    event_seq: u64,
}

impl Default for LegProfilePipeline {
    fn default() -> Self {
        Self::new(NQ_TICK)
    }
}

impl LegProfilePipeline {
    /// Create a pipeline for an instrument tick size (NQ = 0.25).
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            active: None,
            completed: VecDeque::new(),
            events: VecDeque::new(),
            event_seq: 0,
        }
    }

    /// Clear the active rotation, completed-leg ring, event ring, and seq.
    pub fn reset(&mut self) {
        self.active = None;
        self.completed.clear();
        self.events.clear();
        self.event_seq = 0;
    }

    /// Monotonic count of `leg_started` / `leg_completed` events this segment.
    ///
    /// Unlike [`Self::recent_events`] length, this keeps growing after the ring
    /// saturates so [`crate::pipelines::FlowEventEmitter`] can detect new tails.
    pub fn event_seq(&self) -> u64 {
        self.event_seq
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    fn tick_price(&self, price: f64) -> f64 {
        self.discretize(price) as f64 * self.tick_size
    }

    fn ticks_apart(&self, a: f64, b: f64) -> i64 {
        (self.discretize(a) - self.discretize(b)).abs()
    }

    fn confluence_band(&self) -> f64 {
        CONFLUENCE_TICKS as f64 * self.tick_size
    }

    /// Apply one classified trade. `is_overnight` is Globex (true) vs RTH (false).
    ///
    /// A flip between those two kinds resets the engine so RTH and Globex never
    /// share a rotation. London is still Globex (`is_overnight = true`) and
    /// does not flip.
    pub fn on_trade(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        is_buy: bool,
        is_overnight: bool,
    ) {
        if volume <= 0.0 || !price.is_finite() || !timestamp_ms.is_finite() {
            return;
        }
        let price = self.tick_price(price);

        if let Some(active) = &self.active {
            if active.is_overnight != is_overnight {
                self.reset();
            }
        }

        if self.active.is_none() {
            self.active =
                Some(self.new_rotation(timestamp_ms, price, volume, is_buy, is_overnight));
            return;
        }

        let qualifies = self
            .active
            .as_ref()
            .is_some_and(|a| Self::rotation_qualifies(a, timestamp_ms));

        if let Some(direction) = self.active.as_ref().and_then(|a| a.direction) {
            if qualifies && self.is_reversal(direction, price) {
                self.close_and_reverse(timestamp_ms, price, volume, is_buy, is_overnight);
                return;
            }
            self.extend_active(timestamp_ms, price, volume, is_buy);
            return;
        }

        self.extend_forming(timestamp_ms, price, volume, is_buy);
    }

    fn new_rotation(
        &self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        is_buy: bool,
        is_overnight: bool,
    ) -> ActiveRotation {
        let mut rotation = ActiveRotation {
            direction: None,
            is_overnight,
            start_ms: timestamp_ms,
            anchor_ms: timestamp_ms,
            anchor_price: price,
            extreme_price: price,
            extreme_ms: timestamp_ms,
            forming_high: price,
            forming_low: price,
            forming_high_ms: timestamp_ms,
            forming_low_ms: timestamp_ms,
            volume: 0.0,
            net_delta: 0.0,
            volume_by_price: HashMap::new(),
            delta_by_price: HashMap::new(),
            pending_volume: 0.0,
            pending_net_delta: 0.0,
            pending_volume_by_price: HashMap::new(),
            pending_delta_by_price: HashMap::new(),
        };
        Self::add_trade_to(&mut rotation, self.discretize(price), volume, is_buy);
        rotation
    }

    fn add_trade_to(rotation: &mut ActiveRotation, key: i64, volume: f64, is_buy: bool) {
        let signed = if is_buy { volume } else { -volume };
        rotation.volume += volume;
        rotation.net_delta += signed;
        *rotation.volume_by_price.entry(key).or_insert(0.0) += volume;
        *rotation.delta_by_price.entry(key).or_insert(0.0) += signed;
    }

    fn add_pending_trade(rotation: &mut ActiveRotation, key: i64, volume: f64, is_buy: bool) {
        let signed = if is_buy { volume } else { -volume };
        rotation.pending_volume += volume;
        rotation.pending_net_delta += signed;
        *rotation.pending_volume_by_price.entry(key).or_insert(0.0) += volume;
        *rotation.pending_delta_by_price.entry(key).or_insert(0.0) += signed;
    }

    fn clear_pending(rotation: &mut ActiveRotation) {
        rotation.pending_volume = 0.0;
        rotation.pending_net_delta = 0.0;
        rotation.pending_volume_by_price.clear();
        rotation.pending_delta_by_price.clear();
    }

    fn take_pending(rotation: &mut ActiveRotation) -> PendingMaps {
        let volume_by_price = std::mem::take(&mut rotation.pending_volume_by_price);
        let delta_by_price = std::mem::take(&mut rotation.pending_delta_by_price);
        let volume = std::mem::replace(&mut rotation.pending_volume, 0.0);
        let net_delta = std::mem::replace(&mut rotation.pending_net_delta, 0.0);

        for (&key, &vol) in &volume_by_price {
            if let Some(slot) = rotation.volume_by_price.get_mut(&key) {
                *slot -= vol;
                if *slot <= 1e-12 {
                    rotation.volume_by_price.remove(&key);
                }
            }
        }
        for (&key, &delta) in &delta_by_price {
            if let Some(slot) = rotation.delta_by_price.get_mut(&key) {
                *slot -= delta;
                if slot.abs() <= 1e-12 {
                    rotation.delta_by_price.remove(&key);
                }
            }
        }
        rotation.volume = (rotation.volume - volume).max(0.0);
        rotation.net_delta -= net_delta;

        PendingMaps {
            volume,
            net_delta,
            volume_by_price,
            delta_by_price,
        }
    }

    fn rotation_qualifies(rotation: &ActiveRotation, now_ms: f64) -> bool {
        let age = (now_ms - rotation.start_ms).max(0.0);
        age >= MIN_ELAPSED_MS && rotation.volume >= MIN_VOLUME
    }

    fn is_reversal(&self, direction: LegDirection, price: f64) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        match direction {
            LegDirection::Up => {
                self.ticks_apart(active.extreme_price, price) >= MIN_REVERSAL_TICKS
                    && price < active.extreme_price
            }
            LegDirection::Down => {
                self.ticks_apart(active.extreme_price, price) >= MIN_REVERSAL_TICKS
                    && price > active.extreme_price
            }
        }
    }

    fn extend_active(&mut self, timestamp_ms: f64, price: f64, volume: f64, is_buy: bool) {
        let key = self.discretize(price);
        if let Some(active) = &mut self.active {
            Self::add_trade_to(active, key, volume, is_buy);
            let new_extreme = match active.direction {
                Some(LegDirection::Up) if price >= active.extreme_price => true,
                Some(LegDirection::Down) if price <= active.extreme_price => true,
                _ => false,
            };
            if new_extreme {
                active.extreme_price = price;
                active.extreme_ms = timestamp_ms;
                Self::clear_pending(active);
            } else {
                Self::add_pending_trade(active, key, volume, is_buy);
            }
        }
    }

    fn extend_forming(&mut self, timestamp_ms: f64, price: f64, volume: f64, is_buy: bool) {
        let key = self.discretize(price);
        if let Some(active) = &mut self.active {
            Self::add_trade_to(active, key, volume, is_buy);
            if price > active.forming_high
                || (price == active.forming_high && timestamp_ms >= active.forming_high_ms)
            {
                active.forming_high = price;
                active.forming_high_ms = timestamp_ms;
            }
            if price < active.forming_low
                || (price == active.forming_low && timestamp_ms >= active.forming_low_ms)
            {
                active.forming_low = price;
                active.forming_low_ms = timestamp_ms;
            }
        }
        self.maybe_confirm_bootstrap(timestamp_ms);
    }

    fn maybe_confirm_bootstrap(&mut self, timestamp_ms: f64) {
        let Some(active) = self.active.as_ref() else {
            return;
        };
        if active.direction.is_some() {
            return;
        }
        if self.ticks_apart(active.forming_high, active.forming_low) < MIN_REVERSAL_TICKS {
            return;
        }
        if !Self::rotation_qualifies(active, timestamp_ms) {
            return;
        }
        let up_is_latest = active.forming_high_ms >= active.forming_low_ms;
        let (direction, anchor, extreme, anchor_ms, extreme_ms) = if up_is_latest {
            (
                LegDirection::Up,
                active.forming_low,
                active.forming_high,
                active.forming_low_ms,
                active.forming_high_ms,
            )
        } else {
            (
                LegDirection::Down,
                active.forming_high,
                active.forming_low,
                active.forming_high_ms,
                active.forming_low_ms,
            )
        };
        if let Some(active) = self.active.as_mut() {
            active.direction = Some(direction);
            active.anchor_price = anchor;
            active.anchor_ms = anchor_ms;
            active.extreme_price = extreme;
            active.extreme_ms = extreme_ms;
            Self::clear_pending(active);
        }
        self.push_event(timestamp_ms, EVENT_LEG_STARTED);
    }

    fn close_and_reverse(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        is_buy: bool,
        is_overnight: bool,
    ) {
        let Some(mut old) = self.active.take() else {
            return;
        };
        let pending = Self::take_pending(&mut old);
        let direction = old
            .direction
            .expect("reversal requires a confirmed direction");
        let age_ms = (timestamp_ms - old.anchor_ms).max(0.0);
        let poc = volume_poc(self.tick_size, &old.volume_by_price);
        self.push_event_record(LegProfileEvent {
            timestamp_ms,
            event_type: EVENT_LEG_COMPLETED.to_string(),
            direction: direction.as_str().to_string(),
            anchor_price: old.anchor_price,
            extreme_price: old.extreme_price,
            volume: old.volume,
            net_delta: old.net_delta,
            poc,
            age_ms,
        });
        self.completed.push_back(CompletedLeg {
            direction,
            volume: old.volume,
            net_delta: old.net_delta,
            poc,
        });
        if self.completed.len() > MAX_COMPLETED_LEGS {
            self.completed.pop_front();
        }

        let new_direction = direction.opposite();
        let mut next = ActiveRotation {
            direction: Some(new_direction),
            is_overnight,
            start_ms: timestamp_ms,
            anchor_ms: old.extreme_ms,
            anchor_price: old.extreme_price,
            extreme_price: price,
            extreme_ms: timestamp_ms,
            forming_high: price.max(old.extreme_price),
            forming_low: price.min(old.extreme_price),
            forming_high_ms: timestamp_ms,
            forming_low_ms: timestamp_ms,
            volume: pending.volume,
            net_delta: pending.net_delta,
            volume_by_price: pending.volume_by_price,
            delta_by_price: pending.delta_by_price,
            pending_volume: 0.0,
            pending_net_delta: 0.0,
            pending_volume_by_price: HashMap::new(),
            pending_delta_by_price: HashMap::new(),
        };
        Self::add_trade_to(&mut next, self.discretize(price), volume, is_buy);
        self.active = Some(next);
        self.push_event(timestamp_ms, EVENT_LEG_STARTED);
    }

    fn push_event(&mut self, timestamp_ms: f64, event_type: &str) {
        let Some(active) = self.active.as_ref() else {
            return;
        };
        let Some(direction) = active.direction else {
            return;
        };
        let age_ms = (timestamp_ms - active.anchor_ms).max(0.0);
        let poc = if Self::rotation_qualifies(active, timestamp_ms) {
            volume_poc(self.tick_size, &active.volume_by_price)
        } else {
            0.0
        };
        self.push_event_record(LegProfileEvent {
            timestamp_ms,
            event_type: event_type.to_string(),
            direction: direction.as_str().to_string(),
            anchor_price: active.anchor_price,
            extreme_price: active.extreme_price,
            volume: active.volume,
            net_delta: active.net_delta,
            poc,
            age_ms,
        });
    }

    fn push_event_record(&mut self, event: LegProfileEvent) {
        self.event_seq = self.event_seq.saturating_add(1);
        self.events.push_back(event);
        while self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
    }

    /// Lifecycle events for [`crate::pipelines::FlowEventEmitter`].
    pub fn recent_events(&self) -> &VecDeque<LegProfileEvent> {
        &self.events
    }

    /// Compact snapshot. Session POC/VA/DNVA must be the **same** session
    /// (RTH or Globex) as this pipeline — never mix. DNVA follows the delta
    /// pipeline's segment (London resets delta; a Globex-spanning leg may
    /// therefore confluence against London-only DNVA after 02:00 ET).
    pub fn snapshot(
        &self,
        now_ms: f64,
        session_poc: f64,
        session_va_high: f64,
        session_va_low: f64,
        session_dnva_high: f64,
        session_dnva_low: f64,
    ) -> LegProfileSnapshot {
        let last = self.completed.back();
        let Some(active) = &self.active else {
            return LegProfileSnapshot {
                status: STATUS_NONE.to_string(),
                last_direction: last.map(|c| c.direction.as_str().to_string()),
                last_volume: last.map(|c| c.volume).unwrap_or(0.0),
                last_net_delta: last.map(|c| c.net_delta).unwrap_or(0.0),
                last_poc: last.map(|c| c.poc).unwrap_or(0.0),
                ..Default::default()
            };
        };

        let age_ms = (now_ms - active.anchor_ms).max(0.0);
        let mature = active.direction.is_some() && Self::rotation_qualifies(active, now_ms);
        let status = if mature {
            STATUS_ACTIVE
        } else {
            STATUS_INSUFFICIENT
        };

        let geometry = if mature {
            profile_geometry(
                self.tick_size,
                &active.volume_by_price,
                &active.delta_by_price,
            )
        } else {
            ProfileGeometry::default()
        };

        let band = self.confluence_band();
        let session_va_valid = session_va_high >= session_va_low && session_va_high > 0.0;
        let session_dnva_valid = session_dnva_high >= session_dnva_low && session_dnva_high > 0.0;
        let session_poc_valid = session_poc > 0.0;
        let geometry_valid = mature && geometry.poc > 0.0;

        let poc_at_session_poc =
            geometry_valid && session_poc_valid && (geometry.poc - session_poc).abs() <= band;
        let poc_in_session_va = geometry_valid
            && session_va_valid
            && geometry.poc + band >= session_va_low
            && geometry.poc - band <= session_va_high;
        let va_overlaps_session_va = geometry_valid
            && session_va_valid
            && geometry.va_low <= session_va_high
            && session_va_low <= geometry.va_high;
        let poc_in_session_dnva = geometry_valid
            && session_dnva_valid
            && geometry.poc + band >= session_dnva_low
            && geometry.poc - band <= session_dnva_high;

        LegProfileSnapshot {
            status: status.to_string(),
            direction: active.direction.map(|d| d.as_str().to_string()),
            anchor_time_ms: Some(active.anchor_ms),
            anchor_price: active.anchor_price,
            age_ms,
            volume: active.volume,
            net_delta: active.net_delta,
            poc: geometry.poc,
            hvn: geometry.hvn,
            lvn: geometry.lvn,
            va_high: geometry.va_high,
            va_low: geometry.va_low,
            delta_poc: geometry.delta_poc,
            poc_at_session_poc,
            poc_in_session_va,
            va_overlaps_session_va,
            poc_in_session_dnva,
            last_direction: last.map(|c| c.direction.as_str().to_string()),
            last_volume: last.map(|c| c.volume).unwrap_or(0.0),
            last_net_delta: last.map(|c| c.net_delta).unwrap_or(0.0),
            last_poc: last.map(|c| c.poc).unwrap_or(0.0),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ProfileGeometry {
    poc: f64,
    hvn: f64,
    lvn: f64,
    va_high: f64,
    va_low: f64,
    delta_poc: f64,
}

fn volume_poc(tick_size: f64, volume_by_price: &HashMap<i64, f64>) -> f64 {
    let mut best_key: Option<i64> = None;
    let mut best_vol = f64::NEG_INFINITY;
    for (&key, &vol) in volume_by_price {
        if vol > best_vol || (vol == best_vol && best_key.is_none_or(|b| key < b)) {
            best_vol = vol;
            best_key = Some(key);
        }
    }
    best_key.map(|k| k as f64 * tick_size).unwrap_or(0.0)
}

fn volume_lvn(tick_size: f64, volume_by_price: &HashMap<i64, f64>, poc_key: Option<i64>) -> f64 {
    let mut best_key: Option<i64> = None;
    let mut best_vol = f64::INFINITY;
    for (&key, &vol) in volume_by_price {
        if vol <= 0.0 {
            continue;
        }
        if poc_key == Some(key) && volume_by_price.len() > 1 {
            continue;
        }
        if vol < best_vol || (vol == best_vol && best_key.is_none_or(|b| key > b)) {
            best_vol = vol;
            best_key = Some(key);
        }
    }
    best_key.map(|k| k as f64 * tick_size).unwrap_or(0.0)
}

fn delta_poc(tick_size: f64, delta_by_price: &HashMap<i64, f64>) -> f64 {
    let mut best_key: Option<i64> = None;
    let mut best_abs = f64::NEG_INFINITY;
    for (&key, &delta) in delta_by_price {
        let abs = delta.abs();
        if abs > best_abs || (abs == best_abs && best_key.is_none_or(|b| key < b)) {
            best_abs = abs;
            best_key = Some(key);
        }
    }
    best_key.map(|k| k as f64 * tick_size).unwrap_or(0.0)
}

/// Volume value area: start at volume POC and expand like [`super::TpoPipeline`]
/// VA (70% of **leg volume**, not "middle 70% of range").
///
/// Expansion walks the next **occupied** prices (prices that actually traded in
/// this leg), not empty ticks. A sparse swing would otherwise stop at the POC
/// with far less than 70% included.
fn volume_value_area(tick_size: f64, volume_by_price: &HashMap<i64, f64>) -> (f64, f64) {
    if volume_by_price.is_empty() {
        return (0.0, 0.0);
    }
    let total: f64 = volume_by_price.values().sum();
    if total <= 0.0 {
        return (0.0, 0.0);
    }
    let target = total * 0.7;
    let poc = volume_poc(tick_size, volume_by_price);
    let poc_key = (poc / tick_size).round() as i64;
    let mut keys: Vec<i64> = volume_by_price.keys().copied().collect();
    keys.sort_unstable();
    let Some(poc_idx) = keys.iter().position(|&k| k == poc_key) else {
        return (poc, poc);
    };
    let mut lo_idx = poc_idx;
    let mut hi_idx = poc_idx;
    let mut included = volume_by_price.get(&poc_key).copied().unwrap_or(0.0);

    while included < target {
        let below = lo_idx
            .checked_sub(1)
            .and_then(|i| keys.get(i).and_then(|k| volume_by_price.get(k).copied()));
        let above = keys
            .get(hi_idx + 1)
            .and_then(|k| volume_by_price.get(k).copied());
        match (below, above) {
            (None, None) => break,
            (Some(b), Some(a)) if a >= b => {
                hi_idx += 1;
                included += a;
            }
            (Some(b), Some(_)) => {
                lo_idx -= 1;
                included += b;
            }
            (None, Some(a)) => {
                hi_idx += 1;
                included += a;
            }
            (Some(b), None) => {
                lo_idx -= 1;
                included += b;
            }
        }
    }
    (
        keys[hi_idx] as f64 * tick_size,
        keys[lo_idx] as f64 * tick_size,
    )
}

fn profile_geometry(
    tick_size: f64,
    volume_by_price: &HashMap<i64, f64>,
    delta_by_price: &HashMap<i64, f64>,
) -> ProfileGeometry {
    let poc = volume_poc(tick_size, volume_by_price);
    let poc_key = if poc > 0.0 {
        Some((poc / tick_size).round() as i64)
    } else {
        None
    };
    let (va_high, va_low) = volume_value_area(tick_size, volume_by_price);
    ProfileGeometry {
        poc,
        // Compact snapshot: HVN is the single highest-volume node (no clustering).
        hvn: poc,
        lvn: volume_lvn(tick_size, volume_by_price, poc_key),
        va_high,
        va_low,
        delta_poc: delta_poc(tick_size, delta_by_price),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(p: &LegProfilePipeline, now: f64) -> LegProfileSnapshot {
        p.snapshot(now, 0.0, 0.0, 0.0, 0.0, 0.0)
    }

    fn up_then_reversal(p: &mut LegProfilePipeline) {
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, false);
        p.on_trade(20_000.0, 21_008.0, 8.0, false, false);
    }

    #[test]
    fn constants_are_nq_native() {
        assert_eq!(NQ_TICK, 0.25);
        assert_eq!(MIN_REVERSAL_POINTS, 8.0);
        assert_eq!(MIN_REVERSAL_TICKS as f64 * NQ_TICK, MIN_REVERSAL_POINTS);
        assert_ne!(NQ_TICK, 0.01);
        assert_ne!(NQ_TICK, 1.0);
    }

    #[test]
    fn up_swing_then_reversal_closes_leg_with_expected_volume_delta_poc() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, false);
        let before = snap(&p, 16_000.0);
        assert_eq!(before.status, STATUS_ACTIVE);
        assert_eq!(before.direction.as_deref(), Some("up"));
        assert_eq!(before.anchor_price, 21_000.0);
        assert_eq!(before.volume, 70.0);
        assert_eq!(before.net_delta, 70.0);
        assert_eq!(before.poc, 21_008.0);
        assert_eq!(before.hvn, 21_008.0);
        assert_eq!(before.poc, before.hvn);

        p.on_trade(20_000.0, 21_008.0, 8.0, false, false);
        let after = snap(&p, 20_000.0);
        assert_eq!(after.last_direction.as_deref(), Some("up"));
        assert_eq!(after.last_volume, 70.0);
        assert_eq!(after.last_net_delta, 70.0);
        assert_eq!(after.last_poc, 21_008.0);
        assert_eq!(after.direction.as_deref(), Some("down"));
        assert_eq!(after.status, STATUS_INSUFFICIENT);
        assert_eq!(after.volume, 8.0);
        assert_eq!(after.net_delta, -8.0);
        assert_eq!(after.poc, 0.0, "new rotation fails closed on geometry");
        let types: Vec<_> = p
            .recent_events()
            .iter()
            .map(|e| e.event_type.as_str())
            .collect();
        assert_eq!(
            types,
            vec![EVENT_LEG_STARTED, EVENT_LEG_COMPLETED, EVENT_LEG_STARTED]
        );
    }

    #[test]
    fn down_swing_then_reversal() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_016.0, 10.0, false, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, false, false);
        p.on_trade(16_000.0, 21_000.0, 10.0, false, false);
        let mid = snap(&p, 16_000.0);
        assert_eq!(mid.status, STATUS_ACTIVE);
        assert_eq!(mid.direction.as_deref(), Some("down"));
        assert_eq!(mid.anchor_price, 21_016.0);
        assert_eq!(mid.poc, 21_008.0);
        assert_eq!(mid.net_delta, -70.0);

        p.on_trade(20_000.0, 21_008.0, 8.0, true, false);
        let after = snap(&p, 20_000.0);
        assert_eq!(after.last_direction.as_deref(), Some("down"));
        assert_eq!(after.last_volume, 70.0);
        assert_eq!(after.last_net_delta, -70.0);
        assert_eq!(after.last_poc, 21_008.0);
        assert_eq!(after.direction.as_deref(), Some("up"));
        assert_eq!(after.status, STATUS_INSUFFICIENT);
    }

    #[test]
    fn chop_below_reversal_threshold_does_not_form_extra_legs() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        let mut t = 0.0;
        for i in 0..80 {
            let price = if i % 2 == 0 { 21_000.0 } else { 21_002.0 };
            p.on_trade(t, price, 5.0, i % 2 == 0, false);
            t += 1_000.0;
        }
        let s = snap(&p, t);
        assert_eq!(s.status, STATUS_INSUFFICIENT);
        assert!(s.direction.is_none());
        assert!(p.completed.is_empty());
        assert!(p.recent_events().is_empty());
        assert!(s.volume > MIN_VOLUME);
        assert!(s.age_ms > MIN_ELAPSED_MS);
    }

    #[test]
    fn reset_clears_the_active_leg() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        up_then_reversal(&mut p);
        assert!(p.active.is_some());
        assert!(!p.completed.is_empty());
        p.reset();
        let s = snap(&p, 30_000.0);
        assert_eq!(s.status, STATUS_NONE);
        assert!(s.direction.is_none());
        assert_eq!(s.volume, 0.0);
        assert!(p.recent_events().is_empty());
        assert!(p.completed.is_empty());
    }

    #[test]
    fn insufficient_new_rotation_is_labeled_not_mature() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 5.0, true, false);
        p.on_trade(100.0, 21_000.25, 5.0, true, false);
        let s = snap(&p, 100.0);
        assert_eq!(s.status, STATUS_INSUFFICIENT);
        assert_eq!(s.poc, 0.0);
        assert_eq!(s.va_high, 0.0);
        assert_eq!(s.va_low, 0.0);
        assert!(!s.poc_at_session_poc);
    }

    #[test]
    fn va_contains_approx_70pct_poc_inside_va_and_tick_grid() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        // Wide enough for a confirmed up-leg; volume concentrated for a real VA.
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(4_000.0, 21_004.0, 10.0, true, false);
        p.on_trade(8_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(12_000.0, 21_012.0, 10.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 20.0, true, false);
        let s = snap(&p, 16_000.0);
        assert_eq!(s.status, STATUS_ACTIVE);
        assert_eq!(s.poc, 21_008.0);
        assert!(s.va_low <= s.poc && s.poc <= s.va_high);
        assert_eq!((s.poc / NQ_TICK).fract(), 0.0);
        assert_eq!((s.va_high / NQ_TICK).fract(), 0.0);
        assert_eq!((s.va_low / NQ_TICK).fract(), 0.0);

        let mut vols: HashMap<i64, f64> = HashMap::new();
        vols.insert((21_000.0 / NQ_TICK).round() as i64, 10.0);
        vols.insert((21_004.0 / NQ_TICK).round() as i64, 10.0);
        vols.insert((21_008.0 / NQ_TICK).round() as i64, 50.0);
        vols.insert((21_012.0 / NQ_TICK).round() as i64, 10.0);
        vols.insert((21_016.0 / NQ_TICK).round() as i64, 20.0);
        let total: f64 = vols.values().sum();
        let in_va: f64 = vols
            .iter()
            .filter(|(k, _)| {
                let px = **k as f64 * NQ_TICK;
                px >= s.va_low && px <= s.va_high
            })
            .map(|(_, v)| *v)
            .sum();
        let pct = in_va / total;
        assert!(
            (0.60..=1.0).contains(&pct) && pct + 1e-9 >= 0.70,
            "VA should hold at least 70% of leg volume, got {:.1}%",
            pct * 100.0
        );
    }

    #[test]
    fn rth_and_globex_are_not_mixed() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 10.0, true, true);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, true);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, true);
        assert_eq!(snap(&p, 16_000.0).status, STATUS_ACTIVE);
        assert_eq!(snap(&p, 16_000.0).volume, 70.0);

        // RTH trade without an explicit reset_segment still must not keep Globex maps.
        p.on_trade(20_000.0, 21_020.0, 5.0, true, false);
        let s = snap(&p, 20_000.0);
        assert_eq!(s.status, STATUS_INSUFFICIENT);
        assert_eq!(s.volume, 5.0);
        assert_eq!(s.anchor_price, 21_020.0);
        assert!(p.completed.is_empty());
    }

    #[test]
    fn confluence_flags_use_same_session_levels() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, false);
        let s = p.snapshot(16_000.0, 21_008.0, 21_012.0, 21_004.0, 21_010.0, 21_006.0);
        assert!(s.poc_at_session_poc);
        assert!(s.poc_in_session_va);
        assert!(s.va_overlaps_session_va);
        assert!(s.poc_in_session_dnva);
    }

    #[test]
    fn prices_off_tick_are_discretized_to_nq_quarter() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.12, 10.0, true, false);
        let s = snap(&p, 0.0);
        assert_eq!(s.anchor_price, 21_000.00);
        p.on_trade(1_000.0, 21_000.38, 10.0, true, false);
        let s = snap(&p, 1_000.0);
        assert_eq!(s.anchor_price, 21_000.00);
        assert!(s.volume > 0.0);
    }

    #[test]
    fn anchor_time_is_swing_extreme_not_confirmation() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, false);
        let before = snap(&p, 16_000.0);
        assert_eq!(before.anchor_price, 21_000.0);
        assert_eq!(before.anchor_time_ms, Some(0.0));
        assert_eq!(before.age_ms, 16_000.0);

        p.on_trade(20_000.0, 21_008.0, 8.0, false, false);
        let after = snap(&p, 20_000.0);
        assert_eq!(after.anchor_price, 21_016.0);
        assert_eq!(
            after.anchor_time_ms,
            Some(16_000.0),
            "anchor time is when 21016 last printed, not confirmation at 20000"
        );
        assert_eq!(after.age_ms, 4_000.0);
        assert_eq!(after.status, STATUS_INSUFFICIENT);
    }

    #[test]
    fn counter_move_volume_belongs_to_new_leg() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 10.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 50.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 10.0, true, false);
        // 16 ticks of counter-move — not yet a reversal; pending for the next leg.
        p.on_trade(18_000.0, 21_012.0, 20.0, false, false);
        p.on_trade(20_000.0, 21_008.0, 8.0, false, false);
        let after = snap(&p, 20_000.0);
        assert_eq!(after.last_direction.as_deref(), Some("up"));
        assert_eq!(after.last_volume, 70.0);
        assert_eq!(after.last_net_delta, 70.0);
        assert_eq!(after.last_poc, 21_008.0);
        assert_eq!(after.direction.as_deref(), Some("down"));
        assert_eq!(after.volume, 28.0);
        assert_eq!(after.net_delta, -28.0);
    }

    #[test]
    fn event_seq_keeps_growing_after_ring_saturates() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        p.on_trade(0.0, 21_000.0, 20.0, true, false);
        p.on_trade(5_000.0, 21_008.0, 20.0, true, false);
        p.on_trade(16_000.0, 21_016.0, 20.0, true, false);
        let mut t = 16_000.0;
        let mut high = true;
        for _ in 0..40 {
            t += 16_000.0;
            if high {
                p.on_trade(t, 21_008.0, 40.0, false, false);
            } else {
                p.on_trade(t, 21_016.0, 40.0, true, false);
            }
            high = !high;
        }
        assert_eq!(p.recent_events().len(), MAX_EVENTS);
        assert!(p.event_seq() > MAX_EVENTS as u64);
        assert!(p.event_seq() > 80);
    }

    #[test]
    fn reset_zeros_event_seq() {
        let mut p = LegProfilePipeline::new(NQ_TICK);
        up_then_reversal(&mut p);
        assert!(p.event_seq() > 0);
        p.reset();
        assert_eq!(p.event_seq(), 0);
        assert!(p.recent_events().is_empty());
    }
}
