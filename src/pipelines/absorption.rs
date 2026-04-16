use std::collections::{HashMap, VecDeque};

use super::levels::KeyLevel;

const MAX_EVENTS: usize = 400;
const TRADE_MEMORY_MS: f64 = 120_000.0;
const ABSORPTION_WINDOW_MS: f64 = 20_000.0;
const EXHAUSTION_WINDOW_MS: f64 = 12_000.0;
const DIVERGENCE_RATE_LIMIT_MS: f64 = 20_000.0;
const EXHAUSTION_RATE_LIMIT_MS: f64 = 15_000.0;
const ABSORPTION_RATE_LIMIT_MS: f64 = 15_000.0;
const ACTIVE_SIGNAL_FRESHNESS_MS: f64 = 45_000.0;
const ACTIVE_ABSORPTION_DISTANCE_TICKS: f64 = 8.0;
const ACTIVE_EXHAUSTION_FRESHNESS_MS: f64 = 30_000.0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalStatus {
    Candidate,
    Confirmed,
    Invalidated,
}

impl SignalStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Confirmed => "confirmed",
            Self::Invalidated => "invalidated",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecentSignalSnapshot {
    pub is_active: bool,
    pub price: Option<f64>,
    pub direction: Option<String>,
    pub age_ms: Option<f64>,
    pub distance_ticks: Option<f64>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionPhase {
    Open,
    Midday,
    Close,
    Globex,
}

impl SessionPhase {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Midday => "midday",
            Self::Close => "close",
            Self::Globex => "globex",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AbsorptionEvent {
    pub timestamp_ms: f64,
    /// Subtype: absorption, exhaustion, delta_divergence.
    pub event_type: String,
    pub status: String,
    pub price: f64,
    pub severity: f64,
    pub direction: Option<String>,
    pub zone_low: Option<f64>,
    pub zone_high: Option<f64>,
    pub key_level: Option<String>,
    pub confirmation_deadline_ms: Option<f64>,
    pub confirmed_at_ms: Option<f64>,
    pub invalidated_at_ms: Option<f64>,
    pub invalidation_reason: Option<String>,
    pub pace_percentile: f64,
    pub rvol_ratio: f64,
    pub local_volatility_ticks: f64,
    pub regime_phase: String,
}

#[derive(Debug, Clone)]
struct TradeSample {
    timestamp_ms: f64,
    price: f64,
    volume: f64,
    signed_volume: f64,
}

#[derive(Debug, Clone)]
struct ExtremePoint {
    price: f64,
    delta: f64,
    timestamp_ms: f64,
}

#[derive(Debug, Clone)]
struct RegimeContext {
    phase: SessionPhase,
    pace_percentile: f64,
    rvol_ratio: f64,
    local_volatility_ticks: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectionDirection {
    Up,
    Down,
}

impl RejectionDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingSignal {
    subtype: String,
    zone_low: f64,
    zone_high: f64,
    trigger_price: f64,
    key_level: Option<String>,
    expected_rejection: RejectionDirection,
    expires_at_ms: f64,
    confirm_move_ticks: f64,
    invalidate_move_ticks: f64,
    start_delta: f64,
    severity: f64,
    pace_percentile: f64,
    rvol_ratio: f64,
    local_volatility_ticks: f64,
    regime_phase: String,
}

#[derive(Debug, Default)]
pub struct AbsorptionPipeline {
    tick_size: f64,
    trades: VecDeque<TradeSample>,
    recent_events: Vec<AbsorptionEvent>,
    pending_signals: Vec<PendingSignal>,
    last_absorption_at_zone: HashMap<i64, f64>,
    cumulative_delta: f64,
    last_exhaustion_candidate_ms: f64,
    last_divergence_candidate_ms: f64,
    last_high: Option<ExtremePoint>,
    prev_high: Option<ExtremePoint>,
    last_low: Option<ExtremePoint>,
    prev_low: Option<ExtremePoint>,
}

impl AbsorptionPipeline {
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.trades.clear();
        self.recent_events.clear();
        self.pending_signals.clear();
        self.last_absorption_at_zone.clear();
        self.cumulative_delta = 0.0;
        self.last_exhaustion_candidate_ms = 0.0;
        self.last_divergence_candidate_ms = 0.0;
        self.last_high = None;
        self.prev_high = None;
        self.last_low = None;
        self.prev_low = None;
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    fn classify_phase(minute_of_session: i32) -> SessionPhase {
        if minute_of_session < 0 {
            SessionPhase::Globex
        } else if minute_of_session <= 60 {
            SessionPhase::Open
        } else if minute_of_session >= 375 {
            SessionPhase::Close
        } else if (150..=240).contains(&minute_of_session) {
            SessionPhase::Midday
        } else {
            SessionPhase::Open
        }
    }

    fn local_volatility_ticks(&self, now_ms: f64) -> f64 {
        let lookback = now_ms - 60_000.0;
        let mut high = f64::NEG_INFINITY;
        let mut low = f64::INFINITY;
        for t in self.trades.iter().rev() {
            if t.timestamp_ms < lookback {
                break;
            }
            high = high.max(t.price);
            low = low.min(t.price);
        }
        if !high.is_finite() || !low.is_finite() || high < low {
            return 2.0;
        }
        ((high - low) / self.tick_size).max(1.0)
    }

    fn adaptive_zone_ticks(&self, regime: &RegimeContext) -> f64 {
        let mut ticks: f64 = match regime.phase {
            SessionPhase::Open => 4.0,
            SessionPhase::Close => 4.0,
            SessionPhase::Midday => 2.5,
            SessionPhase::Globex => 2.5,
        };
        if regime.local_volatility_ticks > 10.0 {
            ticks += 1.0;
        }
        if regime.local_volatility_ticks > 20.0 {
            ticks += 1.0;
        }
        ticks.clamp(2.0, 5.0)
    }

    fn base_absorption_threshold(&self, regime: &RegimeContext) -> f64 {
        let phase_base = match regime.phase {
            SessionPhase::Open => 105.0,
            SessionPhase::Close => 120.0,
            SessionPhase::Midday => 90.0,
            SessionPhase::Globex => 65.0,
        };
        let pace_scale = if regime.pace_percentile > 0.8 {
            1.2
        } else if regime.pace_percentile < 0.2 {
            0.85
        } else {
            1.0
        };
        let rvol_scale = regime.rvol_ratio.clamp(0.7, 1.4);
        (phase_base * pace_scale * rvol_scale).clamp(45.0, 260.0)
    }

    fn nearest_key_level(
        &self,
        price: f64,
        key_levels: &[KeyLevel],
        max_distance_ticks: f64,
    ) -> Option<(String, f64, f64)> {
        key_levels
            .iter()
            .map(|k| {
                let dist = ((k.price - price) / self.tick_size).abs();
                (format!("{:?}", k.level_type), k.price, dist)
            })
            .filter(|(_, _, dist)| *dist <= max_distance_ticks)
            .min_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
    }

    fn new_event_from_pending(
        pending: &PendingSignal,
        status: SignalStatus,
        timestamp_ms: f64,
        invalidation_reason: Option<&str>,
    ) -> AbsorptionEvent {
        AbsorptionEvent {
            timestamp_ms,
            event_type: pending.subtype.clone(),
            status: status.as_str().to_string(),
            price: pending.trigger_price,
            severity: pending.severity,
            direction: Some(pending.expected_rejection.as_str().to_string()),
            zone_low: Some(pending.zone_low),
            zone_high: Some(pending.zone_high),
            key_level: pending.key_level.clone(),
            confirmation_deadline_ms: Some(pending.expires_at_ms),
            confirmed_at_ms: (status == SignalStatus::Confirmed).then_some(timestamp_ms),
            invalidated_at_ms: (status == SignalStatus::Invalidated).then_some(timestamp_ms),
            invalidation_reason: invalidation_reason.map(|s| s.to_string()),
            pace_percentile: pending.pace_percentile,
            rvol_ratio: pending.rvol_ratio,
            local_volatility_ticks: pending.local_volatility_ticks,
            regime_phase: pending.regime_phase.clone(),
        }
    }

    fn push_event(&mut self, event: AbsorptionEvent) {
        self.recent_events.push(event);
        if self.recent_events.len() > MAX_EVENTS {
            let drain_to = self.recent_events.len() - MAX_EVENTS;
            self.recent_events.drain(0..drain_to);
        }
    }

    fn evaluate_pending_signals(&mut self, timestamp_ms: f64, price: f64) {
        let mut keep = Vec::with_capacity(self.pending_signals.len());
        let mut emitted = Vec::new();

        for pending in self.pending_signals.drain(..) {
            let move_ticks = (price - pending.trigger_price) / self.tick_size;
            let delta_change = self.cumulative_delta - pending.start_delta;

            let confirmed = match pending.expected_rejection {
                RejectionDirection::Down => move_ticks <= -pending.confirm_move_ticks,
                RejectionDirection::Up => move_ticks >= pending.confirm_move_ticks,
            };

            let invalidated = match pending.expected_rejection {
                RejectionDirection::Down => {
                    price >= pending.zone_high + pending.invalidate_move_ticks * self.tick_size
                        && delta_change > 60.0
                }
                RejectionDirection::Up => {
                    price <= pending.zone_low - pending.invalidate_move_ticks * self.tick_size
                        && delta_change < -60.0
                }
            };

            if confirmed {
                emitted.push(Self::new_event_from_pending(
                    &pending,
                    SignalStatus::Confirmed,
                    timestamp_ms,
                    None,
                ));
                continue;
            }
            if invalidated {
                emitted.push(Self::new_event_from_pending(
                    &pending,
                    SignalStatus::Invalidated,
                    timestamp_ms,
                    Some("accepted_through_zone_with_delta_reaccel"),
                ));
                continue;
            }
            if timestamp_ms > pending.expires_at_ms {
                emitted.push(Self::new_event_from_pending(
                    &pending,
                    SignalStatus::Invalidated,
                    timestamp_ms,
                    Some("timeout_no_rejection"),
                ));
                continue;
            }
            keep.push(pending);
        }

        self.pending_signals = keep;
        for event in emitted {
            self.push_event(event);
        }
    }

    fn queue_candidate(&mut self, pending: PendingSignal, timestamp_ms: f64) {
        let event = AbsorptionEvent {
            timestamp_ms,
            event_type: pending.subtype.clone(),
            status: SignalStatus::Candidate.as_str().to_string(),
            price: pending.trigger_price,
            severity: pending.severity,
            direction: Some(pending.expected_rejection.as_str().to_string()),
            zone_low: Some(pending.zone_low),
            zone_high: Some(pending.zone_high),
            key_level: pending.key_level.clone(),
            confirmation_deadline_ms: Some(pending.expires_at_ms),
            confirmed_at_ms: None,
            invalidated_at_ms: None,
            invalidation_reason: None,
            pace_percentile: pending.pace_percentile,
            rvol_ratio: pending.rvol_ratio,
            local_volatility_ticks: pending.local_volatility_ticks,
            regime_phase: pending.regime_phase.clone(),
        };
        self.push_event(event);
        self.pending_signals.push(pending);
    }

    fn recent_signal_snapshot(
        &self,
        subtype: &str,
        timestamp_ms: f64,
        current_price: Option<f64>,
        max_distance_ticks: Option<f64>,
        max_age_ms: f64,
    ) -> RecentSignalSnapshot {
        for event in self.recent_events.iter().rev() {
            if event.event_type != subtype {
                continue;
            }
            let age_ms = timestamp_ms - event.timestamp_ms;
            if !(0.0..=max_age_ms).contains(&age_ms) {
                continue;
            }
            let distance_ticks =
                current_price.map(|price| ((event.price - price) / self.tick_size).abs());
            if let (Some(distance), Some(max_distance)) = (distance_ticks, max_distance_ticks) {
                if distance > max_distance {
                    continue;
                }
            }
            return RecentSignalSnapshot {
                is_active: event.status == SignalStatus::Confirmed.as_str(),
                price: Some(event.price),
                direction: event.direction.clone(),
                age_ms: Some(age_ms),
                distance_ticks,
                status: Some(event.status.clone()),
            };
        }
        RecentSignalSnapshot::default()
    }

    fn detect_absorption_candidate(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        regime: &RegimeContext,
        key_levels: &[KeyLevel],
    ) {
        let zone_ticks = self.adaptive_zone_ticks(regime);
        let level_ctx = self.nearest_key_level(price, key_levels, zone_ticks + 1.5);
        let anchor_price = level_ctx
            .as_ref()
            .map(|(_, level_price, _)| *level_price)
            .unwrap_or(price);
        let zone_low = anchor_price - zone_ticks * self.tick_size;
        let zone_high = anchor_price + zone_ticks * self.tick_size;
        let volume_threshold = self.base_absorption_threshold(regime);
        let cutoff = timestamp_ms - ABSORPTION_WINDOW_MS;

        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;
        let mut zone_trades = Vec::new();

        for trade in &self.trades {
            if trade.timestamp_ms < cutoff {
                continue;
            }
            if trade.price < zone_low || trade.price > zone_high {
                continue;
            }
            zone_trades.push(trade);
            if trade.signed_volume >= 0.0 {
                buy_vol += trade.volume;
            } else {
                sell_vol += trade.volume;
            }
        }

        let total_vol = buy_vol + sell_vol;
        if total_vol < volume_threshold {
            return;
        }

        let Some(first_trade) = zone_trades.first() else {
            return;
        };
        let Some(last_trade) = zone_trades.last() else {
            return;
        };
        let start_price = first_trade.price;
        let end_price = last_trade.price;
        let progress_ticks = (end_price - start_price) / self.tick_size;
        let mut expected_rejection = None;
        let imbalance_ratio = if sell_vol > 0.0 {
            buy_vol / sell_vol
        } else {
            buy_vol
        };
        let inv_imbalance_ratio = if buy_vol > 0.0 {
            sell_vol / buy_vol
        } else {
            sell_vol
        };
        let buy_approach_ticks = (anchor_price - start_price) / self.tick_size;
        let buy_overrun_ticks = (end_price - anchor_price) / self.tick_size;
        let sell_approach_ticks = (start_price - anchor_price) / self.tick_size;
        let sell_overrun_ticks = (anchor_price - end_price) / self.tick_size;

        if imbalance_ratio >= 1.35
            && buy_approach_ticks >= 0.5
            && buy_overrun_ticks <= zone_ticks * 0.6
            && progress_ticks <= zone_ticks
        {
            expected_rejection = Some(RejectionDirection::Down);
        } else if inv_imbalance_ratio >= 1.35
            && sell_approach_ticks >= 0.5
            && sell_overrun_ticks <= zone_ticks * 0.6
            && progress_ticks >= -zone_ticks
        {
            expected_rejection = Some(RejectionDirection::Up);
        }

        let Some(rejection_dir) = expected_rejection else {
            return;
        };

        let zone_key = self.discretize(anchor_price);
        let last = self
            .last_absorption_at_zone
            .get(&zone_key)
            .copied()
            .unwrap_or(f64::NEG_INFINITY);
        if timestamp_ms - last < ABSORPTION_RATE_LIMIT_MS {
            return;
        }
        self.last_absorption_at_zone.insert(zone_key, timestamp_ms);

        let imbalance_strength = imbalance_ratio.max(inv_imbalance_ratio);
        let size_strength = (total_vol / volume_threshold).clamp(0.5, 2.0);
        let proximity_bonus = level_ctx
            .as_ref()
            .map(|(_, _, dist_ticks)| if *dist_ticks <= 1.0 { 0.5 } else { 0.0 })
            .unwrap_or(0.0);
        let severity = (size_strength * 2.0 + imbalance_strength + proximity_bonus).clamp(1.0, 5.0);
        let confirm_ticks = (2.5 + regime.local_volatility_ticks * 0.1).clamp(2.0, 6.0);
        let invalidate_ticks = (1.5 + regime.local_volatility_ticks * 0.06).clamp(1.5, 4.0);
        let timeout_ms = match regime.phase {
            SessionPhase::Open => 18_000.0,
            SessionPhase::Midday => 28_000.0,
            SessionPhase::Close => 16_000.0,
            SessionPhase::Globex => 25_000.0,
        };

        self.queue_candidate(
            PendingSignal {
                subtype: "absorption".to_string(),
                zone_low,
                zone_high,
                trigger_price: price,
                key_level: level_ctx.map(|(name, _, _)| name),
                expected_rejection: rejection_dir,
                expires_at_ms: timestamp_ms + timeout_ms,
                confirm_move_ticks: confirm_ticks,
                invalidate_move_ticks: invalidate_ticks,
                start_delta: self.cumulative_delta,
                severity,
                pace_percentile: regime.pace_percentile,
                rvol_ratio: regime.rvol_ratio,
                local_volatility_ticks: regime.local_volatility_ticks,
                regime_phase: regime.phase.as_str().to_string(),
            },
            timestamp_ms,
        );
    }

    fn detect_exhaustion_candidate(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        regime: &RegimeContext,
    ) {
        if timestamp_ms - self.last_exhaustion_candidate_ms < EXHAUSTION_RATE_LIMIT_MS {
            return;
        }
        let cutoff = timestamp_ms - EXHAUSTION_WINDOW_MS;
        let mut window = Vec::new();
        for trade in self.trades.iter().rev() {
            if trade.timestamp_ms < cutoff {
                break;
            }
            window.push(trade.clone());
        }
        if window.len() < 12 {
            return;
        }
        window.reverse();

        let mid_ts = cutoff + EXHAUSTION_WINDOW_MS * 0.5;
        let mut first_vol = 0.0;
        let mut second_vol = 0.0;
        let mut first_delta = 0.0;
        let mut second_delta = 0.0;
        let mut first_start = None;
        let mut first_end = None;
        let mut second_start = None;
        let mut second_end = None;

        for t in &window {
            if t.timestamp_ms < mid_ts {
                first_vol += t.volume;
                first_delta += t.signed_volume;
                if first_start.is_none() {
                    first_start = Some(t.price);
                }
                first_end = Some(t.price);
            } else {
                second_vol += t.volume;
                second_delta += t.signed_volume;
                if second_start.is_none() {
                    second_start = Some(t.price);
                }
                second_end = Some(t.price);
            }
        }

        let (Some(fs), Some(fe), Some(ss), Some(se)) =
            (first_start, first_end, second_start, second_end)
        else {
            return;
        };

        let move_first_ticks = (fe - fs) / self.tick_size;
        let move_second_ticks = (se - ss) / self.tick_size;
        let move_total_ticks = (se - fs) / self.tick_size;

        let eff_first = move_first_ticks.abs() / first_delta.abs().max(1.0);
        let eff_second = move_second_ticks.abs() / second_delta.abs().max(1.0);
        let efficiency_collapse = eff_second < eff_first * 0.65;
        let slowdown = move_second_ticks.abs() < move_first_ticks.abs() * 0.55;
        let still_aggressive =
            second_delta.abs() > 40.0 && second_delta.signum() == move_total_ticks.signum();
        let volume_faded = second_vol < first_vol * 0.85;
        let move_floor = (2.5 + regime.local_volatility_ticks * 0.15).clamp(2.0, 8.0);

        if !((efficiency_collapse || slowdown)
            && still_aggressive
            && volume_faded
            && move_total_ticks.abs() >= move_floor)
        {
            return;
        }

        self.last_exhaustion_candidate_ms = timestamp_ms;
        let expected = if move_total_ticks > 0.0 {
            RejectionDirection::Down
        } else {
            RejectionDirection::Up
        };
        let severity = ((eff_first / eff_second.max(0.01)) + move_total_ticks.abs() / move_floor)
            .clamp(1.0, 5.0);

        self.queue_candidate(
            PendingSignal {
                subtype: "exhaustion".to_string(),
                zone_low: price - 2.0 * self.tick_size,
                zone_high: price + 2.0 * self.tick_size,
                trigger_price: price,
                key_level: None,
                expected_rejection: expected,
                expires_at_ms: timestamp_ms + 22_000.0,
                confirm_move_ticks: (2.0 + regime.local_volatility_ticks * 0.1).clamp(2.0, 5.0),
                invalidate_move_ticks: 2.0,
                start_delta: self.cumulative_delta,
                severity,
                pace_percentile: regime.pace_percentile,
                rvol_ratio: regime.rvol_ratio,
                local_volatility_ticks: regime.local_volatility_ticks,
                regime_phase: regime.phase.as_str().to_string(),
            },
            timestamp_ms,
        );
    }

    fn maybe_update_extremes(&mut self, timestamp_ms: f64, price: f64) {
        let push_high = self
            .last_high
            .as_ref()
            .map(|h| price >= h.price + self.tick_size)
            .unwrap_or(true);
        if push_high {
            if let Some(last) = self.last_high.take() {
                self.prev_high = Some(last);
            }
            self.last_high = Some(ExtremePoint {
                price,
                delta: self.cumulative_delta,
                timestamp_ms,
            });
        }

        let push_low = self
            .last_low
            .as_ref()
            .map(|l| price <= l.price - self.tick_size)
            .unwrap_or(true);
        if push_low {
            if let Some(last) = self.last_low.take() {
                self.prev_low = Some(last);
            }
            self.last_low = Some(ExtremePoint {
                price,
                delta: self.cumulative_delta,
                timestamp_ms,
            });
        }
    }

    fn detect_divergence_candidate(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        regime: &RegimeContext,
        key_levels: &[KeyLevel],
    ) {
        if timestamp_ms - self.last_divergence_candidate_ms < DIVERGENCE_RATE_LIMIT_MS {
            return;
        }
        let proximity_ticks = (2.0 + regime.local_volatility_ticks * 0.1).clamp(2.0, 6.0);
        let near_level = self
            .nearest_key_level(price, key_levels, proximity_ticks)
            .map(|(name, _, _)| name);

        if near_level.is_none() {
            return;
        }

        let mut pending: Option<PendingSignal> = None;
        if let (Some(prev), Some(curr)) = (&self.prev_high, &self.last_high) {
            if curr.timestamp_ms == timestamp_ms && curr.price > prev.price {
                let weakening = prev.delta - curr.delta;
                let weakening_ratio = weakening / prev.delta.abs().max(1.0);
                if weakening_ratio > 0.2 {
                    pending = Some(PendingSignal {
                        subtype: "delta_divergence".to_string(),
                        zone_low: curr.price - 2.0 * self.tick_size,
                        zone_high: curr.price + 2.0 * self.tick_size,
                        trigger_price: curr.price,
                        key_level: near_level.clone(),
                        expected_rejection: RejectionDirection::Down,
                        expires_at_ms: timestamp_ms + 25_000.0,
                        confirm_move_ticks: (2.5 + regime.local_volatility_ticks * 0.08)
                            .clamp(2.0, 5.0),
                        invalidate_move_ticks: 2.5,
                        start_delta: self.cumulative_delta,
                        severity: (1.0 + weakening_ratio * 6.0).clamp(1.0, 5.0),
                        pace_percentile: regime.pace_percentile,
                        rvol_ratio: regime.rvol_ratio,
                        local_volatility_ticks: regime.local_volatility_ticks,
                        regime_phase: regime.phase.as_str().to_string(),
                    });
                }
            }
        }
        if pending.is_none() {
            if let (Some(prev), Some(curr)) = (&self.prev_low, &self.last_low) {
                if curr.timestamp_ms == timestamp_ms && curr.price < prev.price {
                    let strengthening = curr.delta - prev.delta;
                    let ratio = strengthening / prev.delta.abs().max(1.0);
                    if ratio > 0.2 {
                        pending = Some(PendingSignal {
                            subtype: "delta_divergence".to_string(),
                            zone_low: curr.price - 2.0 * self.tick_size,
                            zone_high: curr.price + 2.0 * self.tick_size,
                            trigger_price: curr.price,
                            key_level: near_level,
                            expected_rejection: RejectionDirection::Up,
                            expires_at_ms: timestamp_ms + 25_000.0,
                            confirm_move_ticks: (2.5 + regime.local_volatility_ticks * 0.08)
                                .clamp(2.0, 5.0),
                            invalidate_move_ticks: 2.5,
                            start_delta: self.cumulative_delta,
                            severity: (1.0 + ratio * 6.0).clamp(1.0, 5.0),
                            pace_percentile: regime.pace_percentile,
                            rvol_ratio: regime.rvol_ratio,
                            local_volatility_ticks: regime.local_volatility_ticks,
                            regime_phase: regime.phase.as_str().to_string(),
                        });
                    }
                }
            }
        }

        if let Some(signal) = pending {
            self.last_divergence_candidate_ms = timestamp_ms;
            self.queue_candidate(signal, timestamp_ms);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_trade(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        _move_ticks: f64,
        is_buy: bool,
        minute_of_session: i32,
        pace_percentile: f64,
        rvol_ratio: f64,
        key_levels: &[KeyLevel],
    ) {
        let signed_vol = if is_buy { volume } else { -volume };
        self.cumulative_delta += signed_vol;

        self.trades.push_back(TradeSample {
            timestamp_ms,
            price,
            volume,
            signed_volume: signed_vol,
        });
        let cutoff = timestamp_ms - TRADE_MEMORY_MS;
        while let Some(front) = self.trades.front() {
            if front.timestamp_ms < cutoff {
                self.trades.pop_front();
            } else {
                break;
            }
        }

        self.evaluate_pending_signals(timestamp_ms, price);
        self.maybe_update_extremes(timestamp_ms, price);

        let regime = RegimeContext {
            phase: Self::classify_phase(minute_of_session),
            pace_percentile,
            rvol_ratio,
            local_volatility_ticks: self.local_volatility_ticks(timestamp_ms),
        };

        self.detect_absorption_candidate(timestamp_ms, price, &regime, key_levels);
        self.detect_exhaustion_candidate(timestamp_ms, price, &regime);
        self.detect_divergence_candidate(timestamp_ms, price, &regime, key_levels);
    }

    pub fn recent_events(&self) -> &[AbsorptionEvent] {
        &self.recent_events
    }

    pub fn count_confirmed(&self, subtype: &str) -> usize {
        self.recent_events
            .iter()
            .filter(|e| e.event_type == subtype && e.status == SignalStatus::Confirmed.as_str())
            .count()
    }

    pub fn recent_confirmed_absorption_state(
        &self,
        timestamp_ms: f64,
        current_price: f64,
    ) -> RecentSignalSnapshot {
        self.recent_signal_snapshot(
            "absorption",
            timestamp_ms,
            Some(current_price),
            Some(ACTIVE_ABSORPTION_DISTANCE_TICKS),
            ACTIVE_SIGNAL_FRESHNESS_MS,
        )
    }

    pub fn recent_confirmed_exhaustion_state(&self, timestamp_ms: f64) -> RecentSignalSnapshot {
        self.recent_signal_snapshot(
            "exhaustion",
            timestamp_ms,
            None,
            None,
            ACTIVE_EXHAUSTION_FRESHNESS_MS,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::AbsorptionPipeline;
    use crate::pipelines::{KeyLevel, KeyLevelType};

    #[test]
    fn emits_absorption_candidate_with_zone_metadata() {
        let mut p = AbsorptionPipeline::new(0.25);
        for i in 0..30 {
            let price = 21_000.0 + (i % 2) as f64 * 0.25;
            p.on_trade(
                1_000.0 + i as f64 * 250.0,
                price,
                12.0,
                0.0,
                true,
                5,
                0.7,
                1.1,
                &[],
            );
        }
        assert!(p
            .recent_events()
            .iter()
            .any(|e| e.event_type == "absorption" && e.status == "candidate"));
    }

    #[test]
    fn confirms_absorption_after_rotation() {
        let mut p = AbsorptionPipeline::new(0.25);
        for i in 0..30 {
            p.on_trade(
                10_000.0 + i as f64 * 200.0,
                21_000.0 + (i % 2) as f64 * 0.25,
                12.0,
                0.0,
                true,
                10,
                0.75,
                1.1,
                &[],
            );
        }
        for i in 0..10 {
            p.on_trade(
                20_000.0 + i as f64 * 500.0,
                20_998.5 - i as f64 * 0.25,
                8.0,
                -1.0,
                false,
                12,
                0.6,
                1.0,
                &[],
            );
        }

        assert!(p
            .recent_events()
            .iter()
            .any(|e| e.event_type == "absorption" && e.status == "confirmed"));
    }

    #[test]
    fn buy_pressure_into_resistance_marks_down_direction() {
        let mut p = AbsorptionPipeline::new(0.25);
        let level = [KeyLevel {
            level_type: KeyLevelType::PriorDayHigh,
            price: 21_001.0,
        }];

        for i in 0..14 {
            p.on_trade(
                50_000.0 + i as f64 * 250.0,
                21_000.0 + (i.min(4) as f64 * 0.25),
                10.0,
                0.25,
                true,
                15,
                0.7,
                1.0,
                &level,
            );
        }

        let candidate = p
            .recent_events()
            .iter()
            .rev()
            .find(|e| e.event_type == "absorption" && e.status == "candidate")
            .expect("absorption candidate");
        assert_eq!(candidate.direction.as_deref(), Some("down"));
        assert_eq!(candidate.key_level.as_deref(), Some("PriorDayHigh"));
    }

    #[test]
    fn sell_pressure_into_support_marks_up_direction() {
        let mut p = AbsorptionPipeline::new(0.25);
        let level = [KeyLevel {
            level_type: KeyLevelType::PriorDayLow,
            price: 20_999.0,
        }];

        for i in 0..14 {
            p.on_trade(
                60_000.0 + i as f64 * 250.0,
                21_000.0 - (i.min(4) as f64 * 0.25),
                10.0,
                -0.25,
                false,
                15,
                0.7,
                1.0,
                &level,
            );
        }

        let candidate = p
            .recent_events()
            .iter()
            .rev()
            .find(|e| e.event_type == "absorption" && e.status == "candidate")
            .expect("absorption candidate");
        assert_eq!(candidate.direction.as_deref(), Some("up"));
        assert_eq!(candidate.key_level.as_deref(), Some("PriorDayLow"));
    }

    #[test]
    fn detects_exhaustion_candidate_with_time_window() {
        let mut p = AbsorptionPipeline::new(0.25);
        let base = 100_000.0;
        for i in 0..20 {
            p.on_trade(
                base + i as f64 * 500.0,
                21_000.0 + i as f64 * 0.25,
                15.0,
                1.0,
                true,
                40,
                0.8,
                1.2,
                &[],
            );
        }
        for i in 20..40 {
            p.on_trade(
                base + i as f64 * 500.0,
                21_005.0 + (i - 20) as f64 * 0.01,
                5.0,
                0.2,
                true,
                45,
                0.8,
                1.2,
                &[],
            );
        }

        assert!(p
            .recent_events()
            .iter()
            .any(|e| e.event_type == "exhaustion"));
    }
}
