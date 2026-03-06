use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, Default)]
pub struct TapePaceSnapshot {
    pub ticks_per_sec_5s: Option<f64>,
    pub ticks_per_sec_30s: Option<f64>,
    pub ticks_per_sec_5m: Option<f64>,
    pub volume_per_sec_5s: Option<f64>,
    pub volume_per_sec_30s: Option<f64>,
    pub volume_per_sec_5m: Option<f64>,
    /// Smoothed and normalized pace change. Positive = short-term flow is building.
    pub acceleration: Option<f64>,
    /// Raw 5s minus 30s tick pace for debugging and calibration.
    pub raw_acceleration: Option<f64>,
    /// Current 5s pace ranked against session distribution (0.0 = slowest, 1.0 = fastest).
    pub pace_percentile: f64,
    /// Current 5s pace ranked against the recent rolling intraday distribution.
    pub rolling_pace_percentile: f64,
    /// Longer-horizon regime baseline to contextualize short-term pace.
    pub regime_ticks_per_sec_30m_ema: Option<f64>,
    pub regime_volume_per_sec_30m_ema: Option<f64>,
    /// How much of each window is covered by observed event-time data.
    pub coverage_5s: f64,
    pub coverage_30s: f64,
    pub coverage_5m: f64,
    pub valid_5s: bool,
    pub valid_30s: bool,
    pub valid_5m: bool,
    /// Anchor used for the rolling windows. This may extend slightly beyond the
    /// last trade when the feed is fresh, but is bounded to avoid wall-clock distortions.
    pub window_anchor_timestamp_ms: Option<f64>,
    pub last_trade_timestamp_ms: Option<f64>,
    pub event_time_lag_ms: Option<f64>,
}

#[derive(Debug, Default)]
pub struct TapePacePipeline {
    ticks: VecDeque<(f64, f64)>,
    session_tick_count: u64,
    session_start_ms: Option<f64>,
    pace_samples: Vec<f64>,
    rolling_pace_samples: VecDeque<f64>,
    last_sample_ms: Option<f64>,
    last_trade_timestamp_ms: Option<f64>,
    fast_ticks_ema: Option<f64>,
    slow_ticks_ema: Option<f64>,
    regime_ticks_ema_30m: Option<f64>,
    regime_volume_ema_30m: Option<f64>,
    dwell_tracker: DwellTracker,
}

/// Tracks how long price stays at each discretized level.
#[derive(Debug, Default)]
struct DwellTracker {
    current_price_key: Option<i64>,
    arrival_ms: f64,
    dwell_by_price: HashMap<i64, f64>,
}

impl DwellTracker {
    fn on_trade(&mut self, price: f64, timestamp_ms: f64, tick_size: f64) {
        let key = (price / tick_size).round() as i64;
        if let Some(prev_key) = self.current_price_key {
            if prev_key != key {
                let dwell = timestamp_ms - self.arrival_ms;
                *self.dwell_by_price.entry(prev_key).or_insert(0.0) += dwell;
                self.arrival_ms = timestamp_ms;
            }
        } else {
            self.arrival_ms = timestamp_ms;
        }
        self.current_price_key = Some(key);
    }

    fn dwell_at(&self, price: f64, tick_size: f64, anchor_ms: f64) -> f64 {
        let key = (price / tick_size).round() as i64;
        let historical = self.dwell_by_price.get(&key).copied().unwrap_or(0.0);
        if self.current_price_key == Some(key) && anchor_ms >= self.arrival_ms {
            historical + (anchor_ms - self.arrival_ms)
        } else {
            historical
        }
    }

    fn reset(&mut self) {
        self.current_price_key = None;
        self.arrival_ms = 0.0;
        self.dwell_by_price.clear();
    }
}

const TICK_SIZE: f64 = 0.25;
const MAX_WINDOW_MS: f64 = 300_000.0;
const SAMPLE_INTERVAL_MS: f64 = 5_000.0;
const MIN_COVERAGE_RATIO: f64 = 0.60;
const MAX_LIVE_EXTENSION_MS: f64 = 1_000.0;
const MIN_EFFECTIVE_DURATION_MS: f64 = 1_000.0;
const ROLLING_SAMPLE_CAPACITY: usize = 360;
const FAST_EMA_SAMPLES: f64 = 3.0;
const SLOW_EMA_SAMPLES: f64 = 12.0;
const REGIME_EMA_SAMPLES: f64 = 360.0;

#[derive(Debug, Clone, Copy, Default)]
struct WindowStats {
    ticks_per_sec: Option<f64>,
    volume_per_sec: Option<f64>,
    coverage_ratio: f64,
    valid: bool,
}

fn ema_alpha(samples: f64) -> f64 {
    2.0 / (samples + 1.0)
}

fn update_ema(current: &mut Option<f64>, sample: f64, alpha: f64) {
    *current = Some(match *current {
        Some(prev) => prev + alpha * (sample - prev),
        None => sample,
    });
}

impl TapePacePipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.ticks.clear();
        self.session_tick_count = 0;
        self.session_start_ms = None;
        self.pace_samples.clear();
        self.rolling_pace_samples.clear();
        self.last_sample_ms = None;
        self.last_trade_timestamp_ms = None;
        self.fast_ticks_ema = None;
        self.slow_ticks_ema = None;
        self.regime_ticks_ema_30m = None;
        self.regime_volume_ema_30m = None;
        self.dwell_tracker.reset();
    }

    pub fn on_trade(&mut self, timestamp_ms: f64, volume: f64, price: f64) {
        if self.session_start_ms.is_none() {
            self.session_start_ms = Some(timestamp_ms);
        }
        self.session_tick_count = self.session_tick_count.saturating_add(1);
        self.last_trade_timestamp_ms = Some(timestamp_ms);
        self.ticks.push_back((timestamp_ms, volume));
        self.dwell_tracker.on_trade(price, timestamp_ms, TICK_SIZE);

        let cutoff = timestamp_ms - MAX_WINDOW_MS;
        while let Some((ts, _)) = self.ticks.front() {
            if *ts < cutoff {
                let _ = self.ticks.pop_front();
            } else {
                break;
            }
        }

        // Backfill pace samples at fixed 5s event-time intervals so the
        // percentile distribution uses the same trailing-window semantics as snapshots.
        let mut next_sample_ms = self
            .last_sample_ms
            .map(|last| last + SAMPLE_INTERVAL_MS)
            .unwrap_or(timestamp_ms);
        while next_sample_ms <= timestamp_ms {
            self.record_sample(next_sample_ms);
            self.last_sample_ms = Some(next_sample_ms);
            next_sample_ms += SAMPLE_INTERVAL_MS;
        }
    }

    fn record_sample(&mut self, anchor_ms: f64) {
        let stats = self.window_stats(anchor_ms, SAMPLE_INTERVAL_MS);
        if !stats.valid {
            return;
        }

        let tps = stats.ticks_per_sec.unwrap_or(0.0);
        let vps = stats.volume_per_sec.unwrap_or(0.0);
        self.pace_samples.push(tps);
        self.rolling_pace_samples.push_back(tps);
        while self.rolling_pace_samples.len() > ROLLING_SAMPLE_CAPACITY {
            let _ = self.rolling_pace_samples.pop_front();
        }
        update_ema(&mut self.fast_ticks_ema, tps, ema_alpha(FAST_EMA_SAMPLES));
        update_ema(&mut self.slow_ticks_ema, tps, ema_alpha(SLOW_EMA_SAMPLES));
        update_ema(
            &mut self.regime_ticks_ema_30m,
            tps,
            ema_alpha(REGIME_EMA_SAMPLES),
        );
        update_ema(
            &mut self.regime_volume_ema_30m,
            vps,
            ema_alpha(REGIME_EMA_SAMPLES),
        );
    }

    fn bounded_anchor_ms(&self, query_ms: f64) -> Option<f64> {
        let last_trade_ms = self.last_trade_timestamp_ms?;
        if query_ms <= last_trade_ms {
            return Some(query_ms);
        }
        Some(last_trade_ms + (query_ms - last_trade_ms).min(MAX_LIVE_EXTENSION_MS))
    }

    fn window_stats(&self, anchor_ms: f64, window_ms: f64) -> WindowStats {
        let cutoff = anchor_ms - window_ms;
        let mut ticks = 0usize;
        let mut vol = 0.0;
        let mut first_trade_in_window: Option<f64> = None;
        for (ts, v) in self.ticks.iter().rev() {
            if *ts > anchor_ms {
                continue;
            }
            if *ts < cutoff {
                break;
            }
            first_trade_in_window = Some(*ts);
            ticks += 1;
            vol += *v;
        }

        if let Some(first_trade_ms) = first_trade_in_window {
            let effective_duration_ms = (anchor_ms - first_trade_ms).clamp(0.0, window_ms);
            let coverage_ratio = if window_ms > 0.0 {
                (effective_duration_ms / window_ms).clamp(0.0, 1.0)
            } else {
                1.0
            };
            if coverage_ratio < MIN_COVERAGE_RATIO {
                return WindowStats {
                    coverage_ratio,
                    valid: false,
                    ..Default::default()
                };
            }
            let effective_secs =
                (effective_duration_ms / 1000.0).max(MIN_EFFECTIVE_DURATION_MS / 1000.0);
            return WindowStats {
                ticks_per_sec: Some(ticks as f64 / effective_secs),
                volume_per_sec: Some(vol / effective_secs),
                coverage_ratio,
                valid: true,
            };
        }

        let coverage_ratio = self
            .session_start_ms
            .map(|session_start_ms| {
                let observed_duration_ms = (anchor_ms - session_start_ms).clamp(0.0, window_ms);
                if window_ms > 0.0 {
                    (observed_duration_ms / window_ms).clamp(0.0, 1.0)
                } else {
                    1.0
                }
            })
            .unwrap_or(0.0);
        if coverage_ratio < MIN_COVERAGE_RATIO {
            return WindowStats {
                coverage_ratio,
                valid: false,
                ..Default::default()
            };
        }
        WindowStats {
            ticks_per_sec: Some(0.0),
            volume_per_sec: Some(0.0),
            coverage_ratio,
            valid: true,
        }
    }

    fn percentile_rank<'a, I>(&self, samples: I, current_pace: f64) -> f64
    where
        I: IntoIterator<Item = &'a f64>,
    {
        let mut total = 0usize;
        let mut below = 0usize;
        for sample in samples {
            total += 1;
            if *sample <= current_pace {
                below += 1;
            }
        }
        if total == 0 {
            return 0.5;
        }
        below as f64 / total as f64
    }

    /// Dwell time (ms) at a given price level this session.
    pub fn dwell_at_price(&self, price: f64, query_ms: f64) -> Option<f64> {
        let anchor_ms = self.bounded_anchor_ms(query_ms)?;
        Some(self.dwell_tracker.dwell_at(price, TICK_SIZE, anchor_ms))
    }

    pub fn snapshot(&self, query_ms: f64) -> TapePaceSnapshot {
        let Some(anchor_ms) = self.bounded_anchor_ms(query_ms) else {
            return TapePaceSnapshot {
                pace_percentile: 0.5,
                rolling_pace_percentile: 0.5,
                ..Default::default()
            };
        };
        let w5 = self.window_stats(anchor_ms, 5_000.0);
        let w30 = self.window_stats(anchor_ms, 30_000.0);
        let w300 = self.window_stats(anchor_ms, 300_000.0);
        let pace_percentile = w5
            .ticks_per_sec
            .map(|tps| self.percentile_rank(self.pace_samples.iter(), tps))
            .unwrap_or(0.5);
        let rolling_pace_percentile = w5
            .ticks_per_sec
            .map(|tps| self.percentile_rank(self.rolling_pace_samples.iter(), tps))
            .unwrap_or(0.5);
        let raw_acceleration = match (w5.ticks_per_sec, w30.ticks_per_sec) {
            (Some(t5), Some(t30)) => Some(t5 - t30),
            _ => None,
        };
        let acceleration = match (
            self.fast_ticks_ema,
            self.slow_ticks_ema,
            self.regime_ticks_ema_30m,
        ) {
            (Some(fast), Some(slow), Some(regime)) => Some((fast - slow) / regime.max(1.0)),
            _ => None,
        };
        TapePaceSnapshot {
            ticks_per_sec_5s: w5.ticks_per_sec,
            ticks_per_sec_30s: w30.ticks_per_sec,
            ticks_per_sec_5m: w300.ticks_per_sec,
            volume_per_sec_5s: w5.volume_per_sec,
            volume_per_sec_30s: w30.volume_per_sec,
            volume_per_sec_5m: w300.volume_per_sec,
            acceleration,
            raw_acceleration,
            pace_percentile,
            rolling_pace_percentile,
            regime_ticks_per_sec_30m_ema: self.regime_ticks_ema_30m,
            regime_volume_per_sec_30m_ema: self.regime_volume_ema_30m,
            coverage_5s: w5.coverage_ratio,
            coverage_30s: w30.coverage_ratio,
            coverage_5m: w300.coverage_ratio,
            valid_5s: w5.valid,
            valid_30s: w30.valid,
            valid_5m: w300.valid,
            window_anchor_timestamp_ms: Some(anchor_ms),
            last_trade_timestamp_ms: self.last_trade_timestamp_ms,
            event_time_lag_ms: self
                .last_trade_timestamp_ms
                .map(|last_trade_ms| (query_ms - last_trade_ms).max(0.0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TapePacePipeline;

    #[test]
    fn pace_computes_rolling_windows() {
        let mut p = TapePacePipeline::new();
        p.on_trade(1_000.0, 1.0, 21000.0);
        p.on_trade(2_000.0, 1.0, 21000.0);
        p.on_trade(3_000.0, 2.0, 21000.25);
        let s = p.snapshot(3_000.0);
        assert!(!s.valid_5s);
        assert!(s.ticks_per_sec_5s.is_none());
    }

    #[test]
    fn tracks_dwell_time() {
        let mut p = TapePacePipeline::new();
        p.on_trade(0.0, 1.0, 21000.0);
        p.on_trade(1000.0, 1.0, 21000.0);
        p.on_trade(2000.0, 1.0, 21000.25);
        assert!(p.dwell_at_price(21000.0, 2_000.0).unwrap_or_default() >= 2000.0);
    }

    #[test]
    fn percentile_starts_at_midpoint() {
        let p = TapePacePipeline::new();
        let s = p.snapshot(0.0);
        assert!((s.pace_percentile - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn valid_window_uses_effective_duration_once_coverage_is_sufficient() {
        let mut p = TapePacePipeline::new();
        p.on_trade(0.0, 1.0, 21000.0);
        p.on_trade(2_000.0, 1.0, 21000.0);
        p.on_trade(4_000.0, 1.0, 21000.25);
        let s = p.snapshot(4_000.0);
        assert!(s.valid_5s);
        assert!(s.coverage_5s >= 0.8);
        assert!(s.ticks_per_sec_5s.unwrap_or_default() > 0.0);
        assert!(s.volume_per_sec_5s.unwrap_or_default() > 0.0);
    }

    #[test]
    fn quiet_full_window_is_valid_zero_pace() {
        let mut p = TapePacePipeline::new();
        p.on_trade(0.0, 1.0, 21000.0);
        p.on_trade(12_000.0, 1.0, 21000.25);
        let s = p.snapshot(10_000.0);
        assert!(s.valid_5s);
        assert_eq!(s.ticks_per_sec_5s, Some(0.0));
        assert_eq!(s.volume_per_sec_5s, Some(0.0));
    }

    #[test]
    fn stale_queries_bound_window_extension() {
        let mut p = TapePacePipeline::new();
        p.on_trade(0.0, 1.0, 21000.0);
        p.on_trade(5_000.0, 1.0, 21000.25);
        let s = p.snapshot(10_000.0);
        assert_eq!(s.window_anchor_timestamp_ms, Some(6_000.0));
        assert_eq!(s.event_time_lag_ms, Some(5_000.0));
    }

    #[test]
    fn rolling_percentile_and_regime_baseline_update_from_valid_samples() {
        let mut p = TapePacePipeline::new();
        for step in 0..8 {
            let ts = step as f64 * 1_000.0;
            p.on_trade(ts, 2.0 + step as f64, 21000.0 + (step % 2) as f64 * 0.25);
        }
        let s = p.snapshot(7_000.0);
        assert!(s.pace_percentile > 0.0);
        assert!(s.rolling_pace_percentile > 0.0);
        assert!(s.regime_ticks_per_sec_30m_ema.is_some());
        assert!(s.regime_volume_per_sec_30m_ema.is_some());
    }
}
