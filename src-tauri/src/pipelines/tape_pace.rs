use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, Default)]
pub struct TapePaceSnapshot {
    pub ticks_per_sec_5s: f64,
    pub ticks_per_sec_30s: f64,
    pub ticks_per_sec_5m: f64,
    pub volume_per_sec_5s: f64,
    pub volume_per_sec_30s: f64,
    pub volume_per_sec_5m: f64,
    pub acceleration: f64,
    /// Current 5s pace ranked against session distribution (0.0 = slowest, 1.0 = fastest).
    pub pace_percentile: f64,
}

#[derive(Debug, Default)]
pub struct TapePacePipeline {
    ticks: VecDeque<(f64, f64)>,
    session_tick_count: u64,
    session_start_ms: Option<f64>,
    pace_samples: Vec<f64>,
    last_sample_ms: f64,
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

    fn dwell_at(&self, price: f64, tick_size: f64) -> f64 {
        let key = (price / tick_size).round() as i64;
        self.dwell_by_price.get(&key).copied().unwrap_or(0.0)
    }

    fn reset(&mut self) {
        self.current_price_key = None;
        self.arrival_ms = 0.0;
        self.dwell_by_price.clear();
    }
}

const TICK_SIZE: f64 = 0.25;

impl TapePacePipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.ticks.clear();
        self.session_tick_count = 0;
        self.session_start_ms = None;
        self.pace_samples.clear();
        self.last_sample_ms = 0.0;
        self.dwell_tracker.reset();
    }

    pub fn on_trade(&mut self, timestamp_ms: f64, volume: f64, price: f64) {
        if self.session_start_ms.is_none() {
            self.session_start_ms = Some(timestamp_ms);
        }
        self.session_tick_count = self.session_tick_count.saturating_add(1);
        self.ticks.push_back((timestamp_ms, volume));
        self.dwell_tracker.on_trade(price, timestamp_ms, TICK_SIZE);

        let cutoff = timestamp_ms - 300_000.0;
        while let Some((ts, _)) = self.ticks.front() {
            if *ts < cutoff {
                let _ = self.ticks.pop_front();
            } else {
                break;
            }
        }

        // Sample pace every 5 seconds for percentile calculation
        if timestamp_ms - self.last_sample_ms >= 5_000.0 {
            let (tps, _) = self.window_stats(timestamp_ms, 5_000.0);
            self.pace_samples.push(tps);
            self.last_sample_ms = timestamp_ms;
        }
    }

    fn window_stats(&self, now_ms: f64, window_ms: f64) -> (f64, f64) {
        let cutoff = now_ms - window_ms;
        let mut ticks = 0.0;
        let mut vol = 0.0;
        for (ts, v) in self.ticks.iter().rev() {
            if *ts < cutoff {
                break;
            }
            ticks += 1.0;
            vol += *v;
        }
        let secs = (window_ms / 1000.0).max(1.0);
        (ticks / secs, vol / secs)
    }

    fn pace_percentile(&self, current_pace: f64) -> f64 {
        if self.pace_samples.is_empty() {
            return 0.5;
        }
        let below = self
            .pace_samples
            .iter()
            .filter(|&&p| p <= current_pace)
            .count();
        below as f64 / self.pace_samples.len() as f64
    }

    /// Dwell time (ms) at a given price level this session.
    pub fn dwell_at_price(&self, price: f64) -> f64 {
        self.dwell_tracker.dwell_at(price, TICK_SIZE)
    }

    pub fn snapshot(&self, now_ms: f64) -> TapePaceSnapshot {
        let (t5, v5) = self.window_stats(now_ms, 5_000.0);
        let (t30, v30) = self.window_stats(now_ms, 30_000.0);
        let (t300, v300) = self.window_stats(now_ms, 300_000.0);
        TapePaceSnapshot {
            ticks_per_sec_5s: t5,
            ticks_per_sec_30s: t30,
            ticks_per_sec_5m: t300,
            volume_per_sec_5s: v5,
            volume_per_sec_30s: v30,
            volume_per_sec_5m: v300,
            acceleration: t5 - t30,
            pace_percentile: self.pace_percentile(t5),
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
        assert!(s.ticks_per_sec_5s > 0.0);
        assert!(s.volume_per_sec_5s > 0.0);
    }

    #[test]
    fn tracks_dwell_time() {
        let mut p = TapePacePipeline::new();
        p.on_trade(0.0, 1.0, 21000.0);
        p.on_trade(1000.0, 1.0, 21000.0);
        p.on_trade(2000.0, 1.0, 21000.25);
        assert!(p.dwell_at_price(21000.0) >= 2000.0);
    }

    #[test]
    fn percentile_starts_at_midpoint() {
        let p = TapePacePipeline::new();
        let s = p.snapshot(0.0);
        assert!((s.pace_percentile - 0.5).abs() < f64::EPSILON);
    }
}
