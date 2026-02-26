use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// A delta momentum reversal event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PinchEvent {
    pub timestamp_ms: f64,
    pub timeframe_label: String,
    pub severity: f64,
    pub pre_pinch_delta: f64,
    pub post_pinch_delta: f64,
    pub price_at_pinch: f64,
    pub price_displacement: f64,
    pub duration_ms: f64,
}

/// Rolling delta tracker for a single timeframe window.
#[derive(Debug)]
struct DeltaWindow {
    label: String,
    window_ms: f64,
    entries: VecDeque<(f64, f64, f64)>, // (timestamp, signed_volume, price)
    flow_rate_window_ms: f64,
}

impl DeltaWindow {
    fn new(label: &str, window_ms: f64) -> Self {
        Self {
            label: label.to_string(),
            window_ms,
            entries: VecDeque::new(),
            flow_rate_window_ms: 15_000.0,
        }
    }

    fn add(&mut self, timestamp_ms: f64, signed_vol: f64, price: f64) {
        self.entries.push_back((timestamp_ms, signed_vol, price));
        let cutoff = timestamp_ms - self.window_ms;
        while let Some((ts, _, _)) = self.entries.front() {
            if *ts < cutoff {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    fn window_delta(&self) -> f64 {
        self.entries.iter().map(|(_, v, _)| v).sum()
    }

    /// Short-trailing delta flow rate (delta per second in last N seconds).
    fn recent_flow_rate(&self, now_ms: f64) -> f64 {
        let cutoff = now_ms - self.flow_rate_window_ms;
        let recent_delta: f64 = self
            .entries
            .iter()
            .filter(|(ts, _, _)| *ts >= cutoff)
            .map(|(_, v, _)| v)
            .sum();
        let secs = (self.flow_rate_window_ms / 1000.0).max(1.0);
        recent_delta / secs
    }

    fn avg_flow_rate(&self, now_ms: f64) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let first_ts = self.entries.front().map(|(t, _, _)| *t).unwrap_or(now_ms);
        let span_secs = ((now_ms - first_ts) / 1000.0).max(1.0);
        self.window_delta() / span_secs
    }

    fn price_range(&self) -> (f64, f64) {
        let mut lo = f64::MAX;
        let mut hi = f64::MIN;
        for (_, _, p) in &self.entries {
            lo = lo.min(*p);
            hi = hi.max(*p);
        }
        if lo > hi {
            (0.0, 0.0)
        } else {
            (lo, hi)
        }
    }

    fn reset(&mut self) {
        self.entries.clear();
    }
}

/// Multi-timeframe delta pinch detector.
#[derive(Debug)]
pub struct PinchPipeline {
    windows: Vec<DeltaWindow>,
    events: Vec<PinchEvent>,
    last_event_ms: f64,
}

const RATE_LIMIT_MS: f64 = 10_000.0;
const MAX_EVENTS: usize = 100;

impl Default for PinchPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl PinchPipeline {
    pub fn new() -> Self {
        Self {
            windows: vec![
                DeltaWindow::new("1m", 60_000.0),
                DeltaWindow::new("5m", 300_000.0),
                DeltaWindow::new("15m", 900_000.0),
                DeltaWindow::new("30m", 1_800_000.0),
            ],
            events: Vec::new(),
            last_event_ms: 0.0,
        }
    }

    pub fn reset(&mut self) {
        for w in &mut self.windows {
            w.reset();
        }
        self.events.clear();
        self.last_event_ms = 0.0;
    }

    pub fn on_trade(&mut self, timestamp_ms: f64, price: f64, volume: f64, is_buy: bool) {
        let signed = if is_buy { volume } else { -volume };

        for w in &mut self.windows {
            w.add(timestamp_ms, signed, price);
        }

        if timestamp_ms - self.last_event_ms < RATE_LIMIT_MS {
            return;
        }

        // Check each timeframe for pinch conditions
        let mut new_events = Vec::new();
        for w in &self.windows {
            let window_delta = w.window_delta();
            let recent_rate = w.recent_flow_rate(timestamp_ms);
            let avg_rate = w.avg_flow_rate(timestamp_ms);

            // Condition 1: sustained one-sided accumulation
            let sustained = window_delta.abs() > 50.0;

            // Condition 2: sudden opposing flow (recent rate opposes accumulated direction)
            let opposing = (window_delta > 0.0 && recent_rate < 0.0)
                || (window_delta < 0.0 && recent_rate > 0.0);

            // Condition 3: flow rate spike (2x average pace in opposing direction)
            let spike = recent_rate.abs() >= avg_rate.abs() * 2.0 && avg_rate.abs() > 0.0;

            if sustained && opposing && spike {
                let (price_lo, price_hi) = w.price_range();
                let severity = self.compute_severity(
                    window_delta.abs(),
                    recent_rate.abs(),
                    price_hi - price_lo,
                );
                new_events.push(PinchEvent {
                    timestamp_ms,
                    timeframe_label: w.label.clone(),
                    severity,
                    pre_pinch_delta: window_delta,
                    post_pinch_delta: recent_rate * (w.flow_rate_window_ms / 1000.0),
                    price_at_pinch: price,
                    price_displacement: price_hi - price_lo,
                    duration_ms: w.flow_rate_window_ms,
                });
            }
        }

        if !new_events.is_empty() {
            self.last_event_ms = timestamp_ms;
            for evt in new_events {
                self.events.push(evt);
            }
            if self.events.len() > MAX_EVENTS {
                let drain_to = self.events.len() - MAX_EVENTS;
                self.events.drain(0..drain_to);
            }
        }
    }

    fn compute_severity(&self, accumulated: f64, flow_rate: f64, displacement: f64) -> f64 {
        let vol_score = (accumulated / 200.0).min(2.0);
        let rate_score = (flow_rate / 10.0).min(2.0);
        let disp_score = (displacement / 5.0).min(1.0);
        (vol_score + rate_score + disp_score).min(5.0)
    }

    pub fn recent_events(&self) -> &[PinchEvent] {
        &self.events
    }

    /// Events from a specific timeframe.
    pub fn events_for_timeframe(&self, label: &str) -> Vec<&PinchEvent> {
        self.events
            .iter()
            .filter(|e| e.timeframe_label == label)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::PinchPipeline;

    #[test]
    fn no_pinch_without_opposing_flow() {
        let mut p = PinchPipeline::new();
        for i in 0..100 {
            p.on_trade(i as f64 * 100.0, 21000.0, 5.0, true);
        }
        assert!(p.recent_events().is_empty());
    }
}
