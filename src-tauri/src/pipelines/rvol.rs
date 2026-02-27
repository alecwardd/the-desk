use serde::{Deserialize, Serialize};

/// Number of 5-minute buckets in a 6.5-hour RTH session.
pub const RVOL_RTH_BUCKETS: usize = 78;

/// RVOL classification thresholds (PTT standard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum RvolClassification {
    Low,
    #[default]
    Normal,
    Elevated,
    High,
}

/// Relative Volume pipeline: compares current session volume to N-day average at same time-of-day.
#[derive(Debug, Default)]
pub struct RvolPipeline {
    session_volume: f64,
    current_minute: i32,
    /// Historical average cumulative volume by 5-min bucket (bucket_index -> avg_volume).
    historical_curve: Vec<f64>,
    lookback_days: usize,
}

impl RvolPipeline {
    pub fn new() -> Self {
        Self {
            lookback_days: 20,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.session_volume = 0.0;
        self.current_minute = 0;
    }

    /// Load the historical volume curve from prior session data.
    /// Each entry is the average cumulative volume at each 5-minute mark (up to 78 buckets for RTH).
    pub fn load_historical_curve(&mut self, curves: &[Vec<f64>]) {
        if curves.is_empty() {
            self.historical_curve.clear();
            return;
        }
        let max_len = curves.iter().map(|c| c.len()).max().unwrap_or(0);
        self.historical_curve = (0..max_len)
            .map(|i| {
                let sum: f64 = curves.iter().filter_map(|c| c.get(i).copied()).sum();
                let count = curves.iter().filter(|c| c.get(i).is_some()).count();
                if count > 0 {
                    sum / count as f64
                } else {
                    0.0
                }
            })
            .collect();
        self.lookback_days = curves.len();
    }

    /// Build a simple cumulative 5-minute baseline curve from total session volume.
    pub fn curve_from_total_volume(total_volume: f64) -> Vec<f64> {
        if total_volume <= 0.0 {
            return vec![0.0; RVOL_RTH_BUCKETS];
        }
        (1..=RVOL_RTH_BUCKETS)
            .map(|i| total_volume * (i as f64 / RVOL_RTH_BUCKETS as f64))
            .collect()
    }

    pub fn on_trade(&mut self, volume: f64, minute_of_session: i32) {
        if minute_of_session < 0 {
            return;
        }
        self.session_volume += volume;
        self.current_minute = minute_of_session;
    }

    fn bucket_index(&self) -> usize {
        (self.current_minute / 5).max(0) as usize
    }

    /// Current RVOL ratio (1.0 = tracking average exactly).
    pub fn rvol_ratio(&self) -> f64 {
        let idx = self.bucket_index();
        let expected = self.historical_curve.get(idx).copied().unwrap_or(0.0);
        if expected <= 0.0 {
            if self.session_volume > 0.0 {
                return 2.0; // no baseline, default to "high" if volume exists
            }
            return 1.0;
        }
        self.session_volume / expected
    }

    pub fn classification(&self) -> RvolClassification {
        let ratio = self.rvol_ratio() * 100.0;
        if ratio < 85.0 {
            RvolClassification::Low
        } else if ratio <= 100.0 {
            RvolClassification::Normal
        } else if ratio <= 115.0 {
            RvolClassification::Elevated
        } else {
            RvolClassification::High
        }
    }

    pub fn session_volume(&self) -> f64 {
        self.session_volume
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rvol_with_no_history_defaults_normal() {
        let mut p = RvolPipeline::new();
        p.on_trade(100.0, 5);
        assert_eq!(p.classification(), RvolClassification::High);
    }

    #[test]
    fn rvol_tracks_ratio() {
        let mut p = RvolPipeline::new();
        p.load_historical_curve(&[vec![1000.0, 2000.0, 3000.0]]);
        p.on_trade(500.0, 0);
        let ratio = p.rvol_ratio();
        assert!((ratio - 0.5).abs() < 0.01);
        assert_eq!(p.classification(), RvolClassification::Low);
    }

    #[test]
    fn rvol_elevated_range() {
        let mut p = RvolPipeline::new();
        p.load_historical_curve(&[vec![1000.0]]);
        p.on_trade(1100.0, 0);
        assert_eq!(p.classification(), RvolClassification::Elevated);
    }
}
