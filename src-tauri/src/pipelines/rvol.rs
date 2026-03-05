use serde::{Deserialize, Serialize};

/// Number of 5-minute buckets in a 6.5-hour RTH session (9:30–16:00 ET).
pub const RVOL_RTH_BUCKETS: usize = 78;
/// Number of 5-minute buckets in a Globex session (18:00–09:30 ET = 15.5h = 186 buckets).
/// We use 18:00→09:30 because that's the overnight window before RTH opens.
pub const RVOL_GLOBEX_BUCKETS: usize = 186;
/// Globex start in ET minutes from midnight (18:00 = 1080).
const GLOBEX_START_ET: i32 = 1080;
/// RTH open in ET minutes from midnight (09:30 = 570).
const RTH_OPEN_ET: i32 = 570;

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
///
/// Supports both RTH and Globex sessions. Tracks actual per-bucket cumulative volume
/// for curve persistence, and computes velocity, acceleration, and percentile metrics.
#[derive(Debug)]
pub struct RvolPipeline {
    // --- Core state ---
    session_volume: f64,
    current_minute: i32,
    lookback_days: usize,
    is_globex: bool,

    // --- Historical baseline ---
    /// Average cumulative volume by 5-min bucket for the active session type.
    historical_curve: Vec<f64>,
    /// Per-day ratios at each bucket index for percentile computation.
    /// historical_ratios_at_bucket[bucket_idx] = vec of ratios from each historical day.
    historical_ratios_at_bucket: Vec<Vec<f64>>,

    // --- Globex baseline (loaded separately) ---
    globex_historical_curve: Vec<f64>,
    globex_ratios_at_bucket: Vec<Vec<f64>>,

    // --- Actual curve capture ---
    /// Cumulative volume snapshotted at each completed 5-min bucket boundary.
    bucket_volumes: Vec<f64>,
    last_completed_bucket: Option<usize>,

    // --- Velocity tracking ---
    /// RVOL ratio at each completed bucket boundary, for velocity/acceleration.
    bucket_ratios: Vec<f64>,
}

impl Default for RvolPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl RvolPipeline {
    pub fn new() -> Self {
        Self {
            session_volume: 0.0,
            current_minute: 0,
            lookback_days: 20,
            is_globex: false,
            historical_curve: Vec::new(),
            historical_ratios_at_bucket: Vec::new(),
            globex_historical_curve: Vec::new(),
            globex_ratios_at_bucket: Vec::new(),
            bucket_volumes: vec![0.0; RVOL_RTH_BUCKETS],
            last_completed_bucket: None,
            bucket_ratios: Vec::new(),
        }
    }

    /// Reset volume state for a new session. Call `start_session` after to set session type.
    pub fn reset(&mut self) {
        self.session_volume = 0.0;
        self.current_minute = 0;
        self.last_completed_bucket = None;
        self.bucket_ratios.clear();
        // bucket_volumes re-initialized in start_session
    }

    /// Configure the pipeline for a new session. Must be called after `reset()`.
    pub fn start_session(&mut self, is_globex: bool) {
        self.is_globex = is_globex;
        let num_buckets = self.total_buckets();
        self.bucket_volumes = vec![0.0; num_buckets];
    }

    /// Total number of 5-minute buckets for the current session type.
    pub fn total_buckets(&self) -> usize {
        if self.is_globex {
            RVOL_GLOBEX_BUCKETS
        } else {
            RVOL_RTH_BUCKETS
        }
    }

    /// Load the historical volume curve from prior session data.
    /// Each entry in `curves` is one session's actual cumulative volume by 5-min bucket.
    /// Computes the average curve and per-day per-bucket ratios for percentile computation.
    pub fn load_historical_curve(&mut self, curves: &[Vec<f64>]) {
        let (avg, ratios) = Self::compute_curve_stats(curves);
        self.historical_curve = avg;
        self.historical_ratios_at_bucket = ratios;
        self.lookback_days = curves.len();
    }

    /// Load Globex historical curves separately from RTH.
    pub fn load_globex_historical_curve(&mut self, curves: &[Vec<f64>]) {
        let (avg, ratios) = Self::compute_curve_stats(curves);
        self.globex_historical_curve = avg;
        self.globex_ratios_at_bucket = ratios;
    }

    /// Compute average curve and per-day ratios from a set of session curves.
    fn compute_curve_stats(curves: &[Vec<f64>]) -> (Vec<f64>, Vec<Vec<f64>>) {
        if curves.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let max_len = curves.iter().map(|c| c.len()).max().unwrap_or(0);

        // Compute average curve
        let avg: Vec<f64> = (0..max_len)
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

        // Compute per-day ratios at each bucket for percentile ranking
        let ratios: Vec<Vec<f64>> = (0..max_len)
            .map(|i| {
                let expected = avg[i];
                if expected <= 0.0 {
                    return Vec::new();
                }
                curves
                    .iter()
                    .filter_map(|c| c.get(i).map(|&v| v / expected))
                    .collect()
            })
            .collect();

        (avg, ratios)
    }

    /// Build a simple cumulative 5-minute baseline curve from total session volume.
    /// Used as fallback when no real per-bucket curves are stored.
    pub fn curve_from_total_volume(total_volume: f64) -> Vec<f64> {
        if total_volume <= 0.0 {
            return vec![0.0; RVOL_RTH_BUCKETS];
        }
        (1..=RVOL_RTH_BUCKETS)
            .map(|i| total_volume * (i as f64 / RVOL_RTH_BUCKETS as f64))
            .collect()
    }

    /// Process a trade. For RTH, `minute_of_session` is minutes since 9:30 ET.
    /// For Globex, pass `et_minutes` (minutes since midnight ET) and call with is_globex=true.
    pub fn on_trade(&mut self, volume: f64, minute_of_session: i32) {
        let bucket = if self.is_globex {
            match Self::globex_bucket_index(minute_of_session) {
                Some(b) => b,
                None => return,
            }
        } else {
            if minute_of_session < 0 {
                return;
            }
            (minute_of_session / 5) as usize
        };

        self.session_volume += volume;
        self.current_minute = minute_of_session;

        // Snapshot completed buckets when the bucket index advances
        if let Some(last) = self.last_completed_bucket {
            if bucket > last {
                // Fill any skipped buckets and the just-completed one
                for b in (last + 1)..bucket {
                    if b < self.bucket_volumes.len() {
                        self.bucket_volumes[b] = self.session_volume - volume;
                    }
                }
                // Record RVOL ratio at completed bucket
                let ratio_at_complete = self.compute_ratio_at_bucket(bucket.saturating_sub(1));
                self.bucket_ratios.push(ratio_at_complete);
            }
        } else if bucket > 0 {
            // First trade is past bucket 0 — fill earlier buckets
            for b in 0..bucket {
                if b < self.bucket_volumes.len() {
                    self.bucket_volumes[b] = 0.0;
                }
            }
            self.last_completed_bucket = Some(0);
        }

        // Update current bucket's cumulative volume
        if bucket < self.bucket_volumes.len() {
            self.bucket_volumes[bucket] = self.session_volume;
        }
        if self.last_completed_bucket.is_none() || bucket > self.last_completed_bucket.unwrap_or(0)
        {
            self.last_completed_bucket = Some(bucket);
        }
    }

    /// Compute RVOL ratio at a specific bucket index against the active historical curve.
    fn compute_ratio_at_bucket(&self, bucket: usize) -> f64 {
        let curve = self.active_curve();
        let expected = curve.get(bucket).copied().unwrap_or(0.0);
        let actual = self
            .bucket_volumes
            .get(bucket)
            .copied()
            .unwrap_or(self.session_volume);
        if expected <= 0.0 {
            if actual > 0.0 {
                return 2.0;
            }
            return 1.0;
        }
        actual / expected
    }

    /// Return the active historical curve based on session type.
    fn active_curve(&self) -> &[f64] {
        if self.is_globex {
            &self.globex_historical_curve
        } else {
            &self.historical_curve
        }
    }

    /// Return the active per-bucket ratios based on session type.
    fn active_ratios(&self) -> &[Vec<f64>] {
        if self.is_globex {
            &self.globex_ratios_at_bucket
        } else {
            &self.historical_ratios_at_bucket
        }
    }

    /// Convert ET minutes to a Globex bucket index.
    /// Globex runs 18:00 ET → 09:30 ET next day = 15.5 hours = 186 five-minute buckets.
    pub fn globex_bucket_index(et_minutes: i32) -> Option<usize> {
        let globex_minute = if et_minutes >= GLOBEX_START_ET {
            // Evening portion: 18:00 (1080) → 23:59 (1439)
            et_minutes - GLOBEX_START_ET
        } else if et_minutes < RTH_OPEN_ET {
            // Next-day portion: 00:00 (0) → 09:29 (569)
            et_minutes + (1440 - GLOBEX_START_ET)
        } else {
            // During RTH or transition — not Globex
            return None;
        };
        let bucket = (globex_minute / 5) as usize;
        if bucket < RVOL_GLOBEX_BUCKETS {
            Some(bucket)
        } else {
            None
        }
    }

    /// Current bucket index based on the latest trade.
    pub fn bucket_index(&self) -> usize {
        if self.is_globex {
            Self::globex_bucket_index(self.current_minute).unwrap_or(0)
        } else {
            (self.current_minute / 5).max(0) as usize
        }
    }

    /// Current RVOL ratio (1.0 = tracking average exactly).
    pub fn rvol_ratio(&self) -> f64 {
        let idx = self.bucket_index();
        let expected = self.active_curve().get(idx).copied().unwrap_or(0.0);
        if expected <= 0.0 {
            if self.session_volume > 0.0 {
                return 2.0; // no baseline, default to "high" if volume exists
            }
            return 1.0;
        }
        self.session_volume / expected
    }

    /// Expected cumulative volume at the current bucket from the historical baseline.
    pub fn expected_volume_at_bucket(&self) -> f64 {
        let idx = self.bucket_index();
        self.active_curve().get(idx).copied().unwrap_or(0.0)
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

    /// Rate of change of RVOL ratio over the last completed bucket transition.
    /// Positive = volume accelerating vs expectation, negative = decelerating.
    pub fn rvol_velocity(&self) -> f64 {
        if self.bucket_ratios.len() < 2 {
            return 0.0;
        }
        let n = self.bucket_ratios.len();
        self.bucket_ratios[n - 1] - self.bucket_ratios[n - 2]
    }

    /// Second derivative: acceleration of RVOL velocity.
    pub fn rvol_acceleration(&self) -> f64 {
        if self.bucket_ratios.len() < 3 {
            return 0.0;
        }
        let n = self.bucket_ratios.len();
        let v1 = self.bucket_ratios[n - 1] - self.bucket_ratios[n - 2];
        let v0 = self.bucket_ratios[n - 2] - self.bucket_ratios[n - 3];
        v1 - v0
    }

    /// Percentile rank of today's RVOL ratio at the current bucket vs the last N days.
    /// Returns 0–100. 50 = median, 90 = higher volume than 90% of historical days at this time.
    pub fn rvol_percentile(&self) -> f64 {
        let idx = self.bucket_index();
        let today_ratio = self.rvol_ratio();
        let ratios = self.active_ratios();

        let historical = match ratios.get(idx) {
            Some(v) if !v.is_empty() => v,
            _ => return 50.0, // default to median if no history
        };

        let count_below = historical.iter().filter(|&&r| r < today_ratio).count();
        (count_below as f64 / historical.len() as f64) * 100.0
    }

    /// Return the actual cumulative volume curve for the current session.
    /// Used for persisting to `session_volume_curves` at session end.
    pub fn current_curve(&self) -> Vec<f64> {
        self.bucket_volumes.clone()
    }

    pub fn session_volume(&self) -> f64 {
        self.session_volume
    }

    pub fn lookback_days(&self) -> usize {
        self.lookback_days
    }

    pub fn is_globex(&self) -> bool {
        self.is_globex
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rvol_with_no_history_defaults_high() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        p.on_trade(100.0, 5);
        // No historical curve → ratio defaults to 2.0 → High
        assert_eq!(p.classification(), RvolClassification::High);
    }

    #[test]
    fn rvol_tracks_ratio() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        p.load_historical_curve(&[vec![1000.0, 2000.0, 3000.0]]);
        p.on_trade(500.0, 0);
        let ratio = p.rvol_ratio();
        assert!((ratio - 0.5).abs() < 0.01);
        assert_eq!(p.classification(), RvolClassification::Low);
    }

    #[test]
    fn rvol_elevated_range() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        p.load_historical_curve(&[vec![1000.0]]);
        p.on_trade(1100.0, 0);
        assert_eq!(p.classification(), RvolClassification::Elevated);
    }

    #[test]
    fn bucket_volumes_captured_on_transition() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        p.load_historical_curve(&[vec![100.0; 78]]);

        // Trade in bucket 0 (minute 0-4)
        p.on_trade(50.0, 2);
        assert_eq!(p.bucket_index(), 0);
        assert_eq!(p.bucket_volumes[0], 50.0);

        // Trade in bucket 1 (minute 5-9) — should snapshot bucket 0
        p.on_trade(30.0, 5);
        assert_eq!(p.bucket_index(), 1);
        assert_eq!(p.bucket_volumes[1], 80.0); // cumulative

        // Trade in bucket 3 (skipping bucket 2) — fills gap
        p.on_trade(20.0, 15);
        assert_eq!(p.bucket_index(), 3);
        assert_eq!(p.bucket_volumes[3], 100.0); // cumulative
    }

    #[test]
    fn current_curve_returns_snapshot() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        p.on_trade(100.0, 0);
        p.on_trade(50.0, 5);
        let curve = p.current_curve();
        assert_eq!(curve.len(), RVOL_RTH_BUCKETS);
        assert_eq!(curve[0], 100.0); // first bucket was 100 when we moved to bucket 1
        assert_eq!(curve[1], 150.0); // second bucket cumulative
    }

    #[test]
    fn velocity_and_acceleration() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        // Historical: each bucket expects 100 cumulative
        p.load_historical_curve(&[vec![100.0, 200.0, 300.0, 400.0]]);

        p.on_trade(110.0, 0); // bucket 0: ratio = 110/100 = 1.1
        p.on_trade(10.0, 5); // bucket 1: ratio = 120/200 = 0.6, records ratio at bucket 0
        p.on_trade(200.0, 10); // bucket 2: ratio = 320/300 = 1.07, records ratio at bucket 1
        p.on_trade(50.0, 15); // bucket 3: records ratio at bucket 2

        // velocity = last ratio - prev ratio
        assert!(p.bucket_ratios.len() >= 2);
        let vel = p.rvol_velocity();
        // vel should be positive since bucket 2 ratio > bucket 1 ratio
        assert!(vel != 0.0);
    }

    #[test]
    fn percentile_ranking() {
        let mut p = RvolPipeline::new();
        p.start_session(false);
        // 5 historical days, bucket 0 volumes: 80, 90, 100, 110, 120
        // Average at bucket 0 = 100. Ratios: 0.8, 0.9, 1.0, 1.1, 1.2
        p.load_historical_curve(&[
            vec![80.0],
            vec![90.0],
            vec![100.0],
            vec![110.0],
            vec![120.0],
        ]);

        // Today at bucket 0: 105 volume → ratio = 105/100 = 1.05
        p.on_trade(105.0, 0);
        let pct = p.rvol_percentile();
        // 1.05 is above 0.8, 0.9, 1.0 (3 of 5) → 60th percentile
        assert!((pct - 60.0).abs() < 0.01);
    }

    #[test]
    fn globex_bucket_index_mapping() {
        // 18:00 ET (1080 minutes) = bucket 0
        assert_eq!(RvolPipeline::globex_bucket_index(1080), Some(0));
        // 18:05 = bucket 1
        assert_eq!(RvolPipeline::globex_bucket_index(1085), Some(1));
        // Midnight (0) = bucket 72 (360 minutes / 5)
        assert_eq!(RvolPipeline::globex_bucket_index(0), Some(72));
        // 09:25 ET (565) = bucket 185
        assert_eq!(RvolPipeline::globex_bucket_index(565), Some(185));
        // 09:30 ET (570) = RTH, not Globex
        assert_eq!(RvolPipeline::globex_bucket_index(570), None);
        // 16:00 ET (960) = RTH, not Globex
        assert_eq!(RvolPipeline::globex_bucket_index(960), None);
    }

    #[test]
    fn globex_session_uses_globex_curve() {
        let mut p = RvolPipeline::new();
        p.start_session(true);
        assert_eq!(p.total_buckets(), RVOL_GLOBEX_BUCKETS);
        assert_eq!(p.bucket_volumes.len(), RVOL_GLOBEX_BUCKETS);

        // Load Globex historical curve
        p.load_globex_historical_curve(&[vec![50.0; RVOL_GLOBEX_BUCKETS]]);

        // Trade at 18:00 ET (et_minutes = 1080, globex bucket 0)
        p.on_trade(60.0, 1080);
        let ratio = p.rvol_ratio();
        // 60 / 50 = 1.2
        assert!((ratio - 1.2).abs() < 0.01);
        assert_eq!(p.classification(), RvolClassification::High);
    }

    #[test]
    fn curve_from_total_volume_fallback() {
        let curve = RvolPipeline::curve_from_total_volume(7800.0);
        assert_eq!(curve.len(), RVOL_RTH_BUCKETS);
        // Linear: bucket 0 = 7800 * 1/78 = 100, bucket 77 = 7800
        assert!((curve[0] - 100.0).abs() < 0.01);
        assert!((curve[77] - 7800.0).abs() < 0.01);
    }
}
