/// Incremental VWAP pipeline.
#[derive(Debug, Default)]
pub struct VwapPipeline {
    sum_pv: f64,
    sum_v: f64,
    sum_p2v: f64,
}

impl VwapPipeline {
    /// Create pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all accumulators for a new session.
    pub fn reset(&mut self) {
        self.sum_pv = 0.0;
        self.sum_v = 0.0;
        self.sum_p2v = 0.0;
    }

    /// Add one trade incrementally.
    pub fn add_trade(&mut self, price: f64, volume: f64) {
        if volume <= 0.0 {
            return;
        }
        self.sum_pv += price * volume;
        self.sum_v += volume;
        self.sum_p2v += price * price * volume;
    }

    /// Current VWAP.
    pub fn vwap(&self) -> f64 {
        if self.sum_v == 0.0 {
            return 0.0;
        }
        self.sum_pv / self.sum_v
    }

    /// Weighted standard deviation around VWAP.
    pub fn std_dev(&self) -> f64 {
        if self.sum_v == 0.0 {
            return 0.0;
        }
        let mean = self.vwap();
        let variance = (self.sum_p2v / self.sum_v) - (mean * mean);
        variance.max(0.0).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::VwapPipeline;

    #[test]
    fn vwap_matches_simple_manual_case() {
        let mut pipeline = VwapPipeline::new();
        pipeline.add_trade(100.0, 2.0);
        pipeline.add_trade(101.0, 3.0);
        let expected = (100.0 * 2.0 + 101.0 * 3.0) / 5.0;
        assert!((pipeline.vwap() - expected).abs() < 1e-9);
    }

    #[test]
    fn reset_clears_accumulators() {
        let mut pipeline = VwapPipeline::new();
        pipeline.add_trade(100.0, 2.0);
        assert!(pipeline.vwap() > 0.0);
        pipeline.reset();
        assert_eq!(pipeline.vwap(), 0.0);
        assert_eq!(pipeline.std_dev(), 0.0);
    }
}
