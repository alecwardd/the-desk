use std::collections::HashMap;

/// Incremental delta profile pipeline.
#[derive(Debug)]
pub struct DeltaPipeline {
    tick_size: f64,
    delta_by_price: HashMap<i64, f64>,
    session_delta: f64,
}

impl DeltaPipeline {
    /// Create delta pipeline for an instrument.
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            delta_by_price: HashMap::new(),
            session_delta: 0.0,
        }
    }

    /// Reset delta profile and session accumulators for a new session.
    pub fn reset(&mut self) {
        self.delta_by_price.clear();
        self.session_delta = 0.0;
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    /// Add one trade classified as buy/sell.
    pub fn add_trade(&mut self, price: f64, volume: f64, is_buy: bool) {
        let key = self.discretize(price);
        let signed = if is_buy { volume } else { -volume };
        self.session_delta += signed;
        *self.delta_by_price.entry(key).or_insert(0.0) += signed;
    }

    /// Session cumulative delta.
    pub fn session_delta(&self) -> f64 {
        self.session_delta
    }

    /// Delta at a specific price level.
    pub fn delta_at_price(&self, price: f64) -> f64 {
        self.delta_by_price
            .get(&self.discretize(price))
            .copied()
            .unwrap_or(0.0)
    }

    /// Full delta profile as (price, delta), sorted by price.
    pub fn profile(&self) -> Vec<(f64, f64)> {
        let mut out: Vec<(f64, f64)> = self
            .delta_by_price
            .iter()
            .map(|(k, v)| (*k as f64 * self.tick_size, *v))
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Delta Neutral Pivot — midpoint of the DNVA high and low.
    pub fn dnp(&self) -> f64 {
        let (high, low) = self.dnva_bounds();
        (high + low) / 2.0
    }

    /// DNVA bounds via cumulative percentile: the range containing the middle 70%
    /// of absolute delta when sweeping from lowest to highest price.
    /// Returns (dnva_high, dnva_low).
    fn dnva_bounds(&self) -> (f64, f64) {
        if self.delta_by_price.is_empty() {
            return (0.0, 0.0);
        }
        let total_abs: f64 = self.delta_by_price.values().map(|v| v.abs()).sum::<f64>();
        if total_abs <= 0.0 {
            return (0.0, 0.0);
        }
        let target_low = total_abs * 0.15; // 15th percentile
        let target_high = total_abs * 0.85; // 85th percentile

        let mut prices: Vec<i64> = self.delta_by_price.keys().copied().collect();
        prices.sort();

        let mut cumulative = 0.0;
        let mut low_price: Option<i64> = None;
        let mut high_price: Option<i64> = None;

        for &price in &prices {
            let abs_d = self
                .delta_by_price
                .get(&price)
                .copied()
                .unwrap_or(0.0)
                .abs();
            cumulative += abs_d;
            if low_price.is_none() && cumulative >= target_low {
                low_price = Some(price);
            }
            if high_price.is_none() && cumulative >= target_high {
                high_price = Some(price);
                break;
            }
        }

        let last = *prices.last().unwrap();
        let low = (low_price.unwrap_or(last) as f64) * self.tick_size;
        let high = (high_price.unwrap_or(last) as f64) * self.tick_size;
        (high, low)
    }

    /// DNVA high.
    pub fn dnva_high(&self) -> f64 {
        self.dnva_bounds().0
    }

    /// DNVA low.
    pub fn dnva_low(&self) -> f64 {
        self.dnva_bounds().1
    }

    /// Check whether delta confirms a level for the trade direction.
    pub fn delta_confirmation_at_price(&self, price: f64, is_buy_setup: bool) -> bool {
        let d = self.delta_at_price(price);
        if is_buy_setup {
            d > 0.0
        } else {
            d < 0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DeltaPipeline;

    #[test]
    fn tracks_session_delta() {
        let mut pipeline = DeltaPipeline::new(0.25);
        pipeline.add_trade(21000.0, 5.0, true);
        pipeline.add_trade(21000.25, 2.0, false);
        assert_eq!(pipeline.session_delta(), 3.0);
    }

    #[test]
    fn reset_clears_delta_profile() {
        let mut pipeline = DeltaPipeline::new(0.25);
        pipeline.add_trade(21000.0, 5.0, true);
        assert_eq!(pipeline.session_delta(), 5.0);
        pipeline.reset();
        assert_eq!(pipeline.session_delta(), 0.0);
        assert_eq!(pipeline.dnp(), 0.0);
    }

    #[test]
    fn percentile_dnva_single_price() {
        let mut p = DeltaPipeline::new(0.25);
        p.add_trade(21000.0, 100.0, true);
        assert_eq!(p.dnva_low(), 21000.0);
        assert_eq!(p.dnva_high(), 21000.0);
        assert_eq!(p.dnp(), 21000.0);
    }

    #[test]
    fn percentile_dnva_skewed_low() {
        // Most delta at low prices: 21000=80, 21000.25=10, 21000.5=5, 21000.75=5
        // Total abs=100. 15%=15 (hit at 21000, cum=80), 85%=85 (hit at 21000.25, cum=90)
        let mut p = DeltaPipeline::new(0.25);
        p.add_trade(21000.0, 80.0, true);
        p.add_trade(21000.25, 10.0, false);
        p.add_trade(21000.5, 5.0, true);
        p.add_trade(21000.75, 5.0, false);
        assert_eq!(p.dnva_low(), 21000.0);
        assert_eq!(p.dnva_high(), 21000.25);
        assert_eq!(p.dnp(), 21000.125);
    }

    #[test]
    fn percentile_dnva_skewed_high() {
        // Most delta at high prices: 21000=5, 21000.25=5, 21000.5=10, 21000.75=80
        // Cumulative: 5, 10, 20, 100. 15%=15 (hit at 21000.5, cum=20), 85%=85 (hit at 21000.75, cum=100)
        let mut p = DeltaPipeline::new(0.25);
        p.add_trade(21000.0, 5.0, true);
        p.add_trade(21000.25, 5.0, false);
        p.add_trade(21000.5, 10.0, true);
        p.add_trade(21000.75, 80.0, false);
        assert_eq!(p.dnva_low(), 21000.5);
        assert_eq!(p.dnva_high(), 21000.75);
        assert_eq!(p.dnp(), 21000.625);
    }

    #[test]
    fn percentile_dnva_spans_middle_70() {
        // 10 at each of 5 levels: 21000, 21000.25, 21000.5, 21000.75, 21001
        // Total=50. 15%=7.5 (hit at 21000, cum=10), 85%=42.5 (hit at 21001, cum=50)
        let mut p = DeltaPipeline::new(0.25);
        p.add_trade(21000.0, 10.0, true);
        p.add_trade(21000.25, 10.0, false);
        p.add_trade(21000.5, 10.0, true);
        p.add_trade(21000.75, 10.0, false);
        p.add_trade(21001.0, 10.0, true);
        assert_eq!(p.dnva_low(), 21000.0);
        assert_eq!(p.dnva_high(), 21001.0);
        assert_eq!(p.dnp(), 21000.5);
    }
}
