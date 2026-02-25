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

    /// Delta-neutral pivot where cumulative profile crosses zero.
    pub fn dnp(&self) -> f64 {
        if self.delta_by_price.is_empty() {
            return 0.0;
        }
        let mut keys: Vec<i64> = self.delta_by_price.keys().copied().collect();
        keys.sort();
        let mut running = 0.0;
        let mut closest_key = keys[0];
        let mut closest_abs = f64::MAX;
        for key in keys {
            running += self.delta_by_price.get(&key).copied().unwrap_or(0.0);
            let distance = running.abs();
            if distance < closest_abs {
                closest_abs = distance;
                closest_key = key;
            }
            if running == 0.0 {
                return key as f64 * self.tick_size;
            }
        }
        closest_key as f64 * self.tick_size
    }

    fn dnva_bounds(&self) -> (f64, f64) {
        if self.delta_by_price.is_empty() {
            return (0.0, 0.0);
        }
        let total_abs: f64 = self.delta_by_price.values().map(|v| v.abs()).sum::<f64>();
        let target = total_abs * 0.7;
        let mut prices: Vec<i64> = self.delta_by_price.keys().copied().collect();
        prices.sort();
        let mut center = prices[0];
        let mut best_abs = 0.0;
        for (price, delta) in &self.delta_by_price {
            let abs_delta = delta.abs();
            if abs_delta > best_abs {
                best_abs = abs_delta;
                center = *price;
            }
        }
        let mut low = center;
        let mut high = center;
        let mut covered = self
            .delta_by_price
            .get(&center)
            .copied()
            .unwrap_or(0.0)
            .abs();
        while covered < target {
            let below = self
                .delta_by_price
                .get(&(low - 1))
                .copied()
                .unwrap_or(0.0)
                .abs();
            let above = self
                .delta_by_price
                .get(&(high + 1))
                .copied()
                .unwrap_or(0.0)
                .abs();
            if above >= below {
                high += 1;
                covered += above;
            } else {
                low -= 1;
                covered += below;
            }
            if below == 0.0 && above == 0.0 {
                break;
            }
        }
        let low = low as f64 * self.tick_size;
        let high = high as f64 * self.tick_size;
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
}
