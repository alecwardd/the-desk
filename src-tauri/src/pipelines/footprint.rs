use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub struct FootprintLevel {
    pub bid_volume: f64,
    pub ask_volume: f64,
}

impl FootprintLevel {
    pub fn total(&self) -> f64 {
        self.bid_volume + self.ask_volume
    }
    pub fn delta(&self) -> f64 {
        self.ask_volume - self.bid_volume
    }
    pub fn imbalance_ratio(&self) -> f64 {
        let small = self.bid_volume.min(self.ask_volume).max(1.0);
        let large = self.bid_volume.max(self.ask_volume);
        large / small
    }
    /// Delta normalized by total volume. Ranges from -1.0 (all sells) to 1.0 (all buys).
    pub fn delta_per_volume(&self) -> f64 {
        let total = self.total();
        if total == 0.0 {
            0.0
        } else {
            self.delta() / total
        }
    }
}

/// Individual trade record for time-windowed queries.
#[derive(Debug, Clone)]
struct TimedTrade {
    timestamp_ms: f64,
    price_key: i64,
    volume: f64,
    is_buy: bool,
}

#[derive(Debug, Default)]
pub struct FootprintPipeline {
    tick_size: f64,
    levels: HashMap<i64, FootprintLevel>,
    trades: Vec<TimedTrade>,
}

impl FootprintPipeline {
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            levels: HashMap::new(),
            trades: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.levels.clear();
        self.trades.clear();
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    pub fn on_trade(&mut self, price: f64, volume: f64, is_buy: bool, timestamp_ms: f64) {
        let key = self.discretize(price);
        let level = self.levels.entry(key).or_default();
        if is_buy {
            level.ask_volume += volume;
        } else {
            level.bid_volume += volume;
        }
        self.trades.push(TimedTrade {
            timestamp_ms,
            price_key: key,
            volume,
            is_buy,
        });
    }

    pub fn level(&self, price: f64) -> Option<FootprintLevel> {
        self.levels.get(&self.discretize(price)).copied()
    }

    pub fn levels(&self) -> Vec<(f64, FootprintLevel)> {
        let mut out: Vec<(f64, FootprintLevel)> = self
            .levels
            .iter()
            .map(|(k, v)| (*k as f64 * self.tick_size, *v))
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Footprint for a time window. Returns levels with only the volume from that window.
    pub fn levels_in_window(&self, start_ms: f64, end_ms: f64) -> Vec<(f64, FootprintLevel)> {
        let mut windowed: HashMap<i64, FootprintLevel> = HashMap::new();
        for t in &self.trades {
            if t.timestamp_ms >= start_ms && t.timestamp_ms <= end_ms {
                let level = windowed.entry(t.price_key).or_default();
                if t.is_buy {
                    level.ask_volume += t.volume;
                } else {
                    level.bid_volume += t.volume;
                }
            }
        }
        let mut out: Vec<(f64, FootprintLevel)> = windowed
            .into_iter()
            .map(|(k, v)| (k as f64 * self.tick_size, v))
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Stacked imbalances: N consecutive price levels with the same side showing `min_ratio` or
    /// greater imbalance. Returns the prices where the run reached `consecutive` levels.
    pub fn stacked_imbalances(&self, min_ratio: f64, consecutive: usize) -> Vec<f64> {
        let mut out = Vec::new();
        let sorted = self.levels();
        let mut run = 0_usize;
        for (price, lvl) in &sorted {
            if lvl.imbalance_ratio() >= min_ratio && lvl.total() > 0.0 {
                run += 1;
                if run >= consecutive {
                    out.push(*price);
                }
            } else {
                run = 0;
            }
        }
        out
    }

    /// Diagonal imbalances: bid volume at price N compared to ask volume at price N+1.
    /// If bid_vol[N] / ask_vol[N+1] >= `min_ratio` (or vice versa), it's a diagonal imbalance.
    /// Returns (price_low, price_high, ratio, is_buy_imbalance).
    pub fn diagonal_imbalances(&self, min_ratio: f64) -> Vec<(f64, f64, f64, bool)> {
        let sorted = self.levels();
        let mut out = Vec::new();
        for pair in sorted.windows(2) {
            let (price_low, lvl_low) = &pair[0];
            let (price_high, lvl_high) = &pair[1];

            // Buy diagonal: ask volume at lower price vs bid volume at upper price
            if lvl_low.ask_volume > 0.0 && lvl_high.bid_volume > 0.0 {
                let ratio = lvl_low.ask_volume / lvl_high.bid_volume.max(1.0);
                if ratio >= min_ratio {
                    out.push((*price_low, *price_high, ratio, true));
                }
            }
            // Sell diagonal: bid volume at upper price vs ask volume at lower price
            if lvl_high.bid_volume > 0.0 && lvl_low.ask_volume > 0.0 {
                let ratio = lvl_high.bid_volume / lvl_low.ask_volume.max(1.0);
                if ratio >= min_ratio {
                    out.push((*price_low, *price_high, ratio, false));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::FootprintPipeline;

    #[test]
    fn tracks_bid_and_ask_volume() {
        let mut p = FootprintPipeline::new(0.25);
        p.on_trade(21000.0, 10.0, true, 1000.0);
        p.on_trade(21000.0, 5.0, false, 1001.0);
        let lvl = p.level(21000.0).expect("level");
        assert_eq!(lvl.ask_volume, 10.0);
        assert_eq!(lvl.bid_volume, 5.0);
    }

    #[test]
    fn delta_per_volume_ratio() {
        let mut p = FootprintPipeline::new(0.25);
        p.on_trade(21000.0, 8.0, true, 1000.0);
        p.on_trade(21000.0, 2.0, false, 1001.0);
        let lvl = p.level(21000.0).unwrap();
        assert!((lvl.delta_per_volume() - 0.6).abs() < 0.001);
    }

    #[test]
    fn time_windowed_footprint() {
        let mut p = FootprintPipeline::new(0.25);
        p.on_trade(21000.0, 10.0, true, 1000.0);
        p.on_trade(21000.0, 5.0, true, 5000.0);
        let window = p.levels_in_window(4000.0, 6000.0);
        assert_eq!(window.len(), 1);
        assert_eq!(window[0].1.ask_volume, 5.0);
    }

    #[test]
    fn diagonal_imbalances_detected() {
        let mut p = FootprintPipeline::new(0.25);
        p.on_trade(21000.0, 50.0, true, 1000.0);
        p.on_trade(21000.25, 5.0, false, 1001.0);
        let diags = p.diagonal_imbalances(3.0);
        assert!(!diags.is_empty());
    }
}
