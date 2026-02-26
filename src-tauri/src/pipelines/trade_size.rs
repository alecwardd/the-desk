use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub struct TradeSizeSnapshot {
    pub lot_1: u64,
    pub lot_2_5: u64,
    pub lot_6_20: u64,
    pub lot_21_plus: u64,
    pub avg_trade_size: f64,
}

#[derive(Debug, Default)]
pub struct TradeSizePipeline {
    lot_1: u64,
    lot_2_5: u64,
    lot_6_20: u64,
    lot_21_plus: u64,
    total_size: f64,
    total_trades: u64,
    tick_size: f64,
    size_at_price: HashMap<i64, TradeSizeSnapshot>,
}

impl TradeSizePipeline {
    pub fn new() -> Self {
        Self {
            tick_size: 0.25,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.lot_1 = 0;
        self.lot_2_5 = 0;
        self.lot_6_20 = 0;
        self.lot_21_plus = 0;
        self.total_size = 0.0;
        self.total_trades = 0;
        self.size_at_price.clear();
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    pub fn on_trade(&mut self, volume: f64, price: f64) {
        self.total_trades = self.total_trades.saturating_add(1);
        self.total_size += volume.max(0.0);

        let bucket = Self::classify(volume);
        match bucket {
            SizeBucket::Lot1 => self.lot_1 = self.lot_1.saturating_add(1),
            SizeBucket::Lot2_5 => self.lot_2_5 = self.lot_2_5.saturating_add(1),
            SizeBucket::Lot6_20 => self.lot_6_20 = self.lot_6_20.saturating_add(1),
            SizeBucket::Lot21Plus => self.lot_21_plus = self.lot_21_plus.saturating_add(1),
        }

        let key = self.discretize(price);
        let at_price = self.size_at_price.entry(key).or_default();
        at_price.avg_trade_size = {
            let prev_count =
                at_price.lot_1 + at_price.lot_2_5 + at_price.lot_6_20 + at_price.lot_21_plus;
            let prev_total = at_price.avg_trade_size * prev_count as f64;
            (prev_total + volume) / (prev_count + 1) as f64
        };
        match bucket {
            SizeBucket::Lot1 => at_price.lot_1 = at_price.lot_1.saturating_add(1),
            SizeBucket::Lot2_5 => at_price.lot_2_5 = at_price.lot_2_5.saturating_add(1),
            SizeBucket::Lot6_20 => at_price.lot_6_20 = at_price.lot_6_20.saturating_add(1),
            SizeBucket::Lot21Plus => at_price.lot_21_plus = at_price.lot_21_plus.saturating_add(1),
        }
    }

    fn classify(volume: f64) -> SizeBucket {
        if volume <= 1.0 {
            SizeBucket::Lot1
        } else if volume <= 5.0 {
            SizeBucket::Lot2_5
        } else if volume <= 20.0 {
            SizeBucket::Lot6_20
        } else {
            SizeBucket::Lot21Plus
        }
    }

    pub fn snapshot(&self) -> TradeSizeSnapshot {
        TradeSizeSnapshot {
            lot_1: self.lot_1,
            lot_2_5: self.lot_2_5,
            lot_6_20: self.lot_6_20,
            lot_21_plus: self.lot_21_plus,
            avg_trade_size: if self.total_trades == 0 {
                0.0
            } else {
                self.total_size / self.total_trades as f64
            },
        }
    }

    /// Trade size distribution at a specific price level.
    pub fn snapshot_at_price(&self, price: f64) -> TradeSizeSnapshot {
        let key = self.discretize(price);
        self.size_at_price.get(&key).copied().unwrap_or_default()
    }

    /// Prices where large (21+) trades have occurred, sorted by count descending.
    pub fn large_trade_prices(&self) -> Vec<(f64, u64)> {
        let mut out: Vec<(f64, u64)> = self
            .size_at_price
            .iter()
            .filter(|(_, s)| s.lot_21_plus > 0)
            .map(|(k, s)| (*k as f64 * self.tick_size, s.lot_21_plus))
            .collect();
        out.sort_by(|a, b| b.1.cmp(&a.1));
        out
    }
}

enum SizeBucket {
    Lot1,
    Lot2_5,
    Lot6_20,
    Lot21Plus,
}

#[cfg(test)]
mod tests {
    use super::TradeSizePipeline;

    #[test]
    fn buckets_trade_sizes() {
        let mut p = TradeSizePipeline::new();
        p.on_trade(1.0, 21000.0);
        p.on_trade(3.0, 21000.0);
        p.on_trade(10.0, 21000.25);
        p.on_trade(25.0, 21000.50);
        let s = p.snapshot();
        assert_eq!(s.lot_1, 1);
        assert_eq!(s.lot_2_5, 1);
        assert_eq!(s.lot_6_20, 1);
        assert_eq!(s.lot_21_plus, 1);
    }

    #[test]
    fn tracks_size_at_price() {
        let mut p = TradeSizePipeline::new();
        p.on_trade(25.0, 21000.0);
        p.on_trade(30.0, 21000.0);
        p.on_trade(1.0, 21000.25);
        let at_21000 = p.snapshot_at_price(21000.0);
        assert_eq!(at_21000.lot_21_plus, 2);
        let at_21000_25 = p.snapshot_at_price(21000.25);
        assert_eq!(at_21000_25.lot_1, 1);
    }

    #[test]
    fn large_trade_prices_sorted() {
        let mut p = TradeSizePipeline::new();
        p.on_trade(25.0, 21000.0);
        p.on_trade(30.0, 21000.0);
        p.on_trade(25.0, 21000.25);
        let large = p.large_trade_prices();
        assert_eq!(large.len(), 2);
        assert_eq!(large[0].1, 2); // 21000.0 has 2 large trades
    }
}
