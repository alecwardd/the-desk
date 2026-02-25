use std::collections::{HashMap, HashSet};

/// Incremental TPO profile with OR/IB tracking.
#[derive(Debug)]
pub struct TpoPipeline {
    tick_size: f64,
    tpo_letters: HashMap<i64, HashSet<i32>>,
    or_high: f64,
    or_low: f64,
    ib_high: f64,
    ib_low: f64,
    initialized: bool,
}

impl TpoPipeline {
    /// Build TPO pipeline for an instrument tick size.
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            tpo_letters: HashMap::new(),
            or_high: 0.0,
            or_low: 0.0,
            ib_high: 0.0,
            ib_low: 0.0,
            initialized: false,
        }
    }

    /// Reset profile and OR/IB tracking for a new session.
    pub fn reset(&mut self) {
        self.tpo_letters.clear();
        self.or_high = 0.0;
        self.or_low = 0.0;
        self.ib_high = 0.0;
        self.ib_low = 0.0;
        self.initialized = false;
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    /// Add one trade and update profile incrementally.
    pub fn add_trade(&mut self, price: f64, minute_of_session: i32) {
        let bracket = minute_of_session / 30;
        let price_key = self.discretize(price);
        self.tpo_letters
            .entry(price_key)
            .or_default()
            .insert(bracket);

        if !self.initialized {
            self.or_high = price;
            self.or_low = price;
            self.ib_high = price;
            self.ib_low = price;
            self.initialized = true;
        }
        if minute_of_session < 30 {
            self.or_high = self.or_high.max(price);
            self.or_low = self.or_low.min(price);
        }
        if minute_of_session < 60 {
            self.ib_high = self.ib_high.max(price);
            self.ib_low = self.ib_low.min(price);
        }
    }

    /// Point of control by highest TPO count.
    pub fn poc(&self) -> f64 {
        let mut best_price = 0;
        let mut best_count = 0usize;
        for (price, letters) in &self.tpo_letters {
            if letters.len() > best_count {
                best_count = letters.len();
                best_price = *price;
            }
        }
        best_price as f64 * self.tick_size
    }

    fn value_area_bounds(&self) -> (f64, f64) {
        if self.tpo_letters.is_empty() {
            return (0.0, 0.0);
        }
        let counts: HashMap<i64, usize> = self
            .tpo_letters
            .iter()
            .map(|(price, letters)| (*price, letters.len()))
            .collect();
        let total: usize = counts.values().sum();
        let target = (total as f64 * 0.7).ceil() as usize;
        let mut low = (self.poc() / self.tick_size).round() as i64;
        let mut high = low;
        let mut included = counts.get(&low).copied().unwrap_or_default();

        while included < target {
            let below = counts.get(&(low - 1)).copied().unwrap_or(0);
            let above = counts.get(&(high + 1)).copied().unwrap_or(0);
            if above >= below {
                high += 1;
                included += above;
            } else {
                low -= 1;
                included += below;
            }
            if below == 0 && above == 0 {
                break;
            }
        }
        (high as f64 * self.tick_size, low as f64 * self.tick_size)
    }

    /// Value area high.
    pub fn va_high(&self) -> f64 {
        self.value_area_bounds().0
    }

    /// Value area low.
    pub fn va_low(&self) -> f64 {
        self.value_area_bounds().1
    }

    /// Opening range high.
    pub fn or_high(&self) -> f64 {
        self.or_high
    }

    /// Opening range low.
    pub fn or_low(&self) -> f64 {
        self.or_low
    }

    /// Initial balance high.
    pub fn ib_high(&self) -> f64 {
        self.ib_high
    }

    /// Initial balance low.
    pub fn ib_low(&self) -> f64 {
        self.ib_low
    }

    /// Price levels that have exactly one TPO letter.
    pub fn single_prints(&self) -> Vec<f64> {
        let mut levels: Vec<f64> = self
            .tpo_letters
            .iter()
            .filter_map(|(price, letters)| {
                if letters.len() == 1 {
                    Some(*price as f64 * self.tick_size)
                } else {
                    None
                }
            })
            .collect();
        levels.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        levels
    }
}

#[cfg(test)]
mod tests {
    use super::TpoPipeline;

    #[test]
    fn tracks_or_and_ib() {
        let mut pipeline = TpoPipeline::new(0.25);
        pipeline.add_trade(21000.0, 0);
        pipeline.add_trade(21005.0, 20);
        pipeline.add_trade(20995.0, 45);
        assert_eq!(pipeline.or_high(), 21005.0);
        assert_eq!(pipeline.or_low(), 21000.0);
        assert_eq!(pipeline.ib_low(), 20995.0);
    }
}
