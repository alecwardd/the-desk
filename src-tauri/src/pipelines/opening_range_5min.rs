use serde::{Deserialize, Serialize};

/// Direction of the 5-min Opening Range breakout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum Or5BreakDirection {
    #[default]
    None,
    Up,
    Down,
}

/// Leo's 5-minute micro Opening Range pipeline.
///
/// The first 5 minutes of RTH establish a micro OR whose midpoint is the key level.
/// After the range locks, tracks breakout direction, midpoint retests, and extension targets.
#[derive(Debug, Default)]
pub struct OpeningRange5MinPipeline {
    or5_high: f64,
    or5_low: f64,
    locked: bool,
    break_direction: Or5BreakDirection,
    mid_retested: bool,
    first_price_after_lock: Option<f64>,
}

impl OpeningRange5MinPipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Feed a trade. `minute_of_session` is 0-based from RTH open (09:30 ET = minute 0).
    pub fn on_trade(&mut self, price: f64, minute_of_session: i32) {
        if minute_of_session < 0 {
            return;
        }

        if minute_of_session < 5 {
            if self.or5_high == 0.0 && self.or5_low == 0.0 {
                self.or5_high = price;
                self.or5_low = price;
            } else {
                self.or5_high = self.or5_high.max(price);
                self.or5_low = self.or5_low.min(price);
            }
            return;
        }

        if !self.locked {
            self.locked = true;
        }

        if self.first_price_after_lock.is_none() {
            self.first_price_after_lock = Some(price);
        }

        if self.break_direction == Or5BreakDirection::None {
            if price > self.or5_high {
                self.break_direction = Or5BreakDirection::Up;
            } else if price < self.or5_low {
                self.break_direction = Or5BreakDirection::Down;
            }
        }

        if self.break_direction != Or5BreakDirection::None && !self.mid_retested {
            let mid = self.or5_mid();
            if (price - mid).abs() <= 0.50 {
                self.mid_retested = true;
            }
        }
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn or5_high(&self) -> f64 {
        self.or5_high
    }

    pub fn or5_low(&self) -> f64 {
        self.or5_low
    }

    pub fn or5_mid(&self) -> f64 {
        if self.or5_high == 0.0 {
            return 0.0;
        }
        (self.or5_high + self.or5_low) / 2.0
    }

    pub fn or5_range(&self) -> f64 {
        self.or5_high - self.or5_low
    }

    pub fn break_direction(&self) -> Or5BreakDirection {
        self.break_direction
    }

    pub fn mid_retested(&self) -> bool {
        self.mid_retested
    }

    /// Extension targets from the midpoint: 75% and 100% of OR5 range.
    pub fn extension_targets(&self) -> (f64, f64, f64, f64) {
        let mid = self.or5_mid();
        let half_range = self.or5_range() / 2.0;
        let ext_75 = half_range * 1.5;
        let ext_100 = half_range * 2.0;
        (
            mid + ext_75,  // upside 75% extension
            mid + ext_100, // upside 100% extension
            mid - ext_75,  // downside 75% extension
            mid - ext_100, // downside 100% extension
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locks_after_5_minutes() {
        let mut p = OpeningRange5MinPipeline::new();
        p.on_trade(21000.0, 0);
        p.on_trade(21010.0, 2);
        p.on_trade(20995.0, 4);
        assert!(!p.is_locked());
        p.on_trade(21015.0, 5);
        assert!(p.is_locked());
        assert_eq!(p.or5_high(), 21010.0);
        assert_eq!(p.or5_low(), 20995.0);
    }

    #[test]
    fn detects_upward_break() {
        let mut p = OpeningRange5MinPipeline::new();
        p.on_trade(21000.0, 0);
        p.on_trade(21010.0, 3);
        p.on_trade(20995.0, 4);
        p.on_trade(21015.0, 6);
        assert_eq!(p.break_direction(), Or5BreakDirection::Up);
    }

    #[test]
    fn detects_mid_retest() {
        let mut p = OpeningRange5MinPipeline::new();
        p.on_trade(21000.0, 0);
        p.on_trade(21010.0, 3);
        p.on_trade(20990.0, 4);
        p.on_trade(21015.0, 6); // break up
        assert!(!p.mid_retested());
        p.on_trade(21000.0, 8); // retest mid
        assert!(p.mid_retested());
    }

    #[test]
    fn ignores_pre_rth() {
        let mut p = OpeningRange5MinPipeline::new();
        p.on_trade(20500.0, -10);
        assert_eq!(p.or5_high(), 0.0);
    }
}
