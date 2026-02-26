use serde::{Deserialize, Serialize};

/// Classification of a key reference level on the price ladder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KeyLevelType {
    /// Previous RTH session high.
    PriorDayHigh,
    /// Previous RTH session low.
    PriorDayLow,
    /// Previous RTH session closing price.
    PriorDayClose,
    /// Previous session value area high.
    PriorVaHigh,
    /// Previous session value area low.
    PriorVaLow,
    /// Previous session point of control.
    PriorPoc,
    /// Overnight (Globex) session high.
    OvernightHigh,
    /// Overnight (Globex) session low.
    OvernightLow,
    /// Current RTH session high.
    SessionHigh,
    /// Current RTH session low.
    SessionLow,
}

/// A single key reference level with its type and price.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyLevel {
    /// What kind of reference level this represents.
    pub level_type: KeyLevelType,
    /// Price value of this level.
    pub price: f64,
}

/// Incremental key levels tracker with prior-day, overnight, and session ranges.
#[derive(Debug)]
pub struct LevelsPipeline {
    /// Previous RTH session high.
    pub prior_day_high: f64,
    /// Previous RTH session low.
    pub prior_day_low: f64,
    /// Previous RTH session closing price.
    pub prior_day_close: f64,
    /// Previous session value area high.
    pub prior_va_high: f64,
    /// Previous session value area low.
    pub prior_va_low: f64,
    /// Previous session point of control.
    pub prior_poc: f64,
    /// Overnight (Globex) session high.
    pub overnight_high: f64,
    /// Overnight (Globex) session low.
    pub overnight_low: f64,
    /// Current RTH session high.
    pub session_high: f64,
    /// Current RTH session low.
    pub session_low: f64,
    /// Most recent trade price seen by this pipeline.
    pub last_price: f64,
    initialized: bool,
    rth_started: bool,
}

impl Default for LevelsPipeline {
    fn default() -> Self {
        Self {
            prior_day_high: 0.0,
            prior_day_low: 0.0,
            prior_day_close: 0.0,
            prior_va_high: 0.0,
            prior_va_low: 0.0,
            prior_poc: 0.0,
            overnight_high: 0.0,
            overnight_low: 0.0,
            session_high: 0.0,
            session_low: 0.0,
            last_price: 0.0,
            initialized: false,
            rth_started: false,
        }
    }
}

impl LevelsPipeline {
    /// Reset session tracking while preserving prior-day levels.
    /// Current session high/low/close become the new prior-day reference.
    pub fn reset_session(&mut self) {
        if self.rth_started {
            self.prior_day_high = self.session_high;
            self.prior_day_low = self.session_low;
            self.prior_day_close = self.last_price;
        }
        self.overnight_high = 0.0;
        self.overnight_low = 0.0;
        self.session_high = 0.0;
        self.session_low = 0.0;
        self.initialized = false;
        self.rth_started = false;
    }

    /// Set prior day reference levels from historical data or config.
    pub fn set_prior_day(&mut self, high: f64, low: f64, close: f64) {
        self.prior_day_high = high;
        self.prior_day_low = low;
        self.prior_day_close = close;
    }

    /// Set prior session VA/POC from stored data.
    pub fn set_prior_profile(&mut self, va_high: f64, va_low: f64, poc: f64) {
        self.prior_va_high = va_high;
        self.prior_va_low = va_low;
        self.prior_poc = poc;
    }

    /// Apply one trade update and maintain key levels.
    pub fn on_trade(&mut self, price: f64, minute_of_session: i32) {
        self.last_price = price;

        if !self.initialized {
            self.overnight_high = price;
            self.overnight_low = price;
            self.initialized = true;
        }

        if minute_of_session < 0 {
            self.overnight_high = self.overnight_high.max(price);
            self.overnight_low = self.overnight_low.min(price);
        } else {
            if !self.rth_started {
                if self.prior_day_high <= 0.0 {
                    // Bootstrap prior-day references when no historical loader is available yet.
                    self.prior_day_high = self.overnight_high;
                    self.prior_day_low = self.overnight_low;
                    self.prior_day_close = self.last_price;
                }
                self.session_high = price;
                self.session_low = price;
                self.rth_started = true;
            }
            self.session_high = self.session_high.max(price);
            self.session_low = self.session_low.min(price);
        }
    }

    /// All active key levels for display.
    pub fn key_levels(&self) -> Vec<KeyLevel> {
        let mut levels = Vec::new();

        if self.prior_day_high > 0.0 {
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorDayHigh,
                price: self.prior_day_high,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorDayLow,
                price: self.prior_day_low,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorDayClose,
                price: self.prior_day_close,
            });
        }

        if self.prior_va_high > 0.0 {
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorVaHigh,
                price: self.prior_va_high,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorVaLow,
                price: self.prior_va_low,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::PriorPoc,
                price: self.prior_poc,
            });
        }

        if self.initialized {
            levels.push(KeyLevel {
                level_type: KeyLevelType::OvernightHigh,
                price: self.overnight_high,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::OvernightLow,
                price: self.overnight_low,
            });
        }

        if self.rth_started {
            levels.push(KeyLevel {
                level_type: KeyLevelType::SessionHigh,
                price: self.session_high,
            });
            levels.push(KeyLevel {
                level_type: KeyLevelType::SessionLow,
                price: self.session_low,
            });
        }

        levels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_overnight_range() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.on_trade(21000.0, -30);
        pipeline.on_trade(21050.0, -20);
        pipeline.on_trade(20980.0, -10);
        assert_eq!(pipeline.overnight_high, 21050.0);
        assert_eq!(pipeline.overnight_low, 20980.0);
    }

    #[test]
    fn tracks_session_range_separately() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.on_trade(21000.0, -10);
        pipeline.on_trade(21050.0, -5);
        pipeline.on_trade(21020.0, 0);
        pipeline.on_trade(21040.0, 5);
        pipeline.on_trade(21010.0, 10);
        assert_eq!(pipeline.overnight_high, 21050.0);
        assert_eq!(pipeline.overnight_low, 21000.0);
        assert_eq!(pipeline.session_high, 21040.0);
        assert_eq!(pipeline.session_low, 21010.0);
    }

    #[test]
    fn prior_day_levels_appear_in_key_levels() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.set_prior_day(21100.0, 20900.0, 21050.0);
        pipeline.on_trade(21000.0, 0);
        let levels = pipeline.key_levels();
        assert!(levels
            .iter()
            .any(|l| matches!(l.level_type, KeyLevelType::PriorDayHigh)));
        assert!(levels
            .iter()
            .any(|l| matches!(l.level_type, KeyLevelType::SessionHigh)));
    }

    #[test]
    fn reset_session_rolls_levels() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.on_trade(21000.0, -10);
        pipeline.on_trade(21020.0, 0);
        pipeline.on_trade(21050.0, 5);
        pipeline.on_trade(21010.0, 10);
        assert_eq!(pipeline.session_high, 21050.0);
        assert_eq!(pipeline.session_low, 21010.0);

        pipeline.reset_session();
        assert_eq!(pipeline.prior_day_high, 21050.0);
        assert_eq!(pipeline.prior_day_low, 21010.0);
        assert_eq!(pipeline.prior_day_close, 21010.0);
        assert_eq!(pipeline.session_high, 0.0);
        assert!(!pipeline.rth_started);
    }
}
