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
    /// 5-minute Opening Range midpoint.
    Or5Mid,
    /// IB 0.5x extension above.
    IbExt05xHigh,
    /// IB 0.5x extension below.
    IbExt05xLow,
    /// IB 1.0x extension above.
    IbExt10xHigh,
    /// IB 1.0x extension below.
    IbExt10xLow,
    /// IB 1.5x extension above.
    IbExt15xHigh,
    /// IB 1.5x extension below.
    IbExt15xLow,
}

/// A key level with its distance from current price (in ticks).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProximityLevel {
    pub level_type: KeyLevelType,
    pub price: f64,
    pub distance_ticks: f64,
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
    /// Prior RTH session DNVA high (from prior_day_levels).
    pub prior_dnva_high: f64,
    /// Prior RTH session DNVA low (from prior_day_levels).
    pub prior_dnva_low: f64,
    /// Prior RTH session DNP (from prior_day_levels).
    pub prior_dnp: f64,
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
    /// Last trade price observed during RTH only.
    pub rth_close_price: f64,
    /// Globex OR30 high (18:00-18:30 ET).
    pub globex_or30_high: f64,
    /// Globex OR30 low (18:00-18:30 ET).
    pub globex_or30_low: f64,
    /// London OR60 high (02:00-03:00 ET).
    pub london_or60_high: f64,
    /// London OR60 low (02:00-03:00 ET).
    pub london_or60_low: f64,
    initialized: bool,
    rth_started: bool,
    globex_or30_initialized: bool,
    london_or60_initialized: bool,
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
            prior_dnva_high: 0.0,
            prior_dnva_low: 0.0,
            prior_dnp: 0.0,
            overnight_high: 0.0,
            overnight_low: 0.0,
            session_high: 0.0,
            session_low: 0.0,
            last_price: 0.0,
            rth_close_price: 0.0,
            globex_or30_high: 0.0,
            globex_or30_low: 0.0,
            london_or60_high: 0.0,
            london_or60_low: 0.0,
            initialized: false,
            rth_started: false,
            globex_or30_initialized: false,
            london_or60_initialized: false,
        }
    }
}

impl LevelsPipeline {
    /// Reset session tracking while preserving prior-day levels.
    /// If RTH was active (RTH→Globex transition), saves prior-day reference
    /// and clears overnight range for fresh Globex tracking.
    /// If RTH was NOT active (Globex→RTH transition), preserves the overnight
    /// range so RTH has access to the full Globex high/low.
    pub fn reset_session(&mut self) {
        if self.rth_started {
            self.prior_day_high = self.session_high;
            self.prior_day_low = self.session_low;
            self.prior_day_close = if self.rth_close_price > 0.0 {
                self.rth_close_price
            } else {
                self.last_price
            };
            self.overnight_high = 0.0;
            self.overnight_low = 0.0;
            self.globex_or30_high = 0.0;
            self.globex_or30_low = 0.0;
            self.london_or60_high = 0.0;
            self.london_or60_low = 0.0;
            self.initialized = false;
            self.globex_or30_initialized = false;
            self.london_or60_initialized = false;
        }
        self.session_high = 0.0;
        self.session_low = 0.0;
        self.rth_close_price = 0.0;
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

    /// Set prior RTH session DNVA from stored data.
    pub fn set_prior_dnva(&mut self, dnva_high: f64, dnva_low: f64, dnp: f64) {
        self.prior_dnva_high = dnva_high;
        self.prior_dnva_low = dnva_low;
        self.prior_dnp = dnp;
    }

    /// Apply one trade update and maintain key levels.
    /// `is_overnight` should be true for Globex/overnight ticks (6 PM - 9:30 AM ET),
    /// false for RTH ticks (9:30 AM - 4:15 PM ET).
    pub fn on_trade(&mut self, price: f64, is_overnight: bool, et_minutes: i32) {
        self.last_price = price;

        if !self.initialized {
            self.overnight_high = price;
            self.overnight_low = price;
            self.initialized = true;
        }

        if is_overnight {
            self.overnight_high = self.overnight_high.max(price);
            self.overnight_low = self.overnight_low.min(price);
            // Globex OR30: 18:00-18:30 ET.
            if (18 * 60..18 * 60 + 30).contains(&et_minutes) {
                if !self.globex_or30_initialized {
                    self.globex_or30_high = price;
                    self.globex_or30_low = price;
                    self.globex_or30_initialized = true;
                } else {
                    self.globex_or30_high = self.globex_or30_high.max(price);
                    self.globex_or30_low = self.globex_or30_low.min(price);
                }
            }
            // London OR60: 02:00-03:00 ET.
            if (2 * 60..3 * 60).contains(&et_minutes) {
                if !self.london_or60_initialized {
                    self.london_or60_high = price;
                    self.london_or60_low = price;
                    self.london_or60_initialized = true;
                } else {
                    self.london_or60_high = self.london_or60_high.max(price);
                    self.london_or60_low = self.london_or60_low.min(price);
                }
            }
        } else {
            if !self.rth_started {
                if self.prior_day_high <= 0.0 {
                    self.prior_day_high = self.overnight_high;
                    self.prior_day_low = self.overnight_low;
                    self.prior_day_close = self.last_price;
                }
                self.session_high = price;
                self.session_low = price;
                self.rth_close_price = price;
                self.rth_started = true;
            }
            self.session_high = self.session_high.max(price);
            self.session_low = self.session_low.min(price);
            self.rth_close_price = price;
        }
    }

    /// IB extension levels: 0.5x, 1.0x, 1.5x of IB range projected above and below.
    pub fn ib_extension_levels(&self, ib_high: f64, ib_low: f64) -> Vec<KeyLevel> {
        let ib_range = ib_high - ib_low;
        if ib_range <= 0.0 {
            return Vec::new();
        }
        vec![
            KeyLevel {
                level_type: KeyLevelType::IbExt05xHigh,
                price: ib_high + ib_range * 0.5,
            },
            KeyLevel {
                level_type: KeyLevelType::IbExt05xLow,
                price: ib_low - ib_range * 0.5,
            },
            KeyLevel {
                level_type: KeyLevelType::IbExt10xHigh,
                price: ib_high + ib_range,
            },
            KeyLevel {
                level_type: KeyLevelType::IbExt10xLow,
                price: ib_low - ib_range,
            },
            KeyLevel {
                level_type: KeyLevelType::IbExt15xHigh,
                price: ib_high + ib_range * 1.5,
            },
            KeyLevel {
                level_type: KeyLevelType::IbExt15xLow,
                price: ib_low - ib_range * 1.5,
            },
        ]
    }

    /// Which key levels is the current price near, sorted by distance ascending.
    pub fn proximity_report(
        &self,
        current_price: f64,
        max_distance_ticks: f64,
        tick_size: f64,
        extra_levels: &[KeyLevel],
    ) -> Vec<ProximityLevel> {
        let all = self
            .key_levels()
            .into_iter()
            .chain(extra_levels.iter().cloned())
            .collect::<Vec<_>>();
        let max_dist = max_distance_ticks * tick_size;
        let mut nearby: Vec<ProximityLevel> = all
            .into_iter()
            .filter(|kl| kl.price > 0.0)
            .map(|kl| {
                let dist = (current_price - kl.price).abs();
                ProximityLevel {
                    level_type: kl.level_type,
                    price: kl.price,
                    distance_ticks: dist / tick_size,
                }
            })
            .filter(|pl| pl.distance_ticks <= max_distance_ticks || max_dist <= 0.0)
            .collect();
        nearby.sort_by(|a, b| {
            a.distance_ticks
                .partial_cmp(&b.distance_ticks)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        nearby
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
        pipeline.on_trade(21000.0, true, 18 * 60);
        pipeline.on_trade(21050.0, true, 18 * 60 + 1);
        pipeline.on_trade(20980.0, true, 18 * 60 + 2);
        assert_eq!(pipeline.overnight_high, 21050.0);
        assert_eq!(pipeline.overnight_low, 20980.0);
    }

    #[test]
    fn tracks_session_range_separately() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.on_trade(21000.0, true, 18 * 60);
        pipeline.on_trade(21050.0, true, 18 * 60 + 1);
        pipeline.on_trade(21020.0, false, 9 * 60 + 30);
        pipeline.on_trade(21040.0, false, 9 * 60 + 31);
        pipeline.on_trade(21010.0, false, 9 * 60 + 32);
        assert_eq!(pipeline.overnight_high, 21050.0);
        assert_eq!(pipeline.overnight_low, 21000.0);
        assert_eq!(pipeline.session_high, 21040.0);
        assert_eq!(pipeline.session_low, 21010.0);
    }

    #[test]
    fn prior_day_levels_appear_in_key_levels() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.set_prior_day(21100.0, 20900.0, 21050.0);
        pipeline.on_trade(21000.0, false, 9 * 60 + 30);
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
        pipeline.on_trade(21000.0, true, 18 * 60);
        pipeline.on_trade(21020.0, false, 9 * 60 + 30);
        pipeline.on_trade(21050.0, false, 9 * 60 + 31);
        pipeline.on_trade(21010.0, false, 9 * 60 + 32);
        assert_eq!(pipeline.session_high, 21050.0);
        assert_eq!(pipeline.session_low, 21010.0);

        pipeline.reset_session();
        assert_eq!(pipeline.prior_day_high, 21050.0);
        assert_eq!(pipeline.prior_day_low, 21010.0);
        assert_eq!(pipeline.prior_day_close, 21010.0);
        assert_eq!(pipeline.session_high, 0.0);
        assert!(!pipeline.rth_started);
    }

    #[test]
    fn overnight_preserved_across_globex_to_rth_reset() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.set_prior_day(21100.0, 20900.0, 21050.0);

        // Globex session builds overnight range
        pipeline.on_trade(21020.0, true, 18 * 60);
        pipeline.on_trade(21080.0, true, 18 * 60 + 1);
        pipeline.on_trade(20950.0, true, 18 * 60 + 2);
        assert_eq!(pipeline.overnight_high, 21080.0);
        assert_eq!(pipeline.overnight_low, 20950.0);

        // Globex→RTH reset should preserve overnight range
        pipeline.reset_session();
        assert_eq!(pipeline.overnight_high, 21080.0);
        assert_eq!(pipeline.overnight_low, 20950.0);

        // RTH ticks should not overwrite overnight range
        pipeline.on_trade(21000.0, false, 9 * 60 + 30);
        pipeline.on_trade(21040.0, false, 9 * 60 + 31);
        assert_eq!(pipeline.overnight_high, 21080.0);
        assert_eq!(pipeline.overnight_low, 20950.0);
        assert_eq!(pipeline.session_high, 21040.0);
        assert_eq!(pipeline.session_low, 21000.0);
    }

    #[test]
    fn tracks_globex_and_london_opening_ranges() {
        let mut pipeline = LevelsPipeline::default();
        pipeline.on_trade(21010.0, true, 18 * 60);
        pipeline.on_trade(21020.0, true, 18 * 60 + 10);
        pipeline.on_trade(21005.0, true, 18 * 60 + 20);
        assert_eq!(pipeline.globex_or30_high, 21020.0);
        assert_eq!(pipeline.globex_or30_low, 21005.0);

        pipeline.on_trade(20990.0, true, 2 * 60);
        pipeline.on_trade(21000.0, true, 2 * 60 + 20);
        pipeline.on_trade(20980.0, true, 2 * 60 + 40);
        assert_eq!(pipeline.london_or60_high, 21000.0);
        assert_eq!(pipeline.london_or60_low, 20980.0);
    }
}
