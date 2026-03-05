use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Per-price TPO detail: which 30-minute brackets printed at this level.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TpoLevelDetail {
    /// Price level.
    pub price: f64,
    /// Number of distinct 30-minute brackets that printed here.
    pub bracket_count: usize,
    /// Raw bracket indices: 0 = first 30 min (OR), 1 = 30-60 min, 2 = 60-90 min, etc.
    pub brackets: Vec<i32>,
    /// Corresponding standard TPO letters (A=bracket 0, B=bracket 1, …).
    pub letters: String,
    /// True if exactly one bracket printed here — a single print / tail candidate.
    pub is_single_print: bool,
}

/// Which session period a single print occurred in.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SinglePrintPeriod {
    /// Within the Opening Range (first 30 minutes).
    Or,
    /// Within the Initial Balance (first 60 minutes, but after OR).
    Ib,
    /// During regular session after IB.
    Regular,
}

/// A single-print price level with its session period.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinglePrint {
    pub price: f64,
    pub period: SinglePrintPeriod,
}

/// Incremental TPO profile with OR/IB tracking.
#[derive(Debug)]
pub struct TpoPipeline {
    tick_size: f64,
    tpo_letters: HashMap<i64, HashSet<i32>>,
    or_high: f64,
    or_low: f64,
    or_locked: bool,
    ib_high: f64,
    ib_low: f64,
    ib_locked: bool,
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
            or_locked: false,
            ib_high: 0.0,
            ib_low: 0.0,
            ib_locked: false,
            initialized: false,
        }
    }

    /// Reset profile and OR/IB tracking for a new session.
    pub fn reset(&mut self) {
        self.tpo_letters.clear();
        self.or_high = 0.0;
        self.or_low = 0.0;
        self.or_locked = false;
        self.ib_high = 0.0;
        self.ib_low = 0.0;
        self.ib_locked = false;
        self.initialized = false;
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    /// Add one trade and update profile incrementally.
    ///
    /// `minute_of_session` is relative to RTH open (09:30 ET = 0). Negative
    /// values indicate Globex/overnight and are ignored for OR/IB tracking.
    pub fn add_trade(&mut self, price: f64, minute_of_session: i32) {
        // Only build TPO letters for non-negative (RTH) minutes.
        // Globex trades are tracked by the levels pipeline, not TPO.
        if minute_of_session >= 0 {
            let bracket = minute_of_session / 30;
            let price_key = self.discretize(price);
            self.tpo_letters
                .entry(price_key)
                .or_default()
                .insert(bracket);
        }

        // OR/IB only track RTH trades (minute_of_session >= 0).
        if minute_of_session < 0 {
            return;
        }

        if !self.initialized {
            self.or_high = price;
            self.or_low = price;
            self.ib_high = price;
            self.ib_low = price;
            self.initialized = true;
        }

        // Lock OR permanently once we see a trade at or past minute 30.
        if minute_of_session >= 30 {
            self.or_locked = true;
        }
        if !self.or_locked {
            self.or_high = self.or_high.max(price);
            self.or_low = self.or_low.min(price);
        }

        // Lock IB permanently once we see a trade at or past minute 60.
        if minute_of_session >= 60 {
            self.ib_locked = true;
        }
        if !self.ib_locked {
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

    /// Whether the Opening Range is locked (past 30 minutes of RTH).
    pub fn or_locked(&self) -> bool {
        self.or_locked
    }

    /// Initial balance high.
    pub fn ib_high(&self) -> f64 {
        self.ib_high
    }

    /// Initial balance low.
    pub fn ib_low(&self) -> f64 {
        self.ib_low
    }

    /// Whether the Initial Balance is locked (past 60 minutes of RTH).
    pub fn ib_locked(&self) -> bool {
        self.ib_locked
    }

    /// Price levels that have exactly one TPO letter, tagged by session period.
    pub fn single_prints(&self) -> Vec<SinglePrint> {
        let or_high_key = self.discretize(self.or_high);
        let or_low_key = self.discretize(self.or_low);
        let ib_high_key = self.discretize(self.ib_high);
        let ib_low_key = self.discretize(self.ib_low);

        let mut prints: Vec<SinglePrint> = self
            .tpo_letters
            .iter()
            .filter_map(|(price, letters)| {
                if letters.len() != 1 {
                    return None;
                }
                let bracket = *letters.iter().next().unwrap();
                let period = if bracket == 0 && *price >= or_low_key && *price <= or_high_key {
                    SinglePrintPeriod::Or
                } else if bracket <= 1 && *price >= ib_low_key && *price <= ib_high_key {
                    SinglePrintPeriod::Ib
                } else {
                    SinglePrintPeriod::Regular
                };
                Some(SinglePrint {
                    price: *price as f64 * self.tick_size,
                    period,
                })
            })
            .collect();
        prints.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        prints
    }

    /// Simple list of single-print price levels (untagged, for backward compat).
    pub fn single_print_prices(&self) -> Vec<f64> {
        self.single_prints()
            .into_iter()
            .map(|sp| sp.price)
            .collect()
    }

    /// Raw TPO counts by price.
    pub fn tpo_count_by_price(&self) -> Vec<(f64, usize)> {
        let mut out: Vec<(f64, usize)> = self
            .tpo_letters
            .iter()
            .map(|(k, v)| (*k as f64 * self.tick_size, v.len()))
            .collect();
        out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Poor high: top price has multiple prints (unfinished auction).
    pub fn poor_high(&self) -> bool {
        let Some((&high_key, letters)) = self.tpo_letters.iter().max_by_key(|(k, _)| *k) else {
            return false;
        };
        let _ = high_key;
        letters.len() > 1
    }

    /// Poor low: bottom price has multiple prints (unfinished auction).
    pub fn poor_low(&self) -> bool {
        let Some((&low_key, letters)) = self.tpo_letters.iter().min_by_key(|(k, _)| *k) else {
            return false;
        };
        let _ = low_key;
        letters.len() > 1
    }

    /// Export per-price TPO letter detail, optionally filtered to a price range.
    ///
    /// Each entry shows which 30-minute brackets printed at that price, expressed
    /// as both raw bracket indices and standard TPO letters (A, B, C, …).  Bracket 0
    /// is the first 30-minute period (Opening Range), bracket 1 completes the Initial
    /// Balance, and so on.  Levels with exactly one bracket are single prints.
    pub fn tpo_letter_detail(
        &self,
        price_low: Option<f64>,
        price_high: Option<f64>,
    ) -> Vec<TpoLevelDetail> {
        let low_key = price_low.map(|p| self.discretize(p));
        let high_key = price_high.map(|p| self.discretize(p));

        let mut out: Vec<TpoLevelDetail> = self
            .tpo_letters
            .iter()
            .filter(|(key, _)| {
                if let Some(lo) = low_key {
                    if **key < lo {
                        return false;
                    }
                }
                if let Some(hi) = high_key {
                    if **key > hi {
                        return false;
                    }
                }
                true
            })
            .map(|(key, letters)| {
                let mut brackets: Vec<i32> = letters.iter().copied().collect();
                brackets.sort_unstable();
                let letter_str: String = brackets
                    .iter()
                    .map(|&b| (b'A' + (b as u8).min(25)) as char)
                    .collect();
                let is_single = brackets.len() == 1;
                TpoLevelDetail {
                    price: *key as f64 * self.tick_size,
                    bracket_count: brackets.len(),
                    brackets,
                    letters: letter_str,
                    is_single_print: is_single,
                }
            })
            .collect();

        out.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out
    }

    /// Direction of single prints relative to POC (for day type classifier).
    pub fn single_prints_direction_vs_poc(&self) -> (usize, usize) {
        let poc = self.poc();
        let prints = self.single_print_prices();
        let above = prints.iter().filter(|p| **p > poc).count();
        let below = prints.iter().filter(|p| **p < poc).count();
        (above, below)
    }

    /// Excess at top/bottom based on single-print tails.
    pub fn excess(&self) -> (bool, bool) {
        let mut top_excess = false;
        let mut bottom_excess = false;
        let mut prints = self.single_print_prices();
        prints.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if prints.len() >= 3 {
            let top_tail = prints.iter().rev().take(3).copied().collect::<Vec<_>>();
            let bottom_tail = prints.iter().take(3).copied().collect::<Vec<_>>();
            top_excess = top_tail
                .windows(2)
                .all(|w| (w[0] - w[1]).abs() <= self.tick_size);
            bottom_excess = bottom_tail
                .windows(2)
                .all(|w| (w[0] - w[1]).abs() <= self.tick_size);
        }
        (top_excess, bottom_excess)
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

    #[test]
    fn tpo_letter_detail_bracket_counts_and_letters() {
        let mut p = TpoPipeline::new(0.25);
        // minute 0 → bracket 0 → letter A
        p.add_trade(21000.0, 0);
        // minute 30 → bracket 1 → letter B
        p.add_trade(21000.0, 30);
        // minute 0 → bracket 0 → letter A  (different price — single print)
        p.add_trade(21000.25, 0);

        let detail = p.tpo_letter_detail(None, None);
        // Should have two price levels.
        assert_eq!(detail.len(), 2);

        let lvl_21000 = detail.iter().find(|d| d.price == 21000.0).expect("21000");
        assert_eq!(lvl_21000.bracket_count, 2);
        assert_eq!(lvl_21000.letters, "AB");
        assert!(!lvl_21000.is_single_print);

        let lvl_21000_25 = detail
            .iter()
            .find(|d| (d.price - 21000.25).abs() < 0.001)
            .expect("21000.25");
        assert_eq!(lvl_21000_25.bracket_count, 1);
        assert_eq!(lvl_21000_25.letters, "A");
        assert!(lvl_21000_25.is_single_print);
    }

    #[test]
    fn tpo_letter_detail_price_filter() {
        let mut p = TpoPipeline::new(0.25);
        p.add_trade(21000.0, 0);
        p.add_trade(21001.0, 0);
        p.add_trade(21002.0, 0);

        // Only levels in [21000.5, 21001.5].
        let detail = p.tpo_letter_detail(Some(21000.5), Some(21001.5));
        assert_eq!(detail.len(), 1);
        assert_eq!(detail[0].price, 21001.0);
    }
}
