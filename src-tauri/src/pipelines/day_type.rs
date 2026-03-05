use serde::{Deserialize, Serialize};

/// Dalton's day type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum DayType {
    #[default]
    Normal,
    NormalVariation,
    Neutral,
    Trend,
    DoubleDistribution,
}

/// TPO profile shape classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum ProfileShape {
    /// Balanced bell curve.
    #[default]
    Gaussian,
    /// Fat top, thin bottom -- longs being built above.
    PShape,
    /// Fat bottom, thin top -- shorts building below.
    BShape,
    /// Two distinct distribution areas.
    DShape,
}

/// Whether the market is balanced or imbalanced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum BalanceState {
    #[default]
    Balanced,
    Imbalanced,
}

/// Direction of single prints relative to POC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum SinglePrintsDirection {
    AbovePoc,
    BelowPoc,
    Both,
    #[default]
    None,
}

/// Day Type Classifier pipeline. Reads from TPO data to classify the session.
#[derive(Debug, Default)]
pub struct DayTypeClassifier {
    day_type: DayType,
    profile_shape: ProfileShape,
    balance_state: BalanceState,
    single_prints_direction: SinglePrintsDirection,
}

impl DayTypeClassifier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Update classification from TPO profile data.
    ///
    /// Parameters:
    /// - `tpo_counts`: price -> TPO count, sorted by price ascending
    /// - `va_high`, `va_low`, `poc`: from TpoPipeline
    /// - `ib_high`, `ib_low`: Initial Balance bounds
    /// - `session_high`, `session_low`: current session range
    /// - `single_print_prices`: prices that have exactly 1 TPO letter
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        tpo_counts: &[(f64, usize)],
        va_high: f64,
        va_low: f64,
        poc: f64,
        ib_high: f64,
        ib_low: f64,
        session_high: f64,
        session_low: f64,
        single_print_prices: &[f64],
    ) {
        if tpo_counts.is_empty() || session_high <= session_low {
            return;
        }

        let range = session_high - session_low;
        let va_width = va_high - va_low;
        let ib_range = ib_high - ib_low;

        // Profile shape analysis
        let mid_price = (session_high + session_low) / 2.0;
        let total_tpos: usize = tpo_counts.iter().map(|(_, c)| *c).sum();
        let upper_tpos: usize = tpo_counts
            .iter()
            .filter(|(p, _)| *p >= mid_price)
            .map(|(_, c)| *c)
            .sum();
        let _lower_tpos: usize = total_tpos.saturating_sub(upper_tpos);

        let skew = if total_tpos > 0 {
            upper_tpos as f64 / total_tpos as f64
        } else {
            0.5
        };

        // Detect D-shape: look for two local maxima in the TPO distribution
        let has_two_peaks = self.detect_double_distribution(tpo_counts);

        self.profile_shape = if has_two_peaks {
            ProfileShape::DShape
        } else if skew > 0.6 {
            ProfileShape::PShape
        } else if skew < 0.4 {
            ProfileShape::BShape
        } else {
            ProfileShape::Gaussian
        };

        // Day type classification
        let va_pct_of_range = if range > 0.0 { va_width / range } else { 0.0 };
        let extended_beyond_ib = session_high > ib_high || session_low < ib_low;
        let has_single_prints = !single_print_prices.is_empty();
        let price_at_extreme = {
            let dist_to_high = (session_high - poc).abs();
            let dist_to_low = (poc - session_low).abs();
            let shorter = dist_to_high.min(dist_to_low);
            shorter / range.max(0.01) < 0.2
        };

        self.day_type = if has_two_peaks {
            DayType::DoubleDistribution
        } else if has_single_prints && price_at_extreme && va_pct_of_range < 0.6 {
            DayType::Trend
        } else if va_pct_of_range > 0.85 {
            DayType::Normal
        } else if extended_beyond_ib && va_pct_of_range > 0.6 {
            DayType::NormalVariation
        } else {
            DayType::Neutral
        };

        // Balance state
        self.balance_state = if self.profile_shape == ProfileShape::Gaussian
            && !has_single_prints
            && va_width >= ib_range * 0.8
        {
            BalanceState::Balanced
        } else {
            BalanceState::Imbalanced
        };

        // Single prints direction relative to POC
        let above_poc = single_print_prices.iter().filter(|p| **p > poc).count();
        let below_poc = single_print_prices.iter().filter(|p| **p < poc).count();
        self.single_prints_direction = if above_poc > 0 && below_poc > 0 {
            SinglePrintsDirection::Both
        } else if above_poc > 0 {
            SinglePrintsDirection::AbovePoc
        } else if below_poc > 0 {
            SinglePrintsDirection::BelowPoc
        } else {
            SinglePrintsDirection::None
        };
    }

    fn detect_double_distribution(&self, tpo_counts: &[(f64, usize)]) -> bool {
        if tpo_counts.len() < 6 {
            return false;
        }
        let max_tpo = tpo_counts.iter().map(|(_, c)| *c).max().unwrap_or(0);
        if max_tpo == 0 {
            return false;
        }
        let threshold = max_tpo / 2;

        // Find valleys: consecutive low-TPO regions between high-TPO peaks
        let mut in_peak = false;
        let mut peaks = 0;
        let mut valley_seen = false;
        for (_, count) in tpo_counts {
            if *count >= threshold {
                if !in_peak && valley_seen {
                    peaks += 1;
                }
                in_peak = true;
            } else {
                if in_peak {
                    if peaks == 0 {
                        peaks = 1;
                    }
                    valley_seen = true;
                }
                in_peak = false;
            }
        }
        if in_peak && valley_seen {
            peaks += 1;
        }
        peaks >= 2
    }

    pub fn day_type(&self) -> DayType {
        self.day_type
    }

    pub fn profile_shape(&self) -> ProfileShape {
        self.profile_shape
    }

    pub fn balance_state(&self) -> BalanceState {
        self.balance_state
    }

    pub fn single_prints_direction(&self) -> SinglePrintsDirection {
        self.single_prints_direction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_normal_day() {
        let mut c = DayTypeClassifier::new();
        let tpo: Vec<(f64, usize)> = (0..20)
            .map(|i| {
                let price = 21000.0 + i as f64 * 0.25;
                let count = if (3..=16).contains(&i) { 5 } else { 1 };
                (price, count)
            })
            .collect();
        c.update(
            &tpo,
            21001.0,
            21003.75,
            21002.5,
            21001.0,
            21003.5,
            21000.0,
            21004.75,
            &[],
        );
        assert_eq!(c.day_type(), DayType::Normal);
        assert_eq!(c.balance_state(), BalanceState::Balanced);
    }

    #[test]
    fn detects_single_prints_direction() {
        let mut c = DayTypeClassifier::new();
        let tpo: Vec<(f64, usize)> = (0..10).map(|i| (21000.0 + i as f64 * 0.25, 3)).collect();
        let singles = vec![21002.0, 21002.25];
        c.update(
            &tpo, 21001.75, 21000.5, 21001.0, 21001.5, 21000.5, 21002.25, 21000.0, &singles,
        );
        assert_eq!(c.single_prints_direction(), SinglePrintsDirection::AbovePoc);
    }
}
