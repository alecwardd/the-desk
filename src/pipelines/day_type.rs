use serde::{Deserialize, Serialize};

/// Dalton's day type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum DayType {
    NonTrend,
    #[default]
    Normal,
    NormalVariation,
    Neutral,
    NeutralCenter,
    NeutralExtreme,
    Trend,
    DoubleDistribution,
    DoubleDistributionTrend,
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
    /// Directional, elongated profile with poor two-sided balance.
    Elongated,
}

/// Normalize historical `Debug` strings and current serde camelCase strings for
/// day-type comparisons across rules, research filters, and persisted rows.
pub fn normalize_day_type_label(label: &str) -> String {
    normalized_label(label)
        .replace("typical", "normal")
        .replace("expandedtypical", "normalvariation")
        .replace("neutralday", "neutral")
        .replace("neutralcenterday", "neutralcenter")
        .replace("neutralextremeday", "neutralextreme")
}

/// Return persisted spellings that should be treated as equivalent for filters.
pub fn day_type_label_aliases(label: &str) -> Vec<&'static str> {
    match normalize_day_type_label(label).as_str() {
        "neutral" | "neutralcenter" => vec!["Neutral", "neutral", "NeutralCenter", "neutralCenter"],
        "neutralextreme" => vec!["NeutralExtreme", "neutralExtreme"],
        "doubledistribution" | "doubledistributiontrend" => vec![
            "DoubleDistribution",
            "doubleDistribution",
            "DoubleDistributionTrend",
            "doubleDistributionTrend",
        ],
        "nontrend" => vec!["NonTrend", "nonTrend"],
        "normalvariation" => vec!["NormalVariation", "normalVariation"],
        "trend" => vec!["Trend", "trend"],
        "normal" => vec!["Normal", "normal"],
        _ => Vec::new(),
    }
}

/// Normalize profile-shape / distribution labels across legacy and new names.
pub fn normalize_profile_shape_label(label: &str) -> String {
    match normalized_label(label).as_str() {
        "d" | "dshape" | "dshaped" | "doubledistribution" => "dshape".to_string(),
        "p" | "pshaped" => "pshape".to_string(),
        "b" | "bshaped" => "bshape".to_string(),
        "balanced" | "singledistribution" | "normaldistribution" => "gaussian".to_string(),
        other => other.to_string(),
    }
}

fn normalized_label(label: &str) -> String {
    label
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
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
    /// - `last_price`: current/closing price for center-vs-extreme day-type splits
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
        last_price: f64,
        single_print_prices: &[f64],
    ) {
        if tpo_counts.is_empty() || session_high <= session_low {
            return;
        }

        let range = session_high - session_low;
        let va_width = va_high - va_low;
        let ib_range = ib_high - ib_low;
        let topology = TpoTopology::new(tpo_counts, session_high, session_low, poc, last_price);
        let has_double_distribution = topology.double_distribution.is_some();

        self.profile_shape = if has_double_distribution {
            ProfileShape::DShape
        } else if topology.skew > 0.6 {
            ProfileShape::PShape
        } else if topology.skew < 0.4 {
            ProfileShape::BShape
        } else if va_width / range.max(0.01) < 0.55 && topology.poc_extreme_pct < 0.25 {
            ProfileShape::Elongated
        } else {
            ProfileShape::Gaussian
        };

        // Day type classification
        let va_pct_of_range = if range > 0.0 { va_width / range } else { 0.0 };
        let extended_beyond_ib = session_high > ib_high || session_low < ib_low;
        let broke_above_ib = session_high > ib_high;
        let broke_below_ib = session_low < ib_low;
        let broke_both_sides = broke_above_ib && broke_below_ib;
        let has_single_prints = !single_print_prices.is_empty();
        let price_at_extreme = topology.poc_extreme_pct < 0.2;
        let close_near_extreme = topology.last_price_extreme_pct < 0.25;
        let narrow_range = ib_range > 0.0 && range <= ib_range * 1.15;
        let trend_candidate =
            !broke_both_sides && has_single_prints && price_at_extreme && va_pct_of_range < 0.6;

        self.day_type = if trend_candidate && !has_double_distribution {
            DayType::Trend
        } else if has_double_distribution && (close_near_extreme || trend_candidate) {
            DayType::DoubleDistributionTrend
        } else if narrow_range && !extended_beyond_ib {
            DayType::NonTrend
        } else if va_pct_of_range > 0.85 {
            DayType::Normal
        } else if broke_both_sides && close_near_extreme {
            DayType::NeutralExtreme
        } else if broke_both_sides {
            DayType::NeutralCenter
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

#[derive(Debug, Clone)]
struct TpoTopology {
    skew: f64,
    poc_extreme_pct: f64,
    last_price_extreme_pct: f64,
    double_distribution: Option<DoubleDistributionEvidence>,
}

#[derive(Debug, Clone)]
struct DoubleDistributionEvidence;

#[derive(Debug, Clone)]
struct PeakRegion {
    start: usize,
    end: usize,
    mass: usize,
    max_count: usize,
    center_price: f64,
}

impl TpoTopology {
    fn new(
        tpo_counts: &[(f64, usize)],
        session_high: f64,
        session_low: f64,
        poc: f64,
        last_price: f64,
    ) -> Self {
        let total_tpos: usize = tpo_counts.iter().map(|(_, c)| *c).sum();
        let mid_price = if session_high > session_low {
            (session_high + session_low) / 2.0
        } else if let (Some(first), Some(last)) = (tpo_counts.first(), tpo_counts.last()) {
            (first.0 + last.0) / 2.0
        } else {
            0.0
        };
        let upper_tpos: usize = tpo_counts
            .iter()
            .filter(|(p, _)| *p >= mid_price)
            .map(|(_, c)| *c)
            .sum();
        let skew = if total_tpos > 0 {
            upper_tpos as f64 / total_tpos as f64
        } else {
            0.5
        };
        let range = if session_high > session_low {
            session_high - session_low
        } else if let (Some(first), Some(last)) = (tpo_counts.first(), tpo_counts.last()) {
            last.0 - first.0
        } else {
            0.0
        };
        let poc_extreme_pct = if range > 0.0 {
            ((session_high - poc).abs().min((poc - session_low).abs()) / range).clamp(0.0, 1.0)
        } else {
            0.5
        };
        let last_price_extreme_pct = if range > 0.0 && session_high > session_low {
            ((session_high - last_price)
                .abs()
                .min((last_price - session_low).abs())
                / range)
                .clamp(0.0, 1.0)
        } else {
            0.5
        };

        Self {
            skew,
            poc_extreme_pct,
            last_price_extreme_pct,
            double_distribution: detect_double_distribution_evidence(tpo_counts, total_tpos),
        }
    }
}

fn detect_double_distribution_evidence(
    tpo_counts: &[(f64, usize)],
    total_tpos: usize,
) -> Option<DoubleDistributionEvidence> {
    if tpo_counts.len() < 12 || total_tpos < 30 {
        return None;
    }
    let max_tpo = tpo_counts.iter().map(|(_, c)| *c).max()?;
    if max_tpo < 4 {
        return None;
    }

    let peak_threshold = ((max_tpo as f64) * 0.60).ceil() as usize;
    let valley_threshold = ((max_tpo as f64) * 0.35).floor().max(1.0) as usize;
    let mut regions = Vec::new();
    let mut i = 0;
    while i < tpo_counts.len() {
        if tpo_counts[i].1 >= peak_threshold {
            let start = i;
            let mut end = i;
            let mut mass = 0usize;
            let mut weighted_price = 0.0;
            let mut max_count = 0usize;
            while end < tpo_counts.len() && tpo_counts[end].1 >= peak_threshold {
                let count = tpo_counts[end].1;
                mass += count;
                weighted_price += tpo_counts[end].0 * count as f64;
                max_count = max_count.max(count);
                end += 1;
            }
            regions.push(PeakRegion {
                start,
                end: end - 1,
                mass,
                max_count,
                center_price: weighted_price / mass.max(1) as f64,
            });
            i = end;
        } else {
            i += 1;
        }
    }

    for left in &regions {
        for right in regions.iter().filter(|region| region.start > left.end + 1) {
            let valley = &tpo_counts[left.end + 1..right.start];
            if valley.len() < 2 {
                continue;
            }
            let valley_min = valley.iter().map(|(_, c)| *c).min().unwrap_or(usize::MAX);
            let valley_mass: usize = valley.iter().map(|(_, c)| *c).sum();
            let separation_ticks = right.start - left.end - 1;
            let left_mass_pct = left.mass as f64 / total_tpos as f64;
            let right_mass_pct = right.mass as f64 / total_tpos as f64;
            let valley_avg = valley_mass as f64 / valley.len() as f64;
            let peak_avg = (left.max_count + right.max_count) as f64 / 2.0;
            let price_separation = (right.center_price - left.center_price).abs();
            let tick_size = tpo_counts
                .windows(2)
                .map(|w| (w[1].0 - w[0].0).abs())
                .find(|step| *step > 0.0)
                .unwrap_or(0.25);

            if left_mass_pct >= 0.12
                && right_mass_pct >= 0.12
                && separation_ticks >= 2
                && price_separation >= tick_size * 6.0
                && valley_min <= valley_threshold
                && valley_avg <= peak_avg * 0.45
            {
                return Some(DoubleDistributionEvidence);
            }
        }
    }
    None
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
            21002.5,
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
            &tpo, 21001.75, 21000.5, 21001.0, 21001.5, 21000.5, 21002.25, 21000.0, 21002.25,
            &singles,
        );
        assert_eq!(c.single_prints_direction(), SinglePrintsDirection::AbovePoc);
    }

    #[test]
    fn shallow_notch_is_not_double_distribution() {
        let mut c = DayTypeClassifier::new();
        let counts = [1, 2, 4, 6, 7, 5, 3, 5, 7, 6, 4, 2, 1];
        let tpo: Vec<(f64, usize)> = counts
            .iter()
            .enumerate()
            .map(|(i, count)| (21000.0 + i as f64 * 0.25, *count))
            .collect();
        c.update(
            &tpo,
            21002.25,
            21000.75,
            21001.0,
            21002.0,
            21000.5,
            21003.0,
            21000.0,
            21001.5,
            &[],
        );

        assert_ne!(c.profile_shape(), ProfileShape::DShape);
        assert_ne!(c.day_type(), DayType::DoubleDistributionTrend);
    }

    #[test]
    fn classifies_true_double_distribution_trend() {
        let mut c = DayTypeClassifier::new();
        let counts = [1, 4, 6, 7, 6, 2, 1, 1, 2, 6, 8, 7, 5, 2];
        let tpo: Vec<(f64, usize)> = counts
            .iter()
            .enumerate()
            .map(|(i, count)| (21000.0 + i as f64 * 0.25, *count))
            .collect();
        c.update(
            &tpo,
            21003.0,
            21000.25,
            21002.5,
            21001.5,
            21000.25,
            21003.25,
            21000.0,
            21003.0,
            &[21001.25, 21001.5, 21001.75],
        );

        assert_eq!(c.profile_shape(), ProfileShape::DShape);
        assert_eq!(c.day_type(), DayType::DoubleDistributionTrend);
    }

    #[test]
    fn splits_neutral_center_and_extreme() {
        let tpo: Vec<(f64, usize)> = (0..20)
            .map(|i| {
                let count = if (6..=13).contains(&i) { 4 } else { 2 };
                (21000.0 + i as f64 * 0.25, count)
            })
            .collect();

        let mut center = DayTypeClassifier::new();
        center.update(
            &tpo,
            21003.0,
            21001.25,
            21002.25,
            21003.0,
            21001.75,
            21004.75,
            21000.0,
            21002.25,
            &[],
        );
        assert_eq!(center.day_type(), DayType::NeutralCenter);

        let mut extreme = DayTypeClassifier::new();
        extreme.update(
            &tpo,
            21003.0,
            21001.25,
            21002.25,
            21003.0,
            21001.75,
            21004.75,
            21000.0,
            21004.5,
            &[21004.5],
        );
        assert_eq!(extreme.day_type(), DayType::NeutralExtreme);
    }

    #[test]
    fn classifies_non_trend_inside_ib() {
        let mut c = DayTypeClassifier::new();
        let tpo: Vec<(f64, usize)> = (0..8)
            .map(|i| {
                let count = if (2..=5).contains(&i) { 6 } else { 3 };
                (21000.0 + i as f64 * 0.25, count)
            })
            .collect();
        c.update(
            &tpo,
            21001.25,
            21000.25,
            21000.75,
            21002.0,
            21000.0,
            21001.75,
            21000.0,
            21000.75,
            &[],
        );

        assert_eq!(c.day_type(), DayType::NonTrend);
    }
}
