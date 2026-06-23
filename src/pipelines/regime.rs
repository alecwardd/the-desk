//! Session regime classification (IDEA-000).
//!
//! Derives a coarse, session-level *regime* from already-computed pipeline
//! outputs. The regime is the top-level selector intended to gate which setup
//! *families* are eligible to fire (continuation vs. responsive vs. stand
//! aside). See `docs/setup-ideas-and-backtesting.md` (IDEA-000).
//!
//! This module is pure math over snapshot inputs — no tick accumulation, no
//! I/O, no LLM. Thresholds are deliberately conservative and tunable; the
//! IDEA-000 backtest hypothesis is whether gating on this regime improves
//! expectancy versus ungated firing, so these constants are expected to be
//! revisited once that backtest runs.

use super::{BalanceState, DayType, IB_EXTENSION_RATIO};
use serde::{Deserialize, Serialize};

/// RVOL ratio at or above which participation counts as "elevated" for regime
/// purposes (1.0 = tracking the historical average for this time of day).
const REGIME_ELEVATED_RVOL: f64 = 1.15;

/// Tape pace percentile (0.0–1.0) at or above which participation counts as
/// "elevated" even if the RVOL ratio has not caught up yet.
const REGIME_ELEVATED_PACE: f64 = 0.66;

/// Top-level session regime used to gate setup-family eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum Regime {
    /// One-sided IB extension accepted away from value with participation —
    /// continuation / initiative setups are appropriate.
    OneSidedAcceptance,
    /// Two-sided extension, double-distribution, or unwind back into value —
    /// responsive / mean-reversion / inventory-clear setups are appropriate.
    Migration,
    /// Mixed / low-participation / liquidity-failure context — favor caution
    /// and reversal-of-defense setups over blind continuation.
    Transition,
    /// Not enough structure yet (e.g. pre-IB, missing VWAP) to classify.
    #[default]
    Unclear,
}

/// Classify whether the session reached no, one-sided, or both-sided 0.5x IB
/// extension. Returns one of `None` / `UpOnly` / `DownOnly` / `BothSides`.
///
/// This is the canonical (Layer 1) implementation; the backfill / session-close
/// path calls down into it so live and stored values stay identical.
pub fn ib_extension_state_from_range(ib_high: f64, ib_low: f64, high: f64, low: f64) -> String {
    if ib_high <= 0.0 || ib_low <= 0.0 || ib_high <= ib_low {
        return "None".to_string();
    }
    let ib_range = ib_high - ib_low;
    let up_extension = high >= ib_high + ib_range * IB_EXTENSION_RATIO;
    let down_extension = low <= ib_low - ib_range * IB_EXTENSION_RATIO;
    match (up_extension, down_extension) {
        (true, true) => "BothSides",
        (true, false) => "UpOnly",
        (false, true) => "DownOnly",
        (false, false) => "None",
    }
    .to_string()
}

/// Inputs to [`classify_regime`], read from a `MarketState` snapshot.
#[derive(Debug, Clone, Copy)]
pub struct RegimeInputs<'a> {
    /// `None` / `UpOnly` / `DownOnly` / `BothSides` from
    /// [`ib_extension_state_from_range`].
    pub ib_extension_state: &'a str,
    pub day_type: DayType,
    pub balance_state: BalanceState,
    pub last_price: f64,
    pub vwap: f64,
    /// Delta-neutral pivot; `<= 0.0` is treated as "not yet available".
    pub dnp: f64,
    pub rvol_ratio: f64,
    /// Tape pace percentile in 0.0–1.0.
    pub pace_percentile: f64,
}

impl RegimeInputs<'_> {
    fn participation_elevated(&self) -> bool {
        self.rvol_ratio >= REGIME_ELEVATED_RVOL || self.pace_percentile >= REGIME_ELEVATED_PACE
    }

    /// Whether price is accepted above both VWAP and (when known) DNP.
    fn accepted_above(&self) -> bool {
        self.last_price > self.vwap && (self.dnp <= 0.0 || self.last_price > self.dnp)
    }

    /// Whether price is accepted below both VWAP and (when known) DNP.
    fn accepted_below(&self) -> bool {
        self.last_price < self.vwap && (self.dnp <= 0.0 || self.last_price < self.dnp)
    }
}

/// Classify the current session regime from snapshot inputs.
///
/// Decision order (most specific first):
/// 1. No VWAP / price yet → `Unclear`.
/// 2. One-sided IB extension with matching acceptance + participation →
///    `OneSidedAcceptance`; one-sided extension without that confirmation →
///    `Transition`.
/// 3. Both-sided IB extension → `Migration`.
/// 4. No extension yet → lean on day type (double-distribution / neutral →
///    `Migration`; trend with participation → `OneSidedAcceptance`; otherwise
///    `Unclear`).
pub fn classify_regime(inputs: &RegimeInputs<'_>) -> Regime {
    if inputs.vwap <= 0.0 || inputs.last_price <= 0.0 {
        return Regime::Unclear;
    }
    let elevated = inputs.participation_elevated();
    match inputs.ib_extension_state {
        "UpOnly" => {
            if inputs.accepted_above() && elevated {
                Regime::OneSidedAcceptance
            } else {
                Regime::Transition
            }
        }
        "DownOnly" => {
            if inputs.accepted_below() && elevated {
                Regime::OneSidedAcceptance
            } else {
                Regime::Transition
            }
        }
        "BothSides" => Regime::Migration,
        _ => match inputs.day_type {
            DayType::DoubleDistribution
            | DayType::DoubleDistributionTrend
            | DayType::Neutral
            | DayType::NeutralCenter
            | DayType::NeutralExtreme => Regime::Migration,
            DayType::Trend => {
                if elevated && (inputs.accepted_above() || inputs.accepted_below()) {
                    Regime::OneSidedAcceptance
                } else {
                    Regime::Transition
                }
            }
            _ => Regime::Unclear,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> RegimeInputs<'static> {
        RegimeInputs {
            ib_extension_state: "None",
            day_type: DayType::Normal,
            balance_state: BalanceState::Balanced,
            last_price: 21_000.0,
            vwap: 20_980.0,
            dnp: 20_975.0,
            rvol_ratio: 1.3,
            pace_percentile: 0.7,
        }
    }

    #[test]
    fn ib_extension_state_classifies_each_side() {
        // IB 20980–21020 (range 40); a 0.5x extension requires 20 pts beyond
        // each edge: up >= 21040, down <= 20960.
        let (ibh, ibl) = (21_020.0, 20_980.0);
        assert_eq!(
            ib_extension_state_from_range(ibh, ibl, 21_030.0, 20_970.0),
            "None"
        );
        assert_eq!(
            ib_extension_state_from_range(ibh, ibl, 21_050.0, 20_970.0),
            "UpOnly"
        );
        assert_eq!(
            ib_extension_state_from_range(ibh, ibl, 21_030.0, 20_950.0),
            "DownOnly"
        );
        assert_eq!(
            ib_extension_state_from_range(ibh, ibl, 21_050.0, 20_950.0),
            "BothSides"
        );
        // Degenerate / unformed IB → None.
        assert_eq!(ib_extension_state_from_range(0.0, 0.0, 5.0, -5.0), "None");
    }

    #[test]
    fn unclear_without_vwap_or_price() {
        let mut i = base();
        i.vwap = 0.0;
        assert_eq!(classify_regime(&i), Regime::Unclear);
        let mut i = base();
        i.last_price = 0.0;
        assert_eq!(classify_regime(&i), Regime::Unclear);
    }

    #[test]
    fn one_sided_up_acceptance_with_participation() {
        let mut i = base();
        i.ib_extension_state = "UpOnly";
        // price above vwap and dnp, elevated participation
        assert_eq!(classify_regime(&i), Regime::OneSidedAcceptance);
    }

    #[test]
    fn one_sided_without_acceptance_is_transition() {
        let mut i = base();
        i.ib_extension_state = "UpOnly";
        i.last_price = 20_970.0; // below vwap → not accepted up
        assert_eq!(classify_regime(&i), Regime::Transition);
    }

    #[test]
    fn one_sided_without_participation_is_transition() {
        let mut i = base();
        i.ib_extension_state = "DownOnly";
        i.last_price = 20_950.0; // accepted below
        i.rvol_ratio = 0.8;
        i.pace_percentile = 0.4;
        assert_eq!(classify_regime(&i), Regime::Transition);
    }

    #[test]
    fn both_sides_is_migration() {
        let mut i = base();
        i.ib_extension_state = "BothSides";
        assert_eq!(classify_regime(&i), Regime::Migration);
    }

    #[test]
    fn double_distribution_without_extension_is_migration() {
        let mut i = base();
        i.day_type = DayType::DoubleDistribution;
        assert_eq!(classify_regime(&i), Regime::Migration);
    }

    #[test]
    fn trend_without_extension_needs_participation() {
        let mut i = base();
        i.day_type = DayType::Trend;
        assert_eq!(classify_regime(&i), Regime::OneSidedAcceptance);
        i.rvol_ratio = 0.5;
        i.pace_percentile = 0.2;
        assert_eq!(classify_regime(&i), Regime::Transition);
    }
}
