//! Unified IDEA-011 + IDEA-025B IB campaign study (offline, repo-native).
//!
//! Layer A (descriptive): one-sided vs both-sided 0.5x IB extension, exact
//! 30-minute one-sided persistence, RVOL regime, first extension metadata,
//! IB close vs midpoint, IB extreme formation order, and next-bar-open-inside.
//!
//! Layer B (tradable): first pullback to the broken IB edge after acceptance,
//! comparing three preregistered gates with a fixed-stop 1R/2R matrix.
//!
//! This path intentionally does **not** force sticky rules-engine conditions
//! through `run_backtest` — persistence and confluence are tick-exact and
//! would over-fire as state flags. Outcomes are computed offline with the same
//! 0.5x IB extension contract as Layer 1 (`IB_EXTENSION_RATIO`).

use crate::db::Database;
use crate::feed::load_feed_config;
use crate::feed::scid_reader::{ScanControl, ScidReader, ScidTick};
use crate::pipelines::event_detector::{
    crossed_level, IB_EXTENSION_DIRECTION_DOWN, IB_EXTENSION_DIRECTION_UP, IB_EXTENSION_RATIO,
};
use crate::pipelines::{RvolClassification, RvolPipeline};
use crate::research::{reliability_tier, ReliabilityTier};
use crate::rules::RULES_ENGINE_SCHEMA_VERSION;
use crate::{session_date_from_timestamp_ms, tick_time_context_from_timestamp_ms, SessionType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

/// Exact one-sided persistence window required by IDEA-011.
pub const ONE_SIDED_PERSISTENCE_MS: f64 = 30.0 * 60_000.0;

/// Fixed stop distance in points (1R). Shared by the 1R/2R target matrix.
pub const FIXED_STOP_POINTS: f64 = 12.0;

/// Minimum RTH ticks for a usable session (excludes stub / partial files).
pub const MIN_RTH_TICKS: i64 = 20_000;

/// RTH intra-session gap (ms) treated as materially gappy.
pub const MATERIAL_RTH_GAP_MS: f64 = 5.0 * 60_000.0;

/// Campaign cutoff in ET minutes since midnight (16:00). Used for one-sided
/// arming, acceptance, outcomes, and time-exit bounds — not the CME 16:15 close.
pub const CAMPAIGN_CUTOFF_ET_MINUTES: i32 = 16 * 60;

/// Noon ET for IDEA-025B descriptive IB-edge breach-by-noon.
pub const NOON_ET_MINUTES: i32 = 12 * 60;

/// First RTH tick must land at or before 09:35 ET for start coverage.
pub const RTH_START_COVERAGE_MAX_ET: i32 = crate::RTH_OPEN_ET + 5;

/// Must observe a tick at or after 15:55 ET for end-of-window coverage.
pub const RTH_END_COVERAGE_MIN_ET: i32 = CAMPAIGN_CUTOFF_ET_MINUTES - 5;

/// Required prior clean RTH curves for causal RVOL inferential eligibility.
pub const CAUSAL_RVOL_BASELINE_SESSIONS: usize = 20;

/// Root symbol scope for campaign causal RVOL (cross-contract, date-ordered).
pub const CAUSAL_RVOL_ROOT_SYMBOL: &str = "NQ";

/// Known CME equity-index holiday / early-close dates to exclude from clean N.
pub const EXCLUDED_SESSION_DATES: &[&str] = &[
    "2025-11-27", // Thanksgiving
    "2025-11-28", // day after Thanksgiving (often thin)
    "2025-12-24",
    "2025-12-25",
    "2025-12-31",
    "2026-01-01",
    "2026-01-19", // MLK
    "2026-02-16", // Presidents Day
    "2026-04-03", // Good Friday
    "2026-05-25", // Memorial Day
    "2026-06-19", // Juneteenth
    "2026-07-03", // Independence Day observed early close context
    "2026-07-04",
];

/// One clean RTH session volume curve eligible for causal RVOL baselines.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatedVolumeCurve {
    pub session_date: String,
    pub contract: String,
    pub root_symbol: String,
    pub curve: Vec<f64>,
}

/// Causal RVOL baseline selected for one evaluated session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CausalRvolBaseline {
    pub session_dates: Vec<String>,
    pub curves: Vec<Vec<f64>>,
    pub count: usize,
    pub root_symbol: String,
}

/// Map a campaign contract id (e.g. `NQH6.CME`) onto the shared root scope.
pub fn campaign_root_symbol(contract: &str) -> String {
    let bare = contract.split('.').next().unwrap_or(contract);
    let alpha: String = bare
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    if alpha.starts_with(CAUSAL_RVOL_ROOT_SYMBOL) || alpha.is_empty() {
        CAUSAL_RVOL_ROOT_SYMBOL.to_string()
    } else {
        alpha
    }
}

/// Select at most `max_sessions` prior clean root-scoped curves strictly before
/// `as_of_date`. Never includes same-date or future curves. Deterministic:
/// ascending date order, then the most recent `max_sessions` priors.
pub fn select_causal_rvol_baseline(
    catalog: &[DatedVolumeCurve],
    as_of_date: &str,
    root_symbol: &str,
    max_sessions: usize,
) -> CausalRvolBaseline {
    let mut eligible: Vec<&DatedVolumeCurve> = catalog
        .iter()
        .filter(|c| c.root_symbol == root_symbol)
        .filter(|c| c.session_date.as_str() < as_of_date)
        .collect();
    eligible.sort_by(|a, b| {
        a.session_date
            .cmp(&b.session_date)
            .then_with(|| a.contract.cmp(&b.contract))
    });
    if max_sessions == 0 || eligible.is_empty() {
        return CausalRvolBaseline {
            session_dates: Vec::new(),
            curves: Vec::new(),
            count: 0,
            root_symbol: root_symbol.to_string(),
        };
    }
    let start = eligible.len().saturating_sub(max_sessions);
    let selected = &eligible[start..];
    CausalRvolBaseline {
        session_dates: selected.iter().map(|c| c.session_date.clone()).collect(),
        curves: selected.iter().map(|c| c.curve.clone()).collect(),
        count: selected.len(),
        root_symbol: root_symbol.to_string(),
    }
}

pub fn rvol_classification_label(class: RvolClassification) -> &'static str {
    match class {
        RvolClassification::Unavailable => "Unavailable",
        RvolClassification::Low => "Low",
        RvolClassification::Normal => "Normal",
        RvolClassification::Elevated => "Elevated",
        RvolClassification::High => "High",
    }
}

/// First directional one-tick IB-edge breach at or before `et_deadline` (inclusive).
///
/// Independent of 0.5x extension / trade gates. Missing breach → `None` (never
/// defaults to success).
pub fn first_ib_edge_breach_by_et(
    high_breach_ts: Option<f64>,
    high_breach_et: Option<i32>,
    low_breach_ts: Option<f64>,
    low_breach_et: Option<i32>,
    et_deadline: i32,
) -> Option<(ExtensionDirection, f64)> {
    let up = match (high_breach_ts, high_breach_et) {
        (Some(ts), Some(et)) if et <= et_deadline => Some((ExtensionDirection::Up, ts)),
        _ => None,
    };
    let down = match (low_breach_ts, low_breach_et) {
        (Some(ts), Some(et)) if et <= et_deadline => Some((ExtensionDirection::Down, ts)),
        _ => None,
    };
    match (up, down) {
        (Some(u), Some(d)) => {
            if u.1 <= d.1 {
                Some(u)
            } else {
                Some(d)
            }
        }
        (Some(u), None) => Some(u),
        (None, Some(d)) => Some(d),
        (None, None) => None,
    }
}

/// Rejection totals must reconcile to the population and never invent exclusions.
pub fn reconcile_rejection_counts(
    population: usize,
    usable: usize,
    excluded: usize,
    exclusion_counts: &BTreeMap<String, usize>,
) -> bool {
    if population != usable.saturating_add(excluded) {
        return false;
    }
    if excluded == 0 {
        return exclusion_counts.values().all(|&c| c == 0) || exclusion_counts.is_empty();
    }
    let reason_hits: usize = exclusion_counts.values().sum();
    // Every excluded session contributes ≥1 reason; reasons may stack.
    reason_hits >= excluded
}

/// Campaign contract windows — never silently pool across contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractWindow {
    pub contract: String,
    pub start_date: String,
    pub end_date: String,
    pub role: WindowRole,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowRole {
    Discovery,
    Holdout,
}

/// Seed start for the H6 development window (pre-roll calendar anchor only —
/// roll boundaries themselves must come from volume crossovers).
pub const CAMPAIGN_H6_START_DATE: &str = "2025-12-15";

/// Inclusive end of the development calendar; no date on/before this is holdout.
pub const DEVELOPMENT_END_DATE: &str = "2026-07-21";

/// Bounded H6↔M6 overlap scan for volume crossover (not full-contract replay).
pub const H6_M6_OVERLAP_SCAN_START: &str = "2026-02-15";
pub const H6_M6_OVERLAP_SCAN_END: &str = "2026-03-20";

/// Bounded M6↔U6 overlap scan for volume crossover (not full-contract replay).
pub const M6_U6_OVERLAP_SCAN_START: &str = "2026-05-15";
pub const M6_U6_OVERLAP_SCAN_END: &str = "2026-06-15";

/// Provisional windows used only when documenting the H6 seed start. Production
/// prepare/replay **must** call `derive_campaign_windows_from_rollover` after
/// resolved crossovers — never treat these end dates as authoritative rolls.
pub fn default_campaign_windows() -> Vec<ContractWindow> {
    vec![
        ContractWindow {
            contract: "NQH6.CME".into(),
            start_date: CAMPAIGN_H6_START_DATE.into(),
            end_date: "2026-03-06".into(),
            role: WindowRole::Discovery,
            rationale: "Provisional H6 shell; replace via volume-crossover derivation.".into(),
        },
        ContractWindow {
            contract: "NQM6.CME".into(),
            start_date: "2026-03-13".into(),
            end_date: "2026-05-29".into(),
            role: WindowRole::Discovery,
            rationale: "Provisional M6 shell; replace via volume-crossover derivation.".into(),
        },
        ContractWindow {
            contract: "NQU6.CME".into(),
            start_date: "2026-06-02".into(),
            end_date: DEVELOPMENT_END_DATE.into(),
            role: WindowRole::Discovery,
            rationale: "Provisional U6 shell; replace via volume-crossover derivation.".into(),
        },
    ]
}

/// Primary contrast target multiple (1R only; 2R is exploratory).
pub const PRIMARY_TARGET_R: f64 = 1.0;

/// Labeled total-cost scenario round-trip point costs (not actual fees).
pub const COST_SCENARIO_POINTS: [f64; 2] = [0.5, 1.0];

/// Fixed seed for deterministic weekly/contract block bootstrap.
pub const BLOCK_BOOTSTRAP_SEED: u64 = 0x1BCA_2026_0722;

/// Bootstrap replicate count (no external RNG dependency).
pub const BLOCK_BOOTSTRAP_REPLICATES: usize = 1_000;

pub const VERDICT_DEVELOPMENT_RESULT: &str = "DEVELOPMENT_RESULT";
pub const VERDICT_PARK_FORWARD_VALIDATION: &str = "PARK_FORWARD_VALIDATION";

/// Canonical V2 campaign root. Run dirs and DBs must be strictly beneath this path.
pub const V2_CAMPAIGN_ROOT: &str = r"T:\TheDesk\temp\backtests\ib-campaign-2026-07-22\v2";

/// Default isolated campaign DB filename (created empty; never a live-DB clone).
pub const V2_ISOLATED_DB_FILENAME: &str = "ib-campaign.db";

/// Deterministic volume-crossover rule for H6→M6 and M6→U6 boundaries.
///
/// Among dates where both front and back print positive RTH volume, choose the
/// first ISO date where `back_rth_volume > front_rth_volume`. The prior overlap
/// date is the front's last included session; the crossover date opens the back.
pub const ROLLOVER_CROSSOVER_RULE: &str = "first_date_back_rth_volume_strictly_exceeds_front";

/// Known artifact basenames owned by a V2 run directory.
pub const V2_RUN_ARTIFACT_NAMES: &[&str] = &[
    "sessions.json",
    "trades.json",
    "report.json",
    "rollover_evidence.json",
    "provenance.json",
    "REPORT.md",
    "v1-v2-count-reconciliation.json",
    "ib-campaign.db",
    "seed_report.json",
];

/// Preserved v1 campaign artifacts (parent of `v2/`). Read-only for reconciliation.
pub const V1_CAMPAIGN_ARTIFACT_DIR: &str = r"T:\TheDesk\temp\backtests\ib-campaign-2026-07-22";

/// Label applied to the preserved v1 result in reconciliation (never reuse its verdict).
pub const V1_RESULT_LABEL: &str = "INVALID_ENGINEERING_DIAGNOSTIC";

/// Live / production DB paths the campaign must never open (read or write).
pub const FORBIDDEN_LIVE_DB_PATHS: &[&str] = &[
    r"T:\TheDesk\state\data.db",
    r"C:\Users\alecw\.the-desk\data.db",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExtensionDirection {
    Up,
    Down,
}

impl ExtensionDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "up",
            Self::Down => "down",
        }
    }

    pub fn from_metadata(raw: &str) -> Option<Self> {
        match raw {
            IB_EXTENSION_DIRECTION_UP | "Up" => Some(Self::Up),
            IB_EXTENSION_DIRECTION_DOWN | "Down" => Some(Self::Down),
            _ => None,
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IbCloseVsMid {
    Above,
    Below,
    At,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IbExtremeOrder {
    /// IB low printed before IB high (bullish confluence leg).
    LowFirstHighLast,
    /// IB high printed before IB low (bearish confluence leg).
    HighFirstLowLast,
    /// Ambiguous / simultaneous within one tick.
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IbConfluence {
    Bullish,
    Bearish,
    None,
}

/// Pure IDEA-011 persistence check: first extension remains one-sided through
/// `check_ts`, and at least [`ONE_SIDED_PERSISTENCE_MS`] has elapsed.
pub fn one_sided_persisted_30m(
    first_ts: f64,
    first_dir: ExtensionDirection,
    opposite_ts: Option<f64>,
    check_ts: f64,
) -> bool {
    if check_ts < first_ts + ONE_SIDED_PERSISTENCE_MS {
        return false;
    }
    match opposite_ts {
        Some(opp) if opp <= check_ts => {
            // Opposite printed at or before the check → not one-sided at check.
            let _ = first_dir;
            false
        }
        _ => true,
    }
}

/// Evaluate whether an *observed* RTH tick qualifies as one-sided 30m acceptance.
///
/// Returns the observed tick timestamp on success — never a synthetic
/// `first_ts + 30m`. Requires:
/// - `observed_ts >= first_ts + ONE_SIDED_PERSISTENCE_MS`
/// - no opposite extension at or before `observed_ts`
/// - `et_minutes` strictly before [`CAMPAIGN_CUTOFF_ET_MINUTES`] (16:00 ET)
pub fn observe_one_sided_acceptance_ts(
    first_ts: f64,
    opposite_ts: Option<f64>,
    observed_ts: f64,
    et_minutes: i32,
) -> Option<f64> {
    if et_minutes >= CAMPAIGN_CUTOFF_ET_MINUTES {
        return None;
    }
    if observed_ts < first_ts + ONE_SIDED_PERSISTENCE_MS {
        return None;
    }
    if let Some(opp) = opposite_ts {
        if opp <= observed_ts {
            return None;
        }
    }
    Some(observed_ts)
}

pub fn classify_ib_close_vs_mid(ib_high: f64, ib_low: f64, ib_complete_price: f64) -> IbCloseVsMid {
    let mid = (ib_high + ib_low) / 2.0;
    if ib_complete_price > mid + 0.25 {
        IbCloseVsMid::Above
    } else if ib_complete_price < mid - 0.25 {
        IbCloseVsMid::Below
    } else {
        IbCloseVsMid::At
    }
}

/// IDEA-025B combined confluence (close vs mid + extreme order).
/// Next-bar-open-inside is reported separately and used as a soft feasibility filter.
pub fn classify_ib_confluence(
    close_vs_mid: IbCloseVsMid,
    extreme_order: IbExtremeOrder,
) -> IbConfluence {
    match (close_vs_mid, extreme_order) {
        (IbCloseVsMid::Above, IbExtremeOrder::LowFirstHighLast) => IbConfluence::Bullish,
        (IbCloseVsMid::Below, IbExtremeOrder::HighFirstLowLast) => IbConfluence::Bearish,
        _ => IbConfluence::None,
    }
}

pub fn confluence_matches_extension(confluence: IbConfluence, dir: ExtensionDirection) -> bool {
    matches!(
        (confluence, dir),
        (IbConfluence::Bullish, ExtensionDirection::Up)
            | (IbConfluence::Bearish, ExtensionDirection::Down)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GateKind {
    UngatedBaseline,
    OneSidedAcceptance30m,
    IbConfluence,
}

impl GateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UngatedBaseline => "ungated_baseline",
            Self::OneSidedAcceptance30m => "one_sided_acceptance_30m",
            Self::IbConfluence => "ib_confluence",
        }
    }

    pub fn hypothesis_id(self) -> &'static str {
        match self {
            Self::UngatedBaseline => "IDEA-011-025B-ungated",
            Self::OneSidedAcceptance30m => "IDEA-011-025B-onesided30m",
            Self::IbConfluence => "IDEA-011-025B-confluence",
        }
    }
}

/// Preregistered primary treated gate (one-sided 30m + causal Elevated/High RVOL).
pub const PRIMARY_TREATED_GATE: GateKind = GateKind::OneSidedAcceptance30m;

/// Preregistered primary control gate (ungated baseline), matched at 1R.
pub const PRIMARY_CONTROL_GATE: GateKind = GateKind::UngatedBaseline;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TradeExitReason {
    Target,
    Stop,
    TimeExit,
    InvalidatedOppositeExtension,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimulatedTrade {
    pub direction: ExtensionDirection,
    pub entry_ts_ms: f64,
    pub entry_price: f64,
    pub stop_price: f64,
    pub target_price: f64,
    pub risk_points: f64,
    pub target_r: f64,
    pub exit_ts_ms: f64,
    pub exit_price: f64,
    pub exit_reason: TradeExitReason,
    pub r_result: f64,
    pub mfe_points: f64,
    pub mae_points: f64,
    pub mfe_r: f64,
    pub mae_r: f64,
    pub win: bool,
}

/// Walk a post-entry price path with a fixed stop / target / time exit.
pub fn simulate_fixed_r_trade(
    direction: ExtensionDirection,
    entry_ts_ms: f64,
    entry_price: f64,
    risk_points: f64,
    target_r: f64,
    path: &[(f64, f64)],
    session_end_ts_ms: f64,
) -> Option<SimulatedTrade> {
    if risk_points <= 0.0 || !risk_points.is_finite() || path.is_empty() {
        return None;
    }
    let (stop_price, target_price) = match direction {
        ExtensionDirection::Up => (
            entry_price - risk_points,
            entry_price + risk_points * target_r,
        ),
        ExtensionDirection::Down => (
            entry_price + risk_points,
            entry_price - risk_points * target_r,
        ),
    };

    let mut mfe = 0.0_f64;
    let mut mae = 0.0_f64;
    let mut exit: Option<(f64, f64, TradeExitReason)> = None;

    for &(ts, px) in path {
        if ts < entry_ts_ms {
            continue;
        }
        let favorable = match direction {
            ExtensionDirection::Up => px - entry_price,
            ExtensionDirection::Down => entry_price - px,
        };
        let adverse = -favorable;
        mfe = mfe.max(favorable);
        mae = mae.max(adverse.max(0.0));

        let hit_target = match direction {
            ExtensionDirection::Up => px >= target_price,
            ExtensionDirection::Down => px <= target_price,
        };
        let hit_stop = match direction {
            ExtensionDirection::Up => px <= stop_price,
            ExtensionDirection::Down => px >= stop_price,
        };
        if hit_stop {
            exit = Some((ts, stop_price, TradeExitReason::Stop));
            break;
        }
        if hit_target {
            exit = Some((ts, target_price, TradeExitReason::Target));
            break;
        }
        if ts >= session_end_ts_ms {
            exit = Some((ts, px, TradeExitReason::TimeExit));
            break;
        }
    }

    let (exit_ts_ms, exit_price, exit_reason) = exit.or_else(|| {
        path.last()
            .map(|&(ts, px)| (ts.max(entry_ts_ms), px, TradeExitReason::TimeExit))
    })?;

    let signed = match direction {
        ExtensionDirection::Up => exit_price - entry_price,
        ExtensionDirection::Down => entry_price - exit_price,
    };
    let r_result = signed / risk_points;
    Some(SimulatedTrade {
        direction,
        entry_ts_ms,
        entry_price,
        stop_price,
        target_price,
        risk_points,
        target_r,
        exit_ts_ms,
        exit_price,
        exit_reason,
        r_result,
        mfe_points: mfe,
        mae_points: mae,
        mfe_r: mfe / risk_points,
        mae_r: mae / risk_points,
        win: r_result > 0.0,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionQuality {
    pub usable: bool,
    pub exclusion_reasons: Vec<String>,
    pub tick_count: i64,
    pub material_gap_count: u32,
    pub max_gap_minutes: f64,
    pub holiday_or_early_close: bool,
    pub first_rth_et_minutes: Option<i32>,
    pub last_rth_et_minutes: Option<i32>,
    pub rth_start_ts_ms: Option<f64>,
    pub rth_end_ts_ms: Option<f64>,
    pub has_rth_start_coverage: bool,
    pub has_rth_end_coverage: bool,
    pub non_monotonic_count: u32,
}

/// Inputs for deterministic session quality / coverage evaluation.
#[derive(Debug, Clone)]
pub struct SessionQualityInputs<'a> {
    pub session_date: &'a str,
    pub tick_count: i64,
    pub material_gap_count: u32,
    pub max_gap_minutes: f64,
    pub ib_high: f64,
    pub ib_low: f64,
    pub first_rth_et_minutes: Option<i32>,
    pub last_rth_et_minutes: Option<i32>,
    pub rth_start_ts_ms: Option<f64>,
    pub rth_end_ts_ms: Option<f64>,
    pub non_monotonic_count: u32,
}

pub fn evaluate_session_quality(input: SessionQualityInputs<'_>) -> SessionQuality {
    let mut reasons = Vec::new();
    let holiday = EXCLUDED_SESSION_DATES.contains(&input.session_date);
    if holiday {
        reasons.push("holiday_or_partial_session_calendar".into());
    }
    if input.tick_count < MIN_RTH_TICKS {
        reasons.push(format!("tiny_tick_count<{MIN_RTH_TICKS}"));
    }
    if input.ib_high <= 0.0 || input.ib_low <= 0.0 || input.ib_high <= input.ib_low {
        reasons.push("invalid_ib_range".into());
    }
    if input.material_gap_count > 0 {
        reasons.push(format!(
            "material_rth_gaps:{}(max={:.1}m)",
            input.material_gap_count, input.max_gap_minutes
        ));
    }
    if input.non_monotonic_count > 0 {
        reasons.push(format!(
            "non_monotonic_timestamps:{}",
            input.non_monotonic_count
        ));
    }
    let has_rth_start_coverage = input
        .first_rth_et_minutes
        .map(|et| (crate::RTH_OPEN_ET..=RTH_START_COVERAGE_MAX_ET).contains(&et))
        .unwrap_or(false);
    if !has_rth_start_coverage {
        reasons.push("missing_rth_start_coverage".into());
    }
    let has_rth_end_coverage = input
        .last_rth_et_minutes
        .map(|et| et >= RTH_END_COVERAGE_MIN_ET)
        .unwrap_or(false);
    if !has_rth_end_coverage {
        reasons.push("missing_rth_end_coverage".into());
    }
    SessionQuality {
        usable: reasons.is_empty(),
        exclusion_reasons: reasons,
        tick_count: input.tick_count,
        material_gap_count: input.material_gap_count,
        max_gap_minutes: input.max_gap_minutes,
        holiday_or_early_close: holiday,
        first_rth_et_minutes: input.first_rth_et_minutes,
        last_rth_et_minutes: input.last_rth_et_minutes,
        rth_start_ts_ms: input.rth_start_ts_ms,
        rth_end_ts_ms: input.rth_end_ts_ms,
        has_rth_start_coverage,
        has_rth_end_coverage,
        non_monotonic_count: input.non_monotonic_count,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionIbFeatures {
    pub session_date: String,
    pub contract: String,
    pub role: WindowRole,
    pub quality: SessionQuality,
    pub ib_high: f64,
    pub ib_low: f64,
    pub ib_range: f64,
    pub ib_mid: f64,
    pub ib_complete_price: Option<f64>,
    pub ib_complete_ts_ms: Option<f64>,
    pub close_vs_ib_mid: Option<IbCloseVsMid>,
    pub extreme_order: Option<IbExtremeOrder>,
    pub next_bar_open_inside_ib: Option<bool>,
    pub next_bar_open_price: Option<f64>,
    pub confluence: IbConfluence,
    pub first_extension_direction: Option<ExtensionDirection>,
    pub first_extension_ts_ms: Option<f64>,
    pub opposite_extension_ts_ms: Option<f64>,
    pub end_extension_state: String,
    pub persisted_one_sided_30m: bool,
    pub acceptance_ts_ms: Option<f64>,
    pub rvol_ratio_at_acceptance: Option<f64>,
    pub rvol_class_at_acceptance: Option<String>,
    pub rvol_elevated_or_high_at_acceptance: Option<bool>,
    /// Exact prior session dates used for this session's causal RVOL baseline.
    pub rvol_baseline_session_dates: Vec<String>,
    pub rvol_baseline_count: usize,
    /// True when fewer than [`CAUSAL_RVOL_BASELINE_SESSIONS`] prior clean curves
    /// were available. Descriptive metrics may still include the session; trades
    /// / inferential N must exclude it.
    pub causal_rvol_warmup: bool,
    /// `quality.usable && !causal_rvol_warmup`.
    pub inferential_eligible: bool,
    /// First one-tick IB-edge breach by noon ET (direction + timestamp), if observed.
    pub ib_breach_by_noon_direction: Option<ExtensionDirection>,
    pub ib_breach_by_noon_ts_ms: Option<f64>,
    /// First one-tick IB-edge breach by 16:00 ET cutoff (exclusive of cutoff).
    pub ib_breach_by_1600_direction: Option<ExtensionDirection>,
    pub ib_breach_by_1600_ts_ms: Option<f64>,
    pub session_close: f64,
    pub session_high: f64,
    pub session_low: f64,
    pub day_type: String,
    pub continuation_path: String,
    pub failure_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateTradeRow {
    pub gate: GateKind,
    pub hypothesis_id: String,
    pub session_date: String,
    pub contract: String,
    pub role: WindowRole,
    pub target_r: f64,
    pub trade: SimulatedTrade,
    pub rvol_ratio_at_entry: Option<f64>,
    pub day_type: String,
    pub entry_minute_et: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeStats {
    pub n: usize,
    pub reliability_tier: ReliabilityTier,
    pub win_rate: f64,
    pub win_rate_ci95: [f64; 2],
    pub avg_r: f64,
    pub std_r: f64,
    pub median_r: f64,
    pub avg_mfe_r: f64,
    pub avg_mae_r: f64,
    pub target_exits: usize,
    pub stop_exits: usize,
    pub time_exits: usize,
    pub signals_per_active_session: f64,
    pub active_sessions: usize,
}

fn wilson_ci(wins: usize, n: usize) -> [f64; 2] {
    if n == 0 {
        return [0.0, 0.0];
    }
    let z = 1.96_f64;
    let p = wins as f64 / n as f64;
    let z2 = z * z;
    let denom = 1.0 + z2 / n as f64;
    let center = p + z2 / (2.0 * n as f64);
    let margin = z * ((p * (1.0 - p) + z2 / (4.0 * n as f64)) / n as f64).sqrt();
    [
        ((center - margin) / denom).clamp(0.0, 1.0),
        ((center + margin) / denom).clamp(0.0, 1.0),
    ]
}

fn percentile_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let h = (sorted.len() - 1) as f64 * p;
    let lo = h.floor() as usize;
    let hi = h.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let w = h - lo as f64;
        sorted[lo] * (1.0 - w) + sorted[hi] * w
    }
}

pub fn summarize_trades(trades: &[GateTradeRow]) -> OutcomeStats {
    let n = trades.len();
    let wins = trades.iter().filter(|t| t.trade.win).count();
    let rs: Vec<f64> = trades.iter().map(|t| t.trade.r_result).collect();
    let mut sorted = rs.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let avg_r = if n == 0 {
        0.0
    } else {
        rs.iter().sum::<f64>() / n as f64
    };
    let std_r = if n == 0 {
        0.0
    } else {
        let var = rs.iter().map(|r| (r - avg_r).powi(2)).sum::<f64>() / n as f64;
        var.sqrt()
    };
    let active: BTreeMap<(&str, &str), usize> = {
        let mut m = BTreeMap::new();
        for t in trades {
            *m.entry((t.contract.as_str(), t.session_date.as_str()))
                .or_default() += 1;
        }
        m
    };
    let active_sessions = active.len();
    OutcomeStats {
        n,
        reliability_tier: reliability_tier(n),
        win_rate: if n == 0 { 0.0 } else { wins as f64 / n as f64 },
        win_rate_ci95: wilson_ci(wins, n),
        avg_r,
        std_r,
        median_r: percentile_sorted(&sorted, 0.5),
        avg_mfe_r: if n == 0 {
            0.0
        } else {
            trades.iter().map(|t| t.trade.mfe_r).sum::<f64>() / n as f64
        },
        avg_mae_r: if n == 0 {
            0.0
        } else {
            trades.iter().map(|t| t.trade.mae_r).sum::<f64>() / n as f64
        },
        target_exits: trades
            .iter()
            .filter(|t| t.trade.exit_reason == TradeExitReason::Target)
            .count(),
        stop_exits: trades
            .iter()
            .filter(|t| t.trade.exit_reason == TradeExitReason::Stop)
            .count(),
        time_exits: trades
            .iter()
            .filter(|t| t.trade.exit_reason == TradeExitReason::TimeExit)
            .count(),
        signals_per_active_session: if active_sessions == 0 {
            0.0
        } else {
            n as f64 / active_sessions as f64
        },
        active_sessions,
    }
}

/// Matched primary-contrast key: same session, contract, and direction.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContrastKey {
    pub session_date: String,
    pub contract: String,
    pub direction: ExtensionDirection,
}

impl ContrastKey {
    pub fn from_trade(trade: &GateTradeRow) -> Self {
        Self {
            session_date: trade.session_date.clone(),
            contract: trade.contract.clone(),
            direction: trade.trade.direction,
        }
    }
}

/// One matched primary-contrast observation (treated vs control at 1R).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchedContrastPair {
    pub key: ContrastKey,
    pub treated_gross_r: f64,
    pub control_gross_r: f64,
    pub delta_gross_r: f64,
}

/// Labeled gross or total-cost scenario summary for the primary contrast.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CostScenarioStats {
    pub label: String,
    pub cost_points_round_trip: f64,
    /// Always true for non-zero cost rows — scenarios, not actual fees.
    pub is_scenario_not_actual_fees: bool,
    pub n_matched: usize,
    pub treated_avg_r: f64,
    pub control_avg_r: f64,
    pub delta_avg_r: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockBootstrapInterval {
    pub block_kind: String,
    pub seed: u64,
    pub replicates: usize,
    pub n_blocks: usize,
    pub block_ids: Vec<String>,
    /// Ordinary mean of all matched-pair `delta_gross_r` (not equal-weight block means).
    pub point_mean_delta_r: f64,
    pub ci95_low: f64,
    pub ci95_high: f64,
    /// Explicit when n_blocks is too small for a stable inferential CI.
    pub sparse_block_limitation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FutureAcceptancePolicy {
    pub require_untouched_matched_holdout: bool,
    pub primary_contrast: String,
    pub min_matched_n_for_consideration: usize,
    pub require_same_gate_and_key: bool,
    pub bootstrap_ci_must_exclude_zero_for_accept_candidate: bool,
    pub cost_rows_are_scenarios_not_fees: bool,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignVerdict {
    pub result: String,
    pub disposition: String,
    pub rationale: String,
    pub future_holdout_required: bool,
    pub future_acceptance_policy: FutureAcceptancePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrimaryContrastReport {
    pub treated_gate: String,
    pub control_gate: String,
    pub target_r: f64,
    pub matched_n: usize,
    pub matched_keys: Vec<ContrastKey>,
    pub pairs: Vec<MatchedContrastPair>,
    pub scenarios: Vec<CostScenarioStats>,
    pub weekly_bootstrap: BlockBootstrapInterval,
    pub contract_bootstrap: BlockBootstrapInterval,
}

/// Convert gross R to a labeled total-cost scenario net R.
///
/// `cost_points` is round-trip total cost in points; never actual fees.
pub fn apply_round_trip_cost_r(gross_r: f64, cost_points: f64, risk_points: f64) -> f64 {
    if risk_points <= 0.0 || !risk_points.is_finite() {
        return gross_r;
    }
    gross_r - cost_points / risk_points
}

fn is_primary_1r(trade: &GateTradeRow, gate: GateKind) -> bool {
    trade.gate == gate && (trade.target_r - PRIMARY_TARGET_R).abs() < 1e-9
}

/// Build matched treated/control pairs on identical session+contract+direction keys.
///
/// Only 1R primary gates participate. Overlapping gates and 2R variants are excluded
/// from this matched N (they are never independent primary observations).
pub fn build_matched_primary_pairs(trades: &[GateTradeRow]) -> Vec<MatchedContrastPair> {
    let mut treated: BTreeMap<ContrastKey, f64> = BTreeMap::new();
    let mut control: BTreeMap<ContrastKey, f64> = BTreeMap::new();
    for t in trades {
        let key = ContrastKey::from_trade(t);
        if is_primary_1r(t, PRIMARY_TREATED_GATE) {
            treated.insert(key, t.trade.r_result);
        } else if is_primary_1r(t, PRIMARY_CONTROL_GATE) {
            control.insert(key, t.trade.r_result);
        }
    }
    let mut pairs = Vec::new();
    for (key, &treated_gross_r) in &treated {
        let Some(&control_gross_r) = control.get(key) else {
            continue;
        };
        pairs.push(MatchedContrastPair {
            key: key.clone(),
            treated_gross_r,
            control_gross_r,
            delta_gross_r: treated_gross_r - control_gross_r,
        });
    }
    pairs
}

fn mean_f64(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

fn scenario_stats(pairs: &[MatchedContrastPair], cost_points: f64) -> CostScenarioStats {
    let n = pairs.len();
    let treated: Vec<f64> = pairs
        .iter()
        .map(|p| apply_round_trip_cost_r(p.treated_gross_r, cost_points, FIXED_STOP_POINTS))
        .collect();
    let control: Vec<f64> = pairs
        .iter()
        .map(|p| apply_round_trip_cost_r(p.control_gross_r, cost_points, FIXED_STOP_POINTS))
        .collect();
    let delta: Vec<f64> = treated
        .iter()
        .zip(control.iter())
        .map(|(t, c)| t - c)
        .collect();
    let label = if cost_points == 0.0 {
        "gross".to_string()
    } else {
        format!("total_cost_{cost_points}_pt")
    };
    CostScenarioStats {
        label,
        cost_points_round_trip: cost_points,
        is_scenario_not_actual_fees: cost_points > 0.0,
        n_matched: n,
        treated_avg_r: mean_f64(&treated),
        control_avg_r: mean_f64(&control),
        delta_avg_r: mean_f64(&delta),
    }
}

fn week_block_id(session_date: &str) -> String {
    use chrono::{Datelike, NaiveDate};
    NaiveDate::parse_from_str(session_date, "%Y-%m-%d")
        .map(|d| {
            let iso = d.iso_week();
            format!("{:04}-W{:02}", iso.year(), iso.week())
        })
        .unwrap_or_else(|_| session_date.to_string())
}

/// Tiny deterministic LCG (no external RNG dependency).
#[derive(Debug, Clone)]
struct Lcg64 {
    state: u64,
}

impl Lcg64 {
    fn new(seed: u64) -> Self {
        Self {
            // SplitMix64 needs a non-zero-ish seed; mix in odd constant.
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64 — high-quality low bits (NR LCG alternates parity and collapses n=2).
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gen_index(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next_u64() as usize) % n
    }
}

fn group_pairs_into_blocks(
    pairs: &[MatchedContrastPair],
    block_id: impl Fn(&MatchedContrastPair) -> String,
) -> (Vec<String>, Vec<Vec<f64>>) {
    let mut blocks: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    for p in pairs {
        blocks.entry(block_id(p)).or_default().push(p.delta_gross_r);
    }
    let mut ids = Vec::new();
    let mut obs = Vec::new();
    for (id, vals) in blocks {
        ids.push(id);
        obs.push(vals);
    }
    (ids, obs)
}

fn sparse_block_limitation_note(n_blocks: usize) -> Option<String> {
    match n_blocks {
        0 => Some(
            "n_blocks=0: no bootstrap clusters; CI collapses to the empty/point mean and is not inferential."
                .into(),
        ),
        1 => Some(
            "n_blocks=1: cluster bootstrap cannot capture between-block variability; CI is not a stable inferential interval."
                .into(),
        ),
        2 => Some(
            "n_blocks=2: cluster bootstrap CI is sparse and should not be treated as a stable inferential interval."
                .into(),
        ),
        _ => None,
    }
}

/// Deterministic cluster/block bootstrap CI for mean matched delta gross R.
///
/// - Point estimate = ordinary mean over all matched-pair observations.
/// - Each replicate: resample exactly `n_blocks` block IDs WITH replacement,
///   concatenate every observation from each selected block (repeats included),
///   then take the mean of that pooled sample (unequal block sizes respected).
pub fn block_bootstrap_delta_ci(
    block_ids: &[String],
    block_observations: &[Vec<f64>],
    seed: u64,
    replicates: usize,
) -> BlockBootstrapInterval {
    debug_assert_eq!(block_ids.len(), block_observations.len());
    let n_blocks = block_observations.len();
    let all_obs: Vec<f64> = block_observations.iter().flatten().copied().collect();
    let point = mean_f64(&all_obs);
    let limitation = sparse_block_limitation_note(n_blocks);
    if n_blocks == 0 || replicates == 0 || all_obs.is_empty() {
        return BlockBootstrapInterval {
            block_kind: String::new(),
            seed,
            replicates,
            n_blocks,
            block_ids: block_ids.to_vec(),
            point_mean_delta_r: point,
            ci95_low: point,
            ci95_high: point,
            sparse_block_limitation: limitation,
        };
    }
    let mut rng = Lcg64::new(seed);
    let mut reps = Vec::with_capacity(replicates);
    for _ in 0..replicates {
        let mut pooled = Vec::new();
        for _ in 0..n_blocks {
            let idx = rng.gen_index(n_blocks);
            pooled.extend_from_slice(&block_observations[idx]);
        }
        reps.push(mean_f64(&pooled));
    }
    reps.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let lo = percentile_sorted(&reps, 0.025);
    let hi = percentile_sorted(&reps, 0.975);
    BlockBootstrapInterval {
        block_kind: String::new(),
        seed,
        replicates,
        n_blocks,
        block_ids: block_ids.to_vec(),
        point_mean_delta_r: point,
        ci95_low: lo,
        ci95_high: hi,
        sparse_block_limitation: limitation,
    }
}

/// Assemble the preregistered primary contrast package (matched 1R only).
pub fn build_primary_contrast_report(trades: &[GateTradeRow]) -> PrimaryContrastReport {
    let pairs = build_matched_primary_pairs(trades);
    let mut scenarios = vec![scenario_stats(&pairs, 0.0)];
    for &cost in &COST_SCENARIO_POINTS {
        scenarios.push(scenario_stats(&pairs, cost));
    }
    let (week_ids, week_obs) =
        group_pairs_into_blocks(&pairs, |p| week_block_id(&p.key.session_date));
    let mut weekly = block_bootstrap_delta_ci(
        &week_ids,
        &week_obs,
        BLOCK_BOOTSTRAP_SEED,
        BLOCK_BOOTSTRAP_REPLICATES,
    );
    weekly.block_kind = "weekly".into();

    let (contract_ids, contract_obs) = group_pairs_into_blocks(&pairs, |p| p.key.contract.clone());
    let mut contract = block_bootstrap_delta_ci(
        &contract_ids,
        &contract_obs,
        BLOCK_BOOTSTRAP_SEED ^ 0xC0FF_EE00,
        BLOCK_BOOTSTRAP_REPLICATES,
    );
    contract.block_kind = "contract".into();

    PrimaryContrastReport {
        treated_gate: PRIMARY_TREATED_GATE.as_str().into(),
        control_gate: PRIMARY_CONTROL_GATE.as_str().into(),
        target_r: PRIMARY_TARGET_R,
        matched_n: pairs.len(),
        matched_keys: pairs.iter().map(|p| p.key.clone()).collect(),
        pairs,
        scenarios,
        weekly_bootstrap: weekly,
        contract_bootstrap: contract,
    }
}

/// V2 verdict: always development + park forward validation; never ACCEPT.
pub fn decide_v2_verdict(primary: &PrimaryContrastReport) -> CampaignVerdict {
    let gross = primary
        .scenarios
        .iter()
        .find(|s| s.cost_points_round_trip == 0.0);
    let delta = gross.map(|s| s.delta_avg_r).unwrap_or(0.0);
    let rationale = format!(
        "Preregistered primary contrast {} vs {} at 1R on matched session+direction keys: matched_n={}, gross_delta_avg_r={:.4}, weekly_boot_ci=[{:.4},{:.4}]. All dates through {} are development; a future untouched matched holdout is required before any acceptance decision. {}",
        primary.treated_gate,
        primary.control_gate,
        primary.matched_n,
        delta,
        primary.weekly_bootstrap.ci95_low,
        primary.weekly_bootstrap.ci95_high,
        DEVELOPMENT_END_DATE,
        VERDICT_PARK_FORWARD_VALIDATION
    );
    CampaignVerdict {
        result: VERDICT_DEVELOPMENT_RESULT.into(),
        disposition: VERDICT_PARK_FORWARD_VALIDATION.into(),
        rationale,
        future_holdout_required: true,
        future_acceptance_policy: FutureAcceptancePolicy {
            require_untouched_matched_holdout: true,
            primary_contrast: format!(
                "{} vs {} @ {}R matched key",
                PRIMARY_TREATED_GATE.as_str(),
                PRIMARY_CONTROL_GATE.as_str(),
                PRIMARY_TARGET_R as i32
            ),
            min_matched_n_for_consideration: 30,
            require_same_gate_and_key: true,
            bootstrap_ci_must_exclude_zero_for_accept_candidate: true,
            cost_rows_are_scenarios_not_fees: true,
            note: format!(
                "Future policy only: never ACCEPT on development results; never compare a selected discovery gate to a different holdout key; never count overlapping gates or 1R/2R variants as independent. Untouched holdout must begin after {DEVELOPMENT_END_DATE}."
            ),
        },
    }
}

/// RTH minute-of-session index of 10:30 ET. IB is the half-open interval
/// `[09:30, 10:30)` → `minute_of_session ∈ [0, IB_END_MINUTE)`.
const IB_END_MINUTE: i32 = 60;

#[derive(Debug, Default)]
struct IbFormationTracker {
    ib_high: f64,
    ib_low: f64,
    /// Timestamp when the running IB high was last extended.
    high_set_ts: Option<f64>,
    /// Timestamp when the running IB low was last extended.
    low_set_ts: Option<f64>,
    locked: bool,
    /// Last observed tick strictly before 10:30 ET (IB close). Never synthetic.
    complete_price: Option<f64>,
    complete_ts: Option<f64>,
    /// First observed tick at or after 10:30 ET (post-IB 30m bar open).
    next_bar_open: Option<f64>,
    next_bar_inside: Option<bool>,
}

impl IbFormationTracker {
    /// Track IB formation under half-open `[09:30, 10:30)` semantics.
    ///
    /// - IB close = last observed pre-10:30 tick (`minute < IB_END_MINUTE`)
    /// - Post-IB open = first observed tick at/after 10:30 (`minute >= IB_END_MINUTE`)
    /// - No synthetic close/open ticks are invented when observations are missing
    fn on_rth_tick(&mut self, minute: i32, price: f64, ts: f64) {
        if minute < IB_END_MINUTE {
            if self.high_set_ts.is_none() && self.low_set_ts.is_none() {
                self.ib_high = price;
                self.ib_low = price;
                self.high_set_ts = Some(ts);
                self.low_set_ts = Some(ts);
            } else {
                if price > self.ib_high {
                    self.ib_high = price;
                    self.high_set_ts = Some(ts);
                }
                if price < self.ib_low {
                    self.ib_low = price;
                    self.low_set_ts = Some(ts);
                }
            }
            // Rolling IB close: last tick inside the half-open IB window.
            self.complete_price = Some(price);
            self.complete_ts = Some(ts);
            return;
        }

        // First tick at/after 10:30: lock the already-observed IB close and set
        // the post-IB open from *this* tick (not a later one).
        if !self.locked && self.ib_high > 0.0 && self.ib_low > 0.0 && self.ib_high >= self.ib_low {
            self.locked = true;
            // Do not overwrite complete_* — that would steal the post-IB open as
            // the IB close (the prior off-by-one defect).
            if self.next_bar_open.is_none() {
                self.next_bar_open = Some(price);
                self.next_bar_inside = Some(price >= self.ib_low && price <= self.ib_high);
            }
            return;
        }

        if self.locked && self.next_bar_open.is_none() {
            self.next_bar_open = Some(price);
            self.next_bar_inside = Some(price >= self.ib_low && price <= self.ib_high);
        }
    }

    fn extreme_order(&self) -> Option<IbExtremeOrder> {
        match (self.low_set_ts, self.high_set_ts) {
            (Some(l), Some(h)) if l < h => Some(IbExtremeOrder::LowFirstHighLast),
            (Some(l), Some(h)) if h < l => Some(IbExtremeOrder::HighFirstLowLast),
            (Some(_), Some(_)) => Some(IbExtremeOrder::Ambiguous),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct ExtensionTracker {
    first_dir: Option<ExtensionDirection>,
    first_ts: Option<f64>,
    opposite_ts: Option<f64>,
    up_hit: bool,
    down_hit: bool,
}

impl ExtensionTracker {
    fn observe(&mut self, dir: ExtensionDirection, ts: f64) {
        match dir {
            ExtensionDirection::Up => self.up_hit = true,
            ExtensionDirection::Down => self.down_hit = true,
        }
        match self.first_dir {
            None => {
                self.first_dir = Some(dir);
                self.first_ts = Some(ts);
            }
            Some(first) if first != dir && self.opposite_ts.is_none() => {
                self.opposite_ts = Some(ts);
            }
            _ => {}
        }
    }

    fn end_state(&self) -> String {
        match (self.up_hit, self.down_hit) {
            (true, true) => "BothSides".into(),
            (true, false) => "UpOnly".into(),
            (false, true) => "DownOnly".into(),
            (false, false) => "None".into(),
        }
    }
}

type GateEntry = (
    ExtensionDirection,
    f64,
    f64,
    Option<f64>,
    String,
    Option<i32>,
);

/// Per-gate trigger state. Eligibility, arming, and consumption are independent
/// across gates — a pre-arm retest or another gate's entry never consumes this one.
#[derive(Debug, Default)]
struct GateRuntime {
    armed: bool,
    armed_ts: Option<f64>,
    entry: Option<GateEntry>,
}

impl GateRuntime {
    fn consumed(&self) -> bool {
        self.entry.is_some()
    }
}

#[derive(Debug)]
struct SessionAccum {
    session_date: String,
    contract: String,
    role: WindowRole,
    formation: IbFormationTracker,
    extensions: ExtensionTracker,
    tick_count: i64,
    material_gap_count: u32,
    max_gap_minutes: f64,
    last_ts: Option<f64>,
    /// Prior trade price for shared `crossed_level` semantics (never synthetic).
    prev_price: f64,
    path: Vec<(f64, f64)>,
    gates: BTreeMap<GateKind, GateRuntime>,
    /// First observed RTH tick that qualifies as one-sided 30m acceptance.
    /// Never a synthetic `first_extension_ts + 30m`.
    acceptance_ts_ms: Option<f64>,
    rvol_ratio_at_acceptance: Option<f64>,
    rvol_class_at_acceptance: Option<String>,
    rvol_elevated_or_high_at_acceptance: Option<bool>,
    rvol_baseline_session_dates: Vec<String>,
    rvol_baseline_count: usize,
    causal_rvol_warmup: bool,
    last_price: f64,
    session_high: f64,
    session_low: f64,
    day_type_label: String,
    first_rth_et_minutes: Option<i32>,
    first_rth_ts_ms: Option<f64>,
    last_rth_et_minutes: Option<i32>,
    last_rth_ts_ms: Option<f64>,
    /// Last observed tick strictly before 16:00 ET (trade outcome / time-exit bound).
    pre_cutoff_last_ts_ms: Option<f64>,
    /// Last observed price strictly before 16:00 ET (study close — never a post-cutoff print).
    pre_cutoff_last_price: Option<f64>,
    non_monotonic_count: u32,
    ib_high_breach_ts_ms: Option<f64>,
    ib_high_breach_et: Option<i32>,
    ib_low_breach_ts_ms: Option<f64>,
    ib_low_breach_et: Option<i32>,
}

impl SessionAccum {
    fn new(
        session_date: String,
        contract: String,
        role: WindowRole,
        baseline: &CausalRvolBaseline,
    ) -> Self {
        Self {
            session_date,
            contract,
            role,
            formation: IbFormationTracker::default(),
            extensions: ExtensionTracker::default(),
            tick_count: 0,
            material_gap_count: 0,
            max_gap_minutes: 0.0,
            last_ts: None,
            prev_price: 0.0,
            path: Vec::new(),
            gates: BTreeMap::new(),
            acceptance_ts_ms: None,
            rvol_ratio_at_acceptance: None,
            rvol_class_at_acceptance: None,
            rvol_elevated_or_high_at_acceptance: None,
            rvol_baseline_session_dates: baseline.session_dates.clone(),
            rvol_baseline_count: baseline.count,
            causal_rvol_warmup: baseline.count < CAUSAL_RVOL_BASELINE_SESSIONS,
            last_price: 0.0,
            session_high: 0.0,
            session_low: 0.0,
            day_type_label: "Unknown".into(),
            first_rth_et_minutes: None,
            first_rth_ts_ms: None,
            last_rth_et_minutes: None,
            last_rth_ts_ms: None,
            pre_cutoff_last_ts_ms: None,
            pre_cutoff_last_price: None,
            non_monotonic_count: 0,
            ib_high_breach_ts_ms: None,
            ib_high_breach_et: None,
            ib_low_breach_ts_ms: None,
            ib_low_breach_et: None,
        }
    }

    fn gate_mut(&mut self, gate: GateKind) -> &mut GateRuntime {
        self.gates.entry(gate).or_default()
    }

    /// Attribute a `MonotonicTickGuard` rejection to this session without applying price.
    fn note_monotonic_rejection(&mut self) {
        self.non_monotonic_count = self.non_monotonic_count.saturating_add(1);
    }

    fn on_tick(&mut self, tick: &ScidTick, minute: i32, et_minutes: i32, rvol: &RvolPipeline) {
        if let Some(prev) = self.last_ts {
            // Mirror guard policy: non-increasing timestamps are rejected, not processed.
            if tick.timestamp_ms <= prev {
                self.note_monotonic_rejection();
                return;
            }
            let gap = tick.timestamp_ms - prev;
            if gap > MATERIAL_RTH_GAP_MS {
                self.material_gap_count += 1;
                self.max_gap_minutes = self.max_gap_minutes.max(gap / 60_000.0);
            }
        }
        self.last_ts = Some(tick.timestamp_ms);
        self.tick_count += 1;
        self.last_price = tick.price;
        if self.first_rth_ts_ms.is_none() {
            self.first_rth_ts_ms = Some(tick.timestamp_ms);
            self.first_rth_et_minutes = Some(et_minutes);
        }
        // Coverage tracks the full observed RTH span (may include post-16:00).
        self.last_rth_ts_ms = Some(tick.timestamp_ms);
        self.last_rth_et_minutes = Some(et_minutes);
        if et_minutes < CAMPAIGN_CUTOFF_ET_MINUTES {
            self.pre_cutoff_last_ts_ms = Some(tick.timestamp_ms);
            self.pre_cutoff_last_price = Some(tick.price);
        }
        if self.session_high == 0.0 || tick.price > self.session_high {
            self.session_high = tick.price;
        }
        if self.session_low == 0.0 || tick.price < self.session_low {
            self.session_low = tick.price;
        }
        let prev_price = self.prev_price;
        self.formation
            .on_rth_tick(minute, tick.price, tick.timestamp_ms);

        // Dense path only after IB lock, and only pre-16:00 for trade outcomes.
        if self.formation.locked && et_minutes < CAMPAIGN_CUTOFF_ET_MINUTES {
            self.path.push((tick.timestamp_ms, tick.price));
        }

        let ib_high = self.formation.ib_high;
        let ib_low = self.formation.ib_low;
        if self.formation.locked && ib_high > ib_low && ib_high > 0.0 {
            // IDEA-025B descriptive IB-edge breaches (independent of 0.5x / gates).
            if tick.price > ib_high && self.ib_high_breach_ts_ms.is_none() {
                self.ib_high_breach_ts_ms = Some(tick.timestamp_ms);
                self.ib_high_breach_et = Some(et_minutes);
            }
            if tick.price < ib_low && self.ib_low_breach_ts_ms.is_none() {
                self.ib_low_breach_ts_ms = Some(tick.timestamp_ms);
                self.ib_low_breach_et = Some(et_minutes);
            }

            let ib_range = ib_high - ib_low;
            let up_level = ib_high + ib_range * IB_EXTENSION_RATIO;
            let down_level = ib_low - ib_range * IB_EXTENSION_RATIO;

            // Shared EventDetector crossed_level semantics — IB-edge breach alone
            // is not a 0.5x extension.
            if !self.extensions.up_hit && crossed_level(prev_price, tick.price, up_level).is_some()
            {
                self.extensions
                    .observe(ExtensionDirection::Up, tick.timestamp_ms);
            }
            if !self.extensions.down_hit
                && crossed_level(prev_price, tick.price, down_level).is_some()
            {
                self.extensions
                    .observe(ExtensionDirection::Down, tick.timestamp_ms);
            }

            self.maybe_record_acceptance(tick.timestamp_ms, et_minutes, rvol);
            self.update_gate_triggers(prev_price, tick.price, tick.timestamp_ms, et_minutes, rvol);
        }

        self.prev_price = tick.price;
    }

    /// Record acceptance on the first qualifying *observed* tick (not synthetic).
    /// Snapshots causal RVOL at this tick — later session volume must not rewrite it.
    fn maybe_record_acceptance(&mut self, ts: f64, et_minutes: i32, rvol: &RvolPipeline) {
        if self.acceptance_ts_ms.is_some() {
            return;
        }
        let Some(first_ts) = self.extensions.first_ts else {
            return;
        };
        if let Some(accepted) =
            observe_one_sided_acceptance_ts(first_ts, self.extensions.opposite_ts, ts, et_minutes)
        {
            self.acceptance_ts_ms = Some(accepted);
            let ratio = rvol.rvol_ratio();
            self.rvol_ratio_at_acceptance = if ratio.is_finite() { Some(ratio) } else { None };
            let class = rvol.classification();
            self.rvol_class_at_acceptance = Some(rvol_classification_label(class).to_string());
            self.rvol_elevated_or_high_at_acceptance = Some(matches!(
                class,
                RvolClassification::Elevated | RvolClassification::High
            ));
        }
    }

    fn day_type_label_now(&self) -> String {
        match self.extensions.end_state().as_str() {
            "UpOnly" | "DownOnly" => "OneSidedExtension".into(),
            "BothSides" => "BothSidesExtension".into(),
            _ => "NoExtension".into(),
        }
    }

    fn gate_eligible(
        &self,
        gate: GateKind,
        first_dir: ExtensionDirection,
        ts: f64,
        et_minutes: i32,
        rvol: &RvolPipeline,
    ) -> bool {
        if et_minutes >= CAMPAIGN_CUTOFF_ET_MINUTES {
            return false;
        }
        let Some(first_ts) = self.extensions.first_ts else {
            return false;
        };
        if ts < first_ts {
            return false;
        }
        match gate {
            GateKind::UngatedBaseline => true,
            GateKind::OneSidedAcceptance30m => {
                let elevated = matches!(
                    rvol.classification(),
                    RvolClassification::Elevated | RvolClassification::High
                );
                // Arm only after an observed acceptance tick has been recorded.
                self.acceptance_ts_ms
                    .map(|accepted| ts >= accepted)
                    .unwrap_or(false)
                    && elevated
            }
            GateKind::IbConfluence => {
                let close = self.formation.complete_price.map(|p| {
                    classify_ib_close_vs_mid(self.formation.ib_high, self.formation.ib_low, p)
                });
                let order = self.formation.extreme_order();
                let conf = match (close, order) {
                    (Some(c), Some(o)) => classify_ib_confluence(c, o),
                    _ => IbConfluence::None,
                };
                // Missing next-bar qualifier must never default true.
                let next_ok = self.formation.next_bar_inside == Some(true);
                confluence_matches_extension(conf, first_dir) && next_ok
            }
        }
    }

    fn update_gate_triggers(
        &mut self,
        prev_price: f64,
        price: f64,
        ts: f64,
        et_minutes: i32,
        rvol: &RvolPipeline,
    ) {
        let Some(first_dir) = self.extensions.first_dir else {
            return;
        };
        if let Some(opp) = self.extensions.opposite_ts {
            if opp <= ts {
                return;
            }
        }
        if et_minutes >= CAMPAIGN_CUTOFF_ET_MINUTES {
            return;
        }

        let ib_high = self.formation.ib_high;
        let ib_low = self.formation.ib_low;
        let outside = match first_dir {
            ExtensionDirection::Up => price > ib_high,
            ExtensionDirection::Down => price < ib_low,
        };
        let edge = match first_dir {
            ExtensionDirection::Up => ib_high,
            ExtensionDirection::Down => ib_low,
        };
        let edge_retest = match first_dir {
            ExtensionDirection::Up => {
                crossed_level(prev_price, price, ib_high).as_deref() == Some("from_above")
            }
            ExtensionDirection::Down => {
                crossed_level(prev_price, price, ib_low).as_deref() == Some("from_below")
            }
        };

        let day_type = self.day_type_label_now();
        self.day_type_label = day_type.clone();
        let rvol_ratio = rvol.rvol_ratio();
        let rvol_opt = if rvol_ratio.is_finite() {
            Some(rvol_ratio)
        } else {
            None
        };

        let gates = [
            GateKind::UngatedBaseline,
            GateKind::OneSidedAcceptance30m,
            GateKind::IbConfluence,
        ];
        let eligibility = [
            self.gate_eligible(gates[0], first_dir, ts, et_minutes, rvol),
            self.gate_eligible(gates[1], first_dir, ts, et_minutes, rvol),
            self.gate_eligible(gates[2], first_dir, ts, et_minutes, rvol),
        ];

        // Arm independently: only after that gate's eligibility is true and price
        // is still outside the relevant IB edge. Pre-eligibility retests are ignored.
        for (idx, gate) in gates.into_iter().enumerate() {
            let st = self.gate_mut(gate);
            if st.consumed() || st.armed {
                continue;
            }
            if eligibility[idx] && outside {
                st.armed = true;
                st.armed_ts = Some(ts);
            }
        }

        // Consume independently on the first *later* exact-edge retest.
        if !edge_retest {
            return;
        }
        for gate in gates {
            let st = self.gate_mut(gate);
            if !st.armed || st.consumed() {
                continue;
            }
            let Some(armed_ts) = st.armed_ts else {
                continue;
            };
            if ts <= armed_ts {
                continue;
            }
            st.entry = Some((
                first_dir,
                ts,
                edge, // exact IB edge fill — never the favorable tick extreme
                rvol_opt,
                day_type.clone(),
                Some(et_minutes),
            ));
        }
    }

    fn finish(mut self) -> (SessionIbFeatures, Vec<GateTradeRow>) {
        let ib_high = self.formation.ib_high;
        let ib_low = self.formation.ib_low;
        let ib_range = (ib_high - ib_low).max(0.0);
        let ib_mid = if ib_high > ib_low {
            (ib_high + ib_low) / 2.0
        } else {
            0.0
        };
        let quality = evaluate_session_quality(SessionQualityInputs {
            session_date: &self.session_date,
            tick_count: self.tick_count,
            material_gap_count: self.material_gap_count,
            max_gap_minutes: self.max_gap_minutes,
            ib_high,
            ib_low,
            first_rth_et_minutes: self.first_rth_et_minutes,
            last_rth_et_minutes: self.last_rth_et_minutes,
            rth_start_ts_ms: self.first_rth_ts_ms,
            rth_end_ts_ms: self.last_rth_ts_ms,
            non_monotonic_count: self.non_monotonic_count,
        });
        let close_vs = self
            .formation
            .complete_price
            .map(|p| classify_ib_close_vs_mid(ib_high, ib_low, p));
        let extreme_order = self.formation.extreme_order();
        let confluence = match (close_vs, extreme_order) {
            (Some(c), Some(o)) => classify_ib_confluence(c, o),
            _ => IbConfluence::None,
        };
        // Observed acceptance only — never synthesize first_ts + 30m at finalize.
        let acceptance_ts = self.acceptance_ts_ms;
        let persisted = acceptance_ts.is_some();

        // By noon: et <= 12:00. By 16:00: last minute before cutoff (15:59), never at/after 16:00.
        let breach_noon = first_ib_edge_breach_by_et(
            self.ib_high_breach_ts_ms,
            self.ib_high_breach_et,
            self.ib_low_breach_ts_ms,
            self.ib_low_breach_et,
            NOON_ET_MINUTES,
        );
        let breach_1600 = first_ib_edge_breach_by_et(
            self.ib_high_breach_ts_ms,
            self.ib_high_breach_et,
            self.ib_low_breach_ts_ms,
            self.ib_low_breach_et,
            CAMPAIGN_CUTOFF_ET_MINUTES - 1,
        );

        // Study close is the last observed pre-16:00 price — never a post-cutoff print.
        let session_close = self.pre_cutoff_last_price.unwrap_or(0.0);
        let session_high = self.session_high;
        let session_low = self.session_low;
        let end_state = self.extensions.end_state();
        self.day_type_label = match end_state.as_str() {
            "UpOnly" | "DownOnly" => "OneSidedExtension".into(),
            "BothSides" => "BothSidesExtension".into(),
            _ => "NoExtension".into(),
        };
        let continuation_path = match self.extensions.first_dir {
            Some(ExtensionDirection::Up) if session_close > ib_high => "held_above_ib".into(),
            Some(ExtensionDirection::Down) if session_close < ib_low => "held_below_ib".into(),
            Some(_) if end_state == "BothSides" => "became_two_sided".into(),
            Some(_) => "failed_to_hold_extension".into(),
            None => "no_extension".into(),
        };
        let failure_path = if end_state == "BothSides" {
            "opposite_extension".into()
        } else if matches!(
            self.extensions.first_dir,
            Some(ExtensionDirection::Up) if session_close < ib_mid
        ) || matches!(
            self.extensions.first_dir,
            Some(ExtensionDirection::Down) if session_close > ib_mid
        ) {
            "closed_against_first_extension".into()
        } else {
            "none".into()
        };

        let inferential_eligible = quality.usable && !self.causal_rvol_warmup;
        let features = SessionIbFeatures {
            session_date: self.session_date.clone(),
            contract: self.contract.clone(),
            role: self.role,
            quality,
            ib_high,
            ib_low,
            ib_range,
            ib_mid,
            ib_complete_price: self.formation.complete_price,
            ib_complete_ts_ms: self.formation.complete_ts,
            close_vs_ib_mid: close_vs,
            extreme_order,
            next_bar_open_inside_ib: self.formation.next_bar_inside,
            next_bar_open_price: self.formation.next_bar_open,
            confluence,
            first_extension_direction: self.extensions.first_dir,
            first_extension_ts_ms: self.extensions.first_ts,
            opposite_extension_ts_ms: self.extensions.opposite_ts,
            end_extension_state: end_state,
            persisted_one_sided_30m: persisted,
            acceptance_ts_ms: acceptance_ts,
            rvol_ratio_at_acceptance: self.rvol_ratio_at_acceptance,
            rvol_class_at_acceptance: self.rvol_class_at_acceptance.clone(),
            rvol_elevated_or_high_at_acceptance: self.rvol_elevated_or_high_at_acceptance,
            rvol_baseline_session_dates: self.rvol_baseline_session_dates.clone(),
            rvol_baseline_count: self.rvol_baseline_count,
            causal_rvol_warmup: self.causal_rvol_warmup,
            inferential_eligible,
            ib_breach_by_noon_direction: breach_noon.map(|(d, _)| d),
            ib_breach_by_noon_ts_ms: breach_noon.map(|(_, ts)| ts),
            ib_breach_by_1600_direction: breach_1600.map(|(d, _)| d),
            ib_breach_by_1600_ts_ms: breach_1600.map(|(_, ts)| ts),
            session_close,
            session_high,
            session_low,
            day_type: self.day_type_label.clone(),
            continuation_path,
            failure_path,
        };

        // Time exit / outcomes bound to last observed pre-16:00 tick (never synthetic close).
        let session_cutoff_ts = self
            .pre_cutoff_last_ts_ms
            .or_else(|| self.path.last().map(|(t, _)| *t))
            .unwrap_or(0.0);
        let mut trades = Vec::new();
        // Warmup sessions remain descriptive but never enter inferential/trade N.
        if features.inferential_eligible {
            for (gate, runtime) in &self.gates {
                let Some((dir, entry_ts, entry_px, rvol, day_type, minute)) = &runtime.entry else {
                    continue;
                };
                for target_r in [1.0_f64, 2.0_f64] {
                    if let Some(trade) = simulate_fixed_r_trade(
                        *dir,
                        *entry_ts,
                        *entry_px,
                        FIXED_STOP_POINTS,
                        target_r,
                        &self.path,
                        session_cutoff_ts,
                    ) {
                        trades.push(GateTradeRow {
                            gate: *gate,
                            hypothesis_id: gate.hypothesis_id().into(),
                            session_date: self.session_date.clone(),
                            contract: self.contract.clone(),
                            role: self.role,
                            target_r,
                            trade,
                            rvol_ratio_at_entry: *rvol,
                            day_type: day_type.clone(),
                            entry_minute_et: *minute,
                        });
                    }
                }
            }
        }
        (features, trades)
    }
}

fn scid_path_for(symbol: &str) -> PathBuf {
    let cfg = load_feed_config();
    PathBuf::from(cfg.sierra_data_dir.trim_end_matches(['\\', '/'])).join(format!("{symbol}.scid"))
}

type WindowReplayResult = (Vec<SessionIbFeatures>, Vec<GateTradeRow>, Vec<String>);

/// Apply a replay-path `MonotonicTickGuard` decision to the active session.
///
/// Returns `true` when the tick may be processed. On `Skip`, attributes the
/// rejection to `accum` and returns `false` without implying any price update.
fn apply_monotonic_guard_to_session(
    accum: &mut SessionAccum,
    decision: crate::feed::monotonic::MonotonicTimestampDecision,
) -> bool {
    match decision {
        crate::feed::monotonic::MonotonicTimestampDecision::Accept => true,
        crate::feed::monotonic::MonotonicTimestampDecision::Skip(_) => {
            accum.note_monotonic_rejection();
            false
        }
    }
}

fn finalize_campaign_session(
    accum: SessionAccum,
    rvol: &RvolPipeline,
    catalog: &mut Vec<DatedVolumeCurve>,
    features: &mut Vec<SessionIbFeatures>,
    trades: &mut Vec<GateTradeRow>,
) {
    let curve = rvol.current_curve();
    let root = campaign_root_symbol(&accum.contract);
    let contract = accum.contract.clone();
    let (f, t) = accum.finish();
    // Clean descriptive sessions contribute to the chronological causal catalog.
    // Warmup sessions still contribute once clean so later dates can graduate.
    if f.quality.usable {
        catalog.push(DatedVolumeCurve {
            session_date: f.session_date.clone(),
            contract,
            root_symbol: root,
            curve,
        });
    }
    features.push(f);
    trades.extend(t);
}

fn rvol_pipeline_for_baseline(baseline: &CausalRvolBaseline) -> RvolPipeline {
    let mut rvol = RvolPipeline::new();
    rvol.start_session(false);
    if !baseline.curves.is_empty() {
        rvol.load_historical_curve(&baseline.curves);
    }
    rvol
}

/// Lean SCID replay for the IB campaign.
///
/// Uses the same 0.5x IB extension contract as Layer 1, but avoids the full
/// PipelineEngine/EventDetector hot path so multi-month windows finish in
/// minutes rather than hours. RVOL baselines are causal: each session loads at
/// most the prior 20 clean root-NQ curves with dates strictly earlier than
/// itself, mutating `catalog` as clean sessions complete.
pub fn replay_contract_window(
    window: &ContractWindow,
    catalog: &mut Vec<DatedVolumeCurve>,
) -> Result<WindowReplayResult, String> {
    let path = scid_path_for(&window.contract);
    if !path.exists() {
        return Err(format!("SCID not found: {}", path.display()));
    }
    let cfg = load_feed_config();
    let reader = ScidReader::with_price_scale(&path, cfg.price_scale);
    let (start_ms, end_ms) = crate::backfill::parse_backfill_date_range(
        Some(&window.start_date),
        Some(&window.end_date),
    )
    .map_err(|e| e.to_string())?;

    let mut warnings = Vec::new();
    if let Ok(bounds) = reader.file_timestamp_bounds() {
        let (first, last) = bounds;
        if let (Some(s), Some(e), Some(f), Some(l)) = (start_ms, end_ms, first, last) {
            if l < s || f >= e {
                warnings.push(format!(
                    "scid_window_mismatch: {} file {}..{} vs window {}..{}",
                    window.contract,
                    session_date_from_timestamp_ms(f),
                    session_date_from_timestamp_ms(l),
                    window.start_date,
                    window.end_date
                ));
            }
        }
    }

    let root = campaign_root_symbol(&window.contract);
    let mut rvol = RvolPipeline::new();
    rvol.start_session(false);

    let mut current_date = String::new();
    let mut in_rth = false;
    let mut accum: Option<SessionAccum> = None;
    let mut features = Vec::new();
    let mut trades = Vec::new();
    let mut monotonic = crate::feed::monotonic::MonotonicTickGuard::default();
    let mut records = 0usize;
    let mut last_progress = 0usize;

    reader
        .scan_range_in_file_order(start_ms, end_ms, |tick| {
            records += 1;
            if records - last_progress >= 2_000_000 {
                last_progress = records;
                eprintln!(
                    "  … scanned {}M records (sessions so far {})",
                    records / 1_000_000,
                    features.len()
                );
            }
            let decision = monotonic.observe(tick.timestamp_ms);
            let Some(ctx) = tick_time_context_from_timestamp_ms(tick.timestamp_ms) else {
                return Ok(ScanControl::Continue);
            };

            let is_rth = ctx.session_type == SessionType::Rth;
            if is_rth {
                if !in_rth || ctx.session_date != current_date {
                    if let Some(prev) = accum.take() {
                        finalize_campaign_session(prev, &rvol, catalog, &mut features, &mut trades);
                    }
                    current_date = ctx.session_date.clone();
                    in_rth = true;
                    let baseline = select_causal_rvol_baseline(
                        catalog,
                        &current_date,
                        &root,
                        CAUSAL_RVOL_BASELINE_SESSIONS,
                    );
                    rvol = rvol_pipeline_for_baseline(&baseline);
                    accum = Some(SessionAccum::new(
                        current_date.clone(),
                        window.contract.clone(),
                        window.role,
                        &baseline,
                    ));
                }

                // Attribute guard rejects to this session; never process the rejected price.
                if let Some(a) = accum.as_mut() {
                    if !apply_monotonic_guard_to_session(a, decision) {
                        return Ok(ScanControl::Continue);
                    }
                } else if !matches!(
                    decision,
                    crate::feed::monotonic::MonotonicTimestampDecision::Accept
                ) {
                    return Ok(ScanControl::Continue);
                }

                rvol.on_trade(tick.volume, ctx.minute_of_session);
                if let Some(a) = accum.as_mut() {
                    a.on_tick(&tick, ctx.minute_of_session, ctx.et_minutes, &rvol);
                }
            } else if in_rth {
                if let Some(prev) = accum.take() {
                    finalize_campaign_session(prev, &rvol, catalog, &mut features, &mut trades);
                }
                in_rth = false;
            }

            Ok(ScanControl::Continue)
        })
        .map_err(|e| e.to_string())?;

    if let Some(prev) = accum.take() {
        finalize_campaign_session(prev, &rvol, catalog, &mut features, &mut trades);
    }

    Ok((features, trades, warnings))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescriptiveSummary {
    pub population_sessions: usize,
    pub usable_sessions: usize,
    pub excluded_sessions: usize,
    pub exclusion_counts: BTreeMap<String, usize>,
    /// True when usable + excluded == population and reason hits cover exclusions.
    pub rejection_counts_reconciled: bool,
    pub extension_state_counts: BTreeMap<String, usize>,
    pub one_sided_sessions: usize,
    pub both_sided_sessions: usize,
    pub persisted_one_sided_30m: usize,
    pub persisted_one_sided_30m_reliability: ReliabilityTier,
    pub confluence_bullish: usize,
    pub confluence_bearish: usize,
    pub next_bar_open_inside_rate: Option<f64>,
    pub next_bar_open_inside_n: usize,
    pub next_bar_open_inside_reliability: ReliabilityTier,
    pub breach_by_noon_up: usize,
    pub breach_by_noon_down: usize,
    pub breach_by_noon_n: usize,
    pub breach_by_noon_reliability: ReliabilityTier,
    pub breach_by_1600_up: usize,
    pub breach_by_1600_down: usize,
    pub breach_by_1600_n: usize,
    pub breach_by_1600_reliability: ReliabilityTier,
    pub continuation_path_counts: BTreeMap<String, usize>,
    pub failure_path_counts: BTreeMap<String, usize>,
    /// Overall usable-session tier (not a substitute for metric-specific tiers).
    pub reliability_tier: ReliabilityTier,
}

pub fn summarize_descriptive(sessions: &[SessionIbFeatures]) -> DescriptiveSummary {
    let usable: Vec<_> = sessions.iter().filter(|s| s.quality.usable).collect();
    let mut exclusion_counts = BTreeMap::new();
    for s in sessions {
        for r in &s.quality.exclusion_reasons {
            let key = r.split(':').next().unwrap_or(r);
            let key = key.split('(').next().unwrap_or(key).to_string();
            *exclusion_counts.entry(key).or_default() += 1;
        }
    }
    let population = sessions.len();
    let usable_n = usable.len();
    let excluded = population.saturating_sub(usable_n);
    let rejection_counts_reconciled =
        reconcile_rejection_counts(population, usable_n, excluded, &exclusion_counts);

    let mut extension_state_counts = BTreeMap::new();
    let mut continuation_path_counts = BTreeMap::new();
    let mut failure_path_counts = BTreeMap::new();
    let mut one_sided = 0usize;
    let mut both = 0usize;
    let mut persisted = 0usize;
    let mut bull = 0usize;
    let mut bear = 0usize;
    let mut inside = 0usize;
    let mut inside_n = 0usize;
    let mut noon_up = 0usize;
    let mut noon_down = 0usize;
    let mut close_up = 0usize;
    let mut close_down = 0usize;
    for s in &usable {
        *extension_state_counts
            .entry(s.end_extension_state.clone())
            .or_default() += 1;
        *continuation_path_counts
            .entry(s.continuation_path.clone())
            .or_default() += 1;
        *failure_path_counts
            .entry(s.failure_path.clone())
            .or_default() += 1;
        match s.end_extension_state.as_str() {
            "UpOnly" | "DownOnly" => one_sided += 1,
            "BothSides" => both += 1,
            _ => {}
        }
        if s.persisted_one_sided_30m {
            persisted += 1;
        }
        match s.confluence {
            IbConfluence::Bullish => bull += 1,
            IbConfluence::Bearish => bear += 1,
            IbConfluence::None => {}
        }
        if let Some(v) = s.next_bar_open_inside_ib {
            inside_n += 1;
            if v {
                inside += 1;
            }
        }
        match s.ib_breach_by_noon_direction {
            Some(ExtensionDirection::Up) => noon_up += 1,
            Some(ExtensionDirection::Down) => noon_down += 1,
            None => {}
        }
        match s.ib_breach_by_1600_direction {
            Some(ExtensionDirection::Up) => close_up += 1,
            Some(ExtensionDirection::Down) => close_down += 1,
            None => {}
        }
    }
    // Metric-specific denominators (never substitute generic usable N).
    let breach_noon_n = usable_n; // observed among coverage-qualified usable sessions
    let breach_1600_n = usable_n;
    DescriptiveSummary {
        population_sessions: population,
        usable_sessions: usable_n,
        excluded_sessions: excluded,
        exclusion_counts,
        rejection_counts_reconciled,
        extension_state_counts,
        one_sided_sessions: one_sided,
        both_sided_sessions: both,
        persisted_one_sided_30m: persisted,
        persisted_one_sided_30m_reliability: reliability_tier(one_sided),
        confluence_bullish: bull,
        confluence_bearish: bear,
        next_bar_open_inside_rate: if inside_n == 0 {
            None
        } else {
            Some(inside as f64 / inside_n as f64)
        },
        next_bar_open_inside_n: inside_n,
        next_bar_open_inside_reliability: reliability_tier(inside_n),
        breach_by_noon_up: noon_up,
        breach_by_noon_down: noon_down,
        breach_by_noon_n: breach_noon_n,
        breach_by_noon_reliability: reliability_tier(breach_noon_n),
        breach_by_1600_up: close_up,
        breach_by_1600_down: close_down,
        breach_by_1600_n: breach_1600_n,
        breach_by_1600_reliability: reliability_tier(breach_1600_n),
        continuation_path_counts,
        failure_path_counts,
        reliability_tier: reliability_tier(usable_n),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSha256 {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignProvenance {
    pub git_commit: String,
    pub git_dirty: bool,
    pub cargo_version: String,
    pub rules_schema_version: u32,
    pub study_id: String,
    pub study_version: i64,
    pub v2_root: String,
    pub isolated_db_path: Option<String>,
    pub canonical_isolated_db_path: Option<String>,
    pub artifact_dir: String,
    pub canonical_artifact_dir: String,
    pub fixed_stop_points: f64,
    pub persistence_ms: f64,
    pub ib_extension_ratio: f64,
    pub min_rth_ticks: i64,
    pub windows: Vec<ContractWindow>,
    pub excluded_session_dates: Vec<String>,
    pub transition_exclusions: Vec<String>,
    pub commands: Vec<String>,
    pub source_file_hashes: Vec<FileSha256>,
    pub binary_path: Option<String>,
    pub binary_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignReport {
    pub provenance: CampaignProvenance,
    pub warnings: Vec<String>,
    pub descriptive_all: DescriptiveSummary,
    pub descriptive_discovery: DescriptiveSummary,
    /// Reserved for a future untouched holdout; empty while all windows are development.
    pub descriptive_holdout: DescriptiveSummary,
    pub tradable: BTreeMap<String, BTreeMap<String, OutcomeStats>>,
    pub primary_contrast: PrimaryContrastReport,
    pub verdict: CampaignVerdict,
    /// Flattened decision label for legacy readers (`DEVELOPMENT_RESULT`).
    pub decision: String,
    pub decision_rationale: String,
    pub refinement_proposal: Option<String>,
}

/// One calendar date of overlapping front/back RTH volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DailyOverlapVolume {
    pub session_date: String,
    pub front_contract: String,
    pub front_rth_volume: f64,
    pub back_contract: String,
    pub back_rth_volume: f64,
}

/// Derived boundary for one contract transition (e.g. H6→M6).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RolloverTransition {
    pub from_contract: String,
    pub to_contract: String,
    pub crossover_rule: String,
    pub crossover_date: Option<String>,
    pub front_last_included_date: Option<String>,
    pub back_first_included_date: Option<String>,
    pub transition_exclusions: Vec<String>,
    pub raw_daily_volumes: Vec<DailyOverlapVolume>,
}

/// Machine-readable rollover evidence artifact (`rollover_evidence.json`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RolloverEvidence {
    pub study_version: i64,
    pub development_end_date: String,
    pub crossover_rule: String,
    pub crossover_rule_documentation: String,
    /// `local_scid_overlap_scan` (production default) or `fixture`.
    pub volume_source: String,
    pub overlap_scan_windows: Vec<OverlapScanWindow>,
    pub transitions: Vec<RolloverTransition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OverlapScanWindow {
    pub front_contract: String,
    pub back_contract: String,
    pub start_date: String,
    pub end_date: String,
}

/// Result of preparing a V2 run directory without replaying the full study.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareV2Result {
    pub canonical_run_dir: String,
    pub canonical_db_path: String,
    pub provenance: CampaignProvenance,
    pub rollover: RolloverEvidence,
    /// Crossover-derived development windows (same set eventual replay must use).
    pub windows: Vec<ContractWindow>,
}

/// Strip Windows `\\?\` verbatim prefix for stable path comparisons.
pub fn strip_verbatim_prefix(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else if let Some(rest) = s.strip_prefix("//?/") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    }
}

fn path_cmp_key(path: &Path) -> String {
    let mut s = strip_verbatim_prefix(path)
        .to_string_lossy()
        .replace('/', "\\");
    while s.ends_with('\\') && s.len() > 3 {
        s.pop();
    }
    if cfg!(windows) {
        s.to_ascii_lowercase()
    } else {
        s
    }
}

/// True when `child` resolves strictly beneath `parent` (not equal).
pub fn is_strictly_beneath(child: &Path, parent: &Path) -> bool {
    let c = path_cmp_key(child);
    let p = path_cmp_key(parent);
    if c == p {
        return false;
    }
    let prefix = format!("{p}\\");
    c.starts_with(&prefix)
}

/// Refuse live/production DB paths (read or write).
pub fn is_forbidden_live_db_path(path: &Path) -> bool {
    let key = path_cmp_key(path);
    FORBIDDEN_LIVE_DB_PATHS
        .iter()
        .any(|live| key == path_cmp_key(Path::new(live)))
}

/// Canonicalize an existing directory, creating it first when requested.
pub fn ensure_existing_dir_canonical(path: &Path) -> Result<PathBuf, String> {
    std::fs::create_dir_all(path).map_err(|e| format!("create_dir_all {}: {e}", path.display()))?;
    let canon =
        std::fs::canonicalize(path).map_err(|e| format!("canonicalize {}: {e}", path.display()))?;
    Ok(strip_verbatim_prefix(&canon))
}

fn longest_existing_ancestor(path: &Path) -> PathBuf {
    let mut cur = path.to_path_buf();
    loop {
        if cur.exists() {
            return cur;
        }
        if !cur.pop() {
            return path
                .components()
                .next()
                .map(|c| PathBuf::from(c.as_os_str()))
                .unwrap_or_else(|| PathBuf::from("."));
        }
    }
}

fn join_under_normalized(base: &Path, relative: &Path) -> Result<PathBuf, String> {
    let mut out = base.to_path_buf();
    for comp in relative.components() {
        match comp {
            Component::Normal(seg) => out.push(seg),
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return Err("path traversal escapes filesystem root".into());
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err("refusing absolute path components in relative join".into());
            }
        }
    }
    Ok(out)
}

/// Resolve `candidate` against an existing ancestor, then require it strictly beneath `root`.
///
/// Rejects equal-root, outside paths, `..` traversal that escapes the root, and
/// junction/symlink escapes (via post-canonicalize comparison when the path exists).
pub fn resolve_path_strictly_beneath(candidate: &Path, root: &Path) -> Result<PathBuf, String> {
    let root_canon = ensure_existing_dir_canonical(root)?;
    if candidate.as_os_str().is_empty() {
        return Err("empty path".into());
    }

    let ancestor = longest_existing_ancestor(candidate);
    let ancestor_canon = std::fs::canonicalize(&ancestor)
        .map_err(|e| format!("canonicalize ancestor {}: {e}", ancestor.display()))?;
    let ancestor_canon = strip_verbatim_prefix(&ancestor_canon);

    let rest = candidate.strip_prefix(&ancestor).unwrap_or(candidate);
    let mut resolved = join_under_normalized(&ancestor_canon, rest)?;
    resolved = strip_verbatim_prefix(&resolved);

    // If the target already exists, re-canonicalize to catch junction/symlink escapes.
    if resolved.exists() {
        let existing = std::fs::canonicalize(&resolved)
            .map_err(|e| format!("canonicalize existing {}: {e}", resolved.display()))?;
        resolved = strip_verbatim_prefix(&existing);
    }

    if path_cmp_key(&resolved) == path_cmp_key(&root_canon) {
        return Err(format!(
            "refusing equal-root path (must be strictly beneath {}): {}",
            root_canon.display(),
            resolved.display()
        ));
    }
    if !is_strictly_beneath(&resolved, &root_canon) {
        return Err(format!(
            "path escapes v2 root {}: {}",
            root_canon.display(),
            resolved.display()
        ));
    }
    if is_forbidden_live_db_path(&resolved) {
        return Err(format!(
            "refusing forbidden live DB path: {}",
            resolved.display()
        ));
    }
    Ok(resolved)
}

fn run_owned_artifact_path(run_dir: &Path, name: &str) -> Result<PathBuf, String> {
    if name.contains(['/', '\\']) || name == ".." || name == "." || name.is_empty() {
        return Err(format!("invalid artifact basename: {name}"));
    }
    let p = run_dir.join(name);
    if p.parent().map(path_cmp_key) != Some(path_cmp_key(run_dir)) {
        return Err(format!("artifact parent mismatch: {}", p.display()));
    }
    Ok(p)
}

/// Delete a single validated run-owned artifact path. Never deletes outside `run_dir`.
pub fn delete_run_owned_target(run_dir: &Path, target: &Path) -> Result<(), String> {
    let run_key = path_cmp_key(run_dir);
    let target_key = path_cmp_key(target);
    if target_key == run_key {
        return Err("refusing to delete the run directory itself via artifact helper".into());
    }
    if !is_strictly_beneath(target, run_dir) {
        return Err(format!(
            "refusing delete outside run dir {}: {}",
            run_dir.display(),
            target.display()
        ));
    }
    if !target.exists() {
        return Ok(());
    }
    if target.is_dir() {
        std::fs::remove_dir_all(target).map_err(|e| e.to_string())?;
    } else {
        std::fs::remove_file(target).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Prepare a run directory under `v2_root`. Existing artifacts require `--overwrite`.
pub fn prepare_run_directory(
    run_dir: &Path,
    v2_root: &Path,
    overwrite: bool,
) -> Result<PathBuf, String> {
    let resolved = resolve_path_strictly_beneath(run_dir, v2_root)?;
    if resolved.exists() {
        let mut existing = Vec::new();
        for name in V2_RUN_ARTIFACT_NAMES {
            let p = run_owned_artifact_path(&resolved, name)?;
            if p.exists() {
                existing.push(p);
            }
        }
        if !existing.is_empty() {
            if !overwrite {
                return Err(format!(
                    "refusing existing run artifacts without --overwrite: {}",
                    existing
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            for p in &existing {
                delete_run_owned_target(&resolved, p)?;
            }
        }
    } else {
        std::fs::create_dir_all(&resolved)
            .map_err(|e| format!("create run dir {}: {e}", resolved.display()))?;
        // Re-resolve after create so junction escapes are caught.
        let again = resolve_path_strictly_beneath(&resolved, v2_root)?;
        return Ok(again);
    }
    let canon = std::fs::canonicalize(&resolved)
        .map_err(|e| format!("canonicalize run dir {}: {e}", resolved.display()))?;
    let canon = strip_verbatim_prefix(&canon);
    if !is_strictly_beneath(&canon, &ensure_existing_dir_canonical(v2_root)?) {
        return Err(format!(
            "run dir escaped v2 root after create: {}",
            canon.display()
        ));
    }
    Ok(canon)
}

/// Create a fresh empty isolated campaign DB strictly under the run directory.
pub fn create_fresh_isolated_campaign_db(
    run_dir: &Path,
    db_filename: &str,
    overwrite: bool,
) -> Result<PathBuf, String> {
    let db_path = run_owned_artifact_path(run_dir, db_filename)?;
    if is_forbidden_live_db_path(&db_path) {
        return Err(format!(
            "refusing forbidden live DB path: {}",
            db_path.display()
        ));
    }
    if db_path.exists() {
        if !overwrite {
            return Err(format!(
                "refusing existing DB without --overwrite: {}",
                db_path.display()
            ));
        }
        delete_run_owned_target(run_dir, &db_path)?;
    }
    let path_str = db_path
        .to_str()
        .ok_or_else(|| format!("non-utf8 db path: {}", db_path.display()))?;
    Database::open(path_str).map_err(|e| format!("create isolated DB: {e}"))?;
    let canon = if db_path.exists() {
        strip_verbatim_prefix(
            &std::fs::canonicalize(&db_path)
                .map_err(|e| format!("canonicalize db {}: {e}", db_path.display()))?,
        )
    } else {
        db_path.clone()
    };
    if !is_strictly_beneath(&canon, run_dir) {
        // Best-effort cleanup if somehow misplaced.
        let _ = std::fs::remove_file(&canon);
        return Err(format!(
            "isolated DB not under run dir after create: {}",
            canon.display()
        ));
    }
    Ok(canon)
}

/// SHA-256 hex digest of a file's bytes.
pub fn sha256_file(path: &Path) -> Result<String, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Campaign-owned source files hashed into provenance.
pub fn campaign_source_file_paths() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    vec![
        root.join("src").join("research").join("ib_campaign.rs"),
        root.join("src").join("bin").join("the-desk-ib-campaign.rs"),
    ]
}

/// Hash every campaign source file; paths recorded in canonical form when possible.
pub fn hash_campaign_sources() -> Result<Vec<FileSha256>, String> {
    let mut out = Vec::new();
    for path in campaign_source_file_paths() {
        if !path.exists() {
            return Err(format!("missing campaign source: {}", path.display()));
        }
        let canon = std::fs::canonicalize(&path)
            .map(|p| strip_verbatim_prefix(&p))
            .unwrap_or_else(|_| path.clone());
        out.push(FileSha256 {
            path: canon.display().to_string(),
            sha256: sha256_file(&path)?,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Hash the currently executing binary when available.
pub fn hash_current_binary() -> Result<(String, String), String> {
    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let canon = std::fs::canonicalize(&exe)
        .map(|p| strip_verbatim_prefix(&p))
        .unwrap_or_else(|_| exe.clone());
    let digest = sha256_file(&exe)?;
    Ok((canon.display().to_string(), digest))
}

/// Apply the documented volume-crossover rule to overlapping daily RTH volumes.
pub fn select_volume_crossover(
    front_contract: &str,
    back_contract: &str,
    daily: &[DailyOverlapVolume],
) -> RolloverTransition {
    let mut rows: Vec<DailyOverlapVolume> = daily
        .iter()
        .filter(|r| r.front_rth_volume > 0.0 && r.back_rth_volume > 0.0)
        .cloned()
        .collect();
    rows.sort_by(|a, b| a.session_date.cmp(&b.session_date));

    let crossover_date = rows
        .iter()
        .find(|r| r.back_rth_volume > r.front_rth_volume)
        .map(|r| r.session_date.clone());

    let (front_last_included_date, back_first_included_date, transition_exclusions) =
        match &crossover_date {
            Some(cross) => {
                // Last mutual-overlap date before crossover where front strictly leads.
                let front_last = rows
                    .iter()
                    .rev()
                    .find(|r| {
                        r.session_date.as_str() < cross.as_str()
                            && r.front_rth_volume > r.back_rth_volume
                    })
                    .map(|r| r.session_date.clone());
                let exclusions: Vec<String> = rows
                    .iter()
                    .map(|r| r.session_date.clone())
                    .filter(|d| match &front_last {
                        Some(fl) => d.as_str() > fl.as_str() && d.as_str() < cross.as_str(),
                        None => d.as_str() < cross.as_str(),
                    })
                    .collect();
                (front_last, Some(cross.clone()), exclusions)
            }
            None => {
                let exclusions = rows.iter().map(|r| r.session_date.clone()).collect();
                (None, None, exclusions)
            }
        };

    RolloverTransition {
        from_contract: front_contract.to_string(),
        to_contract: back_contract.to_string(),
        crossover_rule: ROLLOVER_CROSSOVER_RULE.to_string(),
        crossover_date,
        front_last_included_date,
        back_first_included_date,
        transition_exclusions,
        raw_daily_volumes: rows,
    }
}

/// Campaign contract pairs whose overlapping RTH volume defines roll boundaries.
pub fn campaign_rollover_pairs() -> Vec<(&'static str, &'static str)> {
    vec![("NQH6.CME", "NQM6.CME"), ("NQM6.CME", "NQU6.CME")]
}

/// Bounded overlap scan calendars for each roll pair.
pub fn campaign_overlap_scan_windows() -> Vec<OverlapScanWindow> {
    vec![
        OverlapScanWindow {
            front_contract: "NQH6.CME".into(),
            back_contract: "NQM6.CME".into(),
            start_date: H6_M6_OVERLAP_SCAN_START.into(),
            end_date: H6_M6_OVERLAP_SCAN_END.into(),
        },
        OverlapScanWindow {
            front_contract: "NQM6.CME".into(),
            back_contract: "NQU6.CME".into(),
            start_date: M6_U6_OVERLAP_SCAN_START.into(),
            end_date: M6_U6_OVERLAP_SCAN_END.into(),
        },
    ]
}

/// Production vs test volume input. Default production path is local SCID scan.
#[derive(Debug, Clone, Copy)]
pub enum RolloverVolumeSource<'a> {
    /// Bounded local SCID RTH volume aggregation (production default).
    ScanLocalScid,
    /// Explicit fixture for unit tests only — never the production default.
    Fixture(&'a BTreeMap<(String, String), Vec<DailyOverlapVolume>>),
    /// Already-collected map (exactly one prior scan). Label records provenance.
    Precollected {
        volumes: &'a BTreeMap<(String, String), Vec<DailyOverlapVolume>>,
        label: &'a str,
    },
}

impl RolloverVolumeSource<'_> {
    pub fn source_label(self) -> String {
        match self {
            Self::ScanLocalScid => "local_scid_overlap_scan".into(),
            Self::Fixture(_) => "fixture".into(),
            Self::Precollected { label, .. } => label.to_string(),
        }
    }

    pub fn is_production_default(self) -> bool {
        matches!(self, Self::ScanLocalScid)
    }
}

/// Aggregate observed RTH trade volume by session_date from a local SCID file.
pub fn aggregate_daily_rth_volume_from_scid(
    contract: &str,
    start_date: &str,
    end_date: &str,
) -> Result<BTreeMap<String, f64>, String> {
    let path = scid_path_for(contract);
    if !path.exists() {
        return Err(format!(
            "BLOCKED: SCID not found for overlap scan: {}",
            path.display()
        ));
    }
    let cfg = load_feed_config();
    let reader = ScidReader::with_price_scale(&path, cfg.price_scale);
    let (start_ms, end_ms) =
        crate::backfill::parse_backfill_date_range(Some(start_date), Some(end_date))
            .map_err(|e| e.to_string())?;

    let mut by_date: BTreeMap<String, f64> = BTreeMap::new();
    // Use timestamp-bounded `scan_range` (binary-searched offsets) — not full-file
    // `scan_range_in_file_order` — so overlap windows stay light.
    reader
        .scan_range(start_ms, end_ms, |tick| {
            let Some(ctx) = tick_time_context_from_timestamp_ms(tick.timestamp_ms) else {
                return Ok(ScanControl::Continue);
            };
            if ctx.session_type != SessionType::Rth {
                return Ok(ScanControl::Continue);
            }
            if tick.volume > 0.0 {
                *by_date.entry(ctx.session_date).or_default() += tick.volume;
            }
            Ok(ScanControl::Continue)
        })
        .map_err(|e| format!("SCID scan {contract}: {e}"))?;
    Ok(by_date)
}

/// Keep only dates where both contracts printed positive RTH volume.
pub fn merge_mutual_overlap_volumes(
    front_contract: &str,
    back_contract: &str,
    front_vols: &BTreeMap<String, f64>,
    back_vols: &BTreeMap<String, f64>,
) -> Vec<DailyOverlapVolume> {
    let mut out = Vec::new();
    for (date, &front_rth_volume) in front_vols {
        if front_rth_volume <= 0.0 {
            continue;
        }
        let Some(&back_rth_volume) = back_vols.get(date) else {
            continue;
        };
        if back_rth_volume <= 0.0 {
            continue;
        }
        out.push(DailyOverlapVolume {
            session_date: date.clone(),
            front_contract: front_contract.to_string(),
            front_rth_volume,
            back_contract: back_contract.to_string(),
            back_rth_volume,
        });
    }
    out.sort_by(|a, b| a.session_date.cmp(&b.session_date));
    out
}

/// Production default: scan both roll pairs over bounded overlap calendars.
pub fn scan_campaign_rollover_volumes_from_scid() -> Result<RolloverVolumeMap, String> {
    let mut out = BTreeMap::new();
    for w in campaign_overlap_scan_windows() {
        let front_vols =
            aggregate_daily_rth_volume_from_scid(&w.front_contract, &w.start_date, &w.end_date)?;
        let back_vols =
            aggregate_daily_rth_volume_from_scid(&w.back_contract, &w.start_date, &w.end_date)?;
        let rows = merge_mutual_overlap_volumes(
            &w.front_contract,
            &w.back_contract,
            &front_vols,
            &back_vols,
        );
        if rows.is_empty() {
            return Err(format!(
                "BLOCKED: no mutual RTH overlap volume for {}→{} in {}..{}",
                w.front_contract, w.back_contract, w.start_date, w.end_date
            ));
        }
        out.insert((w.front_contract.clone(), w.back_contract.clone()), rows);
    }
    Ok(out)
}

/// Per front→back pair: mutual-overlap daily RTH volume rows.
pub type RolloverVolumeMap = BTreeMap<(String, String), Vec<DailyOverlapVolume>>;

fn validate_rollover_volume_map(map: &RolloverVolumeMap) -> Result<(), String> {
    if map.is_empty() {
        return Err("BLOCKED: empty rollover volume map (fixture/missing evidence)".into());
    }
    for &(front, back) in &campaign_rollover_pairs() {
        let key = (front.to_string(), back.to_string());
        match map.get(&key) {
            None => {
                return Err(format!(
                    "BLOCKED: missing rollover volume evidence for {front}→{back}"
                ));
            }
            Some(rows) if rows.is_empty() => {
                return Err(format!(
                    "BLOCKED: empty rollover volume evidence for {front}→{back}"
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn resolve_rollover_volumes(
    source: RolloverVolumeSource<'_>,
) -> Result<(RolloverVolumeMap, String), String> {
    match source {
        RolloverVolumeSource::ScanLocalScid => {
            let vols = scan_campaign_rollover_volumes_from_scid()?;
            Ok((vols, source.source_label()))
        }
        RolloverVolumeSource::Fixture(map) => {
            validate_rollover_volume_map(map)?;
            Ok((map.clone(), source.source_label()))
        }
        RolloverVolumeSource::Precollected { volumes, label } => {
            validate_rollover_volume_map(volumes)?;
            Ok((volumes.clone(), label.to_string()))
        }
    }
}

/// Build rollover evidence shells from daily overlap rows (does not yet require resolution).
pub fn build_rollover_evidence(
    volumes_by_pair: &BTreeMap<(String, String), Vec<DailyOverlapVolume>>,
    volume_source: &str,
) -> RolloverEvidence {
    let mut transitions = Vec::new();
    for &(front, back) in &campaign_rollover_pairs() {
        let key = (front.to_string(), back.to_string());
        let daily = volumes_by_pair.get(&key).cloned().unwrap_or_default();
        transitions.push(select_volume_crossover(front, back, &daily));
    }
    RolloverEvidence {
        study_version: 2,
        development_end_date: DEVELOPMENT_END_DATE.to_string(),
        crossover_rule: ROLLOVER_CROSSOVER_RULE.to_string(),
        crossover_rule_documentation: format!(
            "For each front→back pair, scan bounded local SCID overlap windows and retain raw \
             per-date RTH volumes on mutual-overlap dates (both volumes > 0). \
             {ROLLOVER_CROSSOVER_RULE}: the first ISO date where back_rth_volume > front_rth_volume \
             is the back_first_included_date / crossover; the last prior mutual-overlap date where \
             front_rth_volume > back_rth_volume is front_last_included_date. transition_exclusions \
             list only observed mutual-overlap session dates strictly between front_last and \
             crossover (e.g. volume ties). Weekends/holidays with no mutual-overlap observation \
             are never invented. Both H6→M6 and M6→U6 crossovers must resolve or the run is BLOCKED. \
             All dates through {DEVELOPMENT_END_DATE} remain development."
        ),
        volume_source: volume_source.to_string(),
        overlap_scan_windows: campaign_overlap_scan_windows(),
        transitions,
    }
}

/// Fail closed unless both H6→M6 and M6→U6 crossovers fully resolve.
pub fn require_resolved_rollover_crossovers(evidence: &RolloverEvidence) -> Result<(), String> {
    if evidence.transitions.len() != campaign_rollover_pairs().len() {
        return Err(format!(
            "BLOCKED: expected {} rollover transitions, found {}",
            campaign_rollover_pairs().len(),
            evidence.transitions.len()
        ));
    }
    for (i, &(front, back)) in campaign_rollover_pairs().iter().enumerate() {
        let t = &evidence.transitions[i];
        if t.from_contract != front || t.to_contract != back {
            return Err(format!(
                "BLOCKED: transition[{i}] expected {front}→{back}, got {}→{}",
                t.from_contract, t.to_contract
            ));
        }
        if t.raw_daily_volumes.is_empty() {
            return Err(format!(
                "BLOCKED: no raw mutual-overlap volumes for {front}→{back}"
            ));
        }
        if t.crossover_date.is_none()
            || t.front_last_included_date.is_none()
            || t.back_first_included_date.is_none()
        {
            return Err(format!(
                "BLOCKED: unresolved volume crossover for {front}→{back} \
                 (need front_last_included_date and crossover/back_first_included_date)"
            ));
        }
    }
    Ok(())
}

/// Derive development ContractWindows from resolved crossovers. U6 ends at 2026-07-21.
pub fn derive_campaign_windows_from_rollover(
    evidence: &RolloverEvidence,
) -> Result<Vec<ContractWindow>, String> {
    require_resolved_rollover_crossovers(evidence)?;
    let h6m6 = &evidence.transitions[0];
    let m6u6 = &evidence.transitions[1];
    let h6_end = h6m6.front_last_included_date.clone().expect("checked");
    let m6_start = h6m6.back_first_included_date.clone().expect("checked");
    let m6_end = m6u6.front_last_included_date.clone().expect("checked");
    let u6_start = m6u6.back_first_included_date.clone().expect("checked");

    if h6_end.as_str() < CAMPAIGN_H6_START_DATE {
        return Err(format!(
            "BLOCKED: H6 front_last {h6_end} precedes campaign start {CAMPAIGN_H6_START_DATE}"
        ));
    }
    if m6_start.as_str() > m6_end.as_str() {
        return Err(format!(
            "BLOCKED: M6 window inverted ({m6_start}..{m6_end})"
        ));
    }
    if u6_start.as_str() > DEVELOPMENT_END_DATE {
        return Err(format!(
            "BLOCKED: U6 start {u6_start} after development end {DEVELOPMENT_END_DATE}"
        ));
    }

    Ok(vec![
        ContractWindow {
            contract: "NQH6.CME".into(),
            start_date: CAMPAIGN_H6_START_DATE.into(),
            end_date: h6_end,
            role: WindowRole::Discovery,
            rationale: format!(
                "H6 development front from seed start through H6→M6 front_last (crossover {}); development only.",
                h6m6.crossover_date.as_deref().unwrap_or("?")
            ),
        },
        ContractWindow {
            contract: "NQM6.CME".into(),
            start_date: m6_start,
            end_date: m6_end,
            role: WindowRole::Discovery,
            rationale: format!(
                "M6 development front from H6→M6 back_first through M6→U6 front_last (crossover {}); development only.",
                m6u6.crossover_date.as_deref().unwrap_or("?")
            ),
        },
        ContractWindow {
            contract: "NQU6.CME".into(),
            start_date: u6_start,
            end_date: DEVELOPMENT_END_DATE.into(),
            role: WindowRole::Discovery,
            rationale: format!(
                "U6 development front from M6→U6 back_first through {DEVELOPMENT_END_DATE}; development only."
            ),
        },
    ])
}

/// Build evidence + derived windows from an already-collected volume map (no SCID I/O).
pub fn resolve_rollover_and_windows_from_volumes(
    volumes: &BTreeMap<(String, String), Vec<DailyOverlapVolume>>,
    volume_source_label: &str,
) -> Result<(RolloverEvidence, Vec<ContractWindow>), String> {
    validate_rollover_volume_map(volumes)?;
    let evidence = build_rollover_evidence(volumes, volume_source_label);
    require_resolved_rollover_crossovers(&evidence)?;
    let windows = derive_campaign_windows_from_rollover(&evidence)?;
    Ok((evidence, windows))
}

/// Resolve volumes → evidence → require crossovers → derive windows.
///
/// `ScanLocalScid` performs exactly one bounded overlap scan for the two pairs.
pub fn resolve_rollover_and_windows(
    source: RolloverVolumeSource<'_>,
) -> Result<(RolloverEvidence, Vec<ContractWindow>), String> {
    let (volumes, label) = resolve_rollover_volumes(source)?;
    resolve_rollover_and_windows_from_volumes(&volumes, &label)
}

fn collect_transition_exclusions(evidence: &RolloverEvidence) -> Vec<String> {
    let mut set = BTreeMap::new();
    for t in &evidence.transitions {
        for d in &t.transition_exclusions {
            set.insert(d.clone(), ());
        }
    }
    set.into_keys().collect()
}

fn build_provenance(
    git_commit: &str,
    git_dirty: bool,
    artifact_dir: &Path,
    isolated_db: Option<&Path>,
    windows: Vec<ContractWindow>,
    transition_exclusions: Vec<String>,
    commands: Vec<String>,
) -> Result<CampaignProvenance, String> {
    let sources = hash_campaign_sources()?;
    let (binary_path, binary_sha256) = match hash_current_binary() {
        Ok((p, h)) => (Some(p), Some(h)),
        Err(_) => (None, None),
    };
    let canonical_artifact = std::fs::canonicalize(artifact_dir)
        .map(|p| strip_verbatim_prefix(&p).display().to_string())
        .unwrap_or_else(|_| artifact_dir.display().to_string());
    let canonical_db = isolated_db.map(|p| {
        std::fs::canonicalize(p)
            .map(|c| strip_verbatim_prefix(&c).display().to_string())
            .unwrap_or_else(|_| p.display().to_string())
    });
    Ok(CampaignProvenance {
        git_commit: git_commit.to_string(),
        git_dirty,
        cargo_version: env!("CARGO_PKG_VERSION").to_string(),
        rules_schema_version: RULES_ENGINE_SCHEMA_VERSION,
        study_id: "IDEA-011-025B-unified-ib".into(),
        study_version: 2,
        v2_root: V2_CAMPAIGN_ROOT.to_string(),
        isolated_db_path: isolated_db.map(|p| p.display().to_string()),
        canonical_isolated_db_path: canonical_db,
        artifact_dir: artifact_dir.display().to_string(),
        canonical_artifact_dir: canonical_artifact,
        fixed_stop_points: FIXED_STOP_POINTS,
        persistence_ms: ONE_SIDED_PERSISTENCE_MS,
        ib_extension_ratio: IB_EXTENSION_RATIO,
        min_rth_ticks: MIN_RTH_TICKS,
        windows,
        excluded_session_dates: EXCLUDED_SESSION_DATES
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
        transition_exclusions,
        commands,
        source_file_hashes: sources,
        binary_path,
        binary_sha256,
    })
}

/// Inputs for `prepare_v2_campaign_run` (keeps the public call site tidy).
#[derive(Debug, Clone)]
pub struct PrepareV2Request<'a> {
    pub run_dir: &'a Path,
    pub v2_root: &'a Path,
    pub overwrite: bool,
    pub db_filename: &'a str,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
    /// Production default is `RolloverVolumeSource::ScanLocalScid`.
    pub volume_source: RolloverVolumeSource<'a>,
}

/// Prepare a V2 run: path safety, fresh isolated DB, rollover evidence, derived windows.
///
/// Does **not** open any live DB and does **not** run the full study replay.
pub fn prepare_v2_campaign_run(req: PrepareV2Request<'_>) -> Result<PrepareV2Result, String> {
    if is_forbidden_live_db_path(req.run_dir) {
        return Err(format!(
            "refusing forbidden live DB path as run dir: {}",
            req.run_dir.display()
        ));
    }
    let (rollover, windows) = resolve_rollover_and_windows(req.volume_source)?;
    let canonical_run = prepare_run_directory(req.run_dir, req.v2_root, req.overwrite)?;
    let canonical_db =
        create_fresh_isolated_campaign_db(&canonical_run, req.db_filename, req.overwrite)?;
    let transition_exclusions = collect_transition_exclusions(&rollover);
    let provenance = build_provenance(
        req.git_commit,
        req.git_dirty,
        &canonical_run,
        Some(&canonical_db),
        windows.clone(),
        transition_exclusions,
        req.commands,
    )?;
    write_json(&canonical_run.join("rollover_evidence.json"), &rollover)?;
    write_json(&canonical_run.join("provenance.json"), &provenance)?;
    Ok(PrepareV2Result {
        canonical_run_dir: canonical_run.display().to_string(),
        canonical_db_path: canonical_db.display().to_string(),
        provenance,
        rollover,
        windows,
    })
}

/// Legacy wrapper retained for call sites; V2 always parks forward validation.
pub fn decide_verdict(
    _discovery_trades: &BTreeMap<String, OutcomeStats>,
    _holdout_trades: &BTreeMap<String, OutcomeStats>,
    _descriptive: &DescriptiveSummary,
) -> (String, String, Option<String>) {
    let empty = PrimaryContrastReport {
        treated_gate: PRIMARY_TREATED_GATE.as_str().into(),
        control_gate: PRIMARY_CONTROL_GATE.as_str().into(),
        target_r: PRIMARY_TARGET_R,
        matched_n: 0,
        matched_keys: vec![],
        pairs: vec![],
        scenarios: vec![],
        weekly_bootstrap: BlockBootstrapInterval {
            block_kind: "weekly".into(),
            seed: BLOCK_BOOTSTRAP_SEED,
            replicates: BLOCK_BOOTSTRAP_REPLICATES,
            n_blocks: 0,
            block_ids: vec![],
            point_mean_delta_r: 0.0,
            ci95_low: 0.0,
            ci95_high: 0.0,
            sparse_block_limitation: Some(
                "n_blocks=0: no bootstrap clusters; CI collapses to the empty/point mean and is not inferential."
                    .into(),
            ),
        },
        contract_bootstrap: BlockBootstrapInterval {
            block_kind: "contract".into(),
            seed: BLOCK_BOOTSTRAP_SEED,
            replicates: BLOCK_BOOTSTRAP_REPLICATES,
            n_blocks: 0,
            block_ids: vec![],
            point_mean_delta_r: 0.0,
            ci95_low: 0.0,
            ci95_high: 0.0,
            sparse_block_limitation: Some(
                "n_blocks=0: no bootstrap clusters; CI collapses to the empty/point mean and is not inferential."
                    .into(),
            ),
        },
    };
    let v = decide_v2_verdict(&empty);
    (v.result, v.rationale, None)
}

pub fn build_tradable_maps(
    trades: &[GateTradeRow],
) -> (
    BTreeMap<String, OutcomeStats>,
    BTreeMap<String, OutcomeStats>,
    BTreeMap<String, OutcomeStats>,
) {
    let mut all = BTreeMap::new();
    let mut discovery = BTreeMap::new();
    let mut holdout = BTreeMap::new();
    for gate in [
        GateKind::UngatedBaseline,
        GateKind::OneSidedAcceptance30m,
        GateKind::IbConfluence,
    ] {
        for target_r in [1.0, 2.0] {
            let key = format!("{}|{}R", gate.as_str(), target_r as i32);
            let subset: Vec<_> = trades
                .iter()
                .filter(|t| t.gate == gate && (t.target_r - target_r).abs() < 1e-9)
                .cloned()
                .collect();
            all.insert(key.clone(), summarize_trades(&subset));
            let d: Vec<_> = subset
                .iter()
                .filter(|t| t.role == WindowRole::Discovery)
                .cloned()
                .collect();
            let h: Vec<_> = subset
                .iter()
                .filter(|t| t.role == WindowRole::Holdout)
                .cloned()
                .collect();
            discovery.insert(key.clone(), summarize_trades(&d));
            holdout.insert(key, summarize_trades(&h));
        }
    }
    (all, discovery, holdout)
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    std::fs::write(path, text).map_err(|e| e.to_string())
}

/// Count metric extracted from a campaign report for v1↔v2 reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationCounts {
    pub population_sessions: Option<usize>,
    pub usable_sessions: Option<usize>,
    pub excluded_sessions: Option<usize>,
    pub one_sided_sessions: Option<usize>,
    pub persisted_one_sided_30m: Option<usize>,
    pub trade_rows: Option<usize>,
    pub matched_primary_n: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CountDelta {
    pub metric: String,
    pub v1: Option<usize>,
    pub v2: Option<usize>,
    pub delta: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct V1V2CountReconciliation {
    pub v1_label: String,
    pub v1_artifact_dir: String,
    pub v1_decision_ignored: Option<String>,
    pub v1_counts: ReconciliationCounts,
    pub v2_counts: ReconciliationCounts,
    pub deltas: Vec<CountDelta>,
    pub definition_changes: Vec<String>,
    pub note: String,
}

/// Inputs for full study replay using a prepared plan (no window re-derivation).
#[derive(Debug, Clone)]
pub struct ExecuteV2Request<'a> {
    pub run_dir: &'a Path,
    pub isolated_db_path: &'a Path,
    pub windows: Vec<ContractWindow>,
    pub rollover: RolloverEvidence,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
    pub v1_artifact_dir: &'a Path,
}

fn counts_from_descriptive(d: &DescriptiveSummary) -> ReconciliationCounts {
    ReconciliationCounts {
        population_sessions: Some(d.population_sessions),
        usable_sessions: Some(d.usable_sessions),
        excluded_sessions: Some(d.excluded_sessions),
        one_sided_sessions: Some(d.one_sided_sessions),
        persisted_one_sided_30m: Some(d.persisted_one_sided_30m),
        trade_rows: None,
        matched_primary_n: None,
    }
}

fn metric_delta(metric: &str, v1: Option<usize>, v2: Option<usize>) -> CountDelta {
    let delta = match (v1, v2) {
        (Some(a), Some(b)) => Some(b as i64 - a as i64),
        _ => None,
    };
    CountDelta {
        metric: metric.into(),
        v1,
        v2,
        delta,
    }
}

fn read_json_value(path: &Path) -> Result<serde_json::Value, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))
}

fn json_usize(v: &serde_json::Value, camel: &str) -> Option<usize> {
    v.get(camel).and_then(|x| x.as_u64()).map(|n| n as usize)
}

/// Build v1↔v2 count reconciliation from preserved parent v1 JSON (never reuse v1 verdict).
pub fn build_v1_v2_count_reconciliation(
    v1_artifact_dir: &Path,
    v2_report: &CampaignReport,
    v2_trade_rows: usize,
) -> Result<V1V2CountReconciliation, String> {
    let report_path = v1_artifact_dir.join("report.json");
    let trades_path = v1_artifact_dir.join("trades.json");
    if !report_path.exists() {
        return Err(format!(
            "BLOCKED: preserved v1 report.json missing at {}",
            report_path.display()
        ));
    }
    let v1_report = read_json_value(&report_path)?;
    let v1_decision = v1_report
        .get("decision")
        .and_then(|d| d.as_str())
        .map(|s| s.to_string());
    let desc = v1_report
        .get("descriptiveAll")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let v1_trade_rows = if trades_path.exists() {
        let trades = read_json_value(&trades_path)?;
        Some(trades.as_array().map(|a| a.len()).unwrap_or(0))
    } else {
        None
    };
    // v1 had no matched primary contrast package.
    let v1_matched = v1_report
        .pointer("/primaryContrast/matchedN")
        .and_then(|x| x.as_u64())
        .map(|n| n as usize);

    let v1_counts = ReconciliationCounts {
        population_sessions: json_usize(&desc, "populationSessions"),
        usable_sessions: json_usize(&desc, "usableSessions"),
        excluded_sessions: json_usize(&desc, "excludedSessions"),
        one_sided_sessions: json_usize(&desc, "oneSidedSessions"),
        persisted_one_sided_30m: json_usize(&desc, "persistedOneSided30m"),
        trade_rows: v1_trade_rows,
        matched_primary_n: v1_matched,
    };

    let mut v2_counts = counts_from_descriptive(&v2_report.descriptive_all);
    v2_counts.trade_rows = Some(v2_trade_rows);
    v2_counts.matched_primary_n = Some(v2_report.primary_contrast.matched_n);

    let deltas = vec![
        metric_delta(
            "population_sessions",
            v1_counts.population_sessions,
            v2_counts.population_sessions,
        ),
        metric_delta(
            "usable_sessions",
            v1_counts.usable_sessions,
            v2_counts.usable_sessions,
        ),
        metric_delta(
            "excluded_sessions",
            v1_counts.excluded_sessions,
            v2_counts.excluded_sessions,
        ),
        metric_delta(
            "one_sided_sessions",
            v1_counts.one_sided_sessions,
            v2_counts.one_sided_sessions,
        ),
        metric_delta(
            "persisted_one_sided_30m",
            v1_counts.persisted_one_sided_30m,
            v2_counts.persisted_one_sided_30m,
        ),
        metric_delta("trade_rows", v1_counts.trade_rows, v2_counts.trade_rows),
        metric_delta(
            "matched_primary_n",
            v1_counts.matched_primary_n,
            v2_counts.matched_primary_n,
        ),
    ];

    let v1_decision_for_note = v1_decision.clone();
    Ok(V1V2CountReconciliation {
        v1_label: V1_RESULT_LABEL.into(),
        v1_artifact_dir: v1_artifact_dir.display().to_string(),
        v1_decision_ignored: v1_decision,
        v1_counts,
        v2_counts,
        deltas,
        definition_changes: vec![
            "v1 used provisional/hardcoded contract windows; v2 uses volume-crossover-derived windows.".into(),
            "v1 IB close/open and gate arming had engineering defects; v2 enforces half-open IB, independent gates, observed 30m acceptance, causal RVOL.".into(),
            "v1 lacked a matched 1R primary contrast package; v2 matched_primary_n is one-sided-30m vs ungated at 1R on identical session+direction keys.".into(),
            "v1 decision/verdict is INVALID_ENGINEERING_DIAGNOSTIC and must never be reused for acceptance.".into(),
            "v2 coverage/rejection accounting and transition exclusions are explicit; reliability uses metric-specific N.".into(),
        ],
        note: format!(
            "Compare counts only. Do not promote v1 decision ({:?}). V2 verdict is independent: {} / {}.",
            v1_decision_ignored_str(v1_decision_for_note.as_deref()),
            v2_report.verdict.result,
            v2_report.verdict.disposition
        ),
    })
}

fn v1_decision_ignored_str(d: Option<&str>) -> &str {
    d.unwrap_or("<missing>")
}

/// Render a human-readable REPORT.md for the V2 campaign run.
pub fn render_campaign_report_md(
    report: &CampaignReport,
    reconciliation: &V1V2CountReconciliation,
) -> String {
    let mut out = String::new();
    out.push_str("# Unified IB Campaign V2 Report\n\n");
    out.push_str(&format!(
        "- studyVersion: {}\n- decision: {} / {}\n- git: {} (dirty={})\n",
        report.provenance.study_version,
        report.verdict.result,
        report.verdict.disposition,
        report.provenance.git_commit,
        report.provenance.git_dirty
    ));
    out.push_str(&format!(
        "- artifactDir: `{}`\n- isolatedDb: `{}`\n",
        report.provenance.canonical_artifact_dir,
        report
            .provenance
            .canonical_isolated_db_path
            .as_deref()
            .unwrap_or("none")
    ));
    if let Some(ref h) = report.provenance.binary_sha256 {
        out.push_str(&format!("- binarySha256: `{h}`\n"));
    }
    out.push_str("\n## Derived windows\n\n");
    for w in &report.provenance.windows {
        out.push_str(&format!(
            "- {} {}..{} ({:?})\n",
            w.contract, w.start_date, w.end_date, w.role
        ));
    }
    out.push_str("\n## Descriptive (all)\n\n");
    let d = &report.descriptive_all;
    out.push_str(&format!(
        "- population={} usable={} excluded={} one_sided={} persisted_30m={}\n",
        d.population_sessions,
        d.usable_sessions,
        d.excluded_sessions,
        d.one_sided_sessions,
        d.persisted_one_sided_30m
    ));
    out.push_str("\n## Primary contrast (1R matched)\n\n");
    let p = &report.primary_contrast;
    out.push_str(&format!(
        "- {} vs {} matched_n={}\n",
        p.treated_gate, p.control_gate, p.matched_n
    ));
    for s in &p.scenarios {
        out.push_str(&format!(
            "- scenario {}: treated_avg_r={:.4} control_avg_r={:.4} delta_avg_r={:.4} (scenario_not_fees={})\n",
            s.label, s.treated_avg_r, s.control_avg_r, s.delta_avg_r, s.is_scenario_not_actual_fees
        ));
    }
    out.push_str(&format!(
        "- weekly bootstrap seed={} n_blocks={} point_mean_delta_r={:.6} ci95=[{:.4},{:.4}]\n",
        p.weekly_bootstrap.seed,
        p.weekly_bootstrap.n_blocks,
        p.weekly_bootstrap.point_mean_delta_r,
        p.weekly_bootstrap.ci95_low,
        p.weekly_bootstrap.ci95_high
    ));
    if let Some(note) = &p.weekly_bootstrap.sparse_block_limitation {
        out.push_str(&format!("- weekly bootstrap limitation: {note}\n"));
    }
    out.push_str(&format!(
        "- contract bootstrap seed={} n_blocks={} point_mean_delta_r={:.6} ci95=[{:.4},{:.4}]\n",
        p.contract_bootstrap.seed,
        p.contract_bootstrap.n_blocks,
        p.contract_bootstrap.point_mean_delta_r,
        p.contract_bootstrap.ci95_low,
        p.contract_bootstrap.ci95_high
    ));
    if let Some(note) = &p.contract_bootstrap.sparse_block_limitation {
        out.push_str(&format!("- contract bootstrap limitation: {note}\n"));
    }
    out.push_str("\n## Verdict\n\n");
    out.push_str(&format!("{}\n\n", report.verdict.rationale));
    out.push_str("## v1↔v2 reconciliation\n\n");
    out.push_str(&format!(
        "- v1 labeled `{}` (decision ignored: {:?})\n",
        reconciliation.v1_label, reconciliation.v1_decision_ignored
    ));
    for delta in &reconciliation.deltas {
        out.push_str(&format!(
            "- {}: v1={:?} v2={:?} delta={:?}\n",
            delta.metric, delta.v1, delta.v2, delta.delta
        ));
    }
    out.push_str("\n### Definition changes\n\n");
    for c in &reconciliation.definition_changes {
        out.push_str(&format!("- {c}\n"));
    }
    out.push_str("\n## Commands\n\n");
    for c in &report.provenance.commands {
        out.push_str(&format!("- `{c}`\n"));
    }
    out.push_str("\n## Source hashes\n\n");
    for h in &report.provenance.source_file_hashes {
        out.push_str(&format!("- `{}`: `{}`\n", h.path, h.sha256));
    }
    out
}

fn assemble_campaign_report(
    provenance: CampaignProvenance,
    warnings: Vec<String>,
    all_features: &[SessionIbFeatures],
    all_trades: &[GateTradeRow],
) -> CampaignReport {
    let descriptive_all = summarize_descriptive(all_features);
    let discovery_feats: Vec<_> = all_features
        .iter()
        .filter(|s| s.role == WindowRole::Discovery)
        .cloned()
        .collect();
    let holdout_feats: Vec<_> = all_features
        .iter()
        .filter(|s| s.role == WindowRole::Holdout)
        .cloned()
        .collect();
    let descriptive_discovery = summarize_descriptive(&discovery_feats);
    let descriptive_holdout = summarize_descriptive(&holdout_feats);
    let (tradable_all, tradable_discovery, tradable_holdout) = build_tradable_maps(all_trades);
    let primary_contrast = build_primary_contrast_report(all_trades);
    let verdict = decide_v2_verdict(&primary_contrast);
    let mut tradable = BTreeMap::new();
    tradable.insert("all".into(), tradable_all);
    tradable.insert("discovery".into(), tradable_discovery);
    tradable.insert("holdout".into(), tradable_holdout);
    CampaignReport {
        provenance,
        warnings,
        descriptive_all,
        descriptive_discovery,
        descriptive_holdout,
        tradable,
        primary_contrast,
        verdict: verdict.clone(),
        decision: verdict.result.clone(),
        decision_rationale: verdict.rationale.clone(),
        refinement_proposal: Some(verdict.future_acceptance_policy.note.clone()),
    }
}

/// Write all required V2 artifacts for an assembled report (shared by execute + tests).
pub fn write_v2_campaign_artifacts(
    run_dir: &Path,
    report: &CampaignReport,
    rollover: &RolloverEvidence,
    sessions: &[SessionIbFeatures],
    trades: &[GateTradeRow],
    reconciliation: &V1V2CountReconciliation,
) -> Result<(), String> {
    write_json(&run_dir.join("sessions.json"), &sessions)?;
    write_json(&run_dir.join("trades.json"), &trades)?;
    write_json(&run_dir.join("report.json"), report)?;
    write_json(&run_dir.join("rollover_evidence.json"), rollover)?;
    write_json(&run_dir.join("provenance.json"), &report.provenance)?;
    write_json(
        &run_dir.join("v1-v2-count-reconciliation.json"),
        reconciliation,
    )?;
    let md = render_campaign_report_md(report, reconciliation);
    std::fs::write(run_dir.join("REPORT.md"), md).map_err(|e| e.to_string())?;
    Ok(())
}

/// Full V2 study replay using **exactly** the prepared windows + rollover evidence.
///
/// Does not re-derive windows and does not open a live DB.
pub fn execute_v2_campaign(req: ExecuteV2Request<'_>) -> Result<CampaignReport, String> {
    if req.windows.is_empty() {
        return Err("BLOCKED: execute requires prepared crossover-derived windows".into());
    }
    require_resolved_rollover_crossovers(&req.rollover)?;
    if is_forbidden_live_db_path(req.isolated_db_path) {
        return Err(format!(
            "refusing forbidden live DB path: {}",
            req.isolated_db_path.display()
        ));
    }

    let mut all_features = Vec::new();
    let mut all_trades = Vec::new();
    let mut warnings = Vec::new();
    let mut rvol_catalog: Vec<DatedVolumeCurve> = Vec::new();

    for w in &req.windows {
        eprintln!(
            "Executing replay {} {}..{} ({:?})...",
            w.contract, w.start_date, w.end_date, w.role
        );
        match replay_contract_window(w, &mut rvol_catalog) {
            Ok((feats, trades, wwarn)) => {
                warnings.extend(wwarn);
                let warmup_n = feats.iter().filter(|s| s.causal_rvol_warmup).count();
                eprintln!(
                    "  sessions={} trades_rows={} causal_rvol_warmup={} catalog={}",
                    feats.len(),
                    trades.len(),
                    warmup_n,
                    rvol_catalog.len()
                );
                all_features.extend(feats);
                all_trades.extend(trades);
            }
            Err(e) => warnings.push(format!("{}: {e}", w.contract)),
        }
    }

    let warmup_excluded = all_features.iter().filter(|s| s.causal_rvol_warmup).count();
    if warmup_excluded > 0 {
        warnings.push(format!(
            "causal_rvol_warmup: {warmup_excluded} sessions retained for descriptive metrics but excluded from inferential/trade N"
        ));
    }
    let holdout_n = all_features
        .iter()
        .filter(|s| s.role == WindowRole::Holdout)
        .count();
    if holdout_n > 0 {
        warnings.push(
            "holdout_role_present: unexpected Holdout-labeled sessions; treat as development only"
                .into(),
        );
    }

    let transition_exclusions = collect_transition_exclusions(&req.rollover);
    let provenance = build_provenance(
        req.git_commit,
        req.git_dirty,
        req.run_dir,
        Some(req.isolated_db_path),
        req.windows.clone(),
        transition_exclusions,
        req.commands.clone(),
    )?;

    let mut report = assemble_campaign_report(provenance, warnings, &all_features, &all_trades);
    report.warnings.push(format!(
        "primary_contrast_matched_n={}; cost scenarios are labeled total-cost sensitivities, not actual fees",
        report.primary_contrast.matched_n
    ));

    let reconciliation =
        build_v1_v2_count_reconciliation(req.v1_artifact_dir, &report, all_trades.len())?;
    write_v2_campaign_artifacts(
        req.run_dir,
        &report,
        &req.rollover,
        &all_features,
        &all_trades,
        &reconciliation,
    )?;
    Ok(report)
}

/// Write artifacts from supplied features/trades (integration tests; no SCID I/O).
pub fn execute_v2_campaign_with_sessions(
    req: ExecuteV2Request<'_>,
    all_features: &[SessionIbFeatures],
    all_trades: &[GateTradeRow],
) -> Result<CampaignReport, String> {
    if req.windows.is_empty() {
        return Err("BLOCKED: execute requires prepared crossover-derived windows".into());
    }
    require_resolved_rollover_crossovers(&req.rollover)?;
    let transition_exclusions = collect_transition_exclusions(&req.rollover);
    let provenance = build_provenance(
        req.git_commit,
        req.git_dirty,
        req.run_dir,
        Some(req.isolated_db_path),
        req.windows.clone(),
        transition_exclusions,
        req.commands.clone(),
    )?;
    let report = assemble_campaign_report(provenance, Vec::new(), all_features, all_trades);
    let reconciliation =
        build_v1_v2_count_reconciliation(req.v1_artifact_dir, &report, all_trades.len())?;
    write_v2_campaign_artifacts(
        req.run_dir,
        &report,
        &req.rollover,
        all_features,
        all_trades,
        &reconciliation,
    )?;
    Ok(report)
}

/// Deprecated entry: always BLOCKED. Use prepare + `execute_v2_campaign` with supplied plan.
pub fn run_campaign(
    _artifact_dir: &Path,
    _isolated_db_path: Option<&Path>,
    _git_commit: &str,
    _git_dirty: bool,
    _commands: Vec<String>,
) -> Result<CampaignReport, String> {
    Err(
        "BLOCKED: legacy run_campaign is disabled; use prepare_v2_campaign_run + execute_v2_campaign with crossover-derived windows"
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_requires_full_30_minutes() {
        let t0 = 1_000_000.0;
        assert!(!one_sided_persisted_30m(
            t0,
            ExtensionDirection::Up,
            None,
            t0 + ONE_SIDED_PERSISTENCE_MS - 1.0
        ));
        assert!(one_sided_persisted_30m(
            t0,
            ExtensionDirection::Up,
            None,
            t0 + ONE_SIDED_PERSISTENCE_MS
        ));
    }

    #[test]
    fn persistence_fails_if_opposite_prints_before_check() {
        let t0 = 1_000_000.0;
        let opp = t0 + 10.0 * 60_000.0;
        assert!(!one_sided_persisted_30m(
            t0,
            ExtensionDirection::Up,
            Some(opp),
            t0 + ONE_SIDED_PERSISTENCE_MS
        ));
    }

    #[test]
    fn acceptance_rejects_without_qualifying_observed_tick() {
        let t0 = 1_000_000.0;
        let threshold = t0 + ONE_SIDED_PERSISTENCE_MS;
        // Before threshold: no acceptance.
        assert!(observe_one_sided_acceptance_ts(t0, None, threshold - 1.0, 700).is_none());
        // Cutoff alone does not invent a synthetic acceptance timestamp.
        assert!(
            observe_one_sided_acceptance_ts(t0, None, threshold, CAMPAIGN_CUTOFF_ET_MINUTES)
                .is_none()
        );
    }

    #[test]
    fn acceptance_uses_first_observed_tick_after_threshold_not_synthetic() {
        let t0 = 1_000_000.0;
        let threshold = t0 + ONE_SIDED_PERSISTENCE_MS;
        let observed = threshold + 12_345.0;
        let accepted = observe_one_sided_acceptance_ts(t0, None, observed, 700);
        assert_eq!(accepted, Some(observed));
        assert_ne!(
            accepted,
            Some(threshold),
            "must not synthesize first_ts + 30m as the acceptance stamp"
        );
    }

    #[test]
    fn acceptance_rejects_opposite_at_or_before_qualifying_tick() {
        let t0 = 1_000_000.0;
        let threshold = t0 + ONE_SIDED_PERSISTENCE_MS;
        let observed = threshold + 1_000.0;
        assert!(
            observe_one_sided_acceptance_ts(t0, Some(observed), observed, 700).is_none(),
            "opposite at the qualifying tick invalidates"
        );
        assert!(
            observe_one_sided_acceptance_ts(t0, Some(threshold - 1.0), observed, 700).is_none(),
            "opposite before the qualifying tick invalidates"
        );
        // Opposite after the observed tick does not invalidate this observation.
        assert_eq!(
            observe_one_sided_acceptance_ts(t0, Some(observed + 1.0), observed, 700),
            Some(observed)
        );
    }

    #[test]
    fn acceptance_rejects_at_or_after_1600_cutoff() {
        let t0 = 1_000_000.0;
        let observed = t0 + ONE_SIDED_PERSISTENCE_MS + 1_000.0;
        assert!(
            observe_one_sided_acceptance_ts(t0, None, observed, CAMPAIGN_CUTOFF_ET_MINUTES)
                .is_none(),
            "exact 16:00 ET is ineligible"
        );
        assert!(
            observe_one_sided_acceptance_ts(t0, None, observed, CAMPAIGN_CUTOFF_ET_MINUTES + 1)
                .is_none(),
            "after 16:00 ET is ineligible"
        );
        assert_eq!(
            observe_one_sided_acceptance_ts(t0, None, observed, CAMPAIGN_CUTOFF_ET_MINUTES - 1),
            Some(observed)
        );
    }

    #[test]
    fn confluence_requires_both_legs() {
        assert_eq!(
            classify_ib_confluence(IbCloseVsMid::Above, IbExtremeOrder::LowFirstHighLast),
            IbConfluence::Bullish
        );
        assert_eq!(
            classify_ib_confluence(IbCloseVsMid::Below, IbExtremeOrder::HighFirstLowLast),
            IbConfluence::Bearish
        );
        assert_eq!(
            classify_ib_confluence(IbCloseVsMid::Above, IbExtremeOrder::HighFirstLowLast),
            IbConfluence::None
        );
    }

    #[test]
    fn fixed_r_matrix_hits_stop_and_target() {
        let entry = 20_000.0;
        let path = vec![
            (100.0, 20_000.0),
            (200.0, 19_990.0), // -10
            (300.0, 19_988.0), // stop at -12
        ];
        let stop = simulate_fixed_r_trade(
            ExtensionDirection::Up,
            100.0,
            entry,
            12.0,
            1.0,
            &path,
            10_000.0,
        )
        .unwrap();
        assert_eq!(stop.exit_reason, TradeExitReason::Stop);
        assert!((stop.r_result + 1.0).abs() < 1e-9);

        let path2 = vec![(100.0, 20_000.0), (200.0, 20_012.0)];
        let tgt = simulate_fixed_r_trade(
            ExtensionDirection::Up,
            100.0,
            entry,
            12.0,
            1.0,
            &path2,
            10_000.0,
        )
        .unwrap();
        assert_eq!(tgt.exit_reason, TradeExitReason::Target);
        assert!((tgt.r_result - 1.0).abs() < 1e-9);

        let path3 = vec![(100.0, 20_000.0), (200.0, 19_995.0), (300.0, 20_024.0)];
        let tgt2 = simulate_fixed_r_trade(
            ExtensionDirection::Up,
            100.0,
            entry,
            12.0,
            2.0,
            &path3,
            10_000.0,
        )
        .unwrap();
        assert_eq!(tgt2.exit_reason, TradeExitReason::Target);
        assert!((tgt2.r_result - 2.0).abs() < 1e-9);
    }

    #[test]
    fn short_mirror_works() {
        let trade = simulate_fixed_r_trade(
            ExtensionDirection::Down,
            1.0,
            20_000.0,
            12.0,
            1.0,
            &[(1.0, 20_000.0), (2.0, 19_988.0)],
            100.0,
        )
        .unwrap();
        assert!(trade.win);
        assert_eq!(trade.exit_reason, TradeExitReason::Target);
    }

    fn quality_inputs(
        date: &str,
        ticks: i64,
        gaps: u32,
        max_gap: f64,
        first_et: Option<i32>,
        last_et: Option<i32>,
        non_mono: u32,
    ) -> SessionQualityInputs<'_> {
        SessionQualityInputs {
            session_date: date,
            tick_count: ticks,
            material_gap_count: gaps,
            max_gap_minutes: max_gap,
            ib_high: 100.0,
            ib_low: 90.0,
            first_rth_et_minutes: first_et,
            last_rth_et_minutes: last_et,
            rth_start_ts_ms: Some(1.0),
            rth_end_ts_ms: Some(2.0),
            non_monotonic_count: non_mono,
        }
    }

    #[test]
    fn quality_excludes_holidays_and_tiny_ticks() {
        let q = evaluate_session_quality(quality_inputs(
            "2026-07-04",
            1000,
            0,
            0.0,
            Some(crate::RTH_OPEN_ET),
            Some(RTH_END_COVERAGE_MIN_ET),
            0,
        ));
        assert!(!q.usable);
        assert!(q.holiday_or_early_close);
        let q2 = evaluate_session_quality(quality_inputs(
            "2026-07-08",
            50_000,
            0,
            0.0,
            Some(crate::RTH_OPEN_ET),
            Some(RTH_END_COVERAGE_MIN_ET),
            0,
        ));
        assert!(q2.usable);
    }

    #[test]
    fn ib_close_vs_mid_tick_tolerance() {
        assert_eq!(
            classify_ib_close_vs_mid(100.0, 90.0, 95.3),
            IbCloseVsMid::Above
        );
        assert_eq!(
            classify_ib_close_vs_mid(100.0, 90.0, 94.7),
            IbCloseVsMid::Below
        );
        assert_eq!(
            classify_ib_close_vs_mid(100.0, 90.0, 95.0),
            IbCloseVsMid::At
        );
    }

    #[test]
    fn ib_boundary_half_open_close_is_last_pre_1030_tick() {
        // Sequence: last IB tick at minute 59, then two post-IB ticks at minute 60+.
        // Contract: IB = [09:30,10:30); close = last pre-10:30; open = first ≥10:30.
        let mut tracker = IbFormationTracker::default();
        tracker.on_rth_tick(0, 20_000.0, 1.0);
        tracker.on_rth_tick(30, 20_010.0, 2.0);
        tracker.on_rth_tick(59, 20_005.0, 3.0); // IB close candidate
        assert_eq!(tracker.complete_price, Some(20_005.0));
        assert_eq!(tracker.complete_ts, Some(3.0));
        assert!(!tracker.locked);
        assert!(tracker.next_bar_open.is_none());

        tracker.on_rth_tick(60, 20_020.0, 4.0); // first post-IB open
        assert!(tracker.locked);
        assert_eq!(
            tracker.complete_price,
            Some(20_005.0),
            "IB close must remain the last pre-10:30 tick"
        );
        assert_eq!(tracker.complete_ts, Some(3.0));
        assert_eq!(
            tracker.next_bar_open,
            Some(20_020.0),
            "post-IB open must be the first tick at/after 10:30"
        );
        assert_eq!(tracker.next_bar_inside, Some(false));

        tracker.on_rth_tick(61, 20_001.0, 5.0); // later post-IB tick must not steal open
        assert_eq!(tracker.complete_price, Some(20_005.0));
        assert_eq!(tracker.next_bar_open, Some(20_020.0));
    }

    #[test]
    fn ib_boundary_rejects_prior_first_post_ib_as_close() {
        // Prior defect: first minute>=60 tick was stored as IB close and the
        // *second* post-IB tick became next_bar_open. Prove that is rejected.
        let mut tracker = IbFormationTracker::default();
        tracker.on_rth_tick(59, 19_990.0, 10.0);
        tracker.on_rth_tick(60, 20_100.0, 11.0);
        tracker.on_rth_tick(60, 20_050.0, 12.0);

        assert_ne!(
            tracker.complete_price,
            Some(20_100.0),
            "must not treat the first post-IB tick as IB close"
        );
        assert_ne!(
            tracker.next_bar_open,
            Some(20_050.0),
            "must not defer post-IB open to the second post-IB tick"
        );
        assert_eq!(tracker.complete_price, Some(19_990.0));
        assert_eq!(tracker.next_bar_open, Some(20_100.0));
    }

    #[test]
    fn ib_boundary_no_synthetic_close_or_open_without_observations() {
        let mut empty = IbFormationTracker::default();
        empty.on_rth_tick(60, 20_000.0, 1.0);
        assert!(!empty.locked);
        assert!(empty.complete_price.is_none());
        assert!(empty.next_bar_open.is_none());

        let mut ib_only = IbFormationTracker::default();
        ib_only.on_rth_tick(0, 20_000.0, 1.0);
        ib_only.on_rth_tick(59, 20_010.0, 2.0);
        assert_eq!(ib_only.complete_price, Some(20_010.0));
        assert!(!ib_only.locked);
        assert!(
            ib_only.next_bar_open.is_none(),
            "missing post-IB ticks must not invent a synthetic open"
        );
    }

    fn synth_tick(ts: f64, price: f64) -> ScidTick {
        ScidTick {
            timestamp_ms: ts,
            price,
            volume: 1.0,
            bid: price - 0.25,
            ask: price + 0.25,
            side: crate::feed::TradeSide::Buy,
        }
    }

    fn new_accum() -> SessionAccum {
        SessionAccum::new(
            "2026-07-08".into(),
            "NQU6.CME".into(),
            WindowRole::Discovery,
            &CausalRvolBaseline {
                root_symbol: CAUSAL_RVOL_ROOT_SYMBOL.into(),
                count: CAUSAL_RVOL_BASELINE_SESSIONS,
                session_dates: (1..=CAUSAL_RVOL_BASELINE_SESSIONS)
                    .map(|i| format!("2026-06-{i:02}"))
                    .collect(),
                curves: vec![vec![100.0; 81]; CAUSAL_RVOL_BASELINE_SESSIONS],
            },
        )
    }

    fn warmup_accum() -> SessionAccum {
        SessionAccum::new(
            "2026-07-08".into(),
            "NQU6.CME".into(),
            WindowRole::Discovery,
            &CausalRvolBaseline {
                root_symbol: CAUSAL_RVOL_ROOT_SYMBOL.into(),
                count: 3,
                session_dates: vec![
                    "2026-06-01".into(),
                    "2026-06-02".into(),
                    "2026-06-03".into(),
                ],
                curves: vec![vec![100.0; 81]; 3],
            },
        )
    }

    fn dated_curve(date: &str, contract: &str, vol: f64) -> DatedVolumeCurve {
        DatedVolumeCurve {
            session_date: date.into(),
            contract: contract.into(),
            root_symbol: CAUSAL_RVOL_ROOT_SYMBOL.into(),
            curve: vec![vol; 81],
        }
    }

    fn idle_rvol() -> RvolPipeline {
        RvolPipeline::new()
    }

    fn elevated_rvol_at(minute: i32) -> RvolPipeline {
        let mut rvol = RvolPipeline::new();
        rvol.start_session(false);
        // 81 RTH 5-minute buckets; low expected volume so injected size classifies High.
        rvol.load_historical_curve(&vec![vec![100.0; 81]; 5]);
        rvol.on_trade(50_000.0, minute);
        assert!(matches!(
            rvol.classification(),
            RvolClassification::Elevated | RvolClassification::High
        ));
        rvol
    }

    /// IB 80–100 (0.5x up=110 / down=70). Low-first / close-above-mid for long confluence.
    fn form_ib_long(accum: &mut SessionAccum, rvol: &RvolPipeline, next_open: f64) {
        accum.on_tick(&synth_tick(1_000.0, 80.0), 0, 570, rvol);
        accum.on_tick(&synth_tick(2_000.0, 100.0), 20, 590, rvol);
        accum.on_tick(&synth_tick(3_000.0, 96.0), 59, 629, rvol);
        accum.on_tick(&synth_tick(4_000.0, next_open), 60, 630, rvol);
        assert!(accum.formation.locked);
        assert_eq!(accum.formation.ib_high, 100.0);
        assert_eq!(accum.formation.ib_low, 80.0);
    }

    /// IB 80–100. High-first / close-below-mid for short confluence.
    fn form_ib_short(accum: &mut SessionAccum, rvol: &RvolPipeline, next_open: f64) {
        accum.on_tick(&synth_tick(1_000.0, 100.0), 0, 570, rvol);
        accum.on_tick(&synth_tick(2_000.0, 80.0), 20, 590, rvol);
        accum.on_tick(&synth_tick(3_000.0, 84.0), 59, 629, rvol);
        accum.on_tick(&synth_tick(4_000.0, next_open), 60, 630, rvol);
        assert!(accum.formation.locked);
        assert_eq!(accum.formation.ib_high, 100.0);
        assert_eq!(accum.formation.ib_low, 80.0);
    }

    fn gate_armed(accum: &SessionAccum, gate: GateKind) -> bool {
        accum.gates.get(&gate).map(|g| g.armed).unwrap_or(false)
    }

    fn gate_entry_px(accum: &SessionAccum, gate: GateKind) -> Option<f64> {
        accum
            .gates
            .get(&gate)
            .and_then(|g| g.entry.as_ref())
            .map(|e| e.2)
    }

    #[test]
    fn breach_before_half_extension_does_not_arm_long_or_short() {
        // Long: trade above IB high but below 0.5x (110); retest must not arm/enter.
        let rvol = idle_rvol();
        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(5_000.0, 105.0), 61, 631, &rvol);
        long.on_tick(&synth_tick(6_000.0, 100.0), 62, 632, &rvol);
        assert!(!gate_armed(&long, GateKind::UngatedBaseline));
        assert!(gate_entry_px(&long, GateKind::UngatedBaseline).is_none());
        assert!(long.extensions.first_dir.is_none());

        // Short mirror: below IB low but above 0.5x (70).
        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(5_000.0, 75.0), 61, 631, &rvol);
        short.on_tick(&synth_tick(6_000.0, 80.0), 62, 632, &rvol);
        assert!(!gate_armed(&short, GateKind::UngatedBaseline));
        assert!(gate_entry_px(&short, GateKind::UngatedBaseline).is_none());
        assert!(short.extensions.first_dir.is_none());
    }

    #[test]
    fn half_extension_then_edge_retest_enters_at_exact_edge_long_and_short() {
        let rvol = idle_rvol();

        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        // crossed_level up through 110 (band 109.5): prev 109 → cur 110.
        long.on_tick(&synth_tick(5_000.0, 109.0), 61, 631, &rvol);
        long.on_tick(&synth_tick(6_000.0, 110.0), 62, 632, &rvol);
        assert_eq!(long.extensions.first_dir, Some(ExtensionDirection::Up));
        assert!(gate_armed(&long, GateKind::UngatedBaseline));
        assert!(gate_entry_px(&long, GateKind::UngatedBaseline).is_none());
        // Later from_above retest of IB high=100 (prev > 100.5, cur <= 100.5).
        // Tick prints through the edge to a favorable extreme — fill must stay at 100.
        long.on_tick(&synth_tick(7_000.0, 101.0), 63, 633, &rvol);
        long.on_tick(&synth_tick(8_000.0, 99.75), 64, 634, &rvol);
        assert_eq!(
            gate_entry_px(&long, GateKind::UngatedBaseline),
            Some(100.0),
            "long fill must be exact IB high, not the tick extreme"
        );

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(5_000.0, 71.0), 61, 631, &rvol);
        short.on_tick(&synth_tick(6_000.0, 70.0), 62, 632, &rvol);
        assert_eq!(short.extensions.first_dir, Some(ExtensionDirection::Down));
        assert!(gate_armed(&short, GateKind::UngatedBaseline));
        short.on_tick(&synth_tick(7_000.0, 79.0), 63, 633, &rvol);
        short.on_tick(&synth_tick(8_000.0, 80.25), 64, 634, &rvol);
        assert_eq!(
            gate_entry_px(&short, GateKind::UngatedBaseline),
            Some(80.0),
            "short fill must be exact IB low, not the tick extreme"
        );
    }

    #[test]
    fn early_retest_before_arm_does_not_block_later_entry_long_and_short() {
        let rvol = elevated_rvol_at(90);

        // Long: IB-edge retest before 0.5x must not consume; later valid path still enters.
        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(5_000.0, 105.0), 61, 631, &rvol);
        long.on_tick(&synth_tick(6_000.0, 100.0), 62, 632, &rvol); // pre-extension retest
        assert!(gate_entry_px(&long, GateKind::UngatedBaseline).is_none());
        long.on_tick(&synth_tick(7_000.0, 109.0), 63, 633, &rvol);
        long.on_tick(&synth_tick(8_000.0, 110.0), 64, 634, &rvol);
        assert!(gate_armed(&long, GateKind::UngatedBaseline));
        long.on_tick(&synth_tick(9_000.0, 101.0), 65, 635, &rvol);
        long.on_tick(&synth_tick(10_000.0, 100.0), 66, 636, &rvol);
        assert_eq!(gate_entry_px(&long, GateKind::UngatedBaseline), Some(100.0));

        // Short mirror.
        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(5_000.0, 75.0), 61, 631, &rvol);
        short.on_tick(&synth_tick(6_000.0, 80.0), 62, 632, &rvol);
        assert!(gate_entry_px(&short, GateKind::UngatedBaseline).is_none());
        short.on_tick(&synth_tick(7_000.0, 71.0), 63, 633, &rvol);
        short.on_tick(&synth_tick(8_000.0, 70.0), 64, 634, &rvol);
        assert!(gate_armed(&short, GateKind::UngatedBaseline));
        short.on_tick(&synth_tick(9_000.0, 79.0), 65, 635, &rvol);
        short.on_tick(&synth_tick(10_000.0, 80.0), 66, 636, &rvol);
        assert_eq!(gate_entry_px(&short, GateKind::UngatedBaseline), Some(80.0));
    }

    #[test]
    fn one_gate_firing_does_not_consume_another_long_and_short() {
        // Baseline can enter on an early post-0.5x retest; one-sided must still arm after
        // +30m and enter on its own later retest.
        let rvol = elevated_rvol_at(90);
        let hit_ts = 8_000.0;
        let early_retest_ts = hit_ts + 5.0 * 60_000.0;
        let arm30_ts = hit_ts + ONE_SIDED_PERSISTENCE_MS;
        let late_retest_ts = arm30_ts + 60_000.0;

        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(hit_ts - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit_ts, 110.0), 71, 641, &rvol);
        assert!(gate_armed(&long, GateKind::UngatedBaseline));
        assert!(!gate_armed(&long, GateKind::OneSidedAcceptance30m));
        // Early retest: baseline consumes; one-sided must remain unconsumed/unarmed.
        long.on_tick(&synth_tick(early_retest_ts - 1.0, 101.0), 76, 646, &rvol);
        long.on_tick(&synth_tick(early_retest_ts, 100.0), 76, 646, &rvol);
        assert_eq!(gate_entry_px(&long, GateKind::UngatedBaseline), Some(100.0));
        assert!(gate_entry_px(&long, GateKind::OneSidedAcceptance30m).is_none());
        assert!(!gate_armed(&long, GateKind::OneSidedAcceptance30m));
        // Stay outside until +30m so one-sided can arm, then later retest.
        long.on_tick(&synth_tick(arm30_ts, 105.0), 101, 671, &rvol);
        assert!(gate_armed(&long, GateKind::OneSidedAcceptance30m));
        long.on_tick(&synth_tick(late_retest_ts - 1.0, 101.0), 102, 672, &rvol);
        long.on_tick(&synth_tick(late_retest_ts, 100.0), 102, 672, &rvol);
        assert_eq!(
            gate_entry_px(&long, GateKind::OneSidedAcceptance30m),
            Some(100.0),
            "one-sided must still enter after baseline already fired"
        );

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(hit_ts - 1_000.0, 71.0), 70, 640, &rvol);
        short.on_tick(&synth_tick(hit_ts, 70.0), 71, 641, &rvol);
        short.on_tick(&synth_tick(early_retest_ts - 1.0, 79.0), 76, 646, &rvol);
        short.on_tick(&synth_tick(early_retest_ts, 80.0), 76, 646, &rvol);
        assert_eq!(gate_entry_px(&short, GateKind::UngatedBaseline), Some(80.0));
        assert!(gate_entry_px(&short, GateKind::OneSidedAcceptance30m).is_none());
        short.on_tick(&synth_tick(arm30_ts, 75.0), 101, 671, &rvol);
        assert!(gate_armed(&short, GateKind::OneSidedAcceptance30m));
        short.on_tick(&synth_tick(late_retest_ts - 1.0, 79.0), 102, 672, &rvol);
        short.on_tick(&synth_tick(late_retest_ts, 80.0), 102, 672, &rvol);
        assert_eq!(
            gate_entry_px(&short, GateKind::OneSidedAcceptance30m),
            Some(80.0)
        );
    }

    #[test]
    fn observed_acceptance_absent_without_post_threshold_tick() {
        let rvol = idle_rvol();
        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        let hit = 8_000.0;
        long.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        long.on_tick(
            &synth_tick(hit + ONE_SIDED_PERSISTENCE_MS - 1.0, 111.0),
            100,
            670,
            &rvol,
        );
        assert!(
            long.acceptance_ts_ms.is_none(),
            "no acceptance without an observed tick at/after +30m"
        );

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(hit - 1_000.0, 71.0), 70, 640, &rvol);
        short.on_tick(&synth_tick(hit, 70.0), 71, 641, &rvol);
        short.on_tick(
            &synth_tick(hit + ONE_SIDED_PERSISTENCE_MS - 1.0, 69.0),
            100,
            670,
            &rvol,
        );
        assert!(short.acceptance_ts_ms.is_none());
    }

    #[test]
    fn observed_acceptance_records_first_post_threshold_tick_long_and_short() {
        let rvol = idle_rvol();
        let hit = 8_000.0;
        let threshold = hit + ONE_SIDED_PERSISTENCE_MS;
        let observed = threshold + 45_000.0;

        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        long.on_tick(&synth_tick(threshold - 1.0, 111.0), 100, 670, &rvol);
        assert!(long.acceptance_ts_ms.is_none());
        long.on_tick(&synth_tick(observed, 112.0), 101, 671, &rvol);
        assert_eq!(long.acceptance_ts_ms, Some(observed));
        assert_ne!(long.acceptance_ts_ms, Some(threshold));

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(hit - 1_000.0, 71.0), 70, 640, &rvol);
        short.on_tick(&synth_tick(hit, 70.0), 71, 641, &rvol);
        short.on_tick(&synth_tick(threshold - 1.0, 69.0), 100, 670, &rvol);
        assert!(short.acceptance_ts_ms.is_none());
        short.on_tick(&synth_tick(observed, 68.0), 101, 671, &rvol);
        assert_eq!(short.acceptance_ts_ms, Some(observed));
        assert_ne!(short.acceptance_ts_ms, Some(threshold));
    }

    #[test]
    fn observed_acceptance_rejects_opposite_before_qualification_long_and_short() {
        let rvol = idle_rvol();
        let hit = 8_000.0;
        let threshold = hit + ONE_SIDED_PERSISTENCE_MS;

        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        // Opposite 0.5x down before +30m.
        long.on_tick(&synth_tick(hit + 5.0 * 60_000.0, 71.0), 80, 650, &rvol);
        long.on_tick(
            &synth_tick(hit + 5.0 * 60_000.0 + 1.0, 70.0),
            80,
            650,
            &rvol,
        );
        long.on_tick(&synth_tick(threshold + 1_000.0, 112.0), 101, 671, &rvol);
        assert!(
            long.acceptance_ts_ms.is_none(),
            "opposite before qualification must reject long acceptance"
        );

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(hit - 1_000.0, 71.0), 70, 640, &rvol);
        short.on_tick(&synth_tick(hit, 70.0), 71, 641, &rvol);
        short.on_tick(&synth_tick(hit + 5.0 * 60_000.0, 109.0), 80, 650, &rvol);
        short.on_tick(
            &synth_tick(hit + 5.0 * 60_000.0 + 1.0, 110.0),
            80,
            650,
            &rvol,
        );
        short.on_tick(&synth_tick(threshold + 1_000.0, 68.0), 101, 671, &rvol);
        assert!(
            short.acceptance_ts_ms.is_none(),
            "opposite before qualification must reject short acceptance"
        );
    }

    #[test]
    fn observed_acceptance_rejects_exact_or_later_cutoff_long_and_short() {
        let rvol = idle_rvol();
        let hit = 8_000.0;
        let observed = hit + ONE_SIDED_PERSISTENCE_MS + 1_000.0;

        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        long.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        long.on_tick(
            &synth_tick(observed, 112.0),
            390,
            CAMPAIGN_CUTOFF_ET_MINUTES,
            &rvol,
        );
        assert!(
            long.acceptance_ts_ms.is_none(),
            "exact 16:00 ET observed candidate is ineligible"
        );
        long.on_tick(
            &synth_tick(observed + 1.0, 113.0),
            391,
            CAMPAIGN_CUTOFF_ET_MINUTES + 5,
            &rvol,
        );
        assert!(long.acceptance_ts_ms.is_none());

        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(hit - 1_000.0, 71.0), 70, 640, &rvol);
        short.on_tick(&synth_tick(hit, 70.0), 71, 641, &rvol);
        short.on_tick(
            &synth_tick(observed, 68.0),
            390,
            CAMPAIGN_CUTOFF_ET_MINUTES,
            &rvol,
        );
        assert!(short.acceptance_ts_ms.is_none());
        short.on_tick(
            &synth_tick(observed + 1.0, 67.0),
            391,
            CAMPAIGN_CUTOFF_ET_MINUTES + 5,
            &rvol,
        );
        assert!(short.acceptance_ts_ms.is_none());
    }

    #[test]
    fn causal_rvol_rejects_future_and_same_date_curves() {
        let catalog = vec![
            dated_curve("2026-06-01", "NQH6.CME", 100.0),
            dated_curve("2026-06-10", "NQH6.CME", 110.0),
            dated_curve("2026-06-15", "NQM6.CME", 120.0), // same-date as as_of
            dated_curve("2026-06-20", "NQM6.CME", 130.0), // future
        ];
        let baseline = select_causal_rvol_baseline(
            &catalog,
            "2026-06-15",
            CAUSAL_RVOL_ROOT_SYMBOL,
            CAUSAL_RVOL_BASELINE_SESSIONS,
        );
        assert_eq!(baseline.session_dates, vec!["2026-06-01", "2026-06-10"]);
        assert!(baseline
            .session_dates
            .iter()
            .all(|d| d.as_str() < "2026-06-15"));
        assert!(!baseline.session_dates.iter().any(|d| d == "2026-06-15"));
        assert!(!baseline.session_dates.iter().any(|d| d == "2026-06-20"));
    }

    #[test]
    fn causal_rvol_orders_and_truncates_to_20_most_recent_priors() {
        let mut catalog = Vec::new();
        for i in 1..=25 {
            catalog.push(dated_curve(
                &format!("2026-05-{i:02}"),
                if i <= 12 { "NQH6.CME" } else { "NQM6.CME" },
                i as f64,
            ));
        }
        // Noise from another root must not enter NQ baseline.
        catalog.push(DatedVolumeCurve {
            session_date: "2026-05-30".into(),
            contract: "ESM6.CME".into(),
            root_symbol: "ES".into(),
            curve: vec![999.0; 81],
        });
        let baseline = select_causal_rvol_baseline(
            &catalog,
            "2026-06-01",
            CAUSAL_RVOL_ROOT_SYMBOL,
            CAUSAL_RVOL_BASELINE_SESSIONS,
        );
        assert_eq!(baseline.count, 20);
        assert_eq!(
            baseline.session_dates.first().map(String::as_str),
            Some("2026-05-06")
        );
        assert_eq!(
            baseline.session_dates.last().map(String::as_str),
            Some("2026-05-25")
        );
        assert_eq!(baseline.curves.len(), 20);
        assert!((baseline.curves[0][0] - 6.0).abs() < 1e-9);
        assert!((baseline.curves[19][0] - 25.0).abs() < 1e-9);
    }

    #[test]
    fn causal_rvol_warmup_excludes_inferential_trades() {
        let mut warmup = warmup_accum();
        assert!(warmup.causal_rvol_warmup);
        assert_eq!(warmup.rvol_baseline_count, 3);
        // Force a completed path with an entry so finish would otherwise emit trades.
        let rvol = elevated_rvol_at(90);
        form_ib_long(&mut warmup, &rvol, 95.0);
        let hit = 8_000.0;
        warmup.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        warmup.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        let arm = hit + ONE_SIDED_PERSISTENCE_MS;
        warmup.on_tick(&synth_tick(arm, 105.0), 101, 671, &rvol);
        warmup.on_tick(&synth_tick(arm + 60_000.0 - 1.0, 101.0), 102, 672, &rvol);
        warmup.on_tick(&synth_tick(arm + 60_000.0, 100.0), 102, 672, &rvol);
        // Unit-scope: satisfy descriptive quality without SCID density/gap realism.
        warmup.tick_count = MIN_RTH_TICKS;
        warmup.material_gap_count = 0;
        warmup.max_gap_minutes = 0.0;
        warmup.non_monotonic_count = 0;
        warmup.first_rth_et_minutes = Some(crate::RTH_OPEN_ET);
        warmup.first_rth_ts_ms = Some(1.0);
        warmup.last_rth_et_minutes = Some(RTH_END_COVERAGE_MIN_ET);
        warmup.last_rth_ts_ms = Some(2.0);
        let (features, trades) = warmup.finish();
        assert!(features.causal_rvol_warmup);
        assert!(!features.inferential_eligible);
        assert_eq!(features.rvol_baseline_count, 3);
        assert_eq!(
            features.rvol_baseline_session_dates,
            vec![
                "2026-06-01".to_string(),
                "2026-06-02".to_string(),
                "2026-06-03".to_string()
            ]
        );
        assert!(
            trades.is_empty(),
            "causal_rvol_warmup sessions must not enter trade N"
        );
        assert!(
            features.quality.usable,
            "warmup sessions may remain descriptively usable"
        );
    }

    #[test]
    fn acceptance_rvol_snapshot_frozen_against_later_ticks() {
        let mut rvol = elevated_rvol_at(90);
        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        let hit = 8_000.0;
        long.on_tick(&synth_tick(hit - 1_000.0, 109.0), 70, 640, &rvol);
        long.on_tick(&synth_tick(hit, 110.0), 71, 641, &rvol);
        let accept_ts = hit + ONE_SIDED_PERSISTENCE_MS + 1_000.0;
        long.on_tick(&synth_tick(accept_ts, 112.0), 101, 671, &rvol);
        assert_eq!(long.acceptance_ts_ms, Some(accept_ts));
        let snap_ratio = long.rvol_ratio_at_acceptance;
        let snap_class = long.rvol_class_at_acceptance.clone();
        let snap_elev = long.rvol_elevated_or_high_at_acceptance;
        assert!(snap_ratio.is_some());
        assert_eq!(snap_class.as_deref(), Some("High"));
        assert_eq!(snap_elev, Some(true));

        // Later volume must not rewrite the acceptance snapshot.
        rvol.on_trade(1_000_000.0, 120);
        long.on_tick(&synth_tick(accept_ts + 60_000.0, 113.0), 120, 690, &rvol);
        assert_eq!(long.rvol_ratio_at_acceptance, snap_ratio);
        assert_eq!(long.rvol_class_at_acceptance, snap_class);
        assert_eq!(long.rvol_elevated_or_high_at_acceptance, snap_elev);

        long.tick_count = MIN_RTH_TICKS;
        let (features, _) = long.finish();
        assert_eq!(features.rvol_ratio_at_acceptance, snap_ratio);
        assert_eq!(features.rvol_class_at_acceptance, snap_class);
        assert_eq!(features.rvol_elevated_or_high_at_acceptance, snap_elev);
    }

    #[test]
    fn ib_edge_breach_noon_versus_close_are_distinct() {
        let rvol = idle_rvol();
        let mut long = new_accum();
        form_ib_long(&mut long, &rvol, 95.0);
        // After IB, still inside until afternoon — no noon breach.
        long.on_tick(&synth_tick(5_000.0, 99.0), 90, 660, &rvol); // 11:00
        long.on_tick(&synth_tick(6_000.0, 98.0), 120, 690, &rvol); // 11:30
        assert!(first_ib_edge_breach_by_et(
            long.ib_high_breach_ts_ms,
            long.ib_high_breach_et,
            long.ib_low_breach_ts_ms,
            long.ib_low_breach_et,
            NOON_ET_MINUTES,
        )
        .is_none());
        // First breach after noon but before 16:00.
        long.on_tick(&synth_tick(7_000.0, 100.25), 180, 750, &rvol); // 12:30
        let noon = first_ib_edge_breach_by_et(
            long.ib_high_breach_ts_ms,
            long.ib_high_breach_et,
            long.ib_low_breach_ts_ms,
            long.ib_low_breach_et,
            NOON_ET_MINUTES,
        );
        let by_close = first_ib_edge_breach_by_et(
            long.ib_high_breach_ts_ms,
            long.ib_high_breach_et,
            long.ib_low_breach_ts_ms,
            long.ib_low_breach_et,
            CAMPAIGN_CUTOFF_ET_MINUTES - 1,
        );
        assert!(noon.is_none(), "post-noon breach must not count as by-noon");
        assert_eq!(by_close, Some((ExtensionDirection::Up, 7_000.0)));

        // Short mirror: low breach only after noon.
        let mut short = new_accum();
        form_ib_short(&mut short, &rvol, 90.0);
        short.on_tick(&synth_tick(5_000.0, 81.0), 90, 660, &rvol);
        short.on_tick(&synth_tick(7_000.0, 79.75), 180, 750, &rvol);
        assert!(first_ib_edge_breach_by_et(
            short.ib_high_breach_ts_ms,
            short.ib_high_breach_et,
            short.ib_low_breach_ts_ms,
            short.ib_low_breach_et,
            NOON_ET_MINUTES,
        )
        .is_none());
        assert_eq!(
            first_ib_edge_breach_by_et(
                short.ib_high_breach_ts_ms,
                short.ib_high_breach_et,
                short.ib_low_breach_ts_ms,
                short.ib_low_breach_et,
                CAMPAIGN_CUTOFF_ET_MINUTES - 1,
            ),
            Some((ExtensionDirection::Down, 7_000.0))
        );
    }

    #[test]
    fn missing_coverage_and_timestamp_reversal_and_material_gap_reject() {
        let missing_start = evaluate_session_quality(quality_inputs(
            "2026-07-08",
            MIN_RTH_TICKS,
            0,
            0.0,
            Some(600), // 10:00 — too late
            Some(RTH_END_COVERAGE_MIN_ET),
            0,
        ));
        assert!(!missing_start.usable);
        assert!(missing_start
            .exclusion_reasons
            .iter()
            .any(|r| r == "missing_rth_start_coverage"));

        let missing_end = evaluate_session_quality(quality_inputs(
            "2026-07-08",
            MIN_RTH_TICKS,
            0,
            0.0,
            Some(crate::RTH_OPEN_ET),
            Some(800), // 13:20 — too early
            0,
        ));
        assert!(!missing_end.usable);
        assert!(missing_end
            .exclusion_reasons
            .iter()
            .any(|r| r == "missing_rth_end_coverage"));

        let reversed = evaluate_session_quality(quality_inputs(
            "2026-07-08",
            MIN_RTH_TICKS,
            0,
            0.0,
            Some(crate::RTH_OPEN_ET),
            Some(RTH_END_COVERAGE_MIN_ET),
            2,
        ));
        assert!(!reversed.usable);
        assert!(reversed
            .exclusion_reasons
            .iter()
            .any(|r| r.starts_with("non_monotonic_timestamps")));

        let gappy = evaluate_session_quality(quality_inputs(
            "2026-07-08",
            MIN_RTH_TICKS,
            1,
            12.0,
            Some(crate::RTH_OPEN_ET),
            Some(RTH_END_COVERAGE_MIN_ET),
            0,
        ));
        assert!(!gappy.usable);
        assert!(gappy
            .exclusion_reasons
            .iter()
            .any(|r| r.starts_with("material_rth_gaps")));

        // Sequence-level: reversal increments counter and does not process price;
        // gap increments only on accepted advancing ticks.
        let rvol = idle_rvol();
        let mut accum = new_accum();
        accum.on_tick(&synth_tick(10.0, 100.0), 0, crate::RTH_OPEN_ET, &rvol);
        accum.on_tick(&synth_tick(5.0, 999.0), 1, crate::RTH_OPEN_ET + 1, &rvol);
        assert_eq!(accum.non_monotonic_count, 1);
        assert_eq!(
            accum.last_price, 100.0,
            "rejected non-monotonic tick must not update price"
        );
        accum.on_tick(
            &synth_tick(10.0 + MATERIAL_RTH_GAP_MS + 1.0, 101.0),
            10,
            crate::RTH_OPEN_ET + 10,
            &rvol,
        );
        assert!(accum.material_gap_count >= 1);
    }

    #[test]
    fn monotonic_guard_rejection_reaches_session_quality_and_reconciliation() {
        use crate::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};

        let mut guard = MonotonicTickGuard::default();
        let mut accum = new_accum();
        let accept = guard.observe(1_000.0);
        assert_eq!(accept, MonotonicTimestampDecision::Accept);
        assert!(apply_monotonic_guard_to_session(&mut accum, accept));

        let rvol = idle_rvol();
        accum.on_tick(&synth_tick(1_000.0, 95.0), 0, crate::RTH_OPEN_ET, &rvol);
        assert_eq!(accum.non_monotonic_count, 0);

        let reject = guard.observe(999.0);
        assert!(matches!(reject, MonotonicTimestampDecision::Skip(_)));
        assert!(
            !apply_monotonic_guard_to_session(&mut accum, reject),
            "guard Skip must not authorize processing"
        );
        assert_eq!(accum.non_monotonic_count, 1);
        assert_eq!(accum.last_price, 95.0);

        accum.tick_count = MIN_RTH_TICKS;
        accum.material_gap_count = 0;
        accum.max_gap_minutes = 0.0;
        accum.first_rth_et_minutes = Some(crate::RTH_OPEN_ET);
        accum.first_rth_ts_ms = Some(1.0);
        accum.last_rth_et_minutes = Some(RTH_END_COVERAGE_MIN_ET);
        accum.last_rth_ts_ms = Some(2.0);
        let (features, _) = accum.finish();
        assert!(
            !features.quality.usable,
            "monotonic guard rejection must make the session unusable"
        );
        assert!(features
            .quality
            .exclusion_reasons
            .iter()
            .any(|r| r.starts_with("non_monotonic_timestamps")));
        assert_eq!(features.quality.non_monotonic_count, 1);

        let summary = summarize_descriptive(std::slice::from_ref(&features));
        assert_eq!(summary.population_sessions, 1);
        assert_eq!(summary.usable_sessions, 0);
        assert_eq!(summary.excluded_sessions, 1);
        assert!(summary.rejection_counts_reconciled);
        assert!(
            summary
                .exclusion_counts
                .get("non_monotonic_timestamps")
                .copied()
                .unwrap_or(0)
                >= 1
        );
    }

    #[test]
    fn session_close_and_time_exit_ignore_post_cutoff_ticks() {
        let rvol = idle_rvol();
        let mut accum = new_accum();
        form_ib_long(&mut accum, &rvol, 95.0);
        let pre_ts = 50_000.0;
        let pre_px = 97.25;
        accum.on_tick(
            &synth_tick(pre_ts, pre_px),
            300,
            CAMPAIGN_CUTOFF_ET_MINUTES - 1,
            &rvol,
        );
        assert_eq!(accum.pre_cutoff_last_ts_ms, Some(pre_ts));
        assert_eq!(accum.pre_cutoff_last_price, Some(pre_px));

        let post_ts = pre_ts + 60_000.0;
        let post_px = 50.0;
        accum.on_tick(
            &synth_tick(post_ts, post_px),
            301,
            CAMPAIGN_CUTOFF_ET_MINUTES + 5,
            &rvol,
        );
        assert_eq!(accum.pre_cutoff_last_price, Some(pre_px));
        assert_eq!(accum.pre_cutoff_last_ts_ms, Some(pre_ts));
        assert_eq!(
            accum.last_rth_et_minutes,
            Some(CAMPAIGN_CUTOFF_ET_MINUTES + 5),
            "coverage end may still observe post-cutoff ticks"
        );
        assert_eq!(accum.last_price, post_px);

        accum.tick_count = MIN_RTH_TICKS;
        accum.material_gap_count = 0;
        accum.max_gap_minutes = 0.0;
        accum.non_monotonic_count = 0;
        accum.first_rth_et_minutes = Some(crate::RTH_OPEN_ET);
        accum.first_rth_ts_ms = Some(1.0);
        accum.last_rth_et_minutes = Some(CAMPAIGN_CUTOFF_ET_MINUTES + 5);
        accum.last_rth_ts_ms = Some(post_ts);
        let (features, _) = accum.finish();
        assert_eq!(features.session_close, pre_px);
        assert_ne!(features.session_close, post_px);
        assert_eq!(
            features.quality.last_rth_et_minutes,
            Some(CAMPAIGN_CUTOFF_ET_MINUTES + 5)
        );

        let trade = simulate_fixed_r_trade(
            ExtensionDirection::Up,
            pre_ts - 1.0,
            pre_px,
            12.0,
            1.0,
            &[(pre_ts - 1.0, pre_px), (pre_ts, pre_px)],
            pre_ts,
        )
        .unwrap();
        assert_eq!(trade.exit_reason, TradeExitReason::TimeExit);
        assert_eq!(trade.exit_ts_ms, pre_ts);
        assert!((trade.exit_price - pre_px).abs() < 1e-9);
    }

    #[test]
    fn cutoff_time_exit_uses_pre_1600_bound() {
        let entry = 100.0;
        let path = vec![
            (1.0, 100.0),
            (2.0, 100.5), // no stop/target
            (3.0, 100.25),
        ];
        let trade = simulate_fixed_r_trade(
            ExtensionDirection::Up,
            1.0,
            entry,
            12.0,
            1.0,
            &path,
            3.0, // cutoff = last pre-16:00 tick
        )
        .unwrap();
        assert_eq!(trade.exit_reason, TradeExitReason::TimeExit);
        assert_eq!(trade.exit_ts_ms, 3.0);
        assert!((trade.exit_price - 100.25).abs() < 1e-9);
    }

    #[test]
    fn rejection_counts_reconcile_to_population_and_are_monotonic() {
        let usable = SessionIbFeatures {
            session_date: "2026-07-08".into(),
            contract: "NQU6.CME".into(),
            role: WindowRole::Discovery,
            quality: evaluate_session_quality(quality_inputs(
                "2026-07-08",
                MIN_RTH_TICKS,
                0,
                0.0,
                Some(crate::RTH_OPEN_ET),
                Some(RTH_END_COVERAGE_MIN_ET),
                0,
            )),
            ib_high: 100.0,
            ib_low: 90.0,
            ib_range: 10.0,
            ib_mid: 95.0,
            ib_complete_price: Some(96.0),
            ib_complete_ts_ms: Some(1.0),
            close_vs_ib_mid: Some(IbCloseVsMid::Above),
            extreme_order: Some(IbExtremeOrder::LowFirstHighLast),
            next_bar_open_inside_ib: Some(true),
            next_bar_open_price: Some(95.0),
            confluence: IbConfluence::Bullish,
            first_extension_direction: None,
            first_extension_ts_ms: None,
            opposite_extension_ts_ms: None,
            end_extension_state: "None".into(),
            persisted_one_sided_30m: false,
            acceptance_ts_ms: None,
            rvol_ratio_at_acceptance: None,
            rvol_class_at_acceptance: None,
            rvol_elevated_or_high_at_acceptance: None,
            rvol_baseline_session_dates: vec![],
            rvol_baseline_count: 0,
            causal_rvol_warmup: true,
            inferential_eligible: false,
            ib_breach_by_noon_direction: None,
            ib_breach_by_noon_ts_ms: None,
            ib_breach_by_1600_direction: None,
            ib_breach_by_1600_ts_ms: None,
            session_close: 95.0,
            session_high: 100.0,
            session_low: 90.0,
            day_type: "NoExtension".into(),
            continuation_path: "no_extension".into(),
            failure_path: "none".into(),
        };
        assert!(usable.quality.usable);
        let mut excluded = usable.clone();
        excluded.session_date = "2026-07-09".into();
        excluded.quality = evaluate_session_quality(quality_inputs(
            "2026-07-09",
            MIN_RTH_TICKS,
            1,
            6.0,
            Some(600),
            Some(700),
            1,
        ));
        assert!(!excluded.quality.usable);

        let summary1 = summarize_descriptive(std::slice::from_ref(&usable));
        assert!(summary1.rejection_counts_reconciled);
        assert_eq!(summary1.excluded_sessions, 0);

        let summary2 = summarize_descriptive(&[usable, excluded]);
        assert!(summary2.rejection_counts_reconciled);
        assert_eq!(
            summary2.population_sessions,
            summary2.usable_sessions + summary2.excluded_sessions
        );
        assert!(summary2.excluded_sessions >= summary1.excluded_sessions);
        for (k, v1) in &summary1.exclusion_counts {
            let v2 = summary2.exclusion_counts.get(k).copied().unwrap_or(0);
            assert!(v2 >= *v1, "exclusion count for {k} must be monotonic");
        }
    }

    #[test]
    fn metric_specific_reliability_uses_metric_denominator() {
        fn blank_usable(date: &str, next_bar: Option<bool>, one_sided: bool) -> SessionIbFeatures {
            SessionIbFeatures {
                session_date: date.into(),
                contract: "NQM6.CME".into(),
                role: WindowRole::Discovery,
                quality: SessionQuality {
                    usable: true,
                    exclusion_reasons: vec![],
                    tick_count: MIN_RTH_TICKS,
                    material_gap_count: 0,
                    max_gap_minutes: 0.0,
                    holiday_or_early_close: false,
                    first_rth_et_minutes: Some(crate::RTH_OPEN_ET),
                    last_rth_et_minutes: Some(RTH_END_COVERAGE_MIN_ET),
                    rth_start_ts_ms: Some(1.0),
                    rth_end_ts_ms: Some(2.0),
                    has_rth_start_coverage: true,
                    has_rth_end_coverage: true,
                    non_monotonic_count: 0,
                },
                ib_high: 100.0,
                ib_low: 90.0,
                ib_range: 10.0,
                ib_mid: 95.0,
                ib_complete_price: Some(96.0),
                ib_complete_ts_ms: Some(1.0),
                close_vs_ib_mid: Some(IbCloseVsMid::Above),
                extreme_order: Some(IbExtremeOrder::LowFirstHighLast),
                next_bar_open_inside_ib: next_bar,
                next_bar_open_price: next_bar.map(|_| 95.0),
                confluence: IbConfluence::None,
                first_extension_direction: one_sided.then_some(ExtensionDirection::Up),
                first_extension_ts_ms: None,
                opposite_extension_ts_ms: None,
                end_extension_state: if one_sided {
                    "UpOnly".into()
                } else {
                    "None".into()
                },
                persisted_one_sided_30m: one_sided,
                acceptance_ts_ms: None,
                rvol_ratio_at_acceptance: None,
                rvol_class_at_acceptance: None,
                rvol_elevated_or_high_at_acceptance: None,
                rvol_baseline_session_dates: vec![],
                rvol_baseline_count: 0,
                causal_rvol_warmup: true,
                inferential_eligible: false,
                ib_breach_by_noon_direction: None,
                ib_breach_by_noon_ts_ms: None,
                ib_breach_by_1600_direction: None,
                ib_breach_by_1600_ts_ms: None,
                session_close: 95.0,
                session_high: 100.0,
                session_low: 90.0,
                day_type: "NoExtension".into(),
                continuation_path: "no_extension".into(),
                failure_path: "none".into(),
            }
        }

        let mut sessions = Vec::new();
        for i in 0..25 {
            sessions.push(blank_usable(
                &format!("2026-03-{:02}", i + 1),
                if i < 5 { Some(true) } else { None },
                i < 8,
            ));
        }
        let summary = summarize_descriptive(&sessions);
        assert_eq!(summary.usable_sessions, 25);
        assert_eq!(
            summary.reliability_tier,
            reliability_tier(summary.usable_sessions)
        );
        assert_eq!(summary.next_bar_open_inside_n, 5);
        assert_eq!(
            summary.next_bar_open_inside_reliability,
            reliability_tier(5),
            "next-bar reliability must use inside_n, not usable N"
        );
        assert_ne!(
            summary.next_bar_open_inside_reliability,
            summary.reliability_tier
        );
        assert_eq!(summary.one_sided_sessions, 8);
        assert_eq!(
            summary.persisted_one_sided_30m_reliability,
            reliability_tier(8),
            "persisted reliability must use one-sided denominator"
        );
    }

    fn synth_gate_trade(
        gate: GateKind,
        session_date: &str,
        contract: &str,
        direction: ExtensionDirection,
        target_r: f64,
        r_result: f64,
    ) -> GateTradeRow {
        GateTradeRow {
            gate,
            hypothesis_id: format!("{}|{}R", gate.as_str(), target_r as i32),
            session_date: session_date.into(),
            contract: contract.into(),
            role: WindowRole::Discovery,
            target_r,
            trade: SimulatedTrade {
                direction,
                entry_ts_ms: 1.0,
                entry_price: 100.0,
                stop_price: 88.0,
                target_price: 100.0 + 12.0 * target_r,
                risk_points: FIXED_STOP_POINTS,
                target_r,
                exit_ts_ms: 2.0,
                exit_price: 100.0 + 12.0 * r_result,
                exit_reason: TradeExitReason::Target,
                r_result,
                mfe_points: 12.0 * r_result.max(0.0),
                mae_points: 0.0,
                mfe_r: r_result.max(0.0),
                mae_r: 0.0,
                win: r_result > 0.0,
            },
            rvol_ratio_at_entry: Some(1.5),
            day_type: "Trend".into(),
            entry_minute_et: Some(700),
        }
    }

    #[test]
    fn primary_contrast_matches_same_session_direction_key_only() {
        let trades = vec![
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-10",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-10",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.0,
            ),
            // Same session, opposite direction — must not match Up key.
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-10",
                "NQM6.CME",
                ExtensionDirection::Down,
                1.0,
                -1.0,
            ),
            // Treated without control on another date — unmatched.
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-11",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
        ];
        let pairs = build_matched_primary_pairs(&trades);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].key.session_date, "2026-03-10");
        assert_eq!(pairs[0].key.direction, ExtensionDirection::Up);
        assert!((pairs[0].delta_gross_r - 1.0).abs() < 1e-9);
    }

    #[test]
    fn primary_contrast_ignores_overlapping_gates_and_2r_variants() {
        let mut trades = vec![
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-04-01",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-04-01",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.5,
            ),
        ];
        // Overlapping confluence + 2R variants on the same key must not inflate matched N.
        trades.push(synth_gate_trade(
            GateKind::IbConfluence,
            "2026-04-01",
            "NQM6.CME",
            ExtensionDirection::Up,
            1.0,
            2.0,
        ));
        trades.push(synth_gate_trade(
            GateKind::OneSidedAcceptance30m,
            "2026-04-01",
            "NQM6.CME",
            ExtensionDirection::Up,
            2.0,
            2.0,
        ));
        trades.push(synth_gate_trade(
            GateKind::UngatedBaseline,
            "2026-04-01",
            "NQM6.CME",
            ExtensionDirection::Up,
            2.0,
            1.0,
        ));
        let report = build_primary_contrast_report(&trades);
        assert_eq!(report.matched_n, 1);
        assert_eq!(report.target_r, PRIMARY_TARGET_R);
        assert_eq!(report.treated_gate, PRIMARY_TREATED_GATE.as_str());
        assert_eq!(report.control_gate, PRIMARY_CONTROL_GATE.as_str());
        assert!(
            (report.pairs[0].delta_gross_r - 0.5).abs() < 1e-9,
            "delta must come from 1R primary pair only"
        );
    }

    #[test]
    fn cost_scenario_math_uses_labeled_round_trip_points() {
        assert!(
            (apply_round_trip_cost_r(1.0, 0.5, FIXED_STOP_POINTS) - (1.0 - 0.5 / 12.0)).abs()
                < 1e-12
        );
        assert!(
            (apply_round_trip_cost_r(1.0, 1.0, FIXED_STOP_POINTS) - (1.0 - 1.0 / 12.0)).abs()
                < 1e-12
        );

        let trades = vec![
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-05-01",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-05-01",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.0,
            ),
        ];
        let report = build_primary_contrast_report(&trades);
        let labels: Vec<_> = report.scenarios.iter().map(|s| s.label.as_str()).collect();
        assert_eq!(
            labels,
            vec!["gross", "total_cost_0.5_pt", "total_cost_1_pt"]
        );
        let c05 = report
            .scenarios
            .iter()
            .find(|s| (s.cost_points_round_trip - 0.5).abs() < 1e-12)
            .expect("0.5pt scenario");
        let c10 = report
            .scenarios
            .iter()
            .find(|s| (s.cost_points_round_trip - 1.0).abs() < 1e-12)
            .expect("1.0pt scenario");
        assert!(c05.is_scenario_not_actual_fees);
        assert!(c10.is_scenario_not_actual_fees);
        // Both legs pay the same round-trip cost → delta unchanged vs gross.
        assert!((c05.delta_avg_r - 1.0).abs() < 1e-12);
        assert!((c10.delta_avg_r - 1.0).abs() < 1e-12);
        assert!((c05.treated_avg_r - (1.0 - 0.5 / 12.0)).abs() < 1e-12);
        assert!((c10.treated_avg_r - (1.0 - 1.0 / 12.0)).abs() < 1e-12);
    }

    #[test]
    fn block_bootstrap_is_deterministic_with_recorded_seed_and_blocks() {
        let trades = vec![
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-02",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-02",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.0,
            ),
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-09",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.5,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-09",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                -0.5,
            ),
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-06-03",
                "NQU6.CME",
                ExtensionDirection::Down,
                1.0,
                0.25,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-06-03",
                "NQU6.CME",
                ExtensionDirection::Down,
                1.0,
                -0.25,
            ),
        ];
        let a = build_primary_contrast_report(&trades);
        let b = build_primary_contrast_report(&trades);
        assert_eq!(a.weekly_bootstrap.seed, BLOCK_BOOTSTRAP_SEED);
        assert_eq!(a.weekly_bootstrap.replicates, BLOCK_BOOTSTRAP_REPLICATES);
        assert_eq!(a.weekly_bootstrap.block_kind, "weekly");
        assert_eq!(a.contract_bootstrap.block_kind, "contract");
        assert_eq!(
            a.weekly_bootstrap.n_blocks,
            a.weekly_bootstrap.block_ids.len()
        );
        assert_eq!(
            a.contract_bootstrap.n_blocks,
            a.contract_bootstrap.block_ids.len()
        );
        assert!(a.weekly_bootstrap.n_blocks >= 2);
        assert!(a.contract_bootstrap.n_blocks >= 2);
        assert_eq!(a.weekly_bootstrap.ci95_low, b.weekly_bootstrap.ci95_low);
        assert_eq!(a.weekly_bootstrap.ci95_high, b.weekly_bootstrap.ci95_high);
        assert_eq!(a.contract_bootstrap.ci95_low, b.contract_bootstrap.ci95_low);
        assert_eq!(
            a.contract_bootstrap.ci95_high,
            b.contract_bootstrap.ci95_high
        );
        assert_eq!(a.weekly_bootstrap.block_ids, b.weekly_bootstrap.block_ids);
        let gross = a
            .scenarios
            .iter()
            .find(|s| s.cost_points_round_trip == 0.0)
            .expect("gross");
        assert!(
            (a.weekly_bootstrap.point_mean_delta_r - gross.delta_avg_r).abs() < 1e-12,
            "weekly point must equal ordinary matched mean"
        );
        assert!(
            (a.contract_bootstrap.point_mean_delta_r - gross.delta_avg_r).abs() < 1e-12,
            "contract point must equal ordinary matched mean"
        );
        // 3 weekly / 2 contract blocks in this fixture — sparse note only for ≤2.
        assert!(a.weekly_bootstrap.n_blocks >= 2);
        if a.contract_bootstrap.n_blocks <= 2 {
            assert!(a.contract_bootstrap.sparse_block_limitation.is_some());
        }
    }

    #[test]
    fn cluster_bootstrap_respects_unequal_block_sizes_and_ordinary_point_mean() {
        // Block A: two +1 deltas; block B: one -1 delta.
        // Ordinary mean = (1+1-1)/3 = 1/3. Equal-weight block-mean would be 0.
        let ids = vec!["A".into(), "B".into()];
        let obs = vec![vec![1.0, 1.0], vec![-1.0]];
        let ci = block_bootstrap_delta_ci(&ids, &obs, 42, 2000);
        assert_eq!(ci.n_blocks, 2);
        assert_eq!(ci.block_ids, ids);
        assert!((ci.point_mean_delta_r - (1.0 / 3.0)).abs() < 1e-12);
        assert!(ci.sparse_block_limitation.is_some());
        // With replacement over unequal sizes, CI must not collapse to equal-weight 0-only.
        assert!(
            ci.ci95_low < ci.point_mean_delta_r && ci.ci95_high > ci.point_mean_delta_r
                || (ci.ci95_high - ci.ci95_low).abs() > 1e-9,
            "expected non-degenerate CI for unequal blocks: [{}, {}]",
            ci.ci95_low,
            ci.ci95_high
        );
    }

    #[test]
    fn cluster_bootstrap_fixed_seed_is_deterministic_and_resamples_with_replacement() {
        let ids = vec!["w1".into(), "w2".into()];
        let obs = vec![vec![2.0], vec![-1.0]];
        let a = block_bootstrap_delta_ci(&ids, &obs, BLOCK_BOOTSTRAP_SEED, 500);
        let b = block_bootstrap_delta_ci(&ids, &obs, BLOCK_BOOTSTRAP_SEED, 500);
        assert_eq!(a.seed, BLOCK_BOOTSTRAP_SEED);
        assert_eq!(a.ci95_low, b.ci95_low);
        assert_eq!(a.ci95_high, b.ci95_high);
        assert_eq!(a.point_mean_delta_r, b.point_mean_delta_r);
        // Two differing singleton blocks: with-replacement yields means in {-1, 0.5, 2}.
        assert!(
            (a.ci95_high - a.ci95_low).abs() > 1e-9,
            "two-block differing values must yield non-degenerate CI"
        );
        assert!(a.ci95_low < 0.5 || a.ci95_high > 0.5);
        // Prove with-replacement: a replicate mean of exactly 2.0 (both draws block w1)
        // or -1.0 (both draws w2) must appear in the percentile envelope extremes over many reps.
        let wide = block_bootstrap_delta_ci(&ids, &obs, BLOCK_BOOTSTRAP_SEED, 5000);
        assert!(
            wide.ci95_low <= -0.999 || wide.ci95_high >= 1.999,
            "with-replacement must admit same-block repeats; got ci=[{}, {}]",
            wide.ci95_low,
            wide.ci95_high
        );
    }

    #[test]
    fn cluster_bootstrap_two_block_ci_non_degenerate_when_block_values_differ() {
        let ids = vec!["c1".into(), "c2".into()];
        let obs = vec![vec![1.0, 1.0, 1.0], vec![-1.0]];
        let ci = block_bootstrap_delta_ci(&ids, &obs, 7, 3000);
        assert_eq!(ci.n_blocks, 2);
        assert!((ci.point_mean_delta_r - 0.5).abs() < 1e-12); // (1+1+1-1)/4
        assert!(ci.ci95_low < ci.ci95_high);
        assert!(ci
            .sparse_block_limitation
            .as_deref()
            .unwrap()
            .contains("n_blocks=2"));
    }

    #[test]
    fn primary_contrast_point_mean_equals_gross_matched_delta() {
        let trades = vec![
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-02",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                1.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-02",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.0,
            ),
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-03",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                0.0,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-03",
                "NQM6.CME",
                ExtensionDirection::Up,
                1.0,
                -1.0,
            ),
            synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                "2026-03-09",
                "NQU6.CME",
                ExtensionDirection::Down,
                1.0,
                0.5,
            ),
            synth_gate_trade(
                GateKind::UngatedBaseline,
                "2026-03-09",
                "NQU6.CME",
                ExtensionDirection::Down,
                1.0,
                -0.5,
            ),
        ];
        let report = build_primary_contrast_report(&trades);
        let gross = report
            .scenarios
            .iter()
            .find(|s| s.cost_points_round_trip == 0.0)
            .unwrap();
        let ordinary: f64 =
            report.pairs.iter().map(|p| p.delta_gross_r).sum::<f64>() / report.pairs.len() as f64;
        assert!((gross.delta_avg_r - ordinary).abs() < 1e-12);
        assert!((report.weekly_bootstrap.point_mean_delta_r - ordinary).abs() < 1e-12);
        assert!((report.contract_bootstrap.point_mean_delta_r - ordinary).abs() < 1e-12);
    }

    #[test]
    fn v2_verdict_never_accept_even_for_positive_synthetic_results() {
        let mut trades = Vec::new();
        for i in 0..40 {
            let date = format!("2026-03-{:02}", (i % 20) + 1);
            let dir = if i % 2 == 0 {
                ExtensionDirection::Up
            } else {
                ExtensionDirection::Down
            };
            trades.push(synth_gate_trade(
                GateKind::OneSidedAcceptance30m,
                &date,
                "NQM6.CME",
                dir,
                1.0,
                1.0,
            ));
            trades.push(synth_gate_trade(
                GateKind::UngatedBaseline,
                &date,
                "NQM6.CME",
                dir,
                1.0,
                -1.0,
            ));
        }
        let primary = build_primary_contrast_report(&trades);
        assert!(primary.matched_n >= 20);
        let gross = primary
            .scenarios
            .iter()
            .find(|s| s.cost_points_round_trip == 0.0)
            .unwrap();
        assert!(
            gross.delta_avg_r > 0.0,
            "synthetic treated edge must be strongly positive"
        );
        let verdict = decide_v2_verdict(&primary);
        assert_eq!(verdict.result, VERDICT_DEVELOPMENT_RESULT);
        assert_eq!(verdict.disposition, VERDICT_PARK_FORWARD_VALIDATION);
        assert!(verdict.future_holdout_required);
        assert!(
            verdict
                .future_acceptance_policy
                .require_untouched_matched_holdout
        );
        assert!(!verdict.result.contains("ACCEPT"));
        assert!(!verdict.disposition.contains("ACCEPT"));
        assert!(!verdict.rationale.contains("ACCEPT"));
        assert!(
            default_campaign_windows()
                .iter()
                .all(|w| w.role == WindowRole::Discovery),
            "all windows through {} must be development Discovery",
            DEVELOPMENT_END_DATE
        );
    }

    fn overlap_row(
        date: &str,
        front: &str,
        front_vol: f64,
        back: &str,
        back_vol: f64,
    ) -> DailyOverlapVolume {
        DailyOverlapVolume {
            session_date: date.into(),
            front_contract: front.into(),
            front_rth_volume: front_vol,
            back_contract: back.into(),
            back_rth_volume: back_vol,
        }
    }

    #[test]
    fn volume_crossover_selects_first_back_exceeds_front_and_lists_exclusions() {
        let rows = vec![
            overlap_row("2026-03-02", "NQH6.CME", 100.0, "NQM6.CME", 40.0),
            overlap_row("2026-03-03", "NQH6.CME", 90.0, "NQM6.CME", 80.0),
            overlap_row("2026-03-05", "NQH6.CME", 50.0, "NQM6.CME", 120.0),
            overlap_row("2026-03-06", "NQH6.CME", 30.0, "NQM6.CME", 150.0),
        ];
        let t = select_volume_crossover("NQH6.CME", "NQM6.CME", &rows);
        assert_eq!(t.crossover_rule, ROLLOVER_CROSSOVER_RULE);
        assert_eq!(t.crossover_date.as_deref(), Some("2026-03-05"));
        assert_eq!(t.front_last_included_date.as_deref(), Some("2026-03-03"));
        assert_eq!(t.back_first_included_date.as_deref(), Some("2026-03-05"));
        assert!(t.transition_exclusions.is_empty());
        assert_eq!(t.raw_daily_volumes.len(), 4);

        // Tie date between last strict front lead and crossover → transition exclusion.
        let with_tie = vec![
            overlap_row("2026-03-02", "NQH6.CME", 100.0, "NQM6.CME", 10.0),
            overlap_row("2026-03-03", "NQH6.CME", 90.0, "NQM6.CME", 20.0),
            overlap_row("2026-03-04", "NQH6.CME", 70.0, "NQM6.CME", 70.0),
            overlap_row("2026-03-05", "NQH6.CME", 10.0, "NQM6.CME", 200.0),
        ];
        let t2 = select_volume_crossover("NQH6.CME", "NQM6.CME", &with_tie);
        assert_eq!(t2.front_last_included_date.as_deref(), Some("2026-03-03"));
        assert_eq!(t2.crossover_date.as_deref(), Some("2026-03-05"));
        assert_eq!(t2.transition_exclusions, vec!["2026-03-04".to_string()]);
    }

    #[test]
    fn volume_crossover_without_back_lead_excludes_all_overlap_dates() {
        let rows = vec![
            overlap_row("2026-05-20", "NQM6.CME", 100.0, "NQU6.CME", 50.0),
            overlap_row("2026-05-21", "NQM6.CME", 80.0, "NQU6.CME", 70.0),
        ];
        let t = select_volume_crossover("NQM6.CME", "NQU6.CME", &rows);
        assert!(t.crossover_date.is_none());
        assert_eq!(
            t.transition_exclusions,
            vec!["2026-05-20".to_string(), "2026-05-21".to_string()]
        );
    }

    #[test]
    fn build_rollover_evidence_covers_h6_m6_and_m6_u6() {
        let mut map = BTreeMap::new();
        map.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-01", "NQH6.CME", 100.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-02", "NQH6.CME", 20.0, "NQM6.CME", 80.0),
            ],
        );
        map.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-05-28", "NQM6.CME", 90.0, "NQU6.CME", 40.0),
                overlap_row("2026-05-29", "NQM6.CME", 30.0, "NQU6.CME", 100.0),
            ],
        );
        let ev = build_rollover_evidence(&map, "fixture");
        assert_eq!(ev.study_version, 2);
        assert_eq!(ev.development_end_date, DEVELOPMENT_END_DATE);
        assert_eq!(ev.volume_source, "fixture");
        assert_eq!(ev.transitions.len(), 2);
        assert_eq!(
            ev.transitions[0].crossover_date.as_deref(),
            Some("2026-03-02")
        );
        assert_eq!(
            ev.transitions[1].crossover_date.as_deref(),
            Some("2026-05-29")
        );
        require_resolved_rollover_crossovers(&ev).expect("resolved");
        assert!(!ev.crossover_rule_documentation.is_empty());
    }

    #[test]
    fn empty_or_missing_rollover_evidence_is_blocked() {
        let empty = BTreeMap::new();
        let err = resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&empty))
            .expect_err("empty must block");
        assert!(err.contains("BLOCKED"), "{err}");

        let mut missing_pair = BTreeMap::new();
        missing_pair.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![overlap_row(
                "2026-03-01",
                "NQH6.CME",
                100.0,
                "NQM6.CME",
                10.0,
            )],
        );
        let err2 = resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&missing_pair))
            .expect_err("missing M6→U6 must block");
        assert!(err2.contains("BLOCKED"), "{err2}");

        let mut no_cross = BTreeMap::new();
        no_cross.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-01", "NQH6.CME", 100.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-02", "NQH6.CME", 20.0, "NQM6.CME", 80.0),
            ],
        );
        no_cross.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-05-28", "NQM6.CME", 90.0, "NQU6.CME", 40.0),
                overlap_row("2026-05-29", "NQM6.CME", 80.0, "NQU6.CME", 50.0),
            ],
        );
        let err3 = resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&no_cross))
            .expect_err("unresolved crossover must block");
        assert!(err3.contains("BLOCKED"), "{err3}");
        assert!(
            err3.contains("unresolved") || err3.contains("crossover"),
            "{err3}"
        );
    }

    #[test]
    fn production_default_volume_source_is_local_scid_scan() {
        assert!(RolloverVolumeSource::ScanLocalScid.is_production_default());
        assert_eq!(
            RolloverVolumeSource::ScanLocalScid.source_label(),
            "local_scid_overlap_scan"
        );
        let fixture_map = BTreeMap::new();
        assert!(!RolloverVolumeSource::Fixture(&fixture_map).is_production_default());
        assert_eq!(campaign_overlap_scan_windows().len(), 2);
    }

    #[test]
    fn synthetic_crossovers_derive_windows_and_exclusions() {
        let mut map = BTreeMap::new();
        map.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-02", "NQH6.CME", 100.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-03", "NQH6.CME", 90.0, "NQM6.CME", 20.0),
                overlap_row("2026-03-04", "NQH6.CME", 70.0, "NQM6.CME", 70.0),
                overlap_row("2026-03-05", "NQH6.CME", 10.0, "NQM6.CME", 200.0),
            ],
        );
        map.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-05-27", "NQM6.CME", 100.0, "NQU6.CME", 20.0),
                overlap_row("2026-05-28", "NQM6.CME", 80.0, "NQU6.CME", 40.0),
                overlap_row("2026-05-30", "NQM6.CME", 20.0, "NQU6.CME", 90.0),
            ],
        );
        let (ev, windows) =
            resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&map)).expect("resolve");
        assert_eq!(ev.volume_source, "fixture");
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].contract, "NQH6.CME");
        assert_eq!(windows[0].start_date, CAMPAIGN_H6_START_DATE);
        assert_eq!(windows[0].end_date, "2026-03-03");
        assert_eq!(windows[1].contract, "NQM6.CME");
        assert_eq!(windows[1].start_date, "2026-03-05");
        assert_eq!(windows[1].end_date, "2026-05-28");
        assert_eq!(windows[2].contract, "NQU6.CME");
        assert_eq!(windows[2].start_date, "2026-05-30");
        assert_eq!(windows[2].end_date, DEVELOPMENT_END_DATE);
        assert!(windows.iter().all(|w| w.role == WindowRole::Discovery));
        assert!(ev.transitions[0]
            .transition_exclusions
            .contains(&"2026-03-04".to_string()));
        // Friday front_last 2026-05-28 → Monday crossover 2026-05-30 with no observed
        // intervening mutual-overlap: must NOT invent Saturday 2026-05-29.
        assert!(ev.transitions[1].transition_exclusions.is_empty());
        assert!(!ev.transitions[1]
            .transition_exclusions
            .contains(&"2026-05-29".to_string()));
        assert_ne!(windows[0].end_date, default_campaign_windows()[0].end_date);
        assert_ne!(
            windows[1].start_date,
            default_campaign_windows()[1].start_date
        );
    }

    #[test]
    fn friday_to_monday_rollover_does_not_invent_weekend_exclusions() {
        let mut map = BTreeMap::new();
        map.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-12", "NQH6.CME", 100.0, "NQM6.CME", 10.0), // Thu
                overlap_row("2026-03-13", "NQH6.CME", 90.0, "NQM6.CME", 20.0),  // Fri front_last
                overlap_row("2026-03-16", "NQH6.CME", 10.0, "NQM6.CME", 200.0), // Mon crossover
            ],
        );
        map.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-06-11", "NQM6.CME", 100.0, "NQU6.CME", 20.0),
                overlap_row("2026-06-12", "NQM6.CME", 80.0, "NQU6.CME", 40.0),
                overlap_row("2026-06-15", "NQM6.CME", 20.0, "NQU6.CME", 90.0),
            ],
        );
        let (ev, _) =
            resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&map)).expect("resolve");
        let t = &ev.transitions[0];
        assert_eq!(t.front_last_included_date.as_deref(), Some("2026-03-13"));
        assert_eq!(t.crossover_date.as_deref(), Some("2026-03-16"));
        assert!(
            t.transition_exclusions.is_empty(),
            "must not invent Sat/Sun exclusions: {:?}",
            t.transition_exclusions
        );
        assert!(!t.transition_exclusions.iter().any(|d| d == "2026-03-14"));
        assert!(!t.transition_exclusions.iter().any(|d| d == "2026-03-15"));

        // Observed intervening mutual-overlap (tie) is still excluded.
        let mut with_tie = map.clone();
        with_tie.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-12", "NQH6.CME", 100.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-13", "NQH6.CME", 90.0, "NQM6.CME", 20.0),
                overlap_row("2026-03-14", "NQH6.CME", 50.0, "NQM6.CME", 50.0), // observed Sat tie
                overlap_row("2026-03-16", "NQH6.CME", 10.0, "NQM6.CME", 200.0),
            ],
        );
        let (ev2, _) = resolve_rollover_and_windows(RolloverVolumeSource::Fixture(&with_tie))
            .expect("resolve");
        assert_eq!(
            ev2.transitions[0].transition_exclusions,
            vec!["2026-03-14".to_string()]
        );
        assert!(!ev2.transitions[0]
            .transition_exclusions
            .contains(&"2026-03-15".to_string()));
    }

    #[test]
    fn path_safety_rejects_escape_equal_root_and_requires_overwrite() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let v2 = tmp.path().join("v2");
        std::fs::create_dir_all(&v2).unwrap();
        let run = v2.join("run-a");

        // Equal-root refused.
        let eq = resolve_path_strictly_beneath(&v2, &v2);
        assert!(eq.is_err(), "equal-root must be rejected");

        // Traversal escape refused.
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let escape = resolve_path_strictly_beneath(&v2.join("..").join("outside").join("x"), &v2);
        assert!(
            escape.is_err(),
            "traversal escape must be rejected: {escape:?}"
        );

        // Fresh prepare succeeds.
        let prepared = prepare_run_directory(&run, &v2, false).expect("prepare");
        assert!(is_strictly_beneath(
            &prepared,
            &ensure_existing_dir_canonical(&v2).unwrap()
        ));
        let db = create_fresh_isolated_campaign_db(&prepared, V2_ISOLATED_DB_FILENAME, false)
            .expect("db");
        assert!(db.exists());
        assert!(is_strictly_beneath(&db, &prepared));

        // Existing artifact without overwrite refuses.
        let refuse = prepare_run_directory(&run, &v2, false);
        assert!(
            refuse.is_err(),
            "must refuse existing artifacts without overwrite"
        );
        assert!(refuse.unwrap_err().contains("--overwrite"));

        // Overwrite deletes only the validated run-owned DB and allows recreate.
        let prepared2 = prepare_run_directory(&run, &v2, true).expect("overwrite prepare");
        assert!(!prepared2.join(V2_ISOLATED_DB_FILENAME).exists());
        let db2 = create_fresh_isolated_campaign_db(&prepared2, V2_ISOLATED_DB_FILENAME, true)
            .expect("db overwrite");
        assert!(db2.exists());

        // Forbidden live DB path detection.
        assert!(is_forbidden_live_db_path(Path::new(
            r"T:\TheDesk\state\data.db"
        )));
        assert!(!is_forbidden_live_db_path(&db2));
    }

    #[test]
    fn prepare_v2_writes_provenance_and_rollover_without_live_db() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let v2 = tmp.path().join("v2");
        let run = v2.join("run-b");
        let mut vols = BTreeMap::new();
        vols.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-01", "NQH6.CME", 50.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-02", "NQH6.CME", 10.0, "NQM6.CME", 60.0),
            ],
        );
        vols.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-05-28", "NQM6.CME", 90.0, "NQU6.CME", 40.0),
                overlap_row("2026-05-29", "NQM6.CME", 30.0, "NQU6.CME", 100.0),
            ],
        );
        let result = prepare_v2_campaign_run(PrepareV2Request {
            run_dir: &run,
            v2_root: &v2,
            overwrite: false,
            db_filename: V2_ISOLATED_DB_FILENAME,
            git_commit: "deadbeef",
            git_dirty: true,
            commands: vec!["unit-test-prepare".into()],
            volume_source: RolloverVolumeSource::Fixture(&vols),
        })
        .expect("prepare_v2");
        assert_eq!(result.provenance.study_version, 2);
        assert_eq!(result.provenance.git_commit, "deadbeef");
        assert!(!result.provenance.source_file_hashes.is_empty());
        assert!(Path::new(&result.canonical_db_path).exists());
        assert!(Path::new(&result.canonical_run_dir)
            .join("rollover_evidence.json")
            .exists());
        assert!(Path::new(&result.canonical_run_dir)
            .join("provenance.json")
            .exists());
        assert_eq!(
            result.rollover.transitions[0].crossover_date.as_deref(),
            Some("2026-03-02")
        );
        assert_eq!(result.windows.len(), 3);
        assert_eq!(
            result.provenance.windows[0].end_date,
            result.windows[0].end_date
        );
        assert_eq!(result.rollover.volume_source, "fixture");
        // No live DB path leaked into provenance.
        let db_disp = result.provenance.isolated_db_path.unwrap();
        assert!(!db_disp.to_ascii_lowercase().contains(r"state\data.db"));
    }

    /// Bounded local-SCID overlap smoke (machine-dependent; ~seconds–minutes in release).
    /// Run explicitly:
    /// `cargo test --release --lib research::ib_campaign::tests::smoke_bounded_scid_overlap_scan_writes_under_temp_v2_when_available -- --ignored --nocapture`
    #[test]
    #[ignore = "machine-dependent local SCID overlap smoke; run explicitly in --release with --ignored"]
    fn smoke_bounded_scid_overlap_scan_writes_under_temp_v2_when_available() {
        // Soft-skip when local SCIDs are absent (CI / machines without Sierra data).
        let needed = ["NQH6.CME", "NQM6.CME", "NQU6.CME"];
        for c in needed {
            let p = scid_path_for(c);
            if !p.exists() {
                eprintln!(
                    "skip smoke_bounded_scid_overlap_scan: missing {}",
                    p.display()
                );
                return;
            }
        }
        // Exactly one bounded overlap scan for both pairs; reuse the map thereafter.
        let volumes = scan_campaign_rollover_volumes_from_scid().expect("scan volumes once");
        assert_eq!(volumes.len(), 2);
        for rows in volumes.values() {
            assert!(!rows.is_empty());
            assert!(rows
                .iter()
                .all(|r| r.front_rth_volume > 0.0 && r.back_rth_volume > 0.0));
        }
        let (ev, windows) =
            resolve_rollover_and_windows_from_volumes(&volumes, "local_scid_overlap_scan")
                .expect("resolve from single scan");
        assert_eq!(ev.volume_source, "local_scid_overlap_scan");
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[2].end_date, DEVELOPMENT_END_DATE);
        eprintln!(
            "SMOKE H6→M6 crossover={:?} front_last={:?} back_first={:?}",
            ev.transitions[0].crossover_date,
            ev.transitions[0].front_last_included_date,
            ev.transitions[0].back_first_included_date
        );
        eprintln!(
            "SMOKE M6→U6 crossover={:?} front_last={:?} back_first={:?}",
            ev.transitions[1].crossover_date,
            ev.transitions[1].front_last_included_date,
            ev.transitions[1].back_first_included_date
        );
        for w in &windows {
            eprintln!(
                "SMOKE window {} {}..{}",
                w.contract, w.start_date, w.end_date
            );
        }

        // Write evidence only beneath a temporary v2 run (reuse the same collected map).
        let tmp = tempfile::tempdir().expect("tempdir");
        let v2 = tmp.path().join("v2");
        let run = v2.join("smoke-run");
        let prepared = prepare_v2_campaign_run(PrepareV2Request {
            run_dir: &run,
            v2_root: &v2,
            overwrite: false,
            db_filename: V2_ISOLATED_DB_FILENAME,
            git_commit: "smoke",
            git_dirty: true,
            commands: vec!["smoke-scid-overlap".into()],
            volume_source: RolloverVolumeSource::Precollected {
                volumes: &volumes,
                label: "local_scid_overlap_scan",
            },
        })
        .expect("prepare smoke");
        assert_eq!(prepared.rollover.volume_source, "local_scid_overlap_scan");
        assert_eq!(prepared.windows, windows);
        assert!(Path::new(&prepared.canonical_run_dir)
            .join("rollover_evidence.json")
            .exists());
        assert!(is_strictly_beneath(
            Path::new(&prepared.canonical_run_dir),
            &ensure_existing_dir_canonical(&v2).unwrap()
        ));
    }

    #[test]
    fn legacy_run_campaign_is_blocked() {
        let err = run_campaign(Path::new("."), None, "x", false, vec![]).expect_err("blocked");
        assert!(err.contains("BLOCKED"), "{err}");
        assert!(err.contains("execute_v2_campaign"), "{err}");
    }

    #[test]
    fn execute_uses_supplied_windows_and_writes_all_required_artifacts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let v2 = tmp.path().join("v2");
        let run = v2.join("exec-int");
        let v1 = tmp.path().join("v1-parent");
        std::fs::create_dir_all(&v1).unwrap();
        // Minimal preserved v1 report/trades for reconciliation.
        write_json(
            &v1.join("report.json"),
            &serde_json::json!({
                "decision": "PARK",
                "descriptiveAll": {
                    "populationSessions": 10,
                    "usableSessions": 8,
                    "excludedSessions": 2,
                    "oneSidedSessions": 3,
                    "persistedOneSided30m": 2
                }
            }),
        )
        .unwrap();
        write_json(&v1.join("trades.json"), &serde_json::json!([{}, {}, {}])).unwrap();

        let mut vols = BTreeMap::new();
        vols.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                overlap_row("2026-03-01", "NQH6.CME", 50.0, "NQM6.CME", 10.0),
                overlap_row("2026-03-02", "NQH6.CME", 10.0, "NQM6.CME", 60.0),
            ],
        );
        vols.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                overlap_row("2026-05-28", "NQM6.CME", 90.0, "NQU6.CME", 40.0),
                overlap_row("2026-05-29", "NQM6.CME", 30.0, "NQU6.CME", 100.0),
            ],
        );
        let prepared = prepare_v2_campaign_run(PrepareV2Request {
            run_dir: &run,
            v2_root: &v2,
            overwrite: false,
            db_filename: V2_ISOLATED_DB_FILENAME,
            git_commit: "exec-test",
            git_dirty: false,
            commands: vec!["integration-execute".into()],
            volume_source: RolloverVolumeSource::Fixture(&vols),
        })
        .expect("prepare");

        let supplied_windows = prepared.windows.clone();
        let report = execute_v2_campaign_with_sessions(
            ExecuteV2Request {
                run_dir: Path::new(&prepared.canonical_run_dir),
                isolated_db_path: Path::new(&prepared.canonical_db_path),
                windows: supplied_windows.clone(),
                rollover: prepared.rollover.clone(),
                git_commit: "exec-test",
                git_dirty: false,
                commands: vec!["integration-execute --execute".into()],
                v1_artifact_dir: &v1,
            },
            &[],
            &[],
        )
        .expect("execute with sessions");

        assert_eq!(report.provenance.windows, supplied_windows);
        assert_eq!(report.provenance.study_version, 2);
        assert_eq!(report.verdict.result, VERDICT_DEVELOPMENT_RESULT);
        assert_eq!(report.verdict.disposition, VERDICT_PARK_FORWARD_VALIDATION);
        assert!(!report.verdict.result.contains("ACCEPT"));

        let run_path = Path::new(&prepared.canonical_run_dir);
        for name in [
            "sessions.json",
            "trades.json",
            "report.json",
            "rollover_evidence.json",
            "provenance.json",
            "REPORT.md",
            "v1-v2-count-reconciliation.json",
        ] {
            assert!(
                run_path.join(name).exists(),
                "missing required artifact {name}"
            );
            if name.ends_with(".json") {
                let text = std::fs::read_to_string(run_path.join(name)).unwrap();
                let _: serde_json::Value = serde_json::from_str(&text).expect(name);
            }
        }

        let recon: V1V2CountReconciliation = serde_json::from_str(
            &std::fs::read_to_string(run_path.join("v1-v2-count-reconciliation.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(recon.v1_label, V1_RESULT_LABEL);
        assert_eq!(recon.v1_decision_ignored.as_deref(), Some("PARK"));
        assert_eq!(recon.v1_counts.trade_rows, Some(3));
        assert_eq!(recon.v2_counts.matched_primary_n, Some(0));
        assert!(!recon.note.to_lowercase().contains("accept as market"));
    }
}
