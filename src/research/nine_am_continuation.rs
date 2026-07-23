//! IDEA-025D — 9AM hour continuation (directional bias / event-probability study).
//!
//! Stage 1: deterministic core, artifact schema, path safety, fixture tests.
//! Stage 2: local-SCID rollover windows + one-pass bounded SCID replay.
//!
//! Primary NY outcome reproduces nqstats exactly: last observed trade strictly
//! before 16:00 minus first observed trade at/after 09:30. Desk 16:15 is only a
//! separately labeled sensitivity. This study never fabricates a stop/target
//! system and is structurally incapable of emitting ACCEPT.

use crate::feed::load_feed_config;
use crate::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};
use crate::feed::scid_reader::{ScanControl, ScidReader};
use crate::research::ib_campaign::{
    self, create_fresh_isolated_campaign_db, delete_run_owned_target, hash_current_binary,
    is_forbidden_live_db_path, require_resolved_rollover_crossovers, resolve_path_strictly_beneath,
    resolve_rollover_and_windows, sha256_file, strip_verbatim_prefix, write_json, ContractWindow,
    FileSha256, RolloverEvidence, RolloverVolumeSource, DEVELOPMENT_END_DATE,
    EXCLUDED_SESSION_DATES, FORBIDDEN_LIVE_DB_PATHS, MATERIAL_RTH_GAP_MS,
};
use crate::research::{reliability_tier, ReliabilityTier};
use crate::tick_time_context_from_timestamp_ms;
use chrono::{NaiveDate, TimeZone};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const STUDY_ID: &str = "IDEA-025D-9am-continuation";
pub const STUDY_VERSION: i64 = 1;

pub const VERDICT_DEVELOPMENT_RESULT: &str = "DEVELOPMENT_RESULT";
pub const VERDICT_PARK_FORWARD_VALIDATION: &str = "PARK_FORWARD_VALIDATION";

/// Isolated campaign root (Stage 1+2). Runs must be strictly beneath this path.
pub const V1_CAMPAIGN_ROOT: &str =
    r"T:\TheDesk\temp\backtests\nine-am-continuation-campaign-2026-07-22\v1";

pub const V1_ISOLATED_DB_FILENAME: &str = "nine-am-continuation.db";

pub const V1_RUN_ARTIFACT_NAMES: &[&str] = &[
    "sessions.json",
    "report.json",
    "rollover_evidence.json",
    "provenance.json",
    "REPORT.md",
    "count-reconciliation.json",
    V1_ISOLATED_DB_FILENAME,
];

/// Half-open signal hour [09:00, 10:00) in ET minutes since midnight.
pub const SIGNAL_HOUR_START_ET: i32 = 9 * 60;
pub const SIGNAL_HOUR_END_ET: i32 = 10 * 60;

/// Canonical NY equity session open (inclusive) and close bound (exclusive).
pub const NY_OPEN_ET: i32 = 9 * 60 + 30;
pub const NY_CLOSE_EXCLUSIVE_ET: i32 = 16 * 60;

/// Desk RTH close used only for labeled 16:15 sensitivity (never primary).
pub const DESK_RTH_CLOSE_EXCLUSIVE_ET: i32 = 16 * 60 + 15;

/// Futures trade-session open/close for secondary full-session outcome.
pub const FUTURES_SESSION_OPEN_ET: i32 = 18 * 60;
pub const FUTURES_SESSION_CLOSE_EXCLUSIVE_ET: i32 = 17 * 60;

/// External nqstats benchmarks for 09:00 hour (metadata only; never blended).
pub const SOURCE_BENCHMARK_GREEN_NY_CONTINUATION_PCT: f64 = 70.6;
pub const SOURCE_BENCHMARK_RED_NY_CONTINUATION_PCT: f64 = 62.8;
pub const SOURCE_BENCHMARK_GREEN_FULL_CONTINUATION_PCT: f64 = 69.2;
pub const SOURCE_BENCHMARK_RED_FULL_CONTINUATION_PCT: f64 = 59.1;
pub const SOURCE_BENCHMARK_TOTAL_SESSIONS: usize = 2_472;

/// Deterministic seed reserved for Stage-2 cluster bootstrap of lift (if used).
pub const LIFT_BOOTSTRAP_SEED: u64 = 0x025D_9A00_2026;

/// Maximum accepted lag from an ideal boundary clock for liquid NQ ticks.
///
/// - Open boundaries (`>= T`): first print must satisfy `0 <= ts - T_ms <= MAX`.
/// - Close boundaries (`< T`): last print must satisfy `0 < T_ms - ts <= MAX`.
pub const MAX_BOUNDARY_LAG_MS: f64 = 60_000.0;

/// Intra-window material-gap scan uses the IB campaign threshold.
pub const METRIC_MATERIAL_GAP_MS: f64 = MATERIAL_RTH_GAP_MS;

// ---------------------------------------------------------------------------
// Direction / observed prints
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CandleDirection {
    Green,
    Red,
    Doji,
}

impl CandleDirection {
    pub fn from_open_close(open: f64, close: f64) -> Self {
        if close > open {
            Self::Green
        } else if close < open {
            Self::Red
        } else {
            Self::Doji
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Green => "green",
            Self::Red => "red",
            Self::Doji => "doji",
        }
    }

    pub fn continues(self, outcome: Self) -> Option<bool> {
        match (self, outcome) {
            (Self::Green, Self::Green) | (Self::Red, Self::Red) => Some(true),
            (Self::Green, Self::Red) | (Self::Red, Self::Green) => Some(false),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservedPrint {
    pub timestamp_ms: f64,
    pub price: f64,
    pub et_minutes: i32,
}

impl ObservedPrint {
    pub fn new(timestamp_ms: f64, price: f64, et_minutes: i32) -> Self {
        Self {
            timestamp_ms,
            price,
            et_minutes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalHourBar {
    pub open: ObservedPrint,
    pub close: ObservedPrint,
    pub direction: CandleDirection,
    pub return_points: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeWindow {
    pub label: String,
    pub open: ObservedPrint,
    pub close: ObservedPrint,
    pub direction: CandleDirection,
    pub return_points: f64,
    /// True only for the canonical primary (strict pre-16:00).
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExcursionDiagnostics {
    pub label: String,
    pub from_price: f64,
    pub signal_direction: String,
    /// Signal-adjusted maximum favorable excursion (points).
    pub mfe_points: f64,
    /// Signal-adjusted maximum adverse excursion (points; typically ≤ 0).
    pub mae_points: f64,
    /// Raw max(price − entry); separately labeled, not signal-adjusted.
    pub raw_max_up_points: f64,
    /// Raw min(price − entry); separately labeled, not signal-adjusted.
    pub raw_max_down_points: f64,
    pub note: String,
}

// ---------------------------------------------------------------------------
// Session row / quality
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SessionQualityFlags {
    /// Session survives holiday / monotonic / any fatal material-gap policy.
    pub fatal_quality_ok: bool,
    /// Primary-study usable := fatal_ok ∧ signal ∧ ny_primary (does NOT gate secondary metrics).
    pub usable: bool,
    pub exclusion_reasons: Vec<String>,
    pub holiday_or_partial: bool,
    pub monotonic_rejections: u64,
    pub material_gap: bool,
    pub material_gap_windows: Vec<String>,
    pub tick_count: u64,
    pub signal_eligible: bool,
    pub ny_primary_eligible: bool,
    pub ny_desk_1615_eligible: bool,
    pub full_session_eligible: bool,
    pub has_signal_open: bool,
    pub has_signal_close: bool,
    pub has_ny_open: bool,
    pub has_ny_close_pre_1600: bool,
    pub has_desk_close_pre_1615: bool,
    pub has_futures_open_1800: bool,
    pub has_futures_close_pre_1700: bool,
    pub stale_or_missing_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionContinuationRow {
    pub ny_date: String,
    pub contract: String,
    pub quality: SessionQualityFlags,
    pub signal: Option<SignalHourBar>,
    /// Canonical primary: 09:30 open → strict pre-16:00 close.
    pub ny_primary: Option<OutcomeWindow>,
    /// Separately labeled Desk-RTH sensitivity (pre-16:15); never drives primary.
    pub ny_desk_1615_sensitivity: Option<OutcomeWindow>,
    /// Secondary: 18:00 open (prior evening) → strict pre-17:00 close on NY date.
    pub full_session: Option<OutcomeWindow>,
    pub post_1000_to_1600_excursion: Option<ExcursionDiagnostics>,
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectionBucketStats {
    pub signal_direction: String,
    pub outcome_label: String,
    pub wins: usize,
    pub losses: usize,
    pub doji_outcomes: usize,
    pub excluded: usize,
    /// Exact metric-specific N for the continuation rate (= wins + losses).
    pub n: usize,
    pub continuation_rate: f64,
    pub wilson_ci95: [f64; 2],
    pub unconditional_base_rate: f64,
    pub lift_vs_base: f64,
    pub reliability_tier: ReliabilityTier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PooledContinuationStats {
    pub label: String,
    pub wins: usize,
    pub losses: usize,
    pub n: usize,
    pub continuation_rate: f64,
    pub wilson_ci95: [f64; 2],
    pub reliability_tier: ReliabilityTier,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExcursionSummary {
    pub n: usize,
    pub avg_mfe_points: f64,
    pub avg_mae_points: f64,
    pub avg_raw_max_up_points: f64,
    pub avg_raw_max_down_points: f64,
    /// Explicit diagnostic eligibility (must match `n` filter).
    pub eligibility_label: String,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PopulationSummary {
    pub population_sessions: usize,
    pub usable_sessions: usize,
    /// Exact `population_sessions - usable_sessions` (primary exclusions only).
    pub excluded_sessions: usize,
    /// Reasons that prevent primary usability (fatal / signal / NY-primary only).
    /// Does NOT include desk-16:15 or full-session-only failures on primary-usable rows.
    pub primary_exclusion_counts: BTreeMap<String, usize>,
    /// Reasons preventing desk-16:15 metric eligibility (fatal / signal / desk).
    pub desk_1615_exclusion_counts: BTreeMap<String, usize>,
    /// Reasons preventing full-session metric eligibility (fatal / signal / futures).
    pub full_session_exclusion_counts: BTreeMap<String, usize>,
    pub green_signal_n: usize,
    pub red_signal_n: usize,
    pub doji_signal_n: usize,
    pub ny_primary_denominator: usize,
    pub ny_desk_1615_denominator: usize,
    pub full_session_denominator: usize,
    pub reliability_tier: ReliabilityTier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StudyContract {
    pub study_id: String,
    pub study_version: i64,
    pub development_end_date: String,
    pub signal_hour: String,
    pub primary_ny_outcome: String,
    pub desk_1615_sensitivity: String,
    pub full_session_outcome: String,
    pub primary_is_trade_system: bool,
    pub accept_structurally_impossible: bool,
    pub source_benchmarks: SourceBenchmarks,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceBenchmarks {
    pub source_page: String,
    pub total_sessions_reported: usize,
    pub green_ny_continuation_pct: f64,
    pub red_ny_continuation_pct: f64,
    pub green_full_continuation_pct: f64,
    pub red_full_continuation_pct: f64,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignVerdict {
    pub result: String,
    pub disposition: String,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvenanceCoverage {
    pub population_sessions: usize,
    pub usable_sessions: usize,
    pub excluded_sessions: usize,
    pub primary_exclusion_counts: BTreeMap<String, usize>,
    pub desk_1615_exclusion_counts: BTreeMap<String, usize>,
    pub full_session_exclusion_counts: BTreeMap<String, usize>,
    pub green_ny_n: usize,
    pub red_ny_n: usize,
    pub full_session_denominator: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProvenanceCrossover {
    pub from_contract: String,
    pub to_contract: String,
    pub crossover_date: Option<String>,
    pub front_last_included_date: Option<String>,
    pub back_first_included_date: Option<String>,
    pub transition_exclusions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignProvenance {
    pub git_commit: String,
    pub git_dirty: bool,
    pub study_id: String,
    pub study_version: i64,
    pub development_end_date: String,
    pub study_contract: StudyContract,
    pub campaign_root: String,
    pub artifact_dir: String,
    pub canonical_artifact_dir: String,
    pub isolated_db_path: Option<String>,
    pub canonical_isolated_db_path: Option<String>,
    pub windows: Vec<ContractWindow>,
    pub crossovers: Vec<ProvenanceCrossover>,
    pub excluded_session_dates: Vec<String>,
    pub transition_exclusions: Vec<String>,
    pub coverage: ProvenanceCoverage,
    pub commands: Vec<String>,
    pub source_file_hashes: Vec<FileSha256>,
    pub binary_path: Option<String>,
    pub binary_sha256: Option<String>,
    pub stage: String,
    pub execute_implemented: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountReconciliation {
    pub study_id: String,
    pub study_version: i64,
    pub population_sessions: usize,
    pub usable_sessions: usize,
    pub excluded_sessions: usize,
    pub primary_exclusion_counts: BTreeMap<String, usize>,
    pub desk_1615_exclusion_counts: BTreeMap<String, usize>,
    pub full_session_exclusion_counts: BTreeMap<String, usize>,
    pub green_ny_n: usize,
    pub red_ny_n: usize,
    pub full_session_n: usize,
    pub excursion_diagnostic_n: usize,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CampaignReport {
    pub study_contract: StudyContract,
    pub provenance: CampaignProvenance,
    pub population: PopulationSummary,
    pub green_ny_primary: DirectionBucketStats,
    pub red_ny_primary: DirectionBucketStats,
    pub green_full_session: DirectionBucketStats,
    pub red_full_session: DirectionBucketStats,
    pub green_ny_desk_1615_sensitivity: DirectionBucketStats,
    pub red_ny_desk_1615_sensitivity: DirectionBucketStats,
    pub pooled_ny_primary: PooledContinuationStats,
    pub excursion_summary: ExcursionSummary,
    pub verdict: CampaignVerdict,
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Wilson + study contract / verdict
// ---------------------------------------------------------------------------

/// Wilson score interval (95%, z=1.96) for a binomial proportion.
pub fn wilson_ci95(wins: usize, n: usize) -> [f64; 2] {
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

pub fn study_contract() -> StudyContract {
    StudyContract {
        study_id: STUDY_ID.into(),
        study_version: STUDY_VERSION,
        development_end_date: DEVELOPMENT_END_DATE.into(),
        signal_hour: "half-open [09:00,10:00) ET; open=first>=09:00; close=last<10:00".into(),
        primary_ny_outcome:
            "NY equity session return = last observed strictly before 16:00 MINUS first observed at/after 09:30"
                .into(),
        desk_1615_sensitivity:
            "Separately labeled Desk-RTH sensitivity: last observed strictly before 16:15 minus first observed >=09:30; never primary; uses its own unconditional base rates"
                .into(),
        full_session_outcome:
            "Futures trade session: 18:00 ET open (prior evening / trading_day map) vs last observed strictly before 17:00 on NY date"
                .into(),
        primary_is_trade_system: false,
        accept_structurally_impossible: true,
        source_benchmarks: SourceBenchmarks {
            source_page: "https://nqstats.com/1h_continuation.html".into(),
            total_sessions_reported: SOURCE_BENCHMARK_TOTAL_SESSIONS,
            green_ny_continuation_pct: SOURCE_BENCHMARK_GREEN_NY_CONTINUATION_PCT,
            red_ny_continuation_pct: SOURCE_BENCHMARK_RED_NY_CONTINUATION_PCT,
            green_full_continuation_pct: SOURCE_BENCHMARK_GREEN_FULL_CONTINUATION_PCT,
            red_full_continuation_pct: SOURCE_BENCHMARK_RED_FULL_CONTINUATION_PCT,
            note: "External benchmarks only; never blended into local estimates.".into(),
        },
    }
}

/// Fixed Stage-1/2 verdict. Structurally incapable of ACCEPT.
pub fn decide_verdict(population: &PopulationSummary) -> CampaignVerdict {
    CampaignVerdict {
        result: VERDICT_DEVELOPMENT_RESULT.into(),
        disposition: VERDICT_PARK_FORWARD_VALIDATION.into(),
        rationale: format!(
            "IDEA-025D v{STUDY_VERSION} is a directional-bias/event-probability development study \
             through {DEVELOPMENT_END_DATE} (population={}, usable={}). ACCEPT is structurally \
             impossible; PARK_FORWARD_VALIDATION until an untouched holdout exists.",
            population.population_sessions, population.usable_sessions
        ),
    }
}

// ---------------------------------------------------------------------------
// Boundary extraction (observed ticks only) + freshness
// ---------------------------------------------------------------------------

/// Ideal open clock ms for `date` at `boundary_et` minutes since midnight ET.
pub fn boundary_clock_ms(date: &str, boundary_et_minutes: i32) -> Result<f64, String> {
    let h = (boundary_et_minutes / 60) as u32;
    let m = (boundary_et_minutes % 60) as u32;
    et_wall_to_utc_ms(date, h, m, 0)
}

/// Open freshness: first print at/after boundary within `MAX_BOUNDARY_LAG_MS`.
pub fn open_boundary_is_fresh(print: &ObservedPrint, date: &str, boundary_et: i32) -> bool {
    let Ok(target) = boundary_clock_ms(date, boundary_et) else {
        return false;
    };
    let lag = print.timestamp_ms - target;
    (0.0..=MAX_BOUNDARY_LAG_MS).contains(&lag)
}

/// Close freshness: last print strictly before exclusive end, within `MAX_BOUNDARY_LAG_MS`.
pub fn close_boundary_is_fresh(print: &ObservedPrint, date: &str, end_exclusive_et: i32) -> bool {
    let Ok(target) = boundary_clock_ms(date, end_exclusive_et) else {
        return false;
    };
    let lag = target - print.timestamp_ms;
    lag > 0.0 && lag <= MAX_BOUNDARY_LAG_MS
}

/// First observed print with `et_minutes >= start` and `< end` (half-open window).
pub fn first_observed_in_half_open(
    ticks: &[(f64, f64, i32)],
    start_et: i32,
    end_et: i32,
) -> Option<ObservedPrint> {
    ticks
        .iter()
        .find(|(_, _, et)| *et >= start_et && *et < end_et)
        .map(|(ts, px, et)| ObservedPrint::new(*ts, *px, *et))
}

/// Last observed print with `et_minutes` strictly before `end_et` and `>= start_et`.
pub fn last_observed_before(
    ticks: &[(f64, f64, i32)],
    start_et: i32,
    end_exclusive_et: i32,
) -> Option<ObservedPrint> {
    ticks
        .iter()
        .rev()
        .find(|(_, _, et)| *et >= start_et && *et < end_exclusive_et)
        .map(|(ts, px, et)| ObservedPrint::new(*ts, *px, *et))
}

/// Last observed print strictly before `end_exclusive_et` (no lower bound).
pub fn last_observed_strictly_before(
    ticks: &[(f64, f64, i32)],
    end_exclusive_et: i32,
) -> Option<ObservedPrint> {
    ticks
        .iter()
        .rev()
        .find(|(_, _, et)| *et < end_exclusive_et)
        .map(|(ts, px, et)| ObservedPrint::new(*ts, *px, *et))
}

fn material_gap_in_et_window(ticks: &[(f64, f64, i32)], start_et: i32, end_et: i32) -> bool {
    let mut last: Option<f64> = None;
    for (ts, _, et) in ticks {
        if *et < start_et || *et >= end_et {
            continue;
        }
        if let Some(prev) = last {
            if *ts - prev > METRIC_MATERIAL_GAP_MS {
                return true;
            }
        }
        last = Some(*ts);
    }
    false
}

pub fn build_signal_hour(
    ticks: &[(f64, f64, i32)],
    ny_date: &str,
) -> Result<SignalHourBar, Vec<String>> {
    let mut reasons = Vec::new();
    let Some(open) = first_observed_in_half_open(ticks, SIGNAL_HOUR_START_ET, SIGNAL_HOUR_END_ET)
    else {
        reasons.push("missing_signal_open".into());
        return Err(reasons);
    };
    if !open_boundary_is_fresh(&open, ny_date, SIGNAL_HOUR_START_ET) {
        reasons.push("stale_signal_open".into());
    }
    let Some(close) = last_observed_before(ticks, SIGNAL_HOUR_START_ET, SIGNAL_HOUR_END_ET) else {
        reasons.push("missing_signal_close".into());
        return Err(reasons);
    };
    if close.timestamp_ms < open.timestamp_ms {
        reasons.push("missing_signal_close".into());
        return Err(reasons);
    }
    if !close_boundary_is_fresh(&close, ny_date, SIGNAL_HOUR_END_ET) {
        reasons.push("stale_signal_close".into());
    }
    if material_gap_in_et_window(ticks, SIGNAL_HOUR_START_ET, SIGNAL_HOUR_END_ET) {
        reasons.push("material_gap_signal_hour".into());
    }
    if !reasons.is_empty() {
        return Err(reasons);
    }
    Ok(SignalHourBar {
        return_points: close.price - open.price,
        direction: CandleDirection::from_open_close(open.price, close.price),
        open,
        close,
    })
}

pub fn build_ny_primary(
    ticks: &[(f64, f64, i32)],
    ny_date: &str,
) -> Result<OutcomeWindow, Vec<String>> {
    let mut reasons = Vec::new();
    let Some(open) = first_observed_in_half_open(ticks, NY_OPEN_ET, NY_CLOSE_EXCLUSIVE_ET) else {
        reasons.push("missing_ny_open_0930".into());
        return Err(reasons);
    };
    if !open_boundary_is_fresh(&open, ny_date, NY_OPEN_ET) {
        reasons.push("stale_ny_open_0930".into());
    }
    let Some(close) = last_observed_strictly_before(ticks, NY_CLOSE_EXCLUSIVE_ET) else {
        reasons.push("missing_ny_close_pre_1600".into());
        return Err(reasons);
    };
    if close.timestamp_ms < open.timestamp_ms || close.et_minutes < NY_OPEN_ET {
        reasons.push("missing_ny_close_pre_1600".into());
        return Err(reasons);
    }
    if !close_boundary_is_fresh(&close, ny_date, NY_CLOSE_EXCLUSIVE_ET) {
        reasons.push("stale_ny_close_pre_1600".into());
    }
    if material_gap_in_et_window(ticks, NY_OPEN_ET, NY_CLOSE_EXCLUSIVE_ET) {
        reasons.push("material_gap_ny_primary".into());
    }
    if !reasons.is_empty() {
        return Err(reasons);
    }
    Ok(OutcomeWindow {
        label: "ny_equity_session_pre_1600".into(),
        return_points: close.price - open.price,
        direction: CandleDirection::from_open_close(open.price, close.price),
        open,
        close,
        is_primary: true,
    })
}

pub fn build_ny_desk_1615_sensitivity(
    ticks: &[(f64, f64, i32)],
    ny_date: &str,
) -> Result<OutcomeWindow, Vec<String>> {
    let mut reasons = Vec::new();
    let Some(open) = first_observed_in_half_open(ticks, NY_OPEN_ET, DESK_RTH_CLOSE_EXCLUSIVE_ET)
    else {
        reasons.push("missing_desk_open_0930".into());
        return Err(reasons);
    };
    if !open_boundary_is_fresh(&open, ny_date, NY_OPEN_ET) {
        reasons.push("stale_desk_open_0930".into());
    }
    let Some(close) = last_observed_strictly_before(ticks, DESK_RTH_CLOSE_EXCLUSIVE_ET) else {
        reasons.push("missing_desk_close_pre_1615".into());
        return Err(reasons);
    };
    if close.timestamp_ms < open.timestamp_ms || close.et_minutes < NY_OPEN_ET {
        reasons.push("missing_desk_close_pre_1615".into());
        return Err(reasons);
    }
    if !close_boundary_is_fresh(&close, ny_date, DESK_RTH_CLOSE_EXCLUSIVE_ET) {
        reasons.push("stale_desk_close_pre_1615".into());
    }
    if material_gap_in_et_window(ticks, NY_OPEN_ET, DESK_RTH_CLOSE_EXCLUSIVE_ET) {
        reasons.push("material_gap_desk_1615".into());
    }
    if !reasons.is_empty() {
        return Err(reasons);
    }
    Ok(OutcomeWindow {
        label: "ny_desk_rth_sensitivity_pre_1615".into(),
        return_points: close.price - open.price,
        direction: CandleDirection::from_open_close(open.price, close.price),
        open,
        close,
        is_primary: false,
    })
}

/// Map NY date → prior calendar evening that hosts the 18:00 Globex open.
pub fn futures_open_calendar_date_for_ny_date(ny_date: &str) -> Result<String, String> {
    let d = NaiveDate::parse_from_str(ny_date, "%Y-%m-%d")
        .map_err(|_| format!("invalid NY date: {ny_date}"))?;
    Ok((d - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string())
}

/// Build secondary full-session outcome from open ticks (prior evening) + close ticks (NY date).
pub fn build_full_session_outcome(
    open_ticks: &[(f64, f64, i32)],
    close_ticks: &[(f64, f64, i32)],
    ny_date: &str,
) -> Result<OutcomeWindow, Vec<String>> {
    let mut reasons = Vec::new();
    let prior = match futures_open_calendar_date_for_ny_date(ny_date) {
        Ok(p) => p,
        Err(e) => {
            reasons.push(e);
            return Err(reasons);
        }
    };
    let Some(open) = first_observed_in_half_open(open_ticks, FUTURES_SESSION_OPEN_ET, 24 * 60)
    else {
        reasons.push("missing_futures_open_1800".into());
        return Err(reasons);
    };
    if !open_boundary_is_fresh(&open, &prior, FUTURES_SESSION_OPEN_ET) {
        reasons.push("stale_futures_open_1800".into());
    }
    if material_gap_in_et_window(open_ticks, FUTURES_SESSION_OPEN_ET, 24 * 60) {
        reasons.push("material_gap_futures_open".into());
    }
    let Some(close) =
        last_observed_strictly_before(close_ticks, FUTURES_SESSION_CLOSE_EXCLUSIVE_ET)
    else {
        reasons.push("missing_futures_close_pre_1700".into());
        return Err(reasons);
    };
    if close.timestamp_ms <= open.timestamp_ms {
        reasons.push("missing_futures_close_pre_1700".into());
        return Err(reasons);
    }
    if !close_boundary_is_fresh(&close, ny_date, FUTURES_SESSION_CLOSE_EXCLUSIVE_ET) {
        reasons.push("stale_futures_close_pre_1700".into());
    }
    // Close-side gap after the signal hour through pre-17:00 (same NY calendar day).
    // Intentionally starts at 10:00 so a missing 09:30 (primary-only failure) does not
    // contaminate the secondary full-session metric denominator.
    if material_gap_in_et_window(
        close_ticks,
        SIGNAL_HOUR_END_ET,
        FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
    ) {
        reasons.push("material_gap_futures_close".into());
    }
    if !reasons.is_empty() {
        return Err(reasons);
    }
    Ok(OutcomeWindow {
        label: "futures_trade_session_1800_to_pre_1700".into(),
        return_points: close.price - open.price,
        direction: CandleDirection::from_open_close(open.price, close.price),
        open,
        close,
        is_primary: false,
    })
}

/// Post-10:00 excursion to strict pre-16:00 from signal close (diagnostic only).
///
/// MFE/MAE are signal-direction adjusted: for red signals, declines are favorable.
pub fn post_signal_excursion_to_1600(
    signal_close: &ObservedPrint,
    signal_direction: CandleDirection,
    ticks: &[(f64, f64, i32)],
) -> ExcursionDiagnostics {
    let mut raw_up = 0.0_f64;
    let mut raw_down = 0.0_f64;
    let mut mfe = 0.0_f64;
    let mut mae = 0.0_f64;
    for (ts, px, et) in ticks {
        if *ts <= signal_close.timestamp_ms {
            continue;
        }
        if *et >= NY_CLOSE_EXCLUSIVE_ET || *et < SIGNAL_HOUR_END_ET {
            continue;
        }
        let raw = *px - signal_close.price;
        raw_up = raw_up.max(raw);
        raw_down = raw_down.min(raw);
        let signed = match signal_direction {
            CandleDirection::Green | CandleDirection::Doji => raw,
            CandleDirection::Red => -raw,
        };
        mfe = mfe.max(signed);
        mae = mae.min(signed);
    }
    ExcursionDiagnostics {
        label: "post_1000_to_pre_1600_from_signal_close_signal_adjusted".into(),
        from_price: signal_close.price,
        signal_direction: signal_direction.as_str().into(),
        mfe_points: mfe,
        mae_points: mae,
        raw_max_up_points: raw_up,
        raw_max_down_points: raw_down,
        note: "Diagnostic only — signal-adjusted MFE/MAE; raw extrema labeled separately; not a funded trade edge.".into(),
    }
}

// ---------------------------------------------------------------------------
// Fixture session evaluation (Stage 1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FixtureTick {
    pub timestamp_ms: f64,
    pub price: f64,
}

/// Convert an ET wall-clock on `YYYY-MM-DD` to UTC epoch ms (DST-aware).
pub fn et_wall_to_utc_ms(date: &str, hour: u32, minute: u32, second: u32) -> Result<f64, String> {
    let d =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| format!("invalid date: {date}"))?;
    let naive = d
        .and_hms_opt(hour, minute, second)
        .ok_or_else(|| format!("invalid time {hour}:{minute}:{second}"))?;
    let dt = Eastern
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| {
            format!("ambiguous/invalid local ET datetime {date}T{hour:02}:{minute:02}:{second:02}")
        })?;
    Ok(dt.timestamp_millis() as f64)
}

/// Evaluate one NY-date session from observed fixture ticks (no SCID I/O).
///
/// `ny_date_ticks` — prints whose calendar ET date equals `ny_date` (signal/NY/close).
/// `prior_evening_ticks` — prints on the calendar evening before `ny_date` (18:00 open).
pub fn evaluate_fixture_session(
    ny_date: &str,
    contract: &str,
    ny_date_ticks: &[FixtureTick],
    prior_evening_ticks: &[FixtureTick],
) -> SessionContinuationRow {
    let mut reasons = Vec::new();
    let mut stale_or_missing = Vec::new();
    let mut gap_windows = Vec::new();
    let holiday = EXCLUDED_SESSION_DATES.contains(&ny_date);
    if holiday {
        reasons.push("holiday_or_partial_session_calendar".into());
    }

    let mut guard = MonotonicTickGuard::default();
    let mut mono_rej = 0u64;
    let mut ordered: Vec<FixtureTick> = Vec::new();
    let all: Vec<FixtureTick> = prior_evening_ticks
        .iter()
        .chain(ny_date_ticks.iter())
        .cloned()
        .collect();

    for t in &all {
        match guard.observe(t.timestamp_ms) {
            MonotonicTimestampDecision::Accept => ordered.push(t.clone()),
            MonotonicTimestampDecision::Skip(_) => mono_rej += 1,
        }
    }
    ordered.sort_by(|a, b| {
        a.timestamp_ms
            .partial_cmp(&b.timestamp_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if mono_rej > 0 {
        reasons.push("non_monotonic_timestamps".into());
    }

    let ny_triples: Vec<(f64, f64, i32)> = ordered
        .iter()
        .filter_map(|t| {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms)?;
            if ctx.session_date == ny_date {
                Some((t.timestamp_ms, t.price, ctx.et_minutes))
            } else {
                None
            }
        })
        .collect();
    let prior_triples: Vec<(f64, f64, i32)> = ordered
        .iter()
        .filter_map(|t| {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms)?;
            let prior = futures_open_calendar_date_for_ny_date(ny_date).ok()?;
            if ctx.session_date == prior {
                Some((t.timestamp_ms, t.price, ctx.et_minutes))
            } else {
                None
            }
        })
        .collect();

    let signal_res = build_signal_hour(&ny_triples, ny_date);
    let ny_res = build_ny_primary(&ny_triples, ny_date);
    let desk_res = build_ny_desk_1615_sensitivity(&ny_triples, ny_date);
    let full_res = build_full_session_outcome(&prior_triples, &ny_triples, ny_date);

    let signal = signal_res.as_ref().ok().cloned();
    let ny_primary = ny_res.as_ref().ok().cloned();
    let ny_desk = desk_res.as_ref().ok().cloned();
    let full = full_res.as_ref().ok().cloned();

    let mut push_metric_reasons = |rs: &[String]| {
        for r in rs {
            if r.starts_with("material_gap_") {
                gap_windows.push(r.clone());
            } else {
                stale_or_missing.push(r.clone());
            }
            if !reasons.iter().any(|x| x == r) {
                reasons.push(r.clone());
            }
        }
    };
    if let Err(rs) = &signal_res {
        push_metric_reasons(rs);
    }
    if let Err(rs) = &ny_res {
        push_metric_reasons(rs);
    }
    if let Err(rs) = &desk_res {
        push_metric_reasons(rs);
    }
    if let Err(rs) = &full_res {
        push_metric_reasons(rs);
    }
    gap_windows.sort();
    gap_windows.dedup();
    stale_or_missing.sort();
    stale_or_missing.dedup();

    let excursion = signal
        .as_ref()
        .map(|s| post_signal_excursion_to_1600(&s.close, s.direction, &ny_triples));

    let fatal_quality_ok = !holiday && mono_rej == 0;
    let signal_eligible = fatal_quality_ok && signal.is_some();
    let ny_primary_eligible = fatal_quality_ok && ny_primary.is_some();
    let ny_desk_1615_eligible = fatal_quality_ok && ny_desk.is_some();
    let full_session_eligible = fatal_quality_ok && full.is_some();
    // Primary usable does not gate secondary metric denominators.
    let usable = signal_eligible && ny_primary_eligible;

    let quality = SessionQualityFlags {
        fatal_quality_ok,
        usable,
        exclusion_reasons: reasons,
        holiday_or_partial: holiday,
        monotonic_rejections: mono_rej,
        material_gap: !gap_windows.is_empty(),
        material_gap_windows: gap_windows,
        tick_count: ny_triples.len() as u64,
        signal_eligible,
        ny_primary_eligible,
        ny_desk_1615_eligible,
        full_session_eligible,
        has_signal_open: signal.is_some(),
        has_signal_close: signal.is_some(),
        has_ny_open: ny_primary.is_some(),
        has_ny_close_pre_1600: ny_primary.is_some(),
        has_desk_close_pre_1615: ny_desk.is_some(),
        has_futures_open_1800: full
            .as_ref()
            .map(|f| f.open.et_minutes >= FUTURES_SESSION_OPEN_ET)
            .unwrap_or(false),
        has_futures_close_pre_1700: full.is_some(),
        stale_or_missing_reasons: stale_or_missing,
    };

    SessionContinuationRow {
        ny_date: ny_date.into(),
        contract: contract.into(),
        quality,
        signal,
        ny_primary,
        ny_desk_1615_sensitivity: ny_desk,
        full_session: full,
        post_1000_to_1600_excursion: excursion,
        warnings: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

fn empty_bucket(signal: &str, outcome_label: &str) -> DirectionBucketStats {
    DirectionBucketStats {
        signal_direction: signal.into(),
        outcome_label: outcome_label.into(),
        wins: 0,
        losses: 0,
        doji_outcomes: 0,
        excluded: 0,
        n: 0,
        continuation_rate: 0.0,
        wilson_ci95: [0.0, 0.0],
        unconditional_base_rate: 0.0,
        lift_vs_base: 0.0,
        reliability_tier: ReliabilityTier::Insufficient,
    }
}

fn finalize_bucket(mut b: DirectionBucketStats, base: f64) -> DirectionBucketStats {
    b.n = b.wins + b.losses;
    b.continuation_rate = if b.n == 0 {
        0.0
    } else {
        b.wins as f64 / b.n as f64
    };
    b.wilson_ci95 = wilson_ci95(b.wins, b.n);
    b.unconditional_base_rate = base;
    b.lift_vs_base = b.continuation_rate - base;
    b.reliability_tier = reliability_tier(b.n);
    b
}

fn outcome_base_rates(
    sessions: &[SessionContinuationRow],
    eligible: impl Fn(&SessionContinuationRow) -> bool,
    outcome_of: impl Fn(&SessionContinuationRow) -> Option<&OutcomeWindow>,
) -> (f64, f64) {
    let mut up = 0usize;
    let mut down = 0usize;
    for s in sessions.iter().filter(|s| eligible(s)) {
        if let Some(out) = outcome_of(s) {
            match out.direction {
                CandleDirection::Green => up += 1,
                CandleDirection::Red => down += 1,
                CandleDirection::Doji => {}
            }
        }
    }
    let denom = up + down;
    if denom == 0 {
        (0.0, 0.0)
    } else {
        (up as f64 / denom as f64, down as f64 / denom as f64)
    }
}

fn accumulate_direction_bucket(
    sessions: &[SessionContinuationRow],
    want: CandleDirection,
    metric_eligible: impl Fn(&SessionContinuationRow) -> bool,
    outcome_of: impl Fn(&SessionContinuationRow) -> Option<&OutcomeWindow>,
    outcome_label: &str,
    base_for_want: f64,
) -> DirectionBucketStats {
    let mut b = empty_bucket(want.as_str(), outcome_label);
    for s in sessions {
        if !s.quality.fatal_quality_ok {
            b.excluded += 1;
            continue;
        }
        if !s.quality.signal_eligible {
            b.excluded += 1;
            continue;
        }
        let Some(ref sig) = s.signal else {
            b.excluded += 1;
            continue;
        };
        if sig.direction != want {
            continue;
        }
        if !metric_eligible(s) {
            b.excluded += 1;
            continue;
        }
        let Some(out) = outcome_of(s) else {
            b.excluded += 1;
            continue;
        };
        match sig.direction.continues(out.direction) {
            Some(true) => b.wins += 1,
            Some(false) => b.losses += 1,
            None => b.doji_outcomes += 1,
        }
    }
    finalize_bucket(b, base_for_want)
}

/// Metric bucket for an exclusion reason string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExclusionReasonClass {
    /// Blocks all metrics (holiday / non-monotonic).
    Fatal,
    /// Blocks signal → primary and secondary denominators that require signal.
    Signal,
    /// Blocks NY primary (and therefore primary usable).
    NyPrimary,
    /// Desk-16:15 sensitivity only.
    Desk1615,
    /// Full-session secondary only.
    FullSession,
}

fn classify_exclusion_reason(reason: &str) -> ExclusionReasonClass {
    if reason == "holiday_or_partial_session_calendar" || reason == "non_monotonic_timestamps" {
        return ExclusionReasonClass::Fatal;
    }
    if reason.contains("signal") {
        return ExclusionReasonClass::Signal;
    }
    if reason.contains("desk") {
        return ExclusionReasonClass::Desk1615;
    }
    if reason.contains("futures") {
        return ExclusionReasonClass::FullSession;
    }
    // missing_ny_*, stale_ny_*, material_gap_ny_primary, etc.
    ExclusionReasonClass::NyPrimary
}

fn reason_blocks_primary(class: ExclusionReasonClass) -> bool {
    matches!(
        class,
        ExclusionReasonClass::Fatal
            | ExclusionReasonClass::Signal
            | ExclusionReasonClass::NyPrimary
    )
}

fn reason_blocks_desk(class: ExclusionReasonClass) -> bool {
    matches!(
        class,
        ExclusionReasonClass::Fatal | ExclusionReasonClass::Signal | ExclusionReasonClass::Desk1615
    )
}

fn reason_blocks_full(class: ExclusionReasonClass) -> bool {
    matches!(
        class,
        ExclusionReasonClass::Fatal
            | ExclusionReasonClass::Signal
            | ExclusionReasonClass::FullSession
    )
}

fn bump_reason(map: &mut BTreeMap<String, usize>, reason: &str) {
    *map.entry(reason.to_string()).or_default() += 1;
}

pub fn summarize_population(sessions: &[SessionContinuationRow]) -> PopulationSummary {
    let population_sessions = sessions.len();
    let usable_sessions = sessions.iter().filter(|s| s.quality.usable).count();
    let mut primary_exclusion_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut desk_1615_exclusion_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut full_session_exclusion_counts: BTreeMap<String, usize> = BTreeMap::new();

    for s in sessions {
        let desk_ok = s.quality.signal_eligible && s.quality.ny_desk_1615_eligible;
        let full_ok = s.quality.signal_eligible && s.quality.full_session_eligible;
        for r in &s.quality.exclusion_reasons {
            let class = classify_exclusion_reason(r);
            if !s.quality.usable && reason_blocks_primary(class) {
                bump_reason(&mut primary_exclusion_counts, r);
            }
            if !desk_ok && reason_blocks_desk(class) {
                bump_reason(&mut desk_1615_exclusion_counts, r);
            }
            if !full_ok && reason_blocks_full(class) {
                bump_reason(&mut full_session_exclusion_counts, r);
            }
        }
    }

    let mut green = 0usize;
    let mut red = 0usize;
    let mut doji = 0usize;
    let mut ny_n = 0usize;
    let mut desk_n = 0usize;
    let mut full_n = 0usize;
    for s in sessions {
        if s.quality.signal_eligible {
            if let Some(ref sig) = s.signal {
                match sig.direction {
                    CandleDirection::Green => green += 1,
                    CandleDirection::Red => red += 1,
                    CandleDirection::Doji => doji += 1,
                }
            }
        }
        if s.quality.signal_eligible && s.quality.ny_primary_eligible {
            ny_n += 1;
        }
        if s.quality.signal_eligible && s.quality.ny_desk_1615_eligible {
            desk_n += 1;
        }
        if s.quality.signal_eligible && s.quality.full_session_eligible {
            full_n += 1;
        }
    }
    PopulationSummary {
        population_sessions,
        usable_sessions,
        excluded_sessions: population_sessions.saturating_sub(usable_sessions),
        primary_exclusion_counts,
        desk_1615_exclusion_counts,
        full_session_exclusion_counts,
        green_signal_n: green,
        red_signal_n: red,
        doji_signal_n: doji,
        ny_primary_denominator: ny_n,
        ny_desk_1615_denominator: desk_n,
        full_session_denominator: full_n,
        reliability_tier: reliability_tier(usable_sessions),
    }
}

pub fn build_campaign_report(
    sessions: &[SessionContinuationRow],
    provenance: CampaignProvenance,
    warnings: Vec<String>,
) -> CampaignReport {
    let population = summarize_population(sessions);
    let (ny_up, ny_down) = outcome_base_rates(
        sessions,
        |s| s.quality.fatal_quality_ok && s.quality.ny_primary_eligible,
        |s| s.ny_primary.as_ref(),
    );
    let (desk_up, desk_down) = outcome_base_rates(
        sessions,
        |s| s.quality.fatal_quality_ok && s.quality.ny_desk_1615_eligible,
        |s| s.ny_desk_1615_sensitivity.as_ref(),
    );
    let (full_up, full_down) = outcome_base_rates(
        sessions,
        |s| s.quality.fatal_quality_ok && s.quality.full_session_eligible,
        |s| s.full_session.as_ref(),
    );

    let green_ny = accumulate_direction_bucket(
        sessions,
        CandleDirection::Green,
        |s| s.quality.ny_primary_eligible,
        |s| s.ny_primary.as_ref(),
        "ny_equity_session_pre_1600",
        ny_up,
    );
    let red_ny = accumulate_direction_bucket(
        sessions,
        CandleDirection::Red,
        |s| s.quality.ny_primary_eligible,
        |s| s.ny_primary.as_ref(),
        "ny_equity_session_pre_1600",
        ny_down,
    );
    let green_full = accumulate_direction_bucket(
        sessions,
        CandleDirection::Green,
        |s| s.quality.full_session_eligible,
        |s| s.full_session.as_ref(),
        "futures_trade_session_1800_to_pre_1700",
        full_up,
    );
    let red_full = accumulate_direction_bucket(
        sessions,
        CandleDirection::Red,
        |s| s.quality.full_session_eligible,
        |s| s.full_session.as_ref(),
        "futures_trade_session_1800_to_pre_1700",
        full_down,
    );
    let green_desk = accumulate_direction_bucket(
        sessions,
        CandleDirection::Green,
        |s| s.quality.ny_desk_1615_eligible,
        |s| s.ny_desk_1615_sensitivity.as_ref(),
        "ny_desk_rth_sensitivity_pre_1615",
        desk_up,
    );
    let red_desk = accumulate_direction_bucket(
        sessions,
        CandleDirection::Red,
        |s| s.quality.ny_desk_1615_eligible,
        |s| s.ny_desk_1615_sensitivity.as_ref(),
        "ny_desk_rth_sensitivity_pre_1615",
        desk_down,
    );

    let pooled_wins = green_ny.wins + red_ny.wins;
    let pooled_losses = green_ny.losses + red_ny.losses;
    let pooled_n = pooled_wins + pooled_losses;
    let pooled = PooledContinuationStats {
        label: "pooled_direction_correct_ny_primary".into(),
        wins: pooled_wins,
        losses: pooled_losses,
        n: pooled_n,
        continuation_rate: if pooled_n == 0 {
            0.0
        } else {
            pooled_wins as f64 / pooled_n as f64
        },
        wilson_ci95: wilson_ci95(pooled_wins, pooled_n),
        reliability_tier: reliability_tier(pooled_n),
        note: "Secondary pooled metric only; primary reporting is green/red separate.".into(),
    };

    let verdict = decide_verdict(&population);
    let excursion_summary = summarize_excursions(sessions);
    CampaignReport {
        study_contract: study_contract(),
        provenance,
        population,
        green_ny_primary: green_ny,
        red_ny_primary: red_ny,
        green_full_session: green_full,
        red_full_session: red_full,
        green_ny_desk_1615_sensitivity: green_desk,
        red_ny_desk_1615_sensitivity: red_desk,
        pooled_ny_primary: pooled,
        excursion_summary,
        verdict,
        warnings,
    }
}

/// Diagnostic post-10:00→pre-16:00 excursion summary.
///
/// Eligibility: `quality.usable` only (fatal_ok ∧ signal ∧ ny_primary). Excluded /
/// non-monotonic / holiday / stale-primary rows never enter MFE/MAE averages.
pub const EXCURSION_ELIGIBILITY_LABEL: &str =
    "primary_usable_only: quality.usable (fatal_ok ∧ signal ∧ ny_primary)";

fn summarize_excursions(sessions: &[SessionContinuationRow]) -> ExcursionSummary {
    let mut n = 0usize;
    let mut mfe = 0.0;
    let mut mae = 0.0;
    let mut raw_up = 0.0;
    let mut raw_down = 0.0;
    for s in sessions {
        if !s.quality.usable {
            continue;
        }
        if let Some(ref e) = s.post_1000_to_1600_excursion {
            n += 1;
            mfe += e.mfe_points;
            mae += e.mae_points;
            raw_up += e.raw_max_up_points;
            raw_down += e.raw_max_down_points;
        }
    }
    ExcursionSummary {
        n,
        avg_mfe_points: if n == 0 { 0.0 } else { mfe / n as f64 },
        avg_mae_points: if n == 0 { 0.0 } else { mae / n as f64 },
        avg_raw_max_up_points: if n == 0 { 0.0 } else { raw_up / n as f64 },
        avg_raw_max_down_points: if n == 0 {
            0.0
        } else {
            raw_down / n as f64
        },
        eligibility_label: EXCURSION_ELIGIBILITY_LABEL.into(),
        note: format!(
            "Diagnostic signal-adjusted post-10:00→pre-16:00 excursion; eligibility={EXCURSION_ELIGIBILITY_LABEL}; not a funded trade edge."
        ),
    }
}

// ---------------------------------------------------------------------------
// Path safety / prepare / provenance (Stage 2 ready)
// ---------------------------------------------------------------------------

pub fn campaign_source_file_paths() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    vec![
        root.join("src")
            .join("research")
            .join("nine_am_continuation.rs"),
        root.join("src")
            .join("bin")
            .join("the-desk-nine-am-continuation.rs"),
    ]
}

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

fn run_owned_artifact_path(run_dir: &Path, name: &str) -> Result<PathBuf, String> {
    if name.contains(['/', '\\']) || name == ".." || name == "." || name.is_empty() {
        return Err(format!("invalid artifact basename: {name}"));
    }
    let p = run_dir.join(name);
    if p.parent().map(|x| x.as_os_str()) != Some(run_dir.as_os_str()) {
        // Compare via display-normalized keys when parents differ only by slash style.
        let parent = p
            .parent()
            .ok_or_else(|| "artifact has no parent".to_string())?;
        let a = strip_verbatim_prefix(parent);
        let b = strip_verbatim_prefix(run_dir);
        if a != b {
            return Err(format!("artifact parent mismatch: {}", p.display()));
        }
    }
    Ok(p)
}

/// Prepare a run directory under the IDEA-025D v1 root. Existing artifacts need `--overwrite`.
pub fn prepare_run_directory(
    run_dir: &Path,
    campaign_root: &Path,
    overwrite: bool,
) -> Result<PathBuf, String> {
    let resolved = resolve_path_strictly_beneath(run_dir, campaign_root)?;
    if resolved.exists() {
        let mut existing = Vec::new();
        for name in V1_RUN_ARTIFACT_NAMES {
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
        let again = resolve_path_strictly_beneath(&resolved, campaign_root)?;
        return Ok(again);
    }
    let canon = std::fs::canonicalize(&resolved)
        .map_err(|e| format!("canonicalize run dir {}: {e}", resolved.display()))?;
    Ok(strip_verbatim_prefix(&canon))
}

#[derive(Debug, Clone)]
pub struct PrepareV1Request<'a> {
    pub run_dir: &'a Path,
    pub campaign_root: &'a Path,
    pub overwrite: bool,
    pub db_filename: &'a str,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
    /// Production default: `RolloverVolumeSource::ScanLocalScid`.
    pub volume_source: RolloverVolumeSource<'a>,
}

#[derive(Debug, Clone)]
pub struct PrepareV1Result {
    pub canonical_run_dir: String,
    pub canonical_db_path: String,
    pub provenance: CampaignProvenance,
    pub rollover: RolloverEvidence,
    pub windows: Vec<ContractWindow>,
}

#[derive(Debug, Clone)]
pub struct ExecuteV1Request<'a> {
    pub run_dir: &'a Path,
    pub isolated_db_path: &'a Path,
    pub windows: Vec<ContractWindow>,
    pub rollover: RolloverEvidence,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
    pub campaign_root: &'a Path,
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

fn scid_path_for(symbol: &str) -> PathBuf {
    let cfg = load_feed_config();
    PathBuf::from(cfg.sierra_data_dir.trim_end_matches(['\\', '/'])).join(format!("{symbol}.scid"))
}

fn empty_coverage() -> ProvenanceCoverage {
    ProvenanceCoverage {
        population_sessions: 0,
        usable_sessions: 0,
        excluded_sessions: 0,
        primary_exclusion_counts: BTreeMap::new(),
        desk_1615_exclusion_counts: BTreeMap::new(),
        full_session_exclusion_counts: BTreeMap::new(),
        green_ny_n: 0,
        red_ny_n: 0,
        full_session_denominator: 0,
    }
}

fn crossovers_from_rollover(evidence: &RolloverEvidence) -> Vec<ProvenanceCrossover> {
    evidence
        .transitions
        .iter()
        .map(|t| ProvenanceCrossover {
            from_contract: t.from_contract.clone(),
            to_contract: t.to_contract.clone(),
            crossover_date: t.crossover_date.clone(),
            front_last_included_date: t.front_last_included_date.clone(),
            back_first_included_date: t.back_first_included_date.clone(),
            transition_exclusions: t.transition_exclusions.clone(),
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn build_provenance(
    req: &PrepareV1Request<'_>,
    canonical_run: &Path,
    canonical_db: Option<&Path>,
    windows: &[ContractWindow],
    transition_exclusions: &[String],
    rollover: &RolloverEvidence,
    stage: &str,
    execute_implemented: bool,
) -> Result<CampaignProvenance, String> {
    let (binary_path, binary_sha256) = match hash_current_binary() {
        Ok((p, h)) => (Some(p), Some(h)),
        Err(_) => (None, None),
    };
    Ok(CampaignProvenance {
        git_commit: req.git_commit.into(),
        git_dirty: req.git_dirty,
        study_id: STUDY_ID.into(),
        study_version: STUDY_VERSION,
        development_end_date: DEVELOPMENT_END_DATE.into(),
        study_contract: study_contract(),
        campaign_root: req.campaign_root.display().to_string(),
        artifact_dir: canonical_run.display().to_string(),
        canonical_artifact_dir: canonical_run.display().to_string(),
        isolated_db_path: canonical_db.map(|p| p.display().to_string()),
        canonical_isolated_db_path: canonical_db.map(|p| p.display().to_string()),
        windows: windows.to_vec(),
        crossovers: crossovers_from_rollover(rollover),
        excluded_session_dates: EXCLUDED_SESSION_DATES
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
        transition_exclusions: transition_exclusions.to_vec(),
        coverage: empty_coverage(),
        commands: req.commands.clone(),
        source_file_hashes: hash_campaign_sources()?,
        binary_path,
        binary_sha256,
        stage: stage.into(),
        execute_implemented,
    })
}

/// Prepare: path safety, isolated DB, rollover resolve, provenance (no SCID study replay).
pub fn prepare_v1_campaign_run(req: PrepareV1Request<'_>) -> Result<PrepareV1Result, String> {
    if is_forbidden_live_db_path(req.run_dir) {
        return Err(format!(
            "refusing forbidden live DB path as run dir: {}",
            req.run_dir.display()
        ));
    }
    for forbidden in FORBIDDEN_LIVE_DB_PATHS {
        if req.db_filename.eq_ignore_ascii_case("data.db") {
            return Err(format!(
                "refusing live DB filename default; forbidden paths include {forbidden}"
            ));
        }
    }
    let (rollover, windows) = resolve_rollover_and_windows(req.volume_source)?;
    require_resolved_rollover_crossovers(&rollover)?;
    let transition_exclusions = collect_transition_exclusions(&rollover);
    let canonical_run = prepare_run_directory(req.run_dir, req.campaign_root, req.overwrite)?;
    let canonical_db =
        create_fresh_isolated_campaign_db(&canonical_run, req.db_filename, req.overwrite)?;
    let provenance = build_provenance(
        &req,
        &canonical_run,
        Some(&canonical_db),
        &windows,
        &transition_exclusions,
        &rollover,
        "stage2_prepare",
        true,
    )?;
    write_json(&canonical_run.join("provenance.json"), &provenance)?;
    write_json(&canonical_run.join("rollover_evidence.json"), &rollover)?;
    Ok(PrepareV1Result {
        canonical_run_dir: canonical_run.display().to_string(),
        canonical_db_path: canonical_db.display().to_string(),
        provenance,
        rollover,
        windows,
    })
}

fn calendar_day_before(date: &str) -> Result<String, String> {
    let d =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| format!("invalid date: {date}"))?;
    Ok((d - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string())
}

/// Keep first + last print per UTC second so boundary freshness and 5-minute
/// material-gap detection stay correct without buffering every NQ print.
fn push_second_downsampled(buf: &mut Vec<FixtureTick>, t: FixtureTick) {
    let sec = (t.timestamp_ms / 1000.0).floor() as i64;
    if buf.is_empty() {
        buf.push(t);
        return;
    }
    let last_sec = (buf.last().unwrap().timestamp_ms / 1000.0).floor() as i64;
    if sec != last_sec {
        buf.push(t);
        return;
    }
    if buf.len() >= 2 {
        let prev_sec = (buf[buf.len() - 2].timestamp_ms / 1000.0).floor() as i64;
        if prev_sec == sec {
            *buf.last_mut().unwrap() = t;
            return;
        }
    }
    buf.push(t);
}

/// One-pass SCID replay for a crossover-derived contract window.
///
/// Scan starts one calendar day before `window.start_date` so Sunday 18:00 is
/// available for a Monday NY date. Ticks at/after 18:00 ET are attributed to
/// `trading_day` prior-evening buffers; earlier ticks go to calendar `session_date`.
///
/// Only metric-relevant ET ranges are retained, downsampled to <=2 prints/second,
/// then finalized with the same `evaluate_fixture_session` rules as fixtures.
pub fn replay_nine_am_contract_window(
    window: &ContractWindow,
) -> Result<(Vec<SessionContinuationRow>, Vec<String>), String> {
    let path = scid_path_for(&window.contract);
    if !path.exists() {
        return Err(format!("SCID not found: {}", path.display()));
    }
    let cfg = load_feed_config();
    let reader = ScidReader::with_price_scale(&path, cfg.price_scale);
    let scan_start = calendar_day_before(&window.start_date)?;
    let (start_ms, end_ms) =
        crate::backfill::parse_backfill_date_range(Some(&scan_start), Some(&window.end_date))
            .map_err(|e| e.to_string())?;

    let mut warnings = Vec::new();
    if let Ok(bounds) = reader.file_timestamp_bounds() {
        let (first, last) = bounds;
        if let (Some(s), Some(e), Some(f), Some(l)) = (start_ms, end_ms, first, last) {
            if l < s || f >= e {
                warnings.push(format!(
                    "scid_window_mismatch: {} file vs window {}..{}",
                    window.contract, window.start_date, window.end_date
                ));
            }
        }
    }

    // ny_date -> (prior_evening, ny_day_ticks) — metric windows only, second-downsampled.
    let mut days: BTreeMap<String, (Vec<FixtureTick>, Vec<FixtureTick>)> = BTreeMap::new();
    let mut records = 0usize;
    let mut last_progress = 0usize;

    reader
        .scan_range_in_file_order(start_ms, end_ms, |tick| {
            records += 1;
            if records - last_progress >= 2_000_000 {
                last_progress = records;
                eprintln!(
                    "  … scanned {}M records (ny-dates so far {})",
                    records / 1_000_000,
                    days.len()
                );
            }
            let Some(ctx) = tick_time_context_from_timestamp_ms(tick.timestamp_ms) else {
                return Ok(ScanControl::Continue);
            };
            let ft = FixtureTick {
                timestamp_ms: tick.timestamp_ms,
                price: tick.price,
            };
            if ctx.et_minutes >= FUTURES_SESSION_OPEN_ET {
                // Prior-evening open path for trading_day (18:00->midnight).
                push_second_downsampled(
                    &mut days.entry(ctx.trading_day.clone()).or_default().0,
                    ft,
                );
            } else if ctx.et_minutes >= SIGNAL_HOUR_START_ET
                && ctx.et_minutes < FUTURES_SESSION_CLOSE_EXCLUSIVE_ET
            {
                // Signal / NY primary / desk-1615 / futures close path (09:00->17:00).
                push_second_downsampled(
                    &mut days.entry(ctx.session_date.clone()).or_default().1,
                    ft,
                );
            }
            Ok(ScanControl::Continue)
        })
        .map_err(|e| e.to_string())?;

    let mut sessions = Vec::new();
    for (date, (prior, ny)) in days {
        if date.as_str() < window.start_date.as_str() || date.as_str() > window.end_date.as_str() {
            continue;
        }
        if date.as_str() > DEVELOPMENT_END_DATE {
            continue;
        }
        sessions.push(evaluate_fixture_session(
            &date,
            &window.contract,
            &ny,
            &prior,
        ));
    }
    sessions.sort_by(|a, b| {
        a.ny_date
            .cmp(&b.ny_date)
            .then_with(|| a.contract.cmp(&b.contract))
    });
    Ok((sessions, warnings))
}

/// Write complete Stage-2 campaign artifacts (no Stage-1 skeleton warnings).
pub fn write_campaign_artifacts(
    run_dir: &Path,
    sessions: &[SessionContinuationRow],
    provenance: &CampaignProvenance,
    rollover: &RolloverEvidence,
    warnings: Vec<String>,
) -> Result<CampaignReport, String> {
    let mut provenance = provenance.clone();
    provenance.crossovers = crossovers_from_rollover(rollover);
    provenance.development_end_date = DEVELOPMENT_END_DATE.into();
    provenance.study_contract = study_contract();
    // Re-hash sources at write time so hashes match post-fmt disk bytes.
    provenance.source_file_hashes = hash_campaign_sources()?;
    if let Ok((p, h)) = hash_current_binary() {
        provenance.binary_path = Some(p);
        provenance.binary_sha256 = Some(h);
    }

    let report = build_campaign_report(sessions, provenance.clone(), warnings);
    provenance.coverage = ProvenanceCoverage {
        population_sessions: report.population.population_sessions,
        usable_sessions: report.population.usable_sessions,
        excluded_sessions: report.population.excluded_sessions,
        primary_exclusion_counts: report.population.primary_exclusion_counts.clone(),
        desk_1615_exclusion_counts: report.population.desk_1615_exclusion_counts.clone(),
        full_session_exclusion_counts: report.population.full_session_exclusion_counts.clone(),
        green_ny_n: report.green_ny_primary.n,
        red_ny_n: report.red_ny_primary.n,
        full_session_denominator: report.population.full_session_denominator,
    };
    let report = CampaignReport {
        provenance: provenance.clone(),
        ..report
    };

    let recon = CountReconciliation {
        study_id: STUDY_ID.into(),
        study_version: STUDY_VERSION,
        population_sessions: report.population.population_sessions,
        usable_sessions: report.population.usable_sessions,
        excluded_sessions: report.population.excluded_sessions,
        primary_exclusion_counts: report.population.primary_exclusion_counts.clone(),
        desk_1615_exclusion_counts: report.population.desk_1615_exclusion_counts.clone(),
        full_session_exclusion_counts: report.population.full_session_exclusion_counts.clone(),
        green_ny_n: report.green_ny_primary.n,
        red_ny_n: report.red_ny_primary.n,
        full_session_n: report.population.full_session_denominator,
        excursion_diagnostic_n: report.excursion_summary.n,
        notes: vec![
            format!(
                "primaryExcluded={} = population({}) - primaryUsable({}); primaryExclusionCounts are ONLY fatal/signal/NY-primary reasons on primary-excluded rows — not a mix with secondary metrics.",
                report.population.excluded_sessions,
                report.population.population_sessions,
                report.population.usable_sessions
            ),
            format!(
                "Secondary maps are independent: desk1615ExclusionCounts / fullSessionExclusionCounts; full-session denominator={} (metric-specific).",
                report.population.full_session_denominator
            ),
            format!(
                "Excursion diagnostic n={} eligibility={}",
                report.excursion_summary.n, report.excursion_summary.eligibility_label
            ),
            format!(
                "Reliability: green_ny={:?} red_ny={:?} pooled={:?} (AGENT.md N<20 insufficient, 20–29 directional, N>=30 reportable).",
                report.green_ny_primary.reliability_tier,
                report.red_ny_primary.reliability_tier,
                report.pooled_ny_primary.reliability_tier
            ),
        ],
    };
    write_json(&run_dir.join("sessions.json"), &sessions)?;
    write_json(&run_dir.join("report.json"), &report)?;
    write_json(&run_dir.join("provenance.json"), &provenance)?;
    write_json(&run_dir.join("rollover_evidence.json"), rollover)?;
    write_json(&run_dir.join("count-reconciliation.json"), &recon)?;
    let md = render_report_md(&report);
    std::fs::write(run_dir.join("REPORT.md"), md).map_err(|e| e.to_string())?;
    Ok(report)
}

/// Fixture/integration path: write artifacts from supplied sessions (no SCID I/O).
pub fn write_stage1_artifacts(
    run_dir: &Path,
    sessions: &[SessionContinuationRow],
    provenance: &CampaignProvenance,
) -> Result<CampaignReport, String> {
    let roll = if run_dir.join("rollover_evidence.json").exists() {
        let text = std::fs::read_to_string(run_dir.join("rollover_evidence.json"))
            .map_err(|e| e.to_string())?;
        serde_json::from_str(&text).map_err(|e| e.to_string())?
    } else {
        ib_campaign::build_rollover_evidence(&BTreeMap::new(), "fixture")
    };
    write_campaign_artifacts(run_dir, sessions, provenance, &roll, vec![])
}

pub fn render_report_md(report: &CampaignReport) -> String {
    let mut out = String::new();
    out.push_str("# IDEA-025D 9AM Continuation Report\n\n");
    out.push_str(&format!(
        "- study: {} v{}\n- verdict: {} / {}\n- stage: {}\n- developmentEnd: {}\n",
        report.study_contract.study_id,
        report.study_contract.study_version,
        report.verdict.result,
        report.verdict.disposition,
        report.provenance.stage,
        report.study_contract.development_end_date
    ));
    out.push_str(&format!(
        "- population={} primary_usable={} primary_excluded={} (primaryExcluded=population-primaryUsable)\n",
        report.population.population_sessions,
        report.population.usable_sessions,
        report.population.excluded_sessions
    ));
    out.push_str(&format!(
        "- primaryExclusionCounts={:?}\n",
        report.population.primary_exclusion_counts
    ));
    out.push_str(&format!(
        "- desk1615ExclusionCounts={:?} (secondary; not a partition of primary_excluded)\n",
        report.population.desk_1615_exclusion_counts
    ));
    out.push_str(&format!(
        "- fullSessionExclusionCounts={:?} (secondary; not a partition of primary_excluded)\n",
        report.population.full_session_exclusion_counts
    ));
    out.push_str(&format!(
        "- signal doji_n={} green_signal_n={} red_signal_n={}\n",
        report.population.doji_signal_n,
        report.population.green_signal_n,
        report.population.red_signal_n
    ));
    for (label, b) in [
        ("green NY primary", &report.green_ny_primary),
        ("red NY primary", &report.red_ny_primary),
        ("green full-session", &report.green_full_session),
        ("red full-session", &report.red_full_session),
        ("green desk-1615", &report.green_ny_desk_1615_sensitivity),
        ("red desk-1615", &report.red_ny_desk_1615_sensitivity),
    ] {
        out.push_str(&format!(
            "- {label}: n={} wins={} losses={} doji={} rate={:.4} wilson=[{:.4},{:.4}] base={:.4} lift={:.4} tier={:?}\n",
            b.n,
            b.wins,
            b.losses,
            b.doji_outcomes,
            b.continuation_rate,
            b.wilson_ci95[0],
            b.wilson_ci95[1],
            b.unconditional_base_rate,
            b.lift_vs_base,
            b.reliability_tier
        ));
    }
    out.push_str(&format!(
        "- pooled NY secondary: n={} rate={:.4} tier={:?}\n",
        report.pooled_ny_primary.n,
        report.pooled_ny_primary.continuation_rate,
        report.pooled_ny_primary.reliability_tier
    ));
    out.push_str(&format!(
        "- full-session denominator={}\n",
        report.population.full_session_denominator
    ));
    let e = &report.excursion_summary;
    out.push_str(&format!(
        "- excursion diagnostic: n={} avg_mfe={:.4} avg_mae={:.4} eligibility={} ({})\n",
        e.n, e.avg_mfe_points, e.avg_mae_points, e.eligibility_label, e.note
    ));
    out.push_str("\n## Windows\n\n");
    for w in &report.provenance.windows {
        out.push_str(&format!(
            "- {} {}..{}\n",
            w.contract, w.start_date, w.end_date
        ));
    }
    out.push_str(&format!("\n{}\n", report.verdict.rationale));
    out.push_str(
        "\nSource benchmarks are metadata only and are not blended into local estimates.\n",
    );
    if !report.warnings.is_empty() {
        out.push_str("\n## Warnings\n\n");
        for w in &report.warnings {
            out.push_str(&format!("- {w}\n"));
        }
    }
    out
}

/// Full Stage-2 execute using prepared crossover-derived windows (no live DB).
pub fn execute_v1_campaign(req: ExecuteV1Request<'_>) -> Result<CampaignReport, String> {
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
    let _ = resolve_path_strictly_beneath(req.run_dir, req.campaign_root)?;

    let mut all_sessions = Vec::new();
    let mut warnings = Vec::new();
    for w in &req.windows {
        eprintln!(
            "Executing IDEA-025D replay {} {}..{}...",
            w.contract, w.start_date, w.end_date
        );
        let (sessions, wwarn) = replay_nine_am_contract_window(w).map_err(|e| {
            format!(
                "BLOCKED: SCID replay failed for {} {}..{}: {e}",
                w.contract, w.start_date, w.end_date
            )
        })?;
        warnings.extend(wwarn);
        eprintln!("  sessions={}", sessions.len());
        all_sessions.extend(sessions);
    }

    let transition_exclusions = collect_transition_exclusions(&req.rollover);
    let prep = PrepareV1Request {
        run_dir: req.run_dir,
        campaign_root: req.campaign_root,
        overwrite: false,
        db_filename: "unused",
        git_commit: req.git_commit,
        git_dirty: req.git_dirty,
        commands: req.commands.clone(),
        volume_source: RolloverVolumeSource::ScanLocalScid, // unused by build_provenance
    };
    let mut provenance = build_provenance(
        &prep,
        req.run_dir,
        Some(req.isolated_db_path),
        &req.windows,
        &transition_exclusions,
        &req.rollover,
        "stage2_execute",
        true,
    )?;
    if let Ok((p, h)) = hash_current_binary() {
        provenance.binary_path = Some(p);
        provenance.binary_sha256 = Some(h);
    }
    provenance.source_file_hashes = hash_campaign_sources()?;

    write_campaign_artifacts(
        req.run_dir,
        &all_sessions,
        &provenance,
        &req.rollover,
        warnings,
    )
}

/// Backward-compatible name: execute requires an `ExecuteV1Request` via the binary.
pub fn execute_v1_campaign_scid_replay(_run_dir: &Path) -> Result<(), String> {
    Err(
        "BLOCKED: call execute_v1_campaign(ExecuteV1Request{...}) with prepared windows/rollover"
            .into(),
    )
}

/// Resolve rollover volumes → evidence → windows (production: ScanLocalScid).
pub fn stage2_resolve_rollover_hook(
    source: RolloverVolumeSource<'_>,
) -> Result<(RolloverEvidence, Vec<ContractWindow>), String> {
    resolve_rollover_and_windows(source)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research::ib_campaign::DailyOverlapVolume;
    use serde_json;

    fn ft(date: &str, h: u32, m: u32, s: u32, price: f64) -> FixtureTick {
        FixtureTick {
            timestamp_ms: et_wall_to_utc_ms(date, h, m, s).expect("et"),
            price,
        }
    }

    /// Dense ET-minute prints with step < material gap; prices linear.
    fn dense_et(
        date: &str,
        start_et: i32,
        end_et_exclusive: i32,
        step_min: i32,
        start_px: f64,
        dp: f64,
    ) -> Vec<FixtureTick> {
        let mut out = Vec::new();
        let mut et = start_et;
        let mut px = start_px;
        while et < end_et_exclusive {
            let h = (et / 60) as u32;
            let m = (et % 60) as u32;
            out.push(ft(date, h, m, 0, px));
            px += dp;
            et += step_min;
        }
        out
    }

    fn triples(ticks: &[FixtureTick]) -> Vec<(f64, f64, i32)> {
        ticks
            .iter()
            .filter_map(|t| {
                let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms)?;
                Some((t.timestamp_ms, t.price, ctx.et_minutes))
            })
            .collect()
    }

    /// Usable green signal + green NY + desk + full-session-ready NY side.
    fn usable_green_ny_ticks(date: &str) -> Vec<FixtureTick> {
        let mut by_et: BTreeMap<i32, FixtureTick> = BTreeMap::new();
        for t in dense_et(
            date,
            SIGNAL_HOUR_START_ET,
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
            4,
            100.0,
            0.05,
        ) {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms).unwrap();
            by_et.insert(ctx.et_minutes, t);
        }
        by_et.insert(SIGNAL_HOUR_START_ET, ft(date, 9, 0, 0, 100.0));
        by_et.insert(NY_OPEN_ET, ft(date, 9, 30, 0, 101.0));
        by_et.insert(SIGNAL_HOUR_END_ET - 1, ft(date, 9, 59, 0, 103.0));
        by_et.insert(NY_CLOSE_EXCLUSIVE_ET - 1, ft(date, 15, 59, 0, 120.0));
        by_et.insert(DESK_RTH_CLOSE_EXCLUSIVE_ET - 1, ft(date, 16, 14, 0, 201.0));
        by_et.insert(
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET - 1,
            ft(date, 16, 59, 0, 202.0),
        );
        by_et.into_values().collect()
    }

    fn fresh_prior_open(prior: &str, px: f64) -> Vec<FixtureTick> {
        dense_et(
            prior,
            FUTURES_SESSION_OPEN_ET,
            FUTURES_SESSION_OPEN_ET + 20,
            4,
            px,
            0.1,
        )
    }

    fn empty_prov() -> CampaignProvenance {
        CampaignProvenance {
            git_commit: "t".into(),
            git_dirty: false,
            study_id: STUDY_ID.into(),
            study_version: STUDY_VERSION,
            development_end_date: DEVELOPMENT_END_DATE.into(),
            study_contract: study_contract(),
            campaign_root: V1_CAMPAIGN_ROOT.into(),
            artifact_dir: "x".into(),
            canonical_artifact_dir: "x".into(),
            isolated_db_path: None,
            canonical_isolated_db_path: None,
            windows: vec![],
            crossovers: vec![],
            excluded_session_dates: vec![],
            transition_exclusions: vec![],
            coverage: empty_coverage(),
            commands: vec![],
            source_file_hashes: vec![],
            binary_path: None,
            binary_sha256: None,
            stage: "test".into(),
            execute_implemented: false,
        }
    }

    fn fixture_rollover_volumes() -> BTreeMap<(String, String), Vec<DailyOverlapVolume>> {
        let mut map = BTreeMap::new();
        map.insert(
            ("NQH6.CME".into(), "NQM6.CME".into()),
            vec![
                DailyOverlapVolume {
                    session_date: "2026-03-01".into(),
                    front_contract: "NQH6.CME".into(),
                    front_rth_volume: 100.0,
                    back_contract: "NQM6.CME".into(),
                    back_rth_volume: 10.0,
                },
                DailyOverlapVolume {
                    session_date: "2026-03-02".into(),
                    front_contract: "NQH6.CME".into(),
                    front_rth_volume: 20.0,
                    back_contract: "NQM6.CME".into(),
                    back_rth_volume: 80.0,
                },
            ],
        );
        map.insert(
            ("NQM6.CME".into(), "NQU6.CME".into()),
            vec![
                DailyOverlapVolume {
                    session_date: "2026-05-28".into(),
                    front_contract: "NQM6.CME".into(),
                    front_rth_volume: 90.0,
                    back_contract: "NQU6.CME".into(),
                    back_rth_volume: 40.0,
                },
                DailyOverlapVolume {
                    session_date: "2026-05-29".into(),
                    front_contract: "NQM6.CME".into(),
                    front_rth_volume: 30.0,
                    back_contract: "NQU6.CME".into(),
                    back_rth_volume: 100.0,
                },
            ],
        );
        map
    }

    fn prep_req<'a>(
        run: &'a Path,
        root: &'a Path,
        volumes: &'a BTreeMap<(String, String), Vec<DailyOverlapVolume>>,
    ) -> PrepareV1Request<'a> {
        PrepareV1Request {
            run_dir: run,
            campaign_root: root,
            overwrite: false,
            db_filename: V1_ISOLATED_DB_FILENAME,
            git_commit: "test",
            git_dirty: false,
            commands: vec!["test".into()],
            volume_source: RolloverVolumeSource::Fixture(volumes),
        }
    }

    #[test]
    fn signal_hour_half_open_boundaries_no_synthetic_close() {
        let date = "2026-03-16";
        let ticks = usable_green_ny_ticks(date);
        let t = triples(&ticks);
        let sig = build_signal_hour(&t, date).expect("signal");
        assert_eq!(sig.open.et_minutes, SIGNAL_HOUR_START_ET);
        assert!(sig.close.et_minutes < SIGNAL_HOUR_END_ET);
        assert!(open_boundary_is_fresh(
            &sig.open,
            date,
            SIGNAL_HOUR_START_ET
        ));
        assert!(close_boundary_is_fresh(
            &sig.close,
            date,
            SIGNAL_HOUR_END_ET
        ));
        assert_eq!(sig.direction, CandleDirection::Green);
        assert!(build_signal_hour(&[], date).is_err());
        let doji_ticks: Vec<(f64, f64, i32)> = (0..15)
            .map(|i| {
                let minute = i * 4;
                (
                    et_wall_to_utc_ms(date, 9, minute as u32, 0).unwrap(),
                    50.0,
                    SIGNAL_HOUR_START_ET + minute,
                )
            })
            .chain(std::iter::once((
                et_wall_to_utc_ms(date, 9, 59, 0).unwrap(),
                50.0,
                SIGNAL_HOUR_END_ET - 1,
            )))
            .collect();
        let bar = build_signal_hour(&doji_ticks, date).unwrap();
        assert_eq!(bar.direction, CandleDirection::Doji);
    }

    #[test]
    fn ny_primary_uses_strict_pre_1600_not_1615() {
        let date = "2026-03-16";
        let t = triples(&usable_green_ny_ticks(date));
        let primary = build_ny_primary(&t, date).expect("primary");
        assert!(primary.is_primary);
        assert!(primary.close.et_minutes < NY_CLOSE_EXCLUSIVE_ET);
        assert!((primary.close.price - 120.0).abs() < 1e-9);
        let desk = build_ny_desk_1615_sensitivity(&t, date).expect("desk");
        assert!(!desk.is_primary);
        assert!((desk.close.price - 201.0).abs() < 1e-9);
        assert_ne!(desk.label, primary.label);
    }

    #[test]
    fn green_red_doji_mirror() {
        assert_eq!(
            CandleDirection::from_open_close(1.0, 2.0),
            CandleDirection::Green
        );
        assert_eq!(
            CandleDirection::from_open_close(2.0, 1.0),
            CandleDirection::Red
        );
        assert_eq!(
            CandleDirection::from_open_close(1.0, 1.0),
            CandleDirection::Doji
        );
        assert_eq!(
            CandleDirection::Green.continues(CandleDirection::Green),
            Some(true)
        );
        assert_eq!(
            CandleDirection::Red.continues(CandleDirection::Red),
            Some(true)
        );
        assert_eq!(
            CandleDirection::Green.continues(CandleDirection::Red),
            Some(false)
        );
        assert_eq!(
            CandleDirection::Green.continues(CandleDirection::Doji),
            None
        );
    }

    #[test]
    fn missing_ny_primary_does_not_discard_full_session_metric() {
        let date = "2026-03-16";
        let prior = futures_open_calendar_date_for_ny_date(date).unwrap();
        let mut by_et: BTreeMap<i32, FixtureTick> = BTreeMap::new();
        for t in dense_et(
            date,
            SIGNAL_HOUR_START_ET,
            SIGNAL_HOUR_END_ET,
            4,
            100.0,
            0.2,
        ) {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms).unwrap();
            by_et.insert(ctx.et_minutes, t);
        }
        by_et.insert(SIGNAL_HOUR_START_ET, ft(date, 9, 0, 0, 100.0));
        by_et.insert(SIGNAL_HOUR_END_ET - 1, ft(date, 9, 59, 0, 103.0));
        for t in dense_et(
            date,
            10 * 60 + 30,
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
            4,
            110.0,
            0.05,
        ) {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms).unwrap();
            by_et.insert(ctx.et_minutes, t);
        }
        by_et.insert(
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET - 1,
            ft(date, 16, 59, 0, 202.0),
        );
        let ticks: Vec<_> = by_et.into_values().collect();
        let row =
            evaluate_fixture_session(date, "NQM6.CME", &ticks, &fresh_prior_open(&prior, 90.0));
        assert!(row.quality.fatal_quality_ok, "{:?}", row.quality);
        assert!(row.quality.signal_eligible, "{:?}", row.quality);
        assert!(!row.quality.ny_primary_eligible);
        assert!(row.quality.full_session_eligible, "{:?}", row.quality);
        let report = build_campaign_report(std::slice::from_ref(&row), empty_prov(), vec![]);
        assert_eq!(report.population.full_session_denominator, 1);
        assert_eq!(report.population.ny_primary_denominator, 0);
    }

    #[test]
    fn missing_boundaries_exclude_metric_specific() {
        let date = "2026-03-16";
        let ticks = vec![ft(date, 8, 0, 0, 100.0), ft(date, 17, 30, 0, 101.0)];
        let row = evaluate_fixture_session(date, "NQM6.CME", &ticks, &[]);
        assert!(!row.quality.usable);
        assert!(!row.quality.signal_eligible);
        assert!(!row.quality.ny_primary_eligible);
        assert!(row.signal.is_none());
        assert!(row.ny_primary.is_none());
        assert!(row.full_session.is_none());
    }

    #[test]
    fn timestamp_reversal_and_material_gap_reject() {
        let date = "2026-03-16";
        let mut ticks = usable_green_ny_ticks(date);
        ticks.push(FixtureTick {
            timestamp_ms: ticks[0].timestamp_ms - 1.0,
            price: 99.0,
        });
        let row = evaluate_fixture_session(date, "NQM6.CME", &ticks, &[]);
        assert!(!row.quality.fatal_quality_ok);
        assert!(row.quality.monotonic_rejections > 0);

        let gappy = vec![
            ft(date, 9, 0, 0, 100.0),
            FixtureTick {
                timestamp_ms: et_wall_to_utc_ms(date, 9, 0, 0).unwrap()
                    + METRIC_MATERIAL_GAP_MS
                    + 1.0,
                price: 101.0,
            },
            ft(date, 9, 59, 0, 103.0),
        ];
        let row2 = evaluate_fixture_session(date, "NQM6.CME", &gappy, &[]);
        assert!(row2.quality.material_gap);
        assert!(!row2.quality.signal_eligible);
        assert!(row2
            .quality
            .material_gap_windows
            .iter()
            .any(|w| w.contains("signal")));
    }

    #[test]
    fn near_boundary_pass_and_stale_boundary_fail() {
        let date = "2026-03-16";
        let fresh = usable_green_ny_ticks(date);
        let row = evaluate_fixture_session(
            date,
            "NQM6.CME",
            &fresh,
            &fresh_prior_open("2026-03-15", 90.0),
        );
        assert!(row.quality.signal_eligible);
        assert!(row.quality.ny_primary_eligible);
        assert!(row.quality.ny_desk_1615_eligible);
        assert!(row.quality.full_session_eligible);

        let mut stale = dense_et(date, 9 * 60 + 5, SIGNAL_HOUR_END_ET, 4, 100.0, 0.1);
        stale.extend(dense_et(
            date,
            SIGNAL_HOUR_END_ET,
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
            4,
            110.0,
            0.05,
        ));
        stale.push(ft(date, 15, 59, 0, 120.0));
        stale.push(ft(date, 16, 14, 0, 121.0));
        stale.push(ft(date, 16, 59, 0, 122.0));
        let row2 = evaluate_fixture_session(date, "NQM6.CME", &stale, &[]);
        assert!(!row2.quality.signal_eligible);
        assert!(row2
            .quality
            .stale_or_missing_reasons
            .iter()
            .any(|r| r.contains("stale_signal_open")));
    }

    #[test]
    fn desk_1615_base_rates_independent_of_pre_1600() {
        let date = "2026-03-16";
        let prior = "2026-03-15";
        let mut by_et: BTreeMap<i32, FixtureTick> = BTreeMap::new();
        for t in dense_et(
            date,
            SIGNAL_HOUR_START_ET,
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
            4,
            100.0,
            0.0,
        ) {
            let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms).unwrap();
            by_et.insert(ctx.et_minutes, t);
        }
        by_et.insert(SIGNAL_HOUR_START_ET, ft(date, 9, 0, 0, 100.0));
        by_et.insert(NY_OPEN_ET, ft(date, 9, 30, 0, 100.0));
        by_et.insert(SIGNAL_HOUR_END_ET - 1, ft(date, 9, 59, 0, 103.0));
        by_et.insert(NY_CLOSE_EXCLUSIVE_ET - 1, ft(date, 15, 59, 0, 130.0));
        by_et.insert(DESK_RTH_CLOSE_EXCLUSIVE_ET - 1, ft(date, 16, 14, 0, 90.0));
        by_et.insert(
            FUTURES_SESSION_CLOSE_EXCLUSIVE_ET - 1,
            ft(date, 16, 59, 0, 95.0),
        );
        let a: Vec<_> = by_et.into_values().collect();
        let row_a = evaluate_fixture_session(date, "NQM6.CME", &a, &fresh_prior_open(prior, 80.0));
        assert!(row_a.quality.ny_primary_eligible, "{:?}", row_a.quality);
        assert!(row_a.quality.ny_desk_1615_eligible, "{:?}", row_a.quality);
        assert_ne!(
            row_a.ny_primary.as_ref().unwrap().direction,
            row_a.ny_desk_1615_sensitivity.as_ref().unwrap().direction
        );
        let report = build_campaign_report(std::slice::from_ref(&row_a), empty_prov(), vec![]);
        assert!((report.green_ny_primary.unconditional_base_rate - 1.0).abs() < 1e-12);
        assert!(
            (report
                .green_ny_desk_1615_sensitivity
                .unconditional_base_rate
                - 0.0)
                .abs()
                < 1e-12
        );
    }

    #[test]
    fn signal_adjusted_excursion_mirrors_green_and_red() {
        let date = "2026-03-16";
        let entry = ObservedPrint::new(
            et_wall_to_utc_ms(date, 9, 59, 0).unwrap(),
            100.0,
            9 * 60 + 59,
        );
        let ticks = vec![
            (
                et_wall_to_utc_ms(date, 10, 30, 0).unwrap(),
                110.0,
                10 * 60 + 30,
            ),
            (et_wall_to_utc_ms(date, 11, 0, 0).unwrap(), 90.0, 11 * 60),
            (et_wall_to_utc_ms(date, 15, 0, 0).unwrap(), 105.0, 15 * 60),
        ];
        let green = post_signal_excursion_to_1600(&entry, CandleDirection::Green, &ticks);
        let red = post_signal_excursion_to_1600(&entry, CandleDirection::Red, &ticks);
        assert!((green.raw_max_up_points - 10.0).abs() < 1e-9);
        assert!((green.raw_max_down_points - (-10.0)).abs() < 1e-9);
        assert!((green.mfe_points - 10.0).abs() < 1e-9);
        assert!((green.mae_points - (-10.0)).abs() < 1e-9);
        assert!((red.mfe_points - 10.0).abs() < 1e-9);
        assert!((red.mae_points - (-10.0)).abs() < 1e-9);
        assert_eq!(green.raw_max_up_points, red.raw_max_up_points);
        assert!(green.label.contains("signal_adjusted"));
    }

    #[test]
    fn study_contract_primary_prose_order() {
        let c = study_contract();
        assert!(c
            .primary_ny_outcome
            .contains("last observed strictly before 16:00 MINUS first observed"));
    }

    #[test]
    fn dst_aware_et_conversion() {
        let ms = et_wall_to_utc_ms("2026-03-16", 9, 30, 0).unwrap();
        let ctx = tick_time_context_from_timestamp_ms(ms).unwrap();
        assert_eq!(ctx.et_minutes, NY_OPEN_ET);
        let ms2 = et_wall_to_utc_ms("2026-01-15", 9, 0, 0).unwrap();
        let ctx2 = tick_time_context_from_timestamp_ms(ms2).unwrap();
        assert_eq!(ctx2.et_minutes, SIGNAL_HOUR_START_ET);
    }

    #[test]
    fn sunday_monday_1800_to_1700_mapping() {
        let ny = "2026-03-16";
        let prior = futures_open_calendar_date_for_ny_date(ny).unwrap();
        assert_eq!(prior, "2026-03-15");
        let row = evaluate_fixture_session(
            ny,
            "NQM6.CME",
            &usable_green_ny_ticks(ny),
            &fresh_prior_open(&prior, 90.0),
        );
        let full = row.full_session.expect("full session");
        assert!(full.open.et_minutes >= FUTURES_SESSION_OPEN_ET);
        assert!(full.close.et_minutes < FUTURES_SESSION_CLOSE_EXCLUSIVE_ET);
        let ctx = tick_time_context_from_timestamp_ms(full.open.timestamp_ms).unwrap();
        assert_eq!(ctx.trading_day, ny);
        assert_eq!(ctx.session_date, prior);
    }

    #[test]
    fn wilson_math_known_values() {
        assert_eq!(wilson_ci95(0, 0), [0.0, 0.0]);
        let ci2 = wilson_ci95(5, 10);
        assert!(ci2[0] < 0.5 && ci2[1] > 0.5);
    }

    #[test]
    fn path_safety_and_live_db_refusal() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("v1");
        std::fs::create_dir_all(&root).unwrap();
        let run = root.join("run-a");
        let _ = prepare_run_directory(&run, &root, false).expect("prepare");
        assert!(resolve_path_strictly_beneath(&root, &root).is_err());
        for live in FORBIDDEN_LIVE_DB_PATHS {
            assert!(is_forbidden_live_db_path(Path::new(live)));
        }
        let vols = fixture_rollover_volumes();
        let err = prepare_v1_campaign_run(PrepareV1Request {
            run_dir: Path::new(FORBIDDEN_LIVE_DB_PATHS[0]),
            campaign_root: &root,
            overwrite: false,
            db_filename: V1_ISOLATED_DB_FILENAME,
            git_commit: "test",
            git_dirty: false,
            commands: vec![],
            volume_source: RolloverVolumeSource::Fixture(&vols),
        })
        .expect_err("live db");
        assert!(err.contains("refusing") || err.contains("forbidden") || err.contains("escape"));
    }

    #[test]
    fn fixed_verdict_never_accept() {
        let pop = PopulationSummary {
            population_sessions: 100,
            usable_sessions: 80,
            excluded_sessions: 20,
            primary_exclusion_counts: BTreeMap::new(),
            desk_1615_exclusion_counts: BTreeMap::new(),
            full_session_exclusion_counts: BTreeMap::new(),
            green_signal_n: 40,
            red_signal_n: 40,
            doji_signal_n: 0,
            ny_primary_denominator: 80,
            ny_desk_1615_denominator: 80,
            full_session_denominator: 70,
            reliability_tier: ReliabilityTier::Reportable,
        };
        let v = decide_verdict(&pop);
        assert_eq!(v.result, VERDICT_DEVELOPMENT_RESULT);
        assert_eq!(v.disposition, VERDICT_PARK_FORWARD_VALIDATION);
        let err = execute_v1_campaign_scid_replay(Path::new(".")).unwrap_err();
        assert!(err.contains("BLOCKED") || err.contains("ExecuteV1Request"));
    }

    #[test]
    fn artifact_json_round_trip_skeleton() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("v1");
        std::fs::create_dir_all(&root).unwrap();
        let run = root.join("run-fixture");
        let vols = fixture_rollover_volumes();
        let prepared = prepare_v1_campaign_run(prep_req(&run, &root, &vols)).expect("prepare");
        assert!(!prepared.windows.is_empty());
        assert_eq!(prepared.rollover.transitions.len(), 2);
        let date = "2026-03-16";
        let sessions = vec![evaluate_fixture_session(
            date,
            "NQM6.CME",
            &usable_green_ny_ticks(date),
            &fresh_prior_open("2026-03-15", 90.0),
        )];
        assert!(sessions[0].quality.usable, "{:?}", sessions[0].quality);
        let report = write_stage1_artifacts(
            Path::new(&prepared.canonical_run_dir),
            &sessions,
            &prepared.provenance,
        )
        .expect("write");
        assert_eq!(report.verdict.result, VERDICT_DEVELOPMENT_RESULT);
        for name in [
            "sessions.json",
            "report.json",
            "provenance.json",
            "rollover_evidence.json",
            "count-reconciliation.json",
            "REPORT.md",
        ] {
            let p = Path::new(&prepared.canonical_run_dir).join(name);
            assert!(p.exists(), "missing {name}");
            if name.ends_with(".json") {
                let _: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).expect(name);
            }
        }
        let report2: CampaignReport = serde_json::from_str(
            &std::fs::read_to_string(Path::new(&prepared.canonical_run_dir).join("report.json"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(report2.green_ny_primary.n, 1);
        assert_eq!(
            report2.provenance.development_end_date,
            DEVELOPMENT_END_DATE
        );
        assert!(report2
            .warnings
            .iter()
            .all(|w| !w.to_lowercase().contains("skeleton")));
    }

    #[test]
    fn red_signal_continuation_and_lift_vs_base() {
        let date = "2026-04-06";
        let prior = futures_open_calendar_date_for_ny_date(date).unwrap();
        let mk = |d: &str, open_px: f64, sig_close: f64, ny_close: f64| {
            let mut by_et: BTreeMap<i32, FixtureTick> = BTreeMap::new();
            for t in dense_et(
                d,
                SIGNAL_HOUR_START_ET,
                FUTURES_SESSION_CLOSE_EXCLUSIVE_ET,
                4,
                open_px,
                0.0,
            ) {
                let ctx = tick_time_context_from_timestamp_ms(t.timestamp_ms).unwrap();
                by_et.insert(ctx.et_minutes, t);
            }
            by_et.insert(SIGNAL_HOUR_START_ET, ft(d, 9, 0, 0, open_px));
            by_et.insert(NY_OPEN_ET, ft(d, 9, 30, 0, open_px));
            by_et.insert(SIGNAL_HOUR_END_ET - 1, ft(d, 9, 59, 0, sig_close));
            by_et.insert(NY_CLOSE_EXCLUSIVE_ET - 1, ft(d, 15, 59, 0, ny_close));
            by_et.insert(DESK_RTH_CLOSE_EXCLUSIVE_ET - 1, ft(d, 16, 14, 0, ny_close));
            by_et.insert(
                FUTURES_SESSION_CLOSE_EXCLUSIVE_ET - 1,
                ft(d, 16, 59, 0, ny_close),
            );
            by_et.into_values().collect::<Vec<_>>()
        };
        let red = mk(date, 200.0, 190.0, 150.0);
        let green_date = "2026-04-07";
        let green = mk(green_date, 100.0, 110.0, 50.0);
        let sessions = vec![
            evaluate_fixture_session(date, "NQM6.CME", &red, &fresh_prior_open(&prior, 195.0)),
            evaluate_fixture_session(
                green_date,
                "NQM6.CME",
                &green,
                &fresh_prior_open(date, 100.0),
            ),
        ];
        assert!(sessions[0].quality.usable, "{:?}", sessions[0].quality);
        assert!(sessions[1].quality.usable, "{:?}", sessions[1].quality);
        let report = build_campaign_report(&sessions, empty_prov(), vec![]);
        assert!(report.red_ny_primary.n >= 1);
        assert!(report.green_ny_primary.n >= 1);
    }

    #[test]
    fn rollover_fixture_derives_h6_m6_u6_windows_through_dev_end() {
        let (ev, windows) = resolve_rollover_and_windows(RolloverVolumeSource::Fixture(
            &fixture_rollover_volumes(),
        ))
        .expect("resolve");
        require_resolved_rollover_crossovers(&ev).expect("both crossovers");
        assert_eq!(windows.len(), 3);
        assert_eq!(windows[0].contract, "NQH6.CME");
        assert_eq!(windows[1].contract, "NQM6.CME");
        assert_eq!(windows[2].contract, "NQU6.CME");
        assert_eq!(windows[2].end_date, DEVELOPMENT_END_DATE);
        assert_eq!(
            ev.transitions[0].crossover_date.as_deref(),
            Some("2026-03-02")
        );
        assert_eq!(
            ev.transitions[1].crossover_date.as_deref(),
            Some("2026-05-29")
        );
        // No invented weekend exclusions — only observed mutual-overlap gaps.
        for t in &ev.transitions {
            for d in &t.transition_exclusions {
                assert!(
                    t.raw_daily_volumes.iter().any(|r| &r.session_date == d),
                    "exclusion {d} must be an observed overlap date"
                );
            }
        }
    }

    #[test]
    fn replay_window_scan_starts_calendar_day_before() {
        assert_eq!(calendar_day_before("2026-03-16").unwrap(), "2026-03-15");
        assert_eq!(calendar_day_before("2026-01-01").unwrap(), "2025-12-31");
        let ny = "2026-03-16";
        let prior = futures_open_calendar_date_for_ny_date(ny).unwrap();
        assert_eq!(prior, "2026-03-15");
        // Attribution: 18:00+ belongs to trading_day prior buf; daytime to session_date.
        let open_ms = et_wall_to_utc_ms(&prior, 18, 0, 0).unwrap();
        let ctx = tick_time_context_from_timestamp_ms(open_ms).unwrap();
        assert_eq!(ctx.trading_day, ny);
        assert!(ctx.et_minutes >= FUTURES_SESSION_OPEN_ET);
        let day_ms = et_wall_to_utc_ms(ny, 10, 0, 0).unwrap();
        let ctx2 = tick_time_context_from_timestamp_ms(day_ms).unwrap();
        assert_eq!(ctx2.session_date, ny);
        assert!(ctx2.et_minutes < FUTURES_SESSION_OPEN_ET);
    }

    #[test]
    fn execute_requires_supplied_windows_and_refuses_live_db() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("v1");
        std::fs::create_dir_all(&root).unwrap();
        let run = root.join("exec-safety");
        let vols = fixture_rollover_volumes();
        let prepared = prepare_v1_campaign_run(prep_req(&run, &root, &vols)).expect("prepare");
        let run_dir = PathBuf::from(&prepared.canonical_run_dir);
        let db = PathBuf::from(&prepared.canonical_db_path);
        let err = execute_v1_campaign(ExecuteV1Request {
            run_dir: &run_dir,
            isolated_db_path: &db,
            windows: vec![],
            rollover: prepared.rollover.clone(),
            git_commit: "t",
            git_dirty: false,
            commands: vec![],
            campaign_root: &root,
        })
        .expect_err("empty windows");
        assert!(err.contains("windows"));

        let live = PathBuf::from(FORBIDDEN_LIVE_DB_PATHS[0]);
        let err2 = execute_v1_campaign(ExecuteV1Request {
            run_dir: &run_dir,
            isolated_db_path: &live,
            windows: prepared.windows.clone(),
            rollover: prepared.rollover.clone(),
            git_commit: "t",
            git_dirty: false,
            commands: vec![],
            campaign_root: &root,
        })
        .expect_err("live db");
        assert!(err2.contains("refusing") || err2.contains("forbidden"));

        // Execute must attempt the supplied/derived windows (error names the contract).
        let bogus = vec![ContractWindow {
            contract: "NQ_BOGUS_NO_SCID.CME".into(),
            start_date: "2026-06-01".into(),
            end_date: "2026-06-02".into(),
            role: prepared.windows[0].role,
            rationale: "test".into(),
        }];
        let err3 = execute_v1_campaign(ExecuteV1Request {
            run_dir: &run_dir,
            isolated_db_path: &db,
            windows: bogus,
            rollover: prepared.rollover.clone(),
            git_commit: "t",
            git_dirty: false,
            commands: vec![],
            campaign_root: &root,
        })
        .expect_err("missing scid");
        assert!(
            err3.contains("NQ_BOGUS_NO_SCID.CME") || err3.contains("SCID"),
            "{err3}"
        );
    }

    #[test]
    fn artifact_completeness_has_no_stage1_skeleton_warning() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("v1");
        std::fs::create_dir_all(&root).unwrap();
        let run = root.join("complete-arts");
        let vols = fixture_rollover_volumes();
        let prepared = prepare_v1_campaign_run(prep_req(&run, &root, &vols)).expect("prepare");
        let date = "2026-03-16";
        let sessions = vec![evaluate_fixture_session(
            date,
            "NQM6.CME",
            &usable_green_ny_ticks(date),
            &fresh_prior_open("2026-03-15", 90.0),
        )];
        let mut prov = prepared.provenance.clone();
        prov.stage = "stage2_execute".into();
        prov.execute_implemented = true;
        let report = write_campaign_artifacts(
            Path::new(&prepared.canonical_run_dir),
            &sessions,
            &prov,
            &prepared.rollover,
            vec![],
        )
        .expect("write");
        assert!(report.warnings.is_empty());
        assert_eq!(report.verdict.result, VERDICT_DEVELOPMENT_RESULT);
        assert_eq!(report.verdict.disposition, VERDICT_PARK_FORWARD_VALIDATION);
        assert!(!report.verdict.result.contains("ACCEPT"));
        assert_eq!(report.provenance.coverage.population_sessions, 1);
        assert!(!report.provenance.crossovers.is_empty());
        for name in V1_RUN_ARTIFACT_NAMES {
            if *name == V1_ISOLATED_DB_FILENAME {
                assert!(
                    Path::new(&prepared.canonical_run_dir).join(name).exists(),
                    "isolated db"
                );
                continue;
            }
            assert!(
                Path::new(&prepared.canonical_run_dir).join(name).exists(),
                "missing {name}"
            );
        }
    }

    #[test]
    fn excursion_summary_excludes_non_usable_rows_with_excursion() {
        let date = "2026-03-16";
        let prior = futures_open_calendar_date_for_ny_date(date).unwrap();
        let usable = evaluate_fixture_session(
            date,
            "NQM6.CME",
            &usable_green_ny_ticks(date),
            &fresh_prior_open(&prior, 90.0),
        );
        assert!(usable.quality.usable, "{:?}", usable.quality);
        assert!(usable.post_1000_to_1600_excursion.is_some());

        // Non-monotonic: excluded from primary usable but may still carry an excursion
        // object if signal bar was formed before quality failed — must not enter summary.
        let mut bad_ticks = usable_green_ny_ticks(date);
        bad_ticks.push(FixtureTick {
            timestamp_ms: bad_ticks[0].timestamp_ms - 1.0,
            price: 99.0,
        });
        let mut excluded = evaluate_fixture_session(
            date,
            "NQM6.CME",
            &bad_ticks,
            &fresh_prior_open(&prior, 90.0),
        );
        assert!(!excluded.quality.usable);
        // Force an excursion payload on the excluded row (regression for summary filter).
        excluded.post_1000_to_1600_excursion = Some(ExcursionDiagnostics {
            label: "forced".into(),
            from_price: 100.0,
            signal_direction: "green".into(),
            mfe_points: 999.0,
            mae_points: -999.0,
            raw_max_up_points: 999.0,
            raw_max_down_points: -999.0,
            note: "injected".into(),
        });

        let report = build_campaign_report(&[usable, excluded], empty_prov(), vec![]);
        assert_eq!(report.population.usable_sessions, 1);
        assert_eq!(report.population.excluded_sessions, 1);
        assert_eq!(report.excursion_summary.n, 1);
        assert!(report.excursion_summary.avg_mfe_points < 900.0);
        assert!(report
            .excursion_summary
            .eligibility_label
            .contains("primary_usable_only"));
    }

    #[test]
    fn primary_usable_full_session_ineligible_keeps_exclusion_maps_separate() {
        let date = "2026-04-17";
        // Usable NY path with no prior-evening 18:00 → full-session ineligible.
        let row = evaluate_fixture_session(
            date,
            "NQM6.CME",
            &usable_green_ny_ticks(date),
            &[], // missing futures open
        );
        assert!(row.quality.usable, "{:?}", row.quality);
        assert!(row.quality.signal_eligible);
        assert!(row.quality.ny_primary_eligible);
        assert!(!row.quality.full_session_eligible, "{:?}", row.quality);
        assert!(row
            .quality
            .exclusion_reasons
            .iter()
            .any(|r| r.contains("futures")));

        let report = build_campaign_report(std::slice::from_ref(&row), empty_prov(), vec![]);
        assert_eq!(report.population.population_sessions, 1);
        assert_eq!(report.population.usable_sessions, 1);
        assert_eq!(report.population.excluded_sessions, 0);
        assert!(
            report.population.primary_exclusion_counts.is_empty(),
            "primary usable must not accumulate secondary futures reasons: {:?}",
            report.population.primary_exclusion_counts
        );
        assert!(
            report
                .population
                .full_session_exclusion_counts
                .keys()
                .any(|k| k.contains("futures")),
            "{:?}",
            report.population.full_session_exclusion_counts
        );
        assert_eq!(report.population.full_session_denominator, 0);
        assert_eq!(report.population.ny_primary_denominator, 1);
        assert_eq!(report.excursion_summary.n, 1);
    }

    /// Long machine smoke: real local SCID one-pass over a short derived window.
    #[test]
    #[ignore]
    fn scid_replay_smoke_short_window() {
        let window = ContractWindow {
            contract: "NQU6.CME".into(),
            start_date: "2026-07-20".into(),
            end_date: DEVELOPMENT_END_DATE.into(),
            role: crate::research::ib_campaign::WindowRole::Discovery,
            rationale: "smoke".into(),
        };
        let (sessions, warnings) = replay_nine_am_contract_window(&window).expect("replay");
        assert!(
            !sessions.is_empty() || !warnings.is_empty(),
            "expected sessions or window warning"
        );
    }
}
