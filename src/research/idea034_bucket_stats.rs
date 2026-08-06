//! IDEA-034 Stage 1 — offline 5-minute bucket-stats replay (participation only).
//!
//! Builds per-trading-day / per-ET-clock-bucket volume, tape-speed, and signed-delta
//! stats from local `.scid` files using the ib-campaign / 9AM-continuation isolation
//! pattern (path safety, rollover windows, one-pass scan). Does **not** compute
//! continuation/reversal outcomes, event rates, calendar extraction, or Stage 2.

use crate::feed::load_feed_config;
use crate::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};
use crate::feed::scid_reader::{ScanControl, ScidReader};
use crate::feed::TradeSide;
use crate::research::ib_campaign::{
    create_fresh_isolated_campaign_db, hash_current_binary, is_forbidden_live_db_path,
    require_resolved_rollover_crossovers, resolve_path_strictly_beneath,
    resolve_rollover_and_windows, sha256_file, strip_verbatim_prefix, write_json, ContractWindow,
    FileSha256, RolloverEvidence, RolloverVolumeSource, CAMPAIGN_H6_START_DATE,
    DEVELOPMENT_END_DATE, EXCLUDED_SESSION_DATES, FORBIDDEN_LIVE_DB_PATHS,
};
use crate::research::{reliability_tier, ReliabilityTier, RESEARCH_PERCENTILE_METHOD};
use crate::{
    tick_time_context_from_timestamp_ms, SessionType, GLOBEX_OPEN_ET, LONDON_OPEN_ET, RTH_CLOSE_ET,
    RTH_OPEN_ET,
};
use chrono::{Datelike, NaiveDate, Weekday};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const STUDY_ID: &str = "IDEA-034-bucket-stats";
pub const STUDY_VERSION: i64 = 1;

/// Isolated campaign root. Run dirs must be strictly beneath this path.
pub const CAMPAIGN_ROOT: &str = r"T:\TheDesk\temp\backtests\idea-034-bucket-stats";

pub const ISOLATED_DB_FILENAME: &str = "idea-034-bucket-stats.db";

pub const RUN_ARTIFACT_NAMES: &[&str] = &[
    "bucket_stats.jsonl",
    "summary.json",
    "coverage.json",
    "exclusions.json",
    "rollover_evidence.json",
    "provenance.json",
    "REPORT.md",
    ISOLATED_DB_FILENAME,
];

/// Clean-subset robustness fold start (inclusive). Underpowered; directional-only.
pub const CLEAN_SUBSET_START_DATE: &str = "2026-06-23";

/// US/EU DST-mismatch cohort (inclusive), per Stage 1 coverage gate.
pub const DST_MISMATCH_START: &str = "2026-03-09";
pub const DST_MISMATCH_END: &str = "2026-03-27";

/// Gap thresholds match `src/backfill.rs` same-session-class logic.
pub const GLOBEX_GAP_THRESHOLD_MS: f64 = 30.0 * 60_000.0;
pub const RTH_GAP_THRESHOLD_MS: f64 = 5.0 * 60_000.0;

/// Fixed 5-minute ET clock buckets per calendar day.
pub const BUCKET_MINUTES: i32 = 5;
pub const BUCKETS_PER_DAY: u16 = 288;
pub const BUCKET_DURATION_SECS: f64 = (BUCKET_MINUTES as f64) * 60.0;

/// Trailing baseline windows (like-days). Primary detection uses 20.
pub const BASELINE_WINDOWS: [usize; 3] = [10, 20, 40];
pub const PRIMARY_BASELINE_WINDOW: usize = 20;
pub const VOLUME_P95_THRESHOLD: f64 = 95.0;
pub const VOLUME_K_MULTIPLES: [f64; 2] = [2.0, 3.0];

/// Baseline scope label: prior eligible NQ front-month chain days (contracts not pooled silently —
/// each row keeps its contract; baseline draws from prior front-month eligible days only).
pub const BASELINE_SCOPE: &str = "nq_front_month_chain_prior_eligible";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SessionSegmentLabel {
    Asia,
    London,
    Rth,
    OutOfScope,
}

impl SessionSegmentLabel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Asia => "asia",
            Self::London => "london",
            Self::Rth => "rth",
            Self::OutOfScope => "out_of_scope",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketStatRow {
    pub trading_date: String,
    pub bucket_index: u16,
    pub bucket_start_et: String,
    pub bucket_end_et: String,
    pub session_segment: String,
    pub contract: String,
    pub executed_volume: f64,
    pub trade_count: u64,
    /// Trades per second over the fixed 5-minute bucket (= trade_count / 300).
    pub tape_speed_trades_per_sec: f64,
    /// Contracts per second over the fixed 5-minute bucket (= volume / 300).
    pub tape_speed_contracts_per_sec: f64,
    /// Buy volume − sell volume; Unknown side contributes 0 (volume still counted).
    pub signed_delta: f64,
    pub quality_status: String,
    pub exclusion_reason: Option<String>,
    pub is_dst_mismatch_cohort: bool,
    pub is_clean_subset: bool,
    pub weekday: String,
    pub monotonic_rejections: u64,
    pub gap_count: u64,
    pub baselines: Vec<BaselineWindowStats>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaselineWindowStats {
    pub window: usize,
    pub baseline_scope: String,
    pub sample_count: usize,
    pub volume_median: Option<f64>,
    pub volume_p95: Option<f64>,
    pub tape_speed_median: Option<f64>,
    pub tape_speed_p95: Option<f64>,
    pub abs_delta_median: Option<f64>,
    pub abs_delta_p95: Option<f64>,
    pub anomaly_volume_p95: Option<bool>,
    pub anomaly_volume_k2: Option<bool>,
    pub anomaly_volume_k3: Option<bool>,
    pub anomaly_tape_speed_p95: Option<bool>,
    pub anomaly_abs_delta_p95: Option<bool>,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    pub study_id: String,
    pub study_version: i64,
    pub stage: String,
    pub git_commit: String,
    pub git_dirty: bool,
    pub campaign_root: String,
    pub run_dir: String,
    pub canonical_run_dir: String,
    pub isolated_db_path: String,
    pub commands: Vec<String>,
    pub windows: Vec<ContractWindow>,
    pub excluded_session_dates: Vec<String>,
    pub transition_exclusions: Vec<String>,
    pub dst_mismatch_range: [String; 2],
    pub clean_subset_start: String,
    pub baseline_windows: Vec<usize>,
    pub baseline_scope: String,
    pub percentile_method: String,
    pub signed_delta_rule: String,
    pub tape_speed_rule: String,
    pub source_file_hashes: Vec<FileSha256>,
    pub binary_path: Option<String>,
    pub binary_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoverageBucketRow {
    pub bucket_index: u16,
    pub bucket_start_et: String,
    pub session_segment: String,
    pub eligible_n: usize,
    pub eligible_n_ex_dst: usize,
    pub clean_subset_n: usize,
    pub reliability_tier: ReliabilityTier,
    pub clean_subset_reliability_tier: ReliabilityTier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DayExclusion {
    pub trading_date: String,
    pub contract: String,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StudySummary {
    pub study_id: String,
    pub study_version: i64,
    pub campaign_start: String,
    pub campaign_end: String,
    pub contract_windows: Vec<ContractWindow>,
    pub trading_days_observed: usize,
    pub trading_days_eligible: usize,
    pub bucket_stat_rows: usize,
    pub eligible_bucket_rows: usize,
    pub in_scope_buckets: usize,
    pub out_of_scope_buckets: usize,
    pub coverage: Vec<CoverageBucketRow>,
    pub coverage_summary: CoverageSummary,
    pub exclusions: Vec<DayExclusion>,
    pub exclusion_reason_counts: BTreeMap<String, usize>,
    pub baseline_availability: BaselineAvailability,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoverageSummary {
    pub in_scope_buckets: usize,
    pub min_eligible_n: usize,
    pub median_eligible_n: usize,
    pub buckets_n_ge_30: usize,
    pub buckets_n_20_29: usize,
    pub buckets_n_lt_20: usize,
    pub clean_subset_min_n: usize,
    pub clean_subset_median_n: usize,
    pub clean_subset_buckets_n_ge_30: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaselineAvailability {
    pub windows: Vec<usize>,
    pub eligible_rows_with_full_baseline: BTreeMap<String, usize>,
    pub eligible_rows_with_partial_baseline: BTreeMap<String, usize>,
    pub anomaly_volume_p95_true_primary20: usize,
    pub anomaly_volume_k2_true_primary20: usize,
    pub anomaly_volume_k3_true_primary20: usize,
    pub anomaly_tape_p95_true_primary20: usize,
    pub anomaly_abs_delta_p95_true_primary20: usize,
}

#[derive(Debug, Clone)]
pub struct PrepareRequest<'a> {
    pub run_dir: &'a Path,
    pub campaign_root: &'a Path,
    pub overwrite: bool,
    pub db_filename: &'a str,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PrepareResult {
    pub canonical_run_dir: PathBuf,
    pub canonical_db_path: PathBuf,
    pub provenance: Provenance,
    pub rollover: RolloverEvidence,
    pub windows: Vec<ContractWindow>,
}

#[derive(Debug, Clone)]
pub struct ExecuteRequest<'a> {
    pub run_dir: &'a Path,
    pub campaign_root: &'a Path,
    pub isolated_db_path: &'a Path,
    pub windows: &'a [ContractWindow],
    pub rollover: &'a RolloverEvidence,
    pub git_commit: &'a str,
    pub git_dirty: bool,
    pub commands: Vec<String>,
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested)
// ---------------------------------------------------------------------------

/// Map ET minutes-from-midnight to a 5-minute bucket index `[0, 287]`.
pub fn bucket_index_from_et_minutes(et_minutes: i32) -> u16 {
    let mins = et_minutes.rem_euclid(24 * 60);
    (mins / BUCKET_MINUTES) as u16
}

/// Format `HH:MM` for a bucket boundary.
pub fn format_et_hhmm(et_minutes: i32) -> String {
    let mins = et_minutes.rem_euclid(24 * 60);
    format!("{:02}:{:02}", mins / 60, mins % 60)
}

/// Segment label for a 5-minute bucket start (half-open session bounds).
pub fn segment_for_bucket(bucket_index: u16) -> SessionSegmentLabel {
    let start = bucket_index as i32 * BUCKET_MINUTES;
    if (RTH_CLOSE_ET..GLOBEX_OPEN_ET).contains(&start) {
        SessionSegmentLabel::OutOfScope
    } else if (RTH_OPEN_ET..RTH_CLOSE_ET).contains(&start) {
        SessionSegmentLabel::Rth
    } else if (LONDON_OPEN_ET..RTH_OPEN_ET).contains(&start) {
        SessionSegmentLabel::London
    } else {
        // 18:00–24:00 and 00:00–02:00
        SessionSegmentLabel::Asia
    }
}

pub fn is_in_scope_bucket(bucket_index: u16) -> bool {
    !matches!(
        segment_for_bucket(bucket_index),
        SessionSegmentLabel::OutOfScope
    )
}

pub fn is_dst_mismatch_date(date: &str) -> bool {
    (DST_MISMATCH_START..=DST_MISMATCH_END).contains(&date)
}

pub fn is_holiday_or_early_close(date: &str) -> bool {
    EXCLUDED_SESSION_DATES.contains(&date)
}

pub fn is_clean_subset_date(date: &str) -> bool {
    date >= CLEAN_SUBSET_START_DATE
}

/// Hyndman/Fan type 7 percentile on a pre-sorted ascending slice (matches research module).
pub fn percentile_type7(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    if sorted.len() == 1 {
        return Some(sorted[0]);
    }
    let clamped = p.clamp(0.0, 100.0) / 100.0;
    let h = clamped * (sorted.len() - 1) as f64;
    let lower_idx = h.floor() as usize;
    let upper_idx = h.ceil() as usize;
    if lower_idx == upper_idx {
        return Some(sorted[lower_idx]);
    }
    let weight = h - lower_idx as f64;
    Some(sorted[lower_idx] + (sorted[upper_idx] - sorted[lower_idx]) * weight)
}

pub fn signed_delta_for_side(side: &TradeSide, volume: f64) -> f64 {
    match side {
        TradeSide::Buy => volume,
        TradeSide::Sell => -volume,
        TradeSide::Unknown => 0.0,
    }
}

/// Select at most `window` prior eligible same-bucket observations strictly before `as_of_date`.
/// No same-day or future leakage. DST-mismatch and other non-baseline-eligible days are omitted
/// from the catalog before calling this.
pub fn select_trailing_same_bucket(
    prior_eligible_values: &[(String, f64)],
    as_of_date: &str,
    window: usize,
) -> Vec<f64> {
    let mut eligible: Vec<f64> = prior_eligible_values
        .iter()
        .filter(|(d, _)| d.as_str() < as_of_date)
        .map(|(_, v)| *v)
        .collect();
    if window == 0 || eligible.is_empty() {
        return Vec::new();
    }
    if eligible.len() > window {
        let start = eligible.len() - window;
        eligible = eligible[start..].to_vec();
    }
    eligible
}

fn weekday_name(date: &str) -> String {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map(|d| match d.weekday() {
            Weekday::Mon => "Mon",
            Weekday::Tue => "Tue",
            Weekday::Wed => "Wed",
            Weekday::Thu => "Thu",
            Weekday::Fri => "Fri",
            Weekday::Sat => "Sat",
            Weekday::Sun => "Sun",
        })
        .unwrap_or("?")
        .to_string()
}

fn is_weekday(date: &str) -> bool {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map(|d| !matches!(d.weekday(), Weekday::Sat | Weekday::Sun))
        .unwrap_or(false)
}

fn calendar_day_before(date: &str) -> Result<String, String> {
    let d =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| format!("invalid date: {date}"))?;
    Ok((d - chrono::Duration::days(1))
        .format("%Y-%m-%d")
        .to_string())
}

fn scid_path_for(symbol: &str) -> PathBuf {
    let cfg = load_feed_config();
    PathBuf::from(cfg.sierra_data_dir.trim_end_matches(['\\', '/'])).join(format!("{symbol}.scid"))
}

fn collect_transition_exclusions(evidence: &RolloverEvidence) -> Vec<String> {
    let mut set = BTreeSet::new();
    for t in &evidence.transitions {
        for d in &t.transition_exclusions {
            set.insert(d.clone());
        }
    }
    set.into_iter().collect()
}

fn gap_threshold_ms(session_type: SessionType) -> f64 {
    match session_type {
        SessionType::Rth => RTH_GAP_THRESHOLD_MS,
        SessionType::Globex => GLOBEX_GAP_THRESHOLD_MS,
        SessionType::Unknown => f64::INFINITY,
    }
}

fn median_usize(mut vals: Vec<usize>) -> usize {
    if vals.is_empty() {
        return 0;
    }
    vals.sort_unstable();
    vals[vals.len() / 2]
}

fn hash_study_sources() -> Result<Vec<FileSha256>, String> {
    let mut paths = vec![
        PathBuf::from("src/research/idea034_bucket_stats.rs"),
        PathBuf::from("src/bin/the-desk-idea034-bucket-stats.rs"),
        PathBuf::from("src/research/ib_campaign.rs"),
        PathBuf::from("src/feed/scid_reader.rs"),
        PathBuf::from("src/lib.rs"),
    ];
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        if !path.exists() {
            // Allow missing binary path during unit tests; still require module.
            if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("the-desk-idea034"))
            {
                continue;
            }
            return Err(format!("missing study source: {}", path.display()));
        }
        let canon = std::fs::canonicalize(&path)
            .map(|p| strip_verbatim_prefix(&p))
            .unwrap_or_else(|_| path.clone());
        out.push(FileSha256 {
            path: canon.display().to_string(),
            sha256: sha256_file(&path)?,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct BucketAccum {
    volume: f64,
    trade_count: u64,
    signed_delta: f64,
}

#[derive(Debug, Clone)]
struct DayAccum {
    contract: String,
    buckets: [BucketAccum; BUCKETS_PER_DAY as usize],
    monotonic_rejections: u64,
    material_gaps: Vec<(f64, f64, String)>,
    tick_count: u64,
}

impl DayAccum {
    fn new(contract: &str) -> Self {
        Self {
            contract: contract.to_string(),
            buckets: std::array::from_fn(|_| BucketAccum::default()),
            monotonic_rejections: 0,
            material_gaps: Vec::new(),
            tick_count: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Prepare / execute
// ---------------------------------------------------------------------------

fn run_owned_artifact(run_dir: &Path, name: &str) -> Result<PathBuf, String> {
    if name.contains(['/', '\\']) || name == ".." || name == "." || name.is_empty() {
        return Err(format!("invalid artifact basename: {name}"));
    }
    Ok(run_dir.join(name))
}

/// Prepare an isolated run directory + fresh DB + rollover evidence (no SCID replay).
pub fn prepare_campaign_run(req: PrepareRequest<'_>) -> Result<PrepareResult, String> {
    if is_forbidden_live_db_path(req.run_dir) {
        return Err(format!(
            "refusing forbidden live DB path as run dir: {}",
            req.run_dir.display()
        ));
    }
    if req.db_filename.eq_ignore_ascii_case("data.db") {
        return Err(format!(
            "refusing live DB filename; forbidden paths include {:?}",
            FORBIDDEN_LIVE_DB_PATHS
        ));
    }

    // Ensure campaign root exists before strict-beneath resolve.
    std::fs::create_dir_all(req.campaign_root)
        .map_err(|e| format!("create campaign root {}: {e}", req.campaign_root.display()))?;

    let (rollover, windows) = resolve_rollover_and_windows(RolloverVolumeSource::ScanLocalScid)?;
    require_resolved_rollover_crossovers(&rollover)?;

    // Verify SCID files exist for each window before proceeding.
    for w in &windows {
        let path = scid_path_for(&w.contract);
        if !path.exists() {
            return Err(format!(
                "BLOCKED: SCID missing for {}: {}",
                w.contract,
                path.display()
            ));
        }
    }

    let resolved = resolve_path_strictly_beneath(req.run_dir, req.campaign_root)?;
    if resolved.exists() {
        let mut existing = Vec::new();
        for name in RUN_ARTIFACT_NAMES {
            let p = run_owned_artifact(&resolved, name)?;
            if p.exists() {
                existing.push(p);
            }
        }
        if !existing.is_empty() {
            if !req.overwrite {
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
                if p.is_dir() {
                    std::fs::remove_dir_all(p).map_err(|e| e.to_string())?;
                } else {
                    std::fs::remove_file(p).map_err(|e| e.to_string())?;
                }
            }
        }
    } else {
        std::fs::create_dir_all(&resolved)
            .map_err(|e| format!("create run dir {}: {e}", resolved.display()))?;
    }
    let canonical_run = resolve_path_strictly_beneath(&resolved, req.campaign_root)?;
    let canonical_db =
        create_fresh_isolated_campaign_db(&canonical_run, req.db_filename, req.overwrite)?;

    let transition_exclusions = collect_transition_exclusions(&rollover);
    let provenance = build_provenance(
        req.git_commit,
        req.git_dirty,
        &canonical_run,
        &canonical_db,
        &windows,
        &transition_exclusions,
        req.commands.clone(),
        "stage1_prepare",
    )?;
    write_json(&canonical_run.join("provenance.json"), &provenance)?;
    write_json(&canonical_run.join("rollover_evidence.json"), &rollover)?;

    Ok(PrepareResult {
        canonical_run_dir: canonical_run,
        canonical_db_path: canonical_db,
        provenance,
        rollover,
        windows,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_provenance(
    git_commit: &str,
    git_dirty: bool,
    run_dir: &Path,
    db_path: &Path,
    windows: &[ContractWindow],
    transition_exclusions: &[String],
    commands: Vec<String>,
    stage: &str,
) -> Result<Provenance, String> {
    let (binary_path, binary_sha256) = match hash_current_binary() {
        Ok((p, h)) => (Some(p), Some(h)),
        Err(_) => (None, None),
    };
    let canonical_run = std::fs::canonicalize(run_dir)
        .map(|p| strip_verbatim_prefix(&p).display().to_string())
        .unwrap_or_else(|_| run_dir.display().to_string());
    Ok(Provenance {
        study_id: STUDY_ID.into(),
        study_version: STUDY_VERSION,
        stage: stage.into(),
        git_commit: git_commit.into(),
        git_dirty,
        campaign_root: CAMPAIGN_ROOT.into(),
        run_dir: run_dir.display().to_string(),
        canonical_run_dir: canonical_run,
        isolated_db_path: db_path.display().to_string(),
        commands,
        windows: windows.to_vec(),
        excluded_session_dates: EXCLUDED_SESSION_DATES.iter().map(|s| (*s).to_string()).collect(),
        transition_exclusions: transition_exclusions.to_vec(),
        dst_mismatch_range: [DST_MISMATCH_START.into(), DST_MISMATCH_END.into()],
        clean_subset_start: CLEAN_SUBSET_START_DATE.into(),
        baseline_windows: BASELINE_WINDOWS.to_vec(),
        baseline_scope: BASELINE_SCOPE.into(),
        percentile_method: RESEARCH_PERCENTILE_METHOD.into(),
        signed_delta_rule: "Buy=+volume, Sell=-volume, Unknown=0; volume always counted in executed_volume (scid_reader TradeSide)".into(),
        tape_speed_rule: "tape_speed_trades_per_sec = trade_count / 300; tape_speed_contracts_per_sec = executed_volume / 300 (fixed 5-min bucket)".into(),
        source_file_hashes: hash_study_sources()?,
        binary_path,
        binary_sha256,
    })
}

/// One-pass SCID replay for a crossover-derived contract window → day accumulators.
fn replay_contract_window(
    window: &ContractWindow,
) -> Result<(BTreeMap<String, DayAccum>, Vec<String>), String> {
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

    let mut days: BTreeMap<String, DayAccum> = BTreeMap::new();
    let mut mono = MonotonicTickGuard::default();
    let mut prev_ts: Option<f64> = None;
    let mut prev_class = SessionType::Unknown;
    let mut records = 0usize;
    let mut last_progress = 0usize;

    reader
        .scan_range_in_file_order(start_ms, end_ms, |tick| {
            records += 1;
            if records - last_progress >= 2_000_000 {
                last_progress = records;
                eprintln!(
                    "  … scanned {}M records (trading-days so far {})",
                    records / 1_000_000,
                    days.len()
                );
            }

            match mono.observe(tick.timestamp_ms) {
                MonotonicTimestampDecision::Accept => {}
                MonotonicTimestampDecision::Skip(_) => {
                    if let Some(ctx) = tick_time_context_from_timestamp_ms(tick.timestamp_ms) {
                        let day = days
                            .entry(ctx.trading_day.clone())
                            .or_insert_with(|| DayAccum::new(&window.contract));
                        day.monotonic_rejections += 1;
                    }
                    return Ok(ScanControl::Continue);
                }
            }

            let Some(ctx) = tick_time_context_from_timestamp_ms(tick.timestamp_ms) else {
                return Ok(ScanControl::Continue);
            };

            // Gap scan: consecutive same-session-class ticks (backfill.rs semantics).
            if let Some(prev) = prev_ts {
                let gap_ms = tick.timestamp_ms - prev;
                if gap_ms > 0.0 && ctx.session_type == prev_class {
                    let threshold = gap_threshold_ms(ctx.session_type);
                    if gap_ms > threshold {
                        let day = days
                            .entry(ctx.trading_day.clone())
                            .or_insert_with(|| DayAccum::new(&window.contract));
                        day.material_gaps.push((
                            prev,
                            tick.timestamp_ms,
                            format!("{:?}", ctx.session_type),
                        ));
                    }
                }
            }
            prev_ts = Some(tick.timestamp_ms);
            prev_class = ctx.session_type;

            let trading_day = ctx.trading_day.clone();
            if trading_day.as_str() < window.start_date.as_str()
                || trading_day.as_str() > window.end_date.as_str()
                || trading_day.as_str() > DEVELOPMENT_END_DATE
            {
                return Ok(ScanControl::Continue);
            }

            let day = days
                .entry(trading_day)
                .or_insert_with(|| DayAccum::new(&window.contract));
            day.tick_count += 1;
            let idx = bucket_index_from_et_minutes(ctx.et_minutes) as usize;
            let bucket = &mut day.buckets[idx];
            bucket.volume += tick.volume;
            bucket.trade_count += 1;
            bucket.signed_delta += signed_delta_for_side(&tick.side, tick.volume);
            Ok(ScanControl::Continue)
        })
        .map_err(|e| e.to_string())?;

    // Drop days outside the window that may have been created only for mono/gap notes.
    days.retain(|d, _| {
        d.as_str() >= window.start_date.as_str()
            && d.as_str() <= window.end_date.as_str()
            && d.as_str() <= DEVELOPMENT_END_DATE
    });

    Ok((days, warnings))
}

fn day_exclusion_reasons(
    date: &str,
    day: &DayAccum,
    transition_exclusions: &BTreeSet<String>,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !is_weekday(date) {
        reasons.push("weekend".into());
    }
    if is_holiday_or_early_close(date) {
        reasons.push("holiday_or_early_close".into());
    }
    if transition_exclusions.contains(date) {
        reasons.push("rollover_transition".into());
    }
    if is_dst_mismatch_date(date) {
        reasons.push("dst_mismatch_cohort".into());
    }
    if !day.material_gaps.is_empty() {
        reasons.push(format!("material_gap_count={}", day.material_gaps.len()));
    }
    reasons
}

/// Primary-eligible for baselines/coverage: weekday, not holiday, not rollover seam,
/// not DST-mismatch cohort, no material gaps. Monotonic rejections are counted but do
/// not alone exclude the day (ticks already dropped).
fn is_baseline_eligible(
    date: &str,
    day: &DayAccum,
    transition_exclusions: &BTreeSet<String>,
) -> bool {
    is_weekday(date)
        && !is_holiday_or_early_close(date)
        && !transition_exclusions.contains(date)
        && !is_dst_mismatch_date(date)
        && day.material_gaps.is_empty()
        && day.tick_count > 0
}

fn build_baseline_stats(
    window: usize,
    volumes: &[f64],
    tape_speeds: &[f64],
    abs_deltas: &[f64],
    observed_volume: f64,
    observed_tape: f64,
    observed_abs_delta: f64,
) -> BaselineWindowStats {
    let available = volumes.len() >= window;
    let mut vol_sorted = volumes.to_vec();
    vol_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut tape_sorted = tape_speeds.to_vec();
    tape_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut delta_sorted = abs_deltas.to_vec();
    delta_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let volume_median = percentile_type7(&vol_sorted, 50.0);
    let volume_p95 = percentile_type7(&vol_sorted, VOLUME_P95_THRESHOLD);
    let tape_speed_median = percentile_type7(&tape_sorted, 50.0);
    let tape_speed_p95 = percentile_type7(&tape_sorted, VOLUME_P95_THRESHOLD);
    let abs_delta_median = percentile_type7(&delta_sorted, 50.0);
    let abs_delta_p95 = percentile_type7(&delta_sorted, VOLUME_P95_THRESHOLD);

    let anomaly_volume_p95 = volume_p95.map(|p| observed_volume > p);
    let anomaly_volume_k2 =
        volume_median.map(|m| m > 0.0 && observed_volume >= VOLUME_K_MULTIPLES[0] * m);
    let anomaly_volume_k3 =
        volume_median.map(|m| m > 0.0 && observed_volume >= VOLUME_K_MULTIPLES[1] * m);
    let anomaly_tape_speed_p95 = tape_speed_p95.map(|p| observed_tape > p);
    let anomaly_abs_delta_p95 = abs_delta_p95.map(|p| observed_abs_delta > p);

    BaselineWindowStats {
        window,
        baseline_scope: BASELINE_SCOPE.into(),
        sample_count: volumes.len(),
        volume_median,
        volume_p95,
        tape_speed_median,
        tape_speed_p95,
        abs_delta_median,
        abs_delta_p95,
        anomaly_volume_p95: if volumes.is_empty() {
            None
        } else {
            anomaly_volume_p95
        },
        anomaly_volume_k2: if volumes.is_empty() {
            None
        } else {
            anomaly_volume_k2
        },
        anomaly_volume_k3: if volumes.is_empty() {
            None
        } else {
            anomaly_volume_k3
        },
        anomaly_tape_speed_p95: if tape_speeds.is_empty() {
            None
        } else {
            anomaly_tape_speed_p95
        },
        anomaly_abs_delta_p95: if abs_deltas.is_empty() {
            None
        } else {
            anomaly_abs_delta_p95
        },
        available,
    }
}

/// Finalize day maps into bucket-stat rows with causal trailing baselines.
fn finalize_bucket_stats(
    all_days: &BTreeMap<String, DayAccum>,
    transition_exclusions: &BTreeSet<String>,
) -> (Vec<BucketStatRow>, Vec<DayExclusion>) {
    // Per-bucket catalogs of (date, volume / tape / abs_delta) for baseline-eligible days only.
    let mut vol_catalog: Vec<Vec<(String, f64)>> = vec![Vec::new(); BUCKETS_PER_DAY as usize];
    let mut tape_catalog: Vec<Vec<(String, f64)>> = vec![Vec::new(); BUCKETS_PER_DAY as usize];
    let mut delta_catalog: Vec<Vec<(String, f64)>> = vec![Vec::new(); BUCKETS_PER_DAY as usize];

    let mut exclusions = Vec::new();
    let mut rows = Vec::new();

    for (date, day) in all_days {
        let reasons = day_exclusion_reasons(date, day, transition_exclusions);
        let baseline_ok = is_baseline_eligible(date, day, transition_exclusions);
        if !reasons.is_empty() {
            exclusions.push(DayExclusion {
                trading_date: date.clone(),
                contract: day.contract.clone(),
                reasons: reasons.clone(),
            });
        }

        for idx in 0..BUCKETS_PER_DAY {
            let accum = &day.buckets[idx as usize];
            let segment = segment_for_bucket(idx);
            let start_min = idx as i32 * BUCKET_MINUTES;
            let end_min = start_min + BUCKET_MINUTES;
            let tape = accum.trade_count as f64 / BUCKET_DURATION_SECS;
            let tape_vol = accum.volume / BUCKET_DURATION_SECS;
            let abs_delta = accum.signed_delta.abs();

            let (quality_status, exclusion_reason) =
                if matches!(segment, SessionSegmentLabel::OutOfScope) {
                    (
                        "out_of_scope".to_string(),
                        Some("session_gap_1615_1800".into()),
                    )
                } else if !baseline_ok {
                    let primary = reasons
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "excluded".into());
                    ("excluded".to_string(), Some(primary))
                } else {
                    ("eligible".to_string(), None)
                };

            let mut baselines = Vec::new();
            for &window in &BASELINE_WINDOWS {
                let vols = select_trailing_same_bucket(&vol_catalog[idx as usize], date, window);
                let tapes = select_trailing_same_bucket(&tape_catalog[idx as usize], date, window);
                let deltas =
                    select_trailing_same_bucket(&delta_catalog[idx as usize], date, window);
                baselines.push(build_baseline_stats(
                    window,
                    &vols,
                    &tapes,
                    &deltas,
                    accum.volume,
                    tape,
                    abs_delta,
                ));
            }

            rows.push(BucketStatRow {
                trading_date: date.clone(),
                bucket_index: idx,
                bucket_start_et: format_et_hhmm(start_min),
                bucket_end_et: format_et_hhmm(end_min),
                session_segment: segment.as_str().to_string(),
                contract: day.contract.clone(),
                executed_volume: accum.volume,
                trade_count: accum.trade_count,
                tape_speed_trades_per_sec: tape,
                tape_speed_contracts_per_sec: tape_vol,
                signed_delta: accum.signed_delta,
                quality_status,
                exclusion_reason,
                is_dst_mismatch_cohort: is_dst_mismatch_date(date),
                is_clean_subset: is_clean_subset_date(date),
                weekday: weekday_name(date),
                monotonic_rejections: day.monotonic_rejections,
                gap_count: day.material_gaps.len() as u64,
                baselines,
            });

            // Append to catalogs only after computing this day's baselines (no leakage).
            if baseline_ok && is_in_scope_bucket(idx) {
                vol_catalog[idx as usize].push((date.clone(), accum.volume));
                tape_catalog[idx as usize].push((date.clone(), tape));
                delta_catalog[idx as usize].push((date.clone(), abs_delta));
            }
        }
    }

    (rows, exclusions)
}

fn build_coverage(rows: &[BucketStatRow]) -> (Vec<CoverageBucketRow>, CoverageSummary) {
    let mut coverage = Vec::new();
    let mut eligible_ns = Vec::new();
    let mut clean_ns = Vec::new();

    for idx in 0..BUCKETS_PER_DAY {
        if !is_in_scope_bucket(idx) {
            continue;
        }
        let segment = segment_for_bucket(idx);
        let eligible_n = rows
            .iter()
            .filter(|r| r.bucket_index == idx && r.quality_status == "eligible")
            .count();
        // eligible already excludes DST; keep field for report symmetry with coverage gate.
        let eligible_n_ex_dst = eligible_n;
        let clean_subset_n = rows
            .iter()
            .filter(|r| {
                r.bucket_index == idx && r.quality_status == "eligible" && r.is_clean_subset
            })
            .count();
        eligible_ns.push(eligible_n);
        clean_ns.push(clean_subset_n);
        coverage.push(CoverageBucketRow {
            bucket_index: idx,
            bucket_start_et: format_et_hhmm(idx as i32 * BUCKET_MINUTES),
            session_segment: segment.as_str().to_string(),
            eligible_n,
            eligible_n_ex_dst,
            clean_subset_n,
            reliability_tier: reliability_tier(eligible_n),
            clean_subset_reliability_tier: reliability_tier(clean_subset_n),
        });
    }

    let summary = CoverageSummary {
        in_scope_buckets: coverage.len(),
        min_eligible_n: eligible_ns.iter().copied().min().unwrap_or(0),
        median_eligible_n: median_usize(eligible_ns.clone()),
        buckets_n_ge_30: eligible_ns.iter().filter(|&&n| n >= 30).count(),
        buckets_n_20_29: eligible_ns
            .iter()
            .filter(|&&n| (20..30).contains(&n))
            .count(),
        buckets_n_lt_20: eligible_ns.iter().filter(|&&n| n < 20).count(),
        clean_subset_min_n: clean_ns.iter().copied().min().unwrap_or(0),
        clean_subset_median_n: median_usize(clean_ns.clone()),
        clean_subset_buckets_n_ge_30: clean_ns.iter().filter(|&&n| n >= 30).count(),
    };
    (coverage, summary)
}

fn build_baseline_availability(rows: &[BucketStatRow]) -> BaselineAvailability {
    let mut full = BTreeMap::new();
    let mut partial = BTreeMap::new();
    let mut vol_p95 = 0usize;
    let mut vol_k2 = 0usize;
    let mut vol_k3 = 0usize;
    let mut tape_p95 = 0usize;
    let mut delta_p95 = 0usize;

    for r in rows.iter().filter(|r| r.quality_status == "eligible") {
        for b in &r.baselines {
            let key = b.window.to_string();
            if b.available {
                *full.entry(key.clone()).or_insert(0) += 1;
            } else if b.sample_count > 0 {
                *partial.entry(key).or_insert(0) += 1;
            }
            if b.window == PRIMARY_BASELINE_WINDOW {
                if b.anomaly_volume_p95 == Some(true) {
                    vol_p95 += 1;
                }
                if b.anomaly_volume_k2 == Some(true) {
                    vol_k2 += 1;
                }
                if b.anomaly_volume_k3 == Some(true) {
                    vol_k3 += 1;
                }
                if b.anomaly_tape_speed_p95 == Some(true) {
                    tape_p95 += 1;
                }
                if b.anomaly_abs_delta_p95 == Some(true) {
                    delta_p95 += 1;
                }
            }
        }
    }

    BaselineAvailability {
        windows: BASELINE_WINDOWS.to_vec(),
        eligible_rows_with_full_baseline: full,
        eligible_rows_with_partial_baseline: partial,
        anomaly_volume_p95_true_primary20: vol_p95,
        anomaly_volume_k2_true_primary20: vol_k2,
        anomaly_volume_k3_true_primary20: vol_k3,
        anomaly_tape_p95_true_primary20: tape_p95,
        anomaly_abs_delta_p95_true_primary20: delta_p95,
    }
}

fn render_report_md(summary: &StudySummary, provenance: &Provenance) -> String {
    let mut out = String::new();
    out.push_str("# IDEA-034 Stage 1 — Bucket-Stats Replay\n\n");
    out.push_str(&format!(
        "- study: {} v{}\n- stage: {}\n- git: {} (dirty={})\n- run: {}\n",
        provenance.study_id,
        provenance.study_version,
        provenance.stage,
        provenance.git_commit,
        provenance.git_dirty,
        provenance.canonical_run_dir
    ));
    out.push_str(&format!(
        "- campaign: {} → {}\n- tradingDaysObserved={} eligibleDays={}\n",
        summary.campaign_start,
        summary.campaign_end,
        summary.trading_days_observed,
        summary.trading_days_eligible
    ));
    out.push_str(&format!(
        "- bucketStatRows={} eligibleBucketRows={}\n",
        summary.bucket_stat_rows, summary.eligible_bucket_rows
    ));
    out.push_str("\n## Contract windows\n\n");
    for w in &summary.contract_windows {
        out.push_str(&format!(
            "- {} {}..{}\n",
            w.contract, w.start_date, w.end_date
        ));
    }
    let cs = &summary.coverage_summary;
    out.push_str("\n## Coverage (in-scope buckets)\n\n");
    out.push_str(&format!(
        "- buckets={} minN={} medianN={} N≥30={} N20–29={} N<20={}\n",
        cs.in_scope_buckets,
        cs.min_eligible_n,
        cs.median_eligible_n,
        cs.buckets_n_ge_30,
        cs.buckets_n_20_29,
        cs.buckets_n_lt_20
    ));
    out.push_str(&format!(
        "- clean-subset 2026-06-23+: minN={} medianN={} N≥30={} (directional-only if N<30)\n",
        cs.clean_subset_min_n, cs.clean_subset_median_n, cs.clean_subset_buckets_n_ge_30
    ));
    out.push_str("\n## Baseline / anomaly (eligible rows, primary window=20)\n\n");
    let ba = &summary.baseline_availability;
    out.push_str(&format!(
        "- full baseline counts by window: {:?}\n",
        ba.eligible_rows_with_full_baseline
    ));
    out.push_str(&format!(
        "- volume p95 anomalies={} k2={} k3={} | tape p95={} | |delta| p95={}\n",
        ba.anomaly_volume_p95_true_primary20,
        ba.anomaly_volume_k2_true_primary20,
        ba.anomaly_volume_k3_true_primary20,
        ba.anomaly_tape_p95_true_primary20,
        ba.anomaly_abs_delta_p95_true_primary20
    ));
    out.push_str(
        "\n**No event-rate, calendar, continuation/reversal, or best-time claims.** Coverage N ≠ event N.\n",
    );
    if !summary.warnings.is_empty() {
        out.push_str("\n## Warnings\n\n");
        for w in &summary.warnings {
            out.push_str(&format!("- {w}\n"));
        }
    }
    out.push_str("\n## Notes\n\n");
    for n in &summary.notes {
        out.push_str(&format!("- {n}\n"));
    }
    out
}

/// Full Stage 1 execute: SCID replay → bucket stats → artifacts under isolated run dir.
pub fn execute_campaign(req: ExecuteRequest<'_>) -> Result<StudySummary, String> {
    if req.windows.is_empty() {
        return Err("BLOCKED: execute requires prepared crossover-derived windows".into());
    }
    require_resolved_rollover_crossovers(req.rollover)?;
    if is_forbidden_live_db_path(req.isolated_db_path) {
        return Err(format!(
            "refusing forbidden live DB path: {}",
            req.isolated_db_path.display()
        ));
    }
    let _ = resolve_path_strictly_beneath(req.run_dir, req.campaign_root)?;

    let transition_list = collect_transition_exclusions(req.rollover);
    let transition_set: BTreeSet<String> = transition_list.iter().cloned().collect();

    let mut all_days: BTreeMap<String, DayAccum> = BTreeMap::new();
    let mut warnings = Vec::new();

    for w in req.windows {
        eprintln!(
            "Executing IDEA-034 bucket-stats replay {} {}..{}...",
            w.contract, w.start_date, w.end_date
        );
        let (days, wwarn) = replay_contract_window(w).map_err(|e| {
            format!(
                "BLOCKED: SCID replay failed for {} {}..{}: {e}",
                w.contract, w.start_date, w.end_date
            )
        })?;
        warnings.extend(wwarn);
        eprintln!("  trading_days={}", days.len());
        for (date, day) in days {
            let new_contract = day.contract.clone();
            if let Some(prev) = all_days.insert(date.clone(), day) {
                warnings.push(format!(
                    "duplicate_trading_day_across_contracts: {date} prev={} new={new_contract}",
                    prev.contract
                ));
            }
        }
    }

    let (rows, exclusions) = finalize_bucket_stats(&all_days, &transition_set);
    let (coverage, coverage_summary) = build_coverage(&rows);
    let baseline_availability = build_baseline_availability(&rows);

    let mut exclusion_reason_counts: BTreeMap<String, usize> = BTreeMap::new();
    for ex in &exclusions {
        for r in &ex.reasons {
            let key = r.split('=').next().unwrap_or(r).to_string();
            *exclusion_reason_counts.entry(key).or_insert(0) += 1;
        }
    }

    let eligible_days = all_days
        .iter()
        .filter(|(d, day)| is_baseline_eligible(d, day, &transition_set))
        .count();
    let eligible_bucket_rows = rows
        .iter()
        .filter(|r| r.quality_status == "eligible")
        .count();

    let mut notes = vec![
        "Stage 1 bucket-stats only — no calendar extraction, event rates, or Stage 2 outcomes."
            .into(),
        format!(
            "Baselines: trailing same-bucket prior eligible days; scope={BASELINE_SCOPE}; windows={BASELINE_WINDOWS:?}; primary={PRIMARY_BASELINE_WINDOW}."
        ),
        "DST-mismatch cohort excluded from primary eligible N and from baseline catalogs (never silently pooled)."
            .into(),
        "Clean subset 2026-06-23+ is underpowered (directional-only) until more accrual."
            .into(),
        format!("Percentile method: {RESEARCH_PERCENTILE_METHOD}."),
        "Coverage N is days of eligible bucket coverage, not detected liquidity-event N.".into(),
    ];
    if !warnings.is_empty() {
        notes.push(format!("{} replay warnings recorded.", warnings.len()));
    }

    let summary = StudySummary {
        study_id: STUDY_ID.into(),
        study_version: STUDY_VERSION,
        campaign_start: CAMPAIGN_H6_START_DATE.into(),
        campaign_end: DEVELOPMENT_END_DATE.into(),
        contract_windows: req.windows.to_vec(),
        trading_days_observed: all_days.len(),
        trading_days_eligible: eligible_days,
        bucket_stat_rows: rows.len(),
        eligible_bucket_rows,
        in_scope_buckets: coverage_summary.in_scope_buckets,
        out_of_scope_buckets: (BUCKETS_PER_DAY as usize) - coverage_summary.in_scope_buckets,
        coverage,
        coverage_summary,
        exclusions: exclusions.clone(),
        exclusion_reason_counts,
        baseline_availability,
        warnings: warnings.clone(),
        notes,
    };

    let mut provenance = build_provenance(
        req.git_commit,
        req.git_dirty,
        req.run_dir,
        req.isolated_db_path,
        req.windows,
        &transition_list,
        req.commands.clone(),
        "stage1_execute",
    )?;
    if let Ok((p, h)) = hash_current_binary() {
        provenance.binary_path = Some(p);
        provenance.binary_sha256 = Some(h);
    }

    write_artifacts(
        req.run_dir,
        &rows,
        &summary,
        &provenance,
        req.rollover,
        &exclusions,
        req.isolated_db_path,
    )?;
    Ok(summary)
}

fn write_artifacts(
    run_dir: &Path,
    rows: &[BucketStatRow],
    summary: &StudySummary,
    provenance: &Provenance,
    rollover: &RolloverEvidence,
    exclusions: &[DayExclusion],
    isolated_db_path: &Path,
) -> Result<(), String> {
    let jsonl_path = run_dir.join("bucket_stats.jsonl");
    {
        let mut f = std::fs::File::create(&jsonl_path).map_err(|e| e.to_string())?;
        for row in rows {
            let line = serde_json::to_string(row).map_err(|e| e.to_string())?;
            writeln!(f, "{line}").map_err(|e| e.to_string())?;
        }
    }
    write_json(&run_dir.join("summary.json"), summary)?;
    write_json(&run_dir.join("coverage.json"), &summary.coverage)?;
    write_json(&run_dir.join("exclusions.json"), &exclusions.to_vec())?;
    write_json(&run_dir.join("provenance.json"), provenance)?;
    write_json(&run_dir.join("rollover_evidence.json"), rollover)?;
    let md = render_report_md(summary, provenance);
    std::fs::write(run_dir.join("REPORT.md"), md).map_err(|e| e.to_string())?;

    // Persist a compact SQLite mirror into the isolated DB (never the live DB).
    persist_rows_to_isolated_db(isolated_db_path, rows, summary)?;
    Ok(())
}

fn persist_rows_to_isolated_db(
    db_path: &Path,
    rows: &[BucketStatRow],
    summary: &StudySummary,
) -> Result<(), String> {
    if is_forbidden_live_db_path(db_path) {
        return Err(format!(
            "refusing persist to forbidden live DB: {}",
            db_path.display()
        ));
    }
    let path_str = db_path
        .to_str()
        .ok_or_else(|| format!("non-utf8 db path: {}", db_path.display()))?;
    let mut conn = rusqlite::Connection::open(path_str).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS idea034_bucket_stats (
            trading_date TEXT NOT NULL,
            bucket_index INTEGER NOT NULL,
            bucket_start_et TEXT NOT NULL,
            bucket_end_et TEXT NOT NULL,
            session_segment TEXT NOT NULL,
            contract TEXT NOT NULL,
            executed_volume REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            tape_speed_trades_per_sec REAL NOT NULL,
            tape_speed_contracts_per_sec REAL NOT NULL,
            signed_delta REAL NOT NULL,
            quality_status TEXT NOT NULL,
            exclusion_reason TEXT,
            is_dst_mismatch_cohort INTEGER NOT NULL,
            is_clean_subset INTEGER NOT NULL,
            baseline_n_20 INTEGER,
            anomaly_volume_p95_20 INTEGER,
            PRIMARY KEY (trading_date, bucket_index)
        );
        CREATE TABLE IF NOT EXISTS idea034_run_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );",
    )
    .map_err(|e| e.to_string())?;

    let tx = conn.transaction().map_err(|e| e.to_string())?;
    {
        let mut stmt = tx
            .prepare(
                "INSERT OR REPLACE INTO idea034_bucket_stats (
                    trading_date, bucket_index, bucket_start_et, bucket_end_et, session_segment,
                    contract, executed_volume, trade_count, tape_speed_trades_per_sec,
                    tape_speed_contracts_per_sec, signed_delta, quality_status, exclusion_reason,
                    is_dst_mismatch_cohort, is_clean_subset, baseline_n_20, anomaly_volume_p95_20
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
            )
            .map_err(|e| e.to_string())?;
        for r in rows {
            let b20 = r
                .baselines
                .iter()
                .find(|b| b.window == PRIMARY_BASELINE_WINDOW);
            stmt.execute(rusqlite::params![
                r.trading_date,
                r.bucket_index,
                r.bucket_start_et,
                r.bucket_end_et,
                r.session_segment,
                r.contract,
                r.executed_volume,
                r.trade_count as i64,
                r.tape_speed_trades_per_sec,
                r.tape_speed_contracts_per_sec,
                r.signed_delta,
                r.quality_status,
                r.exclusion_reason,
                r.is_dst_mismatch_cohort as i64,
                r.is_clean_subset as i64,
                b20.map(|b| b.sample_count as i64),
                b20.and_then(|b| b.anomaly_volume_p95.map(|v| v as i64)),
            ])
            .map_err(|e| e.to_string())?;
        }
    }
    tx.execute(
        "INSERT OR REPLACE INTO idea034_run_meta(key, value) VALUES ('summary_json', ?1)",
        rusqlite::params![serde_json::to_string(summary).map_err(|e| e.to_string())?],
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::TradeSide;

    #[test]
    fn bucket_index_covers_full_day() {
        assert_eq!(bucket_index_from_et_minutes(0), 0);
        assert_eq!(bucket_index_from_et_minutes(18 * 60), 216);
        assert_eq!(bucket_index_from_et_minutes(9 * 60 + 30), 114);
        assert_eq!(bucket_index_from_et_minutes(16 * 60 + 14), 194);
        assert_eq!(bucket_index_from_et_minutes(16 * 60 + 15), 195);
        assert_eq!(bucket_index_from_et_minutes(23 * 60 + 59), 287);
        assert_eq!(BUCKETS_PER_DAY, 288);
    }

    #[test]
    fn session_segments_match_repo_bounds() {
        assert_eq!(segment_for_bucket(216), SessionSegmentLabel::Asia); // 18:00
        assert_eq!(segment_for_bucket(0), SessionSegmentLabel::Asia); // 00:00
        assert_eq!(segment_for_bucket(24), SessionSegmentLabel::London); // 02:00
        assert_eq!(segment_for_bucket(113), SessionSegmentLabel::London); // 09:25
        assert_eq!(segment_for_bucket(114), SessionSegmentLabel::Rth); // 09:30
        assert_eq!(segment_for_bucket(194), SessionSegmentLabel::Rth); // 16:10
        assert_eq!(segment_for_bucket(195), SessionSegmentLabel::OutOfScope); // 16:15
        assert_eq!(segment_for_bucket(215), SessionSegmentLabel::OutOfScope); // 17:55
        let in_scope = (0..BUCKETS_PER_DAY)
            .filter(|&i| is_in_scope_bucket(i))
            .count();
        assert_eq!(in_scope, 267);
    }

    #[test]
    fn trailing_baseline_has_no_future_leakage() {
        let catalog = vec![
            ("2026-01-01".into(), 10.0),
            ("2026-01-02".into(), 20.0),
            ("2026-01-03".into(), 30.0),
            ("2026-01-04".into(), 40.0),
        ];
        let selected = select_trailing_same_bucket(&catalog, "2026-01-03", 10);
        assert_eq!(selected, vec![10.0, 20.0]);
        let selected2 = select_trailing_same_bucket(&catalog, "2026-01-04", 2);
        assert_eq!(selected2, vec![20.0, 30.0]);
        let empty = select_trailing_same_bucket(&catalog, "2026-01-01", 20);
        assert!(empty.is_empty());
    }

    #[test]
    fn percentile_and_anomaly_thresholds() {
        let mut vals: Vec<f64> = (1..=20).map(|v| v as f64).collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95 = percentile_type7(&vals, 95.0).unwrap();
        assert!(p95 > 19.0 && p95 <= 20.0);
        let median = percentile_type7(&vals, 50.0).unwrap();
        assert!((median - 10.5).abs() < 1e-9);

        let b = build_baseline_stats(20, &vals, &vals, &vals, 100.0, 100.0, 100.0);
        assert_eq!(b.anomaly_volume_p95, Some(true));
        assert_eq!(b.anomaly_volume_k2, Some(true));
        assert_eq!(b.anomaly_volume_k3, Some(true));
        assert!(b.available);

        let short = build_baseline_stats(20, &vals[..5], &vals[..5], &vals[..5], 1.0, 1.0, 1.0);
        assert!(!short.available);
        assert_eq!(short.sample_count, 5);
    }

    #[test]
    fn quality_exclusions_dst_holiday_gap() {
        assert!(is_dst_mismatch_date("2026-03-15"));
        assert!(!is_dst_mismatch_date("2026-03-08"));
        assert!(is_holiday_or_early_close("2026-01-01"));
        assert!(is_clean_subset_date("2026-06-23"));
        assert!(!is_clean_subset_date("2026-06-22"));

        let mut day = DayAccum::new("NQH6.CME");
        day.tick_count = 100;
        let empty = BTreeSet::new();
        assert!(is_baseline_eligible("2026-01-05", &day, &empty)); // Monday

        day.material_gaps
            .push((0.0, 40.0 * 60_000.0, "Globex".into()));
        assert!(!is_baseline_eligible("2026-01-05", &day, &empty));

        let mut day2 = DayAccum::new("NQH6.CME");
        day2.tick_count = 100;
        assert!(!is_baseline_eligible("2026-03-16", &day2, &empty)); // DST cohort
        assert!(!is_baseline_eligible("2026-01-01", &day2, &empty)); // holiday
    }

    #[test]
    fn signed_delta_matches_pipeline_convention() {
        assert_eq!(signed_delta_for_side(&TradeSide::Buy, 5.0), 5.0);
        assert_eq!(signed_delta_for_side(&TradeSide::Sell, 5.0), -5.0);
        assert_eq!(signed_delta_for_side(&TradeSide::Unknown, 5.0), 0.0);
    }

    #[test]
    fn finalize_does_not_leak_same_day_into_baseline() {
        let mut days = BTreeMap::new();
        for (date, vol) in [
            ("2026-01-05", 100.0),
            ("2026-01-06", 200.0),
            ("2026-01-07", 300.0),
        ] {
            let mut d = DayAccum::new("NQH6.CME");
            d.tick_count = 1;
            d.buckets[114].volume = vol; // 09:30 RTH
            d.buckets[114].trade_count = 10;
            days.insert(date.to_string(), d);
        }
        let (rows, _) = finalize_bucket_stats(&days, &BTreeSet::new());
        let r070 = rows
            .iter()
            .find(|r| r.trading_date == "2026-01-07" && r.bucket_index == 114)
            .unwrap();
        let b10 = r070.baselines.iter().find(|b| b.window == 10).unwrap();
        assert_eq!(b10.sample_count, 2);
        // Baseline must be prior days only (100, 200) — not 300.
        assert_eq!(b10.volume_median, Some(150.0));
    }

    #[test]
    fn campaign_root_is_isolated_temp_path() {
        assert!(CAMPAIGN_ROOT.starts_with(r"T:\TheDesk\temp\backtests\"));
        assert!(!CAMPAIGN_ROOT.contains(r"state\data.db"));
        for live in FORBIDDEN_LIVE_DB_PATHS {
            assert!(!CAMPAIGN_ROOT.eq_ignore_ascii_case(live));
        }
    }
}
