//! SIL-M3c research query kernel (`query_series` / `query_episodes` / `query_raw` + `run_job`).
//!
//! Trust Level **L0** — structurally incapable of mutating workflow-verb state
//! or placing orders. Episode predicates compose over Desk Catalog fields on
//! co-recorded NQ+ES Journal Frames (and optional event rows). Forward returns
//! use tick-driven MFE/MAE (`outcomes::signed_excursion`); this is not a fill
//! simulator. Unbounded windows are rejected. Bulk output rides `run_job`
//! artifact handles (columnar path + summary), never a token flood.
//!
//! SIL-M3d: the same four operators can be pointed at cold session-partitioned
//! Journal Frame dumps (`store=cold`) without new tools, new envelope fields,
//! or DuckDB. Default `store=hot` is the SQLite window. Events / ticks stay
//! on SQLite even when frames are read from cold.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::catalog::{
    build_catalog, DeskCatalog, FieldDescriptor, FrameRef, PositioningRecord, TrustCeiling,
    TrustLevel, CATALOG_VERSION,
};
use crate::db::{
    journal_frame_second_from_ts, Database, DbError, HypothesisSignalOutcomeRow,
    JournalFrameRecord, ResearchQueryJobRecord,
};
use crate::engine::{
    default_cold_frames_dir, ColdFrameError, ColdFrameStore, FrameStoreKind, JournalFrameRead,
};
use crate::outcomes::{signed_excursion, OutcomeDirection};

use super::{reliability_tier, ReliabilityTier};

/// NQ and ES futures tick size (points). NQ tick value is $5.00/contract.
pub const FUTURES_TICK_SIZE: f64 = 0.25;

/// Default proximity for "near a Positioning level" (ticks).
pub const DEFAULT_POSITIONING_NEAR_TICKS: f64 = 8.0;

/// Flagship extreme seller-aggression threshold on `sessionDelta` (contracts).
pub const FLAGSHIP_SELLER_AGGRESSION_SESSION_DELTA: f64 = -400.0;

/// Maximum `query_series` window (8 hours of 1 Hz frames).
pub const QUERY_SERIES_MAX_WINDOW_MS: f64 = 8.0 * 3_600_000.0;

/// Maximum `query_episodes` window (7 calendar days).
pub const QUERY_EPISODES_MAX_WINDOW_MS: f64 = 7.0 * 86_400_000.0;

/// Maximum `query_raw` window (15 minutes).
pub const QUERY_RAW_MAX_WINDOW_MS: f64 = 15.0 * 60_000.0;

/// Hard cap on `query_raw` rows.
pub const QUERY_RAW_MAX_ROWS: usize = 5_000;

/// Hard cap on series points returned from the engine.
pub const QUERY_SERIES_MAX_POINTS: usize = 60_000;

/// Hard cap on episode matches returned from the engine.
pub const QUERY_EPISODES_MAX_MATCHES: usize = 500;

/// Hard cap on Journal Frames scanned for an Episode Query.
pub const QUERY_EPISODES_MAX_FRAMES: usize = 500_000;

/// Forward horizon for tick-driven MFE/MAE after an episode (15 minutes).
pub const QUERY_FORWARD_HORIZON_MS: f64 = 15.0 * 60_000.0;

/// Hard cap on ticks scanned for one episode's forward MFE/MAE.
pub const QUERY_FORWARD_TICK_CAP: usize = 50_000;

/// Max episode matches for which tick-driven MFE/MAE is computed.
///
/// Each match issues its own `[clock, clock+15m]` tick scan. Without an
/// aggregate bound, a 500-match run is 500 overlapping 15-minute reads.
pub const QUERY_FORWARD_MAX_MATCHES: usize = 50;

/// Page size for Episode Query / series scans (bounds peak memory).
pub const QUERY_PAGE_MS: f64 = 30.0 * 60_000.0;

/// Positioning as-of older than this relative to the episode clock is degraded.
pub const POSITIONING_STALE_MS: f64 = 7.0 * 86_400_000.0;

/// Max journal-backed windows inlined on a hypothesis summary.
pub const HYPOTHESIS_EVIDENCE_WINDOW_CAP: usize = 50;

/// Catalog field: ES/NQ last price.
pub const FIELD_LAST_PRICE: &str = "market.location_structure.lastPrice";
/// Catalog field: session delta (aggression).
pub const FIELD_SESSION_DELTA: &str = "market.flow.sessionDelta";
/// Catalog field: TPO poor low (auction efficiency).
pub const FIELD_POOR_LOW: &str = "market.location_structure.poorLow";
/// Catalog field: compact DOM summary blob (replenishing bids path).
pub const FIELD_DOM_SUMMARY: &str = "market.liquidity.domSummary";
/// Catalog field: Positioning derived levels (Levels-Only Records today).
pub const FIELD_POSITIONING_LEVELS: &str = "positioning.derivedLevels";

/// JSON path inside `domSummary` for replenishing bids (fail closed when absent).
pub const DOM_BID_REPLENISHING_PATH: &str = "bidReplenishing";

/// Errors from the research query kernel. Converted to `String` only at MCP/CLI.
#[derive(Debug, Error)]
pub enum QueryKernelError {
    #[error("query requires startMs and endMs; unbounded windows are rejected")]
    UnboundedWindow,
    #[error("{kind} window {requested_ms} ms exceeds {cap_ms} ms cap")]
    WindowTooLarge {
        kind: &'static str,
        requested_ms: f64,
        cap_ms: f64,
    },
    #[error("startMs must be less than endMs")]
    InvertedWindow,
    #[error("startMs/endMs must be finite positive epoch milliseconds")]
    InvalidTimestamp,
    #[error(
        "window mixes RTH and Globex without explicit sessionType — refuse mixed session scope"
    )]
    MixedSessionScope,
    #[error("unknown catalog field `{0}`")]
    UnknownCatalogField(String),
    #[error("unknown predicate op `{0}`")]
    UnknownOp(String),
    #[error("unknown query kind `{0}` (expected series, episodes, or raw)")]
    UnknownKind(String),
    #[error("unknown query_raw source `{0}` (expected journal_frames, events, or ticks)")]
    UnknownRawSource(String),
    #[error("unknown frame store `{0}` (expected hot or cold)")]
    UnknownFrameStore(String),
    #[error("cold frame store only serves journal_frames; `{0}` remains on the SQLite hot window")]
    ColdStoreUnsupportedSource(String),
    #[error("{0}")]
    Db(String),
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Invalid(String),
}

impl From<DbError> for QueryKernelError {
    fn from(e: DbError) -> Self {
        Self::Db(e.to_string())
    }
}

impl From<ColdFrameError> for QueryKernelError {
    fn from(e: ColdFrameError) -> Self {
        match e {
            ColdFrameError::Io(s) => Self::Io(s),
            ColdFrameError::Invalid(s) => Self::Invalid(s),
        }
    }
}

impl From<std::io::Error> for QueryKernelError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// Query kind for window caps and `run_job`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryKind {
    Series,
    Episodes,
    Raw,
}

impl QueryKind {
    /// Parse wire labels `series` / `episodes` / `raw`.
    pub fn parse(raw: &str) -> Result<Self, QueryKernelError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "series" => Ok(Self::Series),
            "episodes" => Ok(Self::Episodes),
            "raw" => Ok(Self::Raw),
            other => Err(QueryKernelError::UnknownKind(other.to_string())),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Series => "series",
            Self::Episodes => "episodes",
            Self::Raw => "raw",
        }
    }

    fn max_window_ms(self) -> f64 {
        match self {
            Self::Series => QUERY_SERIES_MAX_WINDOW_MS,
            Self::Episodes => QUERY_EPISODES_MAX_WINDOW_MS,
            Self::Raw => QUERY_RAW_MAX_WINDOW_MS,
        }
    }
}

/// Comparison operator over a catalog field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PredicateOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Near,
    Exists,
}

impl PredicateOp {
    /// Parse a wire operator (`eq`, `lte`, `near`, …).
    pub fn parse(raw: &str) -> Result<Self, QueryKernelError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eq" | "==" => Ok(Self::Eq),
            "ne" | "!=" => Ok(Self::Ne),
            "gt" | ">" => Ok(Self::Gt),
            "gte" | ">=" => Ok(Self::Gte),
            "lt" | "<" => Ok(Self::Lt),
            "lte" | "<=" => Ok(Self::Lte),
            "near" => Ok(Self::Near),
            "exists" => Ok(Self::Exists),
            other => Err(QueryKernelError::UnknownOp(other.to_string())),
        }
    }
}

/// One conjunctive predicate over a Desk Catalog field (or an event type).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    pub field: String,
    pub op: PredicateOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Dotted path into a structured catalog blob (e.g. `bidReplenishing` on `domSummary`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerance_ticks: Option<f64>,
    /// When set, this predicate matches an event row at the episode `frame_ref` instead of a frame field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
}

/// Shared time window. `start_ms` and `end_ms` are required (unbounded rejected).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryWindow {
    pub start_ms: Option<f64>,
    pub end_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbols: Option<Vec<String>>,
}

/// `query_series` request (R2 evidence).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySeriesRequest {
    #[serde(flatten)]
    pub window: QueryWindow,
    #[serde(default)]
    pub fields: Vec<String>,
}

/// `query_episodes` request (conjunctive catalog predicates + forward returns).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryEpisodesRequest {
    #[serde(flatten)]
    pub window: QueryWindow,
    #[serde(default)]
    pub predicates: Vec<CatalogPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forward_direction: Option<String>,
}

/// Options for Episode Query. Live MCP / [`query_episodes`] keep product window caps.
///
/// `enforce_product_window_cap = false` is SIL-M3e SLO-scan only — it does not
/// change the MCP/`run_job` contract (still 7 calendar days).
#[derive(Debug, Clone, Copy)]
pub struct QueryEpisodesOpts {
    /// When false, skip [`QUERY_EPISODES_MAX_WINDOW_MS`] so a bench can scan ~2 weeks of 1 Hz frames.
    pub enforce_product_window_cap: bool,
}

impl Default for QueryEpisodesOpts {
    fn default() -> Self {
        Self {
            enforce_product_window_cap: true,
        }
    }
}

/// `query_raw` request (R3, hard-capped).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRawRequest {
    #[serde(flatten)]
    pub window: QueryWindow,
    /// `journal_frames` | `events` | `ticks`.
    #[serde(default)]
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Sample-size / reliability / provenance envelope on every query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultMeta {
    pub n: usize,
    pub sample_size: usize,
    pub reliability_tier: ReliabilityTier,
    pub population_size: usize,
    pub rows_scanned: usize,
    pub truncated: bool,
    pub degraded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub notes: Vec<String>,
    pub catalog_version: String,
    pub trust_level: TrustLevel,
    pub trust_ceiling: TrustCeiling,
    pub mutation_authority: bool,
    pub order_authority: bool,
    pub journal_backed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_fields: Vec<String>,
}

impl QueryResultMeta {
    fn new(sample_size: usize, population_size: usize, rows_scanned: usize) -> Self {
        Self {
            n: sample_size,
            sample_size,
            reliability_tier: reliability_tier(sample_size),
            population_size,
            rows_scanned,
            truncated: false,
            degraded: false,
            scope: None,
            notes: Vec::new(),
            catalog_version: CATALOG_VERSION.to_string(),
            trust_level: TrustLevel::L0,
            trust_ceiling: TrustCeiling::L3,
            mutation_authority: false,
            order_authority: false,
            journal_backed: true,
            unavailable_fields: Vec::new(),
        }
    }

    fn mark_truncated(&mut self, note: String) {
        self.truncated = true;
        self.degraded = true;
        self.reliability_tier = ReliabilityTier::Insufficient;
        self.notes.push(note);
    }
}

/// One series point (1 Hz Journal Frame × root × requested fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesPoint {
    pub clock_ms: f64,
    pub frame_second: i64,
    pub root_symbol: String,
    pub session_type: String,
    pub trading_day: String,
    pub values: BTreeMap<String, Value>,
}

/// `query_series` result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySeriesResult {
    pub kind: QueryKind,
    pub points: Vec<SeriesPoint>,
    pub meta: QueryResultMeta,
}

/// Tick-driven forward return for one episode (no fill simulator).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForwardReturn {
    pub mfe_points: f64,
    pub mae_points: f64,
    pub forward_return_points: f64,
    pub tick_count: usize,
    pub direction: String,
    pub entry_price: f64,
    pub horizon_ms: f64,
    pub source: String,
    /// True when the forward tick scan hit `QUERY_FORWARD_TICK_CAP`.
    pub truncated: bool,
}

/// One conjunctive Episode Query match.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeMatch {
    pub clock_ms: f64,
    pub frame_ref: FrameRef,
    pub session_type: String,
    pub trading_day: String,
    pub values: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forward: Option<ForwardReturn>,
    pub journal_backed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub positioning_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub positioning_as_of_ms: Option<f64>,
}

/// Coverage of one predicate across scanned seconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredicateCoverage {
    pub id: String,
    pub field: String,
    pub available_seconds: usize,
    pub missing_seconds: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// `query_episodes` result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryEpisodesResult {
    pub kind: QueryKind,
    pub matches: Vec<EpisodeMatch>,
    pub predicate_coverage: Vec<PredicateCoverage>,
    pub meta: QueryResultMeta,
}

/// One hard-capped raw row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRow {
    pub timestamp_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_symbol: Option<String>,
    pub payload: Value,
}

/// `query_raw` result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRawResult {
    pub kind: QueryKind,
    pub source: String,
    pub rows: Vec<RawRow>,
    pub meta: QueryResultMeta,
}

/// Artifact handle returned by `run_job` (never the full token payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactHandle {
    pub columnar_path: String,
    pub summary_path: String,
    pub summary: Value,
}

/// `run_job` result: job id + artifact handle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunJobResult {
    pub job_id: String,
    pub status: String,
    pub kind: QueryKind,
    pub artifact: ArtifactHandle,
    pub meta: QueryResultMeta,
    pub trust_level: TrustLevel,
    pub mutation_authority: bool,
    pub order_authority: bool,
}

/// Journal-backed evidence window for hypothesis / research citations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalBackedEvidenceWindow {
    pub fired_at_ms: f64,
    pub frame_ref: FrameRef,
    pub journal_backed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trading_day: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_type: Option<String>,
}

/// Flagship five-predicate Episode Query (expressible over catalog fields).
///
/// 1. ES last price near Positioning `derivedLevels` (Levels-Only Records).
/// 2. ES `sessionDelta` at or below extreme seller aggression.
/// 3. ES `poorLow` (poor auction efficiency).
/// 4. ES `domSummary.bidReplenishing` (fail closed when the blob path is absent).
/// 5. NQ `sessionDelta` non-confirmation (`>= 0` while ES is selling).
pub fn flagship_episode_predicates() -> Vec<CatalogPredicate> {
    vec![
        CatalogPredicate {
            id: Some("esNearPositioningLevel".into()),
            symbol: Some("ES".into()),
            field: FIELD_LAST_PRICE.into(),
            op: PredicateOp::Near,
            value: Some(serde_json::json!({ "source": FIELD_POSITIONING_LEVELS })),
            path: None,
            tolerance_ticks: Some(DEFAULT_POSITIONING_NEAR_TICKS),
            event_type: None,
        },
        CatalogPredicate {
            id: Some("extremeSellerAggression".into()),
            symbol: Some("ES".into()),
            field: FIELD_SESSION_DELTA.into(),
            op: PredicateOp::Lte,
            value: Some(Value::from(FLAGSHIP_SELLER_AGGRESSION_SESSION_DELTA)),
            path: None,
            tolerance_ticks: None,
            event_type: None,
        },
        CatalogPredicate {
            id: Some("poorAuctionEfficiency".into()),
            symbol: Some("ES".into()),
            field: FIELD_POOR_LOW.into(),
            op: PredicateOp::Eq,
            value: Some(Value::Bool(true)),
            path: None,
            tolerance_ticks: None,
            event_type: None,
        },
        CatalogPredicate {
            id: Some("replenishingBids".into()),
            symbol: Some("ES".into()),
            field: FIELD_DOM_SUMMARY.into(),
            op: PredicateOp::Eq,
            value: Some(Value::Bool(true)),
            path: Some(DOM_BID_REPLENISHING_PATH.into()),
            tolerance_ticks: None,
            event_type: None,
        },
        CatalogPredicate {
            id: Some("nqNonConfirmation".into()),
            symbol: Some("NQ".into()),
            field: FIELD_SESSION_DELTA.into(),
            op: PredicateOp::Gte,
            value: Some(Value::from(0.0)),
            path: None,
            tolerance_ticks: None,
            event_type: None,
        },
    ]
}

/// Default artifact directory (`~/.the-desk/research-artifacts`).
pub fn default_research_artifact_dir() -> PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".the-desk")
        .join("research-artifacts")
}

/// Validate a bounded window. Rejects missing, inverted, non-finite, and oversized ranges.
pub fn validate_window(
    kind: QueryKind,
    start_ms: Option<f64>,
    end_ms: Option<f64>,
) -> Result<(f64, f64), QueryKernelError> {
    validate_window_ex(kind, start_ms, end_ms, true)
}

fn validate_window_ex(
    kind: QueryKind,
    start_ms: Option<f64>,
    end_ms: Option<f64>,
    enforce_cap: bool,
) -> Result<(f64, f64), QueryKernelError> {
    let (Some(start), Some(end)) = (start_ms, end_ms) else {
        return Err(QueryKernelError::UnboundedWindow);
    };
    if !start.is_finite() || !end.is_finite() || start <= 0.0 || end <= 0.0 {
        return Err(QueryKernelError::InvalidTimestamp);
    }
    if end <= start {
        return Err(QueryKernelError::InvertedWindow);
    }
    if enforce_cap {
        let span = end - start;
        let cap = kind.max_window_ms();
        if span > cap {
            return Err(QueryKernelError::WindowTooLarge {
                kind: kind.as_str(),
                requested_ms: span,
                cap_ms: cap,
            });
        }
    }
    Ok((start, end))
}

fn resolve_session_type(
    frames: &JournalFrameRead<'_>,
    start_ms: f64,
    end_ms: f64,
    requested: Option<&str>,
) -> Result<Option<String>, QueryKernelError> {
    if let Some(raw) = requested.map(str::trim).filter(|s| !s.is_empty()) {
        return Ok(Some(normalize_session_label(raw)?.to_string()));
    }
    let types = frames.session_types_in_window(start_ms, end_ms)?;
    let distinct: BTreeSet<String> = types.into_iter().filter(|t| t != "Unknown").collect();
    if distinct.contains("RTH") && distinct.contains("Globex") {
        return Err(QueryKernelError::MixedSessionScope);
    }
    Ok(distinct.into_iter().next())
}

fn normalize_session_label(raw: &str) -> Result<&'static str, QueryKernelError> {
    if raw.eq_ignore_ascii_case("rth") {
        Ok("RTH")
    } else if raw.eq_ignore_ascii_case("globex") {
        Ok("Globex")
    } else {
        Err(QueryKernelError::Invalid(format!(
            "sessionType `{raw}` is not RTH or Globex"
        )))
    }
}

fn session_label_for_timestamp(timestamp_ms: f64) -> Option<&'static str> {
    let mins = crate::et_minutes_from_timestamp(timestamp_ms)?;
    match crate::classify_session(mins) {
        crate::SessionType::Rth => Some("RTH"),
        crate::SessionType::Globex => Some("Globex"),
        crate::SessionType::Unknown => Some("Unknown"),
    }
}

/// Intersect `[start_ms, end_ms]` with the requested session on the ET clock.
///
/// `query_ticks_filtered_scoped` applies `LIMIT` before any session filter, so
/// Globex ticks at the open can consume the cap and hide later RTH rows.
/// Returns `None` when the intersection is empty.
fn clamp_window_to_session(
    start_ms: f64,
    end_ms: f64,
    session: Option<&str>,
) -> Option<(f64, f64)> {
    let Some(want) = session else {
        return Some((start_ms, end_ms));
    };
    let mut first: Option<f64> = None;
    let mut last: Option<f64> = None;
    let mut t = start_ms;
    loop {
        if session_label_for_timestamp(t) == Some(want) {
            first.get_or_insert(t);
            let minute_end = ((t / 60_000.0).floor() + 1.0) * 60_000.0 - 1.0;
            last = Some(minute_end.min(end_ms).max(t));
        }
        if t >= end_ms {
            break;
        }
        let next_minute = ((t / 60_000.0).floor() + 1.0) * 60_000.0;
        let next = next_minute.min(end_ms);
        if next <= t {
            break;
        }
        t = next;
    }
    match (first, last) {
        (Some(lo), Some(hi)) if lo <= hi => Some((lo, hi)),
        _ => None,
    }
}

fn event_matches_session(evt: &Value, session: Option<&str>) -> bool {
    let Some(want) = session else {
        return true;
    };
    if let Some(got) = evt.get("sessionType").and_then(|v| v.as_str()) {
        return got.eq_ignore_ascii_case(want);
    }
    evt.get("timestampMs")
        .and_then(|v| v.as_f64())
        .and_then(session_label_for_timestamp)
        .is_some_and(|got| got.eq_ignore_ascii_case(want))
}

fn resolve_catalog_field<'a>(
    catalog: &'a DeskCatalog,
    field_id: &str,
) -> Result<&'a FieldDescriptor, QueryKernelError> {
    catalog
        .fields
        .iter()
        .find(|f| f.id == field_id || f.name == field_id)
        .ok_or_else(|| QueryKernelError::UnknownCatalogField(field_id.to_string()))
}

fn extract_field_value(
    payload: &Value,
    field: &FieldDescriptor,
    path: Option<&str>,
) -> Option<Value> {
    let mut val = payload.get(&field.name).cloned().or_else(|| {
        field
            .id
            .rsplit('.')
            .next()
            .and_then(|tail| payload.get(tail).cloned())
    })?;
    if let Some(path) = path.filter(|p| !p.is_empty()) {
        for part in path.split('.') {
            val = val.get(part)?.clone();
        }
    }
    Some(val)
}

fn as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_i64().map(|i| i as f64))
}

fn values_equal(left: &Value, right: &Value) -> bool {
    if let (Some(a), Some(b)) = (as_f64(left), as_f64(right)) {
        return (a - b).abs() <= f64::EPSILON;
    }
    left == right
}

fn positioning_level_prices(record: &crate::catalog::PositioningRecord) -> Vec<f64> {
    let mut out = vec![
        record.derived_levels.flip,
        record.derived_levels.balance,
        record.derived_levels.upside_test,
        record.derived_levels.downside_test,
    ];
    for wall in &record.derived_levels.walls {
        out.push(wall.strike);
    }
    out.retain(|p| p.is_finite());
    out
}

fn select_positioning(records: &[PositioningRecord], clock_ms: f64) -> Option<&PositioningRecord> {
    let idx = records.partition_point(|r| r.as_of_ms <= clock_ms);
    idx.checked_sub(1).map(|i| &records[i])
}

fn positioning_is_stale(record: &PositioningRecord, clock_ms: f64) -> bool {
    clock_ms - record.as_of_ms > POSITIONING_STALE_MS
}

struct PredicateEval {
    matched: bool,
    available: bool,
    note: Option<String>,
}

fn eval_predicate(
    pred: &CatalogPredicate,
    field: Option<&FieldDescriptor>,
    by_root: &BTreeMap<String, &JournalFrameRecord>,
    events: &[Value],
    positioning_levels: Option<&[f64]>,
) -> PredicateEval {
    if let Some(event_type) = pred
        .event_type
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let root = pred.symbol.as_deref();
        let found = events.iter().any(|evt| {
            let ty = evt.get("eventType").and_then(|v| v.as_str()).unwrap_or("");
            if ty != event_type {
                return false;
            }
            match root {
                Some(want) => evt
                    .get("rootSymbol")
                    .and_then(|v| v.as_str())
                    .is_some_and(|got| got == want),
                None => true,
            }
        });
        return PredicateEval {
            matched: found,
            available: true,
            note: if found {
                None
            } else {
                Some(format!(
                    "no `{event_type}` event at this frame_ref (fail closed; no detector math invented)"
                ))
            },
        };
    }

    let Some(field) = field else {
        return PredicateEval {
            matched: false,
            available: false,
            note: Some(format!("unknown catalog field `{}`", pred.field)),
        };
    };

    if pred.field == FIELD_POSITIONING_LEVELS
        || pred
            .value
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|s| s.as_str())
            == Some(FIELD_POSITIONING_LEVELS)
    {
        // Positioning is joined, not read from the Journal Frame payload.
        if positioning_levels.is_none() {
            return PredicateEval {
                matched: false,
                available: false,
                note: Some(
                    "No Positioning record for this as-of. Levels-Only Records via positioning_entry \
                     are the source that exists today — not live vendor data."
                        .into(),
                ),
            };
        }
    }

    let root = pred
        .symbol
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let payload = match root {
        Some(sym) => by_root.get(sym).map(|f| &f.payload),
        None => by_root.values().next().map(|f| &f.payload),
    };
    let Some(payload) = payload else {
        return PredicateEval {
            matched: false,
            available: false,
            note: Some(format!(
                "no Journal Frame for symbol `{}` at this second",
                root.unwrap_or("?")
            )),
        };
    };

    if pred.op == PredicateOp::Exists {
        let present = extract_field_value(payload, field, pred.path.as_deref()).is_some();
        return PredicateEval {
            matched: present,
            available: true,
            note: None,
        };
    }

    let extracted = extract_field_value(payload, field, pred.path.as_deref());
    let Some(left) = extracted else {
        return PredicateEval {
            matched: false,
            available: false,
            note: Some(format!(
                "field `{}`{} absent on Journal Frame (fail closed)",
                pred.field,
                pred.path
                    .as_deref()
                    .map(|p| format!(".{p}"))
                    .unwrap_or_default()
            )),
        };
    };

    let matched = match pred.op {
        PredicateOp::Eq => pred
            .value
            .as_ref()
            .is_some_and(|rhs| values_equal(&left, rhs)),
        PredicateOp::Ne => pred
            .value
            .as_ref()
            .is_some_and(|rhs| !values_equal(&left, rhs)),
        PredicateOp::Gt => cmp_f64(&left, pred.value.as_ref(), |a, b| a > b),
        PredicateOp::Gte => cmp_f64(&left, pred.value.as_ref(), |a, b| a >= b),
        PredicateOp::Lt => cmp_f64(&left, pred.value.as_ref(), |a, b| a < b),
        PredicateOp::Lte => cmp_f64(&left, pred.value.as_ref(), |a, b| a <= b),
        PredicateOp::Near => eval_near(&left, pred, positioning_levels),
        PredicateOp::Exists => true,
    };
    PredicateEval {
        matched,
        available: true,
        note: None,
    }
}

fn cmp_f64(left: &Value, right: Option<&Value>, op: fn(f64, f64) -> bool) -> bool {
    match (as_f64(left), right.and_then(as_f64)) {
        (Some(a), Some(b)) => op(a, b),
        _ => false,
    }
}

fn predicate_needs_positioning(pred: &CatalogPredicate) -> bool {
    pred.field == FIELD_POSITIONING_LEVELS
        || pred
            .value
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|s| s.as_str())
            == Some(FIELD_POSITIONING_LEVELS)
}

fn eval_near(left: &Value, pred: &CatalogPredicate, positioning_levels: Option<&[f64]>) -> bool {
    let Some(price) = as_f64(left) else {
        return false;
    };
    let ticks = pred
        .tolerance_ticks
        .filter(|t| t.is_finite() && *t >= 0.0)
        .unwrap_or(DEFAULT_POSITIONING_NEAR_TICKS);
    let band = ticks * FUTURES_TICK_SIZE;
    if let Some(levels) = positioning_levels {
        if pred
            .value
            .as_ref()
            .and_then(|v| v.get("source"))
            .and_then(|s| s.as_str())
            == Some(FIELD_POSITIONING_LEVELS)
        {
            return levels.iter().any(|lvl| (price - *lvl).abs() <= band);
        }
    }
    pred.value
        .as_ref()
        .and_then(as_f64)
        .is_some_and(|target| (price - target).abs() <= band)
}

fn l0_meta_notes(session: Option<&str>) -> QueryResultMeta {
    let mut meta = QueryResultMeta::new(0, 0, 0);
    meta.scope = session.map(|s| s.to_string());
    meta
}

fn note_cold_store(meta: &mut QueryResultMeta, frames: &JournalFrameRead<'_>) {
    if frames.is_cold() {
        meta.notes.push(
            "served from cold session-partitioned Journal Frames (SQLite remains the hot window)"
                .into(),
        );
    }
}

/// Parse `store` from a `run_job` request body. Missing/empty → hot.
pub fn frame_store_kind_from_request(request: &Value) -> Result<FrameStoreKind, QueryKernelError> {
    let raw = request
        .get("store")
        .and_then(|v| v.as_str())
        .unwrap_or("hot");
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "hot" => Ok(FrameStoreKind::Hot),
        "cold" => Ok(FrameStoreKind::Cold),
        other => Err(QueryKernelError::UnknownFrameStore(other.to_string())),
    }
}

fn cold_root_from_request(request: &Value) -> PathBuf {
    request
        .get("coldRoot")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(default_cold_frames_dir)
}

fn tick_forward_return(
    db: &Database,
    entry_price: f64,
    start_ms: f64,
    root_symbol: &str,
    direction: OutcomeDirection,
) -> Result<Option<ForwardReturn>, QueryKernelError> {
    let end_ms = start_ms + QUERY_FORWARD_HORIZON_MS;
    let ticks = db.query_ticks_filtered_scoped(
        Some(start_ms),
        Some(end_ms),
        None,
        None,
        None,
        Some(root_symbol),
        None,
        QUERY_FORWARD_TICK_CAP.saturating_add(1),
    )?;
    if ticks.is_empty() {
        return Ok(None);
    }
    let truncated = ticks.len() > QUERY_FORWARD_TICK_CAP;
    let used = if truncated {
        &ticks[..QUERY_FORWARD_TICK_CAP]
    } else {
        ticks.as_slice()
    };
    let mut mfe = 0.0_f64;
    let mut mae = 0.0_f64;
    let mut last = entry_price;
    for tick in used {
        last = tick.price;
        let (fav, adv) = signed_excursion(direction, entry_price, tick.price);
        mfe = mfe.max(fav.max(0.0));
        mae = mae.max(adv.max(0.0));
    }
    let forward = match direction {
        OutcomeDirection::Long => last - entry_price,
        OutcomeDirection::Short => entry_price - last,
    };
    Ok(Some(ForwardReturn {
        mfe_points: mfe,
        mae_points: mae,
        forward_return_points: forward,
        tick_count: used.len(),
        direction: direction.as_str().to_string(),
        entry_price,
        horizon_ms: QUERY_FORWARD_HORIZON_MS,
        source: "raw_ticks".into(),
        truncated,
    }))
}

fn parse_direction(raw: Option<&str>) -> OutcomeDirection {
    raw.and_then(OutcomeDirection::parse)
        .unwrap_or(OutcomeDirection::Short)
}

/// Time-series read of catalog fields from 1 Hz Journal Frames (SQLite hot window).
pub fn query_series(
    db: &Database,
    req: &QuerySeriesRequest,
) -> Result<QuerySeriesResult, QueryKernelError> {
    query_series_with(JournalFrameRead::Hot(db), req)
}

/// Time-series read from a hot or cold Journal Frame store.
pub fn query_series_with(
    frames: JournalFrameRead<'_>,
    req: &QuerySeriesRequest,
) -> Result<QuerySeriesResult, QueryKernelError> {
    let (start, end) = validate_window(QueryKind::Series, req.window.start_ms, req.window.end_ms)?;
    let session = resolve_session_type(&frames, start, end, req.window.session_type.as_deref())?;
    let catalog = build_catalog();
    let mut fields = Vec::new();
    for id in &req.fields {
        fields.push(resolve_catalog_field(&catalog, id)?);
    }
    if fields.is_empty() {
        return Err(QueryKernelError::Invalid(
            "query_series requires at least one catalog field".into(),
        ));
    }

    let roots = req
        .window
        .symbols
        .clone()
        .unwrap_or_else(|| vec!["NQ".into(), "ES".into()]);
    let mut points = Vec::new();
    let mut scanned = 0usize;
    let mut truncated = false;
    let mut unavailable: BTreeSet<String> = BTreeSet::new();
    for root in &roots {
        let count =
            frames.count_in_window(start, end, Some(root.as_str()), session.as_deref())? as usize;
        scanned += count;
        let mut page_start = start;
        let mut root_emitted = 0usize;
        while page_start <= end && points.len() < QUERY_SERIES_MAX_POINTS {
            let page_end = (page_start + QUERY_PAGE_MS).min(end);
            let remain = QUERY_SERIES_MAX_POINTS.saturating_sub(points.len());
            let rows = frames.list_in_window(
                page_start,
                page_end,
                Some(root.as_str()),
                session.as_deref(),
                remain,
            )?;
            root_emitted += rows.len();
            for frame in rows {
                let mut values = BTreeMap::new();
                for field in &fields {
                    match extract_field_value(&frame.payload, field, None) {
                        Some(v) => {
                            values.insert(field.id.clone(), v);
                        }
                        None => {
                            unavailable.insert(field.id.clone());
                        }
                    }
                }
                points.push(SeriesPoint {
                    clock_ms: frame.clock_ms,
                    frame_second: frame.frame_second,
                    root_symbol: frame.root_symbol,
                    session_type: frame.session_type,
                    trading_day: frame.trading_day,
                    values,
                });
                if points.len() >= QUERY_SERIES_MAX_POINTS {
                    truncated = true;
                    break;
                }
            }
            if page_end >= end {
                break;
            }
            page_start = page_end + 1.0;
        }
        if root_emitted < count {
            truncated = true;
        }
    }
    points.sort_by(|a, b| {
        a.clock_ms
            .partial_cmp(&b.clock_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.root_symbol.cmp(&b.root_symbol))
    });
    if points.len() > QUERY_SERIES_MAX_POINTS {
        points.truncate(QUERY_SERIES_MAX_POINTS);
        truncated = true;
    }
    let n = points.len();
    let mut meta = QueryResultMeta::new(n, scanned, scanned);
    meta.scope = session;
    meta.unavailable_fields = unavailable.into_iter().collect();
    if !meta.unavailable_fields.is_empty() {
        meta.degraded = true;
        meta.notes.push(
            "one or more requested catalog fields were absent on scanned Journal Frames".into(),
        );
    }
    if truncated {
        meta.mark_truncated(format!(
            "query_series truncated at {QUERY_SERIES_MAX_POINTS} points; use run_job for the artifact handle"
        ));
    }
    note_cold_store(&mut meta, &frames);
    Ok(QuerySeriesResult {
        kind: QueryKind::Series,
        points,
        meta,
    })
}

/// Conjunctive Episode Query over catalog fields with tick-driven forward returns.
pub fn query_episodes(
    db: &Database,
    req: &QueryEpisodesRequest,
) -> Result<QueryEpisodesResult, QueryKernelError> {
    query_episodes_with(db, JournalFrameRead::Hot(db), req)
}

/// Episode Query with an explicit hot/cold Journal Frame store.
pub fn query_episodes_with(
    db: &Database,
    frame_src: JournalFrameRead<'_>,
    req: &QueryEpisodesRequest,
) -> Result<QueryEpisodesResult, QueryKernelError> {
    query_episodes_with_opts(db, frame_src, req, QueryEpisodesOpts::default())
}

/// Episode Query with explicit store + window-cap policy (SIL-M3e SLO scan).
pub fn query_episodes_with_opts(
    db: &Database,
    frame_src: JournalFrameRead<'_>,
    req: &QueryEpisodesRequest,
    opts: QueryEpisodesOpts,
) -> Result<QueryEpisodesResult, QueryKernelError> {
    let (start, end) = validate_window_ex(
        QueryKind::Episodes,
        req.window.start_ms,
        req.window.end_ms,
        opts.enforce_product_window_cap,
    )?;
    if req.predicates.is_empty() {
        return Err(QueryKernelError::Invalid(
            "query_episodes requires at least one catalog predicate".into(),
        ));
    }
    for pred in &req.predicates {
        let event_pred = pred
            .event_type
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty());
        if !event_pred
            && pred
                .symbol
                .as_deref()
                .map(str::trim)
                .is_none_or(|s| s.is_empty())
        {
            return Err(QueryKernelError::Invalid(
                "query_episodes catalog predicates require `symbol` (NQ or ES); a missing symbol is not defaulted per-second".into(),
            ));
        }
    }
    let session = resolve_session_type(&frame_src, start, end, req.window.session_type.as_deref())?;
    let catalog = build_catalog();
    let mut resolved_fields: Vec<Option<&FieldDescriptor>> = Vec::new();
    for pred in &req.predicates {
        if pred
            .event_type
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
        {
            resolved_fields.push(None);
            continue;
        }
        resolved_fields.push(Some(resolve_catalog_field(&catalog, &pred.field)?));
    }

    let root_filters: Vec<Option<String>> = match &req.window.symbols {
        Some(syms) if !syms.is_empty() => syms.iter().cloned().map(Some).collect(),
        _ => vec![None],
    };
    let mut population = 0usize;
    for root in &root_filters {
        population +=
            frame_src.count_in_window(start, end, root.as_deref(), session.as_deref())? as usize;
    }
    let needs_events = req.predicates.iter().any(|p| {
        p.event_type
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
    });
    let needs_positioning = req.predicates.iter().any(predicate_needs_positioning);
    let positioning_records = if needs_positioning {
        db.list_positioning_records_covering_window(start, end)?
    } else {
        Vec::new()
    };

    let mut coverage: Vec<PredicateCoverage> = req
        .predicates
        .iter()
        .map(|p| PredicateCoverage {
            id: p.id.clone().unwrap_or_else(|| p.field.clone()),
            field: p.field.clone(),
            available_seconds: 0,
            missing_seconds: 0,
            note: None,
        })
        .collect();

    let direction = parse_direction(req.forward_direction.as_deref());
    let mut matches = Vec::new();
    let mut unavailable: BTreeSet<String> = BTreeSet::new();
    let mut any_missing_ticks = false;
    let mut any_missing_positioning = false;
    let mut any_stale_positioning = false;
    let mut any_partial_forward = false;
    let mut any_forward_capped = false;
    let mut forwards_computed = 0usize;
    let mut truncated = false;
    let mut scanned_seconds = 0usize;
    let mut scanned_frames = 0usize;
    let mut page_start = start;

    while page_start <= end
        && matches.len() < QUERY_EPISODES_MAX_MATCHES
        && scanned_frames < QUERY_EPISODES_MAX_FRAMES
    {
        let page_end = (page_start + QUERY_PAGE_MS).min(end);
        let remain = QUERY_EPISODES_MAX_FRAMES.saturating_sub(scanned_frames);
        let mut frames = Vec::new();
        for root in &root_filters {
            let remaining = remain.saturating_sub(frames.len());
            if remaining == 0 {
                break;
            }
            frames.extend(frame_src.list_in_window(
                page_start,
                page_end,
                root.as_deref(),
                session.as_deref(),
                remaining,
            )?);
        }
        scanned_frames += frames.len();
        if scanned_frames >= QUERY_EPISODES_MAX_FRAMES && page_end < end {
            truncated = true;
        }

        let mut by_second: BTreeMap<i64, BTreeMap<String, JournalFrameRecord>> = BTreeMap::new();
        for frame in frames {
            by_second
                .entry(frame.frame_second)
                .or_default()
                .insert(frame.root_symbol.clone(), frame);
        }
        scanned_seconds += by_second.len();

        let mut events_by_second: BTreeMap<i64, Vec<Value>> = BTreeMap::new();
        if needs_events {
            let event_cap = remain.saturating_add(1).min(10_000);
            let mut events = db.list_market_events_in_window(
                page_start,
                page_end,
                None,
                None,
                session.as_deref(),
                event_cap,
            )?;
            if events.len() >= event_cap {
                truncated = true;
                events.truncate(event_cap.saturating_sub(1).max(1));
            }
            for evt in events {
                if !event_matches_session(&evt, session.as_deref()) {
                    continue;
                }
                let second = evt
                    .get("journalFrameSecond")
                    .and_then(|v| v.as_i64())
                    .or_else(|| {
                        evt.get("timestampMs")
                            .and_then(|v| v.as_f64())
                            .and_then(journal_frame_second_from_ts)
                    });
                if let Some(second) = second {
                    events_by_second.entry(second).or_default().push(evt);
                }
            }
        }

        for (second, roots) in &by_second {
            let refs: BTreeMap<String, &JournalFrameRecord> =
                roots.iter().map(|(k, v)| (k.clone(), v)).collect();
            let clock = roots.values().map(|f| f.clock_ms).fold(0.0_f64, f64::max);
            let selected = if needs_positioning {
                select_positioning(&positioning_records, clock)
            } else {
                None
            };
            let levels_owned = selected.map(positioning_level_prices);
            let levels = levels_owned.as_deref();
            if needs_positioning && selected.is_none() {
                any_missing_positioning = true;
            }
            if selected.is_some_and(|r| positioning_is_stale(r, clock)) {
                any_stale_positioning = true;
            }
            let second_events = events_by_second.get(second).cloned().unwrap_or_default();

            let mut all_match = true;
            for (i, pred) in req.predicates.iter().enumerate() {
                let eval = eval_predicate(pred, resolved_fields[i], &refs, &second_events, levels);
                if eval.available {
                    coverage[i].available_seconds += 1;
                } else {
                    coverage[i].missing_seconds += 1;
                    coverage[i].note = eval.note.clone();
                    unavailable.insert(pred.field.clone());
                    all_match = false;
                }
                if !eval.matched {
                    all_match = false;
                }
            }
            if !all_match {
                continue;
            }

            let sample = roots.values().next();
            let mut values = BTreeMap::new();
            for (root, frame) in roots {
                if let Some(v) = frame.payload.get("lastPrice") {
                    values.insert(format!("{root}.{FIELD_LAST_PRICE}"), v.clone());
                }
                if let Some(v) = frame.payload.get("sessionDelta") {
                    values.insert(format!("{root}.{FIELD_SESSION_DELTA}"), v.clone());
                }
            }

            let es_entry = roots
                .get("ES")
                .and_then(|f| f.payload.get("lastPrice"))
                .and_then(as_f64)
                .or_else(|| {
                    roots
                        .values()
                        .next()
                        .and_then(|f| f.payload.get("lastPrice"))
                        .and_then(as_f64)
                });
            let forward = if let Some(entry) = es_entry {
                if forwards_computed >= QUERY_FORWARD_MAX_MATCHES {
                    any_forward_capped = true;
                    None
                } else {
                    let root = if roots.contains_key("ES") {
                        "ES"
                    } else {
                        roots.keys().next().map(String::as_str).unwrap_or("NQ")
                    };
                    forwards_computed += 1;
                    match tick_forward_return(db, entry, clock, root, direction)? {
                        Some(fwd) => {
                            if fwd.truncated {
                                any_partial_forward = true;
                            }
                            Some(fwd)
                        }
                        None => {
                            any_missing_ticks = true;
                            None
                        }
                    }
                }
            } else {
                any_missing_ticks = true;
                None
            };

            let frame_ref = FrameRef {
                journal_frame_second: Some(*second),
                root_symbol: Some(if roots.contains_key("ES") {
                    "ES".into()
                } else {
                    roots.keys().next().cloned().unwrap_or_else(|| "NQ".into())
                }),
            };
            matches.push(EpisodeMatch {
                clock_ms: clock,
                frame_ref,
                session_type: sample
                    .map(|f| f.session_type.clone())
                    .unwrap_or_else(|| "Unknown".into()),
                trading_day: sample.map(|f| f.trading_day.clone()).unwrap_or_default(),
                values,
                forward,
                journal_backed: true,
                positioning_id: selected.map(|r| r.id.clone()),
                positioning_as_of_ms: selected.map(|r| r.as_of_ms),
            });
            if matches.len() >= QUERY_EPISODES_MAX_MATCHES {
                truncated = true;
                break;
            }
        }

        if page_end >= end {
            break;
        }
        page_start = page_end + 1.0;
    }

    if scanned_frames < population && population > QUERY_EPISODES_MAX_FRAMES {
        truncated = true;
    }

    let n = matches.len();
    let mut meta = QueryResultMeta::new(n, population, scanned_seconds);
    meta.scope = session;
    meta.unavailable_fields = unavailable.into_iter().collect();
    if coverage.iter().any(|c| c.missing_seconds > 0) || any_missing_positioning {
        meta.degraded = true;
        meta.notes.push(
            "one or more predicates were unavailable on scanned seconds (fail closed; no invented detector math)"
                .into(),
        );
    }
    if any_stale_positioning {
        meta.degraded = true;
        meta.notes.push(format!(
            "Positioning as-of is older than {POSITIONING_STALE_MS} ms before the episode clock (Levels-Only Record still applied; not live vendor data)"
        ));
    }
    if any_missing_ticks {
        meta.degraded = true;
        meta.notes.push(
            "tick-driven MFE/MAE unavailable for some matches (no raw_ticks in the forward horizon; no fill simulator)"
                .into(),
        );
    }
    if any_partial_forward {
        meta.degraded = true;
        meta.notes.push(format!(
            "tick-driven MFE/MAE truncated at {QUERY_FORWARD_TICK_CAP} ticks; forward horizon is partial"
        ));
    }
    if any_forward_capped {
        meta.degraded = true;
        meta.notes.push(format!(
            "tick-driven MFE/MAE computed for the first {QUERY_FORWARD_MAX_MATCHES} matches (named cap); later matches have forward=null — not a fill simulator"
        ));
    }
    if matches.iter().any(|m| m.forward.is_none()) {
        meta.degraded = true;
    }
    if truncated {
        meta.mark_truncated(format!(
            "query_episodes truncated at {QUERY_EPISODES_MAX_MATCHES} matches or {QUERY_EPISODES_MAX_FRAMES} frames"
        ));
    }
    note_cold_store(&mut meta, &frame_src);
    Ok(QueryEpisodesResult {
        kind: QueryKind::Episodes,
        matches,
        predicate_coverage: coverage,
        meta,
    })
}

/// Hard-capped raw read of journal frames, events, or ticks (SQLite hot window).
pub fn query_raw(db: &Database, req: &QueryRawRequest) -> Result<QueryRawResult, QueryKernelError> {
    query_raw_with(db, JournalFrameRead::Hot(db), req)
}

/// Raw read with an explicit hot/cold Journal Frame store.
pub fn query_raw_with(
    db: &Database,
    frame_src: JournalFrameRead<'_>,
    req: &QueryRawRequest,
) -> Result<QueryRawResult, QueryKernelError> {
    let (start, end) = validate_window(QueryKind::Raw, req.window.start_ms, req.window.end_ms)?;
    let session = resolve_session_type(&frame_src, start, end, req.window.session_type.as_deref())?;
    let source = if req.source.trim().is_empty() {
        "journal_frames"
    } else {
        req.source.trim()
    };
    if frame_src.is_cold() && source != "journal_frames" {
        return Err(QueryKernelError::ColdStoreUnsupportedSource(
            source.to_string(),
        ));
    }
    let cap = req
        .limit
        .unwrap_or(QUERY_RAW_MAX_ROWS)
        .min(QUERY_RAW_MAX_ROWS);
    if cap == 0 {
        return Err(QueryKernelError::Invalid(
            "query_raw limit must be at least 1".into(),
        ));
    }
    let roots: Vec<Option<String>> = match &req.window.symbols {
        Some(syms) if !syms.is_empty() => syms.iter().cloned().map(Some).collect(),
        _ => vec![None],
    };

    let mut rows = Vec::new();
    let mut population = 0usize;
    let mut truncated = false;
    match source {
        "journal_frames" => {
            for root in &roots {
                let root_ref = root.as_deref();
                population +=
                    frame_src.count_in_window(start, end, root_ref, session.as_deref())? as usize;
                let frames = frame_src.list_in_window(
                    start,
                    end,
                    root_ref,
                    session.as_deref(),
                    cap.saturating_add(1),
                )?;
                for f in frames {
                    rows.push(RawRow {
                        timestamp_ms: f.clock_ms,
                        root_symbol: Some(f.root_symbol),
                        payload: f.payload,
                    });
                }
            }
        }
        "events" => {
            for root in &roots {
                let fetch = cap.saturating_add(1);
                let events = db.list_market_events_in_window(
                    start,
                    end,
                    root.as_deref(),
                    None,
                    session.as_deref(),
                    fetch,
                )?;
                if events.len() > cap {
                    truncated = true;
                }
                for evt in events {
                    if !event_matches_session(&evt, session.as_deref()) {
                        continue;
                    }
                    rows.push(RawRow {
                        timestamp_ms: evt
                            .get("timestampMs")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0),
                        root_symbol: evt
                            .get("rootSymbol")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        payload: evt,
                    });
                }
            }
            population = rows.len();
        }
        "ticks" => {
            for root in &roots {
                let Some((tick_start, tick_end)) =
                    clamp_window_to_session(start, end, session.as_deref())
                else {
                    continue;
                };
                let fetch = cap.saturating_add(1);
                let ticks = db.query_ticks_filtered_scoped(
                    Some(tick_start),
                    Some(tick_end),
                    None,
                    None,
                    None,
                    root.as_deref(),
                    None,
                    fetch,
                )?;
                if ticks.len() > cap {
                    truncated = true;
                }
                for t in ticks {
                    if let Some(want) = session.as_deref() {
                        if session_label_for_timestamp(t.timestamp_ms) != Some(want) {
                            continue;
                        }
                    }
                    rows.push(RawRow {
                        timestamp_ms: t.timestamp_ms,
                        root_symbol: t.root_symbol,
                        payload: serde_json::json!({
                            "price": t.price,
                            "volume": t.volume,
                            "bid": t.bid,
                            "ask": t.ask,
                            "isBuy": t.is_buy,
                            "sessionDate": t.session_date,
                        }),
                    });
                }
            }
            population = rows.len();
        }
        other => return Err(QueryKernelError::UnknownRawSource(other.to_string())),
    }

    rows.sort_by(|a, b| {
        a.timestamp_ms
            .partial_cmp(&b.timestamp_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.root_symbol.cmp(&b.root_symbol))
    });
    if rows.len() > cap {
        rows.truncate(cap);
        truncated = true;
    }
    if source == "journal_frames" && rows.len() < population {
        truncated = true;
    }

    let n = rows.len();
    let mut meta = QueryResultMeta::new(n, population, n);
    meta.scope = session.clone();
    if truncated {
        meta.mark_truncated(format!(
            "query_raw truncated at {cap} rows (hard cap {QUERY_RAW_MAX_ROWS}); use run_job for the artifact handle"
        ));
    }
    note_cold_store(&mut meta, &frame_src);
    Ok(QueryRawResult {
        kind: QueryKind::Raw,
        source: source.to_string(),
        rows,
        meta,
    })
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn write_csv(path: &Path, header: &[&str], rows: &[Vec<String>]) -> Result<(), QueryKernelError> {
    let mut out = String::new();
    out.push_str(
        &header
            .iter()
            .copied()
            .map(csv_escape)
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');
    for row in rows {
        out.push_str(
            &row.iter()
                .map(|c| csv_escape(c))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    fs::write(path, out)?;
    Ok(())
}

fn json_cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Compute a research job's artifacts (read-only against SQLite).
///
/// Does not upsert the job row — the MCP path inserts `queued` on the writer,
/// runs this on the read pool, then persists `completed`/`error`.
pub fn execute_research_job(
    db: &Database,
    kind: QueryKind,
    request: &Value,
    artifact_dir: &Path,
    _now_ms: f64,
    job_id: &str,
) -> Result<RunJobResult, QueryKernelError> {
    fs::create_dir_all(artifact_dir)?;
    let job_dir = artifact_dir.join(job_id);
    fs::create_dir_all(&job_dir)?;
    let columnar_path = job_dir.join("columns.csv");
    let summary_path = job_dir.join("summary.json");

    let store_kind = frame_store_kind_from_request(request)?;
    let cold_store = if store_kind == FrameStoreKind::Cold {
        Some(ColdFrameStore::new(cold_root_from_request(request)))
    } else {
        None
    };
    let frame_read = match &cold_store {
        Some(store) => JournalFrameRead::Cold(store),
        None => JournalFrameRead::Hot(db),
    };

    let (meta, summary, csv_header, csv_rows) = match kind {
        QueryKind::Series => {
            let req: QuerySeriesRequest = serde_json::from_value(request.clone())
                .map_err(|e| QueryKernelError::Invalid(e.to_string()))?;
            let result = query_series_with(frame_read, &req)?;
            let header = vec![
                "clockMs",
                "frameSecond",
                "rootSymbol",
                "sessionType",
                "tradingDay",
                "valuesJson",
            ];
            let rows: Vec<Vec<String>> = result
                .points
                .iter()
                .map(|p| {
                    vec![
                        p.clock_ms.to_string(),
                        p.frame_second.to_string(),
                        p.root_symbol.clone(),
                        p.session_type.clone(),
                        p.trading_day.clone(),
                        serde_json::to_string(&p.values).unwrap_or_else(|_| "{}".into()),
                    ]
                })
                .collect();
            let summary = serde_json::json!({
                "kind": "series",
                "pointCount": result.points.len(),
                "n": result.meta.n,
                "reliabilityTier": result.meta.reliability_tier,
                "degraded": result.meta.degraded,
                "truncated": result.meta.truncated,
                "notes": result.meta.notes,
            });
            (result.meta, summary, header, rows)
        }
        QueryKind::Episodes => {
            let req: QueryEpisodesRequest = serde_json::from_value(request.clone())
                .map_err(|e| QueryKernelError::Invalid(e.to_string()))?;
            let result = query_episodes_with(db, frame_read, &req)?;
            let header = vec![
                "clockMs",
                "frameSecond",
                "rootSymbol",
                "sessionType",
                "tradingDay",
                "mfePoints",
                "maePoints",
                "forwardReturnPoints",
                "tickCount",
                "valuesJson",
            ];
            let rows: Vec<Vec<String>> = result
                .matches
                .iter()
                .map(|m| {
                    let fwd = m.forward.as_ref();
                    vec![
                        m.clock_ms.to_string(),
                        m.frame_ref
                            .journal_frame_second
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                        m.frame_ref.root_symbol.clone().unwrap_or_default(),
                        m.session_type.clone(),
                        m.trading_day.clone(),
                        fwd.map(|f| f.mfe_points.to_string()).unwrap_or_default(),
                        fwd.map(|f| f.mae_points.to_string()).unwrap_or_default(),
                        fwd.map(|f| f.forward_return_points.to_string())
                            .unwrap_or_default(),
                        fwd.map(|f| f.tick_count.to_string()).unwrap_or_default(),
                        serde_json::to_string(&m.values).unwrap_or_else(|_| "{}".into()),
                    ]
                })
                .collect();
            let summary = serde_json::json!({
                "kind": "episodes",
                "matchCount": result.matches.len(),
                "n": result.meta.n,
                "reliabilityTier": result.meta.reliability_tier,
                "degraded": result.meta.degraded,
                "truncated": result.meta.truncated,
                "predicateCoverage": result.predicate_coverage,
                "notes": result.meta.notes,
            });
            (result.meta, summary, header, rows)
        }
        QueryKind::Raw => {
            let req: QueryRawRequest = serde_json::from_value(request.clone())
                .map_err(|e| QueryKernelError::Invalid(e.to_string()))?;
            let result = query_raw_with(db, frame_read, &req)?;
            let header = vec!["timestampMs", "rootSymbol", "payloadJson"];
            let rows: Vec<Vec<String>> = result
                .rows
                .iter()
                .map(|r| {
                    vec![
                        r.timestamp_ms.to_string(),
                        r.root_symbol.clone().unwrap_or_default(),
                        json_cell(&r.payload),
                    ]
                })
                .collect();
            let summary = serde_json::json!({
                "kind": "raw",
                "source": result.source,
                "rowCount": result.rows.len(),
                "n": result.meta.n,
                "reliabilityTier": result.meta.reliability_tier,
                "degraded": result.meta.degraded,
                "truncated": result.meta.truncated,
                "notes": result.meta.notes,
            });
            (result.meta, summary, header, rows)
        }
    };

    write_csv(&columnar_path, &csv_header, &csv_rows)?;
    let summary_doc = serde_json::json!({
        "jobId": job_id,
        "kind": kind.as_str(),
        "meta": meta,
        "summary": summary,
        "columnarPath": columnar_path.to_string_lossy(),
        "trustLevel": TrustLevel::L0,
        "mutationAuthority": false,
        "orderAuthority": false,
    });
    fs::write(
        &summary_path,
        serde_json::to_vec_pretty(&summary_doc).unwrap_or_default(),
    )?;

    Ok(RunJobResult {
        job_id: job_id.to_string(),
        status: "completed".into(),
        kind,
        artifact: ArtifactHandle {
            columnar_path: columnar_path.to_string_lossy().to_string(),
            summary_path: summary_path.to_string_lossy().to_string(),
            summary,
        },
        meta,
        trust_level: TrustLevel::L0,
        mutation_authority: false,
        order_authority: false,
    })
}

/// Queued job row written on the writer before compute (MCP `run_job`).
pub fn queued_research_job_record(
    job_id: &str,
    kind: QueryKind,
    request: &Value,
    now_ms: f64,
) -> ResearchQueryJobRecord {
    let (start, end) = window_from_request(request);
    ResearchQueryJobRecord {
        id: job_id.to_string(),
        kind: kind.as_str().to_string(),
        status: "queued".into(),
        submitted_at_ms: now_ms,
        started_at_ms: None,
        finished_at_ms: None,
        window_start_ms: start,
        window_end_ms: end,
        request: request.clone(),
        summary: None,
        artifact_dir: None,
        columnar_path: None,
        summary_path: None,
        sample_size: None,
        reliability_tier: None,
        degraded: false,
        error: None,
    }
}

/// Persist a completed or failed research query job on the writer connection.
pub fn persist_research_job(
    db: &Database,
    result: &RunJobResult,
    request: &Value,
    now_ms: f64,
    error: Option<String>,
) -> Result<(), QueryKernelError> {
    let (start, end) = window_from_request(request);
    let status = if error.is_some() {
        "error"
    } else {
        result.status.as_str()
    };
    let record = ResearchQueryJobRecord {
        id: result.job_id.clone(),
        kind: result.kind.as_str().to_string(),
        status: status.into(),
        submitted_at_ms: now_ms,
        started_at_ms: Some(now_ms),
        finished_at_ms: Some(now_ms),
        window_start_ms: start,
        window_end_ms: end,
        request: request.clone(),
        summary: Some(result.artifact.summary.clone()),
        artifact_dir: Path::new(&result.artifact.columnar_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string()),
        columnar_path: Some(result.artifact.columnar_path.clone()),
        summary_path: Some(result.artifact.summary_path.clone()),
        sample_size: Some(result.meta.sample_size as i64),
        reliability_tier: Some(format!("{:?}", result.meta.reliability_tier).to_ascii_lowercase()),
        degraded: result.meta.degraded,
        error,
    };
    db.upsert_research_query_job(&record)?;
    Ok(())
}

fn window_from_request(request: &Value) -> (Option<f64>, Option<f64>) {
    (
        request.get("startMs").and_then(|v| v.as_f64()),
        request.get("endMs").and_then(|v| v.as_f64()),
    )
}

/// Allocate a research query job id (`rq-<uuid>`).
pub fn new_research_job_id() -> String {
    format!("rq-{}", Uuid::new_v4())
}

/// Empty handle used when persist-on-error has no artifact.
pub fn failed_research_job_result(job_id: String, kind: QueryKind) -> RunJobResult {
    let mut meta = QueryResultMeta::new(0, 0, 0);
    meta.degraded = true;
    RunJobResult {
        job_id,
        status: "error".into(),
        kind,
        artifact: ArtifactHandle {
            columnar_path: String::new(),
            summary_path: String::new(),
            summary: serde_json::json!({}),
        },
        meta,
        trust_level: TrustLevel::L0,
        mutation_authority: false,
        order_authority: false,
    }
}

/// Run a query, write artifacts, and persist the job row (writable DB, tests / CLI).
///
/// The MCP tool inserts `queued` first, computes on the read pool via
/// [`execute_research_job`], then persists `completed` so the returned handle
/// is populated. A separate poll operator is out of scope for M3c.
pub fn run_job(
    db: &Database,
    kind: QueryKind,
    request: &Value,
    artifact_dir: &Path,
    now_ms: f64,
) -> Result<RunJobResult, QueryKernelError> {
    let job_id = new_research_job_id();
    db.upsert_research_query_job(&queued_research_job_record(&job_id, kind, request, now_ms))?;
    match execute_research_job(db, kind, request, artifact_dir, now_ms, &job_id) {
        Ok(result) => {
            persist_research_job(db, &result, request, now_ms, None)?;
            Ok(result)
        }
        Err(e) => {
            let failed = failed_research_job_result(job_id, kind);
            let _ = persist_research_job(db, &failed, request, now_ms, Some(e.to_string()));
            Err(e)
        }
    }
}

/// Cite Journal Frames for hypothesis / research fires (`frame_ref` join).
///
/// Rootless outcome rows are not attributed to NQ. Returns at most
/// [`HYPOTHESIS_EVIDENCE_WINDOW_CAP`] windows; the bool is `true` when the
/// input was truncated.
pub fn journal_backed_evidence_windows(
    db: &Database,
    rows: &[HypothesisSignalOutcomeRow],
) -> Result<(Vec<JournalBackedEvidenceWindow>, bool), QueryKernelError> {
    let truncated = rows.len() > HYPOTHESIS_EVIDENCE_WINDOW_CAP;
    let slice = if truncated {
        &rows[..HYPOTHESIS_EVIDENCE_WINDOW_CAP]
    } else {
        rows
    };
    if slice.is_empty() {
        return Ok((Vec::new(), truncated));
    }

    let mut seconds_by_root: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for row in slice {
        let Some(root) = row
            .root_symbol
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let Some(second) = journal_frame_second_from_ts(row.fired_at_ms) else {
            continue;
        };
        seconds_by_root
            .entry(root.to_string())
            .or_default()
            .push(second);
    }

    let mut meta_by_key: BTreeMap<(String, i64), (String, String)> = BTreeMap::new();
    for (root, seconds) in &seconds_by_root {
        for frame in db.list_journal_frames_at_seconds(root, seconds)? {
            meta_by_key.insert(
                (frame.root_symbol.clone(), frame.frame_second),
                (frame.trading_day.clone(), frame.session_type.clone()),
            );
        }
    }

    let mut out = Vec::with_capacity(slice.len());
    for row in slice {
        let root = row
            .root_symbol
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let second = journal_frame_second_from_ts(row.fired_at_ms);
        let Some(root) = root else {
            out.push(JournalBackedEvidenceWindow {
                fired_at_ms: row.fired_at_ms,
                frame_ref: FrameRef {
                    journal_frame_second: second,
                    root_symbol: None,
                },
                journal_backed: false,
                trading_day: None,
                session_type: None,
            });
            continue;
        };
        let Some(second) = second else {
            out.push(JournalBackedEvidenceWindow {
                fired_at_ms: row.fired_at_ms,
                frame_ref: FrameRef {
                    journal_frame_second: None,
                    root_symbol: Some(root.to_string()),
                },
                journal_backed: false,
                trading_day: None,
                session_type: None,
            });
            continue;
        };
        let meta = meta_by_key.get(&(root.to_string(), second));
        out.push(JournalBackedEvidenceWindow {
            fired_at_ms: row.fired_at_ms,
            frame_ref: FrameRef {
                journal_frame_second: Some(second),
                root_symbol: Some(root.to_string()),
            },
            journal_backed: meta.is_some(),
            trading_day: meta.map(|(day, _)| day.clone()),
            session_type: meta.map(|(_, session)| session.clone()),
        });
    }
    Ok((out, truncated))
}

/// L0 envelope fields shared by MCP tool responses.
pub fn l0_capability_fields() -> Value {
    serde_json::json!({
        "trustLevel": TrustLevel::L0,
        "mutationAuthority": false,
        "orderAuthority": false,
        "trustCeiling": TrustCeiling::L3,
    })
}

/// Empty meta used when a query is expressible but no rows matched.
pub fn empty_degraded_meta(note: &str) -> QueryResultMeta {
    let mut meta = l0_meta_notes(None);
    meta.degraded = true;
    meta.journal_backed = true;
    meta.notes.push(note.to_string());
    meta
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        accept_levels_only_entry, DerivedLevels, PositioningEntryInput, PositioningWall,
        LEVELS_ONLY_RECORD_KIND,
    };
    use crate::db::JournalFrameRecord;
    use crate::pipelines::event_detector::MarketEvent;
    use tempfile::{NamedTempFile, TempDir};

    const FLAGSHIP_CLOCK_MS: f64 = 1_704_207_600_000.0;
    const FLAGSHIP_TRADING_DAY: &str = "2024-01-02";
    const FLAGSHIP_ES_PRICE: f64 = 5_750.0;
    const FLAGSHIP_NQ_PRICE: f64 = 20_000.0;
    const FLAGSHIP_ES_DELTA: f64 = -500.0;
    const FLAGSHIP_NQ_DELTA: f64 = 50.0;
    const FLAGSHIP_MATCHING_SECONDS: usize = 1;
    const FLAGSHIP_NON_MATCHING_SECONDS: usize = 1;
    const FLAGSHIP_MFE: f64 = 15.0;
    const FLAGSHIP_MAE: f64 = 2.0;
    const FLAGSHIP_FORWARD: f64 = 15.0;

    fn test_db() -> Database {
        let file = NamedTempFile::new().expect("temp db");
        Database::open(file.path().to_string_lossy().as_ref()).expect("open db")
    }

    fn frame(clock_ms: f64, root: &str, session: &str, payload: Value) -> JournalFrameRecord {
        JournalFrameRecord {
            clock_ms,
            frame_second: journal_frame_second_from_ts(clock_ms).expect("second"),
            root_symbol: root.into(),
            session_type: session.into(),
            session_segment: "None".into(),
            trading_day: FLAGSHIP_TRADING_DAY.into(),
            payload,
        }
    }

    fn matching_es_payload() -> Value {
        serde_json::json!({
            "lastPrice": FLAGSHIP_ES_PRICE,
            "sessionDelta": FLAGSHIP_ES_DELTA,
            "poorLow": true,
            "poorHigh": false,
            "rootSymbol": "ES",
            "sessionType": "RTH",
            "tradingDay": FLAGSHIP_TRADING_DAY,
            "domSummary": { "bidReplenishing": true },
        })
    }

    fn matching_nq_payload() -> Value {
        serde_json::json!({
            "lastPrice": FLAGSHIP_NQ_PRICE,
            "sessionDelta": FLAGSHIP_NQ_DELTA,
            "rootSymbol": "NQ",
            "sessionType": "RTH",
            "tradingDay": FLAGSHIP_TRADING_DAY,
        })
    }

    fn seed_positioning(db: &Database, as_of_ms: f64) {
        let levels = DerivedLevels {
            flip: FLAGSHIP_ES_PRICE,
            walls: vec![PositioningWall {
                strike: 5_800.0,
                role: "call_wall".into(),
            }],
            balance: 5_745.0,
            upside_test: 5_825.0,
            downside_test: 5_680.0,
        };
        let record = accept_levels_only_entry(PositioningEntryInput {
            id: Some("pos-flagship".into()),
            record_kind: Some(LEVELS_ONLY_RECORD_KIND.into()),
            completeness: Some(LEVELS_ONLY_RECORD_KIND.into()),
            trading_day: None,
            captured_at_ms: Some(as_of_ms),
            as_of_ms: Some(as_of_ms),
            derived_levels: Some(levels),
            now_ms: as_of_ms,
            ..Default::default()
        })
        .expect("levels-only");
        db.upsert_positioning_record(&record, as_of_ms)
            .expect("upsert positioning");
    }

    fn seed_flagship_frames(db: &Database) {
        let match_clock = FLAGSHIP_CLOCK_MS;
        let miss_clock = FLAGSHIP_CLOCK_MS + 1_000.0;
        let globex_clock = FLAGSHIP_CLOCK_MS + 50_000.0;
        db.insert_journal_frames(&[
            frame(match_clock, "ES", "RTH", matching_es_payload()),
            frame(match_clock, "NQ", "RTH", matching_nq_payload()),
            frame(
                miss_clock,
                "ES",
                "RTH",
                serde_json::json!({
                    "lastPrice": 5_900.0,
                    "sessionDelta": FLAGSHIP_ES_DELTA,
                    "poorLow": true,
                    "rootSymbol": "ES",
                    "sessionType": "RTH",
                    "domSummary": { "bidReplenishing": true },
                }),
            ),
            frame(miss_clock, "NQ", "RTH", matching_nq_payload()),
            frame(globex_clock, "ES", "Globex", matching_es_payload()),
            frame(globex_clock, "NQ", "Globex", matching_nq_payload()),
        ])
        .expect("frames");
        seed_positioning(db, match_clock);
        db.insert_raw_tick_with_contract(
            match_clock + 1_000.0,
            FLAGSHIP_ES_PRICE - 10.0,
            1.0,
            FLAGSHIP_ES_PRICE - 10.25,
            FLAGSHIP_ES_PRICE - 9.75,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick fav");
        db.insert_raw_tick_with_contract(
            match_clock + 2_000.0,
            FLAGSHIP_ES_PRICE + 2.0,
            1.0,
            FLAGSHIP_ES_PRICE + 1.75,
            FLAGSHIP_ES_PRICE + 2.25,
            true,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick adv");
        db.insert_raw_tick_with_contract(
            match_clock + 3_000.0,
            FLAGSHIP_ES_PRICE - FLAGSHIP_MFE,
            1.0,
            FLAGSHIP_ES_PRICE - FLAGSHIP_MFE - 0.25,
            FLAGSHIP_ES_PRICE - FLAGSHIP_MFE + 0.25,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick mfe");
    }

    fn flagship_request(start: f64, end: f64, session: &str) -> QueryEpisodesRequest {
        QueryEpisodesRequest {
            window: QueryWindow {
                start_ms: Some(start),
                end_ms: Some(end),
                session_type: Some(session.into()),
                symbols: Some(vec!["NQ".into(), "ES".into()]),
            },
            predicates: flagship_episode_predicates(),
            forward_direction: Some("short".into()),
        }
    }

    #[test]
    fn flagship_predicates_are_five_catalog_fields() {
        let preds = flagship_episode_predicates();
        assert_eq!(preds.len(), 5);
        let catalog = build_catalog();
        for pred in &preds {
            assert!(
                catalog.fields.iter().any(|f| f.id == pred.field),
                "flagship field {} must be in the Desk Catalog",
                pred.field
            );
        }
        assert_eq!(preds[0].id.as_deref(), Some("esNearPositioningLevel"));
        assert_eq!(preds[3].path.as_deref(), Some(DOM_BID_REPLENISHING_PATH));
    }

    #[test]
    fn unbounded_and_oversized_windows_are_rejected() {
        let err = validate_window(QueryKind::Raw, None, Some(1.0)).unwrap_err();
        assert!(matches!(err, QueryKernelError::UnboundedWindow));
        let err = validate_window(QueryKind::Raw, Some(1.0), None).unwrap_err();
        assert!(matches!(err, QueryKernelError::UnboundedWindow));
        let start = FLAGSHIP_CLOCK_MS;
        let end = start + QUERY_RAW_MAX_WINDOW_MS + 1.0;
        let err = validate_window(QueryKind::Raw, Some(start), Some(end)).unwrap_err();
        assert!(matches!(err, QueryKernelError::WindowTooLarge { .. }));
        let err = validate_window(QueryKind::Series, Some(end), Some(start)).unwrap_err();
        assert!(matches!(err, QueryKernelError::InvertedWindow));
    }

    #[test]
    fn flagship_episode_query_matches_pinned_fixture_and_tick_mfe_mae() {
        let db = test_db();
        seed_flagship_frames(&db);
        let req = flagship_request(
            FLAGSHIP_CLOCK_MS - 500.0,
            FLAGSHIP_CLOCK_MS + 4_000.0,
            "RTH",
        );
        let result = query_episodes(&db, &req).expect("episodes");
        assert_eq!(
            result.matches.len(),
            FLAGSHIP_MATCHING_SECONDS,
            "match count must equal FLAGSHIP_MATCHING_SECONDS"
        );
        assert_eq!(result.meta.n, FLAGSHIP_MATCHING_SECONDS);
        assert_eq!(result.meta.sample_size, FLAGSHIP_MATCHING_SECONDS);
        assert_eq!(result.meta.reliability_tier, ReliabilityTier::Insufficient);
        assert_eq!(result.meta.trust_level, TrustLevel::L0);
        assert!(!result.meta.mutation_authority);
        assert!(!result.meta.order_authority);
        let m = &result.matches[0];
        assert!(m.journal_backed);
        assert_eq!(
            m.frame_ref.journal_frame_second,
            journal_frame_second_from_ts(FLAGSHIP_CLOCK_MS)
        );
        let fwd = m.forward.as_ref().expect("tick-driven forward");
        assert_eq!(fwd.source, "raw_ticks");
        assert!((fwd.mfe_points - FLAGSHIP_MFE).abs() < 1e-9);
        assert!((fwd.mae_points - FLAGSHIP_MAE).abs() < 1e-9);
        assert!((fwd.forward_return_points - FLAGSHIP_FORWARD).abs() < 1e-9);
        assert_eq!(fwd.tick_count, 3);
        assert_eq!(fwd.direction, "short");
        assert!(!fwd.truncated);
        assert_eq!(result.predicate_coverage.len(), 5);
        assert_eq!(m.positioning_id.as_deref(), Some("pos-flagship"));
    }

    #[test]
    fn flagship_does_not_match_the_non_matching_second() {
        let db = test_db();
        seed_flagship_frames(&db);
        let req = flagship_request(
            FLAGSHIP_CLOCK_MS - 500.0,
            FLAGSHIP_CLOCK_MS + 4_000.0,
            "RTH",
        );
        let result = query_episodes(&db, &req).expect("episodes");
        let seconds: Vec<i64> = result
            .matches
            .iter()
            .filter_map(|m| m.frame_ref.journal_frame_second)
            .collect();
        let miss = journal_frame_second_from_ts(FLAGSHIP_CLOCK_MS + 1_000.0).unwrap();
        assert!(
            !seconds.contains(&miss),
            "non-matching second must not be counted (pinned FLAGSHIP_NON_MATCHING_SECONDS={FLAGSHIP_NON_MATCHING_SECONDS})"
        );
        assert_eq!(FLAGSHIP_NON_MATCHING_SECONDS, 1);
    }

    #[test]
    fn mixed_session_without_scope_is_rejected() {
        let db = test_db();
        seed_flagship_frames(&db);
        let req = QueryEpisodesRequest {
            window: QueryWindow {
                start_ms: Some(FLAGSHIP_CLOCK_MS),
                end_ms: Some(FLAGSHIP_CLOCK_MS + 60_000.0),
                session_type: None,
                symbols: None,
            },
            predicates: flagship_episode_predicates(),
            forward_direction: Some("short".into()),
        };
        let err = query_episodes(&db, &req).unwrap_err();
        assert!(matches!(err, QueryKernelError::MixedSessionScope));
    }

    #[test]
    fn replenishing_bids_fail_closed_when_dom_path_absent() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[
            frame(
                clock,
                "ES",
                "RTH",
                serde_json::json!({
                    "lastPrice": FLAGSHIP_ES_PRICE,
                    "sessionDelta": FLAGSHIP_ES_DELTA,
                    "poorLow": true,
                    "rootSymbol": "ES",
                    "domSummary": { "askReplenishing": true },
                }),
            ),
            frame(clock, "NQ", "RTH", matching_nq_payload()),
        ])
        .expect("frames");
        seed_positioning(&db, clock);
        let req = flagship_request(clock, clock + 1_000.0, "RTH");
        let result = query_episodes(&db, &req).expect("episodes");
        assert_eq!(result.matches.len(), 0);
        assert!(result.meta.degraded);
        assert!(result
            .meta
            .unavailable_fields
            .iter()
            .any(|f| f == FIELD_DOM_SUMMARY));
        let bids = result
            .predicate_coverage
            .iter()
            .find(|c| c.id == "replenishingBids")
            .expect("coverage");
        assert_eq!(bids.missing_seconds, FLAGSHIP_MATCHING_SECONDS);
        assert!(bids.note.as_deref().unwrap_or("").contains("fail closed"));
    }

    #[test]
    fn query_series_returns_n_and_reliability() {
        let db = test_db();
        seed_flagship_frames(&db);
        let result = query_series(
            &db,
            &QuerySeriesRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 2_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["ES".into()]),
                },
                fields: vec![FIELD_LAST_PRICE.into(), FIELD_SESSION_DELTA.into()],
            },
        )
        .expect("series");
        assert_eq!(result.points.len(), 2);
        assert_eq!(result.meta.n, 2);
        assert_eq!(result.meta.reliability_tier, ReliabilityTier::Insufficient);
        assert_eq!(result.meta.trust_level, TrustLevel::L0);
        assert_eq!(
            result.points[0].values.get(FIELD_LAST_PRICE),
            Some(&serde_json::json!(FLAGSHIP_ES_PRICE))
        );
    }

    #[test]
    fn query_raw_hard_cap_and_unbounded_rejection() {
        let db = test_db();
        seed_flagship_frames(&db);
        let err = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: None,
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 1.0),
                    session_type: Some("RTH".into()),
                    symbols: None,
                },
                source: "journal_frames".into(),
                limit: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, QueryKernelError::UnboundedWindow));

        let result = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 2_000.0),
                    session_type: Some("RTH".into()),
                    symbols: None,
                },
                source: "journal_frames".into(),
                limit: Some(1),
            },
        )
        .expect("raw");
        assert_eq!(result.rows.len(), 1);
        assert!(result.meta.truncated);
        assert_eq!(result.meta.reliability_tier, ReliabilityTier::Insufficient);
        assert!(result.meta.degraded);
    }

    #[test]
    fn query_raw_full_window_is_not_truncated() {
        let db = test_db();
        seed_flagship_frames(&db);
        let result = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 2_000.0),
                    session_type: Some("RTH".into()),
                    symbols: None,
                },
                source: "journal_frames".into(),
                limit: Some(QUERY_RAW_MAX_ROWS),
            },
        )
        .expect("raw");
        // Two RTH seconds × ES+NQ = 4 frames. Pin the constant-derived count.
        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.meta.n, 4);
        assert!(!result.meta.truncated);
        assert!(!result.meta.degraded);
    }

    #[test]
    fn episode_query_without_positioning_is_not_degraded_for_missing_vendor_data() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[
            frame(clock, "ES", "RTH", matching_es_payload()),
            frame(clock, "NQ", "RTH", matching_nq_payload()),
        ])
        .expect("frames");
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            FLAGSHIP_ES_PRICE - 1.0,
            1.0,
            FLAGSHIP_ES_PRICE - 1.25,
            FLAGSHIP_ES_PRICE - 0.75,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
        let req = QueryEpisodesRequest {
            window: QueryWindow {
                start_ms: Some(clock),
                end_ms: Some(clock + 1_000.0),
                session_type: Some("RTH".into()),
                symbols: None,
            },
            predicates: vec![CatalogPredicate {
                id: Some("poorAuctionEfficiency".into()),
                symbol: Some("ES".into()),
                field: FIELD_POOR_LOW.into(),
                op: PredicateOp::Eq,
                value: Some(Value::Bool(true)),
                path: None,
                tolerance_ticks: None,
                event_type: None,
            }],
            forward_direction: Some("short".into()),
        };
        let result = query_episodes(&db, &req).expect("episodes");
        assert_eq!(result.matches.len(), 1);
        assert!(
            !result.meta.degraded,
            "queries that do not join Positioning must not degrade when no Levels-Only Record exists"
        );
        assert!(result.meta.unavailable_fields.is_empty());
        assert!(!result
            .meta
            .notes
            .iter()
            .any(|n| n.to_ascii_lowercase().contains("positioning")));
    }

    #[test]
    fn run_job_writes_columnar_artifact_not_a_token_flood() {
        let db = test_db();
        seed_flagship_frames(&db);
        let dir = TempDir::new().expect("tmpdir");
        let req = flagship_request(
            FLAGSHIP_CLOCK_MS - 500.0,
            FLAGSHIP_CLOCK_MS + 4_000.0,
            "RTH",
        );
        let value = serde_json::to_value(&req).expect("json");
        let job = run_job(
            &db,
            QueryKind::Episodes,
            &value,
            dir.path(),
            FLAGSHIP_CLOCK_MS,
        )
        .expect("run_job");
        assert!(job.job_id.starts_with("rq-"));
        assert_eq!(job.status, "completed");
        assert_eq!(job.trust_level, TrustLevel::L0);
        assert!(!job.mutation_authority);
        assert!(!job.order_authority);
        assert_eq!(job.meta.n, FLAGSHIP_MATCHING_SECONDS);
        assert!(Path::new(&job.artifact.columnar_path).exists());
        assert!(Path::new(&job.artifact.summary_path).exists());
        let csv = fs::read_to_string(&job.artifact.columnar_path).expect("csv");
        let data_lines = csv.lines().skip(1).filter(|l| !l.is_empty()).count();
        assert_eq!(data_lines, FLAGSHIP_MATCHING_SECONDS);
        assert!(job.artifact.summary.get("matchCount").is_some());
        assert!(job.artifact.summary.get("matches").is_none());
        let stored = db
            .get_research_query_job(&job.job_id)
            .expect("db")
            .expect("row");
        assert_eq!(stored.status, "completed");
        assert_eq!(stored.sample_size, Some(FLAGSHIP_MATCHING_SECONDS as i64));
    }

    #[test]
    fn execute_research_job_store_cold_keeps_l0_and_rejects_unknown_store() {
        let db = test_db();
        let cold_dir = TempDir::new().expect("cold");
        let store = ColdFrameStore::new(cold_dir.path());
        store
            .upsert_frames(&[frame(FLAGSHIP_CLOCK_MS, "ES", "RTH", matching_es_payload())])
            .expect("cold");
        let artifact = TempDir::new().expect("artifacts");
        let mut request = serde_json::json!({
            "startMs": FLAGSHIP_CLOCK_MS,
            "endMs": FLAGSHIP_CLOCK_MS + 2_000.0,
            "sessionType": "RTH",
            "symbols": ["ES"],
            "fields": [FIELD_LAST_PRICE],
            "store": "cold",
            "coldRoot": cold_dir.path().to_string_lossy(),
        });
        let job = execute_research_job(
            &db,
            QueryKind::Series,
            &request,
            artifact.path(),
            FLAGSHIP_CLOCK_MS,
            "rq-cold-test",
        )
        .expect("cold job");
        assert_eq!(job.trust_level, TrustLevel::L0);
        assert!(!job.mutation_authority);
        assert!(!job.order_authority);
        assert_eq!(
            job.meta.n, 1,
            "store=cold must read dumps, not empty SQLite"
        );
        assert!(job.meta.notes.iter().any(|n| n.contains("cold")));

        request["store"] = serde_json::json!("duckdb");
        let err = execute_research_job(
            &db,
            QueryKind::Series,
            &request,
            artifact.path(),
            FLAGSHIP_CLOCK_MS,
            "rq-duckdb",
        )
        .expect_err("unknown store");
        assert!(matches!(err, QueryKernelError::UnknownFrameStore(_)));
    }

    #[test]
    fn hypothesis_evidence_cites_journal_backed_windows() {
        let db = test_db();
        seed_flagship_frames(&db);
        let row = HypothesisSignalOutcomeRow {
            signal_id: "sig-1".into(),
            setup_id: "hyp_test_v1".into(),
            setup_name: Some("test".into()),
            session_date: FLAGSHIP_TRADING_DAY.into(),
            session_type: Some("RTH".into()),
            day_type: None,
            rvol_bucket_at_fire: None,
            fired_at_ms: FLAGSHIP_CLOCK_MS,
            fired_price: FLAGSHIP_NQ_PRICE,
            target_price: None,
            stop_price: None,
            outcome: "target_hit".into(),
            max_favorable_excursion: Some(10.0),
            max_adverse_excursion: Some(1.0),
            r_result: Some(1.0),
            time_to_outcome_ms: Some(1_000.0),
            root_symbol: Some("NQ".into()),
            contract_symbol: None,
            direction: Some("long".into()),
            entry_price: Some(FLAGSHIP_NQ_PRICE),
            risk_points: Some(8.0),
            exit_price: None,
            outcome_quality: "verified".into(),
        };
        let (windows, truncated) = journal_backed_evidence_windows(&db, &[row]).expect("windows");
        assert!(!truncated);
        assert_eq!(windows.len(), 1);
        assert!(windows[0].journal_backed);
        assert_eq!(
            windows[0].frame_ref.journal_frame_second,
            journal_frame_second_from_ts(FLAGSHIP_CLOCK_MS)
        );
        assert_eq!(windows[0].frame_ref.root_symbol.as_deref(), Some("NQ"));
        assert_eq!(windows[0].session_type.as_deref(), Some("RTH"));
    }

    fn outcome_row(fired_at_ms: f64, root: Option<&str>) -> HypothesisSignalOutcomeRow {
        HypothesisSignalOutcomeRow {
            signal_id: format!("sig-{fired_at_ms}"),
            setup_id: "hyp_test_v1".into(),
            setup_name: Some("test".into()),
            session_date: FLAGSHIP_TRADING_DAY.into(),
            session_type: Some("RTH".into()),
            day_type: None,
            rvol_bucket_at_fire: None,
            fired_at_ms,
            fired_price: FLAGSHIP_NQ_PRICE,
            target_price: None,
            stop_price: None,
            outcome: "target_hit".into(),
            max_favorable_excursion: Some(10.0),
            max_adverse_excursion: Some(1.0),
            r_result: Some(1.0),
            time_to_outcome_ms: Some(1_000.0),
            root_symbol: root.map(str::to_string),
            contract_symbol: None,
            direction: Some("long".into()),
            entry_price: Some(FLAGSHIP_NQ_PRICE),
            risk_points: Some(8.0),
            exit_price: None,
            outcome_quality: "verified".into(),
        }
    }

    #[test]
    fn rootless_evidence_windows_are_not_attributed_to_nq() {
        let db = test_db();
        seed_flagship_frames(&db);
        let (windows, truncated) =
            journal_backed_evidence_windows(&db, &[outcome_row(FLAGSHIP_CLOCK_MS, None)])
                .expect("windows");
        assert!(!truncated);
        assert_eq!(windows.len(), 1);
        assert!(!windows[0].journal_backed);
        assert_eq!(windows[0].frame_ref.root_symbol, None);
    }

    #[test]
    fn evidence_windows_are_hard_capped() {
        let db = test_db();
        seed_flagship_frames(&db);
        let rows: Vec<_> = (0..=HYPOTHESIS_EVIDENCE_WINDOW_CAP)
            .map(|i| outcome_row(FLAGSHIP_CLOCK_MS + i as f64, Some("NQ")))
            .collect();
        assert_eq!(rows.len(), HYPOTHESIS_EVIDENCE_WINDOW_CAP + 1);
        let (windows, truncated) = journal_backed_evidence_windows(&db, &rows).expect("windows");
        assert!(truncated);
        assert_eq!(windows.len(), HYPOTHESIS_EVIDENCE_WINDOW_CAP);
    }

    #[test]
    fn exists_predicate_is_available_when_the_field_is_absent() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[frame(
            clock,
            "ES",
            "RTH",
            serde_json::json!({ "sessionDelta": -1.0, "rootSymbol": "ES" }),
        )])
        .expect("frames");
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            FLAGSHIP_ES_PRICE,
            1.0,
            FLAGSHIP_ES_PRICE - 0.25,
            FLAGSHIP_ES_PRICE + 0.25,
            true,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
        let result = query_episodes(
            &db,
            &QueryEpisodesRequest {
                window: QueryWindow {
                    start_ms: Some(clock),
                    end_ms: Some(clock + 1_000.0),
                    session_type: Some("RTH".into()),
                    symbols: None,
                },
                predicates: vec![CatalogPredicate {
                    id: Some("hasLastPrice".into()),
                    symbol: Some("ES".into()),
                    field: FIELD_LAST_PRICE.into(),
                    op: PredicateOp::Exists,
                    value: None,
                    path: None,
                    tolerance_ticks: None,
                    event_type: None,
                }],
                forward_direction: Some("short".into()),
            },
        )
        .expect("episodes");
        assert!(
            result.matches.is_empty(),
            "absent lastPrice must not match Exists"
        );
        assert_eq!(result.predicate_coverage[0].available_seconds, 1);
        assert_eq!(result.predicate_coverage[0].missing_seconds, 0);
        assert!(
            !result.meta.degraded,
            "Exists on an absent field is available=false-match, not unavailable"
        );
    }

    #[test]
    fn unknown_session_type_is_rejected() {
        let db = test_db();
        let err = query_episodes(
            &db,
            &QueryEpisodesRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 1_000.0),
                    session_type: Some("Overnight".into()),
                    symbols: None,
                },
                predicates: flagship_episode_predicates(),
                forward_direction: None,
            },
        )
        .unwrap_err();
        match err {
            QueryKernelError::Invalid(msg) => {
                assert!(
                    msg.contains("RTH") && msg.contains("Globex"),
                    "rejection must name allowed session types, got {msg}"
                );
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn stale_positioning_is_applied_and_degraded() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[
            frame(clock, "ES", "RTH", matching_es_payload()),
            frame(clock, "NQ", "RTH", matching_nq_payload()),
        ])
        .expect("frames");
        seed_positioning(&db, clock - POSITIONING_STALE_MS - 1.0);
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            FLAGSHIP_ES_PRICE - 1.0,
            1.0,
            FLAGSHIP_ES_PRICE - 1.25,
            FLAGSHIP_ES_PRICE - 0.75,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
        let result = query_episodes(&db, &flagship_request(clock, clock + 1_000.0, "RTH"))
            .expect("episodes");
        assert_eq!(result.matches.len(), FLAGSHIP_MATCHING_SECONDS);
        assert_eq!(
            result.matches[0].positioning_id.as_deref(),
            Some("pos-flagship")
        );
        assert!(result.meta.degraded);
        assert!(result
            .meta
            .notes
            .iter()
            .any(|n| n.to_ascii_lowercase().contains("older than")));
    }

    #[test]
    fn query_raw_scans_all_requested_roots_and_applies_session_scope() {
        let db = test_db();
        seed_flagship_frames(&db);
        let nq_only = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 2_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["NQ".into()]),
                },
                source: "journal_frames".into(),
                limit: Some(QUERY_RAW_MAX_ROWS),
            },
        )
        .expect("nq");
        assert_eq!(nq_only.rows.len(), 2);
        assert!(nq_only
            .rows
            .iter()
            .all(|r| r.root_symbol.as_deref() == Some("NQ")));

        let both = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 2_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["NQ".into(), "ES".into()]),
                },
                source: "journal_frames".into(),
                limit: Some(QUERY_RAW_MAX_ROWS),
            },
        )
        .expect("both");
        assert_eq!(both.rows.len(), 4);

        let globex_tick_ms = FLAGSHIP_CLOCK_MS - 8.0 * 3_600_000.0;
        db.insert_raw_tick_with_contract(
            globex_tick_ms,
            FLAGSHIP_ES_PRICE,
            1.0,
            FLAGSHIP_ES_PRICE - 0.25,
            FLAGSHIP_ES_PRICE + 0.25,
            true,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("globex tick");
        let globex_as_rth = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(globex_tick_ms),
                    end_ms: Some(globex_tick_ms + 60_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["ES".into()]),
                },
                source: "ticks".into(),
                limit: Some(QUERY_RAW_MAX_ROWS),
            },
        )
        .expect("globex-as-rth");
        assert!(
            globex_as_rth.rows.is_empty(),
            "Globex-clock ticks must be excluded when sessionType=RTH"
        );
        let rth_ticks = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 4_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["ES".into()]),
                },
                source: "ticks".into(),
                limit: Some(QUERY_RAW_MAX_ROWS),
            },
        )
        .expect("rth ticks");
        assert_eq!(rth_ticks.rows.len(), 3);
    }

    #[test]
    fn query_raw_events_apply_session_filter_before_limit() {
        let db = test_db();
        let start = FLAGSHIP_CLOCK_MS;
        let mut events = Vec::new();
        for i in 0..5 {
            events.push(MarketEvent {
                session_date: FLAGSHIP_TRADING_DAY.into(),
                timestamp_ms: start + f64::from(i),
                event_type: "poor_low".into(),
                level_name: None,
                price: FLAGSHIP_NQ_PRICE,
                direction: Some("down".into()),
                sequence_num: Some(i),
                metadata: None,
                session_type: "Globex".into(),
                session_segment: "None".into(),
                trading_day: FLAGSHIP_TRADING_DAY.into(),
            });
        }
        events.push(MarketEvent {
            session_date: FLAGSHIP_TRADING_DAY.into(),
            timestamp_ms: start + 5.0,
            event_type: "poor_low".into(),
            level_name: None,
            price: FLAGSHIP_NQ_PRICE + 1.0,
            direction: Some("down".into()),
            sequence_num: Some(5),
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: FLAGSHIP_TRADING_DAY.into(),
        });
        db.insert_market_events_batch_scoped(Some("NQ"), &events)
            .expect("events");
        let result = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(start),
                    end_ms: Some(start + 10.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["NQ".into()]),
                },
                source: "events".into(),
                limit: Some(3),
            },
        )
        .expect("raw events");
        assert_eq!(
            result.rows.len(),
            1,
            "RTH event must still be returned when five Globex rows would fill LIMIT 3"
        );
        assert_eq!(
            result.rows[0]
                .payload
                .get("sessionType")
                .and_then(|v| v.as_str()),
            Some("RTH")
        );
    }

    #[test]
    fn clamp_window_to_session_intersects_rth_across_the_open() {
        let rth_open_ms = FLAGSHIP_CLOCK_MS - 30.0 * 60_000.0;
        let start = rth_open_ms - 10.0 * 60_000.0;
        let end = rth_open_ms + 5.0 * 60_000.0;
        let (lo, hi) = clamp_window_to_session(start, end, Some("RTH")).expect("rth overlap");
        assert!(
            (lo - rth_open_ms).abs() < 1.0,
            "RTH clamp must start at the open, got {lo} vs open {rth_open_ms}"
        );
        assert_eq!(hi, end);
        assert!(clamp_window_to_session(start, rth_open_ms - 1.0, Some("RTH")).is_none());
        assert!(clamp_window_to_session(start, end, None).is_some());
    }

    #[test]
    fn query_raw_ticks_apply_session_filter_before_limit() {
        let db = test_db();
        let rth_open_ms = FLAGSHIP_CLOCK_MS - 30.0 * 60_000.0;
        let start = rth_open_ms - 10.0 * 60_000.0;
        let end = rth_open_ms + 5.0 * 60_000.0;
        for i in 0..5 {
            db.insert_raw_tick_with_contract(
                start + f64::from(i),
                FLAGSHIP_ES_PRICE,
                1.0,
                FLAGSHIP_ES_PRICE - 0.25,
                FLAGSHIP_ES_PRICE + 0.25,
                true,
                FLAGSHIP_TRADING_DAY,
                Some("ES"),
                Some("ESH24.CME"),
            )
            .expect("globex tick");
        }
        let rth_tick_ms = rth_open_ms + 60_000.0;
        db.insert_raw_tick_with_contract(
            rth_tick_ms,
            FLAGSHIP_ES_PRICE + 1.0,
            1.0,
            FLAGSHIP_ES_PRICE + 0.75,
            FLAGSHIP_ES_PRICE + 1.25,
            true,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("rth tick");
        let result = query_raw(
            &db,
            &QueryRawRequest {
                window: QueryWindow {
                    start_ms: Some(start),
                    end_ms: Some(end),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["ES".into()]),
                },
                source: "ticks".into(),
                limit: Some(3),
            },
        )
        .expect("raw ticks");
        assert_eq!(
            result.rows.len(),
            1,
            "RTH tick must still be returned when five Globex ticks would fill LIMIT 3"
        );
        assert_eq!(result.rows[0].timestamp_ms, rth_tick_ms);
    }

    #[test]
    fn query_episodes_caps_forward_mfe_mae_scans() {
        let db = test_db();
        seed_positioning(&db, FLAGSHIP_CLOCK_MS);
        let n = QUERY_FORWARD_MAX_MATCHES + 1;
        let mut frames = Vec::with_capacity(n * 2);
        for i in 0..n {
            let clock = FLAGSHIP_CLOCK_MS + (i as f64) * 1_000.0;
            frames.push(frame(clock, "ES", "RTH", matching_es_payload()));
            frames.push(frame(clock, "NQ", "RTH", matching_nq_payload()));
        }
        db.insert_journal_frames(&frames).expect("frames");
        db.insert_raw_tick_with_contract(
            FLAGSHIP_CLOCK_MS + (n as f64) * 1_000.0,
            FLAGSHIP_ES_PRICE - 1.0,
            1.0,
            FLAGSHIP_ES_PRICE - 1.25,
            FLAGSHIP_ES_PRICE - 0.75,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
        let result = query_episodes(
            &db,
            &flagship_request(
                FLAGSHIP_CLOCK_MS,
                FLAGSHIP_CLOCK_MS + (n as f64) * 1_000.0,
                "RTH",
            ),
        )
        .expect("episodes");
        assert_eq!(result.matches.len(), n);
        assert!(
            result.matches[..QUERY_FORWARD_MAX_MATCHES]
                .iter()
                .all(|m| m.forward.is_some()),
            "first {QUERY_FORWARD_MAX_MATCHES} matches must compute forward MFE/MAE"
        );
        assert!(
            result.matches[QUERY_FORWARD_MAX_MATCHES].forward.is_none(),
            "matches beyond the named forward cap must not scan ticks"
        );
        assert!(result.meta.degraded);
        assert!(result.meta.notes.iter().any(|note| {
            note.contains(&QUERY_FORWARD_MAX_MATCHES.to_string()) && note.contains("named cap")
        }));
        assert!(
            !result
                .meta
                .notes
                .iter()
                .any(|note| note.contains("no raw_ticks")),
            "capped matches must not be reported as missing ticks"
        );
    }

    #[test]
    fn forward_mfe_mae_sets_truncated_when_tick_cap_is_hit() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[
            frame(clock, "ES", "RTH", matching_es_payload()),
            frame(clock, "NQ", "RTH", matching_nq_payload()),
        ])
        .expect("frames");
        seed_positioning(&db, clock);
        let mut ticks = Vec::with_capacity(QUERY_FORWARD_TICK_CAP + 2);
        for i in 0..(QUERY_FORWARD_TICK_CAP + 2) {
            let ts = clock + 1.0 + i as f64;
            ticks.push((
                ts,
                FLAGSHIP_ES_PRICE - 0.25,
                1.0,
                FLAGSHIP_ES_PRICE - 0.50,
                FLAGSHIP_ES_PRICE,
                false,
                FLAGSHIP_TRADING_DAY.to_string(),
                "ES".to_string(),
                "ESH24.CME".to_string(),
            ));
        }
        db.insert_raw_ticks_batch(&ticks).expect("tick cap fixture");
        let result = query_episodes(&db, &flagship_request(clock, clock + 1_000.0, "RTH"))
            .expect("episodes");
        assert_eq!(result.matches.len(), 1);
        let fwd = result.matches[0].forward.as_ref().expect("forward");
        assert!(fwd.truncated);
        assert_eq!(fwd.tick_count, QUERY_FORWARD_TICK_CAP);
        assert!(result.meta.degraded);
        assert!(result.meta.notes.iter().any(|n| n.contains("truncated")));
    }

    #[test]
    fn journal_frame_second_lookup_does_not_range_scan() {
        let db = test_db();
        let start = FLAGSHIP_CLOCK_MS;
        let mut frames = Vec::with_capacity(100);
        for i in 0..100 {
            frames.push(frame(
                start + i as f64 * 1_000.0,
                "NQ",
                "RTH",
                matching_nq_payload(),
            ));
        }
        db.insert_journal_frames(&frames).expect("dense frames");
        let lo = journal_frame_second_from_ts(start).unwrap();
        let hi = journal_frame_second_from_ts(start + 99_000.0).unwrap();
        let found = db
            .list_journal_frames_at_seconds("NQ", &[lo, hi])
            .expect("bounded lookup");
        assert_eq!(
            found.len(),
            2,
            "must return only the requested seconds, not the 100-second span"
        );
        let (windows, truncated) = journal_backed_evidence_windows(
            &db,
            &[
                outcome_row(start, Some("NQ")),
                outcome_row(start + 99_000.0, Some("NQ")),
            ],
        )
        .expect("windows");
        assert!(!truncated);
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().all(|w| w.journal_backed));
    }

    #[test]
    fn query_episodes_requires_symbol_on_catalog_predicates() {
        let db = test_db();
        let err = query_episodes(
            &db,
            &QueryEpisodesRequest {
                window: QueryWindow {
                    start_ms: Some(FLAGSHIP_CLOCK_MS),
                    end_ms: Some(FLAGSHIP_CLOCK_MS + 1_000.0),
                    session_type: Some("RTH".into()),
                    symbols: None,
                },
                predicates: vec![CatalogPredicate {
                    id: Some("noSymbol".into()),
                    symbol: None,
                    field: FIELD_POOR_LOW.into(),
                    op: PredicateOp::Eq,
                    value: Some(Value::Bool(true)),
                    path: None,
                    tolerance_ticks: None,
                    event_type: None,
                }],
                forward_direction: None,
            },
        )
        .unwrap_err();
        match err {
            QueryKernelError::Invalid(msg) => {
                assert!(
                    msg.contains("symbol"),
                    "rejection must mention symbol, got {msg}"
                );
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn query_episodes_honours_symbols_filter() {
        let db = test_db();
        let clock = FLAGSHIP_CLOCK_MS;
        db.insert_journal_frames(&[
            frame(clock, "ES", "RTH", matching_es_payload()),
            frame(clock, "NQ", "RTH", matching_nq_payload()),
        ])
        .expect("frames");
        db.insert_raw_tick_with_contract(
            clock + 1_000.0,
            FLAGSHIP_ES_PRICE - 1.0,
            1.0,
            FLAGSHIP_ES_PRICE - 1.25,
            FLAGSHIP_ES_PRICE - 0.75,
            false,
            FLAGSHIP_TRADING_DAY,
            Some("ES"),
            Some("ESH24.CME"),
        )
        .expect("tick");
        let nq_only = query_episodes(
            &db,
            &QueryEpisodesRequest {
                window: QueryWindow {
                    start_ms: Some(clock),
                    end_ms: Some(clock + 1_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["NQ".into()]),
                },
                predicates: vec![CatalogPredicate {
                    id: Some("poorAuctionEfficiency".into()),
                    symbol: Some("ES".into()),
                    field: FIELD_POOR_LOW.into(),
                    op: PredicateOp::Eq,
                    value: Some(Value::Bool(true)),
                    path: None,
                    tolerance_ticks: None,
                    event_type: None,
                }],
                forward_direction: Some("short".into()),
            },
        )
        .expect("nq-only");
        assert_eq!(
            nq_only.matches.len(),
            0,
            "symbols=[NQ] must not load ES frames; ES poorLow must fail closed"
        );

        let both = query_episodes(
            &db,
            &QueryEpisodesRequest {
                window: QueryWindow {
                    start_ms: Some(clock),
                    end_ms: Some(clock + 1_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["NQ".into(), "ES".into()]),
                },
                predicates: vec![CatalogPredicate {
                    id: Some("poorAuctionEfficiency".into()),
                    symbol: Some("ES".into()),
                    field: FIELD_POOR_LOW.into(),
                    op: PredicateOp::Eq,
                    value: Some(Value::Bool(true)),
                    path: None,
                    tolerance_ticks: None,
                    event_type: None,
                }],
                forward_direction: Some("short".into()),
            },
        )
        .expect("both");
        assert_eq!(both.matches.len(), 1);
    }

    #[test]
    fn query_series_pages_across_query_page_ms() {
        let db = test_db();
        let first = FLAGSHIP_CLOCK_MS;
        let second = FLAGSHIP_CLOCK_MS + QUERY_PAGE_MS + 1_000.0;
        db.insert_journal_frames(&[
            frame(first, "ES", "RTH", matching_es_payload()),
            frame(second, "ES", "RTH", matching_es_payload()),
        ])
        .expect("paged frames");
        let result = query_series(
            &db,
            &QuerySeriesRequest {
                window: QueryWindow {
                    start_ms: Some(first),
                    end_ms: Some(second + 1_000.0),
                    session_type: Some("RTH".into()),
                    symbols: Some(vec!["ES".into()]),
                },
                fields: vec![FIELD_LAST_PRICE.into()],
            },
        )
        .expect("series");
        assert_eq!(
            result.points.len(),
            2,
            "frames on either side of QUERY_PAGE_MS must both be returned"
        );
        assert_eq!(result.meta.n, 2);
        assert!(!result.meta.truncated);
    }
}
