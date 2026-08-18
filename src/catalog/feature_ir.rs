//! Feature-IR — typed intermediate representation for **Derived Features** (SIL-M5b).
//!
//! Base Detector math stays reviewed Rust and is **not** re-expressed here.
//! A Derived Feature is a finite program over Desk Catalog fields using exactly
//! five funded **Operator Families**. Unfunded families (including surface
//! lookup / interpolation) are rejected at declaration time.
//!
//! The language is not Turing-complete: no unbounded recursion or user-declared
//! loops as control flow. A new Operator Family requires a registry change
//! proposal ([`NEW_OPERATOR_FAMILY_GATE`]). SIL-M5c codegen emitters (`emit_*`)
//! live in [`super::codegen`].
//!
//! Live-shadow and historical evaluation share **one** evaluator over Journal
//! Frames (`clock_ms <= asOf`). The path label does not change semantics.
//! Both paths cap the visible slice at [`FEATURE_IR_EVAL_MAX_FRAMES`] (~8 hours
//! of 1 Hz NQ+ES). That bound is **not** the live pending-journal drain
//! ([`crate::engine::PENDING_JOURNAL_MAX_FRAMES`]). Live evaluation concatenates
//! a bounded SQLite load with pending frames (pending wins on identity).
//! Session-distribution `n` and dwell windows fail closed when that bound would
//! silently drop in-session frames — they must not pretend an unbounded
//! `list_journal_frames` scan equals the eval cap.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::types::{DeskCatalog, FieldDescriptor};
use crate::et_minutes_from_timestamp;

/// Provenance source stamp for Derived Features (not `rust_pipeline`).
pub const FEATURE_IR_SOURCE: &str = "feature_ir";

/// Rust module path recorded on Derived Feature provenance.
pub const FEATURE_IR_MODULE: &str = "catalog::feature_ir";

/// Math-tier stamp: declared Feature-IR, not Tier-1 reviewed detector math.
pub const DERIVED_FEATURE_MATH_TIER: &str = "derived_feature_ir";

/// Gate for adding a sixth Operator Family (documented in the decision log).
pub const NEW_OPERATOR_FAMILY_GATE: &str = "registry_change_proposal";

/// Maximum historical-baseline lookback (keeps scans bounded).
pub const MAX_BASELINE_LOOKBACK_DAYS: u32 = 60;

/// Maximum event-sequence window (24 hours).
pub const MAX_SEQUENCE_WINDOW_MS: f64 = 86_400_000.0;

/// Shared live/historical Feature-IR evaluation cap (~8 hours of 1 Hz NQ+ES).
///
/// Intentionally **not** equal to [`crate::engine::PENDING_JOURNAL_MAX_FRAMES`].
/// The pending buffer is an in-memory drain (8192). Live evaluation loads a
/// bounded SQLite window of this size and concatenates pending frames
/// ([`merge_eval_frames`]; pending wins on `(frame_second, root_symbol)`).
/// Historical loads use a newest-first `LIMIT cap+1` sentinel instead of
/// [`crate::db::Database::list_journal_frames`]. Session-percentile `n` and
/// dwell fail closed when the as-of session is truncated by this cap.
pub const FEATURE_IR_EVAL_MAX_FRAMES: usize = 57_600;

/// Wire labels for the five funded families (camelCase IPC).
pub const FUNDED_OPERATOR_FAMILY_LABELS: &[&str] = &[
    "crossSymbolReferences",
    "sessionDistributionPercentiles",
    "dwellTimeSincePredicate",
    "eventSequences",
    "historicalBaselines",
];

/// CONTEXT.md / SPEC glossary names — use these exactly in docs and errors.
pub const FUNDED_OPERATOR_FAMILY_GLOSSARY: &[&str] = &[
    "Cross-symbol references",
    "Session-distribution percentiles",
    "Dwell / time-since-predicate",
    "Event sequences",
    "Historical baselines",
];

/// Keys that would smuggle unfunded / Turing-complete control flow into a program.
const REJECTED_CONTROL_FLOW_KEYS: &[&str] = &[
    "loop",
    "while",
    "for",
    "recurse",
    "recursion",
    "map",
    "reduce",
    "fold",
    "goto",
    "call",
    "apply",
];

/// Explicitly unfunded families (surface interpolation stays unfunded).
const UNFUNDED_FAMILY_TOKENS: &[&str] = &[
    "surfacelookup",
    "surfaceinterpolation",
    "interpolation",
    "surface",
    "heatmap",
    "spline",
    "vendorstudy",
    "codegen",
    "emitter",
];

/// Funded Operator Families (exactly five).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperatorFamily {
    CrossSymbolReferences,
    SessionDistributionPercentiles,
    DwellTimeSincePredicate,
    EventSequences,
    HistoricalBaselines,
}

impl OperatorFamily {
    /// CamelCase wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossSymbolReferences => "crossSymbolReferences",
            Self::SessionDistributionPercentiles => "sessionDistributionPercentiles",
            Self::DwellTimeSincePredicate => "dwellTimeSincePredicate",
            Self::EventSequences => "eventSequences",
            Self::HistoricalBaselines => "historicalBaselines",
        }
    }

    /// Exact top-level JSON keys accepted for this family (`family` plus variant fields).
    ///
    /// Unknown keys are rejected at parse time so a mistyped optional (for example
    /// `sametimeofday`) cannot be silently dropped by serde.
    pub fn allowed_program_keys(self) -> &'static [&'static str] {
        match self {
            Self::CrossSymbolReferences => &["family", "symbol", "field"],
            Self::SessionDistributionPercentiles => {
                &["family", "field", "symbol", "percentile", "output"]
            }
            Self::DwellTimeSincePredicate => &["family", "predicate", "mode"],
            Self::EventSequences => &["family", "first", "then", "withinMs"],
            Self::HistoricalBaselines => &[
                "family",
                "field",
                "symbol",
                "lookbackDays",
                "sameTimeOfDay",
                "aggregator",
                "percentile",
            ],
        }
    }

    /// Glossary name from CONTEXT.md / SPEC user stories 44–46, 52.
    pub fn glossary_name(self) -> &'static str {
        match self {
            Self::CrossSymbolReferences => "Cross-symbol references",
            Self::SessionDistributionPercentiles => "Session-distribution percentiles",
            Self::DwellTimeSincePredicate => "Dwell / time-since-predicate",
            Self::EventSequences => "Event sequences",
            Self::HistoricalBaselines => "Historical baselines",
        }
    }

    /// Parse a wire label or glossary name. Unknown values are not coerced.
    pub fn parse(raw: &str) -> Option<Self> {
        let compact = compact_family_token(raw);
        match compact.as_str() {
            "crosssymbolreferences" | "crosssymbol" | "crosssymbolreference" => {
                Some(Self::CrossSymbolReferences)
            }
            "sessiondistributionpercentiles"
            | "sessionpercentiles"
            | "sessiondistributionpercentile" => Some(Self::SessionDistributionPercentiles),
            "dwelltimesincepredicate"
            | "dwell"
            | "timesincepredicate"
            | "timesince"
            | "dwelltimesince" => Some(Self::DwellTimeSincePredicate),
            "eventsequences" | "eventsequence" | "athenbwithint" => Some(Self::EventSequences),
            "historicalbaselines" | "historicalbaseline" => Some(Self::HistoricalBaselines),
            _ => None,
        }
    }
}

impl std::fmt::Display for OperatorFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.glossary_name())
    }
}

/// Typed Feature-IR program (one funded family; finite, no loops).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "camelCase")]
pub enum FeatureIrProgram {
    /// Cross-symbol references: read a catalog field on NQ or ES at the same frame second.
    CrossSymbolReferences { symbol: String, field: String },
    /// Session-distribution percentiles of a catalog field (as-of, session-scoped).
    SessionDistributionPercentiles {
        field: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
        /// Used when [`PercentileOutput::Value`]: type-7 percentile of the session sample.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        percentile: Option<f64>,
        #[serde(default)]
        output: PercentileOutput,
    },
    /// Dwell / time-since-predicate over Journal Frames.
    DwellTimeSincePredicate {
        predicate: FieldPredicate,
        #[serde(default)]
        mode: DwellMode,
    },
    /// Event sequences: A then B within T (milliseconds).
    EventSequences {
        first: EventSelector,
        then: EventSelector,
        #[serde(rename = "withinMs")]
        within_ms: f64,
    },
    /// Historical baselines, including same time of day over N prior days.
    HistoricalBaselines {
        field: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
        #[serde(rename = "lookbackDays")]
        lookback_days: u32,
        #[serde(default = "default_true", rename = "sameTimeOfDay")]
        same_time_of_day: bool,
        #[serde(default)]
        aggregator: BaselineAggregator,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        percentile: Option<f64>,
    },
}

fn default_true() -> bool {
    true
}

impl FeatureIrProgram {
    /// Operator family of this program.
    pub fn family(&self) -> OperatorFamily {
        match self {
            Self::CrossSymbolReferences { .. } => OperatorFamily::CrossSymbolReferences,
            Self::SessionDistributionPercentiles { .. } => {
                OperatorFamily::SessionDistributionPercentiles
            }
            Self::DwellTimeSincePredicate { .. } => OperatorFamily::DwellTimeSincePredicate,
            Self::EventSequences { .. } => OperatorFamily::EventSequences,
            Self::HistoricalBaselines { .. } => OperatorFamily::HistoricalBaselines,
        }
    }

    /// Catalog field ids this program reads.
    pub fn catalog_field_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        match self {
            Self::CrossSymbolReferences { field, .. }
            | Self::SessionDistributionPercentiles { field, .. }
            | Self::HistoricalBaselines { field, .. } => ids.push(field.clone()),
            Self::DwellTimeSincePredicate { predicate, .. } => ids.push(predicate.field.clone()),
            Self::EventSequences { .. } => {}
        }
        ids
    }

    /// Parse a JSON declaration. Unfunded families and control-flow keys fail closed.
    pub fn parse_value(raw: &Value) -> Result<Self, FeatureIrError> {
        let Some(obj) = raw.as_object() else {
            return Err(FeatureIrError::InvalidProgram(
                "program must be a JSON object".into(),
            ));
        };
        for key in obj.keys() {
            let compact = compact_family_token(key);
            if REJECTED_CONTROL_FLOW_KEYS.contains(&compact.as_str()) {
                return Err(FeatureIrError::ControlFlowRejected(key.clone()));
            }
            if UNFUNDED_FAMILY_TOKENS.contains(&compact.as_str()) {
                return Err(FeatureIrError::UnfundedOperatorFamily(key.clone()));
            }
        }
        let family_raw = obj
            .get("family")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or(FeatureIrError::MissingFamily)?;
        if let Some(unfunded) = classify_unfunded(family_raw) {
            return Err(FeatureIrError::UnfundedOperatorFamily(unfunded));
        }
        let family = OperatorFamily::parse(family_raw)
            .ok_or_else(|| FeatureIrError::UnfundedOperatorFamily(family_raw.to_string()))?;
        let allowed = family.allowed_program_keys();
        for key in obj.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(FeatureIrError::UnknownProgramKey(key.clone()));
            }
        }
        let mut normalized = raw.clone();
        if let Some(map) = normalized.as_object_mut() {
            map.insert("family".into(), Value::String(family.as_str().into()));
        }
        serde_json::from_value(normalized)
            .map_err(|e| FeatureIrError::InvalidProgram(e.to_string()))
    }

    /// Declaration-time validation against the Desk Catalog (no evaluation).
    pub fn validate(&self, catalog: &DeskCatalog) -> Result<(), FeatureIrError> {
        match self {
            Self::CrossSymbolReferences { symbol, field } => {
                validate_root_symbol(symbol)?;
                resolve_catalog_field(catalog, field)?;
            }
            Self::SessionDistributionPercentiles {
                field,
                symbol,
                percentile,
                output,
            } => {
                if let Some(sym) = symbol.as_deref() {
                    validate_root_symbol(sym)?;
                }
                resolve_catalog_field(catalog, field)?;
                if percentile.is_some() && *output != PercentileOutput::Value {
                    return Err(FeatureIrError::UnusedPercentileKey);
                }
                if *output == PercentileOutput::Value {
                    let p = percentile.unwrap_or(50.0);
                    if !(0.0..=100.0).contains(&p) || !p.is_finite() {
                        return Err(FeatureIrError::InvalidPercentile);
                    }
                }
            }
            Self::DwellTimeSincePredicate { predicate, .. } => {
                if let Some(sym) = predicate.symbol.as_deref() {
                    validate_root_symbol(sym)?;
                }
                if predicate.field.trim().is_empty() {
                    return Err(FeatureIrError::InvalidPredicate);
                }
                resolve_catalog_field(catalog, &predicate.field)?;
            }
            Self::EventSequences {
                first,
                then,
                within_ms,
            } => {
                if first.event_type.trim().is_empty() || then.event_type.trim().is_empty() {
                    return Err(FeatureIrError::InvalidEventSequence);
                }
                if let Some(sym) = first.symbol.as_deref() {
                    validate_root_symbol(sym)?;
                }
                if let Some(sym) = then.symbol.as_deref() {
                    validate_root_symbol(sym)?;
                }
                if !within_ms.is_finite()
                    || *within_ms <= 0.0
                    || *within_ms > MAX_SEQUENCE_WINDOW_MS
                {
                    return Err(FeatureIrError::InvalidEventSequence);
                }
            }
            Self::HistoricalBaselines {
                field,
                symbol,
                lookback_days,
                percentile,
                aggregator,
                ..
            } => {
                if let Some(sym) = symbol.as_deref() {
                    validate_root_symbol(sym)?;
                }
                resolve_catalog_field(catalog, field)?;
                if *lookback_days == 0 || *lookback_days > MAX_BASELINE_LOOKBACK_DAYS {
                    return Err(FeatureIrError::InvalidLookback);
                }
                if percentile.is_some() && *aggregator != BaselineAggregator::Percentile {
                    return Err(FeatureIrError::UnusedPercentileKey);
                }
                if *aggregator == BaselineAggregator::Percentile {
                    let p = percentile.unwrap_or(50.0);
                    if !(0.0..=100.0).contains(&p) || !p.is_finite() {
                        return Err(FeatureIrError::InvalidPercentile);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Percentile program output: rank of current value, or a distribution value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PercentileOutput {
    #[default]
    Rank,
    Value,
}

/// Dwell mode: continuous true duration, or time since last true.
///
/// `timeSince` is unavailable when the predicate never held in the visible
/// session window (it must not return a wall-clock epoch as a duration).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DwellMode {
    #[default]
    Dwell,
    TimeSince,
}

/// Historical baseline aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BaselineAggregator {
    #[default]
    Mean,
    Median,
    Percentile,
}

/// Catalog-field predicate used by dwell / time-since.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldPredicate {
    pub field: String,
    pub op: PredicateOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Comparison operator for a field predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PredicateOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
}

/// Event selector for sequence programs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSelector {
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Event row visible to Feature-IR (Journal Frame join via timestamp).
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureIrEvent {
    pub timestamp_ms: f64,
    pub event_type: String,
    pub root_symbol: String,
}

/// Journal Frame view for Feature-IR evaluation (live-shadow and historical).
///
/// Kept independent of `db::JournalFrameRecord` so the catalog module does not
/// form a compile cycle with `db`. Callers map persisted frames in.
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureIrFrame {
    pub clock_ms: f64,
    pub frame_second: i64,
    pub root_symbol: String,
    pub session_type: String,
    pub trading_day: String,
    pub payload: Value,
}

/// Evaluation path label. Live-shadow and historical share one evaluator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FeatureIrEvalPath {
    LiveShadow,
    Historical,
}

/// Journal Frame + event store for one evaluation.
#[derive(Debug, Clone, Copy)]
pub struct FeatureIrStore<'a> {
    pub catalog: &'a DeskCatalog,
    pub frames: &'a [FeatureIrFrame],
    pub events: &'a [FeatureIrEvent],
    pub eval_root: &'a str,
    /// True when the caller already dropped frames older than
    /// [`FEATURE_IR_EVAL_MAX_FRAMES`] (bounded SQLite sentinel or merged window).
    pub window_truncated: bool,
}

/// Scalar result of one Feature-IR evaluation at `asOf`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureIrValue {
    pub path: FeatureIrEvalPath,
    pub family: OperatorFamily,
    pub as_of_ms: f64,
    pub value: f64,
    pub available: bool,
    pub n: usize,
}

/// Feature-IR declaration / evaluation errors (typed until MCP / CLI).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FeatureIrError {
    #[error("Feature-IR program requires `family`")]
    MissingFamily,
    #[error(
        "unfunded operator family `{0}` is rejected at declaration time \
         (surface lookup / interpolation is unfunded; a new Operator Family \
         requires a registry change proposal)"
    )]
    UnfundedOperatorFamily(String),
    #[error("Feature-IR is not Turing-complete: `{0}` is rejected as user-declared control flow")]
    ControlFlowRejected(String),
    #[error("unknown catalog field `{0}`")]
    UnknownCatalogField(String),
    #[error("invalid Feature-IR program: {0}")]
    InvalidProgram(String),
    #[error("cross-symbol references require symbol=NQ or symbol=ES")]
    InvalidSymbol,
    #[error("event sequences require first, then, and a finite withinMs in (0, 86400000]")]
    InvalidEventSequence,
    #[error("historical baselines require lookbackDays in 1..=60")]
    InvalidLookback,
    #[error("session-distribution percentiles require percentile in 0..=100 when output=value")]
    InvalidPercentile,
    #[error("dwell / time-since-predicate requires a catalog field predicate")]
    InvalidPredicate,
    #[error("unknown Feature-IR program key `{0}` (unknown labels rejected)")]
    UnknownProgramKey(String),
    #[error(
        "percentile requires output=value (session-distribution percentiles) \
         or aggregator=percentile (historical baselines)"
    )]
    UnusedPercentileKey,
}

/// Parse + validate a Derived Feature program at declaration time.
pub fn declare_program(
    raw: &Value,
    catalog: &DeskCatalog,
) -> Result<FeatureIrProgram, FeatureIrError> {
    let program = FeatureIrProgram::parse_value(raw)?;
    program.validate(catalog)?;
    Ok(program)
}

/// Evaluate `program` at `as_of_ms` over Journal Frames. Path is a label only.
pub fn evaluate(
    program: &FeatureIrProgram,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
) -> Result<FeatureIrValue, FeatureIrError> {
    program.validate(store.catalog)?;
    if frames_are_chronological(store.frames) {
        let (slice, truncated) = bound_eval_slice(store.frames, as_of_ms, store.window_truncated);
        return evaluate_on_window(
            program,
            FeatureIrStore {
                catalog: store.catalog,
                frames: slice,
                events: store.events,
                eval_root: store.eval_root,
                window_truncated: truncated,
            },
            as_of_ms,
            path,
        );
    }
    let (owned, truncated) = bound_eval_owned(store.frames, as_of_ms, store.window_truncated);
    evaluate_on_window(
        program,
        FeatureIrStore {
            catalog: store.catalog,
            frames: &owned,
            events: store.events,
            eval_root: store.eval_root,
            window_truncated: truncated,
        },
        as_of_ms,
        path,
    )
}

fn evaluate_on_window(
    program: &FeatureIrProgram,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
) -> Result<FeatureIrValue, FeatureIrError> {
    let truncated = store.window_truncated;
    let value = match program {
        FeatureIrProgram::CrossSymbolReferences { symbol, field } => {
            eval_cross_symbol(store, as_of_ms, symbol, field)?
        }
        FeatureIrProgram::SessionDistributionPercentiles {
            field,
            symbol,
            percentile,
            output,
        } => eval_session_percentile(
            store,
            as_of_ms,
            field,
            symbol.as_deref(),
            *percentile,
            *output,
        )?,
        FeatureIrProgram::DwellTimeSincePredicate { predicate, mode } => {
            eval_dwell(store, as_of_ms, predicate, *mode)?
        }
        FeatureIrProgram::EventSequences {
            first,
            then,
            within_ms,
        } => eval_event_sequence(store, as_of_ms, first, then, *within_ms),
        FeatureIrProgram::HistoricalBaselines {
            field,
            symbol,
            lookback_days,
            same_time_of_day,
            aggregator,
            percentile,
        } => {
            if truncated {
                // Lookback-N-days cannot fit in the bounded eval window; fail
                // closed rather than silently using a truncated day set.
                unavailable()
            } else {
                eval_historical_baseline(
                    store,
                    as_of_ms,
                    field,
                    symbol.as_deref(),
                    *lookback_days,
                    *same_time_of_day,
                    *aggregator,
                    *percentile,
                )?
            }
        }
    };
    Ok(FeatureIrValue {
        path,
        family: program.family(),
        as_of_ms,
        value: value.scalar,
        available: value.available,
        n: value.n,
    })
}

/// Live-shadow evaluation (same evaluator as historical).
///
/// Path is a label only. Callers supply the merged live window (bounded SQLite
/// load + pending drain, capped at [`FEATURE_IR_EVAL_MAX_FRAMES`]) or an
/// equivalent bounded historical load.
pub fn evaluate_live_shadow(
    program: &FeatureIrProgram,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
) -> Result<FeatureIrValue, FeatureIrError> {
    evaluate(program, store, as_of_ms, FeatureIrEvalPath::LiveShadow)
}

/// Historical evaluation over a caller-supplied Journal Frame slice (same evaluator as live-shadow).
///
/// Path is a label only. Runtime historical loads must be bounded (newest
/// [`FEATURE_IR_EVAL_MAX_FRAMES`] `+ 1` sentinel) — not an unbounded
/// `list_journal_frames` full-table read. Live pending drain size is independent.
pub fn evaluate_historical(
    program: &FeatureIrProgram,
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
) -> Result<FeatureIrValue, FeatureIrError> {
    evaluate(program, store, as_of_ms, FeatureIrEvalPath::Historical)
}

struct ScalarN {
    scalar: f64,
    available: bool,
    n: usize,
}

fn eval_cross_symbol(
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    symbol: &str,
    field_id: &str,
) -> Result<ScalarN, FeatureIrError> {
    let field = resolve_catalog_field(store.catalog, field_id)?;
    let Some(as_of_frame) = latest_frame(store.frames, store.eval_root, as_of_ms) else {
        return Ok(unavailable());
    };
    let Some(other) = frame_at_second(store.frames, symbol, as_of_frame.frame_second, as_of_ms)
    else {
        return Ok(unavailable());
    };
    match numeric_field(&other.payload, field) {
        Some(v) => Ok(ScalarN {
            scalar: v,
            available: true,
            n: 1,
        }),
        None => Ok(unavailable()),
    }
}

fn eval_session_percentile(
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    field_id: &str,
    symbol: Option<&str>,
    percentile: Option<f64>,
    output: PercentileOutput,
) -> Result<ScalarN, FeatureIrError> {
    let field = resolve_catalog_field(store.catalog, field_id)?;
    let root = symbol.unwrap_or(store.eval_root);
    let Some(as_of_frame) = latest_frame(store.frames, root, as_of_ms) else {
        return Ok(unavailable());
    };
    if session_window_exceeds_eval_bound(store, root, as_of_frame) {
        return Ok(unavailable());
    }
    let mut sample = Vec::new();
    for frame in visible_frames(store.frames, as_of_ms) {
        if frame.root_symbol != root {
            continue;
        }
        if !same_session(frame, as_of_frame) {
            continue;
        }
        if let Some(v) = numeric_field(&frame.payload, field) {
            sample.push(v);
        }
    }
    if sample.is_empty() {
        return Ok(unavailable());
    }
    let n = sample.len();
    let scalar = match output {
        PercentileOutput::Value => {
            let mut sorted = sample;
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            percentile_type7(&sorted, percentile.unwrap_or(50.0))
        }
        PercentileOutput::Rank => {
            let current = numeric_field(&as_of_frame.payload, field).unwrap_or(sample[n - 1]);
            percentile_rank(&sample, current)
        }
    };
    Ok(ScalarN {
        scalar,
        available: true,
        n,
    })
}

fn eval_dwell(
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    predicate: &FieldPredicate,
    mode: DwellMode,
) -> Result<ScalarN, FeatureIrError> {
    let field = resolve_catalog_field(store.catalog, &predicate.field)?;
    let root = predicate.symbol.as_deref().unwrap_or(store.eval_root);
    let Some(as_of_frame) = latest_frame(store.frames, root, as_of_ms) else {
        return Ok(unavailable());
    };
    if session_window_exceeds_eval_bound(store, root, as_of_frame) {
        return Ok(unavailable());
    }
    let mut session_frames: Vec<&FeatureIrFrame> = visible_frames(store.frames, as_of_ms)
        .filter(|f| f.root_symbol == root && same_session(f, as_of_frame))
        .collect();
    session_frames.sort_by(|a, b| {
        a.clock_ms
            .partial_cmp(&b.clock_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if session_frames.is_empty() {
        return Ok(unavailable());
    }
    let truths: Vec<(f64, bool)> = session_frames
        .iter()
        .map(|f| {
            let ok = numeric_field(&f.payload, field)
                .map(|v| predicate_holds(predicate.op, v, predicate.value))
                .unwrap_or(false);
            (f.clock_ms, ok)
        })
        .collect();
    let n = truths.len();
    let last_clock = as_of_frame.clock_ms.min(as_of_ms);
    let scalar = match mode {
        DwellMode::Dwell => dwell_ms(&truths, last_clock),
        DwellMode::TimeSince => match time_since_true_ms(&truths, last_clock) {
            Some(ms) => ms,
            None => return Ok(unavailable()),
        },
    };
    Ok(ScalarN {
        scalar,
        available: true,
        n,
    })
}

fn eval_event_sequence(
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    first: &EventSelector,
    then: &EventSelector,
    within_ms: f64,
) -> ScalarN {
    let mut firsts: Vec<f64> = store
        .events
        .iter()
        .filter(|e| e.timestamp_ms <= as_of_ms && event_matches(e, first, store.eval_root))
        .map(|e| e.timestamp_ms)
        .collect();
    firsts.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut seconds: Vec<f64> = store
        .events
        .iter()
        .filter(|e| e.timestamp_ms <= as_of_ms && event_matches(e, then, store.eval_root))
        .map(|e| e.timestamp_ms)
        .collect();
    seconds.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut matched = false;
    for a in &firsts {
        if seconds.iter().any(|b| *b > *a && (*b - *a) <= within_ms) {
            matched = true;
            break;
        }
    }
    ScalarN {
        scalar: if matched { 1.0 } else { 0.0 },
        available: true,
        n: firsts.len() + seconds.len(),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_historical_baseline(
    store: FeatureIrStore<'_>,
    as_of_ms: f64,
    field_id: &str,
    symbol: Option<&str>,
    lookback_days: u32,
    same_time_of_day: bool,
    aggregator: BaselineAggregator,
    percentile: Option<f64>,
) -> Result<ScalarN, FeatureIrError> {
    let field = resolve_catalog_field(store.catalog, field_id)?;
    let root = symbol.unwrap_or(store.eval_root);
    let Some(as_of_frame) = latest_frame(store.frames, root, as_of_ms) else {
        return Ok(unavailable());
    };
    let Some(current) = numeric_field(&as_of_frame.payload, field) else {
        return Ok(unavailable());
    };
    let as_of_minutes = et_minutes_from_timestamp(as_of_frame.clock_ms);
    let mut prior_days: Vec<String> = visible_frames(store.frames, as_of_ms)
        .filter(|f| {
            f.root_symbol == root
                && f.session_type == as_of_frame.session_type
                && f.trading_day < as_of_frame.trading_day
        })
        .map(|f| f.trading_day.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    prior_days.sort();
    let start = prior_days.len().saturating_sub(lookback_days as usize);
    let selected: BTreeSet<&str> = prior_days[start..].iter().map(String::as_str).collect();
    let mut per_day: Vec<(String, f64, f64)> = Vec::new();
    for frame in visible_frames(store.frames, as_of_ms) {
        if frame.root_symbol != root || !selected.contains(frame.trading_day.as_str()) {
            continue;
        }
        if frame.session_type != as_of_frame.session_type {
            continue;
        }
        if same_time_of_day {
            let Some(want) = as_of_minutes else {
                continue;
            };
            let Some(got) = et_minutes_from_timestamp(frame.clock_ms) else {
                continue;
            };
            if got != want {
                continue;
            }
        }
        let Some(v) = numeric_field(&frame.payload, field) else {
            continue;
        };
        per_day.push((frame.trading_day.clone(), frame.clock_ms, v));
    }
    per_day.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    let mut last_per_day: Vec<f64> = Vec::new();
    let mut last_day = String::new();
    for (day, _, v) in per_day {
        if day != last_day {
            last_per_day.push(v);
            last_day = day;
        } else if let Some(slot) = last_per_day.last_mut() {
            *slot = v;
        }
    }
    if last_per_day.is_empty() {
        return Ok(unavailable());
    }
    let n = last_per_day.len();
    let baseline = aggregate(&last_per_day, aggregator, percentile);
    Ok(ScalarN {
        scalar: current - baseline,
        available: true,
        n,
    })
}

fn aggregate(values: &[f64], aggregator: BaselineAggregator, percentile: Option<f64>) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    match aggregator {
        BaselineAggregator::Mean => values.iter().sum::<f64>() / values.len() as f64,
        BaselineAggregator::Median => percentile_type7(&sorted, 50.0),
        BaselineAggregator::Percentile => percentile_type7(&sorted, percentile.unwrap_or(50.0)),
    }
}

fn dwell_ms(truths: &[(f64, bool)], as_of_ms: f64) -> f64 {
    if !truths.last().is_some_and(|(_, t)| *t) {
        return 0.0;
    }
    let mut dwell_start = truths.last().map(|(c, _)| *c).unwrap_or(as_of_ms);
    for (clock, ok) in truths.iter().rev() {
        if *ok {
            dwell_start = *clock;
        } else {
            break;
        }
    }
    (as_of_ms - dwell_start).max(0.0)
}

fn time_since_true_ms(truths: &[(f64, bool)], as_of_ms: f64) -> Option<f64> {
    if truths.last().is_some_and(|(_, t)| *t) {
        return Some(0.0);
    }
    truths
        .iter()
        .rev()
        .find(|(_, ok)| *ok)
        .map(|(clock, _)| (as_of_ms - clock).max(0.0))
}

fn predicate_holds(op: PredicateOp, left: f64, right: Option<f64>) -> bool {
    let rhs = right.unwrap_or(0.0);
    match op {
        PredicateOp::Gt => left > rhs,
        PredicateOp::Gte => left >= rhs,
        PredicateOp::Lt => left < rhs,
        PredicateOp::Lte => left <= rhs,
        PredicateOp::Eq => (left - rhs).abs() <= f64::EPSILON,
        PredicateOp::Neq => (left - rhs).abs() > f64::EPSILON,
    }
}

fn event_matches(event: &FeatureIrEvent, selector: &EventSelector, eval_root: &str) -> bool {
    if event.event_type != selector.event_type {
        return false;
    }
    match selector.symbol.as_deref() {
        Some(want) => event.root_symbol == want,
        None => event.root_symbol == eval_root,
    }
}

fn unavailable() -> ScalarN {
    ScalarN {
        scalar: 0.0,
        available: false,
        n: 0,
    }
}

/// Merged Feature-IR eval window (SQLite history + in-memory pending).
#[derive(Debug, Clone, PartialEq)]
pub struct MergedEvalWindow {
    pub frames: Vec<FeatureIrFrame>,
    pub truncated: bool,
}

/// Concatenate history and pending; pending wins on `(frame_second, root_symbol)`.
///
/// Callers that previously chose pending *or* SQLite must use this instead.
/// Results are chronological. When `history_truncated` is set or the merge
/// exceeds `cap`, `truncated` is true and only the newest `cap` frames remain.
pub fn merge_eval_frames(
    history: Vec<FeatureIrFrame>,
    history_truncated: bool,
    pending: Vec<FeatureIrFrame>,
    cap: usize,
) -> MergedEvalWindow {
    let cap = cap.max(1);
    let mut by_identity: HashMap<(i64, String), FeatureIrFrame> =
        HashMap::with_capacity(history.len() + pending.len());
    for frame in history {
        by_identity.insert((frame.frame_second, frame.root_symbol.clone()), frame);
    }
    for frame in pending {
        by_identity.insert((frame.frame_second, frame.root_symbol.clone()), frame);
    }
    let mut frames: Vec<FeatureIrFrame> = by_identity.into_values().collect();
    frames.sort_by(|a, b| {
        a.clock_ms
            .partial_cmp(&b.clock_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.root_symbol.cmp(&b.root_symbol))
    });
    let truncated = history_truncated || frames.len() > cap;
    if frames.len() > cap {
        frames = frames[frames.len() - cap..].to_vec();
    }
    MergedEvalWindow { frames, truncated }
}

fn frames_are_chronological(frames: &[FeatureIrFrame]) -> bool {
    frames
        .windows(2)
        .all(|w| match w[0].clock_ms.partial_cmp(&w[1].clock_ms) {
            Some(std::cmp::Ordering::Less) => true,
            Some(std::cmp::Ordering::Equal) => w[0].root_symbol <= w[1].root_symbol,
            _ => false,
        })
}

/// Prefix `clock_ms <= as_of` then keep the newest `FEATURE_IR_EVAL_MAX_FRAMES`.
///
/// Callers must pass chronological frames. No payload clone.
fn bound_eval_slice(
    frames: &[FeatureIrFrame],
    as_of_ms: f64,
    caller_truncated: bool,
) -> (&[FeatureIrFrame], bool) {
    let end =
        frames.partition_point(|frame| frame.clock_ms.is_finite() && frame.clock_ms <= as_of_ms);
    let visible = &frames[..end];
    let truncated = caller_truncated || visible.len() > FEATURE_IR_EVAL_MAX_FRAMES;
    if visible.len() > FEATURE_IR_EVAL_MAX_FRAMES {
        let start = visible.len() - FEATURE_IR_EVAL_MAX_FRAMES;
        (&visible[start..], truncated)
    } else {
        (visible, truncated)
    }
}

/// Unsorted fallback: clone + sort, then apply the same cap. Avoid this path
/// on the query/runtime hot path (loaders already emit chronological frames).
fn bound_eval_owned(
    frames: &[FeatureIrFrame],
    as_of_ms: f64,
    caller_truncated: bool,
) -> (Vec<FeatureIrFrame>, bool) {
    let mut visible: Vec<&FeatureIrFrame> = visible_frames(frames, as_of_ms).collect();
    visible.sort_by(|a, b| {
        a.clock_ms
            .partial_cmp(&b.clock_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.root_symbol.cmp(&b.root_symbol))
    });
    let truncated = caller_truncated || visible.len() > FEATURE_IR_EVAL_MAX_FRAMES;
    if visible.len() > FEATURE_IR_EVAL_MAX_FRAMES {
        let start = visible.len() - FEATURE_IR_EVAL_MAX_FRAMES;
        visible = visible[start..].to_vec();
    }
    (visible.into_iter().cloned().collect(), truncated)
}

/// Session-percentile / dwell fail closed when the as-of session is cut by the cap.
///
/// Truncation is a mixed NQ+ES timeline. An ES-only second at the left edge
/// must still fail closed for an NQ as-of in the same session.
fn session_window_exceeds_eval_bound(
    store: FeatureIrStore<'_>,
    _root: &str,
    as_of_frame: &FeatureIrFrame,
) -> bool {
    if !store.window_truncated || store.frames.is_empty() {
        return false;
    }
    let Some(oldest) = store.frames.iter().min_by(|a, b| {
        a.clock_ms
            .partial_cmp(&b.clock_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) else {
        return false;
    };
    oldest.trading_day == as_of_frame.trading_day && oldest.session_type == as_of_frame.session_type
}

fn visible_frames(
    frames: &[FeatureIrFrame],
    as_of_ms: f64,
) -> impl Iterator<Item = &FeatureIrFrame> {
    frames
        .iter()
        .filter(move |f| f.clock_ms.is_finite() && f.clock_ms <= as_of_ms)
}

fn latest_frame<'a>(
    frames: &'a [FeatureIrFrame],
    root: &str,
    as_of_ms: f64,
) -> Option<&'a FeatureIrFrame> {
    visible_frames(frames, as_of_ms)
        .filter(|f| f.root_symbol == root)
        .max_by(|a, b| {
            a.clock_ms
                .partial_cmp(&b.clock_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn frame_at_second<'a>(
    frames: &'a [FeatureIrFrame],
    root: &str,
    frame_second: i64,
    as_of_ms: f64,
) -> Option<&'a FeatureIrFrame> {
    visible_frames(frames, as_of_ms)
        .find(|f| f.root_symbol == root && f.frame_second == frame_second)
}

fn same_session(a: &FeatureIrFrame, b: &FeatureIrFrame) -> bool {
    a.trading_day == b.trading_day && a.session_type == b.session_type
}

fn resolve_catalog_field<'a>(
    catalog: &'a DeskCatalog,
    field_id: &str,
) -> Result<&'a FieldDescriptor, FeatureIrError> {
    catalog
        .fields
        .iter()
        .find(|f| f.id == field_id || f.name == field_id)
        .ok_or_else(|| FeatureIrError::UnknownCatalogField(field_id.to_string()))
}

fn numeric_field(payload: &Value, field: &FieldDescriptor) -> Option<f64> {
    let val = payload.get(&field.name).cloned().or_else(|| {
        field
            .id
            .rsplit('.')
            .next()
            .and_then(|tail| payload.get(tail).cloned())
    })?;
    match val {
        Value::Number(n) => n.as_f64(),
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) => Some(0.0),
        _ => None,
    }
}

fn validate_root_symbol(symbol: &str) -> Result<(), FeatureIrError> {
    match symbol.trim() {
        "NQ" | "ES" => Ok(()),
        _ => Err(FeatureIrError::InvalidSymbol),
    }
}

fn classify_unfunded(raw: &str) -> Option<String> {
    let compact = compact_family_token(raw);
    if UNFUNDED_FAMILY_TOKENS.contains(&compact.as_str()) {
        Some(raw.trim().to_string())
    } else {
        None
    }
}

fn compact_family_token(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Hyndman/Fan type 7 (same convention as research distribution queries).
fn percentile_type7(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let clamped = p.clamp(0.0, 100.0) / 100.0;
    let h = clamped * (sorted.len() - 1) as f64;
    let lower_idx = h.floor() as usize;
    let upper_idx = h.ceil() as usize;
    if lower_idx == upper_idx {
        return sorted[lower_idx];
    }
    let weight = h - lower_idx as f64;
    sorted[lower_idx] + (sorted[upper_idx] - sorted[lower_idx]) * weight
}

/// Midrank percentile: 100 * (#{x < v} + 0.5 * #{x == v}) / n.
fn percentile_rank(sample: &[f64], current: f64) -> f64 {
    let n = sample.len() as f64;
    if n == 0.0 {
        return 0.0;
    }
    let mut lt = 0.0;
    let mut eq = 0.0;
    for v in sample {
        if *v < current {
            lt += 1.0;
        } else if (*v - current).abs() <= f64::EPSILON {
            eq += 1.0;
        }
    }
    100.0 * (lt + 0.5 * eq) / n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use serde_json::json;

    const LAST_PRICE: &str = "market.location_structure.lastPrice";

    fn frame(
        clock_ms: f64,
        root: &str,
        trading_day: &str,
        session_type: &str,
        last_price: f64,
    ) -> FeatureIrFrame {
        FeatureIrFrame {
            clock_ms,
            frame_second: (clock_ms / 1000.0).floor() as i64,
            root_symbol: root.into(),
            session_type: session_type.into(),
            trading_day: trading_day.into(),
            payload: json!({ "lastPrice": last_price, "sessionType": session_type }),
        }
    }

    fn catalog() -> DeskCatalog {
        build_catalog()
    }

    fn store<'a>(
        catalog: &'a DeskCatalog,
        frames: &'a [FeatureIrFrame],
        events: &'a [FeatureIrEvent],
    ) -> FeatureIrStore<'a> {
        FeatureIrStore {
            catalog,
            frames,
            events,
            eval_root: "NQ",
            window_truncated: false,
        }
    }

    #[test]
    fn funded_family_names_match_glossary_exactly() {
        assert_eq!(
            OperatorFamily::CrossSymbolReferences.glossary_name(),
            "Cross-symbol references"
        );
        assert_eq!(
            OperatorFamily::SessionDistributionPercentiles.glossary_name(),
            "Session-distribution percentiles"
        );
        assert_eq!(
            OperatorFamily::DwellTimeSincePredicate.glossary_name(),
            "Dwell / time-since-predicate"
        );
        assert_eq!(
            OperatorFamily::EventSequences.glossary_name(),
            "Event sequences"
        );
        assert_eq!(
            OperatorFamily::HistoricalBaselines.glossary_name(),
            "Historical baselines"
        );
        assert_eq!(FUNDED_OPERATOR_FAMILY_GLOSSARY.len(), 5);
        assert_eq!(FUNDED_OPERATOR_FAMILY_LABELS.len(), 5);
        assert_eq!(NEW_OPERATOR_FAMILY_GATE, "registry_change_proposal");
    }

    #[test]
    fn declare_accepts_each_funded_family() {
        let cat = catalog();
        let programs = [
            json!({"family":"crossSymbolReferences","symbol":"ES","field":LAST_PRICE}),
            json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE}),
            json!({"family":"dwellTimeSincePredicate","predicate":{"field":LAST_PRICE,"op":"gte","value":21000.0}}),
            json!({"family":"eventSequences","first":{"eventType":"ib_formed"},"then":{"eventType":"ib_reentry"},"withinMs":600000.0}),
            json!({"family":"historicalBaselines","field":LAST_PRICE,"lookbackDays":5,"sameTimeOfDay":true}),
        ];
        for raw in programs {
            let p = declare_program(&raw, &cat).expect("funded");
            p.validate(&cat).expect("validate");
        }
        let glossary = json!({
            "family": "Cross-symbol references",
            "symbol": "ES",
            "field": LAST_PRICE
        });
        assert_eq!(
            declare_program(&glossary, &cat).unwrap().family(),
            OperatorFamily::CrossSymbolReferences
        );
    }

    #[test]
    fn declare_rejects_surface_lookup_and_unknown_families() {
        let cat = catalog();
        let surface = json!({
            "family": "surfaceLookup",
            "field": LAST_PRICE,
            "strike": 21000.0
        });
        assert!(matches!(
            declare_program(&surface, &cat),
            Err(FeatureIrError::UnfundedOperatorFamily(s)) if s.contains("surface")
        ));
        let interp = json!({"family":"interpolation","field":LAST_PRICE});
        assert!(matches!(
            declare_program(&interp, &cat),
            Err(FeatureIrError::UnfundedOperatorFamily(_))
        ));
        let unknown = json!({"family":"kalmanFilter","field":LAST_PRICE});
        assert!(matches!(
            declare_program(&unknown, &cat),
            Err(FeatureIrError::UnfundedOperatorFamily(_))
        ));
        let nested = json!({
            "family": "crossSymbolReferences",
            "symbol": "ES",
            "field": LAST_PRICE,
            "surfaceLookup": {"strike": 1}
        });
        assert!(matches!(
            declare_program(&nested, &cat),
            Err(FeatureIrError::UnfundedOperatorFamily(_))
        ));
    }

    #[test]
    fn declare_rejects_control_flow_keys() {
        let cat = catalog();
        let looping = json!({
            "family": "sessionDistributionPercentiles",
            "field": LAST_PRICE,
            "loop": {"n": 10}
        });
        assert!(matches!(
            declare_program(&looping, &cat),
            Err(FeatureIrError::ControlFlowRejected(s)) if s == "loop"
        ));
    }

    #[test]
    fn declare_rejects_unknown_catalog_fields() {
        let cat = catalog();
        let raw = json!({
            "family": "crossSymbolReferences",
            "symbol": "ES",
            "field": "market.flow.notARealField"
        });
        assert!(matches!(
            declare_program(&raw, &cat),
            Err(FeatureIrError::UnknownCatalogField(_))
        ));
    }

    #[test]
    fn live_shadow_equals_historical_for_funded_families() {
        let cat = catalog();
        // 2026-03-03 10:00 ET = 1_741_016_400_000, plus prior days at the same minute.
        let t0 = 1_741_016_400_000.0;
        let day_ms = 86_400_000.0;
        let mut frames = Vec::new();
        for day in 0..3 {
            let base = t0 - (2 - day) as f64 * day_ms;
            let trading_day = match day {
                0 => "2026-03-01",
                1 => "2026-03-02",
                _ => "2026-03-03",
            };
            for i in 0..5 {
                let clock = base + i as f64 * 1000.0;
                let nq_px = 21_000.0 + day as f64 * 10.0 + i as f64;
                let es_px = 5_000.0 + day as f64 + i as f64 * 0.25;
                frames.push(frame(clock, "NQ", trading_day, "RTH", nq_px));
                frames.push(frame(clock + 1.0, "ES", trading_day, "RTH", es_px));
                // ES shares the same unix second for co-recording.
                frames.last_mut().unwrap().frame_second = (clock / 1000.0).floor() as i64;
                frames.last_mut().unwrap().clock_ms = clock;
            }
        }
        let events = vec![
            FeatureIrEvent {
                timestamp_ms: t0 + 1000.0,
                event_type: "ib_formed".into(),
                root_symbol: "NQ".into(),
            },
            FeatureIrEvent {
                timestamp_ms: t0 + 2500.0,
                event_type: "ib_reentry".into(),
                root_symbol: "NQ".into(),
            },
        ];
        let as_of = t0 + 4000.0;
        let live_store = store(&cat, &frames, &events);
        let programs = [
            FeatureIrProgram::parse_value(&json!({
                "family":"crossSymbolReferences","symbol":"ES","field":LAST_PRICE
            }))
            .unwrap(),
            FeatureIrProgram::parse_value(&json!({
                "family":"sessionDistributionPercentiles","field":LAST_PRICE
            }))
            .unwrap(),
            FeatureIrProgram::parse_value(&json!({
                "family":"dwellTimeSincePredicate",
                "predicate":{"field":LAST_PRICE,"op":"gte","value":21022.0}
            }))
            .unwrap(),
            FeatureIrProgram::parse_value(&json!({
                "family":"eventSequences",
                "first":{"eventType":"ib_formed"},
                "then":{"eventType":"ib_reentry"},
                "withinMs":2000.0
            }))
            .unwrap(),
            FeatureIrProgram::parse_value(&json!({
                "family":"historicalBaselines",
                "field":LAST_PRICE,
                "lookbackDays":2,
                "sameTimeOfDay":true
            }))
            .unwrap(),
        ];
        for program in programs {
            let shadow = evaluate_live_shadow(&program, live_store, as_of).expect("shadow");
            let hist = evaluate_historical(&program, live_store, as_of).expect("hist");
            assert_eq!(shadow.value, hist.value, "{:?}", program.family());
            assert_eq!(shadow.available, hist.available);
            assert_eq!(shadow.n, hist.n);
            assert_eq!(shadow.path, FeatureIrEvalPath::LiveShadow);
            assert_eq!(hist.path, FeatureIrEvalPath::Historical);
            assert!(
                shadow.available,
                "{:?} should be available",
                program.family()
            );
        }
    }

    #[test]
    fn cross_symbol_reads_co_recorded_es_last_price() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![frame(t0, "NQ", "2026-03-03", "RTH", 21000.0), {
            let mut es = frame(t0, "ES", "2026-03-03", "RTH", 5001.25);
            es.frame_second = (t0 / 1000.0).floor() as i64;
            es
        }];
        let program = declare_program(
            &json!({"family":"crossSymbolReferences","symbol":"ES","field":LAST_PRICE}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let out = evaluate_historical(&program, store(&cat, &frames, &events), t0).unwrap();
        assert!(out.available);
        assert_eq!(out.value, 5001.25);
    }

    #[test]
    fn session_percentile_rank_is_midrank() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames: Vec<_> = (0..5)
            .map(|i| {
                frame(
                    t0 + i as f64 * 1000.0,
                    "NQ",
                    "2026-03-03",
                    "RTH",
                    100.0 + i as f64,
                )
            })
            .collect();
        let program = declare_program(
            &json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let out =
            evaluate_historical(&program, store(&cat, &frames, &events), t0 + 4000.0).unwrap();
        // values 100..104, current 104, midrank = 100*(4+0.5)/5 = 90
        assert!((out.value - 90.0).abs() < 1e-9);
        assert_eq!(out.n, 5);
    }

    #[test]
    fn event_sequence_a_then_b_within_t() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![frame(t0 + 3000.0, "NQ", "2026-03-03", "RTH", 21000.0)];
        let events = vec![
            FeatureIrEvent {
                timestamp_ms: t0 + 1000.0,
                event_type: "ib_formed".into(),
                root_symbol: "NQ".into(),
            },
            FeatureIrEvent {
                timestamp_ms: t0 + 2500.0,
                event_type: "ib_reentry".into(),
                root_symbol: "NQ".into(),
            },
        ];
        let hit = declare_program(
            &json!({
                "family":"eventSequences",
                "first":{"eventType":"ib_formed"},
                "then":{"eventType":"ib_reentry"},
                "withinMs":2000.0
            }),
            &cat,
        )
        .unwrap();
        let miss = declare_program(
            &json!({
                "family":"eventSequences",
                "first":{"eventType":"ib_formed"},
                "then":{"eventType":"ib_reentry"},
                "withinMs":1000.0
            }),
            &cat,
        )
        .unwrap();
        let s = store(&cat, &frames, &events);
        assert_eq!(
            evaluate_historical(&hit, s, t0 + 3000.0).unwrap().value,
            1.0
        );
        assert_eq!(
            evaluate_historical(&miss, s, t0 + 3000.0).unwrap().value,
            0.0
        );
    }

    #[test]
    fn declare_rejects_unknown_program_keys() {
        let cat = catalog();
        let wrong_case = json!({
            "family": "historicalBaselines",
            "field": LAST_PRICE,
            "lookbackDays": 5,
            "sametimeofday": false
        });
        assert!(matches!(
            declare_program(&wrong_case, &cat),
            Err(FeatureIrError::UnknownProgramKey(k)) if k == "sametimeofday"
        ));
        let extra = json!({
            "family": "crossSymbolReferences",
            "symbol": "ES",
            "field": LAST_PRICE,
            "lookbackDays": 5
        });
        assert!(matches!(
            declare_program(&extra, &cat),
            Err(FeatureIrError::UnknownProgramKey(k)) if k == "lookbackDays"
        ));
    }

    #[test]
    fn declare_rejects_percentile_without_matching_output() {
        let cat = catalog();
        let rank_with_p = json!({
            "family": "sessionDistributionPercentiles",
            "field": LAST_PRICE,
            "percentile": 95.0
        });
        assert!(matches!(
            declare_program(&rank_with_p, &cat),
            Err(FeatureIrError::UnusedPercentileKey)
        ));
        let value_ok = json!({
            "family": "sessionDistributionPercentiles",
            "field": LAST_PRICE,
            "percentile": 95.0,
            "output": "value"
        });
        declare_program(&value_ok, &cat).expect("percentile + output=value");
    }

    #[test]
    fn time_since_never_true_is_unavailable() {
        let cat = catalog();
        let t0 = 1_772_636_400_000.0;
        let frames: Vec<_> = (0..5)
            .map(|i| {
                frame(
                    t0 + i as f64 * 1000.0,
                    "NQ",
                    "2026-03-04",
                    "RTH",
                    21_000.0 + i as f64,
                )
            })
            .collect();
        let program = declare_program(
            &json!({
                "family": "dwellTimeSincePredicate",
                "mode": "timeSince",
                "predicate": {"field": LAST_PRICE, "op": "lt", "value": 0.0}
            }),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let out =
            evaluate_historical(&program, store(&cat, &frames, &events), t0 + 4000.0).unwrap();
        assert!(
            !out.available,
            "never-true timeSince must not emit an epoch"
        );
        assert_eq!(out.value, 0.0);
        assert_eq!(out.n, 0);
        assert!(
            out.value < 86_400_000.0,
            "must not leak wall-clock epoch as a duration"
        );
    }

    #[test]
    fn time_since_after_last_true_is_elapsed_ms() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![
            frame(t0, "NQ", "2026-03-03", "RTH", 10.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 20.0),
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 5.0),
            frame(t0 + 3000.0, "NQ", "2026-03-03", "RTH", 6.0),
        ];
        let program = declare_program(
            &json!({
                "family": "dwellTimeSincePredicate",
                "mode": "timeSince",
                "predicate": {"field": LAST_PRICE, "op": "gte", "value": 15.0}
            }),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let out =
            evaluate_historical(&program, store(&cat, &frames, &events), t0 + 3000.0).unwrap();
        assert!(out.available);
        assert!((out.value - 2000.0).abs() < 1e-9);
    }

    #[test]
    fn session_percentile_fails_closed_when_session_exceeds_eval_cap() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![
            frame(t0, "NQ", "2026-03-03", "RTH", 21_000.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_001.0),
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 21_002.0),
        ];
        let program = declare_program(
            &json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let mut live_store = store(&cat, &frames, &events);
        live_store.window_truncated = true;
        let mut hist_store = store(&cat, &frames, &events);
        hist_store.window_truncated = true;
        let as_of = t0 + 2000.0;
        let live = evaluate_live_shadow(&program, live_store, as_of).unwrap();
        let hist = evaluate_historical(&program, hist_store, as_of).unwrap();
        assert_eq!(live.available, hist.available);
        assert_eq!(live.n, hist.n);
        assert!(
            !live.available,
            "session percentile must not silently use a truncated eval window"
        );
        assert_eq!(live.n, 0);
    }

    #[test]
    fn session_percentile_fails_closed_when_oldest_truncated_frame_is_other_root() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![
            frame(t0, "ES", "2026-03-03", "RTH", 5_000.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_000.0),
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 21_001.0),
        ];
        let program = declare_program(
            &json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let mut ir_store = store(&cat, &frames, &events);
        ir_store.window_truncated = true;
        let out = evaluate_historical(&program, ir_store, t0 + 2000.0).unwrap();
        assert!(
            !out.available,
            "an ES-only truncation edge must fail closed for NQ in the same session"
        );
    }

    #[test]
    fn session_percentile_available_when_truncated_edge_is_prior_session() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![
            frame(t0, "NQ", "2026-03-02", "RTH", 21_000.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_001.0),
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 21_002.0),
        ];
        let program = declare_program(
            &json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let mut ir_store = store(&cat, &frames, &events);
        ir_store.window_truncated = true;
        let out = evaluate_historical(&program, ir_store, t0 + 2000.0).unwrap();
        assert!(
            out.available,
            "current session fully inside the window must still evaluate"
        );
        assert_eq!(out.n, 2);
    }

    #[test]
    fn merge_eval_frames_pending_wins_on_identity_and_concatenates() {
        let t0 = 1_700_000_000_000.0;
        let history = vec![
            frame(t0, "NQ", "2026-03-03", "RTH", 21_000.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_001.0),
        ];
        let mut pending_dup = frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_099.0);
        pending_dup.payload = json!({ "lastPrice": 21_099.0, "sessionType": "RTH" });
        let pending = vec![
            pending_dup,
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 21_002.0),
        ];
        let merged = merge_eval_frames(history, false, pending, FEATURE_IR_EVAL_MAX_FRAMES);
        assert!(!merged.truncated);
        assert_eq!(merged.frames.len(), 3);
        assert_eq!(
            merged.frames[1].payload["lastPrice"].as_f64(),
            Some(21_099.0),
            "pending must replace the same (frame_second, root) history row"
        );
        assert_eq!(
            merged.frames[2].payload["lastPrice"].as_f64(),
            Some(21_002.0)
        );
    }

    #[test]
    fn reverse_order_frames_still_evaluate_via_owned_bound() {
        let cat = catalog();
        let t0 = 1_700_000_000_000.0;
        let frames = vec![
            frame(t0 + 2000.0, "NQ", "2026-03-03", "RTH", 21_002.0),
            frame(t0 + 1000.0, "NQ", "2026-03-03", "RTH", 21_001.0),
            frame(t0, "NQ", "2026-03-03", "RTH", 21_000.0),
        ];
        assert!(!frames_are_chronological(&frames));
        let program = declare_program(
            &json!({"family":"sessionDistributionPercentiles","field":LAST_PRICE,"output":"rank"}),
            &cat,
        )
        .unwrap();
        let events: Vec<FeatureIrEvent> = Vec::new();
        let out =
            evaluate_historical(&program, store(&cat, &frames, &events), t0 + 2000.0).unwrap();
        assert!(out.available);
        assert_eq!(out.n, 3);
        assert!((out.value - 5.0 / 6.0 * 100.0).abs() < 1e-9);
    }
}
