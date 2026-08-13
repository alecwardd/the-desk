//! Typed Positioning records — Levels-Only Record write path (SIL-P-VS-a / #15).
//!
//! Manual entry is first-class (completeness `levels_only`), not a degraded or
//! fallback kind. The same schema is what a later capture adapter / Vs3dProvider
//! will write; this ticket only accepts Levels-Only Records. Reads ride
//! `get_state`. Trust Ceiling stays L3; this path has no order authority.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::envelope::{DomainProvenance, ProvenanceSource, StateEnvelope};
use super::types::DeskCatalog;
use crate::trading_day_from_timestamp_ms;

/// Catalog id for a Levels-Only Record (first-class Positioning kind).
pub const LEVELS_ONLY_RECORD_KIND: &str = "levels_only";

/// Provenance source stamp for the manual / as-of write path.
pub const MANUAL_PROVENANCE_SOURCE: &str = "manual";

/// Wall role labels used by Desk-derived levels.
const WALL_ROLES: &[&str] = &["call_wall", "put_wall"];

/// Desk-derived MM levels carried by Slice and Levels-Only Record (ADR-025).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedLevels {
    pub flip: f64,
    pub walls: Vec<PositioningWall>,
    pub balance: f64,
    pub upside_test: f64,
    pub downside_test: f64,
}

/// One Desk-derived wall (strike + role).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningWall {
    pub strike: f64,
    pub role: String,
}

/// Mid-day re-mark on a Positioning card (same shape as the exemplar corpus).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MidDayRead {
    pub as_of: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Durable Positioning record (Levels-Only today; Slice / grid later).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningRecord {
    pub id: String,
    pub record_kind: String,
    pub completeness: String,
    pub trading_day: String,
    pub captured_at_ms: f64,
    pub as_of_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_time_ms: Option<f64>,
    pub freshness_ok: bool,
    pub derived_levels: DerivedLevels,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mid_day_reads: Vec<MidDayRead>,
    pub provenance: PositioningRecordProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Provenance carried on the durable record (never a vendor pretence).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningRecordProvenance {
    pub source: String,
    pub first_class: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Input accepted by the `positioning_entry` workflow verb.
#[derive(Debug, Clone, Default)]
pub struct PositioningEntryInput {
    pub id: Option<String>,
    pub record_kind: Option<String>,
    pub completeness: Option<String>,
    pub trading_day: Option<String>,
    pub captured_at_ms: Option<f64>,
    pub as_of_ms: Option<f64>,
    pub data_time_ms: Option<f64>,
    pub derived_levels: Option<DerivedLevels>,
    pub transitions: Vec<String>,
    pub mid_day_reads: Vec<MidDayRead>,
    pub note: Option<String>,
    pub vendor: Option<String>,
    pub now_ms: f64,
}

/// Positioning domain slice for a StateEnvelope (unprefixed; not symbol-scoped).
#[derive(Debug, Clone, PartialEq)]
pub struct PositioningStateSlice {
    pub values: BTreeMap<String, Value>,
    pub provenance: DomainProvenance,
    pub degraded: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PositioningError {
    #[error("{0}")]
    Invalid(String),
}

/// Accept a Levels-Only Record into the canonical Positioning schema.
///
/// Slice / grid / by-strike writes are rejected here (later capture adapter).
/// Vendor stamps are rejected so a manual card is never presented as live
/// vendor data.
pub fn accept_levels_only_entry(
    input: PositioningEntryInput,
) -> Result<PositioningRecord, PositioningError> {
    let record_kind = normalize_kind(input.record_kind.as_deref())?;
    if record_kind != LEVELS_ONLY_RECORD_KIND {
        return Err(PositioningError::Invalid(format!(
            "positioning_entry accepts Levels-Only Records only (recordKind=`{LEVELS_ONLY_RECORD_KIND}`); \
             `{record_kind}` is a later capture-adapter / Vs3dProvider path"
        )));
    }
    let completeness = match input
        .completeness
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        None => LEVELS_ONLY_RECORD_KIND.to_string(),
        Some(raw) => {
            let c = raw.to_ascii_lowercase();
            if c != LEVELS_ONLY_RECORD_KIND {
                return Err(PositioningError::Invalid(format!(
                    "completeness `{raw}` must be `{LEVELS_ONLY_RECORD_KIND}` for a Levels-Only Record \
                     (first-class; not a fallback kind)"
                )));
            }
            LEVELS_ONLY_RECORD_KIND.to_string()
        }
    };

    reject_vendor_pretence(input.vendor.as_deref(), input.data_time_ms)?;

    let captured_at_ms =
        required_timestamp(input.captured_at_ms.or(Some(input.now_ms)), "capturedAt")?;
    let as_of_ms = required_timestamp(input.as_of_ms.or(Some(captured_at_ms)), "asOf")?;
    let trading_day = match input
        .trading_day
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(day) => validate_trading_day(day)?,
        None => trading_day_from_timestamp_ms(as_of_ms),
    };

    let derived_levels = input.derived_levels.ok_or_else(|| {
        PositioningError::Invalid(
            "positioning_entry requires derivedLevels (flip, walls, balance, upsideTest, downsideTest)"
                .into(),
        )
    })?;
    validate_derived_levels(&derived_levels)?;
    reject_banned_copy(input.note.as_deref())?;
    for read in &input.mid_day_reads {
        reject_banned_copy(read.note.as_deref())?;
    }

    let note = input
        .note
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let provenance_note = note.clone().unwrap_or_else(|| {
        "Levels-Only Record from your annotated sessions / your methodology (manual/as-of). \
         Completeness levels_only is first-class. Not vendor scrape."
            .into()
    });

    Ok(PositioningRecord {
        id: input
            .id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        record_kind: LEVELS_ONLY_RECORD_KIND.into(),
        completeness,
        trading_day,
        captured_at_ms,
        as_of_ms,
        data_time_ms: None,
        freshness_ok: true,
        derived_levels,
        transitions: input.transitions,
        mid_day_reads: input.mid_day_reads,
        provenance: PositioningRecordProvenance {
            source: MANUAL_PROVENANCE_SOURCE.into(),
            first_class: true,
            vendor: None,
            note: Some(provenance_note),
        },
        note,
    })
}

/// Fail-closed Positioning slice for `get_state`.
///
/// `live_trading_day` is set on the live path so a prior-day card never looks
/// like today's vendor data. Historical `as_of` reads pass `None` and use the
/// record's explicit manual as-of.
pub fn positioning_state_slice(
    record: Option<&PositioningRecord>,
    live_trading_day: Option<&str>,
) -> PositioningStateSlice {
    match record {
        None => empty_positioning_slice(),
        Some(record) => slice_from_record(record, live_trading_day),
    }
}

/// Overlay a Positioning slice onto a StateEnvelope that already lists the
/// domain (unprefixed). No-op when Positioning was not selected.
pub fn apply_positioning_slice(
    envelope: &mut StateEnvelope,
    catalog: &DeskCatalog,
    slice: &PositioningStateSlice,
    field_filter: Option<&[String]>,
) {
    if !envelope.provenance.contains_key("positioning") {
        return;
    }
    envelope
        .provenance
        .insert("positioning".into(), slice.provenance.clone());
    envelope
        .degraded
        .insert("positioning".into(), slice.degraded);
    envelope
        .values
        .retain(|key, _| !key.starts_with("positioning."));
    let allow: Option<std::collections::BTreeSet<&str>> =
        field_filter.map(|f| f.iter().map(|s| s.as_str()).collect());
    for field in catalog
        .fields
        .iter()
        .filter(|f| f.domain_id == "positioning")
    {
        if !envelope.resolution.allows(field.cost_hint) {
            continue;
        }
        if let Some(ref allow) = allow {
            if !allow.contains(field.id.as_str()) && !allow.contains(field.name.as_str()) {
                continue;
            }
        }
        if let Some(val) = slice.values.get(&field.id) {
            envelope.values.insert(field.id.clone(), val.clone());
        }
    }
}

/// Fail-closed empty Positioning domain (no record, no vendor pretence).
pub fn empty_positioning_slice() -> PositioningStateSlice {
    let mut values = BTreeMap::new();
    values.insert("positioning.freshnessOk".into(), Value::Bool(false));
    PositioningStateSlice {
        values,
        provenance: DomainProvenance {
            source: ProvenanceSource::Manual,
            data_time: None,
            vendor: None,
            note: Some(
                "No Positioning record. Levels-Only Record path is first-class — write via \
                 positioning_entry (manual/as-of). Not live vendor data."
                    .into(),
            ),
        },
        degraded: true,
    }
}

fn slice_from_record(
    record: &PositioningRecord,
    live_trading_day: Option<&str>,
) -> PositioningStateSlice {
    let (freshness_ok, degraded, note) = evaluate_freshness(record, live_trading_day);
    let mut values = BTreeMap::new();
    values.insert(
        "positioning.recordKind".into(),
        Value::String(record.record_kind.clone()),
    );
    values.insert(
        "positioning.completeness".into(),
        Value::String(record.completeness.clone()),
    );
    values.insert(
        "positioning.capturedAt".into(),
        json_ms(record.captured_at_ms),
    );
    values.insert("positioning.asOf".into(), json_ms(record.as_of_ms));
    values.insert(
        "positioning.dataTime".into(),
        record.data_time_ms.map(json_ms).unwrap_or(Value::Null),
    );
    values.insert("positioning.freshnessOk".into(), Value::Bool(freshness_ok));
    values.insert(
        "positioning.derivedLevels".into(),
        serde_json::to_value(&record.derived_levels).unwrap_or(Value::Null),
    );
    PositioningStateSlice {
        values,
        provenance: DomainProvenance {
            source: ProvenanceSource::Manual,
            data_time: Some(record.as_of_ms),
            vendor: None,
            note: Some(note),
        },
        degraded,
    }
}

/// Recompute fail-closed freshness at read time (never silent live-vendor).
pub fn evaluate_freshness(
    record: &PositioningRecord,
    live_trading_day: Option<&str>,
) -> (bool, bool, String) {
    if !timestamp_ok(record.captured_at_ms) || !timestamp_ok(record.as_of_ms) {
        return (
            false,
            true,
            "Levels-Only Record missing capturedAt/as-of; freshness fails closed. Not live vendor data."
                .into(),
        );
    }
    if record.provenance.vendor.is_some()
        || record.provenance.source.eq_ignore_ascii_case("provider")
        || is_vendor_label(record.provenance.vendor.as_deref())
        || is_vendor_label(Some(&record.provenance.source))
    {
        return (
            false,
            true,
            "Positioning freshness fails closed: vendor/provider stamp is not valid on the \
             Levels-Only Record path. Not live vendor data."
                .into(),
        );
    }
    if let Some(day) = live_trading_day {
        if record.trading_day != day {
            return (
                false,
                true,
                format!(
                    "Levels-Only Record as-of {} from your annotated sessions is first-class \
                     Positioning; freshnessOk=false (not live vendor data).",
                    record.trading_day
                ),
            );
        }
    }
    (
        true,
        false,
        "Levels-Only Record from your annotated sessions / your methodology (manual/as-of). \
         Completeness levels_only is first-class. Not vendor scrape."
            .into(),
    )
}

fn json_ms(ms: f64) -> Value {
    Value::Number(serde_json::Number::from_f64(ms).unwrap_or_else(|| serde_json::Number::from(0)))
}

fn normalize_kind(raw: Option<&str>) -> Result<String, PositioningError> {
    let kind = raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(LEVELS_ONLY_RECORD_KIND);
    match kind.to_ascii_lowercase().as_str() {
        "levels_only" | "levels-only" | "levelsonly" => Ok(LEVELS_ONLY_RECORD_KIND.into()),
        "slice" => Ok("slice".into()),
        "position_grid" | "grid" => Ok("position_grid".into()),
        "positions_by_strike" | "by-strike" | "by_strike" => Ok("positions_by_strike".into()),
        other => Err(PositioningError::Invalid(format!(
            "unknown Positioning recordKind `{other}`"
        ))),
    }
}

fn required_timestamp(raw: Option<f64>, name: &str) -> Result<f64, PositioningError> {
    match raw {
        Some(ms) if timestamp_ok(ms) => Ok(ms),
        _ => Err(PositioningError::Invalid(format!(
            "{name} must be a positive finite epoch-milliseconds value (explicit manual/as-of)"
        ))),
    }
}

fn timestamp_ok(ms: f64) -> bool {
    ms.is_finite() && ms > 0.0
}

fn validate_trading_day(day: &str) -> Result<String, PositioningError> {
    let bytes = day.as_bytes();
    let ok = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes.iter().enumerate().all(|(i, b)| {
            if i == 4 || i == 7 {
                true
            } else {
                b.is_ascii_digit()
            }
        });
    if !ok {
        return Err(PositioningError::Invalid(format!(
            "tradingDay must be YYYY-MM-DD (got `{day}`)"
        )));
    }
    Ok(day.to_string())
}

fn validate_derived_levels(levels: &DerivedLevels) -> Result<(), PositioningError> {
    for (name, value) in [
        ("flip", levels.flip),
        ("balance", levels.balance),
        ("upsideTest", levels.upside_test),
        ("downsideTest", levels.downside_test),
    ] {
        if !value.is_finite() {
            return Err(PositioningError::Invalid(format!(
                "derivedLevels.{name} must be a finite price"
            )));
        }
    }
    for wall in &levels.walls {
        if !wall.strike.is_finite() {
            return Err(PositioningError::Invalid(
                "derivedLevels.walls[].strike must be a finite price".into(),
            ));
        }
        let role = wall.role.trim();
        if role.is_empty() {
            return Err(PositioningError::Invalid(
                "derivedLevels.walls[].role is required".into(),
            ));
        }
        if !WALL_ROLES.contains(&role) {
            return Err(PositioningError::Invalid(format!(
                "derivedLevels.walls[].role `{role}` must be call_wall or put_wall"
            )));
        }
    }
    Ok(())
}

fn reject_vendor_pretence(
    vendor: Option<&str>,
    data_time_ms: Option<f64>,
) -> Result<(), PositioningError> {
    if is_vendor_label(vendor) {
        return Err(PositioningError::Invalid(
            "Levels-Only Records are the manual/as-of path; do not stamp a vendor \
             (VolSignals / Vs3dProvider is a later ticket)"
                .into(),
        ));
    }
    if let Some(ms) = data_time_ms {
        if timestamp_ok(ms) {
            return Err(PositioningError::Invalid(
                "Levels-Only Records must not carry vendor dataTime; use capturedAt/asOf \
                 for explicit manual as-of"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn is_vendor_label(raw: Option<&str>) -> bool {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    let l = s.to_ascii_lowercase();
    matches!(
        l.as_str(),
        "volsignals"
            | "vs3d"
            | "vs3dprovider"
            | "provider"
            | "vendor"
            | "live_vendor"
            | "convexvalue"
    ) || l.contains("volsignal")
        || l.contains("vs3d")
}

fn reject_banned_copy(text: Option<&str>) -> Result<(), PositioningError> {
    let Some(text) = text else {
        return Ok(());
    };
    let l = text.to_lowercase();
    for banned in [
        "you should buy",
        "you should sell",
        "i recommend",
        "this is a good trade",
        "fallback",
        "second-class",
        "second class",
        "partial record",
        "degraded record",
        "degraded mode",
        "degraded path",
        "fallback path",
        "fallback record",
    ] {
        if l.contains(banned) {
            return Err(PositioningError::Invalid(format!(
                "Positioning copy must not use `{banned}` — Levels-Only Records are first-class; \
                 coaching frames as your annotated sessions / your methodology say…"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::envelope::StateResolution;
    use crate::catalog::{build_catalog, build_state_envelope, StateReadRequest, TrustCeiling};

    fn sample_levels() -> DerivedLevels {
        DerivedLevels {
            flip: 5750.0,
            walls: vec![
                PositioningWall {
                    strike: 5800.0,
                    role: "call_wall".into(),
                },
                PositioningWall {
                    strike: 5700.0,
                    role: "put_wall".into(),
                },
            ],
            balance: 5745.0,
            upside_test: 5825.0,
            downside_test: 5680.0,
        }
    }

    fn sample_input(now_ms: f64) -> PositioningEntryInput {
        PositioningEntryInput {
            record_kind: Some(LEVELS_ONLY_RECORD_KIND.into()),
            completeness: Some(LEVELS_ONLY_RECORD_KIND.into()),
            trading_day: Some("2026-02-18".into()),
            captured_at_ms: Some(now_ms),
            as_of_ms: Some(now_ms),
            derived_levels: Some(sample_levels()),
            now_ms,
            ..Default::default()
        }
    }

    #[test]
    fn levels_only_entry_is_first_class_manual_not_vendor() {
        let rec = accept_levels_only_entry(sample_input(1_771_372_800_000.0)).expect("accept");
        assert_eq!(rec.record_kind, LEVELS_ONLY_RECORD_KIND);
        assert_eq!(rec.completeness, LEVELS_ONLY_RECORD_KIND);
        assert!(rec.provenance.first_class);
        assert_eq!(rec.provenance.source, MANUAL_PROVENANCE_SOURCE);
        assert!(rec.provenance.vendor.is_none());
        assert!(rec.data_time_ms.is_none());
        let slice = positioning_state_slice(Some(&rec), Some("2026-02-18"));
        assert!(!slice.degraded, "fresh Levels-Only must not be degraded");
        assert_eq!(slice.provenance.source, ProvenanceSource::Manual);
        assert!(slice.provenance.vendor.is_none());
        assert_eq!(slice.values["positioning.freshnessOk"], Value::Bool(true));
        assert_eq!(
            slice.values["positioning.completeness"],
            Value::String(LEVELS_ONLY_RECORD_KIND.into())
        );
        let note = slice
            .provenance
            .note
            .as_deref()
            .unwrap_or("")
            .to_lowercase();
        assert!(!note.contains("fallback"));
        assert!(!note.contains("second-class"));
        assert!(!note.contains("volsignal"));
        assert_ne!(slice.provenance.source, ProvenanceSource::Provider);
    }

    #[test]
    fn missing_freshness_fails_closed_and_is_never_vendor() {
        let mut rec = accept_levels_only_entry(sample_input(1_771_372_800_000.0)).unwrap();
        rec.captured_at_ms = 0.0;
        rec.as_of_ms = 0.0;
        let slice = positioning_state_slice(Some(&rec), Some("2026-02-18"));
        assert!(slice.degraded);
        assert_eq!(slice.values["positioning.freshnessOk"], Value::Bool(false));
        assert_eq!(slice.provenance.source, ProvenanceSource::Manual);
        assert!(slice.provenance.vendor.is_none());
        assert_eq!(
            slice.values["positioning.completeness"],
            Value::String(LEVELS_ONLY_RECORD_KIND.into())
        );
    }

    #[test]
    fn prior_day_card_is_stale_on_live_path_not_vendor() {
        let rec = accept_levels_only_entry(sample_input(1_771_372_800_000.0)).unwrap();
        let slice = positioning_state_slice(Some(&rec), Some("2026-08-13"));
        assert!(slice.degraded);
        assert_eq!(slice.values["positioning.freshnessOk"], Value::Bool(false));
        assert_eq!(slice.provenance.source, ProvenanceSource::Manual);
        assert!(slice
            .provenance
            .note
            .as_deref()
            .unwrap()
            .contains("first-class"));
        assert!(!slice
            .provenance
            .note
            .as_deref()
            .unwrap()
            .to_lowercase()
            .contains("fallback"));
    }

    #[test]
    fn as_of_path_does_not_stale_a_dated_card() {
        let rec = accept_levels_only_entry(sample_input(1_771_372_800_000.0)).unwrap();
        let slice = positioning_state_slice(Some(&rec), None);
        assert!(!slice.degraded);
        assert_eq!(slice.values["positioning.freshnessOk"], Value::Bool(true));
        assert_eq!(slice.provenance.data_time, Some(rec.as_of_ms));
    }

    #[test]
    fn empty_slice_fails_closed_manual_not_provider() {
        let slice = positioning_state_slice(None, Some("2026-08-13"));
        assert!(slice.degraded);
        assert_eq!(slice.provenance.source, ProvenanceSource::Manual);
        assert!(slice.provenance.data_time.is_none());
        assert!(slice.provenance.vendor.is_none());
        assert_eq!(slice.values["positioning.freshnessOk"], Value::Bool(false));
        assert!(!slice.values.contains_key("positioning.recordKind"));
    }

    #[test]
    fn rejects_slice_and_vendor_pretence() {
        let mut input = sample_input(1.0);
        input.record_kind = Some("slice".into());
        assert!(accept_levels_only_entry(input).is_err());

        let mut input = sample_input(1.0);
        input.vendor = Some("VolSignals".into());
        assert!(accept_levels_only_entry(input).is_err());

        let mut input = sample_input(1.0);
        input.data_time_ms = Some(1.0);
        assert!(accept_levels_only_entry(input).is_err());
    }

    #[test]
    fn rejects_advisory_and_fallback_copy() {
        let mut input = sample_input(1.0);
        input.note = Some("You should buy the call wall".into());
        assert!(accept_levels_only_entry(input).is_err());

        let mut input = sample_input(1.0);
        input.note = Some("fallback record for the backlog".into());
        assert!(accept_levels_only_entry(input).is_err());
    }

    #[test]
    fn overlay_keeps_positioning_unprefixed_and_l0() {
        let catalog = build_catalog();
        let snap = serde_json::json!({
            "lastPrice": 21000.25,
            "rootSymbol": "NQ",
            "sessionType": "RTH",
        });
        let mut env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: Some(vec!["NQ".into()]),
                domains: Some(vec!["positioning".into(), "identity".into()]),
                fields: None,
                resolution: StateResolution::R1,
                as_of: None,
                budget_tokens: None,
                snapshot: Some(&snap),
                snapshot_source: ProvenanceSource::Live,
                data_time: Some(1.0),
                source_degraded: false,
                source_degraded_note: None,
            },
        )
        .expect("env");
        let rec = accept_levels_only_entry(sample_input(1_771_372_800_000.0)).unwrap();
        let slice = positioning_state_slice(Some(&rec), Some("2026-02-18"));
        apply_positioning_slice(&mut env, &catalog, &slice, None);
        assert_eq!(env.trust_level, super::super::envelope::TrustLevel::L0);
        assert_eq!(env.trust_ceiling, TrustCeiling::L3);
        assert!(env.provenance.contains_key("positioning"));
        assert!(!env.provenance.contains_key("NQ.positioning"));
        assert_eq!(
            env.provenance["positioning"].source,
            ProvenanceSource::Manual
        );
        assert_eq!(env.degraded.get("positioning"), Some(&false));
        assert_eq!(
            env.values.get("positioning.completeness"),
            Some(&Value::String(LEVELS_ONLY_RECORD_KIND.into()))
        );
        assert!(!env.values.keys().any(|k| k.starts_with("NQ.positioning")));
    }

    #[test]
    fn overlay_never_silently_omits_stale_or_empty() {
        let catalog = build_catalog();
        let mut env = build_state_envelope(
            &catalog,
            StateReadRequest {
                symbols: None,
                domains: Some(vec!["positioning".into()]),
                fields: None,
                resolution: StateResolution::R0,
                as_of: None,
                budget_tokens: None,
                snapshot: None,
                snapshot_source: ProvenanceSource::Live,
                data_time: None,
                source_degraded: true,
                source_degraded_note: None,
            },
        )
        .unwrap();
        apply_positioning_slice(&mut env, &catalog, &empty_positioning_slice(), None);
        assert!(env.provenance.contains_key("positioning"));
        assert_eq!(env.degraded.get("positioning"), Some(&true));
        assert_eq!(
            env.values.get("positioning.freshnessOk"),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            env.provenance["positioning"].source,
            ProvenanceSource::Manual
        );
    }
}
