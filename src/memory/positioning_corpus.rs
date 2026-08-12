//! Positioning interpretation exemplar corpus (SIL-P-VS-c / #17).
//!
//! Seeds co-annotated Positioning sessions into `agent_insights` so
//! `recall_agent_insights` / `get_memory_brief` can surface them. Historical
//! backlog days enter as **Levels-Only Records** (first-class, ADR-025).
//! Not blocked on ToS or Vs3dProvider.

use crate::catalog;
use crate::db::Database;
use crate::memory::{AgentInsightQuery, AgentInsightRecord, MemoryError, INSIGHT_PINNED};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Category used for Positioning co-annotations (live habit + seed corpus).
pub const POSITIONING_ANNOTATION_CATEGORY: &str = "positioning_annotation";

/// Stable `source` stamp for seeded exemplars.
pub const POSITIONING_CORPUS_SOURCE: &str = "positioning_corpus_seed";

/// Tag always present on Positioning insights for recall filters.
pub const POSITIONING_TAG: &str = "positioning";

const CORPUS_JSON: &str =
    include_str!("../../docs/trader-memory/fixtures/positioning-exemplar-corpus.json");

/// Checked-in Positioning exemplar corpus file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningExemplarCorpus {
    pub corpus_id: String,
    pub methodology_ref: String,
    pub category: String,
    pub source: String,
    pub trust_ceiling: String,
    pub sessions: Vec<PositioningExemplarSession>,
}

/// One co-annotated Positioning session (Slice or Levels-Only Record).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningExemplarSession {
    pub id: String,
    pub trading_day: String,
    pub archetype: String,
    pub record_kind: String,
    pub completeness: String,
    pub summary: String,
    pub tags: Vec<String>,
    pub scope: Value,
    pub confidence: f64,
    pub salience: f64,
    pub evidence: Value,
}

/// Result of an idempotent corpus seed into `agent_insights`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositioningCorpusSeedReport {
    pub inserted: Vec<String>,
    pub skipped_existing: Vec<String>,
    pub corpus_id: String,
    pub session_count: usize,
}

/// Parse the checked-in Positioning exemplar corpus.
pub fn load_positioning_exemplar_corpus() -> Result<PositioningExemplarCorpus, MemoryError> {
    serde_json::from_str(CORPUS_JSON).map_err(|err| {
        MemoryError::Validation(format!(
            "positioning exemplar corpus JSON is invalid: {err}"
        ))
    })
}

/// Catalog Positioning record-kind ids that exemplars may use.
pub fn catalog_positioning_record_kind_ids() -> Vec<String> {
    catalog::build_catalog()
        .positioning_record_kinds
        .into_iter()
        .map(|kind| kind.id)
        .collect()
}

/// Validate corpus vocabulary against Catalog v0 Positioning kinds + shape rules.
pub fn validate_positioning_exemplar_corpus(
    corpus: &PositioningExemplarCorpus,
) -> Result<(), MemoryError> {
    if corpus.category != POSITIONING_ANNOTATION_CATEGORY {
        return Err(MemoryError::Validation(format!(
            "corpus category must be `{POSITIONING_ANNOTATION_CATEGORY}`"
        )));
    }
    if corpus.source != POSITIONING_CORPUS_SOURCE {
        return Err(MemoryError::Validation(format!(
            "corpus source must be `{POSITIONING_CORPUS_SOURCE}`"
        )));
    }
    if corpus.trust_ceiling != "L3" {
        return Err(MemoryError::Validation(
            "corpus trustCeiling must remain L3".into(),
        ));
    }
    let n = corpus.sessions.len();
    if !(8..=14).contains(&n) {
        return Err(MemoryError::Validation(format!(
            "expected ~10 exemplar sessions (got {n})"
        )));
    }

    let kind_ids = catalog_positioning_record_kind_ids();
    let mut levels_only = 0usize;
    for session in &corpus.sessions {
        if session.summary.trim().is_empty() {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` has empty summary",
                session.id
            )));
        }
        let summary_l = session.summary.to_lowercase();
        if summary_l.contains("you should buy")
            || summary_l.contains("you should sell")
            || summary_l.contains("i recommend")
            || summary_l.contains("this is a good trade")
        {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` uses advisory language",
                session.id
            )));
        }
        let blob_vocab = format!("{}{}", session.summary, session.evidence).to_lowercase();
        for banned in [
            "options domain",
            "surface snapshot",
            "degraded record",
            "fallback record",
            "degraded path",
            "fallback path",
        ] {
            if blob_vocab.contains(banned) {
                return Err(MemoryError::Validation(format!(
                    "exemplar `{}` uses forbidden vocabulary `{banned}`",
                    session.id
                )));
            }
        }
        let allowed_buckets = [
            "rth_open",
            "rth_midday",
            "rth_afternoon",
            "globex",
            "asia",
            "london",
        ];
        match &session.scope {
            Value::Object(_) | Value::Null => {}
            other => {
                return Err(MemoryError::Validation(format!(
                    "exemplar `{}` scope must be a JSON object (got {other})",
                    session.id
                )));
            }
        }
        if let Some(bucket) = session.scope.get("timeBucket").and_then(|v| v.as_str()) {
            if !allowed_buckets.contains(&bucket) {
                return Err(MemoryError::Validation(format!(
                    "exemplar `{}` has non-canonical timeBucket `{bucket}`",
                    session.id
                )));
            }
        }
        if !session.tags.iter().any(|t| t == POSITIONING_TAG) {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` missing `{POSITIONING_TAG}` tag",
                session.id
            )));
        }
        if !kind_ids.iter().any(|id| id == &session.record_kind) {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` recordKind `{}` is not a Catalog Positioning kind",
                session.id, session.record_kind
            )));
        }
        if session.completeness != session.record_kind {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` completeness `{}` must match recordKind `{}`",
                session.id, session.completeness, session.record_kind
            )));
        }
        let evidence_kind = session
            .evidence
            .get("recordKind")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if evidence_kind != session.record_kind {
            return Err(MemoryError::Validation(format!(
                "exemplar `{}` evidence.recordKind mismatch",
                session.id
            )));
        }
        let levels = session.evidence.get("derivedLevels").ok_or_else(|| {
            MemoryError::Validation(format!(
                "exemplar `{}` missing evidence.derivedLevels",
                session.id
            ))
        })?;
        for key in ["flip", "walls", "balance", "upsideTest", "downsideTest"] {
            if levels.get(key).is_none() {
                return Err(MemoryError::Validation(format!(
                    "exemplar `{}` derivedLevels missing `{key}`",
                    session.id
                )));
            }
        }
        // Levels-Only Records are first-class — refuse affirmative degraded/fallback labels.
        // (Forbidden bare phrases are already rejected in the vocabulary check above.)
        if session.record_kind == "levels_only" {
            let first_class = session
                .evidence
                .pointer("/provenance/firstClass")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !first_class {
                return Err(MemoryError::Validation(format!(
                    "Levels-Only exemplar `{}` must set provenance.firstClass=true",
                    session.id
                )));
            }
            if !session.tags.iter().any(|t| t == "levels_only") {
                return Err(MemoryError::Validation(format!(
                    "Levels-Only exemplar `{}` missing `levels_only` tag",
                    session.id
                )));
            }
            levels_only += 1;
        }
        if session.record_kind == "slice" && !session.tags.iter().any(|t| t == "slice") {
            return Err(MemoryError::Validation(format!(
                "Slice exemplar `{}` missing `slice` tag",
                session.id
            )));
        }
    }
    if levels_only == 0 {
        return Err(MemoryError::Validation(
            "corpus must include at least one Levels-Only Record backlog exemplar".into(),
        ));
    }
    Ok(())
}

/// Idempotently seed pinned Positioning exemplars into `agent_insights`.
///
/// Existing ids are left untouched (non-destructive). New rows are inserted as
/// `pinned` so teaching exemplars are not lifecycle-staled.
pub fn seed_positioning_exemplar_corpus(
    db: &Database,
) -> Result<PositioningCorpusSeedReport, MemoryError> {
    let corpus = load_positioning_exemplar_corpus()?;
    validate_positioning_exemplar_corpus(&corpus)?;

    let mut report = PositioningCorpusSeedReport {
        corpus_id: corpus.corpus_id.clone(),
        session_count: corpus.sessions.len(),
        ..Default::default()
    };

    // Stable synthetic timestamps so ranking is deterministic across seeds.
    let base_ms = 1_772_582_400_000.0; // ~2026-03-03T00:00:00Z

    for (idx, session) in corpus.sessions.iter().enumerate() {
        if db.get_agent_insight(&session.id)?.is_some() {
            report.skipped_existing.push(session.id.clone());
            continue;
        }
        let created_at_ms = base_ms + (idx as f64) * 86_400_000.0;
        let mut scope = match &session.scope {
            Value::Object(_) => session.scope.clone(),
            Value::Null => Value::Object(serde_json::Map::new()),
            other => {
                return Err(MemoryError::Validation(format!(
                    "exemplar `{}` scope must be a JSON object (got {other})",
                    session.id
                )));
            }
        };
        if scope.get("tradingDay").is_none() {
            scope["tradingDay"] = Value::String(session.trading_day.clone());
        }
        if scope.get("archetype").is_none() {
            scope["archetype"] = Value::String(session.archetype.clone());
        }
        let record = AgentInsightRecord {
            id: session.id.clone(),
            created_at_ms,
            updated_at_ms: created_at_ms,
            session_id: None,
            trade_id: None,
            setup_id: None,
            category: POSITIONING_ANNOTATION_CATEGORY.to_string(),
            status: INSIGHT_PINNED.to_string(),
            summary: session.summary.clone(),
            evidence: session.evidence.clone(),
            tags: session.tags.clone(),
            scope,
            confidence: session.confidence.clamp(0.0, 1.0),
            salience: session.salience.clamp(0.0, 1.0),
            times_surfaced: 0,
            last_surfaced_ms: None,
            superseded_by: None,
            source: POSITIONING_CORPUS_SOURCE.to_string(),
            helpful_count: 0,
            irrelevant_count: 0,
            wrong_count: 0,
        };
        db.upsert_agent_insight(&record)?;
        report.inserted.push(session.id.clone());
    }
    Ok(report)
}

/// Count seeded Positioning exemplars currently recallable by category/tag.
pub fn count_recallable_positioning_exemplars(db: &Database) -> Result<usize, MemoryError> {
    let insights = db.list_agent_insights(&AgentInsightQuery {
        category: Some(POSITIONING_ANNOTATION_CATEGORY.to_string()),
        tag: Some(POSITIONING_TAG.to_string()),
        statuses: Some(vec![INSIGHT_PINNED.to_string()]),
        limit: Some(200),
        ..AgentInsightQuery::default()
    })?;
    Ok(insights
        .into_iter()
        .filter(|i| i.source == POSITIONING_CORPUS_SOURCE)
        .count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{
        build_memory_brief, MemoryBriefQuery, INSIGHT_CANDIDATE, INSIGHT_VALIDATED,
    };

    #[test]
    fn corpus_fixture_validates_against_catalog_kinds() {
        let corpus = load_positioning_exemplar_corpus().expect("parse");
        validate_positioning_exemplar_corpus(&corpus).expect("validate");
        assert!(
            (8..=14).contains(&corpus.sessions.len()),
            "want ~10 sessions"
        );
        let kinds = catalog_positioning_record_kind_ids();
        assert!(kinds.iter().any(|k| k == "slice"));
        assert!(kinds.iter().any(|k| k == "levels_only"));
        assert!(corpus
            .sessions
            .iter()
            .any(|s| s.record_kind == "levels_only"));
        assert!(corpus.sessions.iter().any(|s| s.record_kind == "slice"));
    }

    #[test]
    fn seed_is_idempotent_and_recallable_via_memory_tools() {
        let file = tempfile::NamedTempFile::new().expect("temp");
        let db = Database::open(file.path().to_string_lossy().as_ref()).expect("open");

        let first = seed_positioning_exemplar_corpus(&db).expect("seed");
        assert_eq!(first.session_count, 10, "seed corpus targets ~10 sessions");
        assert_eq!(first.inserted.len(), first.session_count);
        assert!(first.skipped_existing.is_empty());
        assert_eq!(
            count_recallable_positioning_exemplars(&db).expect("count"),
            first.session_count
        );

        // Explicit recall_agent_insights seam: category + positioning tag returns full set.
        let by_category_tag = db
            .list_agent_insights(&AgentInsightQuery {
                category: Some(POSITIONING_ANNOTATION_CATEGORY.to_string()),
                tag: Some(POSITIONING_TAG.to_string()),
                statuses: Some(vec![INSIGHT_PINNED.to_string()]),
                limit: Some(200),
                ..AgentInsightQuery::default()
            })
            .expect("recall category+tag");
        assert_eq!(by_category_tag.len(), first.session_count);

        let second = seed_positioning_exemplar_corpus(&db).expect("reseed");
        assert!(second.inserted.is_empty());
        assert_eq!(second.skipped_existing.len(), first.session_count);

        let recalled = db
            .list_agent_insights(&AgentInsightQuery {
                category: Some(POSITIONING_ANNOTATION_CATEGORY.to_string()),
                tag: Some("levels_only".into()),
                statuses: Some(vec![INSIGHT_PINNED.to_string()]),
                limit: Some(50),
                ..AgentInsightQuery::default()
            })
            .expect("recall");
        assert!(
            recalled.len() >= 2,
            "Levels-Only backlog exemplars must recall by tag"
        );
        for insight in &recalled {
            assert_eq!(insight.status, INSIGHT_PINNED);
            let kind = insight
                .evidence
                .get("recordKind")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            assert_eq!(kind, "levels_only");
            assert_eq!(
                insight
                    .evidence
                    .pointer("/provenance/firstClass")
                    .and_then(|v| v.as_bool()),
                Some(true)
            );
        }

        // Default brief limit (5) still surfaces pinned Positioning exemplars by salience.
        let brief_default = build_memory_brief(
            &db,
            MemoryBriefQuery {
                include_insights: Some(true),
                include_patterns: Some(false),
                include_followups: Some(false),
                include_recent_sessions: Some(false),
                ..MemoryBriefQuery::default()
            },
        )
        .expect("brief default");
        assert!(
            brief_default
                .insights
                .iter()
                .filter(|i| i.category == POSITIONING_ANNOTATION_CATEGORY)
                .count()
                >= 3,
            "default memory brief limit should still surface Positioning exemplars"
        );

        let brief = build_memory_brief(
            &db,
            MemoryBriefQuery {
                include_insights: Some(true),
                include_patterns: Some(false),
                include_followups: Some(false),
                include_recent_sessions: Some(false),
                limit: Some(20),
                ..MemoryBriefQuery::default()
            },
        )
        .expect("brief");
        let surfaced = brief
            .insights
            .iter()
            .filter(|i| i.category == POSITIONING_ANNOTATION_CATEGORY)
            .count();
        assert!(
            surfaced >= 5,
            "memory brief should surface pinned Positioning exemplars (got {surfaced})"
        );
        // pinned status is included by brief query filters
        assert!(brief.insights.iter().any(|i| {
            i.status == INSIGHT_PINNED
                || i.status == INSIGHT_VALIDATED
                || i.status == INSIGHT_CANDIDATE
        }));
    }
}
