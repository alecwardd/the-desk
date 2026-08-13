//! SIL discovery + read-kernel operators.
//!
//! Registered on the MCP router only when `[sil].catalog_discovery = true`.
//! Not specialty market tools; do not add these to `tools/market.rs`.
//!
//! Trust Level: all operators here are L0 (read/query) — structurally
//! incapable of mutation or order authority.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::catalog::{
    apply_positioning_slice, apply_token_budget, build_catalog, build_state_envelope,
    collapse_events_latest_per_dedup, describe_domain, describe_environment,
    kernel_event_from_db_row, kernel_event_from_market_event_scoped, merge_symbol_envelopes,
    positioning_state_slice, search_catalog, state_envelope_json, EventsEnvelope,
    PositioningStateSlice, ProvenanceSource, StateEnvelope, StateReadRequest, StateResolution,
    TrustLevel, KERNEL_READ_QUERY_TOOLS,
};
use the_desk_backend::engine::{parse_requested_roots, RouterRoot, RouterRootError};
use the_desk_backend::trading_day_from_timestamp_ms;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = discovery_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Describe the Desk Catalog environment: catalogVersion, Trust Ceiling (L3), domain list, Positioning stub status, and specialty-market-tool policy. Returns catalog metadata only — never live market data. Enable via [sil].catalog_discovery in config.toml. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn describe_environment(&self) -> Result<CallToolResult, McpError> {
        let catalog = build_catalog();
        let mut out = describe_environment(&catalog, self.sil_config.catalog_discovery);
        if let Some(obj) = out.as_object_mut() {
            obj.insert("trustLevel".into(), serde_json::json!(TrustLevel::L0));
            obj.insert("mutationAuthority".into(), serde_json::json!(false));
            obj.insert("orderAuthority".into(), serde_json::json!(false));
            obj.insert(
                "kernelOperators".into(),
                serde_json::json!(KERNEL_READ_QUERY_TOOLS),
            );
        }
        Ok(text_result(out))
    }

    #[tool(
        description = "Describe one catalog domain by id (identity, location_structure, flow, liquidity, response, volatility, positioning, cross_market, events, meta). Returns field descriptors (unit, session scope, freshness, cost hint) — metadata only, never live market data. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn describe_domain(
        &self,
        Parameters(params): Parameters<DescribeDomainParams>,
    ) -> Result<CallToolResult, McpError> {
        let domain_id = params
            .domain
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                invalid_params_error("describe_domain requires `domain` (catalog domain id)")
            })?;
        let catalog = build_catalog();
        match describe_domain(&catalog, domain_id) {
            Some(mut out) => {
                if let Some(obj) = out.as_object_mut() {
                    obj.insert("trustLevel".into(), serde_json::json!(TrustLevel::L0));
                    obj.insert("mutationAuthority".into(), serde_json::json!(false));
                    obj.insert("orderAuthority".into(), serde_json::json!(false));
                }
                Ok(text_result(out))
            }
            None => Ok(text_result(serde_json::json!({
                "error": "unknown_domain",
                "domain": domain_id,
                "knownDomains": catalog.domains.iter().map(|d| &d.id).collect::<Vec<_>>(),
                "metadataOnly": true,
                "catalogVersion": catalog.catalog_version,
                "trustLevel": TrustLevel::L0,
                "mutationAuthority": false,
                "orderAuthority": false,
            }))),
        }
    }

    #[tool(
        description = "Search the Desk Catalog by text across field ids, names, descriptions, and domains. Returns matching field descriptors with unit, session scope, freshness, and cost hint — metadata only, never live market data. Trust Level L0 (read/query)."
    )]
    pub(crate) async fn search_catalog(
        &self,
        Parameters(params): Parameters<SearchCatalogParams>,
    ) -> Result<CallToolResult, McpError> {
        let query = params.query.unwrap_or_default();
        let catalog = build_catalog();
        let hits = search_catalog(&catalog, &query);
        Ok(text_result(serde_json::json!({
            "catalogVersion": catalog.catalog_version,
            "query": query,
            "hitCount": hits.len(),
            "hits": hits,
            "metadataOnly": true,
            "trustLevel": TrustLevel::L0,
            "mutationAuthority": false,
            "orderAuthority": false,
        })))
    }

    #[tool(
        description = "SIL read kernel: parameterized state read returning a StateEnvelope with per-domain provenance and degraded flags. Params: symbols? (NQ and/or ES; MarketRouter v0), domains?, fields?, resolution (R0|R1 required), as_of?, budget_tokens?. Live reads (no as_of) use published/live snapshots. as_of is served from 1 Hz Journal Frames (provenance source = Journal) — never pipeline_snapshots. When both symbols are requested, values are keyed {ROOT}.{catalogFieldId} in one envelope on the aligned clock. Absence of provenance is a failure; a degraded domain sets its flag rather than failing the whole call. Trust Level L0 — read/query only, no mutation or order authority. Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn get_state(
        &self,
        Parameters(params): Parameters<GetStateParams>,
    ) -> Result<CallToolResult, McpError> {
        let resolution_raw = params
            .resolution
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid_params_error("get_state requires `resolution` of R0 or R1"))?;
        let resolution = StateResolution::parse(resolution_raw)
            .map_err(|e| invalid_params_error(e.to_string()))?;

        let catalog = build_catalog();
        let requested_roots = parse_requested_roots(params.symbols.as_deref()).map_err(|e| {
            invalid_params_error(match e {
                RouterRootError::MicroNotInScope(s) => format!(
                    "get_state symbols include `{s}` — MarketRouter v0 hosts NQ and ES only (micros are out of scope)"
                ),
                other => other.to_string(),
            })
        })?;

        if params.as_of.is_some() {
            return self
                .get_state_from_journal_frames(&catalog, requested_roots, params, resolution)
                .await;
        }

        let (snapshot_owned, snapshot_source, data_time, source_degraded, source_note, as_of) =
            self.resolve_state_snapshot().await?;

        // Live path can return both MarketRouter roots in one StateEnvelope.
        let live_by_symbol = if as_of.is_none() {
            self.collect_live_snapshots_by_root()
        } else {
            std::collections::BTreeMap::new()
        };

        let caller_listed_both = params
            .symbols
            .as_ref()
            .map(|list| {
                parse_requested_roots(Some(list))
                    .map(|v| v.len() > 1)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        let both_live = requested_roots
            .iter()
            .filter(|r| live_by_symbol.contains_key(r))
            .count()
            >= 2;
        let include_multi =
            as_of.is_none() && requested_roots.len() > 1 && (caller_listed_both || both_live);

        if include_multi {
            let mut by_root = std::collections::BTreeMap::new();
            for root in &requested_roots {
                let snap = live_by_symbol.get(root);
                let (snapshot, data_time, degraded, note) = match snap {
                    Some(piece) => (
                        Some(&piece.snapshot),
                        piece.data_time,
                        piece.degraded,
                        piece.note.clone(),
                    ),
                    None => (
                        None,
                        data_time,
                        true,
                        Some(format!(
                            "MarketRouter has no live snapshot for {}",
                            root.as_str()
                        )),
                    ),
                };
                let req = StateReadRequest {
                    symbols: Some(vec![root.as_str().to_string()]),
                    domains: params.domains.clone(),
                    fields: params.fields.clone(),
                    resolution,
                    as_of,
                    budget_tokens: None,
                    snapshot,
                    snapshot_source,
                    data_time,
                    source_degraded: degraded,
                    source_degraded_note: note,
                };
                let env = build_state_envelope(&catalog, req).map_err(|e| match e {
                    the_desk_backend::catalog::EnvelopeError::MissingProvenance(_) => {
                        McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
                    }
                    other => invalid_params_error(other.to_string()),
                })?;
                by_root.insert(root.as_str().to_string(), env);
            }
            if let Some(budget) = params.budget_tokens {
                if let Some(env) = by_root.values_mut().next() {
                    env.budget_tokens = Some(budget);
                }
            }
            let mut clock_ms = self.market_router.clock_ms().or(data_time);
            for piece in live_by_symbol.values() {
                clock_ms = match (clock_ms, piece.data_time) {
                    (Some(a), Some(b)) if a.is_finite() && b.is_finite() => Some(a.max(b)),
                    (Some(a), _) if a.is_finite() => Some(a),
                    (_, Some(b)) if b.is_finite() => Some(b),
                    (other, _) => other,
                };
            }
            let envelope = merge_symbol_envelopes(by_root, clock_ms)
                .map_err(|e| McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;
            return self.finish_get_state(&catalog, envelope, params.fields.as_deref(), None);
        }

        // Single-symbol (M1b shape): requested root must match the snapshot when known.
        if requested_roots.len() == 1 {
            let want = requested_roots[0];
            if let Some(root) = snapshot_owned
                .as_ref()
                .and_then(|s| s.get("rootSymbol"))
                .and_then(|v| v.as_str())
            {
                if let Ok(have) = RouterRoot::parse(root) {
                    if have != want {
                        if let Some(piece) = live_by_symbol.get(&want) {
                            let req = StateReadRequest {
                                symbols: Some(vec![want.as_str().to_string()]),
                                domains: params.domains.clone(),
                                fields: params.fields.clone(),
                                resolution,
                                as_of,
                                budget_tokens: params.budget_tokens,
                                snapshot: Some(&piece.snapshot),
                                snapshot_source,
                                data_time: piece.data_time,
                                source_degraded: piece.degraded,
                                source_degraded_note: piece.note.clone(),
                            };
                            let envelope =
                                build_state_envelope(&catalog, req).map_err(|e| match e {
                                    the_desk_backend::catalog::EnvelopeError::MissingProvenance(
                                        _,
                                    ) => McpError::new(
                                        ErrorCode::INTERNAL_ERROR,
                                        e.to_string(),
                                        None,
                                    ),
                                    other => invalid_params_error(other.to_string()),
                                })?;
                            return self.finish_get_state(
                                &catalog,
                                envelope,
                                params.fields.as_deref(),
                                None,
                            );
                        }
                        return Err(invalid_params_error(format!(
                            "get_state symbol `{}` does not match resolved rootSymbol `{root}` \
                             and MarketRouter has no live snapshot for that root",
                            want.as_str()
                        )));
                    }
                }
            }
        }

        let fields = params.fields.clone();
        let req = StateReadRequest {
            symbols: params.symbols,
            domains: params.domains,
            fields: fields.clone(),
            resolution,
            as_of,
            budget_tokens: params.budget_tokens,
            snapshot: snapshot_owned.as_ref(),
            snapshot_source,
            data_time,
            source_degraded,
            source_degraded_note: source_note,
        };
        let envelope = build_state_envelope(&catalog, req).map_err(|e| match e {
            the_desk_backend::catalog::EnvelopeError::MissingProvenance(_) => {
                McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
            }
            other => invalid_params_error(other.to_string()),
        })?;
        self.finish_get_state(&catalog, envelope, fields.as_deref(), None)
    }

    #[tool(
        description = "SIL read kernel: formalized market events with lifecycle (open → updated → resolved|expired), severity, dedup identity, and frame_ref joining each row to the producing Journal Frame. Trust Level L0 (read/query) — structurally incapable of mutation or order authority. Attention inbox is a ranked view over this stream (get_attention_inbox). Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn get_events(
        &self,
        Parameters(params): Parameters<GetEventsParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(50).clamp(1, 500) as usize;
        let event_type = params
            .event_type
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let since_ms = match params.since_ms {
            Some(t) if !t.is_finite() || t <= 0.0 => {
                return Err(invalid_params_error(
                    "sinceMs must be a positive finite epoch-milliseconds value",
                ));
            }
            other => other,
        };
        let _symbols = params.symbols; // reserved for MarketRouter multi-symbol

        // Prefer SQLite (lifecycle + frame_ref persist without a connected agent).
        // Published rows fill the live gap before the first journal flush.
        // Occurrence rows stay in SQLite for research; the coaching view is
        // latest-per-dedup identity.
        let (db_rows, db_had_rows) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let rows = db
                .list_coaching_market_events(limit, since_ms, event_type)
                .map_err(db_error)?;
            let had = if rows.is_empty() {
                !db.list_recent_market_events(1, since_ms, None)
                    .map_err(db_error)?
                    .is_empty()
            } else {
                true
            };
            (rows, had)
        };
        let mut events: Vec<_> = db_rows.iter().map(kernel_event_from_db_row).collect();
        if events.is_empty() && !db_had_rows {
            if let Some(store) = self.engine_published.as_ref() {
                let published = store.load();
                let mut out = Vec::new();
                for value in published.recent_events.iter().rev() {
                    let root = value
                        .get("rootSymbol")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let Ok(ev) = serde_json::from_value::<the_desk_backend::pipelines::MarketEvent>(
                        value.clone(),
                    ) else {
                        continue;
                    };
                    if let Some(min_ts) = since_ms {
                        if ev.timestamp_ms < min_ts {
                            continue;
                        }
                    }
                    out.push(kernel_event_from_market_event_scoped(
                        &ev,
                        root.as_deref(),
                        None,
                    ));
                }
                events = collapse_events_latest_per_dedup(out);
                if let Some(want) = event_type {
                    events.retain(|e| e.event_type.eq_ignore_ascii_case(want));
                }
                if events.len() > limit {
                    events.truncate(limit);
                }
            }
        }
        let envelope = EventsEnvelope::from_events(events);
        let mut out = serde_json::to_value(&envelope).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = out.as_object_mut() {
            obj.insert("mutationAuthority".into(), serde_json::json!(false));
            obj.insert("orderAuthority".into(), serde_json::json!(false));
        }
        Ok(text_result(out))
    }
}

impl TheDeskMcp {
    /// Overlay Positioning onto a StateEnvelope, then serialize (Trust Level L0).
    fn finish_get_state(
        &self,
        catalog: &the_desk_backend::catalog::DeskCatalog,
        mut envelope: StateEnvelope,
        fields: Option<&[String]>,
        as_of: Option<f64>,
    ) -> Result<CallToolResult, McpError> {
        let slice = self.load_positioning_slice(as_of);
        apply_positioning_slice(&mut envelope, catalog, &slice, fields);
        if let Some(budget) = envelope.budget_tokens {
            apply_token_budget(&mut envelope, budget);
        }
        Ok(text_result(finish_state_envelope_json(&envelope)?))
    }

    /// Load the durable Positioning record for live (`None`) or as-of reads.
    fn load_positioning_slice(&self, as_of: Option<f64>) -> PositioningStateSlice {
        let record = match self.db.lock() {
            Ok(db) => {
                if let Some(ts) = as_of {
                    db.get_positioning_record_as_of(ts).ok().flatten()
                } else {
                    db.latest_positioning_record().ok().flatten()
                }
            }
            Err(_) => None,
        };
        let reference_ms = as_of.unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as f64);
        let reference_day = trading_day_from_timestamp_ms(reference_ms);
        positioning_state_slice(record.as_ref(), Some(reference_day.as_str()))
    }

    /// Serve `get_state(as_of=…)` from 1 Hz Journal Frames (never `pipeline_snapshots`).
    async fn get_state_from_journal_frames(
        &self,
        catalog: &the_desk_backend::catalog::DeskCatalog,
        requested_roots: Vec<RouterRoot>,
        params: GetStateParams,
        resolution: StateResolution,
    ) -> Result<CallToolResult, McpError> {
        let ts = params.as_of.ok_or_else(|| {
            invalid_params_error("asOf must be a positive finite epoch-milliseconds value")
        })?;
        if !ts.is_finite() || ts <= 0.0 {
            return Err(invalid_params_error(
                "asOf must be a positive finite epoch-milliseconds value",
            ));
        }

        let journal = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.get_journal_frames_as_of(ts).map_err(db_error)?
        };

        let note_missing = "as_of Journal Frame unavailable; domains degraded — Your playbook indicates historical structure is incomplete";
        let mut by_root = std::collections::BTreeMap::new();
        for root in &requested_roots {
            let (snapshot, data_time, degraded, note) = match journal.as_ref() {
                Some(snap) => match snap.by_root.get(root.as_str()) {
                    Some(payload) if !payload.is_null() => (
                        Some(payload.clone()),
                        Some(snap.clock_ms),
                        false,
                        Some("as_of served from Journal Frames".into()),
                    ),
                    _ => (None, Some(snap.clock_ms), true, Some(note_missing.into())),
                },
                None => (None, None, true, Some(note_missing.into())),
            };
            let req = StateReadRequest {
                symbols: Some(vec![root.as_str().to_string()]),
                domains: params.domains.clone(),
                fields: params.fields.clone(),
                resolution,
                as_of: Some(ts),
                budget_tokens: None,
                snapshot: snapshot.as_ref(),
                snapshot_source: ProvenanceSource::Journal,
                data_time,
                source_degraded: degraded,
                source_degraded_note: note,
            };
            let env = build_state_envelope(catalog, req).map_err(|e| match e {
                the_desk_backend::catalog::EnvelopeError::MissingProvenance(_) => {
                    McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
                }
                other => invalid_params_error(other.to_string()),
            })?;
            by_root.insert(root.as_str().to_string(), env);
        }

        if let Some(budget) = params.budget_tokens {
            if let Some(env) = by_root.values_mut().next() {
                env.budget_tokens = Some(budget);
            }
        }

        let clock_ms = journal.as_ref().map(|s| s.clock_ms);
        let caller_listed_both = params
            .symbols
            .as_ref()
            .map(|list| {
                parse_requested_roots(Some(list))
                    .map(|v| v.len() > 1)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        let both_in_journal = requested_roots
            .iter()
            .filter(|root| {
                journal
                    .as_ref()
                    .and_then(|snap| snap.by_root.get(root.as_str()))
                    .is_some_and(|payload| !payload.is_null())
            })
            .count()
            >= 2;
        // Same rule as live get_state: merge only when the caller listed both
        // roots, or both roots are actually present. Omitted symbols with a
        // single frame keep the M1b unprefixed envelope (not ES-first BTreeMap).
        let include_multi = requested_roots.len() > 1 && (caller_listed_both || both_in_journal);
        if include_multi {
            let envelope = merge_symbol_envelopes(by_root, clock_ms)
                .map_err(|e| McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;
            return self.finish_get_state(catalog, envelope, params.fields.as_deref(), Some(ts));
        }

        let mut envelope = if requested_roots.len() == 1 {
            by_root.remove(requested_roots[0].as_str()).ok_or_else(|| {
                invalid_params_error("get_state as_of required a MarketRouter root")
            })?
        } else {
            // Omitted symbols: unprefixed envelope of a present frame. Prefer NQ
            // (live primary). If only ES printed this second, serve ES — do not
            // return the degraded NQ placeholder and drop the real frame.
            let preferred = if journal_root_present(journal.as_ref(), RouterRoot::Nq) {
                RouterRoot::Nq
            } else if journal_root_present(journal.as_ref(), RouterRoot::Es) {
                RouterRoot::Es
            } else {
                RouterRoot::Nq
            };
            by_root
                .remove(preferred.as_str())
                .or_else(|| by_root.into_values().next())
                .ok_or_else(|| {
                    invalid_params_error("get_state as_of required a MarketRouter root")
                })?
        };
        envelope.clock_ms = clock_ms;
        self.finish_get_state(catalog, envelope, params.fields.as_deref(), Some(ts))
    }

    /// Resolve a live market snapshot for `get_state` (no `as_of`).
    async fn resolve_state_snapshot(
        &self,
    ) -> Result<
        (
            Option<serde_json::Value>,
            ProvenanceSource,
            Option<f64>,
            bool,
            Option<String>,
            Option<f64>,
        ),
        McpError,
    > {
        if let Some(r) = self.resolve_live_market_view() {
            let degraded = r.degradation_reason.is_some() || r.snapshot.is_null();
            let note = r.degradation_reason.clone();
            let snap = if r.snapshot.is_null() {
                None
            } else {
                Some(r.snapshot)
            };
            Ok((
                snap,
                ProvenanceSource::Live,
                Some(r.as_of_timestamp_ms),
                degraded,
                note,
                None,
            ))
        } else if let Some(r) = self.resolve_market_snapshot_contention_gap() {
            Ok((
                None,
                ProvenanceSource::Live,
                Some(r.as_of_timestamp_ms),
                true,
                r.degradation_reason,
                None,
            ))
        } else {
            Ok((
                None,
                ProvenanceSource::Live,
                None,
                true,
                Some("no live or persisted market snapshot available".into()),
                None,
            ))
        }
    }

    /// Live MarketRouter snapshots keyed by root (embedded lanes + external published).
    fn collect_live_snapshots_by_root(
        &self,
    ) -> std::collections::BTreeMap<RouterRoot, LiveRootSnapshot> {
        let mut out = std::collections::BTreeMap::new();
        if let Some(store) = self.engine_published.as_ref() {
            let published = store.load();
            for root in RouterRoot::ALL {
                if let Some(snap) = published.snapshot_for_root(root.as_str()) {
                    if snap.is_null() {
                        continue;
                    }
                    let data_time = snap
                        .get("tapeLastTradeTimestampMs")
                        .and_then(|v| v.as_f64())
                        .or(published.clock_ms)
                        .or(published.data_time_ms);
                    out.insert(
                        root,
                        LiveRootSnapshot {
                            snapshot: snap.clone(),
                            data_time,
                            degraded: published.degraded
                                && root.as_str().eq_ignore_ascii_case(&published.primary_root),
                            note: published.degraded_note.clone(),
                        },
                    );
                }
            }
            if !out.is_empty() {
                return out;
            }
        }

        if let Some(r) = self.resolve_live_market_view() {
            if !r.snapshot.is_null() {
                let root = r
                    .snapshot
                    .get("rootSymbol")
                    .and_then(|v| v.as_str())
                    .and_then(|s| RouterRoot::parse(s).ok())
                    .unwrap_or(RouterRoot::Nq);
                let degraded = r.degradation_reason.is_some();
                out.insert(
                    root,
                    LiveRootSnapshot {
                        snapshot: r.snapshot,
                        data_time: Some(r.as_of_timestamp_ms),
                        degraded,
                        note: r.degradation_reason,
                    },
                );
            }
        }

        let es = self.market_router.es_host().snapshot_market_state();
        if !es.is_null() {
            let data_time = es
                .get("tapeLastTradeTimestampMs")
                .and_then(|v| v.as_f64())
                .or_else(|| self.market_router.clock_ms());
            out.insert(
                RouterRoot::Es,
                LiveRootSnapshot {
                    snapshot: es,
                    data_time,
                    degraded: false,
                    note: None,
                },
            );
        }
        out
    }
}

struct LiveRootSnapshot {
    snapshot: serde_json::Value,
    data_time: Option<f64>,
    degraded: bool,
    note: Option<String>,
}

fn finish_state_envelope_json(envelope: &StateEnvelope) -> Result<serde_json::Value, McpError> {
    let mut out = state_envelope_json(envelope)
        .map_err(|e| McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;
    if let Some(obj) = out.as_object_mut() {
        obj.insert("mutationAuthority".into(), serde_json::json!(false));
        obj.insert("orderAuthority".into(), serde_json::json!(false));
    }
    Ok(out)
}

fn journal_root_present(
    journal: Option<&the_desk_backend::db::JournalAsOfSnapshot>,
    root: RouterRoot,
) -> bool {
    journal
        .and_then(|snap| snap.by_root.get(root.as_str()))
        .is_some_and(|payload| !payload.is_null())
}
