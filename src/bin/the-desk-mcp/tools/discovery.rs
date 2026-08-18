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
    apply_positioning_slice, apply_token_budget, attach_capsule_refs, build_catalog_with_overlay,
    build_state_envelope, collapse_events_latest_per_dedup, describe_domain, describe_environment,
    kernel_event_from_db_row, kernel_event_from_market_event_scoped, merge_eval_frames,
    merge_symbol_envelopes, positioning_state_slice, request_needs_derived_stamp, search_catalog,
    search_features, stamp_derived_feature_payload, state_envelope_json, EventsEnvelope,
    FeatureIrEvalPath, FeatureIrFrame, FeatureIrStore, KernelEvent, PositioningStateSlice,
    ProvenanceSource, StateEnvelope, StateReadRequest, StateResolution, TrustLevel,
    FEATURE_IR_EVAL_MAX_FRAMES, KERNEL_READ_QUERY_TOOLS,
};
use the_desk_backend::db::{Database, JournalFrameRecord};
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
        let catalog = catalog_with_registry_overlay(self)?;
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
        let catalog = catalog_with_registry_overlay(self)?;
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
        description = "Search the Desk Catalog by text across field ids, names, descriptions, domains, and Feature Registry Base Detectors / Derived Features (schema, provenance, promotion, Feature-IR family). Returns matching field descriptors plus featureHits — metadata only, never live market data. Trust Level L0 (read/query). No specialty getter: registered detectors and derived features are discoverable here."
    )]
    pub(crate) async fn search_catalog(
        &self,
        Parameters(params): Parameters<SearchCatalogParams>,
    ) -> Result<CallToolResult, McpError> {
        let query = params.query.unwrap_or_default();
        let catalog = catalog_with_registry_overlay(self)?;
        let hits = search_catalog(&catalog, &query);
        let feature_hits = search_features(&catalog, &query);
        Ok(text_result(serde_json::json!({
            "catalogVersion": catalog.catalog_version,
            "query": query,
            "hitCount": hits.len(),
            "hits": hits,
            "featureHitCount": feature_hits.len(),
            "featureHits": feature_hits,
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

        let catalog = catalog_with_registry_overlay(self)?;
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
            let as_of_ms = self.market_router.clock_ms().or(data_time).unwrap_or(0.0);
            let (ir_frames, truncated) = load_live_feature_ir_frames(
                self,
                &catalog,
                as_of_ms,
                params.fields.as_deref(),
                params.domains.as_deref(),
            )?;
            let mut by_root = std::collections::BTreeMap::new();
            for root in &requested_roots {
                let snap = live_by_symbol.get(root);
                let stamped = snap.map(|piece| {
                    stamp_get_state_snapshot(
                        &piece.snapshot,
                        &catalog,
                        &ir_frames,
                        root.as_str(),
                        piece.data_time.unwrap_or(as_of_ms),
                        FeatureIrEvalPath::LiveShadow,
                        truncated,
                    )
                });
                let (data_time, degraded, note) = match snap {
                    Some(piece) => (piece.data_time, piece.degraded, piece.note.clone()),
                    None => (
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
                    snapshot: stamped.as_ref(),
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
            if let Some(root) = snapshot_root_symbol(snapshot_owned.as_ref()) {
                if let Ok(have) = RouterRoot::parse(root) {
                    if have != want {
                        if let Some(piece) = live_by_symbol.get(&want) {
                            let as_of_ms = piece.data_time.or(data_time).unwrap_or(0.0);
                            let (ir_frames, truncated) = load_live_feature_ir_frames(
                                self,
                                &catalog,
                                as_of_ms,
                                params.fields.as_deref(),
                                params.domains.as_deref(),
                            )?;
                            let stamped = stamp_get_state_snapshot(
                                &piece.snapshot,
                                &catalog,
                                &ir_frames,
                                want.as_str(),
                                as_of_ms,
                                FeatureIrEvalPath::LiveShadow,
                                truncated,
                            );
                            let req = StateReadRequest {
                                symbols: Some(vec![want.as_str().to_string()]),
                                domains: params.domains.clone(),
                                fields: params.fields.clone(),
                                resolution,
                                as_of,
                                budget_tokens: params.budget_tokens,
                                snapshot: Some(&stamped),
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
        let eval_root = snapshot_root_symbol(snapshot_owned.as_ref())
            .map(|s| s.to_string())
            .or_else(|| requested_roots.first().map(|r| r.as_str().to_string()))
            .unwrap_or_else(|| "NQ".into());
        let as_of_ms = data_time.unwrap_or(0.0);
        let (ir_frames, truncated) = load_live_feature_ir_frames(
            self,
            &catalog,
            as_of_ms,
            params.fields.as_deref(),
            params.domains.as_deref(),
        )?;
        let stamped = snapshot_owned.as_ref().map(|s| {
            stamp_get_state_snapshot(
                s,
                &catalog,
                &ir_frames,
                &eval_root,
                as_of_ms,
                FeatureIrEvalPath::LiveShadow,
                truncated,
            )
        });
        let req = StateReadRequest {
            symbols: params.symbols,
            domains: params.domains,
            fields: fields.clone(),
            resolution,
            as_of,
            budget_tokens: params.budget_tokens,
            snapshot: stamped.as_ref(),
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
        description = "SIL read kernel: formalized market events with lifecycle (open → updated → resolved|expired), severity, dedup identity, frameRef to the producing Journal Frame, and capsuleRef on DOM-family rows (stop_run, iceberg_reload, pull_intent, book_velocity_regime_shift). Trust Level L0 (read/query) — structurally incapable of mutation or order authority. Attention inbox is a ranked view over this stream (get_attention_inbox). Enable via [sil].catalog_discovery."
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
        if let Some(store) = self.engine_published.as_ref() {
            events = merge_published_live_events(
                events,
                db_had_rows,
                &store.load().recent_events,
                since_ms,
                event_type,
                limit,
            );
        }
        {
            let capsules = if events.iter().any(|e| e.requires_capsule) {
                let trigger_ids: Vec<String> = events
                    .iter()
                    .filter(|e| e.requires_capsule)
                    .map(|e| e.identity_id.clone())
                    .collect();
                let dedup_ids: Vec<String> = events
                    .iter()
                    .filter(|e| e.requires_capsule)
                    .map(|e| e.dedup_identity_id.clone())
                    .collect();
                let db = self.db.lock().map_err(|_| lock_error())?;
                db.list_capsules_matching(&trigger_ids, &dedup_ids)
                    .map_err(db_error)?
            } else {
                Vec::new()
            };
            attach_capsule_refs(&mut events, &capsules);
        }
        let envelope = EventsEnvelope::from_events(events);
        let mut out = serde_json::to_value(&envelope).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = out.as_object_mut() {
            obj.insert("mutationAuthority".into(), serde_json::json!(false));
            obj.insert("orderAuthority".into(), serde_json::json!(false));
        }
        Ok(text_result(out))
    }

    #[tool(
        description = "SIL read kernel (R2): time-series of Desk Catalog fields from 1 Hz Journal Frames. Requires startMs and endMs — unbounded windows are rejected. Hard-capped. Optional store=hot (SQLite window, default) or store=cold (session-partitioned dumps). Every result includes N and reliabilityTier (AGENT.md Research Sample Size Policy). Your backtest shows / your rules say — never buy/sell advice. Trust Level L0 (read/query). Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn query_series(
        &self,
        Parameters(params): Parameters<QuerySeriesParams>,
    ) -> Result<CallToolResult, McpError> {
        let req = the_desk_backend::research::query_kernel::QuerySeriesRequest {
            window: query_window_from_params(
                params.start_ms,
                params.end_ms,
                params.session_type,
                params.symbols,
            ),
            fields: params.fields.unwrap_or_default(),
        };
        let store = params.store.clone();
        let cold_dir = self.cold_frames_dir.clone();
        let result = self
            .with_read_db(move |db| {
                let cold = the_desk_backend::engine::ColdFrameStore::new(cold_dir);
                let frames = frame_read_for(db, store.as_deref(), &cold)?;
                the_desk_backend::research::query_kernel::query_series_with(frames, &req)
                    .map_err(query_kernel_error)
            })
            .await?;
        Ok(text_result(merge_l0(
            serde_json::to_value(&result).unwrap_or_default(),
        )))
    }

    #[tool(
        description = "SIL read kernel Episode Query (R2): conjunctive multi-predicate filters over Desk Catalog fields across NQ+ES Journal Frames / events. The flagship query is expressible as five predicates (ES near positioning.derivedLevels, ES sessionDelta extreme seller aggression, ES poorLow, ES domSummary.bidReplenishing, NQ sessionDelta non-confirmation) and returns tick-driven MFE/MAE — not a fill simulator. Missing detector/vendor fields fail closed with provenance. Requires startMs and endMs. Optional store=hot (default) or store=cold for frames (events/ticks stay on SQLite). Every result includes N and reliabilityTier. Trust Level L0. Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn query_episodes(
        &self,
        Parameters(params): Parameters<QueryEpisodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let predicates = convert_predicates(params.predicates)?;
        let req = the_desk_backend::research::query_kernel::QueryEpisodesRequest {
            window: query_window_from_params(
                params.start_ms,
                params.end_ms,
                params.session_type,
                params.symbols,
            ),
            predicates,
            forward_direction: params.forward_direction,
        };
        let store = params.store.clone();
        let cold_dir = self.cold_frames_dir.clone();
        let result = self
            .with_read_db(move |db| {
                let cold = the_desk_backend::engine::ColdFrameStore::new(cold_dir);
                let frames = frame_read_for(db, store.as_deref(), &cold)?;
                the_desk_backend::research::query_kernel::query_episodes_with(db, frames, &req)
                    .map_err(query_kernel_error)
            })
            .await?;
        Ok(text_result(merge_l0(
            serde_json::to_value(&result).unwrap_or_default(),
        )))
    }

    #[tool(
        description = "SIL read kernel (R3): hard-capped raw read of journal_frames, events, or ticks. Requires startMs and endMs — unbounded windows are rejected. Optional store=hot (default) or store=cold for journal_frames only (events/ticks stay on SQLite). Use run_job when you need a bulk artifact instead of tokens. Every result includes N and reliabilityTier. Trust Level L0. Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn query_raw(
        &self,
        Parameters(params): Parameters<QueryRawParams>,
    ) -> Result<CallToolResult, McpError> {
        let req = the_desk_backend::research::query_kernel::QueryRawRequest {
            window: query_window_from_params(
                params.start_ms,
                params.end_ms,
                params.session_type,
                params.symbols,
            ),
            source: params.source.unwrap_or_else(|| "journal_frames".into()),
            limit: params.limit.map(|n| n as usize),
        };
        let store = params.store.clone();
        let cold_dir = self.cold_frames_dir.clone();
        let result = self
            .with_read_db(move |db| {
                let cold = the_desk_backend::engine::ColdFrameStore::new(cold_dir);
                let frames = frame_read_for(db, store.as_deref(), &cold)?;
                the_desk_backend::research::query_kernel::query_raw_with(db, frames, &req)
                    .map_err(query_kernel_error)
            })
            .await?;
        Ok(text_result(merge_l0(
            serde_json::to_value(&result).unwrap_or_default(),
        )))
    }

    #[tool(
        description = "SIL read kernel: run a series/episodes/raw query as an async job and return a job id plus artifact handle (columnar path + summary). Never returns the full row set as tokens. Optional store=hot (SQLite window, default) or store=cold (session-partitioned Journal Frame dumps; events/ticks stay on SQLite). Does not mutate playbook, risk, journal, memory, hypothesis, or orders. Trust Level L0. Enable via [sil].catalog_discovery."
    )]
    pub(crate) async fn run_job(
        &self,
        Parameters(params): Parameters<RunJobParams>,
    ) -> Result<CallToolResult, McpError> {
        let kind_raw = params
            .kind
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid_params_error("run_job requires kind=series|episodes|raw"))?;
        let kind = the_desk_backend::research::query_kernel::QueryKind::parse(kind_raw)
            .map_err(query_kernel_error)?;
        let _store_kind = parse_store_kind(params.store.as_deref())?;
        let mut request = match kind {
            the_desk_backend::research::query_kernel::QueryKind::Series => serde_json::to_value(
                the_desk_backend::research::query_kernel::QuerySeriesRequest {
                    window: query_window_from_params(
                        params.start_ms,
                        params.end_ms,
                        params.session_type,
                        params.symbols,
                    ),
                    fields: params.fields.unwrap_or_default(),
                },
            )
            .unwrap_or_default(),
            the_desk_backend::research::query_kernel::QueryKind::Episodes => serde_json::to_value(
                the_desk_backend::research::query_kernel::QueryEpisodesRequest {
                    window: query_window_from_params(
                        params.start_ms,
                        params.end_ms,
                        params.session_type,
                        params.symbols,
                    ),
                    predicates: convert_predicates(params.predicates)?,
                    forward_direction: params.forward_direction,
                },
            )
            .unwrap_or_default(),
            the_desk_backend::research::query_kernel::QueryKind::Raw => {
                serde_json::to_value(the_desk_backend::research::query_kernel::QueryRawRequest {
                    window: query_window_from_params(
                        params.start_ms,
                        params.end_ms,
                        params.session_type,
                        params.symbols,
                    ),
                    source: params.source.unwrap_or_else(|| "journal_frames".into()),
                    limit: params.limit.map(|n| n as usize),
                })
                .unwrap_or_default()
            }
        };
        if let Some(obj) = request.as_object_mut() {
            if let Some(store) = params
                .store
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                obj.insert("store".into(), serde_json::json!(store));
            }
            if parse_store_kind(params.store.as_deref())?
                == the_desk_backend::engine::FrameStoreKind::Cold
            {
                obj.insert(
                    "coldRoot".into(),
                    serde_json::json!(self.cold_frames_dir.to_string_lossy()),
                );
            }
        }
        let job_id = the_desk_backend::research::query_kernel::new_research_job_id();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.upsert_research_query_job(
                &the_desk_backend::research::query_kernel::queued_research_job_record(
                    &job_id, kind, &request, now_ms,
                ),
            )
            .map_err(db_error)?;
        }
        let artifact_dir = self.research_artifact_dir.clone();
        let request_for_exec = request.clone();
        let job_id_for_exec = job_id.clone();
        let exec = self
            .with_read_db(move |db| {
                the_desk_backend::research::query_kernel::execute_research_job(
                    db,
                    kind,
                    &request_for_exec,
                    &artifact_dir,
                    now_ms,
                    &job_id_for_exec,
                )
                .map_err(query_kernel_error)
            })
            .await;
        match exec {
            Ok(result) => {
                {
                    let db = self.db.lock().map_err(|_| lock_error())?;
                    the_desk_backend::research::query_kernel::persist_research_job(
                        &db, &result, &request, now_ms, None,
                    )
                    .map_err(query_kernel_error)?;
                }
                Ok(text_result(merge_l0(
                    serde_json::to_value(&result).unwrap_or_default(),
                )))
            }
            Err(e) => {
                if let Ok(db) = self.db.lock() {
                    let failed =
                        the_desk_backend::research::query_kernel::failed_research_job_result(
                            job_id, kind,
                        );
                    let _ = the_desk_backend::research::query_kernel::persist_research_job(
                        &db,
                        &failed,
                        &request,
                        now_ms,
                        Some(e.to_string()),
                    );
                }
                Err(e)
            }
        }
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
        let (ir_frames, truncated) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            load_feature_ir_eval_frames(
                &db,
                &[],
                catalog,
                ts,
                params.fields.as_deref(),
                params.domains.as_deref(),
            )
        };

        let note_missing = "as_of Journal Frame unavailable; domains degraded — Your playbook indicates historical structure is incomplete";
        let mut by_root = std::collections::BTreeMap::new();
        for root in &requested_roots {
            let (snapshot, data_time, degraded, note) = match journal.as_ref() {
                Some(snap) => match snap.by_root.get(root.as_str()) {
                    Some(payload) if !payload.is_null() => (
                        Some(stamp_get_state_snapshot(
                            payload,
                            catalog,
                            &ir_frames,
                            root.as_str(),
                            snap.clock_ms,
                            FeatureIrEvalPath::Historical,
                            truncated,
                        )),
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

/// Fold published engine rows that are ahead of SQLite into the coaching view.
///
/// SQLite stays the overnight source of truth. Published rows fill the live gap
/// before the first journal flush, and any events newer than the newest DB row
/// after a flush. When SQLite has rows but the type filter emptied the page,
/// published rows are not used as a replacement.
fn merge_published_live_events(
    mut events: Vec<KernelEvent>,
    db_had_rows: bool,
    published_events: &[serde_json::Value],
    since_ms: Option<f64>,
    event_type: Option<&str>,
    limit: usize,
) -> Vec<KernelEvent> {
    if events.is_empty() && db_had_rows {
        return events;
    }
    let newest_db_ts = events
        .iter()
        .map(|event| event.timestamp_ms)
        .fold(f64::NEG_INFINITY, f64::max);
    let mut extra = Vec::new();
    for value in published_events.iter().rev() {
        let root = value
            .get("rootSymbol")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let Ok(ev) =
            serde_json::from_value::<the_desk_backend::pipelines::MarketEvent>(value.clone())
        else {
            continue;
        };
        if !ev.timestamp_ms.is_finite() || ev.timestamp_ms <= newest_db_ts {
            continue;
        }
        if let Some(min_ts) = since_ms {
            if ev.timestamp_ms < min_ts {
                continue;
            }
        }
        extra.push(kernel_event_from_market_event_scoped(
            &ev,
            root.as_deref(),
            None,
        ));
    }
    if extra.is_empty() {
        return events;
    }
    events.extend(extra);
    events = collapse_events_latest_per_dedup(events);
    if let Some(want) = event_type {
        events.retain(|event| event.event_type.eq_ignore_ascii_case(want));
    }
    if events.len() > limit {
        events.truncate(limit);
    }
    events
}

fn query_window_from_params(
    start_ms: Option<f64>,
    end_ms: Option<f64>,
    session_type: Option<String>,
    symbols: Option<Vec<String>>,
) -> the_desk_backend::research::query_kernel::QueryWindow {
    the_desk_backend::research::query_kernel::QueryWindow {
        start_ms,
        end_ms,
        session_type,
        symbols,
    }
}

fn parse_store_kind(
    raw: Option<&str>,
) -> Result<the_desk_backend::engine::FrameStoreKind, McpError> {
    match raw
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("hot")
        .to_ascii_lowercase()
        .as_str()
    {
        "hot" => Ok(the_desk_backend::engine::FrameStoreKind::Hot),
        "cold" => Ok(the_desk_backend::engine::FrameStoreKind::Cold),
        other => Err(invalid_params_error(format!(
            "unknown frame store `{other}` (expected hot or cold)"
        ))),
    }
}

fn frame_read_for<'a>(
    db: &'a the_desk_backend::db::Database,
    store: Option<&str>,
    cold: &'a the_desk_backend::engine::ColdFrameStore,
) -> Result<the_desk_backend::engine::JournalFrameRead<'a>, McpError> {
    match parse_store_kind(store)? {
        the_desk_backend::engine::FrameStoreKind::Hot => {
            Ok(the_desk_backend::engine::JournalFrameRead::Hot(db))
        }
        the_desk_backend::engine::FrameStoreKind::Cold => {
            Ok(the_desk_backend::engine::JournalFrameRead::Cold(cold))
        }
    }
}

fn convert_predicates(
    raw: Option<Vec<CatalogPredicateParams>>,
) -> Result<Vec<the_desk_backend::research::query_kernel::CatalogPredicate>, McpError> {
    let mut out = Vec::new();
    for pred in raw.unwrap_or_default() {
        let field = pred.field.unwrap_or_default();
        let op_raw = pred.op.unwrap_or_else(|| "eq".into());
        let op = the_desk_backend::research::query_kernel::PredicateOp::parse(&op_raw)
            .map_err(query_kernel_error)?;
        if field.trim().is_empty()
            && pred
                .event_type
                .as_deref()
                .is_none_or(|s| s.trim().is_empty())
        {
            return Err(invalid_params_error(
                "each predicate requires `field` or `eventType`",
            ));
        }
        out.push(the_desk_backend::research::query_kernel::CatalogPredicate {
            id: pred.id,
            symbol: pred.symbol,
            field,
            op,
            value: pred.value,
            path: pred.path,
            tolerance_ticks: pred.tolerance_ticks,
            event_type: pred.event_type,
        });
    }
    Ok(out)
}

fn query_kernel_error(e: the_desk_backend::research::query_kernel::QueryKernelError) -> McpError {
    use the_desk_backend::research::query_kernel::QueryKernelError;
    match e {
        QueryKernelError::Db(_) | QueryKernelError::Io(_) => db_error(e),
        other => invalid_params_error(other.to_string()),
    }
}

fn merge_l0(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        obj.entry("trustLevel".to_string())
            .or_insert(serde_json::json!(TrustLevel::L0));
        obj.insert("mutationAuthority".into(), serde_json::json!(false));
        obj.insert("orderAuthority".into(), serde_json::json!(false));
    }
    value
}

/// Desk Catalog plus Feature Registry overlay rows (discovery only).
fn catalog_with_registry_overlay(
    server: &TheDeskMcp,
) -> Result<the_desk_backend::catalog::DeskCatalog, McpError> {
    let db = server.db.lock().map_err(|_| lock_error())?;
    let overlay = db.list_feature_registry().map_err(db_error)?;
    Ok(build_catalog_with_overlay(overlay))
}

/// Non-empty `rootSymbol` on a live snapshot (empty string is treated as unknown).
fn snapshot_root_symbol(snapshot: Option<&serde_json::Value>) -> Option<&str> {
    snapshot
        .and_then(|s| s.get("rootSymbol"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Loads a Feature-IR eval window from SQLite (historical `as_of` path).
///
/// Never `list_journal_frames()`. Skips the journal read when the request
/// cannot resolve a codegen field.
fn load_feature_ir_eval_frames(
    db: &Database,
    pending: &[JournalFrameRecord],
    catalog: &the_desk_backend::catalog::DeskCatalog,
    as_of_ms: f64,
    fields: Option<&[String]>,
    domains: Option<&[String]>,
) -> (Vec<FeatureIrFrame>, bool) {
    if !request_needs_derived_stamp(catalog, fields, domains) {
        return (Vec::new(), false);
    }
    let (history, truncated) = db
        .list_journal_frames_for_feature_ir(as_of_ms, FEATURE_IR_EVAL_MAX_FRAMES)
        .map(|(rows, truncated)| (rows.iter().map(Into::into).collect(), truncated))
        .unwrap_or_else(|_| (Vec::new(), false));
    let pending_ir = pending.iter().map(Into::into).collect();
    let merged = merge_eval_frames(history, truncated, pending_ir, FEATURE_IR_EVAL_MAX_FRAMES);
    (merged.frames, merged.truncated)
}

fn load_live_feature_ir_frames(
    server: &TheDeskMcp,
    catalog: &the_desk_backend::catalog::DeskCatalog,
    as_of_ms: f64,
    fields: Option<&[String]>,
    domains: Option<&[String]>,
) -> Result<(Vec<FeatureIrFrame>, bool), McpError> {
    if !request_needs_derived_stamp(catalog, fields, domains) {
        return Ok((Vec::new(), false));
    }
    let pending = server.market_router.snapshot_pending_journal_frames();
    {
        let db = server.db.lock().map_err(|_| lock_error())?;
        server
            .market_router
            .hydrate_feature_ir_eval_cache(&db, as_of_ms)
            .map_err(db_error)?;
    }
    let merged = server.market_router.live_feature_ir_eval_window(&pending);
    Ok((merged.frames, merged.truncated))
}

fn stamp_get_state_snapshot(
    snapshot: &serde_json::Value,
    catalog: &the_desk_backend::catalog::DeskCatalog,
    frames: &[FeatureIrFrame],
    eval_root: &str,
    as_of_ms: f64,
    path: FeatureIrEvalPath,
    truncated: bool,
) -> serde_json::Value {
    let mut payload = snapshot.clone();
    stamp_derived_feature_payload(
        &mut payload,
        FeatureIrStore {
            catalog,
            frames,
            events: &[],
            eval_root,
            window_truncated: truncated,
        },
        as_of_ms,
        path,
    );
    payload
}
