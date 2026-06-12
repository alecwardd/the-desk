//! Live market structure reads: snapshot, TPO, delta, levels, tape, footprint, pipelines.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::et_minutes_from_timestamp;
use the_desk_backend::research;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = market_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), Globex/London opening ranges, and session context (sessionType, sessionSegment, tradingDay), plus tape pace, imbalance count, absorption event count, and average trade size. Prefers live pipeline state; falls back to last persisted snapshot."
    )]
    pub(crate) async fn get_market_snapshot(&self) -> Result<CallToolResult, McpError> {
        if let Some(out) = self.current_market_snapshot_payload() {
            return Ok(text_result(out));
        }
        Ok(no_data(
            "No market snapshot available yet or database is temporarily busy. Ensure Sierra Chart is running and .scid data is being ingested.",
        ))
    }

    #[tool(
        description = "Context frame for agent interpretation. Call this when you need session-relative framing, stable buckets, historical analogs, forward-path caveats, or setup-linked outcome context; use get_market_snapshot for raw values, get_session_context for session identity, compare_sessions for explicit analog-only research, and get_attention_inbox for what deserves attention now."
    )]
    pub(crate) async fn get_context_frame(
        &self,
        Parameters(params): Parameters<ContextFrameParams>,
    ) -> Result<CallToolResult, McpError> {
        let include_historical = params.include_historical.unwrap_or(true);
        let setup_id = parse_optional_non_empty_string("setupId", params.setup_id.as_deref())?;
        let matching_mode = match params
            .matching_mode
            .as_deref()
            .unwrap_or("weightedAnalog")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "weightedanalog" | "weighted_analog" => {
                research::context_frame::MatchingMode::WeightedAnalog
            }
            "strictbucket" | "strict_bucket" => research::context_frame::MatchingMode::StrictBucket,
            other => {
                return Err(invalid_params_error(format!(
                    "matchingMode must be weightedAnalog or strictBucket, got: {other}"
                )))
            }
        };
        let (snapshot, options) = if let Some(timestamp_ms) = params.timestamp_ms {
            if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 {
                return Err(invalid_params_error(
                    "timestampMs must be a positive finite epoch-milliseconds value",
                ));
            }
            let (snapshot_ts, payload) = self
                .db
                .lock()
                .map_err(|_| lock_error())?
                .get_snapshot_near(timestamp_ms)
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error(
                        "No historical pipeline snapshots are available for frameAt(timestampMs)",
                    )
                })?;
            (
                payload,
                research::context_frame::ContextFrameOptions {
                    mode: research::context_frame::ContextFrameMode::Historical,
                    requested_timestamp_ms: Some(timestamp_ms),
                    snapshot_timestamp_ms: Some(snapshot_ts),
                    snapshot_distance_ms: Some((snapshot_ts - timestamp_ms).abs()),
                    setup_id: setup_id.clone(),
                    include_historical,
                    matching_mode: matching_mode.clone(),
                },
            )
        } else if let Some(r) = self.resolve_live_market_view() {
            (
                r.snapshot,
                research::context_frame::ContextFrameOptions {
                    mode: research::context_frame::ContextFrameMode::Live,
                    snapshot_timestamp_ms: Some(r.as_of_timestamp_ms),
                    setup_id: setup_id.clone(),
                    include_historical,
                    matching_mode: matching_mode.clone(),
                    ..Default::default()
                },
            )
        } else {
            let payload = self
                .db
                .lock()
                .map_err(|_| lock_error())?
                .latest_feature_state()
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error("No live or persisted market snapshot is available")
                })?;
            (
                payload,
                research::context_frame::ContextFrameOptions {
                    mode: research::context_frame::ContextFrameMode::Live,
                    setup_id: setup_id.clone(),
                    include_historical,
                    matching_mode: matching_mode.clone(),
                    ..Default::default()
                },
            )
        };

        let cache_key = research::context_frame::cache_key_for_snapshot(&snapshot, &options);
        if include_historical {
            if let Ok(cache) = self.context_frame_cache.lock() {
                if let Some(cached) = cache.get(&cache_key) {
                    let mut frame = cached.clone();
                    frame.meta.cache_status = "hit".to_string();
                    return Ok(text_result(
                        serde_json::to_value(frame).unwrap_or_else(|_| serde_json::json!({})),
                    ));
                }
            }
        }

        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut frame = research::context_frame::build_context_frame(&db, &snapshot, options)
            .map_err(db_error)?;
        frame.meta.cache_status = if include_historical {
            "miss".to_string()
        } else {
            "bypassed".to_string()
        };
        if include_historical {
            if let Ok(mut cache) = self.context_frame_cache.lock() {
                if cache.len() >= CONTEXT_FRAME_CACHE_LIMIT {
                    if let Some(first_key) = cache.keys().next().cloned() {
                        cache.remove(&first_key);
                    }
                }
                cache.insert(cache_key, frame.clone());
            }
        }
        Ok(text_result(
            serde_json::to_value(frame).unwrap_or_else(|_| serde_json::json!({})),
        ))
    }

    #[tool(
        description = "Current session context: sessionType (RTH/Globex/Unknown), sessionSegment (Asia/London/None), tradingDay (6 PM ET roll), data freshness, and contract rollover status."
    )]
    pub(crate) async fn get_session_context(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let (_, active_contract) = self.resolve_contract_cached();
            let et_minutes = et_minutes_from_timestamp(r.as_of_timestamp_ms).unwrap_or(-1);
            let (is_transition, transition_from, transition_to, transition_phase) =
                if let Some((from, to, phase)) = transition_hint(et_minutes) {
                    (
                        true,
                        serde_json::json!(from),
                        serde_json::json!(to),
                        serde_json::json!(phase),
                    )
                } else {
                    (
                        false,
                        serde_json::Value::Null,
                        serde_json::Value::Null,
                        serde_json::Value::Null,
                    )
                };
            let mut out = serde_json::json!({
                "sessionType": s.get("sessionType"),
                "sessionSegment": s.get("sessionSegment"),
                "tradingDay": s.get("tradingDay"),
                "rootSymbol": s.get("rootSymbol"),
                "contractSymbol": s.get("contractSymbol"),
                "contractMonth": s.get("contractMonth"),
                "symbolResolutionMode": s.get("symbolResolutionMode"),
                "symbolResolutionSource": s.get("symbolResolutionSource"),
                "rolloverWarning": s.get("rolloverWarning"),
                "carryForwardLevelsValid": s.get("carryForwardLevelsValid"),
                "isTransition": is_transition,
                "transitionFrom": transition_from,
                "transitionTo": transition_to,
                "transitionPhase": transition_phase,
                "etMinutes": et_minutes,
            });
            let rollover_date = s
                .get("tradingDay")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .unwrap_or_else(the_desk_backend::et_now_trading_day);
            let server_contract = self.current_pipeline_contract_metadata();
            let db = self.db.lock().map_err(|_| lock_error())?;
            let rollover_status = self.rollover_status_for_date(
                &db,
                &active_contract,
                server_contract.as_ref(),
                &rollover_date,
                Some(r.data_age_ms),
            )?;
            out["rolloverStatus"] =
                serde_json::to_value(rollover_status).unwrap_or_else(|_| serde_json::json!({}));
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No session context available"))
    }

    #[tool(
        description = "TPO (Time-Price-Opportunity) profile data: POC (point of control), value area high/low, opening range high/low (first 30 min), initial balance high/low (first 60 min). Use for auction market theory analysis."
    )]
    pub(crate) async fn get_tpo_profile(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let mut out = serde_json::json!({
                "poc": s.get("poc"),
                "vaHigh": s.get("vaHigh"),
                "vaLow": s.get("vaLow"),
                "orHigh": s.get("orHigh"),
                "orLow": s.get("orLow"),
                "ibHigh": s.get("ibHigh"),
                "ibLow": s.get("ibLow"),
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No TPO data available"))
    }

    #[tool(
        description = "Delta profile: segment delta (Asia-only, London-only, or RTH-only), combined Globex delta (Asia+London when in Globex), cumulative delta, DNVA high/low, DNP. Use for inventory and positioning analysis."
    )]
    pub(crate) async fn get_delta_profile(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let mut out = serde_json::json!({
                "sessionDelta": s.get("sessionDelta"),
                "globexDelta": s.get("globexDelta"),
                "cumulativeDelta": s.get("cumulativeDelta"),
                "dnvaHigh": s.get("dnvaHigh"),
                "dnvaLow": s.get("dnvaLow"),
                "dnp": s.get("dnp"),
                "sessionSegment": s.get("sessionSegment"),
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No delta data available"))
    }

    #[tool(
        description = "Key reference levels: prior day high/low/close, prior session value area high/low and POC, overnight (Globex) high/low, Globex OR30 and London OR60, and initial balance high/low. Includes sessionType, tradingDay, and contract rollover status so agents can gate carry-forward references."
    )]
    pub(crate) async fn get_key_levels(&self) -> Result<CallToolResult, McpError> {
        let Some(r) = self.resolve_live_market_view() else {
            return Ok(no_data("No key levels available"));
        };
        let s = &r.snapshot;
        let (_, active_contract) = self.resolve_contract_cached();
        let session_type = s.get("sessionType").and_then(|v| v.as_str());
        let session_segment = s.get("sessionSegment").and_then(|v| v.as_str());
        let trading_day = s.get("tradingDay").and_then(|v| v.as_str());
        let is_globex = session_type == Some("Globex");

        let server_contract = self.current_pipeline_contract_metadata();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut out = serde_json::json!({
            "sessionType": s.get("sessionType"),
            "sessionSegment": s.get("sessionSegment"),
            "tradingDay": s.get("tradingDay"),
            "rootSymbol": s.get("rootSymbol"),
            "contractSymbol": s.get("contractSymbol"),
            "contractMonth": s.get("contractMonth"),
            "symbolResolutionMode": s.get("symbolResolutionMode"),
            "symbolResolutionSource": s.get("symbolResolutionSource"),
            "rolloverWarning": s.get("rolloverWarning"),
            "carryForwardLevelsValid": s.get("carryForwardLevelsValid"),
            "priorDayContractSymbol": s.get("priorDayContractSymbol"),
            "priorDayHigh": s.get("priorDayHigh"),
            "priorDayLow": s.get("priorDayLow"),
            "priorDayClose": s.get("priorDayClose"),
            "priorVaHigh": s.get("priorVaHigh"),
            "priorVaLow": s.get("priorVaLow"),
            "priorPoc": s.get("priorPoc"),
            "priorDnvaHigh": s.get("priorDnvaHigh"),
            "priorDnvaLow": s.get("priorDnvaLow"),
            "priorDnp": s.get("priorDnp"),
            "overnightHigh": s.get("overnightHigh"),
            "overnightLow": s.get("overnightLow"),
            "globexOr30High": s.get("globexOr30High"),
            "globexOr30Low": s.get("globexOr30Low"),
            "londonOr60High": s.get("londonOr60High"),
            "londonOr60Low": s.get("londonOr60Low"),
            "sessionHigh": s.get("sessionHigh"),
            "sessionLow": s.get("sessionLow"),
            "ibHigh": s.get("ibHigh"),
            "ibLow": s.get("ibLow"),
            "priorLondonDnvaHigh": serde_json::Value::Null,
            "priorLondonDnvaLow": serde_json::Value::Null,
            "priorLondonDnp": serde_json::Value::Null,
            "priorAsiaDnvaHigh": serde_json::Value::Null,
            "priorAsiaDnvaLow": serde_json::Value::Null,
            "priorAsiaDnp": serde_json::Value::Null,
            "untestedDnps": serde_json::json!([]),
        });
        let rollover_date = trading_day
            .map(ToString::to_string)
            .unwrap_or_else(the_desk_backend::et_now_trading_day);
        let rollover_status = self.rollover_status_for_date(
            &db,
            &active_contract,
            server_contract.as_ref(),
            &rollover_date,
            Some(r.data_age_ms),
        )?;
        out["rolloverStatus"] =
            serde_json::to_value(rollover_status).unwrap_or_else(|_| serde_json::json!({}));
        if is_globex {
            out["sessionScopeNote"] = serde_json::json!("For Globex, use overnightHigh/overnightLow as the session range. sessionHigh, sessionLow, IB, OR, and OR5 are RTH-only and may be zero or from a prior RTH session.");
        }
        let (london_dnva, asia_dnva) =
            load_contextual_prior_dnva(&db, session_type, session_segment, trading_day);
        if let Some((h, l, p)) = london_dnva {
            out["priorLondonDnvaHigh"] = serde_json::json!(h);
            out["priorLondonDnvaLow"] = serde_json::json!(l);
            out["priorLondonDnp"] = serde_json::json!(p);
        }
        if let Some((h, l, p)) = asia_dnva {
            out["priorAsiaDnvaHigh"] = serde_json::json!(h);
            out["priorAsiaDnvaLow"] = serde_json::json!(l);
            out["priorAsiaDnp"] = serde_json::json!(p);
        }
        if let Ok(untested) = db.load_untested_dnps(10) {
            let list: Vec<serde_json::Value> = untested
                .into_iter()
                .map(|(sd, st, dnp)| {
                    serde_json::json!({
                        "sessionDate": sd,
                        "sessionType": st,
                        "dnp": dnp
                    })
                })
                .collect();
            out["untestedDnps"] = serde_json::json!(list);
        }
        merge_tool_live_metadata(&mut out, &r);
        Ok(text_result(out))
    }

    #[tool(
        description = "Tape pace analytics with coverage-aware rolling ticks/sec and volume/sec over 5-second, 30-second, and 5-minute windows. Returns both session-relative and rolling-context pace percentiles, smoothed normalized acceleration plus raw acceleration, 30-minute regime baselines, window validity/coverage, dwell at current price, and explicit data quality metadata so agents can distinguish live vs stale tape context."
    )]
    pub(crate) async fn get_tape_pace(&self) -> Result<CallToolResult, McpError> {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let live_view = self.resolve_live_market_view();
        let data_age_ms = live_view
            .as_ref()
            .map(|r| r.data_age_ms)
            .unwrap_or_else(|| self.data_age_from_db_or_atomic());
        // Try live pipeline first for full snapshot including volume/sec and dwell.
        // Use try_lock to avoid blocking when backfill/poll holds the lock.
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let snap = pipelines.tape_pace.snapshot(now_ms);
            let last_price = pipelines.levels.last_price;
            let payload = serde_json::json!({
                "ticksPerSec5s": snap.ticks_per_sec_5s,
                "ticksPerSec30s": snap.ticks_per_sec_30s,
                "ticksPerSec5m": snap.ticks_per_sec_5m,
                "volumePerSec5s": snap.volume_per_sec_5s,
                "volumePerSec30s": snap.volume_per_sec_30s,
                "volumePerSec5m": snap.volume_per_sec_5m,
                "acceleration": snap.acceleration,
                "rawAcceleration": snap.raw_acceleration,
                "pacePercentile": snap.pace_percentile,
                "rollingPacePercentile": snap.rolling_pace_percentile,
                "regimeTicksPerSec30mEma": snap.regime_ticks_per_sec_30m_ema,
                "regimeVolumePerSec30mEma": snap.regime_volume_per_sec_30m_ema,
                "windowCoverage5s": snap.coverage_5s,
                "windowCoverage30s": snap.coverage_30s,
                "windowCoverage5m": snap.coverage_5m,
                "isValid5s": snap.valid_5s,
                "isValid30s": snap.valid_30s,
                "isValid5m": snap.valid_5m,
                "windowAnchorTimestampMs": snap.window_anchor_timestamp_ms,
                "lastTradeTimestampMs": snap.last_trade_timestamp_ms,
                "dwellAtCurrentPriceMs": if last_price > 0.0 {
                    pipelines.tape_pace.dwell_at_price(last_price, now_ms)
                } else {
                    None
                },
                "currentPrice": if last_price > 0.0 { Some(last_price) } else { None::<f64> },
            });
            let mut out = build_tape_pace_response(payload, data_age_ms, true, now_ms);
            if let Some(ref r) = live_view {
                merge_tool_live_metadata(&mut out, r);
            }
            return Ok(text_result(out));
        }
        // Fallback to DB
        match self
            .db
            .lock()
            .ok()
            .and_then(|d| d.latest_feature_state_with_timestamp().ok().flatten())
        {
            Some((_, s)) => {
                let payload = serde_json::json!({
                    "ticksPerSec5s": s.get("tapePace5s").cloned().unwrap_or(serde_json::Value::Null),
                    "ticksPerSec30s": s.get("tapePace30s").cloned().unwrap_or(serde_json::Value::Null),
                    "ticksPerSec5m": s.get("tapePace5m").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec5s": s.get("tapeVolumePerSec5s").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec30s": s.get("tapeVolumePerSec30s").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec5m": s.get("tapeVolumePerSec5m").cloned().unwrap_or(serde_json::Value::Null),
                    "acceleration": s.get("tapeAcceleration").cloned().unwrap_or(serde_json::Value::Null),
                    "rawAcceleration": s.get("tapeRawAcceleration").cloned().unwrap_or(serde_json::Value::Null),
                    "pacePercentile": s.get("pacePercentile").cloned().unwrap_or(serde_json::Value::Null),
                    "rollingPacePercentile": s.get("tapeRollingPercentile").cloned().unwrap_or(serde_json::Value::Null),
                    "regimeTicksPerSec30mEma": s.get("tapeRegimeTicksPerSec30mEma").cloned().unwrap_or(serde_json::Value::Null),
                    "regimeVolumePerSec30mEma": s.get("tapeRegimeVolumePerSec30mEma").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage5s": s.get("tapeCoverage5s").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage30s": s.get("tapeCoverage30s").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage5m": s.get("tapeCoverage5m").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid5s": s.get("tapeValid5s").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid30s": s.get("tapeValid30s").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid5m": s.get("tapeValid5m").cloned().unwrap_or(serde_json::Value::Null),
                    "windowAnchorTimestampMs": s.get("tapeWindowAnchorTimestampMs").cloned().unwrap_or(serde_json::Value::Null),
                    "lastTradeTimestampMs": s.get("tapeLastTradeTimestampMs").cloned().unwrap_or(serde_json::Value::Null),
                    "dwellAtCurrentPriceMs": s.get("tapeDwellAtCurrentPriceMs").cloned().unwrap_or(serde_json::Value::Null),
                    "currentPrice": s.get("lastPrice").cloned().unwrap_or(serde_json::Value::Null),
                });
                let mut out = build_tape_pace_response(payload, data_age_ms, false, now_ms);
                if let Some(ref r) = live_view {
                    merge_tool_live_metadata(&mut out, r);
                }
                Ok(text_result(out))
            }
            None => Ok(no_data("No tape pace data")),
        }
    }

    #[tool(
        description = "Footprint / volume-at-price data for the current session: top price levels by total volume with bid volume, ask volume, delta, and delta-per-volume ratio. Use price_low/price_high to focus on a specific price zone (e.g. near a key level). For a time-windowed footprint showing what happened at a specific time, use get_footprint_window instead."
    )]
    pub(crate) async fn get_footprint(
        &self,
        Parameters(params): Parameters<FootprintParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let mut all_levels = pipelines.footprint.levels();
            // Apply optional price range filter before sorting/truncating.
            if params.price_low.is_some() || params.price_high.is_some() {
                all_levels.retain(|(price, _)| {
                    if let Some(lo) = params.price_low {
                        if *price < lo {
                            return false;
                        }
                    }
                    if let Some(hi) = params.price_high {
                        if *price > hi {
                            return false;
                        }
                    }
                    true
                });
            }
            // Sort by total volume descending, return top 30.
            all_levels.sort_by(|a, b| {
                b.1.total()
                    .partial_cmp(&a.1.total())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top: Vec<serde_json::Value> = all_levels
                .iter()
                .take(30)
                .map(|(price, lvl)| {
                    serde_json::json!({
                        "price": price,
                        "bidVolume": lvl.bid_volume,
                        "askVolume": lvl.ask_volume,
                        "totalVolume": lvl.total(),
                        "delta": lvl.delta(),
                        "deltaPerVolume": lvl.delta_per_volume(),
                        "imbalanceRatio": lvl.imbalance_ratio(),
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "topLevelsByVolume": top,
                "totalPriceLevels": all_levels.len(),
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Per-level detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No footprint data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Time-windowed footprint: bid/ask volume at each price level traded between start_time_ms and end_time_ms. Ideal for reconstructing what happened at a specific price during a specific time window — e.g. 'show me the footprint at the overnight low between 20:00 and 20:10'. Results are sorted by price ascending. Use get_market_snapshot to find current timestamp_ms, then subtract milliseconds to target earlier windows. Optionally narrow the price range with price_low/price_high."
    )]
    pub(crate) async fn get_footprint_window(
        &self,
        Parameters(params): Parameters<FootprintWindowParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let start = params.start_time_ms.unwrap_or(0.0);
            let end = params.end_time_ms.unwrap_or(f64::MAX);
            let mut levels = pipelines.footprint.levels_in_window(start, end);
            // Apply optional price range filter.
            if params.price_low.is_some() || params.price_high.is_some() {
                levels.retain(|(price, _)| {
                    if let Some(lo) = params.price_low {
                        if *price < lo {
                            return false;
                        }
                    }
                    if let Some(hi) = params.price_high {
                        if *price > hi {
                            return false;
                        }
                    }
                    true
                });
            }
            let total_volume: f64 = levels.iter().map(|(_, l)| l.total()).sum();
            let net_delta: f64 = levels.iter().map(|(_, l)| l.delta()).sum();
            let level_count = levels.len();
            let level_data: Vec<serde_json::Value> = levels
                .iter()
                .map(|(price, lvl)| {
                    serde_json::json!({
                        "price": price,
                        "bidVolume": lvl.bid_volume,
                        "askVolume": lvl.ask_volume,
                        "totalVolume": lvl.total(),
                        "delta": lvl.delta(),
                        "deltaPerVolume": lvl.delta_per_volume(),
                        "imbalanceRatio": lvl.imbalance_ratio(),
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "levels": level_data,
                "levelCount": level_count,
                "windowStartMs": start,
                "windowEndMs": if end == f64::MAX { serde_json::Value::Null } else { serde_json::json!(end) },
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "summary": {
                    "totalVolume": total_volume,
                    "netDelta": net_delta,
                },
                "note": "In-memory current session only. For historical sessions, use query_ticks with time and price filters.",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        Err(McpError::internal_error("Pipeline lock unavailable", None))
    }

    #[tool(
        description = "Per-price TPO letter detail for the current session: shows which 30-minute brackets (A, B, C, …) printed at each price level. Bracket A = first 30 min (Opening Range), B = 30-60 min (completes IB), C onwards = regular session. Single-print levels (is_single_print: true) are tail/excess candidates. Use price_low/price_high to focus on a specific price zone."
    )]
    pub(crate) async fn get_tpo_detail(
        &self,
        Parameters(params): Parameters<TpoDetailParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let detail = pipelines
                .tpo
                .tpo_letter_detail(params.price_low, params.price_high);
            let single_print_prices: Vec<f64> = detail
                .iter()
                .filter(|d| d.is_single_print)
                .map(|d| d.price)
                .collect();
            let level_count = detail.len();
            let single_count = single_print_prices.len();
            return Ok(text_result(serde_json::json!({
                "levels": detail,
                "levelCount": level_count,
                "singlePrintCount": single_count,
                "singlePrintPrices": single_print_prices,
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "note": "In-memory current session only. Brackets: A=0 (OR), B=1 (completes IB), C=2, D=3, ...",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        Err(McpError::internal_error("Pipeline lock unavailable", None))
    }

    #[tool(
        description = "Historical pipeline snapshot nearest to a given timestamp. Pipeline state (VWAP, POC, VA, delta, day type, etc.) is stored every ~30 seconds. Use this to answer 'what was the market structure at 20:00?' — pass that time as epoch milliseconds. The response includes the actual snapshot timestamp so you can see how close the match is. Use get_market_snapshot to get the current timestamp_ms and work backward."
    )]
    pub(crate) async fn get_snapshot_at(
        &self,
        Parameters(params): Parameters<SnapshotAtParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let target_ms = params
            .timestamp_ms
            .unwrap_or_else(|| db.latest_tick_timestamp_ms().ok().flatten().unwrap_or(0.0));
        match db.get_snapshot_near(target_ms) {
            Ok(Some((snapshot_ts, payload))) => Ok(text_result(serde_json::json!({
                "snapshot": payload,
                "snapshotTimestampMs": snapshot_ts,
                "requestedTimestampMs": target_ms,
                "offsetMs": snapshot_ts - target_ms,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No pipeline snapshots found. Snapshots are stored every ~30s once data is flowing.")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Stacked and diagonal imbalance detection from the footprint. Stacked: 3+ consecutive levels where one side dominates (>2:1 ratio) -- shows directional conviction. Diagonal: aggressive lifting/hitting across adjacent levels -- shows urgency. Returns prices and direction for each type."
    )]
    pub(crate) async fn get_imbalances(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let stacked_prices = pipelines.footprint.stacked_imbalances(2.0, 3);
            let diagonals = pipelines.footprint.diagonal_imbalances(2.0);
            let diagonal_data: Vec<serde_json::Value> = diagonals
                .iter()
                .map(|(p1, p2, ratio, is_buy)| {
                    serde_json::json!({
                        "priceLow": p1,
                        "priceHigh": p2,
                        "ratio": ratio,
                        "direction": if *is_buy { "buy" } else { "sell" },
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "stackedImbalancePrices": stacked_prices,
                "stackedCount": stacked_prices.len(),
                "diagonalImbalances": diagonal_data,
                "diagonalCount": diagonals.len(),
                "note": "Stacked: 3+ consecutive levels with >2:1 imbalance ratio. Diagonal: adjacent-level bid/ask imbalances >2:1.",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Stacked/diagonal detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No imbalance data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Recent absorption-flow lifecycle events (absorption, exhaustion, delta divergence). Each event includes subtype, candidate/confirmed/invalidated status, zone bounds, direction, regime metadata, and severity."
    )]
    pub(crate) async fn get_absorption_events(
        &self,
        Parameters(params): Parameters<LimitParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(25) as usize;

        // Try live pipeline first (try_lock to avoid blocking when backfill/poll holds lock)
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let live_events = pipelines.absorption.recent_events();
            if !live_events.is_empty() {
                let events: Vec<serde_json::Value> = live_events
                    .iter()
                    .rev()
                    .take(limit)
                    .map(normalize_live_absorption_event)
                    .collect();
                return Ok(text_result(serde_json::json!({
                    "events": events,
                    "count": events.len(),
                    "source": "live_pipeline",
                    "dataAgeMs": self.data_age_from_db_or_atomic()
                })));
            }
        }

        // Fall back to market_events table (FlowEventEmitter writes absorption_* lifecycle events)
        match self.db.try_lock().ok().and_then(|db| {
            let data_age_ms = compute_data_age(&db);
            db.list_market_events_by_prefix("absorption_", limit)
                .ok()
                .map(|events| (events, data_age_ms))
        }) {
            Some((events, data_age_ms)) => {
                let normalized: Vec<serde_json::Value> =
                    events.iter().map(normalize_db_absorption_event).collect();
                Ok(text_result(serde_json::json!({
                    "events": normalized,
                    "count": normalized.len(),
                    "source": "market_events_db",
                    "dataAgeMs": data_age_ms
                })))
            }
            None => Ok(no_data(
                "No absorption data available or database is temporarily busy.",
            )),
        }
    }

    #[tool(
        description = "Trade size distribution: counts of 1-lot, 2-5 lot, 6-20 lot, and 21+ lot trades for the current session. Includes average trade size and prices where institutional (21+) lot trades clustered. Use for identifying institutional participation and footprint locations."
    )]
    pub(crate) async fn get_trade_size_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let snap = pipelines.trade_size.snapshot();
            let total_trades = snap.lot_1 + snap.lot_2_5 + snap.lot_6_20 + snap.lot_21_plus;
            let large_prices = pipelines.trade_size.large_trade_prices();
            let large_data: Vec<serde_json::Value> = large_prices
                .iter()
                .take(20)
                .map(|(price, count)| {
                    serde_json::json!({
                        "price": price,
                        "largeLotCount": count,
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "lot1": snap.lot_1,
                "lot2to5": snap.lot_2_5,
                "lot6to20": snap.lot_6_20,
                "lot21plus": snap.lot_21_plus,
                "totalTrades": total_trades,
                "avgTradeSize": snap.avg_trade_size,
                "largeTradePrices": large_data,
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Per-price detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No trade size data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Session summary: total tick count, latest tick timestamp, and latest pipeline snapshot. Provides a quick health check of data flow."
    )]
    pub(crate) async fn get_session_summary(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
        let snapshot = db.latest_feature_state().ok().flatten();
        Ok(text_result(serde_json::json!({
            "tickCount": tick_count,
            "latestTickTimestampMs": last_ts,
            "latestSnapshot": snapshot,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "5-minute Opening Range (Leo's A+ setup): OR5 high, low, midpoint (key level), break direction (None/Up/Down), whether mid has been retested after breakout, and extension targets (75%% and 100%% of range from mid)."
    )]
    pub(crate) async fn get_or5_status(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "or5High": s.get("or5High"),
                "or5Low": s.get("or5Low"),
                "or5Mid": s.get("or5Mid"),
                "or5Locked": s.get("or5Locked"),
                "or5BreakDirection": s.get("or5BreakDirection"),
                "or5MidRetested": s.get("or5MidRetested"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data(
                "No OR5 data available. RTH session may not have started.",
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Relative Volume: ratio of current session's cumulative volume vs the N-day average at the same time-of-day. Returns classification (Low/Normal/Elevated/High), percentile rank (0-100 vs history at same time), velocity (rate of change per 5-min bucket), acceleration (second derivative), bucket progress, actual vs expected volume, and lookback days. Use to calibrate participation quality and regime context."
    )]
    pub(crate) async fn get_rvol(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        // Try live pipeline first for full snapshot.
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let rvol = &pipelines.rvol;
            let actual = rvol.session_volume();
            let expected = rvol.expected_volume_at_bucket();
            let total = rvol.total_buckets();
            let bucket = rvol.bucket_index();
            let session_pct = if total > 0 {
                format!("{:.1}%", bucket as f64 / total as f64 * 100.0)
            } else {
                "0.0%".to_string()
            };
            return Ok(text_result(serde_json::json!({
                "rvolRatio": rvol.rvol_ratio(),
                "rvolClassification": format!("{:?}", rvol.classification()),
                "rvolPercentile": rvol.rvol_percentile(),
                "currentBucket": bucket,
                "totalBuckets": total,
                "sessionProgress": session_pct,
                "actualVolume": actual,
                "expectedVolume": expected,
                "volumeDelta": actual - expected,
                "velocity": rvol.rvol_velocity(),
                "acceleration": rvol.rvol_acceleration(),
                "lookbackDays": rvol.lookback_days(),
                "dataAgeMs": compute_data_age(&db),
            })));
        }
        // Fallback to DB snapshot when pipeline lock is unavailable.
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "rvolRatio": s.get("rvolRatio"),
                "rvolClassification": s.get("rvolClassification"),
                "note": "Falling back to DB snapshot. Percentile, velocity, and bucket details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No RVOL data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Day type classification (Dalton): NonTrend, Normal, NormalVariation, Neutral/NeutralCenter/NeutralExtreme, Trend, or DoubleDistributionTrend. Profile shape: Gaussian, PShape, BShape, DShape, or Elongated. Balance state: Balanced vs Imbalanced. Single prints direction relative to POC."
    )]
    pub(crate) async fn get_day_type(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "dayType": s.get("dayType"),
                "profileShape": s.get("profileShape"),
                "balanceState": s.get("balanceState"),
                "singlePrintsDirection": s.get("singlePrintsDirection"),
                "poorHigh": s.get("poorHigh"),
                "poorLow": s.get("poorLow"),
                "excessHigh": s.get("excessHigh"),
                "excessLow": s.get("excessLow"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No day type data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Active rebid/reoffer acceleration zones: price ranges of one-sided aggressive activity. Each zone has type (Buy/Sell), status (Fresh/Retested/Held/Failed), price range, volume, and delta. Key concept: 'never fade a held zone.'"
    )]
    pub(crate) async fn get_rebid_reoffer_zones(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let active: Vec<serde_json::Value> = pipelines
                .rebid_reoffer
                .active_zones()
                .iter()
                .map(|z| {
                    serde_json::json!({
                        "zoneType": z.zone_type,
                        "status": z.status,
                        "high": z.high,
                        "low": z.low,
                        "mid": z.mid(),
                        "volume": z.volume,
                        "delta": z.delta,
                        "timestampMs": z.timestamp_ms,
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "activeZones": active,
                "activeZoneCount": active.len(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "activeZoneCount": s.get("activeZoneCount"),
                "note": "Falling back to DB snapshot. Zone details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No rebid/reoffer data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Recent delta momentum reversal ('pinch') events: when heavy one-sided delta is suddenly met by fast opposing flow, causing inventory to shift. Each event has timeframe (1m/5m/15m/30m), severity score (0-5), pre/post delta, price at pinch, and price displacement."
    )]
    pub(crate) async fn get_pinch_events(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let events = pipelines.pinch.recent_events();
            let event_data: Vec<serde_json::Value> = events
                .iter()
                .map(|e| serde_json::to_value(e).unwrap_or_default())
                .collect();
            return Ok(text_result(serde_json::json!({
                "events": event_data,
                "count": events.len(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "pinchEventCount": s.get("pinchEventCount"),
                "note": "Falling back to DB snapshot. Event details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No pinch data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Cross-session delta inventory: whether current session is Building (extending prior direction), Clearing (opposing prior direction), or Neutral. Direction: Long/Short/Flat. Includes consecutive sessions with same-direction delta (trend count) and DNP shift (how much the delta neutral pivot has migrated from prior session)."
    )]
    pub(crate) async fn get_session_inventory(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let inv = &pipelines.session_inventory;
            return Ok(text_result(serde_json::json!({
                "inventoryState": inv.state(),
                "inventoryDirection": inv.direction(),
                "sessionsInTrend": inv.sessions_in_trend(),
                "dnpShift": inv.dnp_shift(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "inventoryState": s.get("inventoryState"),
                "inventoryDirection": s.get("inventoryDirection"),
                "sessionsInTrend": s.get("sessionsInTrend"),
                "note": "Falling back to DB snapshot. DNP shift not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No session inventory data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Delta at a specific price level from the delta profile. Returns signed delta at that price, buy/sell confirmation, and the top N prices by absolute delta magnitude (where conviction is concentrated). Omit price to use current price."
    )]
    pub(crate) async fn get_delta_at_price(
        &self,
        Parameters(params): Parameters<DeltaAtPriceParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let price = params.price.unwrap_or(pipelines.levels.last_price);
            let top_n = params.top_n.unwrap_or(10);
            let delta = pipelines.delta.delta_at_price(price);
            let confirms_buy = pipelines.delta.delta_confirmation_at_price(price, true);
            let confirms_sell = pipelines.delta.delta_confirmation_at_price(price, false);

            // Top N prices by absolute delta
            let mut profile = pipelines.delta.profile();
            profile.sort_by(|a, b| {
                b.1.abs()
                    .partial_cmp(&a.1.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top: Vec<serde_json::Value> = profile
                .iter()
                .take(top_n)
                .map(|(p, d)| {
                    serde_json::json!({
                        "price": p,
                        "delta": d,
                    })
                })
                .collect();

            let session_delta = pipelines.delta.session_delta();
            drop(pipelines);

            let mut out = serde_json::json!({
                "price": price,
                "deltaAtPrice": delta,
                "confirmsBuy": confirms_buy,
                "confirmsSell": confirms_sell,
                "sessionDelta": session_delta,
                "topPricesByDelta": top,
            });
            if let Some(r) = self.resolve_live_market_view() {
                merge_tool_live_metadata(&mut out, &r);
            } else {
                out["dataAgeMs"] = serde_json::json!(self.data_age_from_db_or_atomic());
            }
            return Ok(text_result(out));
        }
        Ok(no_data(
            "Delta at price requires live pipeline. Pipeline not available.",
        ))
    }

    #[tool(
        description = "Check delta confirmation at session level and at a specific price level. Returns whether session delta and price-level delta both support the trade direction. Use before trade entry for Stowe's 'execution requires delta confirmation'."
    )]
    pub(crate) async fn check_delta_confirmation(
        &self,
        Parameters(params): Parameters<DeltaConfirmParams>,
    ) -> Result<CallToolResult, McpError> {
        let is_buy = params.is_buy_setup.unwrap_or(true);

        // Try pipeline for price-level delta (try_lock to avoid blocking)
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let session_delta = pipelines.delta.session_delta();
            let session_confirms = if is_buy {
                session_delta > 0.0
            } else {
                session_delta < 0.0
            };
            let price = params.price.unwrap_or(pipelines.levels.last_price);
            let price_delta = pipelines.delta.delta_at_price(price);
            let price_confirms = pipelines.delta.delta_confirmation_at_price(price, is_buy);
            let both = session_confirms && price_confirms;
            drop(pipelines);

            let mut out = serde_json::json!({
                "sessionDeltaConfirms": session_confirms,
                "sessionDelta": session_delta,
                "priceLevelDeltaConfirms": price_confirms,
                "deltaAtPrice": price_delta,
                "price": price,
                "bothConfirm": both,
                "direction": if is_buy { "long" } else { "short" },
            });
            if let Some(r) = self.resolve_live_market_view() {
                merge_tool_live_metadata(&mut out, &r);
            } else {
                out["dataAgeMs"] = serde_json::json!(self.data_age_from_db_or_atomic());
            }
            return Ok(text_result(out));
        }

        // Fallback: session-level only
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let session_delta = s
                .get("sessionDelta")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let confirmed = if is_buy {
                session_delta > 0.0
            } else {
                session_delta < 0.0
            };
            let mut out = serde_json::json!({
                "sessionDeltaConfirms": confirmed,
                "sessionDelta": session_delta,
                "direction": if is_buy { "long" } else { "short" },
                "note": "Price-level delta not available (pipeline not live).",
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No delta data available"))
    }

    #[tool(
        description = "Which key levels is price currently near (within specified tick distance). Returns levels sorted by distance ascending. Includes prior day H/L/C, VA/POC, overnight (Globex), Globex OR30, London OR60, IB, OR5 mid, and IB extensions. Response includes sessionType/sessionSegment/tradingDay."
    )]
    pub(crate) async fn get_proximity_report(
        &self,
        Parameters(params): Parameters<ProximityParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let last_price = s.get("lastPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let max_ticks = params.max_distance_ticks.unwrap_or(20.0);

            let mut levels = Vec::new();
            let level_keys = [
                ("priorDayHigh", "PriorDayHigh"),
                ("priorDayLow", "PriorDayLow"),
                ("priorDayClose", "PriorDayClose"),
                ("priorVaHigh", "PriorVaHigh"),
                ("priorVaLow", "PriorVaLow"),
                ("priorPoc", "PriorPoc"),
                ("overnightHigh", "OvernightHigh"),
                ("overnightLow", "OvernightLow"),
                ("globexOr30High", "GlobexOr30High"),
                ("globexOr30Low", "GlobexOr30Low"),
                ("londonOr60High", "LondonOr60High"),
                ("londonOr60Low", "LondonOr60Low"),
                ("ibHigh", "IbHigh"),
                ("ibLow", "IbLow"),
                ("orHigh", "OrHigh"),
                ("orLow", "OrLow"),
                ("or5Mid", "Or5Mid"),
                ("poc", "Poc"),
                ("vaHigh", "VaHigh"),
                ("vaLow", "VaLow"),
                ("dnvaHigh", "DnvaHigh"),
                ("dnvaLow", "DnvaLow"),
                ("dnp", "Dnp"),
            ];
            for (key, label) in &level_keys {
                if let Some(price) = s.get(*key).and_then(|v| v.as_f64()) {
                    if price > 0.0 {
                        let dist = ((last_price - price) / 0.25).abs();
                        if dist <= max_ticks {
                            levels.push(serde_json::json!({
                                "level": label,
                                "price": price,
                                "distanceTicks": dist,
                            }));
                        }
                    }
                }
            }
            levels.sort_by(|a, b| {
                let da = a["distanceTicks"].as_f64().unwrap_or(f64::MAX);
                let db_val = b["distanceTicks"].as_f64().unwrap_or(f64::MAX);
                da.partial_cmp(&db_val).unwrap_or(std::cmp::Ordering::Equal)
            });
            let session_type = s
                .get("sessionType")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");
            let session_segment = s
                .get("sessionSegment")
                .and_then(|v| v.as_str())
                .unwrap_or("None");
            let mut out = serde_json::json!({
                "sessionType": session_type,
                "sessionSegment": session_segment,
                "tradingDay": s.get("tradingDay"),
                "lastPrice": last_price,
                "maxDistanceTicks": max_ticks,
                "nearbyLevels": levels,
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No market data available for proximity report"))
    }
}
