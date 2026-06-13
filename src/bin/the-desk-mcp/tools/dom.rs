//! Depth-of-market tools: DOM snapshots, pull/stack activity, book-reaction analysis.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::depth::{
    enrich_dom_summary, summarize_dom_narrative, DepthReader, DomSummary, DOM_NARRATIVE_HORIZON_MS,
};
use the_desk_backend::feed::load_feed_config;
use the_desk_backend::research;
use the_desk_backend::trading_day_from_timestamp_ms;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = dom_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Delayed DOM snapshot reconstructed from Sierra `.depth` history at or immediately before a timestamp. Returns best bid/ask, spread, touch imbalance, and the top resting levels on each side. Use this when you want the ladder view, not just executed tape. Note: Sierra depth data has ~1 second polling lag, so this is a delayed reconstruction, not real-time."
    )]
    pub(crate) async fn get_dom_snapshot_at(
        &self,
        Parameters(params): Parameters<DomSnapshotAtParams>,
    ) -> Result<CallToolResult, McpError> {
        let levels_per_side = params.levels_per_side.unwrap_or(10).clamp(1, 25) as usize;
        let timestamp_ms = params.timestamp_ms;
        let snapshot = tokio::task::spawn_blocking(move || {
            let reader = depth_reader_for_timestamp(timestamp_ms)?;
            reader
                .snapshot_at(timestamp_ms, levels_per_side)
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("DOM snapshot task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "snapshot": snapshot,
            "requestedTimestampMs": timestamp_ms,
            "note": "This is reconstructed from Sierra historical `.depth` data, not inferred from trade prints."
        })))
    }

    #[tool(
        description = "Estimate pull/stack activity from Sierra `.depth` history over a time window, then align DOM decreases with `.scid` trades to separate likely fills from likely pulls. Use price_low/price_high to focus on a specific zone."
    )]
    pub(crate) async fn get_pull_stack_activity(
        &self,
        Parameters(params): Parameters<PullStackParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_time_window(params.start_time_ms, params.end_time_ms)?;
        let start_time_ms = params.start_time_ms;
        let end_time_ms = params.end_time_ms;
        let price_low = params.price_low;
        let price_high = params.price_high;
        let summary = tokio::task::spawn_blocking(move || {
            let config = load_feed_config();
            let path = DepthReader::find_file_for_timestamp(&config, start_time_ms)
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error(format!(
                        "No Sierra .depth file found for timestamp {start_time_ms}"
                    ))
                })?;
            let depth_reader = DepthReader::new(path, config.price_scale);
            let trades = aggregate_window_trades(&config, start_time_ms, end_time_ms)?;
            depth_reader
                .summarize_window(start_time_ms, end_time_ms, &trades, price_low, price_high)
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("Pull/stack task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "activity": summary,
            "priceFilter": { "low": price_low, "high": price_high },
            "note": "Estimated filled vs pulled is heuristic: DOM decreases are aligned to same-price `.scid` trade volume within the requested window."
        })))
    }

    #[tool(
        description = "Liquidity behavior around a target price over a time window. This focuses pull/stack analysis on a narrow band around a level, such as prior VAH, IB high, or an anchored VWAP level."
    )]
    pub(crate) async fn get_liquidity_behavior_at_level(
        &self,
        Parameters(params): Parameters<LiquidityBehaviorParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_time_window(params.start_time_ms, params.end_time_ms)?;
        let radius_ticks = params.radius_ticks.unwrap_or(4).clamp(1, 20) as f64;
        let low = params.price - radius_ticks * 0.25;
        let high = params.price + radius_ticks * 0.25;
        let start_time_ms = params.start_time_ms;
        let end_time_ms = params.end_time_ms;
        let target_price = params.price;
        let summary = tokio::task::spawn_blocking(move || {
            let config = load_feed_config();
            let path = DepthReader::find_file_for_timestamp(&config, start_time_ms)
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error(format!(
                        "No Sierra .depth file found for timestamp {start_time_ms}"
                    ))
                })?;
            let depth_reader = DepthReader::new(path, config.price_scale);
            let trades = aggregate_window_trades(&config, start_time_ms, end_time_ms)?;
            depth_reader
                .summarize_window(start_time_ms, end_time_ms, &trades, Some(low), Some(high))
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("Liquidity behavior task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "targetPrice": target_price,
            "radiusTicks": radius_ticks,
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms },
            "activity": summary,
            "note": "Use this to inspect whether liquidity near a specific level was stacking, getting pulled, or likely being consumed by trades."
        })))
    }

    #[tool(
        description = "Windowed delayed DOM summary using persisted DOM feature snapshots when available. Returns compact DOM summaries across a time range and optionally narrows the reported pull/stack levels to a price band. DOM data has ~1s polling lag from Sierra."
    )]
    pub(crate) async fn get_dom_window(
        &self,
        Parameters(params): Parameters<DomWindowParams>,
    ) -> Result<CallToolResult, McpError> {
        if let (Some(start), Some(end)) = (params.start_time_ms, params.end_time_ms) {
            validate_time_window(start, end)?;
        }
        let limit = params.limit.unwrap_or(20).clamp(1, 100);
        let mut snapshots = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(params.start_time_ms, params.end_time_ms, limit)
                .map_err(db_error)?
        };
        if snapshots.is_empty() {
            if let (Some(start), Some(end)) = (params.start_time_ms, params.end_time_ms) {
                let price_low = params.price_low;
                let price_high = params.price_high;
                let direct = tokio::task::spawn_blocking(move || {
                    let (feature, _) =
                        compute_dom_feature_for_window(start, end, end, 10, price_low, price_high)?;
                    Ok::<_, McpError>((
                        feature.timestamp_ms,
                        serde_json::to_value(feature).unwrap_or_default(),
                    ))
                })
                .await
                .map_err(|e| db_error(format!("DOM window task failed: {e}")))??;
                snapshots.push(direct);
            }
        }

        let narrative_summaries = dom_summaries_from_rows(&snapshots);
        let session_reference = if let Some((latest_ts, _)) = snapshots.last() {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let rows = db
                .query_dom_feature_snapshots_for_trading_day(
                    &trading_day_from_timestamp_ms(*latest_ts),
                    50_000,
                )
                .map_err(db_error)?;
            Some(dom_summaries_from_rows(&rows))
        } else {
            None
        };

        for (_, payload) in &mut snapshots {
            if let Some(activity) = payload.get_mut("activity").and_then(|v| v.as_object_mut()) {
                for key in ["topPullLevels", "topStackLevels"] {
                    if let Some(levels) = activity.get_mut(key).and_then(|v| v.as_array_mut()) {
                        levels.retain(|level| {
                            let price = level.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            if let Some(low) = params.price_low {
                                if price < low {
                                    return false;
                                }
                            }
                            if let Some(high) = params.price_high {
                                if price > high {
                                    return false;
                                }
                            }
                            true
                        });
                    }
                }
            }
        }

        let latest = snapshots.last().map(|(_, payload)| payload.clone());
        let aggregate =
            if params.include_aggregate.unwrap_or(true) && !narrative_summaries.is_empty() {
                Some(
                    serde_json::to_value(summarize_dom_narrative(
                        &narrative_summaries,
                        session_reference.as_deref(),
                        None,
                    ))
                    .unwrap_or_default(),
                )
            } else {
                None
            };
        Ok(text_result(serde_json::json!({
            "windowStartMs": params.start_time_ms,
            "windowEndMs": params.end_time_ms,
            "priceFilter": { "low": params.price_low, "high": params.price_high },
            "snapshots": snapshots.into_iter().map(|(ts, payload)| serde_json::json!({
                "timestampMs": ts,
                "payload": payload
            })).collect::<Vec<_>>(),
            "latest": latest,
            "aggregate": aggregate,
            "source": if latest.is_some() { "dom_feature_snapshots" } else { "none" }
        })))
    }

    #[tool(
        description = "One-call delayed DOM + tape context at a timestamp. Combines the nearest DOM snapshot, the nearest persisted DOM feature summary, raw-tick footprint over a short window, and derived flow flags. DOM data has ~1s polling lag from Sierra."
    )]
    pub(crate) async fn get_dom_tape_context_at(
        &self,
        Parameters(params): Parameters<DomTapeContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let window_ms = params
            .window_ms
            .unwrap_or(60_000.0)
            .clamp(5_000.0, 300_000.0);
        let start_time_ms = params.timestamp_ms - window_ms;
        let end_time_ms = params.timestamp_ms + 1_000.0;
        validate_time_window(start_time_ms, end_time_ms)?;

        let (mut feature, mut dom_snapshot, ticks) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            (
                db.get_dom_feature_near(params.timestamp_ms)
                    .map_err(db_error)?,
                db.get_dom_snapshot_near(params.timestamp_ms)
                    .map_err(db_error)?,
                db.query_ticks_filtered(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    params.price_low,
                    params.price_high,
                    None,
                    2_000,
                )
                .map_err(db_error)?,
            )
        };

        if feature.is_none() || dom_snapshot.is_none() {
            let timestamp_ms = params.timestamp_ms;
            let price_low = params.price_low;
            let price_high = params.price_high;
            let fallback = tokio::task::spawn_blocking(move || {
                let (feat, snap) = compute_dom_feature_for_window(
                    start_time_ms,
                    end_time_ms,
                    timestamp_ms,
                    10,
                    price_low,
                    price_high,
                )?;
                Ok::<_, McpError>((
                    (
                        snap.snapshot_timestamp_ms,
                        serde_json::to_value(&snap).unwrap_or_default(),
                    ),
                    (
                        feat.timestamp_ms,
                        serde_json::to_value(feat).unwrap_or_default(),
                    ),
                ))
            })
            .await
            .map_err(|e| db_error(format!("DOM tape context task failed: {e}")))??;
            dom_snapshot.get_or_insert(fallback.0);
            feature.get_or_insert(fallback.1);
        }

        let footprint = footprint_from_ticks(&ticks);
        let total_volume: f64 = ticks.iter().map(|tick| tick.volume).sum();
        let net_delta: f64 = ticks
            .iter()
            .map(|tick| {
                if tick.is_buy {
                    tick.volume
                } else {
                    -tick.volume
                }
            })
            .sum();
        let recent_rows = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(
                Some((params.timestamp_ms - DOM_NARRATIVE_HORIZON_MS).max(0.0)),
                Some(params.timestamp_ms),
                512,
            )
            .map_err(db_error)?
        };
        let mut dom_feature_payload = feature.map(|(_, payload)| payload);
        let mut dom_summary_struct = dom_feature_payload
            .as_ref()
            .and_then(dom_summary_from_payload);
        let activity_struct = dom_feature_payload.as_ref().and_then(activity_from_payload);
        let mut session_reference_summaries: Option<Vec<DomSummary>> = None;
        if let Some(summary) = dom_summary_struct.as_mut() {
            let recent_summaries: Vec<DomSummary> = dom_summaries_from_rows(&recent_rows)
                .into_iter()
                .filter(|row| row.timestamp_ms < summary.timestamp_ms - 0.001)
                .collect();
            let session_rows = {
                let db = self.db.lock().map_err(|_| lock_error())?;
                db.query_dom_feature_snapshots_for_trading_day(
                    &trading_day_from_timestamp_ms(summary.timestamp_ms),
                    50_000,
                )
                .unwrap_or_default()
            };
            let session_reference = if session_rows.is_empty() {
                None
            } else {
                Some(dom_summaries_from_rows(&session_rows))
            };
            session_reference_summaries = session_reference.clone();
            enrich_dom_summary(
                summary,
                activity_struct.as_ref(),
                &recent_summaries,
                session_reference.as_deref(),
            );
            if let Some(payload) = dom_feature_payload
                .as_mut()
                .and_then(|value| value.as_object_mut())
            {
                payload.insert(
                    "domSummary".to_string(),
                    serde_json::to_value(summary.clone()).unwrap_or_default(),
                );
            }
        }
        let dom_summary = dom_summary_struct
            .as_ref()
            .and_then(|summary| serde_json::to_value(summary).ok());
        let activity = activity_struct
            .as_ref()
            .and_then(|summary| serde_json::to_value(summary).ok());
        let dom_regime_summary = if let Some(summary) = dom_summary_struct.as_ref() {
            let mut history = dom_summaries_from_rows(&recent_rows);
            history.retain(|row| row.timestamp_ms < summary.timestamp_ms - 0.001);
            history.push(summary.clone());
            Some(
                serde_json::to_value(summarize_dom_narrative(
                    &history,
                    session_reference_summaries.as_deref(),
                    activity_struct.as_ref(),
                ))
                .unwrap_or_default(),
            )
        } else {
            None
        };
        let aggressive_buyers = net_delta > 0.0
            && dom_summary
                .as_ref()
                .and_then(|v| v.get("askPullRate"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                < 0.5;
        let aggressive_sellers = net_delta < 0.0
            && dom_summary
                .as_ref()
                .and_then(|v| v.get("bidPullRate"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                < 0.5;

        Ok(text_result(serde_json::json!({
            "timestampMs": params.timestamp_ms,
            "windowMs": window_ms,
            "domSnapshot": dom_snapshot.map(|(_, payload)| payload),
            "domFeature": dom_feature_payload,
            "domSummary": dom_summary,
            "activity": activity,
            "domRegimeSummary": dom_regime_summary,
            "tape": {
                "tickCount": ticks.len(),
                "totalVolume": total_volume,
                "netDelta": net_delta,
                "footprint": footprint,
            },
            "derivedFlags": {
                "aggressiveBuyers": aggressive_buyers,
                "aggressiveSellers": aggressive_sellers,
                "domSupportsHigher": dom_summary.as_ref().and_then(|v| v.get("liquidityBias")).and_then(|v| v.as_str()) == Some("bid_support"),
                "domCapsHigher": dom_summary.as_ref().and_then(|v| v.get("liquidityBias")).and_then(|v| v.as_str()) == Some("ask_resistance"),
            }
        })))
    }

    #[tool(
        description = "Explanation-oriented delayed DOM read around a timestamp or level. Grounds the interpretation in persisted DOM summaries, nearby depth events, and executed tape. DOM data has ~1s polling lag from Sierra."
    )]
    pub(crate) async fn explain_book_reaction(
        &self,
        Parameters(params): Parameters<ExplainBookReactionParams>,
    ) -> Result<CallToolResult, McpError> {
        let target_time_ms = params
            .timestamp_ms
            .or(params.end_time_ms)
            .ok_or_else(|| invalid_params_error("timestampMs or endTimeMs is required"))?;
        let start_time_ms = params.start_time_ms.unwrap_or(target_time_ms - 30_000.0);
        let end_time_ms = params.end_time_ms.unwrap_or(target_time_ms + 1_000.0);
        validate_time_window(start_time_ms, end_time_ms)?;
        let radius_ticks = params.radius_ticks.unwrap_or(6) as f64;
        let price_low = params.price.map(|price| price - radius_ticks * 0.25);
        let price_high = params.price.map(|price| price + radius_ticks * 0.25);

        let (feature, depth_events, ticks) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            (
                db.get_dom_feature_near(target_time_ms).map_err(db_error)?,
                db.query_depth_events(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    price_low,
                    price_high,
                    200,
                )
                .map_err(db_error)?,
                db.query_ticks_filtered(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    price_low,
                    price_high,
                    None,
                    500,
                )
                .map_err(db_error)?,
            )
        };

        let feature_payload = if let Some((_, payload)) = feature {
            payload
        } else {
            let timestamp_ms = target_time_ms;
            tokio::task::spawn_blocking(move || {
                let (feat, _) = compute_dom_feature_for_window(
                    start_time_ms,
                    end_time_ms,
                    timestamp_ms,
                    10,
                    price_low,
                    price_high,
                )?;
                Ok::<_, McpError>(serde_json::to_value(feat).unwrap_or_default())
            })
            .await
            .map_err(|e| db_error(format!("Explain book reaction task failed: {e}")))??
        };

        let dom_summary = feature_payload
            .get("domSummary")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let bid_pull_rate = dom_summary
            .get("bidPullRate")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let ask_pull_rate = dom_summary
            .get("askPullRate")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let pull_stack_bias = dom_summary
            .get("pullStackBias")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let net_delta: f64 = ticks
            .iter()
            .map(|tick| {
                if tick.is_buy {
                    tick.volume
                } else {
                    -tick.volume
                }
            })
            .sum();

        let liquidity_bias = dom_summary
            .get("liquidityBias")
            .and_then(|v| v.as_str())
            .unwrap_or("balanced");
        let total_volume: f64 = ticks.iter().map(|t| t.volume).sum();

        // Extract top pull/stack prices from activity for narrative
        let top_pull = feature_payload
            .get("activity")
            .and_then(|a| a.get("topPullLevels"))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();
        let top_stack = feature_payload
            .get("activity")
            .and_then(|a| a.get("topStackLevels"))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();

        // Build magnitude-aware narrative
        let mut parts = Vec::new();

        // Pull rate comparison with actual numbers
        let bid_pct = (bid_pull_rate * 100.0).round();
        let ask_pct = (ask_pull_rate * 100.0).round();
        if (bid_pull_rate - ask_pull_rate).abs() > 0.1 {
            if bid_pull_rate > ask_pull_rate {
                parts.push(format!(
                    "Bids pulled at {bid_pct:.0}% rate vs asks at {ask_pct:.0}% — bid-side liquidity was being withdrawn faster."
                ));
            } else {
                parts.push(format!(
                    "Asks pulled at {ask_pct:.0}% rate vs bids at {bid_pct:.0}% — offer-side liquidity was being withdrawn faster."
                ));
            }
        } else {
            parts.push(format!(
                "Pull rates roughly balanced (bids {bid_pct:.0}%, asks {ask_pct:.0}%)."
            ));
        }

        // Top pull level with price
        if let Some(ref pull) = top_pull {
            let price = pull.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let qty = pull
                .get("estimatedPulledQuantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let side = pull
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if qty > 0.0 {
                parts.push(format!(
                    "Top pull level: {price:.2} ({side} side, {qty:.0} contracts pulled)."
                ));
            }
        }

        // Top stack level with price
        if let Some(ref stack) = top_stack {
            let price = stack.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let qty = stack
                .get("stackedQuantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let side = stack
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if qty > 0.0 {
                parts.push(format!(
                    "Top stack level: {price:.2} ({side} side, {qty:.0} contracts stacked)."
                ));
            }
        }

        // Net delta context
        if net_delta.abs() > 0.0 {
            let direction = if net_delta > 0.0 {
                "buyer-led"
            } else {
                "seller-led"
            };
            parts.push(format!(
                "Net delta {net_delta:+.0} over {total_volume:.0} volume — tape was {direction}."
            ));
        }

        // Depth event density
        if !depth_events.is_empty() {
            parts.push(format!(
                "{} depth events in window — {} book activity.",
                depth_events.len(),
                if depth_events.len() > 100 {
                    "heavy"
                } else if depth_events.len() > 30 {
                    "moderate"
                } else {
                    "light"
                }
            ));
        }

        // Overall read combining book + tape
        let overall = if pull_stack_bias > 0.0 && net_delta >= 0.0 {
            "Book and tape aligned supportive: bid-side liquidity held up while tape stayed neutral-to-positive."
        } else if pull_stack_bias < 0.0 && net_delta <= 0.0 {
            "Book and tape aligned defensive: offers held better than bids while tape skewed seller-led."
        } else if pull_stack_bias > 0.0 && net_delta < 0.0 {
            "Book was supportive but tape disagreed — bids were stacking while sellers dominated the tape. Potential absorption."
        } else if pull_stack_bias < 0.0 && net_delta > 0.0 {
            "Book was fragile but tape was buying — offers were pulling while buyers lifted aggressively. Potential breakout setup."
        } else {
            "Liquidity stayed relatively balanced — the reaction looks more tape-driven than book-driven."
        };
        parts.push(overall.to_string());

        let explanation = parts.join(" ");

        Ok(text_result(serde_json::json!({
            "timestampMs": target_time_ms,
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms },
            "priceFocus": { "price": params.price, "radiusTicks": params.radius_ticks },
            "domFeature": feature_payload,
            "depthEventCount": depth_events.len(),
            "tapeTickCount": ticks.len(),
            "totalVolume": total_volume,
            "netDelta": net_delta,
            "pullRates": { "bid": bid_pull_rate, "ask": ask_pull_rate },
            "pullStackBias": pull_stack_bias,
            "liquidityBias": liquidity_bias,
            "topPullLevel": top_pull,
            "topStackLevel": top_stack,
            "explanation": explanation,
        })))
    }

    #[tool(
        description = "Summarize delayed DOM behavior over a window so agents can tell whether liquidity has been persistent, flashing, or flipping. Returns time-in-state, flip counts, persistence, confidence, and a narrative summary."
    )]
    pub(crate) async fn get_dom_regime_summary(
        &self,
        Parameters(params): Parameters<DomRegimeSummaryParams>,
    ) -> Result<CallToolResult, McpError> {
        let end_time_ms = if let Some(end) = params.end_time_ms.or(params.timestamp_ms) {
            end
        } else {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.latest_dom_feature_state()
                .map_err(db_error)?
                .map(|(timestamp_ms, _)| timestamp_ms)
                .ok_or_else(|| {
                    invalid_params_error(
                        "timestampMs or endTimeMs is required when no DOM history is present",
                    )
                })?
        };
        let window_ms = params
            .window_ms
            .unwrap_or(DOM_NARRATIVE_HORIZON_MS)
            .clamp(5_000.0, 1_800_000.0);
        let start_time_ms = params.start_time_ms.unwrap_or(end_time_ms - window_ms);
        validate_time_window(start_time_ms, end_time_ms)?;
        let limit = params.limit.unwrap_or(512).clamp(1, 5_000);

        let rows = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(Some(start_time_ms), Some(end_time_ms), limit)
                .map_err(db_error)?
        };
        let summaries = dom_summaries_from_rows(&rows);
        if summaries.is_empty() {
            return Ok(no_data(
                "No DOM feature snapshots available for the requested window",
            ));
        }
        let latest_payload = rows.last().map(|(_, payload)| payload.clone());
        let latest_activity = latest_payload.as_ref().and_then(activity_from_payload);
        let session_reference = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let day = trading_day_from_timestamp_ms(end_time_ms);
            let session_rows = db
                .query_dom_feature_snapshots_for_trading_day(&day, 50_000)
                .map_err(db_error)?;
            let parsed = dom_summaries_from_rows(&session_rows);
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        };
        let regime = summarize_dom_narrative(
            &summaries,
            session_reference.as_deref(),
            latest_activity.as_ref(),
        );

        Ok(text_result(serde_json::json!({
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms, "windowMs": window_ms },
            "regime": regime,
            "latestSummary": latest_payload.as_ref().and_then(dom_summary_from_payload),
            "latestActivity": latest_activity,
            "sampleCount": summaries.len(),
        })))
    }

    #[tool(
        description = "Historical frequency of DOM behaviors such as persisted bid support, ask resistance, liquidity flips, pulling acceleration, or stacking acceleration. Uses persisted DOM feature snapshots and returns research metadata including sample reliability and truncation status."
    )]
    pub(crate) async fn query_dom_behavior_frequency(
        &self,
        Parameters(params): Parameters<DomBehaviorFrequencyParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let result = self
            .with_read_db(move |db| {
                research::dom_behavior_frequency(
                    db,
                    &behavior,
                    min_duration_ms,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                )
                .map_err(db_error)
            })
            .await?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
    }

    #[tool(
        description = "Historical setup outcome context when a DOM behavior was present near signal fire. Answers questions like whether persistent bid support improved setup follow-through, with research metadata for outcome sample reliability."
    )]
    pub(crate) async fn query_dom_behavior_conditional(
        &self,
        Parameters(params): Parameters<DomBehaviorConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = parse_scope_value(params.scope)?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let setup_id = parse_optional_non_empty_string("setupId", params.setup_id.as_deref())?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let source = parse_optional_signal_source(params.source.as_deref())?;
        let result = self
            .with_read_db(move |db| {
                research::dom_behavior_conditional(
                    db,
                    &behavior,
                    setup_id.as_deref(),
                    min_duration_ms,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                    scope.as_ref(),
                    source,
                    params.job_id.as_deref(),
                    params.include_unverified.unwrap_or(true),
                )
                .map_err(db_error)
            })
            .await?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
    }

    #[tool(
        description = "Historical DOM behavior around a specific event type or level interaction. Helps answer whether persisted support, flips, or pulling acceleration commonly accompanied a class of market events. Returns research metadata and marks capped market-event scans as non-reportable."
    )]
    pub(crate) async fn query_dom_reaction_at_levels(
        &self,
        Parameters(params): Parameters<DomReactionAtLevelsParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = parse_scope_value(params.scope)?;
        let event_type = parse_research_event_type(&params.event_type)?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let result = self
            .with_read_db(move |db| {
                research::dom_reaction_at_levels(
                    db,
                    &event_type,
                    &behavior,
                    min_duration_ms,
                    params.start_date.as_deref(),
                    params.end_date.as_deref(),
                    scope.as_ref(),
                )
                .map_err(db_error)
            })
            .await?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
    }
}
