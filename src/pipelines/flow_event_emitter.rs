use std::collections::HashMap;

use super::event_detector::MarketEvent;
use super::PipelineEngine;
use crate::tick_time_context_from_timestamp_ms;

/// Emits flow events from pipeline ring buffers into the MarketEvent stream.
///
/// Runs alongside the structural `EventDetector`, reading absorption, pinch,
/// rebid/reoffer, trade size, leg-to-leg, and DOM-cluster pipelines to produce
/// `MarketEvent` objects
/// that flow into the same `market_events` DB table — making them queryable
/// via `query_event_frequency` and `query_conditional`.
#[derive(Debug)]
pub struct FlowEventEmitter {
    prev_absorption_count: usize,
    prev_pinch_count: usize,
    prev_zone_count: usize,
    /// High-watermark of [`LegProfilePipeline::event_seq`], not ring length.
    prev_leg_event_seq: u64,
    /// High-watermark of [`DomClusterPipeline::event_seq`], not ring length.
    prev_dom_event_seq: u64,
    /// (high, low) of zones we already emitted a "held" event for.
    prev_held_zones: Vec<(f64, f64)>,
    /// price_key -> last-known 21+ lot count, for large_trade_cluster detection.
    prev_large_trade_counts: HashMap<i64, u64>,
    /// Dedup: event_key -> last emission timestamp.
    last_event_ts: HashMap<String, f64>,
}

impl Default for FlowEventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowEventEmitter {
    fn absorption_market_event_type(status: &str) -> &'static str {
        match status {
            "confirmed" => "absorption_confirmed",
            "invalidated" => "absorption_invalidated",
            _ => "absorption_detected",
        }
    }

    pub fn new() -> Self {
        Self {
            prev_absorption_count: 0,
            prev_pinch_count: 0,
            prev_zone_count: 0,
            prev_leg_event_seq: 0,
            prev_dom_event_seq: 0,
            prev_held_zones: Vec::new(),
            prev_large_trade_counts: HashMap::new(),
            last_event_ts: HashMap::new(),
        }
    }

    /// Reset for a new trading session.
    pub fn reset(&mut self) {
        self.prev_absorption_count = 0;
        self.prev_pinch_count = 0;
        self.prev_zone_count = 0;
        self.prev_leg_event_seq = 0;
        self.prev_dom_event_seq = 0;
        self.prev_held_zones.clear();
        self.prev_large_trade_counts.clear();
        self.last_event_ts.clear();
    }

    /// Sync internal counters to the current pipeline state without emitting
    /// events. Call after a warm-start backfill so the first live `detect()`
    /// doesn't produce a burst of stale events.
    pub fn sync_counts(&mut self, pipelines: &PipelineEngine) {
        self.prev_absorption_count = pipelines.absorption.recent_events().len();
        self.prev_pinch_count = pipelines.pinch.recent_events().len();
        self.prev_zone_count = pipelines.rebid_reoffer.all_zones().len();
        self.prev_leg_event_seq = pipelines.leg_profile.event_seq();
        self.prev_dom_event_seq = pipelines.dom_cluster.event_seq();

        self.prev_held_zones.clear();
        for zone in pipelines.rebid_reoffer.all_zones() {
            if zone.status == super::ZoneStatus::Held {
                self.prev_held_zones.push((zone.high, zone.low));
            }
        }

        self.prev_large_trade_counts.clear();
        for (price, count) in pipelines.trade_size.large_trade_prices() {
            self.prev_large_trade_counts
                .insert(discretize_price(price), count);
        }
    }

    /// Detect new flow events by comparing current pipeline state against
    /// previous counts. Returns `MarketEvent` objects in the same schema as
    /// the structural detector.
    pub fn detect(
        &mut self,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
        current_price: f64,
    ) -> Vec<MarketEvent> {
        let mut events = Vec::new();
        self.detect_into(
            pipelines,
            timestamp_ms,
            session_date,
            current_price,
            &mut events,
        );
        events
    }

    pub fn detect_into(
        &mut self,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
        current_price: f64,
        events: &mut Vec<MarketEvent>,
    ) {
        self.detect_absorption(events, pipelines, timestamp_ms, session_date);
        self.detect_pinch(events, pipelines, timestamp_ms, session_date);
        self.detect_legs(events, pipelines, timestamp_ms, session_date);
        self.detect_dom(events, pipelines, timestamp_ms, session_date);
        self.detect_zones(events, pipelines, timestamp_ms, session_date);
        self.detect_large_trade_clusters(
            events,
            pipelines,
            timestamp_ms,
            session_date,
            current_price,
        );
    }

    /// Depth-poll path: emit new DOM-family events only.
    ///
    /// Must not run the full [`Self::detect_into`] scan. A `.depth` poll that
    /// advanced absorption / pinch / leg / zone / cluster watermarks would
    /// steal those events from the tick loop.
    pub fn detect_dom_into(
        &mut self,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
        events: &mut Vec<MarketEvent>,
    ) {
        self.detect_dom(events, pipelines, timestamp_ms, session_date);
    }

    fn event_context(timestamp_ms: f64, session_date: &str) -> (String, String, String) {
        if let Some(ctx) = tick_time_context_from_timestamp_ms(timestamp_ms) {
            let session_type = match ctx.session_type {
                crate::SessionType::Rth => "RTH".to_string(),
                crate::SessionType::Globex => "Globex".to_string(),
                crate::SessionType::Unknown => "Unknown".to_string(),
            };
            let session_segment = if session_type == "Globex" {
                match ctx.session_segment {
                    crate::SessionSegment::Asia => "Asia".to_string(),
                    crate::SessionSegment::London => "London".to_string(),
                    crate::SessionSegment::None => "None".to_string(),
                }
            } else {
                "None".to_string()
            };
            return (session_type, session_segment, ctx.trading_day);
        }
        (
            "Unknown".to_string(),
            "None".to_string(),
            session_date.to_string(),
        )
    }

    /// Absorption / exhaustion / delta_divergence events.
    fn detect_absorption(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let current = pipelines.absorption.recent_events();
        let count = current.len();
        if count > self.prev_absorption_count {
            for evt in &current[self.prev_absorption_count..] {
                let event_type = Self::absorption_market_event_type(&evt.status);
                let event_key = format!(
                    "{}_{}_{}_{}",
                    event_type,
                    evt.event_type,
                    evt.status,
                    discretize_price(evt.price)
                );
                if self.should_emit(&event_key, timestamp_ms, 30_000.0) {
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms: evt.timestamp_ms,
                        event_type: event_type.to_string(),
                        level_name: None,
                        price: evt.price,
                        direction: evt.direction.clone(),
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "eventSubtype": evt.event_type,
                            "status": evt.status,
                            "severity": evt.severity,
                            "zoneLow": evt.zone_low,
                            "zoneHigh": evt.zone_high,
                            "keyLevel": evt.key_level,
                            "confirmationDeadlineMs": evt.confirmation_deadline_ms,
                            "confirmedAtMs": evt.confirmed_at_ms,
                            "invalidatedAtMs": evt.invalidated_at_ms,
                            "invalidationReason": evt.invalidation_reason,
                            "pacePercentile": evt.pace_percentile,
                            "rvolRatio": evt.rvol_ratio,
                            "localVolatilityTicks": evt.local_volatility_ticks,
                            "regimePhase": evt.regime_phase,
                        })),
                        session_type: session_type.clone(),
                        session_segment: session_segment.clone(),
                        trading_day: trading_day.clone(),
                    });
                }
            }
        }
        self.prev_absorption_count = count;
    }

    /// Pinch (delta momentum reversal) events.
    fn detect_pinch(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let current = pipelines.pinch.recent_events();
        let count = current.len();
        if count > self.prev_pinch_count {
            for evt in &current[self.prev_pinch_count..] {
                let event_key = format!("pinch_detected_{}", evt.timeframe_label);
                if self.should_emit(&event_key, timestamp_ms, 10_000.0) {
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms: evt.timestamp_ms,
                        event_type: "pinch_detected".to_string(),
                        level_name: None,
                        price: evt.price_at_pinch,
                        direction: None,
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "timeframe": evt.timeframe_label,
                            "severity": evt.severity,
                            "prePinchDelta": evt.pre_pinch_delta,
                            "postPinchDelta": evt.post_pinch_delta,
                            "priceAtPinch": evt.price_at_pinch,
                            "priceDisplacement": evt.price_displacement,
                        })),
                        session_type: session_type.clone(),
                        session_segment: session_segment.clone(),
                        trading_day: trading_day.clone(),
                    });
                }
            }
        }
        self.prev_pinch_count = count;
    }

    /// Leg-to-leg rotation start / complete events (not DOM-family / Capsule-mandatory).
    fn detect_legs(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let current = pipelines.leg_profile.recent_events();
        let seq = pipelines.leg_profile.event_seq();
        if seq < self.prev_leg_event_seq {
            // Pipeline reset (Asia/RTH or session-kind flip) while the emitter
            // still holds a high watermark.
            self.prev_leg_event_seq = 0;
        }
        if seq > self.prev_leg_event_seq {
            let newly = (seq - self.prev_leg_event_seq) as usize;
            let emit_n = newly.min(current.len());
            let skip = current.len().saturating_sub(emit_n);
            for evt in current.iter().skip(skip) {
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms: evt.timestamp_ms,
                    event_type: evt.event_type.clone(),
                    level_name: None,
                    price: evt.anchor_price,
                    direction: Some(evt.direction.clone()),
                    sequence_num: None,
                    metadata: Some(serde_json::json!({
                        "volume": evt.volume,
                        "netDelta": evt.net_delta,
                        "poc": evt.poc,
                        "ageMs": evt.age_ms,
                        "anchorPrice": evt.anchor_price,
                        "extremePrice": evt.extreme_price,
                    })),
                    session_type: session_type.clone(),
                    session_segment: session_segment.clone(),
                    trading_day: trading_day.clone(),
                });
            }
        }
        self.prev_leg_event_seq = seq;
    }

    /// DOM-cluster events (Capsule-mandatory DOM-family types).
    fn detect_dom(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let current = pipelines.dom_cluster.recent_events();
        let seq = pipelines.dom_cluster.event_seq();
        if seq < self.prev_dom_event_seq {
            // Pipeline reset (Asia/RTH or session-kind flip) while the emitter
            // still holds a high watermark.
            self.prev_dom_event_seq = 0;
        }
        if seq > self.prev_dom_event_seq {
            let newly = (seq - self.prev_dom_event_seq) as usize;
            let emit_n = newly.min(current.len());
            let skip = current.len().saturating_sub(emit_n);
            for evt in current.iter().skip(skip) {
                events.push(MarketEvent {
                    session_date: session_date.to_string(),
                    timestamp_ms: evt.timestamp_ms,
                    event_type: evt.event_type.clone(),
                    level_name: None,
                    price: evt.price,
                    direction: evt.direction.clone(),
                    sequence_num: evt.sequence_num,
                    metadata: Some(evt.metadata.clone()),
                    session_type: session_type.clone(),
                    session_segment: session_segment.clone(),
                    trading_day: trading_day.clone(),
                });
            }
        }
        self.prev_dom_event_seq = seq;
    }

    /// Acceleration zone created / held events.
    fn detect_zones(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let all_zones = pipelines.rebid_reoffer.all_zones();
        let count = all_zones.len();

        // New zones created since last check
        if count > self.prev_zone_count {
            for zone in &all_zones[self.prev_zone_count..] {
                let event_key = format!(
                    "acceleration_zone_created_{}_{}",
                    discretize_price(zone.high),
                    discretize_price(zone.low)
                );
                if self.should_emit(&event_key, timestamp_ms, 60_000.0) {
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms: zone.timestamp_ms,
                        event_type: "acceleration_zone_created".to_string(),
                        level_name: None,
                        price: zone.mid(),
                        direction: Some(format!("{:?}", zone.zone_type)),
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "zoneType": format!("{:?}", zone.zone_type),
                            "high": zone.high,
                            "low": zone.low,
                            "volume": zone.volume,
                            "delta": zone.delta,
                        })),
                        session_type: session_type.clone(),
                        session_segment: session_segment.clone(),
                        trading_day: trading_day.clone(),
                    });
                }
            }
        }
        self.prev_zone_count = count;

        // Check for zones that transitioned to Held
        for zone in all_zones {
            if zone.status == super::ZoneStatus::Held {
                let key = (zone.high, zone.low);
                if !self.prev_held_zones.contains(&key) {
                    self.prev_held_zones.push(key);
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: "acceleration_zone_held".to_string(),
                        level_name: None,
                        price: zone.mid(),
                        direction: Some(format!("{:?}", zone.zone_type)),
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "zoneType": format!("{:?}", zone.zone_type),
                            "high": zone.high,
                            "low": zone.low,
                            "mid": zone.mid(),
                        })),
                        session_type: session_type.clone(),
                        session_segment: session_segment.clone(),
                        trading_day: trading_day.clone(),
                    });
                }
            }
        }
    }

    /// Large trade cluster: 3+ new 21+ lot trades at the same price since last check.
    /// Scans all prices with large trades, not just the current tick price.
    fn detect_large_trade_clusters(
        &mut self,
        events: &mut Vec<MarketEvent>,
        pipelines: &PipelineEngine,
        timestamp_ms: f64,
        session_date: &str,
        _current_price: f64,
    ) {
        let (session_type, session_segment, trading_day) =
            Self::event_context(timestamp_ms, session_date);
        let large_prices = pipelines.trade_size.large_trade_prices();

        for (price, count) in &large_prices {
            let price_key = discretize_price(*price);
            let prev_count = self
                .prev_large_trade_counts
                .get(&price_key)
                .copied()
                .unwrap_or(0);
            let new_trades = count.saturating_sub(prev_count);

            if new_trades >= 3 {
                let event_key = format!("large_trade_cluster_{}", price_key);
                if self.should_emit(&event_key, timestamp_ms, 60_000.0) {
                    events.push(MarketEvent {
                        session_date: session_date.to_string(),
                        timestamp_ms,
                        event_type: "large_trade_cluster".to_string(),
                        level_name: None,
                        price: *price,
                        direction: None,
                        sequence_num: None,
                        metadata: Some(serde_json::json!({
                            "count": count,
                            "newTrades": new_trades,
                        })),
                        session_type: session_type.clone(),
                        session_segment: session_segment.clone(),
                        trading_day: trading_day.clone(),
                    });
                }
            }
        }

        // Update prev counts from the full set
        self.prev_large_trade_counts.clear();
        for (price, count) in &large_prices {
            self.prev_large_trade_counts
                .insert(discretize_price(*price), *count);
        }
    }

    /// Dedup check with per-event-type gap.
    fn should_emit(&mut self, event_key: &str, timestamp_ms: f64, min_gap_ms: f64) -> bool {
        if let Some(&last_ts) = self.last_event_ts.get(event_key) {
            if timestamp_ms - last_ts < min_gap_ms {
                return false;
            }
        }
        self.last_event_ts
            .insert(event_key.to_string(), timestamp_ms);
        true
    }
}

/// Discretize a price to integer key (NQ tick size 0.25).
fn discretize_price(price: f64) -> i64 {
    (price / 0.25).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitter_starts_with_zero_counts() {
        let emitter = FlowEventEmitter::new();
        assert_eq!(emitter.prev_absorption_count, 0);
        assert_eq!(emitter.prev_pinch_count, 0);
        assert_eq!(emitter.prev_zone_count, 0);
    }

    #[test]
    fn reset_clears_state() {
        let mut emitter = FlowEventEmitter::new();
        emitter.prev_absorption_count = 5;
        emitter.prev_held_zones.push((21000.0, 20995.0));
        emitter.last_event_ts.insert("test".into(), 1000.0);
        emitter.reset();
        assert_eq!(emitter.prev_absorption_count, 0);
        assert!(emitter.prev_held_zones.is_empty());
        assert!(emitter.last_event_ts.is_empty());
    }

    #[test]
    fn dedup_respects_gap() {
        let mut emitter = FlowEventEmitter::new();
        assert!(emitter.should_emit("test_key", 1000.0, 30_000.0));
        assert!(!emitter.should_emit("test_key", 20_000.0, 30_000.0));
        assert!(emitter.should_emit("test_key", 31_001.0, 30_000.0));
    }

    #[test]
    fn detects_leg_events_as_non_dom_types() {
        let mut pipelines = PipelineEngine::new();
        pipelines
            .leg_profile
            .on_trade(0.0, 21_000.0, 10.0, true, false);
        pipelines
            .leg_profile
            .on_trade(5_000.0, 21_008.0, 50.0, true, false);
        pipelines
            .leg_profile
            .on_trade(16_000.0, 21_016.0, 10.0, true, false);
        pipelines
            .leg_profile
            .on_trade(20_000.0, 21_008.0, 8.0, false, false);
        assert!(!pipelines.leg_profile.recent_events().is_empty());

        let mut emitter = FlowEventEmitter::new();
        let events = emitter.detect(&pipelines, 20_000.0, "2026-03-03", 21_008.0);
        assert!(events.iter().any(|e| e.event_type == "leg_started"));
        assert!(events.iter().any(|e| e.event_type == "leg_completed"));
        assert_eq!(
            emitter.prev_leg_event_seq,
            pipelines.leg_profile.event_seq()
        );
        let events2 = emitter.detect(&pipelines, 21_000.0, "2026-03-03", 21_008.0);
        assert!(events2
            .iter()
            .all(|e| e.event_type != "leg_started" && e.event_type != "leg_completed"));
    }

    fn pump_leg_oscillations(pipelines: &mut PipelineEngine, cycles: usize) -> f64 {
        pipelines
            .leg_profile
            .on_trade(0.0, 21_000.0, 20.0, true, false);
        pipelines
            .leg_profile
            .on_trade(5_000.0, 21_008.0, 20.0, true, false);
        pipelines
            .leg_profile
            .on_trade(16_000.0, 21_016.0, 20.0, true, false);
        let mut t = 16_000.0;
        let mut high = true;
        for _ in 0..cycles {
            t += 16_000.0;
            if high {
                pipelines
                    .leg_profile
                    .on_trade(t, 21_008.0, 40.0, false, false);
            } else {
                pipelines
                    .leg_profile
                    .on_trade(t, 21_016.0, 40.0, true, false);
            }
            high = !high;
        }
        t
    }

    #[test]
    fn detect_legs_still_emits_after_event_ring_saturates() {
        let mut pipelines = PipelineEngine::new();
        let t = pump_leg_oscillations(&mut pipelines, 40);
        assert!(pipelines.leg_profile.event_seq() > 64);
        assert_eq!(pipelines.leg_profile.recent_events().len(), 64);

        let mut emitter = FlowEventEmitter::new();
        let first = emitter.detect(&pipelines, t, "2026-03-03", 21_008.0);
        assert!(first.iter().any(|e| e.event_type == "leg_started"));
        let seq_after_first = pipelines.leg_profile.event_seq();

        let t2 = t + 16_000.0;
        pipelines
            .leg_profile
            .on_trade(t2, 21_016.0, 40.0, true, false);
        let t3 = t2 + 16_000.0;
        pipelines
            .leg_profile
            .on_trade(t3, 21_008.0, 40.0, false, false);
        assert!(pipelines.leg_profile.event_seq() > seq_after_first);

        let second = emitter.detect(&pipelines, t3, "2026-03-03", 21_008.0);
        assert!(
            second
                .iter()
                .any(|e| e.event_type == "leg_started" || e.event_type == "leg_completed"),
            "new rotations after the ring saturates must still emit"
        );
        assert_eq!(
            emitter.prev_leg_event_seq,
            pipelines.leg_profile.event_seq()
        );
    }

    #[test]
    fn detect_legs_recovers_when_pipeline_resets_without_emitter_reset() {
        let mut pipelines = PipelineEngine::new();
        let t = pump_leg_oscillations(&mut pipelines, 2);
        let mut emitter = FlowEventEmitter::new();
        let _ = emitter.detect(&pipelines, t, "2026-03-03", 21_008.0);
        assert!(emitter.prev_leg_event_seq > 0);

        pipelines.leg_profile.reset();
        pipelines
            .leg_profile
            .on_trade(0.0, 21_000.0, 20.0, true, false);
        pipelines
            .leg_profile
            .on_trade(5_000.0, 21_008.0, 20.0, true, false);
        pipelines
            .leg_profile
            .on_trade(16_000.0, 21_016.0, 20.0, true, false);
        let recovered = emitter.detect(&pipelines, 16_000.0, "2026-03-03", 21_016.0);
        assert!(recovered.iter().any(|e| e.event_type == "leg_started"));
    }

    #[test]
    fn maps_absorption_status_to_market_event_type() {
        assert_eq!(
            FlowEventEmitter::absorption_market_event_type("candidate"),
            "absorption_detected"
        );
        assert_eq!(
            FlowEventEmitter::absorption_market_event_type("confirmed"),
            "absorption_confirmed"
        );
        assert_eq!(
            FlowEventEmitter::absorption_market_event_type("invalidated"),
            "absorption_invalidated"
        );
    }

    #[test]
    fn detects_absorption_events() {
        let mut pipelines = PipelineEngine::new();
        let key_levels = [super::super::levels::KeyLevel {
            level_type: super::super::levels::KeyLevelType::PriorDayHigh,
            price: 21001.0,
        }];
        for i in 0..14 {
            pipelines.absorption.on_trade(
                1_000.0 + i as f64 * 250.0,
                21000.0 + (i.min(4) as f64 * 0.25),
                10.0,
                0.25,
                true,
                5,
                0.7,
                1.0,
                &key_levels,
            );
        }
        assert!(!pipelines.absorption.recent_events().is_empty());

        let mut emitter = FlowEventEmitter::new();
        let events = emitter.detect(&pipelines, 2000.0, "2026-02-26", 21000.0);
        assert!(events.iter().any(|e| e.event_type == "absorption_detected"));
        assert_eq!(
            emitter.prev_absorption_count,
            pipelines.absorption.recent_events().len()
        );

        // Second call should not re-emit the same events
        let events2 = emitter.detect(&pipelines, 3000.0, "2026-02-26", 21000.0);
        assert!(events2
            .iter()
            .all(|e| e.event_type != "absorption_detected"));
    }

    #[test]
    fn empty_pipelines_emit_nothing() {
        let pipelines = PipelineEngine::new();
        let mut emitter = FlowEventEmitter::new();
        let events = emitter.detect(&pipelines, 1000.0, "2026-02-26", 21000.0);
        assert!(events.is_empty());
    }

    fn pull_heavy_book(
        ts: f64,
    ) -> (
        crate::depth::DomSnapshot,
        crate::depth::PullStackActivitySummary,
    ) {
        use crate::depth::{
            DepthBook, DepthCommand, DepthRecord, DepthSide, PullStackActivitySummary,
            SideActivitySummary,
        };
        let mut book = DepthBook::default();
        book.apply(&DepthRecord {
            timestamp_ms: ts,
            command: DepthCommand::AddBidLevel,
            side: Some(DepthSide::Bid),
            end_of_batch: true,
            num_orders: 1,
            price: 21_000.0,
            quantity: 10,
        });
        book.apply(&DepthRecord {
            timestamp_ms: ts,
            command: DepthCommand::AddAskLevel,
            side: Some(DepthSide::Ask),
            end_of_batch: true,
            num_orders: 1,
            price: 21_000.25,
            quantity: 40,
        });
        let activity = PullStackActivitySummary {
            bid: SideActivitySummary {
                add_events: 0,
                modify_up_events: 0,
                modify_down_events: 4,
                delete_events: 4,
                stacked_quantity: 0.0,
                removed_quantity: 80.0,
                estimated_filled_quantity: 10.0,
                estimated_pulled_quantity: 70.0,
            },
            ask: SideActivitySummary {
                add_events: 2,
                modify_up_events: 2,
                modify_down_events: 0,
                delete_events: 0,
                stacked_quantity: 40.0,
                removed_quantity: 5.0,
                estimated_filled_quantity: 5.0,
                estimated_pulled_quantity: 0.0,
            },
            ..Default::default()
        };
        (book.snapshot("test.depth", ts, 10), activity)
    }

    #[test]
    fn detects_dom_cluster_events_as_capsule_types() {
        let mut pipelines = PipelineEngine::new();
        let ts = 1_704_207_600_000.0;
        let (snap, activity) = pull_heavy_book(ts);
        pipelines.on_dom_feature(&snap, &activity, ts);
        assert!(!pipelines.dom_cluster.recent_events().is_empty());

        let mut emitter = FlowEventEmitter::new();
        let events = emitter.detect(&pipelines, ts, "2024-01-02", 21_000.0);
        assert!(events.iter().any(|e| e.event_type == "pull_intent"));
        assert!(events
            .iter()
            .all(|e| { e.event_type != "mm_flow" && e.event_type != "mm_flow_shift" }));
        assert_eq!(
            emitter.prev_dom_event_seq,
            pipelines.dom_cluster.event_seq()
        );
        let events2 = emitter.detect(&pipelines, ts + 1.0, "2024-01-02", 21_000.0);
        assert!(events2.iter().all(|e| e.event_type != "pull_intent"));
    }

    #[test]
    fn detect_dom_recovers_when_pipeline_resets_without_emitter_reset() {
        let mut pipelines = PipelineEngine::new();
        let ts = 1_704_207_600_000.0;
        let (snap, activity) = pull_heavy_book(ts);
        pipelines.on_dom_feature(&snap, &activity, ts);
        let mut emitter = FlowEventEmitter::new();
        let _ = emitter.detect(&pipelines, ts, "2024-01-02", 21_000.0);
        assert!(emitter.prev_dom_event_seq > 0);

        pipelines.dom_cluster.reset();
        pipelines.on_dom_feature(&snap, &activity, ts);
        let recovered = emitter.detect(&pipelines, ts, "2024-01-02", 21_000.0);
        assert!(recovered.iter().any(|e| e.event_type == "pull_intent"));
    }

    #[test]
    fn detect_dom_into_does_not_steal_other_watermarks() {
        let mut pipelines = PipelineEngine::new();
        let ts = 1_704_207_600_000.0;
        let (snap, activity) = pull_heavy_book(ts);
        pipelines.on_dom_feature(&snap, &activity, ts);

        let mut emitter = FlowEventEmitter::new();
        emitter.prev_absorption_count = 7;
        emitter.prev_pinch_count = 3;
        emitter.prev_leg_event_seq = 11;
        emitter.prev_zone_count = 2;
        let prev_large = emitter.prev_large_trade_counts.len();

        let mut events = Vec::new();
        emitter.detect_dom_into(&pipelines, ts, "2024-01-02", &mut events);
        assert!(events.iter().any(|e| e.event_type == "pull_intent"));
        assert_eq!(emitter.prev_absorption_count, 7);
        assert_eq!(emitter.prev_pinch_count, 3);
        assert_eq!(emitter.prev_leg_event_seq, 11);
        assert_eq!(emitter.prev_zone_count, 2);
        assert_eq!(emitter.prev_large_trade_counts.len(), prev_large);
        assert_eq!(
            emitter.prev_dom_event_seq,
            pipelines.dom_cluster.event_seq()
        );
    }
}
