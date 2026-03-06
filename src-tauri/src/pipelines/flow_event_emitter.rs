use std::collections::HashMap;

use super::event_detector::MarketEvent;
use super::PipelineEngine;
use crate::tick_time_context_from_timestamp_ms;

/// Emits flow events from pipeline ring buffers into the MarketEvent stream.
///
/// Runs alongside the structural `EventDetector`, reading absorption, pinch,
/// rebid/reoffer, and trade size pipelines to produce `MarketEvent` objects
/// that flow into the same `market_events` DB table — making them queryable
/// via `query_event_frequency` and `query_conditional`.
#[derive(Debug)]
pub struct FlowEventEmitter {
    prev_absorption_count: usize,
    prev_pinch_count: usize,
    prev_zone_count: usize,
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
        self.detect_zones(events, pipelines, timestamp_ms, session_date);
        self.detect_large_trade_clusters(
            events,
            pipelines,
            timestamp_ms,
            session_date,
            current_price,
        );
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
}
