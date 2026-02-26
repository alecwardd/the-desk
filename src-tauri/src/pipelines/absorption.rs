use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AbsorptionEvent {
    pub timestamp_ms: f64,
    pub event_type: String,
    pub price: f64,
    pub severity: f64,
}

#[derive(Debug, Default)]
pub struct AbsorptionPipeline {
    tick_size: f64,
    volume_at_price: HashMap<i64, f64>,
    recent_events: Vec<AbsorptionEvent>,
    last_event_at_price: HashMap<i64, f64>,
    volume_history: Vec<(f64, f64)>,
    price_extremes: (f64, f64),
    cumulative_delta: f64,
    delta_at_high: f64,
    delta_at_low: f64,
}

const RATE_LIMIT_MS: f64 = 5_000.0;
const ABSORPTION_THRESHOLD: f64 = 100.0;
const EXHAUSTION_WINDOW: usize = 50;
const MAX_EVENTS: usize = 200;

impl AbsorptionPipeline {
    pub fn new(tick_size: f64) -> Self {
        Self {
            tick_size,
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.volume_at_price.clear();
        self.recent_events.clear();
        self.last_event_at_price.clear();
        self.volume_history.clear();
        self.price_extremes = (0.0, 0.0);
        self.cumulative_delta = 0.0;
        self.delta_at_high = 0.0;
        self.delta_at_low = 0.0;
    }

    fn discretize(&self, price: f64) -> i64 {
        (price / self.tick_size).round() as i64
    }

    pub fn on_trade(
        &mut self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        move_ticks: f64,
        is_buy: bool,
    ) {
        let key = self.discretize(price);
        let at_price = self.volume_at_price.entry(key).or_insert(0.0);
        *at_price += volume;

        let signed_vol = if is_buy { volume } else { -volume };
        self.cumulative_delta += signed_vol;
        self.volume_history.push((volume, move_ticks));
        if self.volume_history.len() > 500 {
            self.volume_history.drain(0..100);
        }

        // Track price extremes and delta at those points
        if self.price_extremes == (0.0, 0.0) {
            self.price_extremes = (price, price);
            self.delta_at_high = self.cumulative_delta;
            self.delta_at_low = self.cumulative_delta;
        }
        if price > self.price_extremes.0 {
            self.price_extremes.0 = price;
            self.delta_at_high = self.cumulative_delta;
        }
        if price < self.price_extremes.1 || self.price_extremes.1 == 0.0 {
            self.price_extremes.1 = price;
            self.delta_at_low = self.cumulative_delta;
        }

        // --- Absorption: high volume at a level with no displacement ---
        if *at_price >= ABSORPTION_THRESHOLD && move_ticks.abs() <= 1.0 {
            let last = self
                .last_event_at_price
                .get(&key)
                .copied()
                .unwrap_or(f64::NEG_INFINITY);
            if timestamp_ms - last >= RATE_LIMIT_MS {
                let severity = (*at_price / ABSORPTION_THRESHOLD).min(5.0);
                self.push_event(AbsorptionEvent {
                    timestamp_ms,
                    event_type: "absorption".to_string(),
                    price: key as f64 * self.tick_size,
                    severity,
                });
                self.last_event_at_price.insert(key, timestamp_ms);
            }
        }

        // --- Exhaustion: declining volume into a directional move ---
        if self.volume_history.len() >= EXHAUSTION_WINDOW {
            let recent = &self.volume_history[self.volume_history.len() - EXHAUSTION_WINDOW..];
            let half = EXHAUSTION_WINDOW / 2;
            let first_half_vol: f64 = recent[..half].iter().map(|(v, _)| v).sum();
            let second_half_vol: f64 = recent[half..].iter().map(|(v, _)| v).sum();
            let net_move: f64 = recent.iter().map(|(_, m)| m).sum();

            if second_half_vol < first_half_vol * 0.6 && net_move.abs() > 4.0 {
                let last_exhaustion = self
                    .recent_events
                    .iter()
                    .rev()
                    .find(|e| e.event_type == "exhaustion")
                    .map(|e| e.timestamp_ms)
                    .unwrap_or(0.0);
                if timestamp_ms - last_exhaustion >= RATE_LIMIT_MS * 2.0 {
                    let severity = (first_half_vol / second_half_vol.max(1.0)).min(5.0);
                    self.push_event(AbsorptionEvent {
                        timestamp_ms,
                        event_type: "exhaustion".to_string(),
                        price,
                        severity,
                    });
                }
            }
        }

        // --- Delta divergence: new price extreme without delta confirmation ---
        let range = self.price_extremes.0 - self.price_extremes.1;
        if range > 8.0 * self.tick_size {
            let price_near_high = (self.price_extremes.0 - price).abs() < 2.0 * self.tick_size;
            let price_near_low = (price - self.price_extremes.1).abs() < 2.0 * self.tick_size;

            if price_near_high && self.cumulative_delta < self.delta_at_high * 0.5 {
                let last_div = self
                    .recent_events
                    .iter()
                    .rev()
                    .find(|e| e.event_type == "delta_divergence")
                    .map(|e| e.timestamp_ms)
                    .unwrap_or(0.0);
                if timestamp_ms - last_div >= RATE_LIMIT_MS * 3.0 {
                    self.push_event(AbsorptionEvent {
                        timestamp_ms,
                        event_type: "delta_divergence".to_string(),
                        price,
                        severity: 3.0,
                    });
                }
            }
            if price_near_low && self.cumulative_delta > self.delta_at_low * 0.5 {
                let last_div = self
                    .recent_events
                    .iter()
                    .rev()
                    .find(|e| e.event_type == "delta_divergence")
                    .map(|e| e.timestamp_ms)
                    .unwrap_or(0.0);
                if timestamp_ms - last_div >= RATE_LIMIT_MS * 3.0 {
                    self.push_event(AbsorptionEvent {
                        timestamp_ms,
                        event_type: "delta_divergence".to_string(),
                        price,
                        severity: 3.0,
                    });
                }
            }
        }
    }

    fn push_event(&mut self, event: AbsorptionEvent) {
        self.recent_events.push(event);
        if self.recent_events.len() > MAX_EVENTS {
            let drain_to = self.recent_events.len() - MAX_EVENTS;
            self.recent_events.drain(0..drain_to);
        }
    }

    pub fn recent_events(&self) -> &[AbsorptionEvent] {
        &self.recent_events
    }
}

#[cfg(test)]
mod tests {
    use super::AbsorptionPipeline;

    #[test]
    fn emits_absorption_event_when_volume_concentrates() {
        let mut p = AbsorptionPipeline::new(0.25);
        for i in 0..15 {
            p.on_trade(1_000.0 + i as f64, 21000.0, 10.0, 0.0, true);
        }
        assert!(!p.recent_events().is_empty());
        assert_eq!(p.recent_events()[0].event_type, "absorption");
    }

    #[test]
    fn rate_limits_absorption_at_same_price() {
        let mut p = AbsorptionPipeline::new(0.25);
        for i in 0..20 {
            p.on_trade(1_000.0 + i as f64, 21000.0, 10.0, 0.0, true);
        }
        let absorptions: Vec<_> = p
            .recent_events()
            .iter()
            .filter(|e| e.event_type == "absorption")
            .collect();
        assert_eq!(
            absorptions.len(),
            1,
            "should only fire once within rate limit"
        );
    }

    #[test]
    fn detects_exhaustion_on_declining_volume() {
        let mut p = AbsorptionPipeline::new(0.25);
        let base_ts = 100_000.0;
        for i in 0..25 {
            p.on_trade(
                base_ts + i as f64 * 100.0,
                21000.0 + i as f64 * 0.25,
                20.0,
                1.0,
                true,
            );
        }
        for i in 25..50 {
            p.on_trade(
                base_ts + i as f64 * 100.0,
                21000.0 + i as f64 * 0.25,
                5.0,
                1.0,
                true,
            );
        }
        let exhaustions: Vec<_> = p
            .recent_events()
            .iter()
            .filter(|e| e.event_type == "exhaustion")
            .collect();
        assert!(
            !exhaustions.is_empty(),
            "should detect exhaustion when volume drops"
        );
    }
}
