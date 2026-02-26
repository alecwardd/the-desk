use serde::{Deserialize, Serialize};

/// Type of acceleration zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ZoneType {
    Buy,
    Sell,
}

/// Lifecycle status of an acceleration zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ZoneStatus {
    Fresh,
    Retested,
    Held,
    Failed,
}

/// An acceleration zone identified by one-sided aggressive activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccelerationZone {
    pub zone_type: ZoneType,
    pub status: ZoneStatus,
    pub high: f64,
    pub low: f64,
    pub timestamp_ms: f64,
    pub volume: f64,
    pub delta: f64,
}

impl AccelerationZone {
    pub fn mid(&self) -> f64 {
        (self.high + self.low) / 2.0
    }
}

/// Detects rebid/reoffer acceleration zones and tracks their lifecycle.
#[derive(Debug, Default)]
pub struct RebidReofferPipeline {
    zones: Vec<AccelerationZone>,
    /// Rolling window of recent trades for acceleration detection.
    window: Vec<WindowTrade>,
    avg_bar_range: f64,
    bar_count: u64,
    bar_range_sum: f64,
    current_bar_high: f64,
    current_bar_low: f64,
    current_bar_buy_vol: f64,
    current_bar_sell_vol: f64,
    current_bar_start_ms: f64,
    bar_interval_ms: f64,
    bar_initialized: bool,
}

// WindowTrade reserved for future rolling-window analysis.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct WindowTrade {
    price: f64,
    volume: f64,
    is_buy: bool,
    timestamp_ms: f64,
}

const PROXIMITY_TICKS: f64 = 2.0;
const TICK_SIZE: f64 = 0.25;
const MAX_ZONES: usize = 50;

impl RebidReofferPipeline {
    pub fn new() -> Self {
        Self {
            bar_interval_ms: 300_000.0, // 5-minute bars
            ..Default::default()
        }
    }

    pub fn reset(&mut self) {
        self.zones.clear();
        self.window.clear();
        self.avg_bar_range = 0.0;
        self.bar_count = 0;
        self.bar_range_sum = 0.0;
        self.current_bar_high = 0.0;
        self.current_bar_low = 0.0;
        self.current_bar_buy_vol = 0.0;
        self.current_bar_sell_vol = 0.0;
        self.current_bar_start_ms = 0.0;
        self.bar_initialized = false;
    }

    pub fn on_trade(&mut self, price: f64, volume: f64, is_buy: bool, timestamp_ms: f64) {
        if !self.bar_initialized {
            self.current_bar_start_ms = timestamp_ms;
            self.current_bar_high = price;
            self.current_bar_low = price;
            self.bar_initialized = true;
        }

        self.current_bar_high = self.current_bar_high.max(price);
        self.current_bar_low = self.current_bar_low.min(price);
        if is_buy {
            self.current_bar_buy_vol += volume;
        } else {
            self.current_bar_sell_vol += volume;
        }

        // Complete bar when interval elapses
        if timestamp_ms - self.current_bar_start_ms >= self.bar_interval_ms {
            self.complete_bar(timestamp_ms);
        }

        // Check zone retests
        self.update_zone_status(price, is_buy);
    }

    fn complete_bar(&mut self, timestamp_ms: f64) {
        let range = self.current_bar_high - self.current_bar_low;

        // Check for acceleration against the PRIOR average (before including this bar)
        let prior_avg = if self.bar_count > 0 {
            self.bar_range_sum / self.bar_count as f64
        } else {
            range // first bar can't be acceleration
        };

        self.bar_range_sum += range;
        self.bar_count += 1;
        self.avg_bar_range = self.bar_range_sum / self.bar_count as f64;

        let total_vol = self.current_bar_buy_vol + self.current_bar_sell_vol;
        if total_vol > 0.0 && prior_avg > 0.0 {
            let buy_pct = self.current_bar_buy_vol / total_vol;
            let sell_pct = self.current_bar_sell_vol / total_vol;
            let is_acceleration = range >= prior_avg * 2.0;
            let is_one_sided = buy_pct > 0.70 || sell_pct > 0.70;

            if is_acceleration && is_one_sided {
                let zone_type = if buy_pct > sell_pct {
                    ZoneType::Buy
                } else {
                    ZoneType::Sell
                };
                let zone = AccelerationZone {
                    zone_type,
                    status: ZoneStatus::Fresh,
                    high: self.current_bar_high,
                    low: self.current_bar_low,
                    timestamp_ms,
                    volume: total_vol,
                    delta: self.current_bar_buy_vol - self.current_bar_sell_vol,
                };
                self.zones.push(zone);
                if self.zones.len() > MAX_ZONES {
                    self.zones.remove(0);
                }
            }
        }

        self.current_bar_start_ms = 0.0;
        self.current_bar_high = 0.0;
        self.current_bar_low = 0.0;
        self.current_bar_buy_vol = 0.0;
        self.current_bar_sell_vol = 0.0;
        self.bar_initialized = false;
    }

    fn update_zone_status(&mut self, price: f64, is_buy: bool) {
        let proximity = PROXIMITY_TICKS * TICK_SIZE;
        for zone in &mut self.zones {
            if zone.status == ZoneStatus::Held || zone.status == ZoneStatus::Failed {
                continue;
            }

            let near_zone = price >= zone.low - proximity && price <= zone.high + proximity;
            let through_zone = match zone.zone_type {
                ZoneType::Buy => price < zone.low - proximity,
                ZoneType::Sell => price > zone.high + proximity,
            };

            if through_zone {
                zone.status = ZoneStatus::Failed;
            } else if near_zone {
                if zone.status == ZoneStatus::Fresh {
                    zone.status = ZoneStatus::Retested;
                }
                // Check delta confirmation on retest
                if zone.status == ZoneStatus::Retested {
                    let delta_confirms = match zone.zone_type {
                        ZoneType::Buy => is_buy,
                        ZoneType::Sell => !is_buy,
                    };
                    if delta_confirms {
                        zone.status = ZoneStatus::Held;
                    }
                }
            }
        }
    }

    pub fn active_zones(&self) -> Vec<&AccelerationZone> {
        self.zones
            .iter()
            .filter(|z| z.status != ZoneStatus::Failed)
            .collect()
    }

    pub fn all_zones(&self) -> &[AccelerationZone] {
        &self.zones
    }

    /// Zones near a given price within N ticks.
    pub fn zones_near_price(&self, price: f64, ticks: f64) -> Vec<&AccelerationZone> {
        let proximity = ticks * TICK_SIZE;
        self.zones
            .iter()
            .filter(|z| {
                z.status != ZoneStatus::Failed
                    && price >= z.low - proximity
                    && price <= z.high + proximity
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_acceleration_zone() {
        let mut p = RebidReofferPipeline::new();
        p.bar_interval_ms = 100.0; // short bars for testing

        // First bar: establish baseline range (spans >= 100ms)
        p.on_trade(21000.0, 1.0, true, 0.0);
        p.on_trade(21001.0, 1.0, true, 50.0);
        p.on_trade(21001.0, 1.0, true, 100.0); // completes bar, range = 1.0

        // Second bar: acceleration (range >= 2x avg, one-sided, spans >= 100ms)
        p.on_trade(21000.0, 10.0, true, 110.0);
        p.on_trade(21005.0, 10.0, true, 150.0);
        p.on_trade(21005.0, 1.0, false, 210.0); // completes bar at 210, range = 5.0

        assert!(!p.zones.is_empty());
        assert_eq!(p.zones[0].zone_type, ZoneType::Buy);
    }
}
