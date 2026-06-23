use super::footprint::{FootprintPipeline, StackedZone};
use serde::{Deserialize, Serialize};

/// Type of acceleration zone. `Buy` = rebid/support, `Sell` = reoffer/resistance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ZoneType {
    Buy,
    Sell,
}

/// Lifecycle status of an acceleration zone (trader doctrine, see
/// memory `rebid-reoffer-zone-doctrine`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ZoneStatus {
    /// Formed and price has moved away; on the watchlist, awaiting a retest.
    Fresh,
    /// Price re-entered the band — this is the entry trigger.
    Retested,
    /// After a retest, price fired back in the zone direction — continuation.
    Held,
    /// Price extended through the band with acceptance — trend change / not real.
    Failed,
    /// Price ran far away and never returned to the band — strong-trend tell.
    Abandoned,
}

/// An acceleration zone: a band of stacked one-sided footprint delta.
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
    /// When the zone was first retested (entry), if it has been.
    #[serde(default)]
    pub retested_ms: Option<f64>,
    /// When price first crossed the failure threshold (for acceptance dwell).
    #[serde(default, skip_serializing)]
    pub failure_cross_ms: Option<f64>,
}

impl AccelerationZone {
    pub fn mid(&self) -> f64 {
        (self.high + self.low) / 2.0
    }
}

/// Tape signals used to decide whether a break through a zone is "acceptance"
/// (a real failure) versus a whip that should fire back.
#[derive(Debug, Clone, Copy, Default)]
pub struct ZoneAcceptance {
    /// Tape pace percentile in 0.0–1.0.
    pub pace_percentile: f64,
    /// RVOL velocity (positive = volume building).
    pub rvol_velocity: f64,
    /// Smoothed tape acceleration (positive = pace speeding up).
    pub tape_acceleration: f64,
}

impl ZoneAcceptance {
    /// Volume/pace confirmation that the auction is continuing through a level.
    fn is_accepting(&self) -> bool {
        self.pace_percentile >= ACCEPT_PACE_PERCENTILE
            || self.rvol_velocity > 0.0
            || self.tape_acceleration > 0.0
    }
}

/// Directional, proximity- and status-aware zone read for the snapshot/rules.
#[derive(Debug, Clone, Default)]
pub struct ZoneSignal {
    pub rebid_near: bool,
    pub reoffer_near: bool,
    pub rebid_retested: bool,
    pub reoffer_retested: bool,
    pub rebid_held: bool,
    pub reoffer_held: bool,
    pub nearest_direction: Option<String>,
    pub nearest_status: Option<String>,
    pub nearest_distance_ticks: Option<f64>,
}

/// Detects footprint rebid/reoffer acceleration zones and tracks their lifecycle.
#[derive(Debug, Default)]
pub struct RebidReofferPipeline {
    zones: Vec<AccelerationZone>,
    last_rescan_ms: f64,
}

const TICK_SIZE: f64 = 0.25;
/// Minimum consecutive one-sided levels to form a band.
const MIN_STACKED_LEVELS: usize = 5;
/// Imbalance ratio for a level to count as one-sided.
const IMBALANCE_RATIO: f64 = 3.0;
/// Price must move this many ticks away from a band before it becomes a zone.
const MOVE_AWAY_TICKS: f64 = 5.0;
/// A retest may poke this many ticks past the far edge and still be a retest.
const POKE_TOLERANCE_TICKS: f64 = 5.0;
/// Beyond the poke tolerance, this much further (with acceptance) is a failure.
const FAILURE_EXTENSION_TICKS: f64 = 5.0;
/// "Near price" proximity for `*_near` signals.
const PROXIMITY_TICKS: f64 = 5.0;
/// Price must travel this far away (without a retest) to abandon a zone.
const ABANDON_DISTANCE_TICKS: f64 = 20.0;
/// ...and this much time must pass before a zone is abandoned.
const ABANDON_TIME_MS: f64 = 600_000.0;
/// Acceptance dwell: price must hold beyond the failure level this long.
const FAILURE_DWELL_MS: f64 = 25_000.0;
/// Pace percentile at/above which tape counts as "accepting".
const ACCEPT_PACE_PERCENTILE: f64 = 0.6;
/// How often to rescan the footprint for new bands.
const RESCAN_INTERVAL_MS: f64 = 2_000.0;
const MAX_ZONES: usize = 50;

impl RebidReofferPipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.zones.clear();
        self.last_rescan_ms = 0.0;
    }

    /// Process a trade: periodically rescan the footprint for new bands, then
    /// advance the lifecycle of existing zones against the current price.
    pub fn on_trade(
        &mut self,
        price: f64,
        timestamp_ms: f64,
        footprint: &FootprintPipeline,
        accept: ZoneAcceptance,
    ) {
        if timestamp_ms - self.last_rescan_ms >= RESCAN_INTERVAL_MS {
            self.last_rescan_ms = timestamp_ms;
            self.detect_new_zones(price, timestamp_ms, footprint);
        }
        self.update_lifecycle(price, timestamp_ms, accept);
    }

    /// Distance in ticks price sits away from a band (0 if inside it).
    fn ticks_away(price: f64, low: f64, high: f64) -> f64 {
        if price > high {
            (price - high) / TICK_SIZE
        } else if price < low {
            (low - price) / TICK_SIZE
        } else {
            0.0
        }
    }

    fn detect_new_zones(&mut self, price: f64, timestamp_ms: f64, footprint: &FootprintPipeline) {
        let bands: Vec<StackedZone> =
            footprint.stacked_imbalance_zones(IMBALANCE_RATIO, MIN_STACKED_LEVELS);
        for band in bands {
            // Skip if a live zone already covers this band.
            let overlaps_existing = self.zones.iter().any(|z| {
                !matches!(z.status, ZoneStatus::Failed | ZoneStatus::Abandoned)
                    && z.low <= band.high
                    && z.high >= band.low
            });
            if overlaps_existing {
                continue;
            }
            // Only form once price has clearly initiated away from the band.
            if Self::ticks_away(price, band.low, band.high) < MOVE_AWAY_TICKS {
                continue;
            }
            self.zones.push(AccelerationZone {
                zone_type: if band.is_buy {
                    ZoneType::Buy
                } else {
                    ZoneType::Sell
                },
                status: ZoneStatus::Fresh,
                high: band.high,
                low: band.low,
                timestamp_ms,
                volume: band.total,
                delta: band.delta,
                retested_ms: None,
                failure_cross_ms: None,
            });
            if self.zones.len() > MAX_ZONES {
                self.zones.remove(0);
            }
        }
    }

    fn update_lifecycle(&mut self, price: f64, timestamp_ms: f64, accept: ZoneAcceptance) {
        let poke = POKE_TOLERANCE_TICKS * TICK_SIZE;
        let fail_extra = FAILURE_EXTENSION_TICKS * TICK_SIZE;
        let accepting = accept.is_accepting();
        for zone in &mut self.zones {
            if matches!(zone.status, ZoneStatus::Failed | ZoneStatus::Abandoned) {
                continue;
            }
            match zone.zone_type {
                ZoneType::Buy => {
                    // Entry: price re-enters the band from above.
                    if zone.status == ZoneStatus::Fresh && price <= zone.high {
                        zone.status = ZoneStatus::Retested;
                        zone.retested_ms = Some(timestamp_ms);
                    }
                    // Held: after a retest, price fires back up out of the band.
                    if zone.status == ZoneStatus::Retested && price > zone.high {
                        zone.status = ZoneStatus::Held;
                    }
                    // Failure: deep break below the poke tolerance, with acceptance.
                    let fail_level = zone.low - poke - fail_extra;
                    if price < fail_level {
                        let crossed = *zone.failure_cross_ms.get_or_insert(timestamp_ms);
                        if accepting && timestamp_ms - crossed >= FAILURE_DWELL_MS {
                            zone.status = ZoneStatus::Failed;
                        }
                    } else {
                        zone.failure_cross_ms = None; // came back -> whip, reset
                    }
                    // Abandoned: never retested, price ran far above for long enough.
                    if zone.status == ZoneStatus::Fresh
                        && (price - zone.high) / TICK_SIZE >= ABANDON_DISTANCE_TICKS
                        && timestamp_ms - zone.timestamp_ms >= ABANDON_TIME_MS
                    {
                        zone.status = ZoneStatus::Abandoned;
                    }
                }
                ZoneType::Sell => {
                    // Entry: price re-enters the band from below.
                    if zone.status == ZoneStatus::Fresh && price >= zone.low {
                        zone.status = ZoneStatus::Retested;
                        zone.retested_ms = Some(timestamp_ms);
                    }
                    // Held: after a retest, price fires back down out of the band.
                    if zone.status == ZoneStatus::Retested && price < zone.low {
                        zone.status = ZoneStatus::Held;
                    }
                    // Failure: deep break above the poke tolerance, with acceptance.
                    let fail_level = zone.high + poke + fail_extra;
                    if price > fail_level {
                        let crossed = *zone.failure_cross_ms.get_or_insert(timestamp_ms);
                        if accepting && timestamp_ms - crossed >= FAILURE_DWELL_MS {
                            zone.status = ZoneStatus::Failed;
                        }
                    } else {
                        zone.failure_cross_ms = None;
                    }
                    if zone.status == ZoneStatus::Fresh
                        && (zone.low - price) / TICK_SIZE >= ABANDON_DISTANCE_TICKS
                        && timestamp_ms - zone.timestamp_ms >= ABANDON_TIME_MS
                    {
                        zone.status = ZoneStatus::Abandoned;
                    }
                }
            }
        }
    }

    /// Zones that have not failed or been abandoned.
    pub fn active_zones(&self) -> Vec<&AccelerationZone> {
        self.zones
            .iter()
            .filter(|z| !matches!(z.status, ZoneStatus::Failed | ZoneStatus::Abandoned))
            .collect()
    }

    pub fn all_zones(&self) -> &[AccelerationZone] {
        &self.zones
    }

    /// Non-terminal zones within `ticks` of `price`.
    pub fn zones_near_price(&self, price: f64, ticks: f64) -> Vec<&AccelerationZone> {
        let proximity = ticks * TICK_SIZE;
        self.zones
            .iter()
            .filter(|z| {
                !matches!(z.status, ZoneStatus::Failed | ZoneStatus::Abandoned)
                    && price >= z.low - proximity
                    && price <= z.high + proximity
            })
            .collect()
    }

    /// Directional, proximity- and status-aware read for the snapshot/rules.
    pub fn zone_signal(&self, price: f64) -> ZoneSignal {
        let mut sig = ZoneSignal::default();
        let mut nearest: Option<(f64, &AccelerationZone)> = None;
        for zone in &self.zones {
            if matches!(zone.status, ZoneStatus::Failed | ZoneStatus::Abandoned) {
                continue;
            }
            let dist_ticks = Self::ticks_away(price, zone.low, zone.high);
            let near = dist_ticks <= PROXIMITY_TICKS;
            match zone.zone_type {
                ZoneType::Buy => {
                    sig.rebid_near |= near;
                    sig.rebid_retested |= near && zone.status == ZoneStatus::Retested;
                    sig.rebid_held |= near && zone.status == ZoneStatus::Held;
                }
                ZoneType::Sell => {
                    sig.reoffer_near |= near;
                    sig.reoffer_retested |= near && zone.status == ZoneStatus::Retested;
                    sig.reoffer_held |= near && zone.status == ZoneStatus::Held;
                }
            }
            if nearest.map(|(d, _)| dist_ticks < d).unwrap_or(true) {
                nearest = Some((dist_ticks, zone));
            }
        }
        if let Some((dist, zone)) = nearest {
            sig.nearest_direction = Some(format!("{:?}", zone.zone_type).to_lowercase());
            sig.nearest_status = Some(format!("{:?}", zone.status).to_lowercase());
            sig.nearest_distance_ticks = Some(dist);
        }
        sig
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipelines::footprint::FootprintPipeline;

    /// Build a footprint with a buy-stacked band at 21000.00..21001.00.
    fn buy_stacked_footprint(ts: f64) -> FootprintPipeline {
        let mut fp = FootprintPipeline::new(TICK_SIZE);
        for i in 0..5 {
            let price = 21_000.0 + i as f64 * TICK_SIZE;
            fp.on_trade(price, 30.0, true, ts);
            fp.on_trade(price, 5.0, false, ts);
        }
        fp
    }

    #[test]
    fn forms_zone_only_after_move_away() {
        let mut p = RebidReofferPipeline::new();
        let fp = buy_stacked_footprint(1_000.0);
        // Price still inside the band -> no zone yet.
        p.on_trade(21_000.5, 1_000.0, &fp, ZoneAcceptance::default());
        assert!(p.all_zones().is_empty());
        // Price initiates >5 ticks above -> a Fresh buy zone forms.
        p.on_trade(21_003.0, 4_000.0, &fp, ZoneAcceptance::default());
        assert_eq!(p.all_zones().len(), 1);
        let z = &p.all_zones()[0];
        assert_eq!(z.zone_type, ZoneType::Buy);
        assert_eq!(z.status, ZoneStatus::Fresh);
    }

    #[test]
    fn retest_then_held_lifecycle() {
        let mut p = RebidReofferPipeline::new();
        let fp = buy_stacked_footprint(1_000.0);
        p.on_trade(21_003.0, 4_000.0, &fp, ZoneAcceptance::default());
        // Drift back into the band -> Retested (entry).
        p.on_trade(21_000.75, 6_000.0, &fp, ZoneAcceptance::default());
        assert_eq!(p.all_zones()[0].status, ZoneStatus::Retested);
        let sig = p.zone_signal(21_000.75);
        assert!(sig.rebid_retested);
        // Fire back up out of the band -> Held.
        p.on_trade(21_002.0, 8_000.0, &fp, ZoneAcceptance::default());
        assert_eq!(p.all_zones()[0].status, ZoneStatus::Held);
    }

    #[test]
    fn deep_break_with_acceptance_fails() {
        let mut p = RebidReofferPipeline::new();
        let fp = buy_stacked_footprint(1_000.0);
        p.on_trade(21_003.0, 4_000.0, &fp, ZoneAcceptance::default());
        p.on_trade(21_000.75, 6_000.0, &fp, ZoneAcceptance::default()); // retest
        let accepting = ZoneAcceptance {
            pace_percentile: 0.9,
            rvol_velocity: 1.0,
            tape_acceleration: 1.0,
        };
        // Break ~11 ticks below the low (past poke+extension) and hold with acceptance.
        let fail_price = 21_000.0 - 11.0 * TICK_SIZE;
        p.on_trade(fail_price, 30_000.0, &fp, accepting); // crosses
        p.on_trade(fail_price, 60_000.0, &fp, accepting); // dwelled >25s + accepting
        assert_eq!(p.all_zones()[0].status, ZoneStatus::Failed);
    }

    #[test]
    fn whip_without_acceptance_does_not_fail() {
        let mut p = RebidReofferPipeline::new();
        let fp = buy_stacked_footprint(1_000.0);
        p.on_trade(21_003.0, 4_000.0, &fp, ZoneAcceptance::default());
        p.on_trade(21_000.75, 6_000.0, &fp, ZoneAcceptance::default());
        let fail_price = 21_000.0 - 11.0 * TICK_SIZE;
        // Poke through then fire right back, no acceptance -> stays Retested.
        p.on_trade(fail_price, 7_000.0, &fp, ZoneAcceptance::default());
        p.on_trade(21_001.0, 9_000.0, &fp, ZoneAcceptance::default());
        assert_ne!(p.all_zones()[0].status, ZoneStatus::Failed);
    }
}
