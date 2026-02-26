pub mod backfill;
pub mod db;
pub mod dtc;
pub mod feed;
pub mod pipelines;
pub mod recording;
pub mod research;
pub mod risk;
pub mod rules;
pub mod templates;

use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;

/// NQ session boundaries in Eastern Time (hour * 60 + minute).
pub const RTH_OPEN_ET: i32 = 9 * 60 + 30; // 09:30
pub const RTH_CLOSE_ET: i32 = 16 * 60 + 15; // 16:15
pub const GLOBEX_OPEN_ET: i32 = 18 * 60; // 18:00

/// Session type for NQ futures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionType {
    Rth,
    Globex,
    Unknown,
}

/// Classify which session a given ET-minute falls into.
pub fn classify_session(et_minutes: i32) -> SessionType {
    if (RTH_OPEN_ET..RTH_CLOSE_ET).contains(&et_minutes) {
        SessionType::Rth
    } else if !(RTH_OPEN_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        SessionType::Globex
    } else {
        SessionType::Unknown
    }
}

/// Convert a Unix timestamp (milliseconds) to ET minutes-from-midnight.
pub fn et_minutes_from_timestamp(timestamp_ms: f64) -> Option<i32> {
    let ts = timestamp_ms as i64;
    Utc.timestamp_millis_opt(ts).single().map(|utc| {
        let et = utc.with_timezone(&Eastern);
        (et.hour() as i32 * 60) + et.minute() as i32
    })
}

/// Compute the session minute relative to RTH open (09:30 ET = minute 0).
/// Negative values indicate pre-RTH (Globex/overnight).
pub fn minute_of_session_from_timestamp(timestamp_ms: f64) -> i32 {
    let dt_utc = if timestamp_ms > 1_000_000_000_000.0 {
        Utc.timestamp_millis_opt(timestamp_ms as i64).single()
    } else if timestamp_ms > 1_000_000_000.0 {
        Utc.timestamp_opt(timestamp_ms as i64, 0).single()
    } else {
        None
    };

    if let Some(utc) = dt_utc {
        let et = utc.with_timezone(&Eastern);
        let total_minutes = (et.hour() as i32 * 60) + et.minute() as i32;
        return total_minutes - RTH_OPEN_ET;
    }

    0
}

/// Format a timestamp as a session date string (YYYY-MM-DD) in Eastern Time.
pub fn session_date_from_timestamp_ms(timestamp_ms: f64) -> String {
    let ts = timestamp_ms as i64;
    if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
        dt.with_timezone(&Eastern).format("%Y-%m-%d").to_string()
    } else {
        chrono::Local::now().format("%Y-%m-%d").to_string()
    }
}

/// Compute a Globex open timestamp (6 PM ET) going `days_back` calendar days.
/// `days_back=1` → current Globex session open (yesterday 6 PM if before 6 PM today).
/// `days_back=2` → prior Globex session open.
pub fn globex_open_ms(days_back: i64) -> f64 {
    let now_et = chrono::Utc::now().with_timezone(&Eastern);
    let base_date = if now_et.hour() >= 18 {
        now_et.date_naive()
    } else {
        now_et.date_naive() - chrono::Duration::days(1)
    };
    let target_date = base_date - chrono::Duration::days(days_back - 1);
    let globex_open = target_date.and_hms_opt(18, 0, 0).unwrap();
    Eastern
        .from_local_datetime(&globex_open)
        .single()
        .map(|dt| dt.timestamp_millis() as f64)
        .unwrap_or(0.0)
}
