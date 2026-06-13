pub mod attention;
pub mod backfill;
pub mod backup;
pub mod db;
pub mod depth;
pub mod feed;
pub mod mcp;
pub mod memory;
pub mod observability;
pub mod options;
pub mod outcome_tracker;
pub mod outcomes;
pub mod pipelines;
pub mod recording;
pub mod research;
pub mod risk;
pub mod rollover;
pub mod rules;
pub mod scid_tick_ingest;
pub mod scid_timestamp_diagnostics;
pub mod templates;

use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;

/// NQ session boundaries in Eastern Time (hour * 60 + minute).
pub const RTH_OPEN_ET: i32 = 9 * 60 + 30; // 09:30
pub const RTH_CLOSE_ET: i32 = 16 * 60; // 16:00
pub const GLOBEX_OPEN_ET: i32 = 18 * 60; // 18:00
pub const LONDON_OPEN_ET: i32 = 2 * 60; // 02:00

/// Session type for NQ futures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionType {
    Rth,
    Globex,
    Unknown,
}

/// Session segment classification for Globex windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSegment {
    Asia,
    London,
    None,
}

/// Delta-reset segment: Asia and London are separate for segment delta; RTH is its own segment.
/// Used to reset segment delta at Asia→London (2 AM ET) while keeping combined Globex delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaSegment {
    Asia,
    London,
    Rth,
    /// 4 PM–6 PM ET gap between RTH close and Globex open.
    Unknown,
}

/// Classify the delta-reset segment for boundary detection.
/// Asia (6 PM–2 AM ET), London (2 AM–9:30 AM ET), RTH (9:30 AM–4 PM ET), Unknown (4–6 PM gap).
pub fn classify_delta_segment(et_minutes: i32) -> DeltaSegment {
    if (RTH_OPEN_ET..RTH_CLOSE_ET).contains(&et_minutes) {
        DeltaSegment::Rth
    } else if (RTH_CLOSE_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        DeltaSegment::Unknown
    } else if !(LONDON_OPEN_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        DeltaSegment::Asia
    } else if (LONDON_OPEN_ET..RTH_OPEN_ET).contains(&et_minutes) {
        DeltaSegment::London
    } else {
        DeltaSegment::Unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickTimeContext {
    pub et_minutes: i32,
    pub minute_of_session: i32,
    pub session_type: SessionType,
    pub session_segment: SessionSegment,
    pub session_date: String,
    pub session_date_key: i32,
    pub trading_day: String,
    pub trading_day_key: i32,
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

/// Classify Globex sub-session segment for a given ET-minute and session type.
pub fn classify_session_segment(et_minutes: i32, session_type: SessionType) -> SessionSegment {
    if session_type != SessionType::Globex {
        return SessionSegment::None;
    }
    if !(LONDON_OPEN_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        SessionSegment::Asia
    } else if (LONDON_OPEN_ET..RTH_OPEN_ET).contains(&et_minutes) {
        SessionSegment::London
    } else {
        SessionSegment::None
    }
}

/// Convert a Unix timestamp to ET minutes-from-midnight.
/// Sierra `.scid` ticks use **epoch milliseconds**. Values in the seconds range are still accepted
/// for backward compatibility with older test fixtures.
pub fn et_minutes_from_timestamp(timestamp_ms: f64) -> Option<i32> {
    let dt_utc = if timestamp_ms > 1_000_000_000_000.0 {
        Utc.timestamp_millis_opt(timestamp_ms as i64).single()
    } else if timestamp_ms > 1_000_000_000.0 {
        Utc.timestamp_opt(timestamp_ms as i64, 0).single()
    } else {
        None
    }?;
    let et = dt_utc.with_timezone(&Eastern);
    Some((et.hour() as i32 * 60) + et.minute() as i32)
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

/// Current date string (YYYY-MM-DD) in Eastern Time.
pub fn et_now_date() -> String {
    Utc::now()
        .with_timezone(&Eastern)
        .format("%Y-%m-%d")
        .to_string()
}

/// Current trading day — rolls forward after 6 PM ET (Globex open).
/// Use this when loading prior-day levels so that during Globex the query
/// `date < trading_day` correctly includes today's RTH data.
pub fn et_now_trading_day() -> String {
    let et = Utc::now().with_timezone(&Eastern);
    let et_minutes = (et.hour() as i32 * 60) + et.minute() as i32;
    let date = if et_minutes >= GLOBEX_OPEN_ET {
        et.date_naive() + chrono::Duration::days(1)
    } else {
        et.date_naive()
    };
    date.format("%Y-%m-%d").to_string()
}

/// Format a timestamp as a session date string (YYYY-MM-DD) in Eastern Time.
pub fn session_date_from_timestamp_ms(timestamp_ms: f64) -> String {
    let ts = timestamp_ms as i64;
    if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
        dt.with_timezone(&Eastern).format("%Y-%m-%d").to_string()
    } else {
        et_now_date()
    }
}

/// Trading day label (YYYY-MM-DD) with a 6:00 PM ET roll.
/// At/after 18:00 ET, ticks are assigned to the next RTH trading day.
pub fn trading_day_from_timestamp_ms(timestamp_ms: f64) -> String {
    let ts = timestamp_ms as i64;
    if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
        let et = dt.with_timezone(&Eastern);
        let date = if (et.hour() as i32 * 60) + et.minute() as i32 >= GLOBEX_OPEN_ET {
            et.date_naive() + chrono::Duration::days(1)
        } else {
            et.date_naive()
        };
        return date.format("%Y-%m-%d").to_string();
    }
    et_now_trading_day()
}

/// Convert a Unix timestamp (milliseconds) to all session-relative ET metadata in one pass.
pub fn tick_time_context_from_timestamp_ms(timestamp_ms: f64) -> Option<TickTimeContext> {
    let ts = timestamp_ms as i64;
    Utc.timestamp_millis_opt(ts).single().map(|utc| {
        let et = utc.with_timezone(&Eastern);
        let et_minutes = (et.hour() as i32 * 60) + et.minute() as i32;
        let session_type = classify_session(et_minutes);
        let session_segment = classify_session_segment(et_minutes, session_type);
        let session_date_key = et.year() * 10_000 + et.month() as i32 * 100 + et.day() as i32;
        let trading_date = if et_minutes >= GLOBEX_OPEN_ET {
            et.date_naive() + chrono::Duration::days(1)
        } else {
            et.date_naive()
        };
        let trading_day_key = trading_date.year() * 10_000
            + trading_date.month() as i32 * 100
            + trading_date.day() as i32;
        TickTimeContext {
            et_minutes,
            minute_of_session: et_minutes - RTH_OPEN_ET,
            session_type,
            session_segment,
            session_date: et.format("%Y-%m-%d").to_string(),
            session_date_key,
            trading_day: trading_date.format("%Y-%m-%d").to_string(),
            trading_day_key,
        }
    })
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
