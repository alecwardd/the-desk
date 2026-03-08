use crate::db::{Database, DepthEventRecord, RawTickRecord};
use crate::depth::{
    tick_key_to_price, DepthBook, DepthCommand, DepthError, DepthReader, DepthRecord, DepthSide,
    DomLevel,
};
use crate::feed::scid_reader::{ScidReader, ScidTick};
use crate::feed::{FeedConfig, TradeSide};
use crate::trading_day_from_timestamp_ms;
use chrono::{NaiveDate, TimeZone};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use thiserror::Error;

const MAX_TAPE_PRINTS: usize = 50;
const CHECKPOINT_INTERVAL: usize = 500;
const NQ_TICK_SIZE: f64 = 0.25;

#[derive(Debug, Error)]
pub enum DomReplayError {
    #[error("db error: {0}")]
    Db(String),
    #[error("depth error: {0}")]
    Depth(#[from] DepthError),
    #[error("scid error: {0}")]
    Scid(String),
    #[error("invalid replay window: {0}")]
    InvalidWindow(String),
    #[error("missing historical depth coverage for the requested window")]
    MissingDepthCoverage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DomReplayEventKind {
    Snapshot,
    Trade,
    Depth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeProfileLevel {
    pub price: f64,
    pub buy_vol: f64,
    pub sell_vol: f64,
    pub total_vol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullStackDelta {
    pub side: String,
    pub price: f64,
    pub stacked_quantity: f64,
    pub removed_quantity: f64,
    pub estimated_filled_quantity: f64,
    pub estimated_pulled_quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TapePrint {
    pub timestamp_ms: f64,
    pub price: f64,
    pub volume: f64,
    pub side: String,
    pub bid: f64,
    pub ask: f64,
    pub crosses_spread: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomReplayFrame {
    pub timestamp_ms: f64,
    pub event_kind: DomReplayEventKind,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub bids: Vec<DomLevel>,
    pub asks: Vec<DomLevel>,
    pub last_trade: Option<TapePrint>,
    pub recent_tape: Vec<TapePrint>,
    pub volume_profile: Vec<VolumeProfileLevel>,
    pub pull_stack_deltas: Vec<PullStackDelta>,
    pub cursor: usize,
    pub total_events: usize,
    pub clip_start_ms: f64,
    pub clip_end_ms: f64,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomReplayLoadResult {
    pub tick_count: usize,
    pub depth_batch_count: usize,
    pub total_events: usize,
    pub start_ms: f64,
    pub end_ms: f64,
    pub source_summary: String,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomReplayStatus {
    pub is_loaded: bool,
    pub is_playing: bool,
    pub cursor: usize,
    pub total_events: usize,
    pub current_timestamp_ms: Option<f64>,
    pub start_ms: Option<f64>,
    pub end_ms: Option<f64>,
    pub speed: f64,
    pub warning: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DomReplayClip {
    pub start_ms: f64,
    pub end_ms: f64,
    pub levels_per_side: usize,
    pub events: Vec<ReplayEvent>,
    pub checkpoints: Vec<ReplayCheckpoint>,
    pub source_summary: String,
    pub warning: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReplayCheckpoint {
    pub cursor: usize,
    pub state: ReplayState,
}

#[derive(Debug, Clone)]
pub struct ReplayState {
    pub timestamp_ms: f64,
    pub book: DepthBook,
    pub profile: BTreeMap<i64, ProfileVolume>,
    pub recent_tape: VecDeque<TapePrint>,
    pub last_trade: Option<TapePrint>,
}

#[derive(Debug, Clone)]
pub enum ReplayEvent {
    Trade(TradeReplayEvent),
    Depth(DepthReplayBatch),
}

impl ReplayEvent {
    pub fn timestamp_ms(&self) -> f64 {
        match self {
            Self::Trade(event) => event.print.timestamp_ms,
            Self::Depth(event) => event.timestamp_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeReplayEvent {
    pub print: TapePrint,
}

#[derive(Debug, Clone)]
pub struct DepthReplayBatch {
    pub timestamp_ms: f64,
    pub records: Vec<DepthRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct ProfileVolume {
    pub buy_vol: f64,
    pub sell_vol: f64,
}

pub fn build_clip(
    db: &Database,
    config: &FeedConfig,
    start_ms: f64,
    end_ms: f64,
    levels_per_side: usize,
) -> Result<DomReplayClip, DomReplayError> {
    if !start_ms.is_finite() || !end_ms.is_finite() || end_ms <= start_ms {
        return Err(DomReplayError::InvalidWindow(
            "start/end must be finite and end must be greater than start".to_string(),
        ));
    }

    let levels_per_side = levels_per_side.clamp(6, 20);
    let session_start_ms = session_start_ms(start_ms)?;
    let seed_ticks = load_ticks(db, config, session_start_ms, start_ms)?;
    let clip_ticks = load_ticks(db, config, start_ms, end_ms)?;
    let depth_source = load_depth_source(db, config, start_ms, end_ms)?;
    let mut seed_book = depth_source.seed_book;
    let seed_profile = build_seed_profile(seed_ticks);
    let mut source_summary = format!(
        "ticks: {}; depth: {}",
        clip_ticks.source, depth_source.source
    );
    if clip_ticks.source != depth_source.source {
        source_summary = format!("{source_summary} (hybrid)");
    }

    let warning = depth_source.warning.clone();
    let events = merge_events(clip_ticks.ticks, depth_source.batches);
    if events.is_empty() {
        return Err(DomReplayError::InvalidWindow(
            "no historical events found in the requested window".to_string(),
        ));
    }

    let mut checkpoints = Vec::new();
    let mut state = ReplayState {
        timestamp_ms: start_ms,
        book: std::mem::take(&mut seed_book),
        profile: seed_profile,
        recent_tape: VecDeque::with_capacity(MAX_TAPE_PRINTS),
        last_trade: None,
    };
    checkpoints.push(ReplayCheckpoint {
        cursor: 0,
        state: state.clone(),
    });

    for (index, event) in events.iter().enumerate() {
        apply_event(&mut state, event);
        let next_cursor = index + 1;
        if next_cursor % CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(ReplayCheckpoint {
                cursor: next_cursor,
                state: state.clone(),
            });
        }
    }
    if checkpoints
        .last()
        .map(|checkpoint| checkpoint.cursor != events.len())
        .unwrap_or(true)
    {
        checkpoints.push(ReplayCheckpoint {
            cursor: events.len(),
            state,
        });
    }

    Ok(DomReplayClip {
        start_ms,
        end_ms,
        levels_per_side,
        events,
        checkpoints,
        source_summary,
        warning,
    })
}

pub fn state_at_cursor(clip: &DomReplayClip, cursor: usize) -> ReplayState {
    let cursor = cursor.min(clip.events.len());
    let checkpoint = clip
        .checkpoints
        .iter()
        .rev()
        .find(|checkpoint| checkpoint.cursor <= cursor)
        .cloned()
        .unwrap_or_else(|| clip.checkpoints[0].clone());
    let mut state = checkpoint.state;
    for event in clip
        .events
        .iter()
        .skip(checkpoint.cursor)
        .take(cursor - checkpoint.cursor)
    {
        apply_event(&mut state, event);
    }
    state
}

pub fn frame_from_state(
    clip: &DomReplayClip,
    state: &ReplayState,
    event_kind: DomReplayEventKind,
    pull_stack_deltas: Vec<PullStackDelta>,
    cursor: usize,
) -> DomReplayFrame {
    let snapshot = state
        .book
        .snapshot("dom-replay", state.timestamp_ms, clip.levels_per_side);
    DomReplayFrame {
        timestamp_ms: state.timestamp_ms,
        event_kind,
        best_bid: snapshot.best_bid,
        best_ask: snapshot.best_ask,
        bids: snapshot.bids,
        asks: snapshot.asks,
        last_trade: state.last_trade.clone(),
        recent_tape: state.recent_tape.iter().cloned().collect(),
        volume_profile: profile_levels(&state.profile),
        pull_stack_deltas,
        cursor,
        total_events: clip.events.len(),
        clip_start_ms: clip.start_ms,
        clip_end_ms: clip.end_ms,
        warning: clip.warning.clone(),
    }
}

pub fn apply_event_and_frame(
    clip: &DomReplayClip,
    state: &mut ReplayState,
    cursor: usize,
) -> Option<DomReplayFrame> {
    let event = clip.events.get(cursor)?;
    let pull_stack_deltas = match event {
        ReplayEvent::Trade(_) => {
            apply_event(state, event);
            Vec::new()
        }
        ReplayEvent::Depth(batch) => apply_depth_batch(state, batch),
    };
    Some(frame_from_state(
        clip,
        state,
        match event {
            ReplayEvent::Trade(_) => DomReplayEventKind::Trade,
            ReplayEvent::Depth(_) => DomReplayEventKind::Depth,
        },
        pull_stack_deltas,
        cursor + 1,
    ))
}

pub fn seek_cursor_for_timestamp(clip: &DomReplayClip, timestamp_ms: f64) -> usize {
    match clip.events.binary_search_by(|event| {
        event
            .timestamp_ms()
            .partial_cmp(&timestamp_ms)
            .unwrap_or(Ordering::Equal)
    }) {
        Ok(index) => index,
        Err(index) => index.min(clip.events.len()),
    }
}

fn load_ticks(
    db: &Database,
    config: &FeedConfig,
    start_ms: f64,
    end_ms: f64,
) -> Result<LoadedTicks, DomReplayError> {
    let ticks = db
        .list_ticks_in_range(start_ms, end_ms)
        .map_err(|err| DomReplayError::Db(err.to_string()))?;
    if !ticks.is_empty() {
        return Ok(LoadedTicks {
            source: "sqlite".to_string(),
            ticks: ticks.into_iter().map(raw_tick_to_print).collect(),
        });
    }

    let reader = ScidReader::from_feed_config(config);
    let mut out = Vec::new();
    reader
        .scan_range(Some(start_ms), Some(end_ms), |tick| {
            out.push(scid_tick_to_print(tick));
            Ok(crate::feed::scid_reader::ScanControl::Continue)
        })
        .map_err(|err| DomReplayError::Scid(err.to_string()))?;
    Ok(LoadedTicks {
        source: "scid".to_string(),
        ticks: out,
    })
}

fn load_depth_source(
    db: &Database,
    config: &FeedConfig,
    start_ms: f64,
    end_ms: f64,
) -> Result<LoadedDepth, DomReplayError> {
    let clip_rows = db
        .list_depth_events_in_range(start_ms, end_ms, None)
        .map_err(|err| DomReplayError::Db(err.to_string()))?;

    if !clip_rows.is_empty() {
        let mut sources: Vec<String> = clip_rows
            .iter()
            .map(|row| row.source_file.clone())
            .collect();
        sources.sort();
        sources.dedup();
        if sources.len() == 1 {
            let source_file = sources[0].clone();
            if let Some(clear_ts) = db
                .latest_depth_clear_before(&source_file, start_ms)
                .map_err(|err| DomReplayError::Db(err.to_string()))?
            {
                let seed_rows = db
                    .list_depth_events_in_range(clear_ts, start_ms, Some(&source_file))
                    .map_err(|err| DomReplayError::Db(err.to_string()))?;
                let mut book = DepthBook::default();
                for row in seed_rows {
                    if let Some(record) = depth_row_to_record(&row) {
                        book.apply(&record);
                    }
                }
                return Ok(LoadedDepth {
                    source: "sqlite".to_string(),
                    seed_book: book,
                    batches: group_depth_batches(clip_rows),
                    warning: None,
                });
            }
        }
    }

    let Some(path) = DepthReader::find_file_for_timestamp(config, start_ms)? else {
        return Err(DomReplayError::MissingDepthCoverage);
    };
    let reader = DepthReader::new(path, config.price_scale);
    let (_, seed_book) = reader.book_at(start_ms)?;
    let mut rows = Vec::new();
    reader.scan_range(Some(start_ms), Some(end_ms), |record| {
        rows.push(record);
        Ok(crate::depth::ScanControl::Continue)
    })?;
    let warning = if clip_rows.is_empty() {
        None
    } else {
        Some("Requested range used file-backed depth replay due to incomplete SQLite depth coverage.".to_string())
    };
    Ok(LoadedDepth {
        source: "depth-file".to_string(),
        seed_book,
        batches: group_depth_records(rows),
        warning,
    })
}

fn merge_events(ticks: Vec<TapePrint>, depth_batches: Vec<DepthReplayBatch>) -> Vec<ReplayEvent> {
    let mut out = Vec::with_capacity(ticks.len() + depth_batches.len());
    out.extend(
        ticks
            .into_iter()
            .map(|print| ReplayEvent::Trade(TradeReplayEvent { print })),
    );
    out.extend(depth_batches.into_iter().map(ReplayEvent::Depth));
    out.sort_by(|left, right| {
        left.timestamp_ms()
            .partial_cmp(&right.timestamp_ms())
            .unwrap_or(Ordering::Equal)
            .then_with(|| match (left, right) {
                (ReplayEvent::Trade(_), ReplayEvent::Depth(_)) => Ordering::Less,
                (ReplayEvent::Depth(_), ReplayEvent::Trade(_)) => Ordering::Greater,
                _ => Ordering::Equal,
            })
    });
    out
}

fn apply_event(state: &mut ReplayState, event: &ReplayEvent) {
    match event {
        ReplayEvent::Trade(event) => {
            state.timestamp_ms = event.print.timestamp_ms;
            let key = price_to_tick_key(event.print.price);
            let profile = state.profile.entry(key).or_default();
            match event.print.side.as_str() {
                "buy" => profile.buy_vol += event.print.volume,
                "sell" => profile.sell_vol += event.print.volume,
                _ => {}
            }
            state.last_trade = Some(event.print.clone());
            state.recent_tape.push_back(event.print.clone());
            while state.recent_tape.len() > MAX_TAPE_PRINTS {
                state.recent_tape.pop_front();
            }
        }
        ReplayEvent::Depth(batch) => {
            let _ = apply_depth_batch(state, batch);
        }
    }
}

fn apply_depth_batch(state: &mut ReplayState, batch: &DepthReplayBatch) -> Vec<PullStackDelta> {
    let mut grouped: HashMap<(DepthSide, i64), PullStackDelta> = HashMap::new();
    state.timestamp_ms = batch.timestamp_ms;

    for record in &batch.records {
        let Some(side) = record.side else {
            state.book.apply(record);
            continue;
        };
        let key = price_to_tick_key(record.price);
        let (old_quantity, _) = state
            .book
            .level_quantity_and_orders(side, key)
            .unwrap_or_default();
        let delta = grouped
            .entry((side, key))
            .or_insert_with(|| PullStackDelta {
                side: match side {
                    DepthSide::Bid => "bid".to_string(),
                    DepthSide::Ask => "ask".to_string(),
                },
                price: record.price,
                stacked_quantity: 0.0,
                removed_quantity: 0.0,
                estimated_filled_quantity: 0.0,
                estimated_pulled_quantity: 0.0,
            });

        match record.command {
            DepthCommand::AddBidLevel | DepthCommand::AddAskLevel => {
                delta.stacked_quantity += record.quantity as f64;
            }
            DepthCommand::ModifyBidLevel | DepthCommand::ModifyAskLevel => {
                if record.quantity >= old_quantity {
                    delta.stacked_quantity += (record.quantity - old_quantity) as f64;
                } else {
                    delta.removed_quantity += (old_quantity - record.quantity) as f64;
                    delta.estimated_pulled_quantity += (old_quantity - record.quantity) as f64;
                }
            }
            DepthCommand::DeleteBidLevel | DepthCommand::DeleteAskLevel => {
                delta.removed_quantity += old_quantity as f64;
                delta.estimated_pulled_quantity += old_quantity as f64;
            }
            DepthCommand::ClearBook | DepthCommand::NoCommand => {}
        }

        state.book.apply(record);
    }

    let mut out: Vec<_> = grouped.into_values().collect();
    out.sort_by(|left, right| {
        right
            .removed_quantity
            .partial_cmp(&left.removed_quantity)
            .unwrap_or(Ordering::Equal)
    });
    out
}

fn build_seed_profile(ticks: LoadedTicks) -> BTreeMap<i64, ProfileVolume> {
    let mut out: BTreeMap<i64, ProfileVolume> = BTreeMap::new();
    for print in ticks.ticks {
        let key = price_to_tick_key(print.price);
        let entry = out.entry(key).or_default();
        match print.side.as_str() {
            "buy" => entry.buy_vol += print.volume,
            "sell" => entry.sell_vol += print.volume,
            _ => {}
        }
    }
    out
}

fn profile_levels(profile: &BTreeMap<i64, ProfileVolume>) -> Vec<VolumeProfileLevel> {
    profile
        .iter()
        .rev()
        .map(|(key, level)| VolumeProfileLevel {
            price: tick_key_to_price(*key),
            buy_vol: level.buy_vol,
            sell_vol: level.sell_vol,
            total_vol: level.buy_vol + level.sell_vol,
        })
        .collect()
}

fn group_depth_batches(rows: Vec<DepthEventRecord>) -> Vec<DepthReplayBatch> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut current_ts = None;
    for row in rows {
        let Some(record) = depth_row_to_record(&row) else {
            continue;
        };
        current_ts = Some(record.timestamp_ms);
        current.push(record);
        if row.end_of_batch {
            out.push(DepthReplayBatch {
                timestamp_ms: current_ts.unwrap_or_default(),
                records: std::mem::take(&mut current),
            });
        }
    }
    if !current.is_empty() {
        out.push(DepthReplayBatch {
            timestamp_ms: current_ts.unwrap_or_default(),
            records: current,
        });
    }
    out
}

fn group_depth_records(records: Vec<DepthRecord>) -> Vec<DepthReplayBatch> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut current_ts = None;
    for record in records {
        current_ts = Some(record.timestamp_ms);
        let end_of_batch = record.end_of_batch;
        current.push(record);
        if end_of_batch {
            out.push(DepthReplayBatch {
                timestamp_ms: current_ts.unwrap_or_default(),
                records: std::mem::take(&mut current),
            });
        }
    }
    if !current.is_empty() {
        out.push(DepthReplayBatch {
            timestamp_ms: current_ts.unwrap_or_default(),
            records: current,
        });
    }
    out
}

fn depth_row_to_record(row: &DepthEventRecord) -> Option<DepthRecord> {
    let command = match row.command.as_str() {
        "ClearBook" => DepthCommand::ClearBook,
        "AddBidLevel" => DepthCommand::AddBidLevel,
        "AddAskLevel" => DepthCommand::AddAskLevel,
        "ModifyBidLevel" => DepthCommand::ModifyBidLevel,
        "ModifyAskLevel" => DepthCommand::ModifyAskLevel,
        "DeleteBidLevel" => DepthCommand::DeleteBidLevel,
        "DeleteAskLevel" => DepthCommand::DeleteAskLevel,
        _ => DepthCommand::NoCommand,
    };
    let side = row.side.as_deref().and_then(|side| match side {
        "bid" => Some(DepthSide::Bid),
        "ask" => Some(DepthSide::Ask),
        _ => None,
    });
    Some(DepthRecord {
        timestamp_ms: row.timestamp_ms,
        command,
        side,
        end_of_batch: row.end_of_batch,
        num_orders: row.num_orders as u16,
        price: row.price,
        quantity: row.quantity.max(0.0) as u32,
    })
}

fn raw_tick_to_print(row: RawTickRecord) -> TapePrint {
    let side = if row.is_buy { "buy" } else { "sell" }.to_string();
    TapePrint {
        timestamp_ms: row.timestamp_ms,
        price: row.price,
        volume: row.volume,
        bid: row.bid,
        ask: row.ask,
        crosses_spread: crosses_spread(&side, row.price, row.bid, row.ask),
        side,
    }
}

fn scid_tick_to_print(row: ScidTick) -> TapePrint {
    let side = match row.side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
        TradeSide::Unknown => "unknown",
    }
    .to_string();
    TapePrint {
        timestamp_ms: row.timestamp_ms,
        price: row.price,
        volume: row.volume,
        bid: row.bid,
        ask: row.ask,
        crosses_spread: crosses_spread(&side, row.price, row.bid, row.ask),
        side,
    }
}

fn crosses_spread(side: &str, price: f64, bid: f64, ask: f64) -> bool {
    match side {
        "buy" => ask > 0.0 && price >= ask,
        "sell" => bid > 0.0 && price <= bid,
        _ => false,
    }
}

fn session_start_ms(timestamp_ms: f64) -> Result<f64, DomReplayError> {
    let trading_day = trading_day_from_timestamp_ms(timestamp_ms);
    let date = NaiveDate::parse_from_str(&trading_day, "%Y-%m-%d")
        .map_err(|err| DomReplayError::InvalidWindow(err.to_string()))?;
    let session_date = date.pred_opt().unwrap_or(date);
    let naive = session_date.and_hms_opt(18, 0, 0).ok_or_else(|| {
        DomReplayError::InvalidWindow("failed to build session start".to_string())
    })?;
    let dt = Eastern
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| DomReplayError::InvalidWindow("ambiguous session start".to_string()))?;
    Ok(dt.timestamp_millis() as f64)
}

fn price_to_tick_key(price: f64) -> i64 {
    (price / NQ_TICK_SIZE).round() as i64
}

struct LoadedTicks {
    source: String,
    ticks: Vec<TapePrint>,
}

struct LoadedDepth {
    source: String,
    seed_book: DepthBook,
    batches: Vec<DepthReplayBatch>,
    warning: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::depth::{DepthCommand, DepthSide};

    fn trade_event(ts: f64) -> ReplayEvent {
        ReplayEvent::Trade(TradeReplayEvent {
            print: TapePrint {
                timestamp_ms: ts,
                price: 100.0,
                volume: 1.0,
                side: "buy".to_string(),
                bid: 99.75,
                ask: 100.0,
                crosses_spread: true,
            },
        })
    }

    fn depth_event(ts: f64) -> ReplayEvent {
        ReplayEvent::Depth(DepthReplayBatch {
            timestamp_ms: ts,
            records: vec![DepthRecord {
                timestamp_ms: ts,
                command: DepthCommand::AddBidLevel,
                side: Some(DepthSide::Bid),
                end_of_batch: true,
                num_orders: 1,
                price: 99.75,
                quantity: 5,
            }],
        })
    }

    #[test]
    fn merge_orders_events_chronologically() {
        let merged = merge_events(
            vec![TapePrint {
                timestamp_ms: 2.0,
                price: 100.0,
                volume: 1.0,
                side: "buy".to_string(),
                bid: 99.75,
                ask: 100.0,
                crosses_spread: true,
            }],
            vec![DepthReplayBatch {
                timestamp_ms: 1.0,
                records: vec![],
            }],
        );
        assert!(matches!(merged[0], ReplayEvent::Depth(_)));
        assert!(matches!(merged[1], ReplayEvent::Trade(_)));
    }

    #[test]
    fn seek_uses_first_event_at_or_after_timestamp() {
        let clip = DomReplayClip {
            start_ms: 0.0,
            end_ms: 10.0,
            levels_per_side: 10,
            events: vec![depth_event(1.0), trade_event(2.0), depth_event(5.0)],
            checkpoints: vec![ReplayCheckpoint {
                cursor: 0,
                state: ReplayState {
                    timestamp_ms: 0.0,
                    book: DepthBook::default(),
                    profile: BTreeMap::new(),
                    recent_tape: VecDeque::new(),
                    last_trade: None,
                },
            }],
            source_summary: "test".to_string(),
            warning: None,
        };
        assert_eq!(seek_cursor_for_timestamp(&clip, 0.5), 0);
        assert_eq!(seek_cursor_for_timestamp(&clip, 1.0), 0);
        assert_eq!(seek_cursor_for_timestamp(&clip, 1.5), 1);
        assert_eq!(seek_cursor_for_timestamp(&clip, 9.0), 3);
    }

    #[test]
    fn apply_depth_batch_updates_book() {
        let mut state = ReplayState {
            timestamp_ms: 0.0,
            book: DepthBook::default(),
            profile: BTreeMap::new(),
            recent_tape: VecDeque::new(),
            last_trade: None,
        };
        let deltas = apply_depth_batch(
            &mut state,
            &DepthReplayBatch {
                timestamp_ms: 10.0,
                records: vec![DepthRecord {
                    timestamp_ms: 10.0,
                    command: DepthCommand::AddAskLevel,
                    side: Some(DepthSide::Ask),
                    end_of_batch: true,
                    num_orders: 1,
                    price: 100.25,
                    quantity: 7,
                }],
            },
        );
        let snapshot = state.book.snapshot("test", 10.0, 5);
        assert_eq!(snapshot.best_ask, Some(100.25));
        assert_eq!(deltas[0].stacked_quantity, 7.0);
    }
}
