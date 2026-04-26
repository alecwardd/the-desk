//! Persist `.scid` trades into `raw_ticks` for periods missing from the database.
//!
//! Research [`crate::backfill::run_backfill_job`] replays history through pipelines but does **not**
//! insert raw ticks; the live MCP tail does. Use this module so `query_ticks` and snapshots cover
//! historical windows.

use crate::backfill::{parse_backfill_date_range, BackfillJobError};
use crate::db::{Database, RawTickBatchRow};
use crate::feed::monotonic::{
    MonotonicTickGuard, MonotonicTimestampDecision, MonotonicTimestampStats,
};
use crate::feed::scid_reader::{ScanControl, ScidError, ScidReader};
use crate::feed::{ContractMetadata, TradeSide};
use crate::session_date_from_timestamp_ms;
use serde::Serialize;
use thiserror::Error;

const TICK_BATCH: usize = 500;

#[derive(Debug, Error)]
pub enum TickIngestError {
    #[error("scid: {0}")]
    Scid(#[from] ScidError),
    #[error("database: {0}")]
    Db(#[from] crate::db::DbError),
    #[error("invalid date range: {0}")]
    DateRange(String),
}

impl From<BackfillJobError> for TickIngestError {
    fn from(e: BackfillJobError) -> Self {
        TickIngestError::DateRange(e.to_string())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TickIngestGap {
    pub label: &'static str,
    pub from_ms: f64,
    /// Exclusive upper bound for [`ScidReader::scan_range`].
    pub to_ms_exclusive: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_scid_records: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct TickIngestGapReport {
    pub scid_path: String,
    pub contract_symbol: String,
    pub root_symbol: String,
    pub scid_first_timestamp_ms: Option<f64>,
    pub scid_last_timestamp_ms: Option<f64>,
    pub db_min_timestamp_ms: Option<f64>,
    pub db_max_timestamp_ms: Option<f64>,
    pub db_tick_count_for_contract: i64,
    pub session_summary_count: i64,
    pub session_summary_date_min: Option<String>,
    pub session_summary_date_max: Option<String>,
    pub clip_start_ms: Option<f64>,
    pub clip_end_ms_exclusive: Option<f64>,
    /// `global` = min/max over all `raw_ticks` for the contract; `date_clip` = min/max only inside the requested window intersected with the SCID span.
    pub db_bounds_scope: &'static str,
    pub gaps: Vec<TickIngestGap>,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct TickIngestResult {
    pub scid_records_scanned: usize,
    pub ticks_submitted_to_insert: usize,
    pub gaps_processed: usize,
    pub gap_labels: Vec<String>,
    pub scid_timestamp_monotonicity: MonotonicTimestampStats,
}

#[derive(Debug, Clone, Copy)]
pub struct TickIngestParams<'a> {
    pub start_date: Option<&'a str>,
    pub end_date: Option<&'a str>,
    /// When true, only prefix/suffix gaps vs existing `raw_ticks` for the contract.
    pub only_gaps: bool,
}

fn clip_bounds(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<(f64, f64), TickIngestError> {
    let (start_ms, end_ms) = parse_backfill_date_range(start_date, end_date)?;
    let lo = start_ms.unwrap_or(f64::NEG_INFINITY);
    let hi_excl = end_ms.unwrap_or(f64::INFINITY);
    Ok((lo, hi_excl))
}

/// Compare SCID file coverage to `raw_ticks` for the active contract and build missing ranges.
pub fn analyze_tick_ingest_gaps(
    reader: &ScidReader,
    db: &Database,
    contract: &ContractMetadata,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<TickIngestGapReport, TickIngestError> {
    let date_clipped = start_date.is_some() || end_date.is_some();
    let (clip_lo, clip_hi_excl) = clip_bounds(start_date, end_date)?;
    let (sf, sl) = reader.file_timestamp_bounds()?;
    let scid_path = reader.path().to_string_lossy().to_string();
    let (smin, smax) = db.session_summary_date_range().unwrap_or((None, None));
    let scount = db.session_summary_count().unwrap_or(0);

    let note: &'static str = "Gaps are prefix/suffix vs SCID bounds for this contract in raw_ticks. \
         INSERT OR IGNORE skips duplicates. Mid-session holes inside existing DB coverage are not detected.";

    let mut report = TickIngestGapReport {
        scid_path,
        contract_symbol: contract.contract_symbol.clone(),
        root_symbol: contract.root_symbol.clone(),
        scid_first_timestamp_ms: sf,
        scid_last_timestamp_ms: sl,
        db_min_timestamp_ms: None,
        db_max_timestamp_ms: None,
        db_tick_count_for_contract: 0,
        session_summary_count: scount,
        session_summary_date_min: smin,
        session_summary_date_max: smax,
        clip_start_ms: clip_lo.is_finite().then_some(clip_lo),
        clip_end_ms_exclusive: clip_hi_excl.is_finite().then_some(clip_hi_excl),
        db_bounds_scope: if date_clipped { "date_clip" } else { "global" },
        gaps: Vec::new(),
        note,
    };

    let (Some(sf), Some(sl)) = (sf, sl) else {
        report.note = "SCID file has no records after the header.";
        return Ok(report);
    };

    // Overlap of SCID file with optional calendar clip (end exclusive).
    let span_lo = sf.max(clip_lo);
    let span_end_excl = (sl + 1.0).min(clip_hi_excl);
    if span_lo >= span_end_excl {
        report.note =
            "Date clip does not overlap the SCID file timestamp span — no tick gaps in range.";
        return Ok(report);
    }

    let (db_min, db_max, db_count) = if date_clipped {
        db.raw_ticks_time_bounds_for_contract_in_range(
            contract.contract_symbol.as_str(),
            span_lo,
            span_end_excl,
        )?
    } else {
        db.raw_ticks_time_bounds_for_contract(contract.contract_symbol.as_str())?
    };
    report.db_min_timestamp_ms = db_min;
    report.db_max_timestamp_ms = db_max;
    report.db_tick_count_for_contract = db_count;

    let mut gaps = Vec::new();

    if db_count == 0 {
        let est = reader
            .estimate_range_records(Some(span_lo), Some(span_end_excl))
            .unwrap_or(0);
        gaps.push(TickIngestGap {
            label: "no_ticks_for_contract",
            from_ms: span_lo,
            to_ms_exclusive: span_end_excl,
            estimated_scid_records: Some(est),
        });
        report.gaps = gaps;
        return Ok(report);
    }

    let db_min = db_min.unwrap_or(sf);
    let db_max = db_max.unwrap_or(sl);

    // DB entirely before the span: whole span missing.
    if db_max < span_lo {
        let est = reader
            .estimate_range_records(Some(span_lo), Some(span_end_excl))
            .unwrap_or(0);
        gaps.push(TickIngestGap {
            label: "prefix",
            from_ms: span_lo,
            to_ms_exclusive: span_end_excl,
            estimated_scid_records: Some(est),
        });
        report.gaps = gaps;
        return Ok(report);
    }

    // DB entirely after the span: whole span missing.
    if db_min >= span_end_excl {
        let est = reader
            .estimate_range_records(Some(span_lo), Some(span_end_excl))
            .unwrap_or(0);
        gaps.push(TickIngestGap {
            label: "suffix",
            from_ms: span_lo,
            to_ms_exclusive: span_end_excl,
            estimated_scid_records: Some(est),
        });
        report.gaps = gaps;
        return Ok(report);
    }

    // Prefix: from span start up to first DB tick (exclusive).
    if span_lo < db_min {
        let to = db_min.min(span_end_excl);
        if span_lo < to {
            let est = reader
                .estimate_range_records(Some(span_lo), Some(to))
                .unwrap_or(0);
            if est > 0 {
                gaps.push(TickIngestGap {
                    label: "prefix",
                    from_ms: span_lo,
                    to_ms_exclusive: to,
                    estimated_scid_records: Some(est),
                });
            }
        }
    }

    // Suffix: more SCID data after the last DB tick in scope (clip vs whole file).
    let suffix_open = if date_clipped {
        db_max + 1e-3 < span_end_excl - 1e-3
    } else {
        db_max + 1.0 < sl && db_max + 1e-3 < span_end_excl
    };
    if suffix_open {
        let from = db_max.max(span_lo);
        if from + 1e-3 < span_end_excl {
            let est = reader
                .estimate_range_records(Some(from), Some(span_end_excl))
                .unwrap_or(0);
            if est > 0 {
                gaps.push(TickIngestGap {
                    label: "suffix",
                    from_ms: from,
                    to_ms_exclusive: span_end_excl,
                    estimated_scid_records: Some(est),
                });
            }
        }
    }

    report.gaps = gaps;
    Ok(report)
}

/// Full clip range intersected with SCID file (single ingest window, not gap-derived).
pub fn analyze_full_clip_range(
    reader: &ScidReader,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<Option<TickIngestGap>, TickIngestError> {
    let (clip_lo, clip_hi_excl) = clip_bounds(start_date, end_date)?;
    let (sf, sl) = reader.file_timestamp_bounds()?;
    let (Some(sf), Some(sl)) = (sf, sl) else {
        return Ok(None);
    };
    let span_lo = sf.max(clip_lo);
    let span_end_excl = (sl + 1.0).min(clip_hi_excl);
    if span_lo >= span_end_excl {
        return Ok(None);
    }
    let est = reader
        .estimate_range_records(Some(span_lo), Some(span_end_excl))
        .unwrap_or(0);
    Ok(Some(TickIngestGap {
        label: "full_clip",
        from_ms: span_lo,
        to_ms_exclusive: span_end_excl,
        estimated_scid_records: Some(est),
    }))
}

/// Scan SCID ranges and batch-insert into `raw_ticks` (`INSERT OR IGNORE`).
pub fn ingest_scid_tick_gaps(
    reader: &ScidReader,
    db: &Database,
    contract: &ContractMetadata,
    gaps: &[TickIngestGap],
) -> Result<TickIngestResult, TickIngestError> {
    let root = contract.root_symbol.clone();
    let cs = contract.contract_symbol.clone();
    let mut buf: Vec<RawTickBatchRow> = Vec::with_capacity(TICK_BATCH);
    let mut scanned = 0_usize;
    let mut submitted = 0_usize;
    let mut labels: Vec<String> = Vec::new();
    let mut monotonic_guard = MonotonicTickGuard::default();

    for gap in gaps {
        labels.push(gap.label.to_string());
        reader.scan_range_in_file_order(Some(gap.from_ms), Some(gap.to_ms_exclusive), |tick| {
            scanned += 1;
            if !matches!(
                monotonic_guard.observe(tick.timestamp_ms),
                MonotonicTimestampDecision::Accept
            ) {
                return Ok(ScanControl::Continue);
            }
            let is_buy = matches!(tick.side, TradeSide::Buy);
            let bid = if tick.bid > 0.0 {
                tick.bid
            } else {
                tick.price - 0.25
            };
            let ask = if tick.ask > 0.0 {
                tick.ask
            } else {
                tick.price + 0.25
            };
            let session_date = session_date_from_timestamp_ms(tick.timestamp_ms);
            buf.push((
                tick.timestamp_ms,
                tick.price,
                tick.volume,
                bid,
                ask,
                is_buy,
                session_date,
                root.clone(),
                cs.clone(),
            ));
            if buf.len() >= TICK_BATCH {
                db.insert_raw_ticks_batch(&buf).map_err(|e| e.to_string())?;
                submitted += buf.len();
                buf.clear();
            }
            Ok(ScanControl::Continue)
        })?;
    }

    if !buf.is_empty() {
        db.insert_raw_ticks_batch(&buf)?;
        submitted += buf.len();
    }

    Ok(TickIngestResult {
        scid_records_scanned: scanned,
        ticks_submitted_to_insert: submitted,
        gaps_processed: gaps.len(),
        gap_labels: labels,
        scid_timestamp_monotonicity: monotonic_guard.into_stats(),
    })
}

/// Analyze gaps, then ingest either those gaps or the full clip window (see [`TickIngestParams::only_gaps`]).
pub fn run_tick_ingest(
    reader: &ScidReader,
    db: &Database,
    contract: &ContractMetadata,
    params: TickIngestParams<'_>,
) -> Result<(TickIngestGapReport, Option<TickIngestResult>), TickIngestError> {
    let report =
        analyze_tick_ingest_gaps(reader, db, contract, params.start_date, params.end_date)?;

    let result = if params.only_gaps {
        if report.gaps.is_empty() {
            None
        } else {
            Some(ingest_scid_tick_gaps(reader, db, contract, &report.gaps)?)
        }
    } else {
        match analyze_full_clip_range(reader, params.start_date, params.end_date)? {
            Some(g) => Some(ingest_scid_tick_gaps(reader, db, contract, &[g])?),
            None => None,
        }
    };

    Ok((report, result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SCID_HEADER_SIZE_TEST: usize = 56;
    const SCID_MAGIC_TEST: &[u8; 4] = b"SCID";
    const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;

    fn write_scid_header(file: &mut NamedTempFile) {
        let mut header = vec![0_u8; SCID_HEADER_SIZE_TEST];
        header[0..4].copy_from_slice(SCID_MAGIC_TEST);
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE_TEST as u32).to_le_bytes());
        header[8..12]
            .copy_from_slice(&(crate::feed::scid_reader::SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).expect("header");
    }

    fn write_record(file: &mut NamedTempFile, timestamp_ms: f64, price: f32) {
        let mut rec = [0_u8; crate::feed::scid_reader::SCID_RECORD_SIZE];
        let unix_us = (timestamp_ms * 1_000.0).round() as i64;
        let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
        rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
        rec[12..16].copy_from_slice(&(price + 0.25).to_le_bytes());
        rec[16..20].copy_from_slice(&(price - 0.25).to_le_bytes());
        rec[20..24].copy_from_slice(&price.to_le_bytes());
        rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
        rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
        rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
        rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
        file.write_all(&rec).expect("record");
    }

    #[test]
    fn ingest_scid_tick_gaps_reports_skipped_non_monotonic_ticks() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        let base = 1_700_000_000_000.0;
        write_record(&mut file, base, 21000.0);
        write_record(&mut file, base, 21000.25);
        write_record(&mut file, base - 1.0, 21000.5);
        write_record(&mut file, base + 2.0, 21000.75);
        file.flush().expect("flush");

        let db = Database::open(":memory:").expect("db");
        let contract = ContractMetadata {
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            ..ContractMetadata::default()
        };
        let result = ingest_scid_tick_gaps(
            &ScidReader::new(file.path()),
            &db,
            &contract,
            &[TickIngestGap {
                label: "full_clip",
                from_ms: base - 10.0,
                to_ms_exclusive: base + 10.0,
                estimated_scid_records: None,
            }],
        )
        .expect("ingest");

        assert_eq!(result.scid_records_scanned, 4);
        assert_eq!(result.ticks_submitted_to_insert, 2);
        assert_eq!(result.scid_timestamp_monotonicity.accepted_ticks, 2);
        assert_eq!(
            result
                .scid_timestamp_monotonicity
                .skipped_non_monotonic_ticks,
            2
        );
        assert_eq!(
            result.scid_timestamp_monotonicity.duplicate_timestamp_ticks,
            1
        );
        assert_eq!(
            result.scid_timestamp_monotonicity.backward_timestamp_ticks,
            1
        );
        assert_eq!(db.raw_tick_count().expect("count"), 2);
    }
}
