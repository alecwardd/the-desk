use crate::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampStats};
use crate::feed::scid_reader::{ScanControl, ScidError, ScidReader};
use serde::Serialize;

/// Historical anomaly scan result for SCID timestamp monotonicity.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScidTimestampAnomalyScan {
    pub scid_path: String,
    pub scan_start_ms: Option<f64>,
    pub scan_end_ms_exclusive: Option<f64>,
    pub records_scanned: usize,
    pub scid_first_timestamp_ms: Option<f64>,
    pub scid_last_timestamp_ms: Option<f64>,
    pub monotonicity: MonotonicTimestampStats,
}

/// Scan a SCID file in byte order and classify equal/backward timestamp anomalies.
pub fn scan_scid_timestamp_anomalies(
    reader: &ScidReader,
    start_ms: Option<f64>,
    end_ms_exclusive: Option<f64>,
    sample_capacity: usize,
) -> Result<ScidTimestampAnomalyScan, ScidError> {
    let (scid_first_timestamp_ms, scid_last_timestamp_ms) = reader.file_timestamp_bounds()?;
    let mut guard = MonotonicTickGuard::new(sample_capacity);
    let scan_stats = reader.scan_range_in_file_order(start_ms, end_ms_exclusive, |tick| {
        let _ = guard.observe(tick.timestamp_ms);
        Ok(ScanControl::Continue)
    })?;

    Ok(ScidTimestampAnomalyScan {
        scid_path: reader.path().to_string_lossy().to_string(),
        scan_start_ms: start_ms.filter(|v| v.is_finite()),
        scan_end_ms_exclusive: end_ms_exclusive.filter(|v| v.is_finite()),
        records_scanned: scan_stats.records_scanned,
        scid_first_timestamp_ms,
        scid_last_timestamp_ms,
        monotonicity: guard.into_stats(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::scid_reader::{ScidReader, SCID_RECORD_SIZE};
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SCID_HEADER_SIZE: usize = 56;
    const SCID_MAGIC: &[u8; 4] = b"SCID";
    const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;

    fn write_scid_header(file: &mut NamedTempFile) {
        let mut header = vec![0_u8; SCID_HEADER_SIZE];
        header[0..4].copy_from_slice(SCID_MAGIC);
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).expect("header");
    }

    fn write_record(file: &mut NamedTempFile, timestamp_ms: f64, price: f32) {
        let mut rec = [0_u8; SCID_RECORD_SIZE];
        let unix_us = (timestamp_ms * 1_000.0).round() as i64;
        let sc_us = SC_TO_UNIX_EPOCH_US + unix_us;
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
    fn anomaly_scan_reports_equal_and_backward_ticks() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        let base = 1_700_000_000_000.0;
        write_record(&mut file, base, 21000.0);
        write_record(&mut file, base, 21000.25);
        write_record(&mut file, base - 1.0, 21000.5);
        write_record(&mut file, base + 2.0, 21000.75);
        file.flush().expect("flush");

        let report = scan_scid_timestamp_anomalies(&ScidReader::new(file.path()), None, None, 8)
            .expect("scan");

        assert_eq!(report.records_scanned, 4);
        assert_eq!(report.monotonicity.accepted_ticks, 2);
        assert_eq!(report.monotonicity.skipped_non_monotonic_ticks, 2);
        assert_eq!(report.monotonicity.duplicate_timestamp_ticks, 1);
        assert_eq!(report.monotonicity.backward_timestamp_ticks, 1);
    }
}
