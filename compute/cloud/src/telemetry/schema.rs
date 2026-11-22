//! Spec §3 column lists and the strict NDJSON → Parquet conversion for operational logs.

use super::LogKind;
use crate::ingest::convert::{rows_to_parquet, Column, Kind};
use serde_json::Value;
use std::sync::OnceLock;

use Kind::{Bool, Float64, Int64, TimestampMs, Utf8};

const COMMON: &[(&str, Kind)] = &[
    ("timestamp", TimestampMs),
    ("event_id", Utf8),
    ("service_id", Utf8),
    ("severity", Utf8),
];

fn specific(kind: LogKind) -> &'static [(&'static str, Kind)] {
    match kind {
        LogKind::CronRuns => &[
            ("cron_kind", Utf8),
            ("run_id", Utf8),
            ("tick_event_id", Utf8),
            ("trigger", Utf8),
            ("actor", Utf8),
            ("bucket", Int64),
            ("outcome", Utf8),
            ("started_at", TimestampMs),
            ("duration_ms", Int64),
            ("vcpu_ms", Int64),
            ("listed_files", Int64),
            ("converted_files", Int64),
            ("deleted_files", Int64),
            ("lines_read", Int64),
            ("rows_written", Int64),
            ("bad_lines", Int64),
            ("bytes_read", Int64),
            ("bytes_written", Int64),
            ("parquet_files_written", Int64),
            ("backend_requests_used", Int64),
            ("cache_lookups", Int64),
            ("cache_replaces", Int64),
            ("kv_ops", Int64),
            ("purges", Int64),
            ("more_pending", Bool),
            ("details_json", Utf8),
            ("error", Utf8),
            ("idle_ticks", Int64),
            ("idle_vcpu_ms", Int64),
            ("worker_executions", Int64),
        ],
        LogKind::CronTicks => &[
            ("trigger", Utf8),
            ("actor", Utf8),
            ("bucket", Int64),
            ("outcome", Utf8),
            ("ticks_count", Int64),
            ("triggered_crons", Utf8),
            ("duration_ms", Int64),
            ("vcpu_ms", Int64),
            ("backend_requests_used", Int64),
            ("pop", Utf8),
            ("error", Utf8),
        ],
        LogKind::IngestFiles => &[
            ("run_id", Utf8),
            ("dataset", Utf8),
            ("raw_key", Utf8),
            ("raw_size", Int64),
            ("status", Utf8),
            ("state_version", Int64),
            ("rows", Int64),
            ("lines_read", Int64),
            ("bad_lines", Int64),
            ("output_keys_json", Utf8),
            ("raw_deleted", Bool),
            ("raw_deleted_at", TimestampMs),
            ("reason", Utf8),
        ],
        LogKind::FosUsage => &[
            ("op", Utf8),
            ("op_class", Utf8),
            ("op_count", Int64),
            ("dataset", Utf8),
            ("bucket", Utf8),
            ("sample_key", Utf8),
            ("caller", Utf8),
            ("status_code", Int64),
            ("duration_ms", Int64),
            ("bytes_in", Int64),
            ("bytes_out", Int64),
            ("bytes_deleted", Int64),
            ("cdn_cache", Utf8),
            ("est_cost_usd", Float64),
            ("harness", Utf8),
        ],
        LogKind::AdminAudit => &[
            ("domain", Utf8),
            ("actor", Utf8),
            ("action", Utf8),
            ("target", Utf8),
            ("job_id", Utf8),
            ("step_name", Utf8),
            ("outcome", Utf8),
            ("ip", Utf8),
            ("source", Utf8),
            ("source_seq", Int64),
            ("details_json", Utf8),
        ],
        LogKind::ApiTelemetry => &[
            ("source", Utf8),
            ("method", Utf8),
            ("path", Utf8),
            ("status_code", Int64),
            ("duration_ms", Int64),
            ("vcpu_ms", Int64),
            ("pop", Utf8),
            ("actor", Utf8),
            ("files_listed", Int64),
            ("files_fetched", Int64),
            ("files_cached", Int64),
            ("cdn_hits", Int64),
            ("rows_scanned", Int64),
            ("bytes_scanned", Int64),
            ("details_json", Utf8),
        ],
        LogKind::PlatformErrors => &[
            ("error_domain", Utf8),
            ("code", Utf8),
            ("caller", Utf8),
            ("status_code", Int64),
            ("object_key", Utf8),
            ("message", Utf8),
            ("sample_json", Utf8),
        ],
    }
}

pub fn columns(kind: LogKind) -> &'static [Column] {
    static ALL: OnceLock<Vec<Vec<Column>>> = OnceLock::new();
    &ALL.get_or_init(|| {
        LogKind::ALL
            .iter()
            .map(|kind| {
                COMMON
                    .iter()
                    .chain(specific(*kind))
                    .map(|&(id, kind)| Column { id, kind })
                    .collect()
            })
            .collect()
    })[LogKind::ALL.iter().position(|k| *k == kind).unwrap_or(0)]
}

/// Decodes a file written by `to_parquet` back into row objects; null cells are omitted.
pub fn from_parquet(kind: LogKind, bytes: Vec<u8>) -> Result<Vec<Value>, String> {
    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .and_then(|builder| builder.build())
        .map_err(|_| "parquet_unreadable".to_string())?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|_| "parquet_unreadable".to_string())?;
        for index in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for column in columns(kind) {
                let Some(array) = batch.column_by_name(column.id) else {
                    continue;
                };
                if array.is_null(index) {
                    continue;
                }
                let value = match column.kind {
                    Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|a| Value::from(a.value(index))),
                    Int64 => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| Value::from(a.value(index))),
                    TimestampMs => array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .map(|a| Value::from(a.value(index))),
                    Float64 => array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .map(|a| Value::from(a.value(index))),
                    Bool => array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|a| Value::from(a.value(index))),
                };
                if let Some(value) = value {
                    row.insert(column.id.to_string(), value);
                }
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}

pub fn to_parquet(kind: LogKind, rows: &[Value]) -> Result<Vec<u8>, String> {
    let objects: Vec<_> = rows
        .iter()
        .map(|row| row.as_object().cloned().unwrap_or_default())
        .collect();
    rows_to_parquet(columns(kind), &objects)
}
