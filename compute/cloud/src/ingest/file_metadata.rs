use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::{Deserialize, Serialize};

pub const METADATA_BYTES: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metadata {
    pub row_groups: Vec<RowGroup>,
    pub columns_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowGroup {
    pub rows: u64,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub offset: u64,
    pub length: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nulls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    pub physical_type: String,
}

pub fn decode_parquet_footer(bytes: &[u8]) -> Result<ParquetMetaData, String> {
    let tail = bytes
        .get(bytes.len().saturating_sub(8)..)
        .ok_or("parquet_footer_missing")?;
    if tail.len() != 8 || &tail[4..] != b"PAR1" {
        return Err("parquet_footer_invalid".into());
    }
    let length =
        u32::from_le_bytes(tail[..4].try_into().map_err(|_| "parquet_footer_invalid")?) as usize;
    let end = bytes.len() - 8;
    let start = end.checked_sub(length).ok_or("parquet_footer_invalid")?;
    ParquetMetaDataReader::decode_metadata(&bytes[start..end]).map_err(|e| e.to_string())
}

pub fn from_parquet(bytes: &[u8]) -> Result<Option<Metadata>, String> {
    let metadata = decode_parquet_footer(bytes)?;
    Ok(from_metadata(&metadata))
}

pub fn from_metadata(metadata: &ParquetMetaData) -> Option<Metadata> {
    let mut groups = Vec::new();
    let columns_complete = metadata.file_metadata().schema_descr().num_columns() <= 12;
    for group in metadata.row_groups() {
        let columns = group
            .columns()
            .iter()
            .filter(|column| {
                columns_complete
                    || matches!(
                        column.column_path().string().as_str(),
                        "ts_ms"
                            | "time"
                            | "status"
                            | "host"
                            | "url"
                            | "path"
                            | "method"
                            | "bytes"
                            | "pop"
                            | "content_type"
                            | "cache_status"
                            | "response_time"
                    )
            })
            .take(12)
            .map(|column| {
                let (offset, length) = column.byte_range();
                let stats = column.statistics();
                Column {
                    name: column.column_path().string(),
                    offset,
                    length,
                    nulls: stats.and_then(|s| s.null_count_opt()),
                    min: stats
                        .filter(|s| s.min_is_exact())
                        .and_then(|s| s.min_bytes_opt())
                        .map(hex::encode),
                    max: stats
                        .filter(|s| s.max_is_exact())
                        .and_then(|s| s.max_bytes_opt())
                        .map(hex::encode),
                    physical_type: column.column_type().to_string(),
                }
            })
            .collect();
        groups.push(RowGroup {
            rows: group.num_rows().try_into().ok()?,
            columns,
        });
        if groups.len() > 8 {
            return None;
        }
    }
    let metadata = Metadata {
        row_groups: groups,
        columns_complete,
    };
    (serde_json::to_vec(&metadata).ok()?.len() <= METADATA_BYTES).then_some(metadata)
}
