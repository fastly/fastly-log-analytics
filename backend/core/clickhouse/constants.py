"""ClickHouse schema constants, table/column allowlists, error types, and dataclasses."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

from backend.core.sql_validator import SAFE_IDENT_RE as _PARAM_NAME

CLICKHOUSE_FACT_TABLE = "log_facts"
CLICKHOUSE_FACT_COLUMNS = (
    "service_id",
    "batch_id",
    "source_identity",
    "row_ordinal",
    "generation",
    "timestamp",
    "country",
    "ip",
    "url",
    "conn_requests",
)
CLICKHOUSE_HIGH_SCALE_TABLES = frozenset(
    {
        "high_scale_batch_publications",
        "request_facts",
        "request_aggregates",
        "rum_vitals_facts",
        "rum_error_facts",
        "cmcd_projection_facts",
        "rum_vitals_aggregates",
        "rum_error_aggregates",
        "cmcd_aggregates",
        "origin_minute_summary",
        "origin_minute_dimensions",
        "performance_minute_dimensions",
        "performance_minute_summary",
        "performance_scatter_samples",
        "security_minute_dimensions",
        "network_minute_dimensions",
    }
)
CLICKHOUSE_HIGH_SCALE_COLUMNS = frozenset(
    {
        "batch_id",
        "service_id",
        "domain",
        "generation",
        "batch_digest",
        "expected_rows",
        "visible_rows",
        "quorum_acked",
        "publication_state",
        "manifest_version",
        "updated_at",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "country",
        "client_ip",
        "url",
        "custom_fields",
        "cmcd",
        "client_id",
        "request_event_id",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "error_message",
        "error_file",
        "projection_key",
        "request_count",
        "event_count",
        "error_count",
        "session_count",
        "value_sum",
        "bucket_start",
        "dimension",
        "value",
        "requests",
        "misses",
        "passes",
        "origin_5xx",
        "status_count",
        "origin_bytes",
        "latency_count",
        "ttlb_count",
        "overhead_count",
        "origin_bytes_count",
        "latency_p50_us",
        "latency_p75_us",
        "latency_p95_us",
        "latency_p99_us",
        "ttlb_p50_us",
        "ttlb_p95_us",
        "cdn_overhead_p50_us",
        "origin_bytes_p50",
        "latency_sk_v",
        "latency_sk_w",
        "ttlb_sk_v",
        "ttlb_sk_w",
        "overhead_sk_v",
        "overhead_sk_w",
        "origin_bytes_sk_v",
        "origin_bytes_sk_w",
        "asn",
        "browser",
        "os",
        "device",
        "city",
        "region",
        "pop",
        "tls",
        "ttfb",
        "error_line",
        "error_col",
        "latency_sum_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "wellknown_bot_name",
        "bot_category",
        "verified_count",
        "impersonator_count",
        "unverified_count",
        "c_speed",
        "errors",
        "tcp_rtt_count",
        "tcp_rtt_sum",
        "tcp_rtt_p50_us",
        "tcp_rtt_p95_us",
        "tcp_rtt_p99_us",
        "ttfb_p50_us",
        "ttfb_p95_us",
        "ploss_sum",
        "ploss_count",
        "component_count",
        "edge_processing_sum_ms",
        "origin_wait_sum_ms",
        "origin_download_sum_ms",
        "client_download_sum_ms",
        "sample_key",
        "origin_ms",
        "edge_ms",
        "cache",
    }
)
CLICKHOUSE_ALLOWED_TABLES = frozenset({CLICKHOUSE_FACT_TABLE}) | CLICKHOUSE_HIGH_SCALE_TABLES
CLICKHOUSE_TABLE_COLUMNS = {
    CLICKHOUSE_FACT_TABLE: frozenset(CLICKHOUSE_FACT_COLUMNS),
    **{table: CLICKHOUSE_HIGH_SCALE_COLUMNS for table in CLICKHOUSE_HIGH_SCALE_TABLES},
}
_DDL_PASSWORD = re.compile(r"(\bIDENTIFIED\s+WITH\s+\w+\s+BY\s+)'(?:[^'\\]|\\.|'')*'", re.IGNORECASE)

# Per-query settings callers may add. They travel as HTTP settings (never as SQL text, where a user
# comment could swallow them) and may only narrow what a query can read or return, never widen it.
_QUERY_SETTINGS = frozenset(
    {
        "additional_table_filters",
        "date_time_input_format",
        "max_result_rows",
        "result_overflow_mode",
        "max_rows_to_read",
        "read_overflow_mode",
    }
)
_PARAM_ESCAPES = {
    ord("\\"): "\\\\",
    ord("\n"): "\\n",
    ord("\t"): "\\t",
    ord("\r"): "\\r",
    ord("\0"): "\\0",
    ord("\b"): "\\b",
    ord("\f"): "\\f",
}


class ClickHouseError(RuntimeError):
    """Sanitized failure; insert timeouts can mean an ambiguously committed write."""

    def __init__(self, reason: str, *, query_id: str, status_code: int | None = None):
        self.query_id = query_id
        self.status_code = status_code
        self.reason = reason
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"ClickHouse {reason}{suffix}; query_id={query_id}")


CLICKHOUSE_SCHEMA_VERSION = 2
FACT_TYPES = (
    ("service_id", "String"),
    ("batch_id", "String"),
    ("source_identity", "String"),
    ("row_ordinal", "UInt64"),
    ("generation", "String"),
    ("timestamp", "DateTime64(6, 'UTC')"),
    ("country", "Nullable(String)"),
    ("ip", "Nullable(String)"),
    ("url", "Nullable(String)"),
    ("conn_requests", "Nullable(Int64)"),
)
CLICKHOUSE_FACT_DDL = """
CREATE TABLE IF NOT EXISTS log_facts (
    service_id String,
    batch_id String,
    source_identity String,
    row_ordinal UInt64,
    generation String,
    timestamp DateTime64(6, 'UTC'),
    country Nullable(String),
    ip Nullable(String),
    url Nullable(String),
    conn_requests Nullable(Int64)
) ENGINE = ReplacingMergeTree
PARTITION BY (service_id, generation)
ORDER BY (service_id, generation, batch_id, row_ordinal)
"""

MAX_DATASET_ROWS = 1_000_000
MAX_BATCH_ROWS = 10_000
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_ROW_BYTES = 64 * 1024
PAYLOAD_COLUMNS = ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests")
ARTIFACT_SCHEMA = pa.schema(
    [
        ("source_identity", pa.string()),
        ("row_ordinal", pa.uint64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("country", pa.string()),
        ("ip", pa.string()),
        ("url", pa.string()),
        ("conn_requests", pa.int64()),
    ]
)


@dataclass(frozen=True)
class Dataset:
    service_id: str
    dataset_id: str
    catalog_identity: str
    source_table: str
    source_snapshot: int
    coverage_start: datetime
    coverage_end: datetime
    expires_at: datetime
    expected_rows: int
    canonical_digest: str
    schema_version: int = CLICKHOUSE_SCHEMA_VERSION


@dataclass(frozen=True)
class Artifact:
    service_id: str
    dataset_id: str
    batch_id: str
    artifact_uri: str
    content_hash: str
    canonical_digest: str
    byte_size: int
    ordinal_start: int
    row_count: int


def _construct(cls: Any, row: Any) -> Any:
    return cls(**{name: row[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class DurableBatch:
    artifact: Artifact
    rows: tuple[tuple, ...]


@dataclass(frozen=True)
class PublicationResult:
    batch_id: str
    status: str
    row_count: int = 0
    fence: int | None = None


@dataclass(frozen=True)
class ReplayResult:
    generation: str
    attempted: int
    published: int
    activated: bool


__all__ = [
    "ARTIFACT_SCHEMA",
    "Artifact",
    "CLICKHOUSE_ALLOWED_TABLES",
    "CLICKHOUSE_FACT_COLUMNS",
    "CLICKHOUSE_FACT_DDL",
    "CLICKHOUSE_FACT_TABLE",
    "CLICKHOUSE_HIGH_SCALE_COLUMNS",
    "CLICKHOUSE_HIGH_SCALE_TABLES",
    "CLICKHOUSE_SCHEMA_VERSION",
    "CLICKHOUSE_TABLE_COLUMNS",
    "ClickHouseError",
    "Dataset",
    "DurableBatch",
    "FACT_TYPES",
    "MAX_ARTIFACT_BYTES",
    "MAX_BATCH_ROWS",
    "MAX_DATASET_ROWS",
    "MAX_ROW_BYTES",
    "PAYLOAD_COLUMNS",
    "PublicationResult",
    "ReplayResult",
    "_DDL_PASSWORD",
    "_PARAM_ESCAPES",
    "_PARAM_NAME",
    "_QUERY_SETTINGS",
    "_construct",
]
