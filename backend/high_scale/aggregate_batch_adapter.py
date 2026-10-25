"""ClickHouse writer for high-scale aggregate and projection batches.

Mirrors :class:`~backend.high_scale.publication.ClickHouseBatchAdapter`'s
idempotent insert-behind-a-publication-row pattern (same
``high_scale_batch_publications`` bookkeeping table, since its schema is
already domain-agnostic). Flat ``*_aggregate`` batches share one dimension
count shape; richer Origin summary and dimension projections use explicit
allowlisted column maps. Their domain names remain distinct from fact batches
for the same source object in the shared bookkeeping table.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from backend.high_scale._common import bucket as _bucket
from backend.high_scale.publication import (
    BaseClickHouseBatchAdapter,
    HighScaleBatch,
    InsertReceipt,
    _batch_uuid,
)

DIMENSIONS_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    "request": (
        "url",
        "country",
        "client_ip",
        "asn",
        "host",
        "method",
        "status",
        "cache",
        "proto",
        "ua",
        "referer",
        "cookie_session",
        "resp_header_content_encoding",
        "ttl",
        "age",
        "hits",
        "digest",
        "city",
        "region",
        "metro",
        "transport",
        "c_speed",
        "c_type",
        "pop",
        "backend",
        "edge",
        "server_region",
        "tls",
        "is_ipv6",
        "conn_requests",
        "waf",
        "waf_resp",
        "waf_ms",
        "p_type",
        "p_desc",
        "ja3",
        "ja4",
        "tls_ciphers_sha",
        "io_input_format",
        "io_output_format",
    ),
    "rum_vitals": ("metric_name",),
    "rum_errors": ("error_message",),
    "cmcd": ("cmcd_session",),
}
_SOURCE_FIELD_OVERRIDES: dict[str, str] = {
    "client_ip": "ip",
    "cmcd_session": "sid",
}


def compute_dimension_counts(
    domain: str,
    rows: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    """Count occurrences of each configured dimension's value, per minute bucket."""
    if domain not in DIMENSIONS_BY_DOMAIN:
        raise ValueError(f"unsupported aggregate write domain: {domain}")
    dimensions = DIMENSIONS_BY_DOMAIN[domain]
    counts: Counter[tuple[str, str, datetime]] = Counter()
    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        for dimension in dimensions:
            field = _SOURCE_FIELD_OVERRIDES.get(dimension, dimension)
            value = str(row.get(field, ""))
            counts[(dimension, value, bucket)] += 1
    return tuple(
        {"dimension": dimension, "value": value, "bucket_start": bucket.isoformat(), "count": count}
        for (dimension, value, bucket), count in counts.items()
    )


_AGGREGATE_TABLES: dict[str, tuple[str, str]] = {
    "request_aggregate": ("request_aggregates", "request_count"),
    "rum_vitals_aggregate": ("rum_vitals_aggregates", "event_count"),
    "rum_errors_aggregate": ("rum_error_aggregates", "error_count"),
    "cmcd_aggregate": ("cmcd_aggregates", "event_count"),
}
_ORIGIN_COLUMNS: dict[str, tuple[str, tuple[str, ...]]] = {
    "origin_summary": (
        "origin_minute_summary",
        (
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
        ),
    ),
    "origin_dimensions": (
        "origin_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "origin_5xx",
            "origin_bytes",
            "latency_count",
            "latency_p50_us",
            "latency_p95_us",
            "latency_p99_us",
            "latency_sk_v",
            "latency_sk_w",
        ),
    ),
    "performance_dimensions": (
        "performance_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "latency_count",
            "latency_sum_ms",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
        ),
    ),
    "performance_summary": (
        "performance_minute_summary",
        (
            "component_count",
            "edge_processing_sum_ms",
            "origin_wait_sum_ms",
            "origin_download_sum_ms",
            "client_download_sum_ms",
        ),
    ),
    "performance_scatter": (
        "performance_scatter_samples",
        ("sample_key", "origin_ms", "edge_ms", "cache"),
    ),
    "security_dimensions": (
        "security_minute_dimensions",
        (
            "dimension",
            "value",
            "requests",
            "wellknown_bot_name",
            "bot_category",
            "verified_count",
            "impersonator_count",
            "unverified_count",
        ),
    ),
    "network_dimensions": (
        "network_minute_dimensions",
        (
            "dimension",
            "value",
            "c_speed",
            "requests",
            "errors",
            "tcp_rtt_count",
            "tcp_rtt_sum",
            "tcp_rtt_p50_us",
            "tcp_rtt_p95_us",
            "tcp_rtt_p99_us",
            "ploss_sum",
            "ploss_count",
            "ttfb_p50_us",
            "ttfb_p95_us",
        ),
    ),
}


def _bucket_start(value: Any) -> datetime:
    """Batch rows carry bucket_start as an ISO string (HighScaleBatch.digest
    requires JSON-serializable row values) — parse back for the insert."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class AggregateBatchAdapter(BaseClickHouseBatchAdapter):
    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain not in _AGGREGATE_TABLES and batch.domain not in _ORIGIN_COLUMNS:
            raise ValueError(f"unsupported aggregate domain: {batch.domain}")
        if not batch.rows:
            raise ValueError("aggregate batch must contain at least one row")
        batch_uuid = _batch_uuid(batch)
        with self._batch_lock(batch_uuid):
            return self._insert_locked(batch, batch_uuid)

    def _insert_locked(self, batch: HighScaleBatch, batch_uuid: str) -> InsertReceipt:
        if batch.domain in _ORIGIN_COLUMNS:
            table, metric_columns = _ORIGIN_COLUMNS[batch.domain]
            columns = ["service_id", "bucket_start", *metric_columns, "batch_id", "publication_state"]
            mapped_rows = [
                (
                    batch.service_id,
                    _bucket_start(row["bucket_start"]),
                    *(row[column] for column in metric_columns),
                    batch_uuid,
                    "visible",
                )
                for row in batch.rows
            ]
        else:
            table, metric_column = _AGGREGATE_TABLES[batch.domain]
            columns = [
                "service_id",
                "bucket_start",
                "dimension",
                "value",
                metric_column,
                "batch_id",
                "publication_state",
            ]
            mapped_rows = [
                (
                    batch.service_id,
                    _bucket_start(row["bucket_start"]),
                    str(row["dimension"]),
                    str(row["value"]),
                    int(row["count"]),
                    batch_uuid,
                    "visible",
                )
                for row in batch.rows
            ]

        return self._publish_table_rows(
            table=table,
            columns=columns,
            mapped_rows=mapped_rows,
            batch=batch,
            batch_uuid=batch_uuid,
        )
