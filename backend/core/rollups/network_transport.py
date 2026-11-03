"""Per-hour transport rollups for the Network page's transport card.

``network_transport.parquet`` holds one row per closed hour (schema:
``hour_start, requests, total_bytes, rtt_count, rtt_p50_us, rtt_p90_us,
rtt_p95_us, rtt_p99_us, retrans_sum, retrans_count, ploss_sum, ploss_count,
delivery_sum, delivery_count``). Counts and sums merge exactly across hours; the
tcp_rtt percentiles are request-weighted by ``rtt_count`` (DuckDB has no sketch
combine), so readers must flag the result ``_approx``.

``network_transport_proto.parquet`` holds ``(proto_clean, requests)`` per hour
using the same label expression as the live query, so the split is exact.
"""

from __future__ import annotations

import logging
from collections.abc import Collection

from backend.core.metric_expressions import protocol_label_expr, sum_or_zero

from ._common import (
    NETWORK_TRANSPORT_BUNDLE_FILENAME,
    NETWORK_TRANSPORT_PROTO_BUNDLE_FILENAME,
    MultiBundleSpec,
    backfill_multi_spec_bundles,
    build_multi_spec_bundles,
    build_per_hour_bundles,
    parquet_zstd_target,
)

logger = logging.getLogger(__name__)

_RTT_QUANTILES = (("0.50", "rtt_p50_us"), ("0.90", "rtt_p90_us"), ("0.95", "rtt_p95_us"), ("0.99", "rtt_p99_us"))


def _context(cols: set[str], table_ident: str) -> frozenset[str]:
    return frozenset(cols)


def _sum_count(cols: Collection[str], col: str, *, positive_only: bool = False) -> tuple[str, str]:
    if col not in cols:
        return "CAST(0 AS DOUBLE)", "CAST(0 AS BIGINT)"
    cond = f"{col} IS NOT NULL AND {col} > 0" if positive_only else f"{col} IS NOT NULL"
    return (
        f"CAST({sum_or_zero(f'CAST({col} AS DOUBLE)', where=cond)} AS DOUBLE)",
        f"CAST(COUNT(*) FILTER (WHERE {cond}) AS BIGINT)",
    )


def transport_aggregate_columns(ctx: Collection[str]) -> str:
    """Select list shared by the hourly writer and the readers' raw boundary slices."""
    bytes_col = "resp_bytes" if "resp_bytes" in ctx else ("bytes" if "bytes" in ctx else None)
    total_bytes = sum_or_zero(bytes_col) if bytes_col else "0"
    rtt_cond = "tcp_rtt IS NOT NULL AND tcp_rtt > 0"
    if "tcp_rtt" in ctx:
        rtt_count = f"CAST(COUNT(*) FILTER (WHERE {rtt_cond}) AS BIGINT)"
        rtt_quants = [
            f"CAST(approx_quantile(CAST(tcp_rtt AS DOUBLE), {q}) FILTER (WHERE {rtt_cond}) AS DOUBLE) AS {alias}"
            for q, alias in _RTT_QUANTILES
        ]
    else:
        rtt_count = "CAST(0 AS BIGINT)"
        rtt_quants = [f"CAST(NULL AS DOUBLE) AS {alias}" for _, alias in _RTT_QUANTILES]
    retrans_sum, retrans_count = _sum_count(ctx, "retrans")
    ploss_sum, ploss_count = _sum_count(ctx, "ploss")
    delivery_sum, delivery_count = _sum_count(ctx, "delivery_rate", positive_only=True)
    return ", ".join(
        [
            "CAST(COUNT(*) AS BIGINT) AS requests",
            f"CAST({total_bytes} AS BIGINT) AS total_bytes",
            f"{rtt_count} AS rtt_count",
            *rtt_quants,
            f"{retrans_sum} AS retrans_sum",
            f"{retrans_count} AS retrans_count",
            f"{ploss_sum} AS ploss_sum",
            f"{ploss_count} AS ploss_count",
            f"{delivery_sum} AS delivery_sum",
            f"{delivery_count} AS delivery_count",
        ]
    )


def _transport_copy_sql(ctx: Collection[str], table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    return (
        f"COPY (SELECT TIMESTAMPTZ '{start_iso}' AS hour_start, {transport_aggregate_columns(ctx)} "
        f"FROM {table_ident} "
        f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}') "
        f"{parquet_zstd_target(tmp_path)}"
    )


def _proto_copy_sql(ctx: Collection[str], table_ident: str, start_iso: str, end_iso: str, tmp_path: str) -> str:
    expr = protocol_label_expr(ctx)
    if expr is None:
        return (
            f"COPY (SELECT CAST(NULL AS VARCHAR) AS proto_clean, CAST(0 AS BIGINT) AS requests WHERE FALSE) "
            f"{parquet_zstd_target(tmp_path)}"
        )
    return (
        f"COPY (SELECT CAST({expr} AS VARCHAR) AS proto_clean, "
        f"CAST(COUNT(*) AS BIGINT) AS requests "
        f"FROM {table_ident} "
        f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' "
        f"GROUP BY proto_clean) {parquet_zstd_target(tmp_path)}"
    )


_SPECS: tuple[MultiBundleSpec, ...] = (
    ("transport", NETWORK_TRANSPORT_BUNDLE_FILENAME, ("timestamp",), _transport_copy_sql),
    ("proto", NETWORK_TRANSPORT_PROTO_BUNDLE_FILENAME, ("timestamp",), _proto_copy_sql),
)


def build_network_transport_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    return build_multi_spec_bundles(
        service_id,
        source,
        hours,
        specs=_SPECS,
        group_label="network_transport",
        tmp_prefix=".tmp_nt_",
        logger=logger,
        context_fn=_context,
        driver=build_per_hour_bundles,
    )


def backfill_network_transport_bundles(service_id: str, source: dict) -> int:
    return backfill_multi_spec_bundles(
        service_id,
        source,
        specs=_SPECS,
        group_label="network_transport",
        tmp_prefix=".tmp_nt_",
        logger=logger,
        context_fn=_context,
        driver=build_per_hour_bundles,
    )
