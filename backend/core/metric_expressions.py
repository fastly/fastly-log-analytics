"""Canonical SQL fragment builders (metric expressions).

A tiny typed catalog of SQL expression builders that were being re-derived,
byte-for-byte, across the rollup writers and the repository query layer. Each
builder takes the set of columns actually present on a service's schema
(``actual_cols``) and returns the exact SQL fragment to embed — so the
availability-adaptive logic lives in one place instead of being copy-pasted
with a "kept in sync rather than imported" comment at each site.

This module lives in ``backend.core`` (a leaf — stdlib only) precisely so the
rollup writers (``backend.core.rollups.*``) can import it without the
import-graph cycle that blocked importing the original helper out of
``backend.repositories._base`` (repositories depends on core, not the other
way around; import-linter's "Core does not depend on routers" and the
rollups→repositories cycle both stay satisfied).

Only expressions that were **byte-identical in meaning** across their call
sites are consolidated here. Fragments that merely look similar but differ in
cast, FILTER clause, quoting, or aggregation are intentionally left at their
call sites — collapsing them would be a behavior change, not DRY.
"""

from __future__ import annotations

from collections.abc import Collection

from backend.models._base import MetricType

__all__ = [
    "HIT_STATES",
    "METRIC_DEFINITIONS",
    "coalesce",
    "column_or_default",
    "get_metric_sql",
    "origin_latency_us",
    "sum_or_zero",
]

# Cache states that count as an edge hit (SQL ``IN`` tuple literal).
HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def coalesce(expr: str, default: str) -> str:
    """Render ``COALESCE(<expr>, <default>)`` byte-for-byte."""
    return f"COALESCE({expr}, {default})"


def sum_or_zero(expr: str, *, zero: str = "0", where: str | None = None) -> str:
    """Render a NULL-safe sum: ``COALESCE(SUM(<expr>)[ FILTER (WHERE <where>)], <zero>)``."""
    agg = f"SUM({expr})" if where is None else f"SUM({expr}) FILTER (WHERE {where})"
    return coalesce(agg, zero)


def column_or_default(
    actual_cols: Collection[str],
    col: str,
    default: str,
    *,
    cast: str | None = None,
) -> str:
    """Return a quoted-column expression with a NULL default, or the bare default.

    Present column: ``COALESCE("<col>", <default>)`` (or
    ``COALESCE(CAST("<col>" AS <cast>), <default>)`` when ``cast`` is given).
    Absent column: ``<default>`` — so the expression is always valid SQL on a
    schema that lacks the optional column.
    """
    if col not in actual_cols:
        return default
    ref = f'"{col}"' if cast is None else f'CAST("{col}" AS {cast})'
    return coalesce(ref, default)


def origin_latency_us(actual_cols: Collection[str], *, absent: str | None = "NULL") -> str | None:
    """Return the origin-latency-in-microseconds SQL expression for a schema.

    Resolves the three availability cases identically to every call site that
    previously inlined this fragment:

    * both ``ottfb`` (µs) and ``ttfb`` (s) present → prefer ``ottfb``, fall
      back to ``ttfb`` scaled to µs;
    * only ``ottfb`` → ``ottfb`` as-is;
    * only ``ttfb`` → ``ttfb`` scaled to µs.

    Args:
        actual_cols: Columns present on the service's schema.
        absent: Value to return when NEITHER latency column exists. Callers
            differ only here: the repository seed returns the literal string
            ``"NULL"`` (so the expression is always valid SQL), while the
            rollup writers pass ``absent=None`` to signal "skip this service —
            there is nothing to roll up".

    Returns:
        The SQL fragment string, or ``absent`` when no latency column exists.
    """
    has_ottfb = "ottfb" in actual_cols
    has_ttfb = "ttfb" in actual_cols
    if has_ottfb and has_ttfb:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if has_ottfb:
        return '"ottfb"'
    if has_ttfb:
        return '"ttfb" * 1000000.0'
    return absent


# ── Centralised SQL metric definitions ───────────────────────────────────────

METRIC_DEFINITIONS = {
    MetricType.REQUESTS.value: {"sql": "count(*)", "label": "Requests"},
    MetricType.ERRORS_5XX.value: {"sql": "sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END)", "label": "5xx Errors"},
    "5xx_rate": {
        "sql": "sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "5xx Error Rate",
    },
    MetricType.ERRORS_4XX.value: {
        "sql": "sum(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END)",
        "label": "4xx Errors",
    },
    "4xx_rate": {
        "sql": "sum(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "4xx Error Rate",
    },
    MetricType.LATENCY_P95.value: {
        "sql": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(elapsed AS DOUBLE)) / 1000.0",
        "label": "p95 Latency (s)",
    },
    MetricType.HIT_RATE.value: {
        "sql": "SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)",
        "label": "Cache Hit Rate",
    },
    MetricType.THROUGHPUT.value: {"sql": "sum(resp_bytes)", "label": "Bandwidth"},
    MetricType.TTFB.value: {
        "sql": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(ottfb AS DOUBLE)) / 1000.0",
        "label": "p95 TTFB (s)",
    },
    MetricType.REQ_SIZE.value: {"sql": "avg(req_bytes + req_header_bytes)", "label": "Request Size (Bytes)"},
}


def get_metric_sql(metric: str, status_codes: list[int] | None = None, table_name: str | None = None) -> str:
    """Return the SQL aggregation expression or a full SELECT fragment for a metric."""
    status_clause = ""
    if status_codes and isinstance(status_codes, list) and len(status_codes) > 0:
        codes_str = ", ".join(str(c) for c in status_codes if isinstance(c, int))
        if codes_str:
            status_clause = f" AND status IN ({codes_str})"

    if metric == MetricType.SPECIFIC_STATUS.value:
        agg_sql = f"sum(CASE WHEN 1=1 {status_clause} THEN 1 ELSE 0 END)"
    elif metric == "specific_status_rate":
        agg_sql = f"sum(CASE WHEN 1=1 {status_clause} THEN 1 ELSE 0 END) * 100.0 / NULLIF(count(*), 0)"
    else:
        agg_sql = METRIC_DEFINITIONS.get(metric, {}).get("sql", "count(*)")

    if table_name:
        if metric in (MetricType.ERRORS_5XX.value, MetricType.ERRORS_4XX.value, MetricType.SPECIFIC_STATUS.value):
            # Optimization: use WHERE clause instead of sum(CASE WHEN...) for simple counts
            if metric == MetricType.ERRORS_5XX.value:
                return f"SELECT count(*) FROM {table_name} WHERE status >= 500"
            elif metric == MetricType.ERRORS_4XX.value:
                return f"SELECT count(*) FROM {table_name} WHERE status BETWEEN 400 AND 499"
            elif metric == MetricType.SPECIFIC_STATUS.value:
                return f"SELECT count(*) FROM {table_name} WHERE 1=1 {status_clause}"
        elif metric == MetricType.TTFB.value:
            return f"SELECT {agg_sql} FROM {table_name} WHERE ottfb IS NOT NULL"

        return f"SELECT {agg_sql} FROM {table_name}"

    return agg_sql
