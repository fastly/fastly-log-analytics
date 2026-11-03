"""Canonical SQL fragment builders and metric SQL expressions."""

from __future__ import annotations

from collections.abc import Collection

from backend.models.common import MetricType

HIT_STATES = "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"


def coalesce(expr: str, default: str) -> str:
    return f"COALESCE({expr}, {default})"


def sum_or_zero(expr: str, *, zero: str = "0", where: str | None = None) -> str:
    agg = f"SUM({expr})" if where is None else f"SUM({expr}) FILTER (WHERE {where})"
    return coalesce(agg, zero)


def column_or_default(actual_cols: Collection[str], col: str, default: str, *, cast: str | None = None) -> str:
    if col not in actual_cols:
        return default
    ref = f'"{col}"' if cast is None else f'CAST("{col}" AS {cast})'
    return coalesce(ref, default)


def protocol_label_expr(actual_cols: Collection[str]) -> str | None:
    """Clean HTTP protocol label for network distribution, or ``None`` when no protocol column exists."""
    col = "protocol" if "protocol" in actual_cols else ("proto" if "proto" in actual_cols else None)
    if col is None:
        if "transport" in actual_cols:
            return f"UPPER({coalesce('CAST(transport AS VARCHAR)', repr('TCP'))})"
        return None
    raw = f"CAST({col} AS VARCHAR)"
    return (
        f"CASE WHEN {raw} IN ('3.0', '3', 'HTTP/3') THEN 'HTTP/3' "
        f"WHEN {raw} IN ('2.0', '2', 'HTTP/2') THEN 'HTTP/2' "
        f"WHEN {raw} IN ('1.1', 'HTTP/1.1') THEN 'HTTP/1.1' "
        f"WHEN {raw} IN ('1.0', 'HTTP/1.0') THEN 'HTTP/1.0' "
        f"ELSE {coalesce(raw, repr('Other'))} END"
    )


def origin_latency_us(actual_cols: Collection[str], *, absent: str | None = "NULL") -> str | None:
    has_ottfb, has_ttfb = "ottfb" in actual_cols, "ttfb" in actual_cols
    if has_ottfb and has_ttfb:
        return 'COALESCE("ottfb", "ttfb" * 1000000.0)'
    if has_ottfb:
        return '"ottfb"'
    if has_ttfb:
        return '"ttfb" * 1000000.0'
    return absent


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
