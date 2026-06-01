"""Centralised SQL metric definitions."""

from __future__ import annotations

from backend.models.metrics import MetricType

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
