"""RUM analytics repository."""

from __future__ import annotations

import logging
import os
import re
import sys as _sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.retry import retry_call
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import track_query

logger = logging.getLogger(__name__)


def is_stale_view_error(exc: BaseException) -> bool:
    from backend.core.storage._core import is_stale_view_error as _is_stale_view_error

    return _is_stale_view_error(exc)


sql_rum = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.rum", _sys.modules[__name__])

BEACON_HEALTH_RECENT = (
    "SELECT COUNT(*) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors) "
    "WHERE timestamp > ?"
)

BEACON_HEALTH_LAST = (
    "SELECT MAX(timestamp) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors)"
)

DISTINCT_ID = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

RUM_TEMP_VITALS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_vitals AS "
    f"SELECT timestamp, req_id, cid, {DISTINCT_ID} AS distinct_id, "
    "metric_name, metric_value, metric_rating, browser, os, device, pathname "
    "FROM client_vitals WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_TEMP_ERRORS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_errors AS "
    f"SELECT timestamp, req_id, cid, {DISTINCT_ID} AS distinct_id, "
    "pathname, error_message, error_file, error_line, error_col "
    "FROM client_errors WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_ROLLUP_MAX_BUCKET = """
SELECT MAX(bucket_start) FROM (
    SELECT MAX(bucket_start) AS bucket_start FROM rum_vitals_aggregates WHERE service_id = ?
    UNION ALL
    SELECT MAX(bucket_start) AS bucket_start FROM rum_error_aggregates WHERE service_id = ?
)
"""

RUM_TAIL_VITALS_CREATE = f"""
CREATE OR REPLACE TEMP TABLE t_tail_vitals AS
SELECT timestamp, req_id, cid, {DISTINCT_ID} AS distinct_id, metric_name, metric_value, metric_rating, browser, os, device, pathname
FROM (
    SELECT
        timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname,
        ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) AS _rn
    FROM client_vitals
    WHERE timestamp >= ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
)
WHERE _rn = 1
"""

RUM_TAIL_ERRORS_CREATE = f"""
CREATE OR REPLACE TEMP TABLE t_tail_errors AS
SELECT timestamp, req_id, cid, {DISTINCT_ID} AS distinct_id, pathname, error_message, error_file, error_line, error_col
FROM (
    SELECT
        timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col,
        ROW_NUMBER() OVER (PARTITION BY timestamp, error_message, error_file, error_line, pathname, cid, req_id ORDER BY timestamp) AS _rn
    FROM client_errors
    WHERE timestamp >= ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
)
WHERE _rn = 1
"""

RUM_TAIL_HAS_ROWS = "SELECT (SELECT COUNT(*) FROM t_tail_vitals) + (SELECT COUNT(*) FROM t_tail_errors)"


def _rum_tail_vitals_branch(
    dimension: str,
    value_expr: str,
    group_by: str,
    *,
    event_count_expr: str = "COUNT(DISTINCT {distinct_id})",
    value_sum_expr: str = "0.0",
    rating_counts: bool = False,
    p50_expr: str = "0.0",
    p75_expr: str = "0.0",
    p99_expr: str = "0.0",
    where_clause: str = "",
    from_clause: str = "t_tail_vitals",
) -> str:
    ratings = (
        "COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count, "
        "COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count, "
        "COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count"
        if rating_counts
        else "0 AS good_count, 0 AS ni_count, 0 AS poor_count"
    )
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    return (
        f"SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, "
        f"'{dimension}' AS dimension, {value_expr} AS value, {event_count_expr} AS event_count, "
        f"{value_sum_expr} AS value_sum, {ratings}, "
        f"{p50_expr} AS p50_value, {p75_expr} AS p75_value, {p99_expr} AS p99_value "
        f"FROM {from_clause}{where_sql} GROUP BY {group_by}"
    )


_RUM_TAIL_VITALS_BRANCHES = "\nUNION ALL\n".join(
    [
        _rum_tail_vitals_branch(
            "total",
            "CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END",
            "bucket_start, value",
        ),
        _rum_tail_vitals_branch(
            "metric_name",
            "metric_name",
            "bucket_start, metric_name",
            event_count_expr="COUNT(*)",
            value_sum_expr="CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE)",
            rating_counts=True,
            p50_expr="CAST(MEDIAN(metric_value) AS DOUBLE)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            p99_expr="CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
        ),
        _rum_tail_vitals_branch("browser", "IFNULL(browser, 'Unknown')", "bucket_start, browser"),
        _rum_tail_vitals_branch("os", "IFNULL(os, 'Unknown')", "bucket_start, os"),
        _rum_tail_vitals_branch("device", "IFNULL(device, 'Unknown')", "bucket_start, device"),
        _rum_tail_vitals_branch("pathname", "pathname", "bucket_start, pathname", where_clause="pathname IS NOT NULL"),
        _rum_tail_vitals_branch(
            "path_metric_load",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            value_sum_expr="CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE)",
            where_clause=(
                "pathname IS NOT NULL AND metric_name IN "
                "('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')"
            ),
        ),
        _rum_tail_vitals_branch(
            "path_metric_lcp",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            where_clause="pathname IS NOT NULL AND metric_name IN ('LCP', 'lcp')",
        ),
        _rum_tail_vitals_branch(
            "path_metric_cls",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            where_clause="pathname IS NOT NULL AND metric_name IN ('CLS', 'cls')",
        ),
        _rum_tail_vitals_branch(
            "total",
            "'total_beacons'",
            "bucket_start",
            event_count_expr="COUNT(DISTINCT distinct_id)",
            from_clause=(
                "(SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_vitals "
                "UNION ALL SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_errors)"
            ),
        ),
    ]
)

RUM_EFF_VITALS_AGG_CREATE = f"""
CREATE OR REPLACE TEMP TABLE t_eff_vitals_agg AS
WITH tail_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_vitals
    UNION
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_errors
)
SELECT
    service_id, bucket_start, dimension, value,
    event_count, value_sum, good_count, ni_count, poor_count,
    p50_value, p75_value, p99_value
FROM rum_vitals_aggregates
WHERE service_id = ?
  AND bucket_start >= CAST(? AS TIMESTAMPTZ)
  AND bucket_start <= CAST(? AS TIMESTAMPTZ)
  AND bucket_start NOT IN (SELECT bucket_start FROM tail_hours)
UNION ALL
{_RUM_TAIL_VITALS_BRANCHES}
"""

RUM_EFF_ERROR_AGG_CREATE = """
CREATE OR REPLACE TEMP TABLE t_eff_error_agg AS
WITH tail_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_vitals
    UNION
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_errors
)
SELECT
    service_id, bucket_start, dimension, value, error_count
FROM rum_error_aggregates
WHERE service_id = ?
  AND bucket_start >= CAST(? AS TIMESTAMPTZ)
  AND bucket_start <= CAST(? AS TIMESTAMPTZ)
  AND bucket_start NOT IN (SELECT bucket_start FROM tail_hours)
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'total' AS dimension,
    'errors' AS value, COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors GROUP BY bucket_start
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'pathname' AS dimension,
    pathname AS value, COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors WHERE pathname IS NOT NULL GROUP BY bucket_start, pathname
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'exception' AS dimension,
    concat(IFNULL(error_message, ''), '|', IFNULL(error_file, ''), '|', CAST(IFNULL(error_line, 0) AS VARCHAR), '|', CAST(IFNULL(error_col, 0) AS VARCHAR)) AS value,
    COUNT(*) AS error_count
FROM t_tail_errors GROUP BY bucket_start, value
"""

RUM_ROLLUP_ANY_DATA = "SELECT (SELECT COUNT(*) FROM {vitals_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)) + (SELECT COUNT(*) FROM {error_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ))"


@dataclass(frozen=True)
class _RumRollupQuerySpec:
    select_exprs: str
    table_placeholder: str = "{vitals_agg_table}"
    dimension_filter: str = ""
    group_by: str = ""
    order_by: str = ""
    limit: int | None = None

    def build_sql(self) -> str:
        dim_clause = f" AND dimension = '{self.dimension_filter}'" if self.dimension_filter else ""
        group_clause = f"\nGROUP BY {self.group_by}" if self.group_by else ""
        order_clause = f"\nORDER BY {self.order_by}" if self.order_by else ""
        limit_clause = f"\nLIMIT {self.limit}" if self.limit is not None else ""
        return (
            f"\nSELECT\n    {self.select_exprs}\n"
            f"FROM {self.table_placeholder}\n"
            f"WHERE service_id = ?{dim_clause} AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)"
            f"{group_clause}{order_clause}{limit_clause}\n"
        )


RUM_ROLLUP_COUNTS_VITALS = _RumRollupQuerySpec(
    "COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'pageviews'), 0) AS pageviews,\n"
    "    COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'interactions'), 0) AS interactions"
).build_sql()

RUM_ROLLUP_COUNTS_ERRORS = _RumRollupQuerySpec(
    "COALESCE(SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors'), 0) AS errors_count",
    table_placeholder="{error_agg_table}",
).build_sql()

RUM_ROLLUP_COUNTS_TOTAL_BEACONS = _RumRollupQuerySpec(
    "COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'total_beacons'), 0) AS total_beacons"
).build_sql()

RUM_ROLLUP_VITALS_SUMMARY = _RumRollupQuerySpec(
    "value AS metric_name,\n"
    "    CASE WHEN SUM(event_count) > 0 THEN SUM(p75_value * event_count) / SUM(event_count) ELSE 0.0 END AS p75,\n"
    "    SUM(event_count) AS total_count,\n"
    "    SUM(good_count) AS good_count,\n"
    "    SUM(ni_count) AS ni_count,\n"
    "    SUM(poor_count) AS poor_count",
    dimension_filter="metric_name",
    group_by="value",
).build_sql()

RUM_ROLLUP_BROWSERS = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="browser", group_by="value"
).build_sql()

RUM_ROLLUP_OS = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="os", group_by="value"
).build_sql()

RUM_ROLLUP_DEVICES = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="device", group_by="value"
).build_sql()

RUM_ROLLUP_COUNTS_VITALS_AND_BEACONS = _RumRollupQuerySpec(
    "COALESCE(SUM(event_count) FILTER (WHERE value = 'pageviews'), 0) AS pageviews,\n"
    "    COALESCE(SUM(event_count) FILTER (WHERE value = 'interactions'), 0) AS interactions,\n"
    "    COALESCE(SUM(event_count) FILTER (WHERE value = 'total_beacons'), 0) AS total_beacons",
    dimension_filter="total",
).build_sql()

RUM_ROLLUP_ENVIRONMENTS = """
SELECT dimension, value, SUM(event_count) AS count
FROM {vitals_agg_table}
WHERE service_id = ?
  AND dimension IN ('browser', 'os', 'device')
  AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
GROUP BY dimension, value
"""

RUM_ROLLUP_WORST_PAGES = """
WITH page_metrics AS (
    SELECT
        value AS path,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'pathname'), 0) AS views,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_load') > 0 THEN SUM(value_sum) FILTER (WHERE dimension = 'path_metric_load') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_load') ELSE 0.0 END AS avg_load_time,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_lcp') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'path_metric_lcp') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_lcp') ELSE 0.0 END AS lcp,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_cls') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'path_metric_cls') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_cls') ELSE 0.0 END AS cls
    FROM {vitals_agg_table}
    WHERE service_id = ?
      AND dimension IN ('pathname', 'path_metric_load', 'path_metric_lcp', 'path_metric_cls')
      AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY value
),
page_errors AS (
    SELECT value AS path, SUM(error_count) AS error_count
    FROM {error_agg_table}
    WHERE service_id = ? AND dimension = 'pathname' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY value
)
SELECT
    m.path, m.views, m.avg_load_time, m.lcp, m.cls,
    COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
FROM page_metrics m
LEFT JOIN page_errors e ON m.path = e.path
ORDER BY error_rate DESC, m.avg_load_time DESC
LIMIT 5
"""

RUM_ROLLUP_TOP_EXCEPTIONS = _RumRollupQuerySpec(
    "value AS serialized, SUM(error_count) AS count",
    table_placeholder="{error_agg_table}",
    dimension_filter="exception",
    group_by="value",
    order_by="count DESC",
    limit=3,
).build_sql()

RUM_ROLLUP_TRENDS = """
WITH hourly_vitals AS (
    SELECT
        bucket_start AS hour,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') / SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') ELSE 0.0 END AS lcp,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') / SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') ELSE 0.0 END AS cls,
        SUM(event_count) FILTER (WHERE dimension = 'pathname') AS views,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'pageviews'), 0) AS pageviews,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'interactions'), 0) AS interactions
    FROM {vitals_agg_table}
    WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY hour
),
hourly_errors AS (
    SELECT bucket_start AS hour, SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors') AS errors
    FROM {error_agg_table}
    WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY hour
)
SELECT
    COALESCE(v.hour, e.hour) AS hour_ts, v.lcp, v.cls,
    COALESCE(v.pageviews, 0) AS pageviews,
    COALESCE(v.interactions, 0) AS interactions,
    COALESCE(e.errors, 0) AS errors,
    COALESCE(e.errors, 0) * 100.0 / NULLIF(COALESCE(v.views, 0) + COALESCE(e.errors, 0), 0) AS error_rate
FROM hourly_vitals v
FULL OUTER JOIN hourly_errors e ON v.hour = e.hour
ORDER BY hour_ts DESC
"""

RUM_CONSOLIDATED_COUNTS = """
SELECT
    COUNT(DISTINCT CASE WHEN src = 'vitals' AND metric_name NOT LIKE 'event_%' THEN distinct_id END) AS pageviews,
    COUNT(DISTINCT CASE WHEN src = 'vitals' AND metric_name LIKE 'event_%' THEN distinct_id END) AS interactions,
    COUNT(DISTINCT CASE WHEN src = 'errors' THEN distinct_id END) AS errors_count,
    COUNT(DISTINCT distinct_id) AS total_beacons
FROM (
    SELECT 'vitals' AS src, metric_name, {distinct_id} AS distinct_id FROM t_client_vitals
    UNION ALL
    SELECT 'errors' AS src, CAST(NULL AS VARCHAR) AS metric_name, {distinct_id} AS distinct_id FROM t_client_errors
)
"""  # str.format template: {where_sql}, {distinct_id}

RUM_VITALS_SUMMARY = """
SELECT
    metric_name,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS p75,
    COUNT(*) AS total_count,
    COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
    COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
    COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count
FROM t_client_vitals
GROUP BY metric_name
"""

RUM_ENVIRONMENTS_CONSOLIDATED = """
SELECT
    COALESCE(browser, 'Unknown') AS browser,
    COALESCE(os, 'Unknown') AS os,
    COALESCE(device, 'Unknown') AS device,
    COUNT(DISTINCT {distinct_id}) AS count,
    grouping(browser) AS g_browser,
    grouping(os) AS g_os,
    grouping(device) AS g_device
FROM t_client_vitals
GROUP BY GROUPING SETS ((browser), (os), (device))
"""  # str.format template: {where_sql}, {distinct_id}

RUM_WORST_PAGES = """
WITH page_metrics AS (
    SELECT
        pathname AS path,
        COUNT(DISTINCT {distinct_id}) AS views,
        COALESCE(AVG(metric_value) FILTER (WHERE metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')), 0.0) AS avg_load_time,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls
    FROM t_client_vitals
    WHERE pathname IS NOT NULL
    GROUP BY pathname
),
page_errors AS (
    SELECT pathname AS path, COUNT(DISTINCT {distinct_id}) AS error_count
    FROM t_client_errors
    WHERE pathname IS NOT NULL
    GROUP BY pathname
)
SELECT
    m.path, m.views, m.avg_load_time, m.lcp, m.cls,
    COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
FROM page_metrics m
LEFT JOIN page_errors e ON m.path = e.path
ORDER BY error_rate DESC, m.avg_load_time DESC
LIMIT 5
"""  # str.format template: {where_sql}, {distinct_id}

RUM_TOP_EXCEPTIONS = """
SELECT error_message, error_file, error_line, error_col, COUNT(*) AS count
FROM t_client_errors
GROUP BY error_message, error_file, error_line, error_col
ORDER BY count DESC
LIMIT 3
"""

RUM_HOURLY_TRENDS = """
WITH hourly_vitals AS (
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls,
        COUNT(DISTINCT {distinct_id}) AS views,
        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name NOT LIKE 'event_%') AS pageviews,
        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name LIKE 'event_%') AS interactions
    FROM t_client_vitals
    GROUP BY hour
),
hourly_errors AS (
    SELECT DATE_TRUNC('hour', timestamp) AS hour, COUNT(DISTINCT {distinct_id}) AS errors
    FROM t_client_errors
    GROUP BY hour
)
SELECT
    COALESCE(v.hour, e.hour) AS hour_ts, v.lcp, v.cls,
    COALESCE(v.pageviews, 0) AS pageviews,
    COALESCE(v.interactions, 0) AS interactions,
    COALESCE(e.errors, 0) AS errors,
    COALESCE(e.errors, 0) * 100.0 / NULLIF(COALESCE(v.views, 0) + COALESCE(e.errors, 0), 0) AS error_rate
FROM hourly_vitals v
FULL OUTER JOIN hourly_errors e ON v.hour = e.hour
ORDER BY hour_ts DESC
"""  # str.format template: {where_sql}, {distinct_id}

RUM_ANY_DATA = """
SELECT CASE WHEN EXISTS (SELECT 1 FROM client_vitals) OR EXISTS (SELECT 1 FROM client_errors) THEN 1 ELSE 0 END
"""

RUM_LIVE_EVENTS = """
WITH vitals_recent AS (
    SELECT timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, city, region, country, pop, tls, ttfb
    FROM client_vitals
    WHERE {where_sql}
    ORDER BY timestamp DESC
    LIMIT 250
),
vitals_base AS (
    SELECT 'pageview' AS type, timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, CAST(NULL AS VARCHAR) AS error_message, city, region, country, pop, tls, ttfb
    FROM vitals_recent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) = 1
),
errors_recent AS (
    SELECT timestamp, pathname, browser, os, device, cid, req_id, error_message, error_file, error_line, city, region, country, pop, tls, ttfb
    FROM client_errors
    WHERE {where_sql}
    ORDER BY timestamp DESC
    LIMIT 250
),
errors_base AS (
    SELECT 'error' AS type, timestamp, pathname, CAST(NULL AS VARCHAR) AS metric_name, CAST(NULL AS DOUBLE) AS metric_value, CAST(NULL AS VARCHAR) AS metric_rating, browser, os, device, cid, req_id, error_message, city, region, country, pop, tls, ttfb
    FROM errors_recent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, error_message, error_file, error_line, pathname, cid, req_id ORDER BY timestamp) = 1
),
combined AS (
    SELECT * FROM vitals_base
    UNION ALL
    SELECT * FROM errors_base
)
SELECT type, timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, error_message, city, region, country, pop, tls, ttfb
FROM combined
ORDER BY timestamp DESC
LIMIT 50
"""  # str.format template: {where_sql}

HEADER_STATS = (
    f"SELECT COUNT(DISTINCT {DISTINCT_ID}), MAX(timestamp) FROM "
    "(SELECT req_id, cid, timestamp FROM client_vitals UNION ALL SELECT req_id, cid, timestamp FROM client_errors)"
)

AGGREGATE_BUCKET_COUNT = "SELECT COUNT(DISTINCT bucket_start) FROM rum_vitals_aggregates WHERE service_id = ?"

CLIENT_VITALS_SCHEMA_PROBE = "SELECT * FROM client_vitals LIMIT 0"

_READ_PARQUET_LIST_RE = re.compile(r"read_parquet\(\s*(?:\[|(?:main\.)?list_value\()(.*?)(?:\]|\))", re.DOTALL)
_QUOTED_PATH_RE = re.compile(r"'([^']+)'")


def _prune_missing_files_from_rum_views(con) -> None:
    """Fallback view self-heal when ``source`` was not passed to a repository helper."""
    try:
        rows = con.execute(
            "SELECT view_name, sql, temporary FROM duckdb_views() WHERE view_name IN ('client_vitals', 'client_errors')"
        ).fetchall()
    except Exception:
        return
    for view_name, view_sql, is_temp in rows:
        if not view_sql or "read_parquet(" not in view_sql:
            continue

        def _replace_list(match: re.Match[str]) -> str:
            inner = match.group(1)
            paths = _QUOTED_PATH_RE.findall(inner)
            surviving = [p for p in paths if os.path.exists(p)]
            if not surviving and paths:
                buf_dir = os.path.dirname(paths[0])
                if os.path.isdir(buf_dir):
                    surviving = sorted(
                        os.path.join(buf_dir, f)
                        for f in os.listdir(buf_dir)
                        if f.startswith("batch_") and f.endswith(".parquet")
                    )
            if not surviving:
                return match.group(0)
            joined = ", ".join(f"'{p}'" for p in surviving)
            return f"read_parquet([{joined}]"

        new_sql = _READ_PARQUET_LIST_RE.sub(_replace_list, view_sql)
        if new_sql != view_sql:
            stmt = new_sql.strip().rstrip(";")
            if stmt.upper().startswith("CREATE VIEW "):
                prefix = "CREATE OR REPLACE TEMP VIEW " if is_temp else "CREATE OR REPLACE VIEW "
                stmt = prefix + stmt[len("CREATE VIEW ") :]
            elif stmt.upper().startswith("CREATE TEMP VIEW "):
                stmt = "CREATE OR REPLACE TEMP VIEW " + stmt[len("CREATE TEMP VIEW ") :]
            try:
                con.execute(stmt)
            except Exception:
                logger.debug("_prune_missing_files_from_rum_views: failed to rebind %s", view_name, exc_info=True)


def _run_with_rum_view_retry(con, source: dict | None, fn):
    """Run ``fn()`` with bounded self-heal retry on stale RUM buffer file errors."""

    def _recover(_exc: BaseException) -> None:
        if source is not None:
            from backend.core.storage import _core as storage_view

            storage_view.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
            storage_view.update_storage_view(con, source, force=True)
        _prune_missing_files_from_rum_views(con)

    return retry_call(fn, retry_on=is_stale_view_error, attempts=3, before_retry=_recover)


def get_beacon_health(con, *, source: dict | None = None) -> tuple[int, datetime | None]:
    """``(beacons in the last hour, newest beacon timestamp)`` across vitals + errors."""

    def _run() -> tuple[int, datetime | None]:
        one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
        with track_query(con, sql_rum.BEACON_HEALTH_RECENT, [one_hour_ago], "rum_health_recent") as cur_recent:
            recent_count = cur_recent.fetchone()[0] or 0
        with track_query(con, sql_rum.BEACON_HEALTH_LAST, [], "rum_health_last") as cur_last:
            last_dt = cur_last.fetchone()[0]
        return recent_count, last_dt

    return _run_with_rum_view_retry(con, source, _run)


def fetch_rum_analytics(
    con,
    *,
    service_id: str,
    start_time: str,
    end_time: str,
    parsed_filters: dict,
    where_sql: str,
    params: list,
    raw_window_max_s: int,
    source: dict | None = None,
) -> dict[str, Any]:
    """Raw aggregate rows for ``/rum/analytics`` (rollup tables when eligible, else raw scan)."""
    return _run_with_rum_view_retry(
        con,
        source,
        lambda: _fetch_rum_analytics_once(
            con,
            service_id=service_id,
            start_time=start_time,
            end_time=end_time,
            parsed_filters=parsed_filters,
            where_sql=where_sql,
            params=params,
            raw_window_max_s=raw_window_max_s,
        ),
    )


def _fetch_rum_analytics_once(
    con,
    *,
    service_id: str,
    start_time: str,
    end_time: str,
    parsed_filters: dict,
    where_sql: str,
    params: list,
    raw_window_max_s: int,
) -> dict[str, Any]:
    from backend.core.rollups.recompute import table_exists

    use_rollup = False
    prefer_raw_window = False
    try:
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st and et and (et - st).total_seconds() <= raw_window_max_s:
            prefer_raw_window = True
    except Exception:
        logger.debug("fetch_rum_analytics: step failed (ignored)", exc_info=True)

    if not parsed_filters and not prefer_raw_window:
        if table_exists(con, "rum_vitals_aggregates") and table_exists(con, "rum_error_aggregates"):
            use_rollup = True

    if use_rollup:
        vitals_agg_table = "rum_vitals_aggregates"
        error_agg_table = "rum_error_aggregates"
        try:
            with track_query(
                con, sql_rum.RUM_ROLLUP_MAX_BUCKET, [service_id, service_id], "rum_rollup_max_bucket"
            ) as cur_max:
                row_max = cur_max.fetchone()
                rollup_max_bucket = row_max[0] if row_max else None

            if rollup_max_bucket is None:
                use_rollup = False
            elif table_exists(con, "client_vitals") and table_exists(con, "client_errors"):
                with track_query(
                    con,
                    sql_rum.RUM_TAIL_VITALS_CREATE,
                    [rollup_max_bucket, start_time, end_time],
                    "rum_tail_vitals_create",
                ):
                    pass
                with track_query(
                    con,
                    sql_rum.RUM_TAIL_ERRORS_CREATE,
                    [rollup_max_bucket, start_time, end_time],
                    "rum_tail_errors_create",
                ):
                    pass
                with track_query(con, sql_rum.RUM_TAIL_HAS_ROWS, [], "rum_tail_has_rows") as cur_tail:
                    tail_rows = (cur_tail.fetchone() or (0,))[0] or 0

                if tail_rows > 0:
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_VITALS_AGG_CREATE.format(distinct_id="distinct_id"),
                        [service_id, start_time, end_time] + [service_id] * 10,
                        "rum_eff_vitals_agg_create",
                    ):
                        pass
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_ERROR_AGG_CREATE.format(distinct_id="distinct_id"),
                        [service_id, start_time, end_time] + [service_id] * 3,
                        "rum_eff_error_agg_create",
                    ):
                        pass
                    vitals_agg_table = "t_eff_vitals_agg"
                    error_agg_table = "t_eff_error_agg"

            if use_rollup:
                logger.info("[rum_rollups] %s: Querying from precomputed RUM aggregates...", service_id)
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_VITALS_AND_BEACONS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_vitals",
                ) as cur_v:
                    pageviews, interactions, total_beacons = cur_v.fetchone()
                    pageviews = pageviews or 0
                    interactions = interactions or 0
                    total_beacons = total_beacons or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_ERRORS.format(error_agg_table=error_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_errors",
                ) as cur_e:
                    errors_count = cur_e.fetchone()[0] or 0

                if total_beacons == 0 and errors_count == 0:
                    with track_query(
                        con,
                        sql_rum.RUM_ROLLUP_ANY_DATA.format(
                            vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                        ),
                        [service_id, start_time, end_time, service_id, start_time, end_time],
                        "rum_rollup_any_data",
                    ) as cur_any:
                        if cur_any.fetchone()[0] == 0:
                            if not table_exists(con, "client_vitals") or not table_exists(con, "client_errors"):
                                return {"no_data": True}
                            with track_query(con, sql_rum.RUM_ANY_DATA, [], "rum_any_data") as cur_raw_any:
                                if not cur_raw_any.fetchone()[0]:
                                    return {"no_data": True}
                            use_rollup = False

            if use_rollup:
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_VITALS_SUMMARY.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_vitals_summary",
                ) as cur_vitals:
                    vitals_rows = cur_vitals.fetchall()

                browsers: dict[str, Any] = {}
                os_dict: dict[str, Any] = {}
                devices: dict[str, Any] = {}
                env_targets = {"browser": browsers, "os": os_dict, "device": devices}
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_ENVIRONMENTS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_environments",
                ) as cur_dim:
                    for dim, val, count in cur_dim.fetchall():
                        target = env_targets.get(dim)
                        if target is not None:
                            target[val] = count

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_WORST_PAGES.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_worst_pages",
                ) as cur_pages:
                    worst_pages_rows = cur_pages.fetchall()

                errors_rows = []
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_TOP_EXCEPTIONS.format(error_agg_table=error_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_top_exceptions",
                ) as cur_errors:
                    for row in cur_errors.fetchall():
                        serialized, count = row
                        parts = serialized.split("|")
                        if len(parts) >= 4:
                            msg, file, line, col = parts[0], parts[1], int(parts[2] or 0), int(parts[3] or 0)
                            errors_rows.append((msg, file, line, col, count))
                        else:
                            errors_rows.append((serialized, "Unknown", 0, 0, count))

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_TRENDS.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_trends",
                ) as cur_trends:
                    trends_rows = cur_trends.fetchall()

                return {
                    "no_data": False,
                    "total_beacons": total_beacons,
                    "pageviews": pageviews,
                    "interactions": interactions,
                    "errors_count": errors_count,
                    "vitals_rows": vitals_rows,
                    "browsers": browsers,
                    "os": os_dict,
                    "devices": devices,
                    "worst_pages_rows": worst_pages_rows,
                    "errors_rows": errors_rows,
                    "trends_rows": trends_rows,
                    "_approx": True,
                }
        finally:
            con.execute("DROP TABLE IF EXISTS t_eff_vitals_agg")
            con.execute("DROP TABLE IF EXISTS t_eff_error_agg")
            con.execute("DROP TABLE IF EXISTS t_tail_vitals")
            con.execute("DROP TABLE IF EXISTS t_tail_errors")

    try:
        with track_query(
            con, sql_rum.RUM_TEMP_VITALS_CREATE.format(where_sql=where_sql), params, "rum_temp_vitals_create"
        ):
            pass
        with track_query(
            con, sql_rum.RUM_TEMP_ERRORS_CREATE.format(where_sql=where_sql), params, "rum_temp_errors_create"
        ):
            pass

        with track_query(
            con,
            sql_rum.RUM_CONSOLIDATED_COUNTS.format(distinct_id="distinct_id"),
            [],
            "rum_consolidated_counts",
        ) as cur_counts:
            pageviews, interactions, errors_count, total_beacons = cur_counts.fetchone()
            pageviews = pageviews or 0
            interactions = interactions or 0
            errors_count = errors_count or 0
            total_beacons = total_beacons or 0

        if total_beacons == 0:
            with track_query(con, sql_rum.RUM_ANY_DATA, [], "rum_any_data") as cur_any:
                if not cur_any.fetchone()[0]:
                    return {"no_data": True}

        with track_query(con, sql_rum.RUM_VITALS_SUMMARY, [], "rum_vitals_summary") as cur_vitals:
            vitals_rows = cur_vitals.fetchall()

        browsers = {}
        os_dict = {}
        devices = {}

        with track_query(
            con,
            sql_rum.RUM_ENVIRONMENTS_CONSOLIDATED.format(distinct_id="distinct_id"),
            [],
            "rum_environments_consolidated",
        ) as cur_envs:
            for r_browser, r_os, r_device, count, g_browser, g_os, g_device in cur_envs.fetchall():
                if g_browser == 0 and r_browser:
                    browsers[r_browser] = count
                elif g_os == 0 and r_os:
                    os_dict[r_os] = count
                elif g_device == 0 and r_device:
                    devices[r_device] = count

        with track_query(
            con, sql_rum.RUM_WORST_PAGES.format(distinct_id="distinct_id"), [], "rum_worst_pages"
        ) as cur_pages:
            worst_pages_rows = cur_pages.fetchall()

        with track_query(con, sql_rum.RUM_TOP_EXCEPTIONS, [], "rum_top_exceptions") as cur_errors:
            errors_rows = cur_errors.fetchall()

        with track_query(
            con, sql_rum.RUM_HOURLY_TRENDS.format(distinct_id="distinct_id"), [], "rum_hourly_trends"
        ) as cur_trends:
            trends_rows = cur_trends.fetchall()

        return {
            "no_data": False,
            "total_beacons": total_beacons,
            "pageviews": pageviews,
            "interactions": interactions,
            "errors_count": errors_count,
            "vitals_rows": vitals_rows,
            "browsers": browsers,
            "os": os_dict,
            "devices": devices,
            "worst_pages_rows": worst_pages_rows,
            "errors_rows": errors_rows,
            "trends_rows": trends_rows,
        }
    finally:
        con.execute("DROP TABLE IF EXISTS t_client_vitals")
        con.execute("DROP TABLE IF EXISTS t_client_errors")


def fetch_rum_live_events(con, *, where_sql: str, params: list, source: dict | None = None) -> list[tuple]:
    """Newest 50 vitals + error beacons for the live ticker, newest first."""

    def _run() -> list[tuple]:
        with track_query(
            con, sql_rum.RUM_LIVE_EVENTS.format(where_sql=where_sql), params * 2, "rum_live_events"
        ) as cur:
            return cur.fetchall()

    return _run_with_rum_view_retry(con, source, _run)


def get_header_stats(con, *, source: dict | None = None) -> tuple[int, Any]:
    """``(distinct beacon count, newest beacon timestamp)`` over vitals + errors for the header badge (one scan)."""

    def _run() -> tuple[int, Any]:
        row = con.execute(sql_rum.HEADER_STATS).fetchone()
        return (row[0] or 0, row[1]) if row else (0, None)

    return _run_with_rum_view_retry(con, source, _run)


def count_aggregate_buckets(con, service_id: str) -> int | None:
    """Distinct ``bucket_start`` values in ``rum_vitals_aggregates`` for the service."""
    row = con.execute(sql_rum.AGGREGATE_BUCKET_COUNT, [service_id]).fetchone()
    return row[0] if row is not None else None


def get_client_vitals_columns(con) -> list[str] | None:
    """Return column names of ``client_vitals`` (or ``None`` if unavailable)."""
    try:
        desc = con.execute(sql_rum.CLIENT_VITALS_SCHEMA_PROBE).description
        if desc:
            return [d[0] for d in desc]
    except Exception:
        logger.debug("client_vitals schema probe failed", exc_info=True)
    return None
