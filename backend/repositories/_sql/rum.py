"""SQL templates for `backend.repositories.rum`."""

from __future__ import annotations

WEB_VITALS_SUMMARY = """
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    UPPER(metric_name) AS metric_name,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS p75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95,
    COUNT(*) AS count
FROM client_vitals
WHERE timestamp >= ? AND timestamp < ?
    AND UPPER(metric_name) IN ('LCP', 'INP', 'CLS', 'TTFB', 'FCP')
GROUP BY hour, UPPER(metric_name)
ORDER BY hour DESC, metric_name
"""

ERROR_RATE_TREND = """
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    COUNT(DISTINCT req_id) FILTER (WHERE error_message IS NOT NULL) AS error_count,
    COUNT(DISTINCT req_id) AS total_requests,
    COUNT(DISTINCT req_id) FILTER (WHERE error_message IS NOT NULL) * 100.0 / NULLIF(COUNT(DISTINCT req_id), 0) AS error_rate_pct
FROM (
    SELECT timestamp, req_id, error_message FROM client_errors
    UNION ALL
    SELECT timestamp, req_id, CAST(NULL AS VARCHAR) AS error_message FROM client_vitals
)
WHERE timestamp >= ? AND timestamp < ?
GROUP BY hour
ORDER BY hour DESC
"""

WORST_PAGES = """
SELECT
    pathname,
    COUNT(*) FILTER (WHERE UPPER(metric_name) = 'LCP' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE UPPER(metric_name) = 'LCP'), 0) AS lcp_poor_pct,
    COUNT(*) FILTER (WHERE UPPER(metric_name) = 'INP' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE UPPER(metric_name) = 'INP'), 0) AS inp_poor_pct,
    COUNT(*) FILTER (WHERE UPPER(metric_name) = 'CLS' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE UPPER(metric_name) = 'CLS'), 0) AS cls_poor_pct,
    COUNT(DISTINCT cid) AS session_count
FROM client_vitals
WHERE timestamp >= ? AND timestamp < ?
    AND pathname IS NOT NULL
GROUP BY pathname
ORDER BY COALESCE(lcp_poor_pct, 0) + COALESCE(inp_poor_pct, 0) + COALESCE(cls_poor_pct, 0) DESC
LIMIT ?
"""

WORST_SESSIONS = """
SELECT
    cid,
    COUNT(DISTINCT pathname) AS page_count,
    COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_metrics,
    (SELECT COUNT(*) FROM client_errors WHERE client_errors.cid = client_vitals.cid) AS error_count,
    MAX(timestamp) AS last_seen
FROM client_vitals
WHERE timestamp >= ? AND timestamp < ?
    AND cid IS NOT NULL
GROUP BY cid
HAVING COUNT(*) > 5  -- At least 5 metrics per session
ORDER BY poor_metrics DESC, error_count DESC
LIMIT ?
"""

BEACON_HEALTH_RECENT = (
    "SELECT COUNT(*) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors) "
    "WHERE timestamp > ?"
)

BEACON_HEALTH_LAST = (
    "SELECT MAX(timestamp) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors)"
)

DISTINCT_ID = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

# --- RUM analytics / live-events (moved verbatim from routers/rum.py) ---

RUM_TEMP_VITALS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_vitals AS "
    "SELECT timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname "
    "FROM client_vitals WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_TEMP_ERRORS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_errors AS "
    "SELECT timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col "
    "FROM client_errors WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_ROLLUP_MAX_BUCKET = """
SELECT MAX(bucket_start) FROM (
    SELECT MAX(bucket_start) AS bucket_start FROM rum_vitals_aggregates WHERE service_id = ?
    UNION ALL
    SELECT MAX(bucket_start) AS bucket_start FROM rum_error_aggregates WHERE service_id = ?
)
"""

RUM_TAIL_VITALS_CREATE = """
CREATE OR REPLACE TEMP TABLE t_tail_vitals AS
SELECT timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname
FROM (
    SELECT
        timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname,
        ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) AS _rn
    FROM client_vitals
    WHERE timestamp >= ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
)
WHERE _rn = 1
"""

RUM_TAIL_ERRORS_CREATE = """
CREATE OR REPLACE TEMP TABLE t_tail_errors AS
SELECT timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col
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

RUM_EFF_VITALS_AGG_CREATE = """
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

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'total' AS dimension,
    CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END AS value,
    COUNT(DISTINCT {distinct_id}) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
GROUP BY bucket_start, value

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'metric_name' AS dimension,
    metric_name AS value,
    COUNT(*) AS event_count,
    CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
    COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
    COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
    COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count,
    CAST(MEDIAN(metric_value) AS DOUBLE) AS p50_value,
    CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
    CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p99_value
FROM t_tail_vitals
GROUP BY bucket_start, metric_name

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'browser' AS dimension,
    IFNULL(browser, 'Unknown') AS value,
    COUNT(DISTINCT {distinct_id}) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
GROUP BY bucket_start, browser

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'os' AS dimension,
    IFNULL(os, 'Unknown') AS value,
    COUNT(DISTINCT {distinct_id}) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
GROUP BY bucket_start, os

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'device' AS dimension,
    IFNULL(device, 'Unknown') AS value,
    COUNT(DISTINCT {distinct_id}) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
GROUP BY bucket_start, device

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'pathname' AS dimension,
    pathname AS value,
    COUNT(DISTINCT {distinct_id}) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
WHERE pathname IS NOT NULL
GROUP BY bucket_start, pathname

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'path_metric_load' AS dimension,
    pathname AS value,
    COUNT(*) AS event_count,
    CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE) AS value_sum,
    0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM t_tail_vitals
WHERE pathname IS NOT NULL
  AND metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')
GROUP BY bucket_start, pathname

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'path_metric_lcp' AS dimension,
    pathname AS value,
    COUNT(*) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value,
    CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
    0.0 AS p99_value
FROM t_tail_vitals
WHERE pathname IS NOT NULL
  AND metric_name IN ('LCP', 'lcp')
GROUP BY bucket_start, pathname

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'path_metric_cls' AS dimension,
    pathname AS value,
    COUNT(*) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value,
    CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE) AS p75_value,
    0.0 AS p99_value
FROM t_tail_vitals
WHERE pathname IS NOT NULL
  AND metric_name IN ('CLS', 'cls')
GROUP BY bucket_start, pathname

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'total' AS dimension,
    'total_beacons' AS value,
    COUNT(DISTINCT distinct_id) AS event_count,
    0.0 AS value_sum, 0 AS good_count, 0 AS ni_count, 0 AS poor_count,
    0.0 AS p50_value, 0.0 AS p75_value, 0.0 AS p99_value
FROM (
    SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_vitals
    UNION ALL
    SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_errors
)
GROUP BY bucket_start
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
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'total' AS dimension,
    'errors' AS value,
    COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors
GROUP BY bucket_start

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'pathname' AS dimension,
    pathname AS value,
    COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors
WHERE pathname IS NOT NULL
GROUP BY bucket_start, pathname

UNION ALL

SELECT
    ? AS service_id,
    DATE_TRUNC('hour', timestamp) AS bucket_start,
    'exception' AS dimension,
    concat(IFNULL(error_message, ''), '|', IFNULL(error_file, ''), '|', CAST(IFNULL(error_line, 0) AS VARCHAR), '|', CAST(IFNULL(error_col, 0) AS VARCHAR)) AS value,
    COUNT(*) AS error_count
FROM t_tail_errors
GROUP BY bucket_start, value
"""

RUM_ROLLUP_ANY_DATA = "SELECT (SELECT COUNT(*) FROM {vitals_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)) + (SELECT COUNT(*) FROM {error_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ))"

RUM_ROLLUP_COUNTS_VITALS = """
            SELECT
                COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'pageviews'), 0) AS pageviews,
                COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'interactions'), 0) AS interactions
            FROM {vitals_agg_table}
            WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            """

RUM_ROLLUP_COUNTS_ERRORS = """
            SELECT
                COALESCE(SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors'), 0) AS errors_count
            FROM {error_agg_table}
            WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            """

RUM_ROLLUP_COUNTS_TOTAL_BEACONS = """
            SELECT
                COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'total_beacons'), 0) AS total_beacons
            FROM {vitals_agg_table}
            WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            """

RUM_ROLLUP_VITALS_SUMMARY = """
            SELECT
                value AS metric_name,
                CASE WHEN SUM(event_count) > 0 THEN SUM(p75_value * event_count) / SUM(event_count) ELSE 0.0 END AS p75,
                SUM(event_count) AS total_count,
                SUM(good_count) AS good_count,
                SUM(ni_count) AS ni_count,
                SUM(poor_count) AS poor_count
            FROM {vitals_agg_table}
            WHERE service_id = ? AND dimension = 'metric_name' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            GROUP BY value
            """

RUM_ROLLUP_BROWSERS = """
            SELECT value, SUM(event_count) AS count
            FROM {vitals_agg_table}
            WHERE service_id = ? AND dimension = 'browser' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            GROUP BY value
            """

RUM_ROLLUP_OS = """
            SELECT value, SUM(event_count) AS count
            FROM {vitals_agg_table}
            WHERE service_id = ? AND dimension = 'os' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            GROUP BY value
            """

RUM_ROLLUP_DEVICES = """
            SELECT value, SUM(event_count) AS count
            FROM {vitals_agg_table}
            WHERE service_id = ? AND dimension = 'device' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            GROUP BY value
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
                SELECT
                    value AS path,
                    SUM(error_count) AS error_count
                FROM {error_agg_table}
                WHERE service_id = ? AND dimension = 'pathname' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
                GROUP BY value
            )
            SELECT
                m.path,
                m.views,
                m.avg_load_time,
                m.lcp,
                m.cls,
                COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
            FROM page_metrics m
            LEFT JOIN page_errors e ON m.path = e.path
            ORDER BY error_rate DESC, m.avg_load_time DESC
            LIMIT 5
            """

RUM_ROLLUP_TOP_EXCEPTIONS = """
            SELECT
                value AS serialized,
                SUM(error_count) AS count
            FROM {error_agg_table}
            WHERE service_id = ? AND dimension = 'exception' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
            GROUP BY value
            ORDER BY count DESC
            LIMIT 3
            """

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
                SELECT
                    bucket_start AS hour,
                    SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors') AS errors
                FROM {error_agg_table}
                WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
                GROUP BY hour
            )
            SELECT
                COALESCE(v.hour, e.hour) AS hour_ts,
                v.lcp,
                v.cls,
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
                SELECT 'vitals' AS src, metric_name, {distinct_id} AS distinct_id
                FROM t_client_vitals
                UNION ALL
                SELECT 'errors' AS src, CAST(NULL AS VARCHAR) AS metric_name, {distinct_id} AS distinct_id
                FROM t_client_errors
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
            GROUP BY GROUPING SETS (
                (browser),
                (os),
                (device)
            )
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
                SELECT
                    pathname AS path,
                    COUNT(DISTINCT {distinct_id}) AS error_count
                FROM t_client_errors
                WHERE pathname IS NOT NULL
                GROUP BY pathname
            )
            SELECT
                m.path,
                m.views,
                m.avg_load_time,
                m.lcp,
                m.cls,
                COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
            FROM page_metrics m
            LEFT JOIN page_errors e ON m.path = e.path
            ORDER BY error_rate DESC, m.avg_load_time DESC
            LIMIT 5
            """  # str.format template: {where_sql}, {distinct_id}

RUM_TOP_EXCEPTIONS = """
            SELECT
                error_message,
                error_file,
                error_line,
                error_col,
                COUNT(*) AS count
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
                SELECT
                    DATE_TRUNC('hour', timestamp) AS hour,
                    COUNT(DISTINCT {distinct_id}) AS errors
                FROM t_client_errors
                GROUP BY hour
            )
            SELECT
                COALESCE(v.hour, e.hour) AS hour_ts,
                v.lcp,
                v.cls,
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
WITH vitals_base AS (
    SELECT
        'pageview' AS type,
        timestamp,
        pathname,
        metric_name,
        metric_value,
        metric_rating,
        browser,
        os,
        device,
        cid,
        req_id,
        CAST(NULL AS VARCHAR) AS error_message,
        city,
        region,
        country,
        pop,
        tls,
        ttfb
    FROM client_vitals
    WHERE {where_sql}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) = 1
),
errors_base AS (
    SELECT
        'error' AS type,
        timestamp,
        pathname,
        CAST(NULL AS VARCHAR) AS metric_name,
        CAST(NULL AS DOUBLE) AS metric_value,
        CAST(NULL AS VARCHAR) AS metric_rating,
        browser,
        os,
        device,
        cid,
        req_id,
        error_message,
        city,
        region,
        country,
        pop,
        tls,
        ttfb
    FROM client_errors
    WHERE {where_sql}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, error_message, error_file, error_line, pathname, cid, req_id ORDER BY timestamp) = 1
),
combined AS (
    SELECT * FROM vitals_base
    UNION ALL
    SELECT * FROM errors_base
)
SELECT
    type,
    timestamp,
    pathname,
    metric_name,
    metric_value,
    metric_rating,
    browser,
    os,
    device,
    cid,
    req_id,
    error_message,
    city,
    region,
    country,
    pop,
    tls,
    ttfb
FROM combined
ORDER BY timestamp DESC
LIMIT 50
"""  # str.format template: {where_sql}

HEADER_STATS = (
    f"SELECT COUNT(DISTINCT {DISTINCT_ID}), MAX(timestamp) FROM "
    "(SELECT req_id, cid, timestamp FROM client_vitals UNION ALL SELECT req_id, cid, timestamp FROM client_errors)"
)

AGGREGATE_BUCKET_COUNT = "SELECT COUNT(DISTINCT bucket_start) FROM rum_vitals_aggregates WHERE service_id = ?"
