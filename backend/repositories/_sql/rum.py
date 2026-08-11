"""SQL templates for `backend.repositories.rum`."""

from __future__ import annotations

WEB_VITALS_SUMMARY = """
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    metric_name,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS p75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95,
    COUNT(*) AS count
FROM client_vitals
WHERE timestamp >= ? AND timestamp < ?
    AND metric_name IN ('LCP', 'INP', 'CLS', 'TTFB', 'FCP')
GROUP BY hour, metric_name
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
    COUNT(*) FILTER (WHERE metric_name = 'LCP' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'LCP'), 0) AS lcp_poor_pct,
    COUNT(*) FILTER (WHERE metric_name = 'INP' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'INP'), 0) AS inp_poor_pct,
    COUNT(*) FILTER (WHERE metric_name = 'CLS' AND metric_rating = 'poor') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'CLS'), 0) AS cls_poor_pct,
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
