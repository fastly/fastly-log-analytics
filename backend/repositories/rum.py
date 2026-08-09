"""RUM repository — query layer for client_vitals and client_errors tables.

Provides analytics queries for Web Vitals trends, error rates, and session correlation.
All queries go through QueryRunner to centralize DuckDB access + caching.
"""

from __future__ import annotations

from datetime import datetime

from backend.repositories._base import QueryRunner


def get_web_vitals_summary(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """Get Core Web Vitals summary (P50/P75/P95 over time window).

    Returns hourly aggregates: [timestamp, metric_name, p50, p75, p95]
    Stub for Phase 2 — full percentile logic implemented after schema is live.
    """
    sql = """
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
    return runner.execute(sql, [since, until]).fetchall() or []


def get_error_rate_trend(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """Get hourly error rate trend (errors per minute).

    Returns: [timestamp, error_count, total_beacons, error_rate]
    """
    sql = """
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour,
        COUNT(*) FILTER (WHERE error_message IS NOT NULL) AS error_count,
        COUNT(*) AS total_beacons,
        COUNT(*) FILTER (WHERE error_message IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0) AS error_rate_pct
    FROM (
        SELECT timestamp, error_message FROM client_errors
        UNION ALL
        SELECT timestamp, NULL FROM client_vitals
    )
    WHERE timestamp >= ? AND timestamp < ?
    GROUP BY hour
    ORDER BY hour DESC
    """
    return runner.execute(sql, [since, until]).fetchall() or []


def get_worst_pages(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
    limit: int = 20,
) -> list[dict]:
    """Get pages with worst Core Web Vitals (Top-N by poor metric count).

    Returns: [pathname, lcp_poor_pct, inp_poor_pct, cls_poor_pct, session_count]
    """
    sql = """
    SELECT
        pathname,
        COUNT(*) FILTER (WHERE metric_name = 'LCP' AND metric_rating = 'poor') * 100.0
            / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'LCP'), 0) AS lcp_poor_pct,
        COUNT(*) FILTER (WHERE metric_name = 'INP' AND metric_rating = 'poor') * 100.0
            / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'INP'), 0) AS inp_poor_pct,
        COUNT(*) FILTER (WHERE metric_name = 'CLS' AND metric_rating = 'poor') * 100.0
            / NULLIF(COUNT(*) FILTER (WHERE metric_name = 'CLS'), 0) AS cls_poor_pct,
        COUNT(DISTINCT rum_cid) AS session_count
    FROM client_vitals
    WHERE timestamp >= ? AND timestamp < ?
        AND pathname IS NOT NULL
    GROUP BY pathname
    HAVING COUNT(*) > 10  -- Require at least 10 metrics to avoid noise
    ORDER BY lcp_poor_pct + inp_poor_pct + cls_poor_pct DESC
    LIMIT ?
    """
    return runner.execute(sql, [since, until, limit]).fetchall() or []


def get_worst_sessions(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
    limit: int = 20,
) -> list[dict]:
    """Get sessions with worst combination of vitals + errors.

    Returns: [session_id, page_count, poor_metrics, error_count, last_seen]
    Stub — full session-scoring correlation implemented in Phase 3.
    """
    sql = """
    SELECT
        rum_cid,
        COUNT(DISTINCT pathname) AS page_count,
        COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_metrics,
        (SELECT COUNT(*) FROM client_errors WHERE client_errors.rum_cid = client_vitals.rum_cid) AS error_count,
        MAX(timestamp) AS last_seen
    FROM client_vitals
    WHERE timestamp >= ? AND timestamp < ?
        AND rum_cid IS NOT NULL
    GROUP BY rum_cid
    HAVING COUNT(*) > 5  -- At least 5 metrics per session
    ORDER BY poor_metrics DESC, error_count DESC
    LIMIT ?
    """
    return runner.execute(sql, [since, until, limit]).fetchall() or []
