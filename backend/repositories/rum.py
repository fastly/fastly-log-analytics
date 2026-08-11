"""RUM repository — query layer for client_vitals and client_errors tables.

Provides analytics queries for Web Vitals trends, error rates, and session correlation.
All queries go through QueryRunner to centralize DuckDB access + caching.
"""

from __future__ import annotations

from datetime import datetime

from backend.repositories._base import QueryRunner
from backend.repositories._sql import rum as sql_rum


def get_web_vitals_summary(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """Get Core Web Vitals summary (P50/P75/P95 over time window).

    Returns hourly aggregates: [timestamp, metric_name, p50, p75, p95]
    """
    return runner.execute(sql_rum.WEB_VITALS_SUMMARY, [since, until]).fetchall() or []


def get_error_rate_trend(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
) -> list[dict]:
    """Get hourly error rate trend (errors per hour).

    Returns: [timestamp, error_count, total_beacons, error_rate]
    """
    return runner.execute(sql_rum.ERROR_RATE_TREND, [since, until]).fetchall() or []


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
    return runner.execute(sql_rum.WORST_PAGES, [since, until, limit]).fetchall() or []


def get_worst_sessions(
    runner: QueryRunner,
    service_id: str,
    since: datetime,
    until: datetime,
    limit: int = 20,
) -> list[dict]:
    """Get sessions with worst combination of vitals + errors.

    Returns: [session_id, page_count, poor_metrics, error_count, last_seen]
    """
    return runner.execute(sql_rum.WORST_SESSIONS, [since, until, limit]).fetchall() or []
