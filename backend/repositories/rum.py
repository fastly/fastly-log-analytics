"""RUM repository — query layer for client_vitals and client_errors tables.

Provides analytics queries for Web Vitals trends, error rates, and session correlation.
All queries go through QueryRunner to centralize DuckDB access + caching.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.repositories._base import QueryRunner
from backend.repositories._sql import rum as sql_rum
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import track_query

logger = logging.getLogger(__name__)


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


def get_beacon_health(con) -> tuple[int, datetime | None]:
    """``(beacons in the last hour, newest beacon timestamp)`` across vitals + errors.

    Takes a raw connection (not a runner) because the caller wraps it in
    ``execute_with_stale_view_retry``, which hands the retry closure the connection.
    """
    one_hour_ago = datetime.now(UTC) - timedelta(hours=1)

    with track_query(con, sql_rum.BEACON_HEALTH_RECENT, [one_hour_ago], "rum_health_recent") as cur_recent:
        recent_count = cur_recent.fetchone()[0] or 0

    with track_query(con, sql_rum.BEACON_HEALTH_LAST, [], "rum_health_last") as cur_last:
        last_dt = cur_last.fetchone()[0]

    return recent_count, last_dt


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
) -> dict[str, Any]:
    """Raw aggregate rows for ``/rum/analytics`` (rollup tables when eligible, else raw scan).

    Takes a raw connection because the caller wraps it in ``execute_with_stale_view_retry``.
    Short windows and filtered requests always scan the raw beacon tables; otherwise the
    cron-built ``rum_*_aggregates`` rollups serve the read. Never recomputes in-request.
    """
    from backend.core.rollups.rum import table_exists

    use_rollup = False
    # Check eligibility:
    # 1. No filters allowed for rollups (same as standard rollups)
    # 2. RUM aggregates tables must exist and have data

    prefer_raw_window = False
    try:
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st and et and (et - st).total_seconds() <= raw_window_max_s:
            prefer_raw_window = True
    except Exception:
        pass

    if not parsed_filters and not prefer_raw_window:
        # RUM reads are pure-read. Serve from cron-built rollups when they
        # exist (even if the newest hour is slightly stale — the rum_commit
        # cron refreshes it, exactly like request-log rollups); otherwise
        # scan the raw tables directly. Never recompute aggregates in-request:
        # opening a second read-write DuckLake connection competes for the
        # process-wide attach lock and, under verify-phase concurrency, wedges
        # the whole /api surface (120s pool-timeout on client_vitals).
        if table_exists(con, "rum_vitals_aggregates") and table_exists(con, "rum_error_aggregates"):
            use_rollup = True

    if use_rollup:
        # Run optimized rollup-based query
        logger.info("[rum_rollups] %s: Querying from precomputed RUM aggregates...", service_id)

        # Check if we have any data
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_ANY_DATA,
            [service_id, start_time, end_time],
            "rum_rollup_any_data",
        ) as cur_any:
            if cur_any.fetchone()[0] == 0:
                # Double check if any raw data exists at all
                if not table_exists(con, "client_vitals") or not table_exists(con, "client_errors"):
                    return {"no_data": True}
                with track_query(
                    con,
                    sql_rum.RUM_ANY_DATA,
                    [],
                    "rum_any_data",
                ) as cur_raw_any:
                    if not cur_raw_any.fetchone()[0]:
                        return {"no_data": True}

                # Rollups hold no rows for this window yet (the rum_commit
                # cron hasn't built them) but raw beacons exist — serve the
                # raw path directly instead of recomputing in-request.
                use_rollup = False

    if use_rollup:
        # 1. Pageviews, interactions, errors, total beacons
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_COUNTS_VITALS,
            [service_id, start_time, end_time],
            "rum_rollup_counts_vitals",
        ) as cur_v:
            pageviews, interactions = cur_v.fetchone()
            pageviews = pageviews or 0
            interactions = interactions or 0

        with track_query(
            con,
            sql_rum.RUM_ROLLUP_COUNTS_ERRORS,
            [service_id, start_time, end_time],
            "rum_rollup_counts_errors",
        ) as cur_e:
            errors_count = cur_e.fetchone()[0] or 0

        # NOT pageviews + interactions + errors_count: those are
        # separate distinct-id partitions and a single physical
        # beacon event can appear in more than one (see the writer's
        # 'total_beacons' comment in backend/core/rollups/rum.py).
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_COUNTS_TOTAL_BEACONS,
            [service_id, start_time, end_time],
            "rum_rollup_counts_total_beacons",
        ) as cur_t:
            total_beacons = cur_t.fetchone()[0] or 0

        # 2. Vitals summary
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_VITALS_SUMMARY,
            [service_id, start_time, end_time],
            "rum_rollup_vitals_summary",
        ) as cur_vitals:
            vitals_rows = cur_vitals.fetchall()

        # 3. Environments
        browsers = {}
        os_dict = {}
        devices = {}

        with track_query(
            con,
            sql_rum.RUM_ROLLUP_BROWSERS,
            [service_id, start_time, end_time],
            "rum_rollup_browsers",
        ) as cur_b:
            for val, count in cur_b.fetchall():
                browsers[val] = count

        with track_query(
            con,
            sql_rum.RUM_ROLLUP_OS,
            [service_id, start_time, end_time],
            "rum_rollup_os",
        ) as cur_o:
            for val, count in cur_o.fetchall():
                os_dict[val] = count

        with track_query(
            con,
            sql_rum.RUM_ROLLUP_DEVICES,
            [service_id, start_time, end_time],
            "rum_rollup_devices",
        ) as cur_d:
            for val, count in cur_d.fetchall():
                devices[val] = count

        # 4. Worst Pages
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_WORST_PAGES,
            [service_id, start_time, end_time, service_id, start_time, end_time],
            "rum_rollup_worst_pages",
        ) as cur_pages:
            worst_pages_rows = cur_pages.fetchall()

        # 5. Top Exceptions
        errors_rows = []
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_TOP_EXCEPTIONS,
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

        # 6. Trends Rows
        with track_query(
            con,
            sql_rum.RUM_ROLLUP_TRENDS,
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

    # Create transient temporary tables pre-filtered for our bounds/filters to cut repeated parquet scans
    with track_query(
        con,
        sql_rum.RUM_TEMP_VITALS_CREATE.format(where_sql=where_sql),
        params,
        "rum_temp_vitals_create",
    ):
        pass

    with track_query(
        con,
        sql_rum.RUM_TEMP_ERRORS_CREATE.format(where_sql=where_sql),
        params,
        "rum_temp_errors_create",
    ):
        pass

    try:
        # B. Total match counts within bounds (Consolidated)
        with track_query(
            con,
            sql_rum.RUM_CONSOLIDATED_COUNTS.format(distinct_id=sql_rum.DISTINCT_ID),
            [],
            "rum_consolidated_counts",
        ) as cur_counts:
            pageviews, interactions, errors_count, total_beacons = cur_counts.fetchone()
            pageviews = pageviews or 0
            interactions = interactions or 0
            errors_count = errors_count or 0
            # NOT pageviews + interactions + errors_count: those are
            # separate distinct-id partitions and a single physical
            # beacon event can appear in more than one (e.g. a vitals
            # metric and an interaction event sharing one distinct_id).
            total_beacons = total_beacons or 0

        # A. Check if any data exists at all (Deferred check ONLY if no matches in selected bounds)
        if total_beacons == 0:
            with track_query(
                con,
                sql_rum.RUM_ANY_DATA,
                [],
                "rum_any_data",
            ) as cur_any:
                if not cur_any.fetchone()[0]:
                    return {"no_data": True}

        # C. Vitals metrics, percentiles & distribution
        with track_query(
            con,
            sql_rum.RUM_VITALS_SUMMARY,
            [],
            "rum_vitals_summary",
        ) as cur_vitals:
            vitals_rows = cur_vitals.fetchall()

        # D. Environments (Consolidated using GROUPING SETS)
        browsers = {}
        os_dict = {}
        devices = {}

        with track_query(
            con,
            sql_rum.RUM_ENVIRONMENTS_CONSOLIDATED.format(distinct_id=sql_rum.DISTINCT_ID),
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

        # E. Worst pages
        with track_query(
            con,
            sql_rum.RUM_WORST_PAGES.format(distinct_id=sql_rum.DISTINCT_ID),
            [],
            "rum_worst_pages",
        ) as cur_pages:
            worst_pages_rows = cur_pages.fetchall()

        # F. Top Exceptions
        with track_query(
            con,
            sql_rum.RUM_TOP_EXCEPTIONS,
            [],
            "rum_top_exceptions",
        ) as cur_errors:
            errors_rows = cur_errors.fetchall()

        # G. Trend buckets
        with track_query(
            con,
            sql_rum.RUM_HOURLY_TRENDS.format(distinct_id=sql_rum.DISTINCT_ID),
            [],
            "rum_hourly_trends",
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


def fetch_rum_live_events(con, *, where_sql: str, params: list) -> list[tuple]:
    """Newest 50 vitals + error beacons for the live ticker, newest first."""
    with track_query(con, sql_rum.RUM_LIVE_EVENTS.format(where_sql=where_sql), params * 2, "rum_live_events") as cur:
        return cur.fetchall()
