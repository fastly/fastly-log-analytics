"""RUM repository — query layer for client_vitals and client_errors tables.

Provides analytics queries for Web Vitals trends, error rates, and session correlation.
All queries go through QueryRunner to centralize DuckDB access + caching.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.core.iceberg import is_stale_view_error
from backend.core.retry import retry_call
from backend.repositories._sql import rum as sql_rum
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import track_query

logger = logging.getLogger(__name__)

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
                # Re-glob sibling batch_*.parquet files in the same buffer dir if any remain
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
            from backend.core.iceberg import view as iceberg_view

            iceberg_view.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
            iceberg_view.update_iceberg_view(con, source, force=True)
        _prune_missing_files_from_rum_views(con)

    return retry_call(
        fn,
        retry_on=is_stale_view_error,
        attempts=3,
        before_retry=_recover,
    )


def get_beacon_health(con, *, source: dict | None = None) -> tuple[int, datetime | None]:
    """``(beacons in the last hour, newest beacon timestamp)`` across vitals + errors.

    Takes a raw connection (not a runner) because the caller wraps it in
    ``execute_with_stale_view_retry``, which hands the retry closure the connection.
    """

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
    """Raw aggregate rows for ``/rum/analytics`` (rollup tables when eligible, else raw scan).

    Takes a raw connection because the caller wraps it in ``execute_with_stale_view_retry``.
    Short windows and filtered requests always scan the raw beacon tables; otherwise the
    cron-built ``rum_*_aggregates`` rollups serve the read. Never recomputes in-request.
    """
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
        logger.debug("fetch_rum_analytics: step failed (ignored)", exc_info=True)

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
        vitals_agg_table = "rum_vitals_aggregates"
        error_agg_table = "rum_error_aggregates"
        try:
            with track_query(
                con,
                sql_rum.RUM_ROLLUP_MAX_BUCKET,
                [service_id, service_id],
                "rum_rollup_max_bucket",
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
                with track_query(
                    con,
                    sql_rum.RUM_TAIL_HAS_ROWS,
                    [],
                    "rum_tail_has_rows",
                ) as cur_tail:
                    tail_rows = (cur_tail.fetchone() or (0,))[0] or 0

                if tail_rows > 0:
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_VITALS_AGG_CREATE.format(distinct_id=sql_rum.DISTINCT_ID),
                        [service_id, start_time, end_time] + [service_id] * 10,
                        "rum_eff_vitals_agg_create",
                    ):
                        pass
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_ERROR_AGG_CREATE.format(distinct_id=sql_rum.DISTINCT_ID),
                        [service_id, start_time, end_time] + [service_id] * 3,
                        "rum_eff_error_agg_create",
                    ):
                        pass
                    vitals_agg_table = "t_eff_vitals_agg"
                    error_agg_table = "t_eff_error_agg"

            if use_rollup:
                logger.info("[rum_rollups] %s: Querying from precomputed RUM aggregates...", service_id)

                # Check if we have any data
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
                        with track_query(
                            con,
                            sql_rum.RUM_ANY_DATA,
                            [],
                            "rum_any_data",
                        ) as cur_raw_any:
                            if not cur_raw_any.fetchone()[0]:
                                return {"no_data": True}
                        use_rollup = False

            if use_rollup:
                # 1. Pageviews, interactions, errors, total beacons
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_VITALS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_vitals",
                ) as cur_v:
                    pageviews, interactions = cur_v.fetchone()
                    pageviews = pageviews or 0
                    interactions = interactions or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_ERRORS.format(error_agg_table=error_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_errors",
                ) as cur_e:
                    errors_count = cur_e.fetchone()[0] or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_TOTAL_BEACONS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_total_beacons",
                ) as cur_t:
                    total_beacons = cur_t.fetchone()[0] or 0

                # 2. Vitals summary
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_VITALS_SUMMARY.format(vitals_agg_table=vitals_agg_table),
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
                    sql_rum.RUM_ROLLUP_BROWSERS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_browsers",
                ) as cur_b:
                    for val, count in cur_b.fetchall():
                        browsers[val] = count

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_OS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_os",
                ) as cur_o:
                    for val, count in cur_o.fetchall():
                        os_dict[val] = count

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_DEVICES.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_devices",
                ) as cur_d:
                    for val, count in cur_d.fetchall():
                        devices[val] = count

                # 4. Worst Pages
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_WORST_PAGES.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_worst_pages",
                ) as cur_pages:
                    worst_pages_rows = cur_pages.fetchall()

                # 5. Top Exceptions
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

                # 6. Trends Rows
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
