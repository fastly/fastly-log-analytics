"""Usage repository — DuckDB queries for log activity, storage stats, and edge ratio."""

from __future__ import annotations

import duckdb

from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories._sql import usage as SQL


def get_edge_ratio(con: duckdb.DuckDBPyConnection, src: dict) -> tuple[float | None, list]:
    """Return (edge_ratio_pct_or_None, debug_queries)."""
    runner = QueryRunner(con, src)
    table = _safe_table(src["name"])
    from backend.core.duckdb import get_schema

    actual_cols = [col["name"] for col in get_schema(con, src, stats=False)]
    if "edge" not in actual_cols:
        return None, runner.debug_queries
    result = runner.execute_with_retry(SQL.EDGE_RATIO_PCT.format(table=table))
    if result is None:
        return None, runner.debug_queries
    row = result.fetchone()
    ratio = round(float(row[0]), 1) if row and row[0] is not None else None
    return ratio, runner.debug_queries


def get_storage_stats(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_str: str,
    end_str: str,
) -> dict:
    """Return ingested-file count and total bytes for the given time window.

    Window filter is pushed into SQLite (COUNT/SUM against the source_name
    index) so the cost panel doesn't pull every row per service open.
    """
    from backend.core import metadata as metadata_db

    total_files, total_bytes = metadata_db.get_storage_stats_window(src["name"], start_str, end_str)
    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "_debug_queries": [],
        "_debug_calls": [],
    }


def get_log_activity(
    src: dict,
    start_str: str,
    end_str: str,
    by: str,
) -> dict:
    """Return time-bucketed log activity (rows and bytes per bucket).

    Primary path reads from the DuckDB overview rollup parquets so the
    data covers the full retention window (the old SQLite
    ``ingested_files`` source was trimmed nightly by metadata_cleanup,
    creating a misleading gap in the chart).  Falls back to the SQLite
    path for brand-new services that have no rollup data yet.
    """
    import os
    from datetime import UTC, datetime, timedelta

    import duckdb as _duckdb

    from backend.core.rollups import OVERVIEW_BUNDLE_FILENAME
    from backend.core.rollups._common import _hour_bundled_root, quote_path_list
    from backend.utils.date_utils import parse_iso_utc

    interval_map = {"hour": "1 hour", "day": "1 day"}
    interval = interval_map.get(by)
    if not interval:
        return _log_activity_fallback(src, start_str, end_str, by)

    st = parse_iso_utc(start_str)
    et = parse_iso_utc(end_str)
    if st is None or et is None or et <= st:
        return _log_activity_fallback(src, start_str, end_str, by)

    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return _log_activity_fallback(src, start_str, end_str, by)

    # Collect available overview parquets without the strict
    # "writer-behind" bailout that collect_hourly_bundle_paths uses.
    # For this chart, partial rollup data beats empty SQLite data.
    active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    rollup_paths: list[str] = []
    cursor = st.replace(minute=0, second=0, microsecond=0)
    while cursor < et:
        hour_str = cursor.strftime("%Y-%m-%d-%H")
        if hour_str >= active_hour_str:
            break
        path = os.path.join(bundled_root, f"hour={hour_str}", OVERVIEW_BUNDLE_FILENAME)
        if os.path.isfile(path):
            rollup_paths.append(path)
        cursor += timedelta(hours=1)

    if not rollup_paths:
        return _log_activity_fallback(src, start_str, end_str, by)

    paths_sql = quote_path_list(rollup_paths)
    st_tz = st.astimezone(UTC).isoformat()
    et_tz = et.astimezone(UTC).isoformat()
    # Parquet files are self-contained — use an in-memory DuckDB
    # connection so we don't contend with the service's DB lock.
    sql = (
        f"SELECT time_bucket(INTERVAL '{interval}', hour_start) AS bucket, "
        f"  SUM(requests) AS total, "
        f"  SUM(total_bandwidth_bytes) AS total_bytes "
        f"FROM read_parquet([{paths_sql}]) "
        f"WHERE hour_start >= TIMESTAMPTZ '{st_tz}' "
        f"  AND hour_start < TIMESTAMPTZ '{et_tz}' "
        f"GROUP BY 1 ORDER BY 1"
    )

    fmt = "%Y-%m-%dT%H:%M" if by == "hour" else "%Y-%m-%d"
    try:
        con = _duckdb.connect()
        try:
            rows = con.execute(sql).fetchall()
        finally:
            con.close()
    except Exception:
        return _log_activity_fallback(src, start_str, end_str, by)

    points: list[dict] = []
    total_rows = 0
    total_bytes = 0
    for r in rows:
        rc = int(r[1])
        bs = int(r[2])
        points.append(
            {
                "time": r[0].strftime(fmt),
                "row_count": rc,
                "bytes": bs,
            }
        )
        total_rows += rc
        total_bytes += bs

    return {
        "data": points,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "granularity": by,
        "_debug_queries": [],
        "_debug_calls": [],
    }


def _log_activity_fallback(
    src: dict,
    start_str: str,
    end_str: str,
    by: str,
) -> dict:
    """Fall back to SQLite ingested_files for services with no rollup data."""
    from backend.core import metadata as metadata_db

    service_id = src.get("name") or src.get("service_id", "")
    out = metadata_db.get_log_activity(service_id, start_str, end_str, by)
    return {**out, "_debug_queries": [], "_debug_calls": []}
