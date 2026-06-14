"""Per-hour 1-minute time-series bundle writer + its backfill driver."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

from ._common import (
    TIME_SERIES_BUNDLE_FILENAME,
    _hour_bundled_root,
    _safe_table_for,
    describe_columns,
    discover_closed_hours,
    parse_hour_token,
)

logger = logging.getLogger(__name__)


def build_time_series_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a 1-minute time_series rollup for each closed hour in ``hours``.

    Output: ``rollups/hour_bundled/hour=H/time_series.parquet`` with one row
    per UTC minute and SUM-aggregatable metric columns. Re-bucketing at read
    time to 5/15/60 minutes works as ``SELECT SUM(...) GROUP BY
    time_bucket(...)`` without any sketch.

    Schema (all columns SUM-aggregatable):
      bucket          TIMESTAMP    -- minute floor in UTC
      requests        BIGINT       -- COUNT(*)
      status_4xx      BIGINT       -- COUNT(*) WHERE status BETWEEN 400 AND 499
      status_5xx      BIGINT       -- COUNT(*) WHERE status >= 500
      hits            BIGINT       -- COUNT(*) WHERE cache IN ('HIT','HIT-STALE')
      cache_total     BIGINT       -- COUNT(*) WHERE cache IS NOT NULL
      resp_bytes_sum  BIGINT       -- SUM(resp_bytes)
      ttfb_sum        DOUBLE       -- SUM(ttfb), seconds
      ttfb_count      BIGINT       -- COUNT(*) WHERE ttfb IS NOT NULL

    Columns that map to a backing column missing from this service's
    schema are written as constant 0 so the file shape stays uniform
    across services (the reader uses NULLIF on the denominator).

    Skips the active UTC hour — that hour is still being written and the
    dashboard serves it live off the base table.

    Idempotent (atomic tmp + rename). Returns the number of bundles
    written this call.
    """
    if not hours:
        return 0

    import duckdb

    from backend.core.duckdb import get_connection
    from backend.core.iceberg.view import _get_service_lock

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    target_hours: list[str] = []
    for h in hours:
        if h == active_hour:
            continue
        if parse_hour_token(h) is None:
            logger.warning("[rollups] skipping malformed hour token: %r", h)
            continue
        target_hours.append(h)
    if not target_hours:
        return 0

    table_ident = _safe_table_for(source)
    if not table_ident:
        return 0

    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")

    con = get_connection(source=source, read_only=True)
    try:
        cols = describe_columns(con, source, table_ident, logger=logger, log_label="cannot describe time_series bundle")
        if cols is None:
            return 0

        if "timestamp" not in cols:
            logger.warning(
                "[rollups] %s: no `timestamp` column on %s; skipping time_series bundle",
                service_id,
                table_ident,
            )
            return 0

        # Build the SELECT, adapting each metric to whether its backing
        # column actually exists on this service's schema. Missing-column
        # rows surface as constant 0 so the parquet shape stays uniform
        # (the reader divides via NULLIF, so 0 cache_total → NULL hit_rate).
        select_parts = [
            "time_bucket(INTERVAL '1 minute', timestamp) AS bucket",
            "CAST(COUNT(*) AS BIGINT) AS requests",
        ]
        if "status" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499) AS BIGINT) AS status_4xx")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE status >= 500) AS BIGINT) AS status_5xx")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS status_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS status_5xx")

        if "cache" in cols:
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS hits")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE cache IS NOT NULL) AS BIGINT) AS cache_total")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS hits")
            select_parts.append("CAST(0 AS BIGINT) AS cache_total")

        if "resp_bytes" in cols:
            select_parts.append("CAST(COALESCE(SUM(resp_bytes), 0) AS BIGINT) AS resp_bytes_sum")
        else:
            select_parts.append("CAST(0 AS BIGINT) AS resp_bytes_sum")

        if "ttfb" in cols:
            select_parts.append("CAST(COALESCE(SUM(ttfb), 0.0) AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(COUNT(*) FILTER (WHERE ttfb IS NOT NULL) AS BIGINT) AS ttfb_count")
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS ttfb_sum")
            select_parts.append("CAST(0 AS BIGINT) AS ttfb_count")

        select_sql = ",\n               ".join(select_parts)

        rebuilt = 0
        for hour in target_hours:
            hour_dt = datetime.strptime(hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()

            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            os.makedirs(bundle_dir, exist_ok=True)
            bundle_path = os.path.join(bundle_dir, TIME_SERIES_BUNDLE_FILENAME)

            tmp_path = os.path.join(bundle_dir, f".tmp_ts_{uuid.uuid4().hex[:12]}.parquet")
            query = (
                f"COPY (SELECT {select_sql} "
                f"FROM {table_ident} "
                f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
                f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
                f"GROUP BY 1) "
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning(
                    "[rollups] %s: time_series COPY failed for hour=%s: %s",
                    service_id,
                    hour,
                    e,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            try:
                with _get_service_lock(lock_key):
                    os.replace(tmp_path, bundle_path)
                rebuilt += 1
            except OSError as e:
                logger.warning(
                    "[rollups] %s: could not publish time_series for hour=%s: %s",
                    service_id,
                    hour,
                    e,
                )
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

        return rebuilt
    finally:
        con.close()


def backfill_time_series_bundles(service_id: str, source: dict, max_hours: int | None = None) -> int:
    """One-shot bulk build of time_series.parquet for closed hours that
    don't yet have one.

    Mirrors :func:`backfill_hour_bundles`: walks the per-field rollup tree
    to discover closed hours (those that have any per-field rollup
    written), then calls :func:`build_time_series_bundles` on the subset
    that doesn't already have a time_series file.
    """
    bundled_root = _hour_bundled_root(source)
    all_hours = discover_closed_hours(source)

    to_build: list[str] = []
    for hour in sorted(all_hours):
        ts_path = os.path.join(bundled_root, f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME)
        if not os.path.exists(ts_path):
            to_build.append(hour)
        if max_hours and len(to_build) >= max_hours:
            break

    if not to_build:
        return 0
    return build_time_series_bundles(service_id, source, to_build)
