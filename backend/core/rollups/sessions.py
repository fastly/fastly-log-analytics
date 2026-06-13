"""Per-hour per-(ip, ja4) sessions bundle writer + its backfill driver."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

from ._common import (
    SESSIONS_BUNDLE_FILENAME,
    _hour_bundled_root,
    _rollups_root,
    _safe_table_for,
)

logger = logging.getLogger(__name__)


def build_session_bundles(service_id: str, source: dict, hours: list[str]) -> int:
    """Write a per-hour ``sessions.parquet`` rollup for each closed hour in
    ``hours``.

    Each row is one ``(ip, ja4)`` group within the hour, holding the
    aggregates the ``/api/sessions`` endpoint needs to render the
    sessions list without re-scanning raw logs:

      bucket           TIMESTAMP -- hour start (UTC, naive — matches
                                   the time_series rollup convention)
      ip               VARCHAR
      ja4              VARCHAR   -- nullable; NULL when the service's
                                   schema has no ja4 column
      first_ts         TIMESTAMP -- MIN(timestamp) for this (ip, ja4, hour)
      last_ts          TIMESTAMP -- MAX(timestamp)
      req_count        BIGINT
      country          VARCHAR   -- MIN(country); nullable
      asn              INTEGER   -- MIN(asn); nullable
      reqs_4xx         BIGINT    -- COUNT(*) WHERE status BETWEEN 400 AND 499
      reqs_5xx         BIGINT    -- COUNT(*) WHERE status >= 500
      total_bytes      BIGINT    -- SUM(resp_bytes)
      rtt_sum          DOUBLE    -- SUM(tcp_rtt), microseconds
      rtt_count        BIGINT    -- COUNT WHERE tcp_rtt IS NOT NULL
      edge_count       BIGINT    -- COUNT WHERE edge = 1
      shield_count     BIGINT    -- COUNT WHERE edge = 0
      ua_min           VARCHAR   -- MIN(ua); cheap stable sample
      edge_sid_max     VARCHAR   -- MAX(edge_sid); representative session id

    Sessions that span multiple hours have a row in each hour bundle —
    the reader stitches by checking that the last_ts of one hour and
    the first_ts of the next for the same (ip, ja4) are within 30 min
    (matching the existing CTE pipeline's session gap threshold).

    Skips the active UTC hour — that hour is still being written and
    the dashboard serves it live. Idempotent via atomic tmp + rename.
    Returns the number of bundles written this call.
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
        try:
            datetime.strptime(h, "%Y-%m-%d-%H")
        except ValueError:
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
        try:
            from backend.core.iceberg import execute_with_stale_view_retry

            cols = {
                c[0]
                for c in execute_with_stale_view_retry(
                    con, source, lambda c: c.execute(f"DESCRIBE {table_ident}").fetchall()
                )
            }
        except duckdb.Error as e:
            logger.warning(
                "[rollups] %s: cannot describe %s for sessions bundle: %s",
                service_id,
                table_ident,
                e,
            )
            return 0

        if "timestamp" not in cols or "ip" not in cols:
            # No timestamp or no ip → no session boundary, nothing to roll up.
            logger.info(
                "[rollups] %s: skipping sessions bundle (timestamp=%s, ip=%s)",
                service_id,
                "timestamp" in cols,
                "ip" in cols,
            )
            return 0

        # Group keys: (ip, ja4) when ja4 exists, else (ip) with NULL ja4.
        # Cast NULL to VARCHAR so the parquet schema is consistent across
        # services regardless of whether ja4 was present at write time.
        ja4_expr = '"ja4"' if "ja4" in cols else "CAST(NULL AS VARCHAR)"

        # Adapt each metric to the service's schema. Missing columns
        # surface as constants so the parquet shape stays uniform across
        # services — same pattern as build_time_series_bundles.
        select_parts = [
            "time_bucket(INTERVAL '1 hour', timestamp) AS bucket",
            'CAST("ip" AS VARCHAR) AS ip',
            f"CAST({ja4_expr} AS VARCHAR) AS ja4",
            "MIN(timestamp) AS first_ts",
            "MAX(timestamp) AS last_ts",
            "CAST(COUNT(*) AS BIGINT) AS req_count",
        ]
        if "country" in cols:
            select_parts.append('CAST(MIN("country") AS VARCHAR) AS country')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS country")
        if "asn" in cols:
            select_parts.append('CAST(MIN("asn") AS INTEGER) AS asn')
        else:
            select_parts.append("CAST(NULL AS INTEGER) AS asn")
        if "status" in cols:
            select_parts.append(
                'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_4xx'
            )
            select_parts.append('CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT) AS reqs_5xx')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS reqs_4xx")
            select_parts.append("CAST(0 AS BIGINT) AS reqs_5xx")
        if "resp_bytes" in cols:
            select_parts.append('CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT) AS total_bytes')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS total_bytes")
        if "tcp_rtt" in cols:
            select_parts.append('CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE) AS rtt_sum')
            select_parts.append('CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT) AS rtt_count')
        else:
            select_parts.append("CAST(0.0 AS DOUBLE) AS rtt_sum")
            select_parts.append("CAST(0 AS BIGINT) AS rtt_count")
        if "edge" in cols:
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT) AS edge_count')
            select_parts.append('CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT) AS shield_count')
        else:
            select_parts.append("CAST(0 AS BIGINT) AS edge_count")
            select_parts.append("CAST(0 AS BIGINT) AS shield_count")
        if "ua" in cols:
            select_parts.append('CAST(MIN("ua") AS VARCHAR) AS ua_min')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS ua_min")
        if "edge_sid" in cols:
            select_parts.append('CAST(MAX("edge_sid") AS VARCHAR) AS edge_sid_max')
        else:
            select_parts.append("CAST(NULL AS VARCHAR) AS edge_sid_max")

        select_sql = ",\n               ".join(select_parts)

        rebuilt = 0
        for hour in target_hours:
            hour_dt = datetime.strptime(hour, "%Y-%m-%d-%H").replace(tzinfo=UTC)
            start_iso = hour_dt.isoformat()
            end_iso = (hour_dt + timedelta(hours=1)).isoformat()

            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            os.makedirs(bundle_dir, exist_ok=True)
            bundle_path = os.path.join(bundle_dir, SESSIONS_BUNDLE_FILENAME)

            tmp_path = os.path.join(bundle_dir, f".tmp_sess_{uuid.uuid4().hex[:12]}.parquet")
            query = (
                f"COPY (SELECT {select_sql} "
                f"FROM {table_ident} "
                f"WHERE timestamp >= TIMESTAMPTZ '{start_iso}' "
                f"AND timestamp < TIMESTAMPTZ '{end_iso}' "
                f'AND "ip" IS NOT NULL '
                f"GROUP BY 1, 2, 3) "
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning(
                    "[rollups] %s: sessions COPY failed for hour=%s: %s",
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
                    "[rollups] %s: could not publish sessions for hour=%s: %s",
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


def backfill_session_bundles(service_id: str, source: dict, max_hours: int | None = None) -> int:
    """One-shot bulk build of sessions.parquet for closed hours that
    don't yet have one.

    Mirrors :func:`backfill_time_series_bundles`: walks the per-field
    rollup tree to discover closed hours (those that have any per-field
    rollup written), then calls :func:`build_session_bundles` on the
    subset that doesn't already have a sessions file.
    """
    hour_root = _rollups_root(source)
    bundled_root = _hour_bundled_root(source)
    if not os.path.isdir(hour_root):
        return 0

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    all_hours: set[str] = set()
    try:
        for field_entry in os.listdir(hour_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(hour_root, field_entry)
            try:
                for hour_entry in os.listdir(field_dir):
                    if not hour_entry.startswith("hour="):
                        continue
                    hour = hour_entry[len("hour=") :]
                    if hour >= active_hour:
                        continue
                    all_hours.add(hour)
            except OSError:
                continue
    except OSError:
        return 0

    to_build: list[str] = []
    for hour in sorted(all_hours):
        sess_path = os.path.join(bundled_root, f"hour={hour}", SESSIONS_BUNDLE_FILENAME)
        if not os.path.exists(sess_path):
            to_build.append(hour)
        if max_hours and len(to_build) >= max_hours:
            break

    if not to_build:
        return 0
    return build_session_bundles(service_id, source, to_build)
