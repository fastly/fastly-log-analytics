"""
Hourly Top-N rollups for the dashboard.

For each tracked field (e.g. ``ip``, ``country``, ``url``, custom fields), we
keep one parquet file per hour at
``<cache>/rollups/hour/field=<field>/hour=<YYYY-MM-DD-HH>/compacted_*.parquet``
holding the top-K most-common values for that field in that hour.

The dashboard reads these instead of scanning the base ``logs`` view when no
filters are active, which cuts the unfiltered 24h top-N from a multi-second
scan to tens of milliseconds. The active hour is always served live off the
base table (rollups don't include the in-progress hour).

Writers:
- ``recompute_touched_hours``: per sync tick, batched per-field COPY ...
  PARTITION_BY (field, hour). Only re-computes the hours actually touched
  by the new chunk.
- ``backfill_rollups``: one-shot bulk build over all historical hours,
  invoked at first-boot and when a new field is added.
- ``cleanup_old_rollups``: drops per-hour directories older than the cfg
  retention window. Called from the daily ``metadata_cleanup`` cron.

Reader:
- ``QueryRunner.execute_top_n_rollups`` in
  ``backend/repositories/_base.py``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import uuid
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

# How many top values per (field, hour) we persist. Dashboards render
# 10-25 at a time; 500 gives generous headroom for filter overlays and
# the long-tail "Other" rollup.
TOP_K = 500

# SQL identifier safelist. Field names land verbatim inside ``"..."``
# quoted identifiers and inside SELECT projections; service names land
# in the table identifier ``logs_<name>``. Both come from cfg / DuckDB
# schema and are PROBABLY already validated upstream — but a single
# stray double-quote or backtick in either would break the query in a
# way that's both a correctness bug and a privilege boundary (the
# fields are derived from admin-controlled custom_field entries).
# Defense in depth: this module reject anything not matching the
# pattern with a logged warning.
_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_ident(name: str) -> bool:
    return bool(name) and bool(_SAFE_IDENT_RE.match(name))


def _safe_table_for(source: dict) -> str | None:
    """Return the DuckDB view name for this service, or ``None`` if no slug.

    Slugifies the same way the dashboard's view-builder does
    (``backend.core.duckdb._safe_table_name``: non-alphanumerics to ``_``,
    lowercased, ``logs_`` prefix) so the rollup COPY/SELECT targets the
    same view name the dashboard creates. Reads ``service_id`` first (the
    canonical slug in normalized source dicts) and falls back to ``name``
    for callers that pass a raw on-disk config — both cases pass through
    the slugifier identically.
    """
    raw = source.get("service_id") or source.get("name") or ""
    if not raw:
        logger.warning("[rollups] no service_id/name in source dict; skipping rollup")
        return None
    from backend.core.duckdb import _safe_table_name

    return _safe_table_name(raw)


def _get_fields(src: dict) -> list[str]:
    """Return the dashboard fields eligible for rollup.

    Custom-field names are validated against ``_SAFE_IDENT_RE`` — anything
    failing the check is skipped with a warning rather than fed into SQL.
    """
    from backend.repositories.dashboard import _VIRTUAL_FIELDS, FIELDS

    lf_config = src.get("log_fields") or {}
    custom_field_names: list[str] = []
    for cf in lf_config.get("custom_fields", []):
        if not cf.get("enabled", True) or not cf.get("show_in_dashboard", True):
            continue
        name = cf.get("name") or ""
        if not _is_safe_ident(name):
            logger.warning("[rollups] skipping custom field with unsafe name: %r", name)
            continue
        custom_field_names.append(name)
    # Virtual fields (e.g. waf_sig_ind) are computed views over CSV columns
    # — they aren't column names, so they can't be rolled up directly.
    actual_fields = [f for f in FIELDS if f not in _VIRTUAL_FIELDS and _is_safe_ident(f)]
    return actual_fields + custom_field_names


def _rollups_root(source: dict) -> str:
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour")


def _day_rollups_root(source: dict) -> str:
    """Per-day compacted rollups directory.

    Companion to `_rollups_root` (which holds per-hour rollups). Populated
    by `compact_closed_days_to_daily` — each (field, closed-day) becomes
    a single parquet file aggregating its 24 source hour parquets. The
    reader (`execute_top_n_rollups`) prefers per-day files for closed
    days and falls back to per-hour for the active trailing window.
    Item 17 / RC-9.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "day")


def _markers_path(source: dict) -> str:
    """JSON file tracking which fields have been backfilled.

    Replaces the prior single ``.backfill_done`` marker which couldn't
    distinguish "fully backfilled" from "backfilled before a new custom
    field was added". Shape: ``{"field": "ISO timestamp", ...}``.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "backfill_markers.json")


def _load_markers(source: dict) -> dict[str, str]:
    path = _markers_path(source)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[rollups] could not read markers at %s: %s", path, e)
        return {}


def _save_markers(source: dict, markers: dict[str, str]) -> None:
    path = _markers_path(source)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Atomic write so a crash mid-write doesn't truncate the file.
    tmp_path = f"{path}.tmp.{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp_path, "w") as f:
            json.dump(markers, f)
        os.replace(tmp_path, path)
    except OSError as e:
        logger.warning("[rollups] could not write markers to %s: %s", path, e)
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def _publish_field_partitions(tmp_field_dir: str, dst_root: str, field: str) -> int:
    """Move per-hour parquet files from a temp PARTITION_BY tree into the
    canonical ``rollups/hour/field=X/hour=Y/`` layout.

    The publish order is RENAME-then-UNLINK to close the race window where
    a concurrent dashboard read could observe an empty hour directory.
    Worst case after this change: a dashboard read briefly sees BOTH the
    new and old parquet for the same hour and double-counts that hour
    until the unlink lands — which is bounded and self-corrects on the
    next refresh. Pre-fix, the dashboard could observe ZERO files for the
    hour (undercount), which was indistinguishable from a real traffic dip.

    Caller MUST hold the per-service iceberg lock around the whole call.
    Returns the number of hour-dirs published.
    """
    field_dir = os.path.join(tmp_field_dir, f"field={field}")
    if not os.path.isdir(field_dir):
        return 0

    published = 0
    for hour_dirname in os.listdir(field_dir):
        if not hour_dirname.startswith("hour="):
            continue
        src_hour_dir = os.path.join(field_dir, hour_dirname)
        dst_hour_dir = os.path.join(dst_root, f"field={field}", hour_dirname)
        os.makedirs(dst_hour_dir, exist_ok=True)

        # 1. Rename new files into place first (overcounting window OK).
        new_names: set[str] = set()
        for fname in os.listdir(src_hour_dir):
            if not fname.endswith(".parquet"):
                continue
            new_name = f"compacted_{uuid.uuid4().hex[:12]}.parquet"
            os.rename(os.path.join(src_hour_dir, fname), os.path.join(dst_hour_dir, new_name))
            new_names.add(new_name)

        # 2. Now unlink any pre-existing files that we didn't just write.
        if new_names:
            for existing in os.listdir(dst_hour_dir):
                if existing.endswith(".parquet") and existing not in new_names:
                    try:
                        os.remove(os.path.join(dst_hour_dir, existing))
                    except OSError as e:
                        logger.warning("[rollups] could not unlink stale %s: %s", existing, e)
            published += 1

    return published


def _build_copy_query(table_ident: str, field: str, where_sql: str) -> str:
    """Return the COPY ... TO <tmp> PARTITION_BY (field, hour) SQL for one field.

    Inputs must already be validated — this function does NO escaping.
    Callers (recompute_touched_hours / backfill_rollups) gate via
    ``_is_safe_ident`` and ``_safe_table_for``.
    """
    return f"""
        SELECT field, hour, value, count FROM (
            SELECT
                '{field}' AS field,
                strftime(timestamp, '%Y-%m-%d-%H') AS hour,
                CAST("{field}" AS VARCHAR) AS value,
                COUNT(*) AS count,
                ROW_NUMBER() OVER (
                    PARTITION BY strftime(timestamp, '%Y-%m-%d-%H')
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM {table_ident}
            WHERE {where_sql}
            GROUP BY 1, 2, 3
        ) WHERE rn <= {TOP_K}
    """


def _hour_bundled_root(source: dict) -> str:
    """Return the per-hour bundled rollup root.

    Layout: cache/<svc>/rollups/hour_bundled/hour=YYYY-MM-DD-HH/all_fields.parquet
    Each bundle contains rows for ALL fields for that hour with the same
    (field, value, count) schema as the per-field hour parquets. Reading
    one bundle replaces opening ~40+ per-field files for that hour.

    The same hour directory also holds ``time_series.parquet`` — see
    :func:`build_time_series_bundles` for the schema.
    """
    from backend.core.duckdb import _cache_dir

    return os.path.join(_cache_dir(source), "rollups", "hour_bundled")


# Filename for the per-hour 1-minute time-series rollup. Kept as a constant
# so the writer + reader can never drift on the name.
TIME_SERIES_BUNDLE_FILENAME = "time_series.parquet"

# Filename for the per-hour per-(ip, ja4) sessions rollup. Stored
# alongside time_series.parquet so the same reader can enumerate both
# in one directory walk.
SESSIONS_BUNDLE_FILENAME = "sessions.parquet"


def _time_series_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME)


def _sessions_bundle_path(source: dict, hour: str) -> str:
    return os.path.join(_hour_bundled_root(source), f"hour={hour}", SESSIONS_BUNDLE_FILENAME)


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
                "[rollups] %s: cannot describe %s for time_series bundle: %s",
                service_id,
                table_ident,
                e,
            )
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


def backfill_time_series_bundles(service_id: str, source: dict, max_hours: int | None = None) -> int:
    """One-shot bulk build of time_series.parquet for closed hours that
    don't yet have one.

    Mirrors :func:`backfill_hour_bundles`: walks the per-field rollup tree
    to discover closed hours (those that have any per-field rollup
    written), then calls :func:`build_time_series_bundles` on the subset
    that doesn't already have a time_series file.
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
        ts_path = os.path.join(bundled_root, f"hour={hour}", TIME_SERIES_BUNDLE_FILENAME)
        if not os.path.exists(ts_path):
            to_build.append(hour)
        if max_hours and len(to_build) >= max_hours:
            break

    if not to_build:
        return 0
    return build_time_series_bundles(service_id, source, to_build)


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


def bundle_hours(service_id: str, source: dict, hours: list[str]) -> int:
    """Combine per-field hour parquets into one bundled parquet per hour.

    For each hour token, reads every per-field parquet under
    rollups/hour/field=*/hour=H/*.parquet and writes a single bundled file
    at rollups/hour_bundled/hour=H/all_fields.parquet.

    Skips hours where:
      - No per-field files exist (nothing to bundle).
      - A bundled file already exists and is fresh enough to skip rebuild
        (per-field mtime <= bundle mtime).

    Returns the count of hours that were rebuilt.

    Skip the active hour — bundles for in-progress hours would race the
    sync's per-field rebuilds. The active hour is served live anyway.
    """
    if not hours:
        return 0

    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    # _rollups_root already returns <cache>/rollups/hour — it's the
    # per-field per-hour tree root, not the rollups/ parent.
    hour_per_field_root = _rollups_root(source)
    bundled_root = _hour_bundled_root(source)
    os.makedirs(bundled_root, exist_ok=True)
    lock_key = source.get("name", "default")
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    rebuilt = 0
    # Use :memory: DuckDB to avoid contending with uvicorn's RW connection
    # on the per-service .duckdb file (mirrors compact_closed_days_to_daily —
    # see the 2026-06-06 incident comment in that function). The bundling
    # COPY only needs to read existing parquets and write a new one; it
    # doesn't need any per-service catalog state.
    con = duckdb.connect(":memory:")
    try:
        for hour in hours:
            if hour == active_hour:
                continue
            # Validate hour token format defensively — string lands in
            # filesystem paths and SQL string literals below.
            try:
                datetime.strptime(hour, "%Y-%m-%d-%H")
            except ValueError:
                continue

            # Enumerate per-field parquets for this hour.
            per_field_paths: list[str] = []
            max_src_mtime = 0.0
            try:
                for field_entry in os.listdir(hour_per_field_root):
                    if not field_entry.startswith("field="):
                        continue
                    hour_dir = os.path.join(hour_per_field_root, field_entry, f"hour={hour}")
                    if not os.path.isdir(hour_dir):
                        continue
                    for fname in os.listdir(hour_dir):
                        if not fname.endswith(".parquet") or fname.startswith(".tmp_"):
                            continue
                        p = os.path.join(hour_dir, fname)
                        per_field_paths.append(p)
                        try:
                            mt = os.path.getmtime(p)
                            if mt > max_src_mtime:
                                max_src_mtime = mt
                        except OSError:
                            pass
            except OSError:
                continue

            if not per_field_paths:
                continue

            # Skip if bundle is already up-to-date.
            bundle_dir = os.path.join(bundled_root, f"hour={hour}")
            bundle_path = os.path.join(bundle_dir, "all_fields.parquet")
            if os.path.exists(bundle_path):
                try:
                    if os.path.getmtime(bundle_path) >= max_src_mtime:
                        continue
                except OSError:
                    pass

            os.makedirs(bundle_dir, exist_ok=True)
            tmp_path = os.path.join(bundle_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
            paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in per_field_paths)
            # Read the per-field parquets (each has columns field/value/count)
            # and write to a single bundled parquet. Use COPY for atomicity
            # via the tmp + rename pattern.
            query = (
                f"COPY (SELECT field, value, CAST(count AS BIGINT) AS count "
                f"FROM read_parquet([{paths_sql}])) "
                f"TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: bundle COPY failed for hour=%s: %s", service_id, hour, e)
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                continue

            with _get_service_lock(lock_key):
                # Atomic publish — os.replace is atomic on POSIX.
                os.replace(tmp_path, bundle_path)
            rebuilt += 1
    finally:
        con.close()

    return rebuilt


def recompute_touched_hours(service_id: str, source: dict, hours: set[str]) -> None:
    """Recompute rollups for all dashboard fields across the given hours.

    Excludes the active (current UTC) hour — the dashboard serves the
    in-progress hour live off the base table. One COPY query per field
    handles all touched hours via PARTITION_BY, so the work is O(fields)
    not O(fields × hours).

    After the per-field rebuild completes, bundles each touched hour's
    per-field parquets into a single bundled file under
    ``rollups/hour_bundled/hour=H/all_fields.parquet`` so the dashboard
    reader can open one file per hour instead of ~40 per-field files.
    """
    if not hours:
        return

    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    parsed: list[tuple[str, datetime]] = []
    for h in hours:
        if h == active_hour:
            continue
        try:
            parsed.append((h, datetime.strptime(h, "%Y-%m-%d-%H").replace(tzinfo=UTC)))
        except ValueError:
            logger.warning("[rollups] skipping malformed hour token: %r", h)
    if not parsed:
        return

    table_ident = _safe_table_for(source)
    if not table_ident:
        return

    min_start = min(dt for _, dt in parsed)
    max_end = max(dt for _, dt in parsed) + timedelta(hours=1)
    hour_list_sql = ", ".join(f"'{h}'" for h, _ in parsed)
    where_sql = (
        f"timestamp >= '{min_start.isoformat()}' "
        f"AND timestamp < '{max_end.isoformat()}' "
        f"AND strftime(timestamp, '%Y-%m-%d-%H') IN ({hour_list_sql})"
    )
    _run_per_field_copy(service_id, source, table_ident, where_sql, _get_fields(source))

    # Bundle the touched hours so the dashboard reader can open one
    # file per hour instead of N per-field files. Best-effort: if
    # bundling fails, the per-field files still serve correctly via
    # the reader's fallback path.
    touched_hours = [h for h, _ in parsed]
    try:
        bundle_hours(service_id, source, touched_hours)
    except Exception as e:
        logger.warning("[rollups] %s: hour bundling failed (per-field still serves): %s", service_id, e)

    # Time-series rollups for the dashboard chart. Same best-effort
    # contract: if the build fails, the dashboard falls back to a raw
    # scan for the affected hours.
    try:
        build_time_series_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: time_series bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )

    # Sessions rollups for /api/sessions. Best-effort: if the build
    # fails, the sessions endpoint falls back to a raw window-function
    # scan for any hours that lack a sessions.parquet.
    try:
        build_session_bundles(service_id, source, touched_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: sessions bundle failed (raw scan will serve): %s",
            service_id,
            e,
        )


def backfill_hour_bundles(service_id: str, source: dict, max_hours: int | None = None) -> int:
    """One-shot bulk bundling for all closed hours that don't yet have a
    per-hour bundled file.

    Walks the existing rollups/hour/field=*/hour=*/ tree, collects the set
    of closed hours, and calls bundle_hours() on any that lack an up-to-
    date bundle. Safe to call on startup and idempotent — bundle_hours
    skips up-to-date hours via mtime comparison.

    ``max_hours``: if set, caps the number of hours processed per call
    (useful for incremental backfills if running synchronously would
    block startup too long).
    """
    # _rollups_root already returns <cache>/rollups/hour — see comment
    # in bundle_hours about the naming.
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

    # Skip hours that already have a bundle.
    to_bundle = []
    for hour in sorted(all_hours):
        bundle_path = os.path.join(bundled_root, f"hour={hour}", "all_fields.parquet")
        if not os.path.exists(bundle_path):
            to_bundle.append(hour)
        if max_hours and len(to_bundle) >= max_hours:
            break

    if not to_bundle:
        rebuilt = 0
    else:
        rebuilt = bundle_hours(service_id, source, to_bundle)

    # Also catch up the time-series bundles. Walks the same hour set and
    # only writes for hours that don't yet have time_series.parquet.
    try:
        backfill_time_series_bundles(service_id, source, max_hours=max_hours)
    except Exception as e:
        logger.warning(
            "[rollups] %s: time_series backfill failed (raw scan will serve): %s",
            service_id,
            e,
        )

    return rebuilt


def backfill_rollups(service_id: str, source: dict, fields: list[str] | None = None) -> None:
    """One-shot bulk build for all historical hours up to (but not including)
    the current hour.

    ``fields``: if provided, only backfills the given subset (used when a
    new custom field is added — see :func:`ensure_field_backfills`).
    Defaults to all eligible fields.
    """
    table_ident = _safe_table_for(source)
    if not table_ident:
        return

    target_fields = fields if fields is not None else _get_fields(source)
    if not target_fields:
        return

    dt_end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    where_sql = f"timestamp < '{dt_end.isoformat()}'"
    _run_per_field_copy(service_id, source, table_ident, where_sql, target_fields)

    # Stamp completion in the markers file so _ensure_rollups can detect
    # which fields still need a backfill on next startup / cfg change.
    markers = _load_markers(source)
    stamp = datetime.now(UTC).isoformat()
    for f in target_fields:
        markers[f] = stamp
    _save_markers(source, markers)


def ensure_field_backfills(service_id: str, source: dict) -> None:
    """Backfill any eligible fields that don't yet have a marker entry.

    Triggered at startup (full backfill if no markers) and by callers that
    mutate the log_fields config (new field added). Idempotent — fields
    already in the markers file are skipped.
    """
    markers = _load_markers(source)
    eligible = _get_fields(source)
    missing = [f for f in eligible if f not in markers]
    if not missing:
        return
    logger.info(
        "[rollups] service %s: backfilling %d new field(s): %s",
        service_id,
        len(missing),
        missing,
    )
    backfill_rollups(service_id, source, fields=missing)


def cleanup_old_rollups(service_id: str, source: dict, max_age_days: int) -> int:
    """Delete per-hour rollup directories older than ``max_age_days``.

    ``max_age_days <= 0`` disables cleanup (keep everything). Returns the
    number of hour-dirs deleted. Safe to call concurrently with the
    writers because we only ever delete hours STRICTLY older than the
    cutoff — current and just-written hours are never candidates.
    """
    if max_age_days <= 0:
        return 0
    rollup_root = _rollups_root(source)
    if not os.path.isdir(rollup_root):
        return 0

    cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).strftime("%Y-%m-%d-%H")
    deleted = 0
    try:
        for field_entry in os.listdir(rollup_root):
            if not field_entry.startswith("field="):
                continue
            field_dir = os.path.join(rollup_root, field_entry)
            for hour_entry in os.listdir(field_dir):
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # String compare works because the format is fixed-width
                # YYYY-MM-DD-HH which sorts lexicographically by time.
                if hour < cutoff:
                    hour_dir = os.path.join(field_dir, hour_entry)
                    try:
                        shutil.rmtree(hour_dir)
                        deleted += 1
                    except OSError as e:
                        logger.warning("[rollups] could not delete %s: %s", hour_dir, e)
    except OSError as e:
        logger.warning("[rollups] cleanup walk failed for %s: %s", service_id, e)
    return deleted


def _run_per_field_copy(
    service_id: str,
    source: dict,
    table_ident: str,
    where_sql: str,
    fields: list[str],
) -> None:
    """Shared core of recompute_touched_hours and backfill_rollups.

    One COPY query per field, writing to a per-field temp directory via
    PARTITION_BY (field, hour), then publishing each hour-dir under the
    per-service iceberg lock.
    """
    import duckdb

    from backend.core.duckdb import _cache_dir, get_connection
    from backend.core.iceberg.view import _get_service_lock

    cache_root = _cache_dir(source)
    rollups_dir = _rollups_root(source)
    os.makedirs(rollups_dir, exist_ok=True)
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
            logger.warning("[rollups] %s: could not describe %s: %s", service_id, table_ident, e)
            return

        for field in fields:
            if not _is_safe_ident(field):
                # Belt-and-suspenders — _get_fields already filters, but
                # defend against direct callers passing raw names.
                logger.warning("[rollups] skipping unsafe field name: %r", field)
                continue
            if field not in cols:
                continue

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp", field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
            os.makedirs(tmp_field_dir, exist_ok=True)

            inner = _build_copy_query(table_ident, field, where_sql)
            query = (
                f"COPY ({inner}) TO '{tmp_field_dir}' "
                "(FORMAT PARQUET, PARTITION_BY (field, hour), OVERWRITE_OR_IGNORE, COMPRESSION ZSTD)"
            )
            try:
                con.execute(query)
            except duckdb.Error as e:
                logger.warning("[rollups] %s: COPY failed for field=%s: %s", service_id, field, e)
                shutil.rmtree(tmp_field_dir, ignore_errors=True)
                continue

            with _get_service_lock(lock_key):
                _publish_field_partitions(tmp_field_dir, rollups_dir, field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
    finally:
        con.close()


# ── Closed-day compaction (item 17 / RC-9) ──────────────────────────────────


def compact_closed_days_to_daily(service_id: str, source: dict) -> int:
    """Consolidate closed-day per-hour rollup parquet into per-day parquet.

    For each (field, closed-day) tuple where either (a) no per-day parquet
    exists, or (b) some constituent per-hour parquet has a newer mtime
    than the per-day parquet, rebuild the per-day parquet by summing the
    24 hour parquets into one. Active (current UTC) day is always skipped
    — it's still being written.

    The per-day file is written via DuckDB COPY to a temp path and
    renamed into place under the per-service iceberg lock so concurrent
    `execute_top_n_rollups` readers never see a half-written file. On
    failure the per-day file is left in its previous state and the
    reader transparently falls back to per-hour parquet.

    Returns the count of (field, day) tuples that were rebuilt.

    Operators can call this from a maintenance script or wire it into a
    daily cron. The reader works whether or not this has ever run — when
    a per-day file is missing, `execute_top_n_rollups` reads the source
    per-hour files. When present, it reads ONE file per closed day per
    field instead of 24, slashing the file-open overhead that dominates
    dashboard cold-load wall time on 7-day queries (1,512 → 30-some
    files per the local audit).
    """
    import duckdb

    from backend.core.iceberg.view import _get_service_lock

    hour_root = _rollups_root(source)
    day_root = _day_rollups_root(source)
    if not os.path.isdir(hour_root):
        return 0

    active_day = datetime.now(UTC).strftime("%Y-%m-%d")
    lock_key = source.get("name", "default")
    rebuilt = 0

    # In-memory DuckDB — we only need it to run COPY against parquet files
    # on the local filesystem. Opening the per-service ``.duckdb`` file
    # would contend with uvicorn's RW connection on the SAME file (held
    # for view rebuilds), since DuckDB does not allow mixed RW+RO from
    # one path. On the 2026-06-06 prod incident an RO ``get_connection``
    # blocked 5+ minutes on that lock and the compaction never produced
    # any per-day files. ``:memory:`` sidesteps the contention entirely
    # — the compaction reads + writes parquet via DuckDB's I/O layer,
    # never touching any persistent DuckDB database.
    con = duckdb.connect(":memory:")
    try:
        for field_entry in sorted(os.listdir(hour_root)):
            if not field_entry.startswith("field="):
                continue
            field = field_entry[len("field=") :]
            if not _is_safe_ident(field):
                continue
            field_hour_dir = os.path.join(hour_root, field_entry)
            # Bucket hour-dirs by their YYYY-MM-DD prefix.
            by_day: dict[str, list[str]] = {}
            try:
                hour_entries = os.listdir(field_hour_dir)
            except OSError:
                continue
            for hour_entry in hour_entries:
                if not hour_entry.startswith("hour="):
                    continue
                hour = hour_entry[len("hour=") :]
                # hour shape: YYYY-MM-DD-HH — first 10 chars are the day.
                if len(hour) < 13:
                    continue
                day = hour[:10]
                if day == active_day:
                    continue
                hour_dir = os.path.join(field_hour_dir, hour_entry)
                try:
                    for fname in os.listdir(hour_dir):
                        if fname.endswith(".parquet"):
                            by_day.setdefault(day, []).append(os.path.join(hour_dir, fname))
                except OSError:
                    continue

            for day, hour_paths in by_day.items():
                if not hour_paths:
                    continue
                day_dir = os.path.join(day_root, field_entry, f"day={day}")
                day_file = os.path.join(day_dir, "compacted.parquet")
                # Skip if the per-day file is newer than every source hour
                # parquet — already up to date.
                try:
                    day_mtime = os.path.getmtime(day_file)
                    max_hour_mtime = max(os.path.getmtime(p) for p in hour_paths)
                    if day_mtime >= max_hour_mtime:
                        continue
                except OSError:
                    pass  # day file missing → rebuild

                tmp_file = os.path.join(day_dir, f".tmp_{uuid.uuid4().hex[:12]}.parquet")
                os.makedirs(day_dir, exist_ok=True)
                paths_sql = ", ".join("'" + p.replace("'", "''") + "'" for p in hour_paths)
                # CAST to BIGINT so the per-day file's count column matches
                # the per-hour files (which are BIGINT). The reader's
                # UNION ALL of day + hour requires matching column types
                # per column; without this CAST, the day file lands as
                # DOUBLE and the union breaks (and the dashboard top-N
                # tabs go blank — 2026-06-06 incident).
                copy_sql = f"""
                    COPY (
                        SELECT field, value, CAST(SUM(count) AS BIGINT) AS count
                        FROM read_parquet([{paths_sql}], hive_partitioning=1)
                        GROUP BY field, value
                    ) TO '{tmp_file}'
                    (FORMAT PARQUET, COMPRESSION ZSTD)
                """
                try:
                    con.execute(copy_sql)
                except duckdb.Error as e:
                    logger.warning(
                        "[rollups] %s: day-compact COPY failed for %s/%s: %s",
                        service_id,
                        field,
                        day,
                        e,
                    )
                    try:
                        os.remove(tmp_file)
                    except OSError:
                        pass
                    continue

                with _get_service_lock(lock_key):
                    try:
                        os.replace(tmp_file, day_file)
                        rebuilt += 1
                    except OSError as e:
                        logger.warning("[rollups] %s: rename to %s failed: %s", service_id, day_file, e)
                        try:
                            os.remove(tmp_file)
                        except OSError:
                            pass
    finally:
        con.close()

    return rebuilt
