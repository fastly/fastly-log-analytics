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
    """Return ``logs_<name>`` iff the service name is a safe identifier."""
    name = source.get("name") or ""
    if not _is_safe_ident(name):
        logger.warning("[rollups] refusing to query unsafe service name: %r", name)
        return None
    return f"logs_{name}"


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


def recompute_touched_hours(service_id: str, source: dict, hours: set[str]) -> None:
    """Recompute rollups for all dashboard fields across the given hours.

    Excludes the active (current UTC) hour — the dashboard serves the
    in-progress hour live off the base table. One COPY query per field
    handles all touched hours via PARTITION_BY, so the work is O(fields)
    not O(fields × hours).
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
    from backend.core.iceberg import _get_service_lock

    cache_root = _cache_dir(source)
    rollups_dir = _rollups_root(source)
    os.makedirs(rollups_dir, exist_ok=True)
    lock_key = source.get("name", "default")

    con = get_connection(source=source, read_only=True)
    try:
        try:
            cols = {c[0] for c in con.execute(f"DESCRIBE {table_ident}").fetchall()}
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
