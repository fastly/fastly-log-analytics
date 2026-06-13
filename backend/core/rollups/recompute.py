"""Recompute / backfill / cleanup for per-field per-hour rollups.

Holds the cron-triggered ``recompute_touched_hours`` (called after every
ingest tick), the one-shot ``backfill_rollups`` / ``ensure_field_backfills``
(boot + new-custom-field path), and ``cleanup_old_rollups`` (daily retention
trim).

The shared write core (``_run_per_field_copy``) lives here too — it's the
single COPY-PARTITION_BY path both recompute and backfill funnel through.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import UTC, datetime, timedelta

from ._common import (
    _VIRTUAL_FIELD_BACKING,
    _build_copy_query,
    _build_virtual_field_copy_query,
    _get_fields,
    _is_safe_ident,
    _load_markers,
    _publish_field_partitions,
    _rollups_root,
    _safe_table_for,
    _save_markers,
)
from .hour_bundles import bundle_hours
from .sessions import build_session_bundles
from .time_series import build_time_series_bundles

logger = logging.getLogger(__name__)


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
            # Virtual fields rollup the unnested CSV column instead of the
            # column itself — skip-test on the BACKING column, not the
            # virtual name (which never exists in the table schema).
            backing_col = _VIRTUAL_FIELD_BACKING.get(field)
            if backing_col is not None:
                if backing_col not in cols or not _is_safe_ident(backing_col):
                    continue
            elif field not in cols:
                continue

            tmp_field_dir = os.path.join(cache_root, "rollups", "tmp", field)
            shutil.rmtree(tmp_field_dir, ignore_errors=True)
            os.makedirs(tmp_field_dir, exist_ok=True)

            if backing_col is not None:
                inner = _build_virtual_field_copy_query(table_ident, field, backing_col, where_sql)
            else:
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
