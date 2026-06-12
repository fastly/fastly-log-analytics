"""Metadata storage stats + age-based cleanup across the per-service SQLite file.

Surfaces row count + bytes per table for the admin dashboard, and the
``cleanup_metadata`` worker that purges aged-out rows from ``usage_log``,
``ingested_files``, and ``cron_runs`` and VACUUMs the file to reclaim space.

Also coordinates the rollup parquet-tree cleanup that lives outside SQLite
under ``<cache>/rollups/...``.
"""

from __future__ import annotations

import logging
import sqlite3
import time as _t
from collections.abc import Callable

from backend.core.metadata.base import db_path, get_con
from backend.core.metadata.usage_log import DEFAULT_METADATA_RETENTION

logger = logging.getLogger(__name__)


# Tables surfaced in the storage stats endpoint. Order matters for the UI.
_STATS_TABLES = (
    "usage_log",
    "ingested_files",
    "cron_runs",
    "alerts",
    "saved_views",
    "audit_log",
    "in_flight_buffers",
    "locally_compacted_files",
)

# (table, retention_key, timestamp_column) for each trimmable table.
_CLEANUP_TABLES = (
    ("usage_log", "usage_log_days", "timestamp"),
    ("ingested_files", "ingested_files_days", "ingested_at"),
    ("cron_runs", "cron_runs_days", "started_at"),
)


def get_metadata_storage_stats(service_id: str) -> dict:
    """Per-table row count + estimated bytes for this service's metadata.db.

    Bytes come from SQLite's ``dbstat`` virtual table (compiled into stock
    Python sqlite3 ≥3.31). If a table doesn't exist (older schema), it's
    omitted rather than erroring. Total ``db_bytes`` is the sum across the
    whole file — including indexes, free pages, and tables not in
    ``_STATS_TABLES``, so it won't equal sum-of-per-table-bytes.
    """
    con = get_con(service_id)
    out: dict[str, dict] = {}
    for t in _STATS_TABLES:
        try:
            rows = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            continue
        try:
            row = con.execute("SELECT sum(pgsize) FROM dbstat WHERE name = ?", (t,)).fetchone()
            bytes_ = int(row[0]) if row and row[0] is not None else 0
        except sqlite3.OperationalError:
            bytes_ = None
        out[t] = {"rows": int(rows or 0), "bytes": bytes_}

    db_bytes: int | None
    try:
        row = con.execute("SELECT sum(pgsize) FROM dbstat").fetchone()
        db_bytes = int(row[0]) if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        db_bytes = None

    return {
        "tables": out,
        "db_bytes": db_bytes,
        "db_path": db_path(service_id),
    }


def is_ingested_files_dedup_active(service_id: str) -> bool:
    """Return True when the ``ingested_files`` table is the active dedup gate.

    The sync's ``delete_after`` flag (default True) makes ingest a destructive
    op: a successfully-ingested .gz is DELETEd from FOS, so a future LIST
    can never re-discover it — the ``ingested_files`` row is vestigial
    after that point. When ``delete_after`` is set to False, the raw files
    stay in FOS forever and the daily ``full_sync`` (cron) does a complete
    LIST; the only thing stopping it from re-ingesting every prior file is
    a matching entry in ``ingested_files``. In that mode the table CANNOT
    be trimmed without causing re-ingestion storms.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = cfg.get("provisioning", {}).get("cron_sync", {}).get("delete_after", True)
    # Treat anything other than an explicit False as safe-to-trim. None,
    # missing, truthy strings — all default to the safe path.
    return delete_after is not False


def cleanup_metadata(
    service_id: str,
    retention: dict | None = None,
    on_event: Callable[[dict], None] | None = None,
) -> dict:
    """Delete rows older than the per-table retention window. VACUUM if any were deleted.

    retention shape: ``{"usage_log_days": int, "ingested_files_days": int,
    "cron_runs_days": int}``. Missing keys fall back to
    ``DEFAULT_METADATA_RETENTION``. A value of 0 (or negative) disables
    cleanup for that table — useful for an analyst-only service that wants
    to retain the full audit trail.

    ``ingested_files_days`` is **force-overridden to 0** when
    ``cron_sync.delete_after`` is False on this service — see
    ``is_ingested_files_dedup_active``. The override is announced via an
    ``on_event`` status message so the operator knows the configured
    retention is being ignored.

    ``on_event``: optional callable receiving event dicts at each milestone
    (status messages, per-table delete results, VACUUM start/end). The
    callback is invoked synchronously from the worker — the manual-cleanup
    endpoint uses a thread-safe queue to bridge to SSE. Event shapes:

        {"type": "status", "message": str}
        {"type": "progress", "current": int, "total": int, "message": str}

    The scheduled cron passes ``on_event=None`` and gets silent operation
    (events still arrive in the function's return dict for logging).

    Returns ``{"deleted": {table: count}, "before": {table: rows},
    "after": {table: rows}, "vacuumed": bool, "duration_s": float}``.
    """

    def _emit(event: dict) -> None:
        if on_event is None:
            return
        try:
            on_event(event)
        except Exception:
            # Never let an event-sink failure abort the cleanup itself.
            pass

    cfg = {**DEFAULT_METADATA_RETENTION, **(retention or {})}

    # Safety override: when cron_sync.delete_after is False, ingested_files
    # is the dedup gate against re-LIST → re-ingest by the daily full_sync.
    # Trimming it would re-ingest every aged-out file. Force-disable the
    # ingested_files retention regardless of what cfg / caller passed,
    # and surface the override so the operator sees why it didn't apply.
    if not is_ingested_files_dedup_active(service_id):
        configured = int(cfg.get("ingested_files_days") or 0)
        if configured > 0:
            _emit(
                {
                    "type": "status",
                    "message": (
                        f"ingested_files retention ({configured}d) ignored — "
                        "cron_sync.delete_after=false makes this table the dedup gate. "
                        "Trimming would cause full_sync to re-ingest aged-out files."
                    ),
                }
            )
        cfg["ingested_files_days"] = 0

    con = get_con(service_id)
    t0 = _t.time()

    # Steps: 3 deletes + 1 vacuum + 1 post-count = 5. Set up the progress
    # framing so the modal can render a determinate bar.
    total_steps = len(_CLEANUP_TABLES) + 2

    _emit({"type": "status", "message": "Reading current row counts…"})
    before: dict[str, int] = {}
    for table, _, _ in _CLEANUP_TABLES:
        try:
            before[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] or 0)
        except sqlite3.OperationalError:
            before[table] = 0

    deleted: dict[str, int] = {}
    for idx, (table, key, ts_col) in enumerate(_CLEANUP_TABLES, start=1):
        days = cfg.get(key)
        try:
            days_int = int(days) if days is not None else 0
        except (TypeError, ValueError):
            days_int = 0
        if days_int <= 0:
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: retention disabled (0 days) — skipped",
                }
            )
            continue
        _emit({"type": "status", "message": f"Trimming {table} (older than {days_int}d)…"})
        try:
            cur = con.execute(
                f"DELETE FROM {table} WHERE {ts_col} < datetime('now', ?)",
                (f"-{days_int} days",),
            )
            deleted[table] = int(cur.rowcount or 0)
            con.commit()
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: deleted {deleted[table]:,} rows (kept rows ≤{days_int}d old)",
                }
            )
        except sqlite3.OperationalError as e:
            logger.warning("[metadata_cleanup] %s: skip %s — %s", service_id, table, e)
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: skipped ({e})",
                }
            )

    vacuumed = False
    if any(deleted.values()):
        # VACUUM cannot run inside an open transaction. Commit + drop the
        # Python wrapper's auto-BEGIN so the next execute() autocommits.
        _emit(
            {
                "type": "status",
                "message": "VACUUMing — rewrites the whole file, may take minutes on large DBs…",
            }
        )
        con.commit()
        old_iso = con.isolation_level
        con.isolation_level = None
        try:
            con.execute("VACUUM")
            vacuumed = True
            _emit(
                {
                    "type": "progress",
                    "current": len(_CLEANUP_TABLES) + 1,
                    "total": total_steps,
                    "message": "VACUUM complete — file shrunk to reflect deletions",
                }
            )
        except sqlite3.OperationalError as e:
            # Locked / busy — not fatal, the delete already shrank the row count.
            logger.warning("[metadata_cleanup] %s: VACUUM skipped — %s", service_id, e)
            _emit(
                {
                    "type": "progress",
                    "current": len(_CLEANUP_TABLES) + 1,
                    "total": total_steps,
                    "message": f"VACUUM skipped ({e}) — row counts already reduced",
                }
            )
        finally:
            con.isolation_level = old_iso
    else:
        _emit(
            {
                "type": "progress",
                "current": len(_CLEANUP_TABLES) + 1,
                "total": total_steps,
                "message": "Nothing deleted — VACUUM skipped (no-op rewrite would waste cycles)",
            }
        )

    after: dict[str, int] = {}
    for table, _, _ in _CLEANUP_TABLES:
        try:
            after[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] or 0)
        except sqlite3.OperationalError:
            after[table] = 0
    _emit(
        {
            "type": "progress",
            "current": total_steps,
            "total": total_steps,
            "message": f"Final counts: {', '.join(f'{t}={n:,}' for t, n in after.items())}",
        }
    )

    # Rollup parquet tree cleanup — independent of the SQLite tables. Skip
    # silently when the rollups module / source aren't available; rollups
    # are an optimisation, never a correctness dependency.
    rollups_deleted = 0
    try:
        rollups_days = int(cfg.get("rollups_days") or 0)
    except (TypeError, ValueError):
        rollups_days = 0
    if rollups_days > 0:
        try:
            from backend.core import rollups as _rollups
            from backend.core.duckdb import get_source_for_service

            src = get_source_for_service(service_id)
            if src is not None:
                rollups_deleted = _rollups.cleanup_old_rollups(service_id, src, rollups_days)
                if rollups_deleted:
                    _emit(
                        {
                            "type": "status",
                            "message": f"Rollups: dropped {rollups_deleted} hour-dir(s) older than {rollups_days}d",
                        }
                    )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: rollups cleanup skipped — %s", service_id, e)

    return {
        "deleted": deleted,
        "before": before,
        "after": after,
        "vacuumed": vacuumed,
        "rollups_deleted": rollups_deleted,
        "duration_s": round(_t.time() - t0, 3),
    }
