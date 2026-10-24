"""Celery/APScheduler/ledger status snapshot, shared by the admin route, the
admin SSE feeder, and the health snapshot.

Lives at backend top level (beside ``celery_app``) so non-router callers
(``celery_status_sampler``, ``routers/admin/health``) don't import through
the routers package.

Design notes:
- One module-level sync Redis client, created lazily and reused — the old
  per-call ``Redis.from_url`` churned a TCP connect per admin poll.
- ``SCAN``-based key discovery only; ``KEYS`` is O(keyspace) and blocks the
  broker for every other client.
- ``broker_reachable`` distinguishes "idle" from "Valkey is down" — the old
  shape collapsed both to empty dicts.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.celery_queue import (
    KNOWN_QUEUES as KNOWN_QUEUES,
)
from backend.celery_queue import (
    celery_queue_depths as celery_queue_depths,
)

logger = logging.getLogger(__name__)


def ingest_ledger_summary() -> dict[str, int]:
    """Aggregate ingest_ledger status counts across all configured services."""
    ledger: dict[str, int] = {}
    try:
        from backend.config import list_configs
        from backend.core.metadata.base import get_con

        for svc in list_configs():
            sid = svc.get("service_id") or svc.get("name")
            if not sid:
                continue
            try:
                con = get_con(sid)
                cur = con.cursor()
                cur.execute(
                    "SELECT status, count(*) FROM ingest_ledger WHERE service_id = ? GROUP BY status",
                    (sid,),
                )
                for status, count in cur.fetchall():
                    ledger[status] = ledger.get(status, 0) + int(count)
            except Exception:
                continue
    except Exception as e:
        logger.debug("[celery_status] ledger summary failed: %s", e)
    return ledger


def inprocess_schedule_entries() -> list[dict[str, Any]]:
    """List registered in-process APScheduler jobs as {name, trigger, next_run_at} dicts."""
    entries: list[dict[str, Any]] = []
    try:
        from backend.cron.scheduler import get_scheduler

        sched = get_scheduler()
        inner_sched = getattr(sched, "_sched", None) or getattr(sched, "scheduler", None)
        for job in inner_sched.get_jobs() if inner_sched is not None else []:
            next_run = getattr(job, "next_run_time", None)
            next_ts: float | None = None
            if next_run is not None:
                try:
                    next_ts = float(next_run.timestamp())
                except Exception:
                    next_ts = None
            entries.append(
                {
                    "name": str(job.id),
                    "trigger": str(job.trigger),
                    "next_run_at": next_ts,
                }
            )
        entries.sort(key=lambda item: item["name"])
    except Exception as e:
        logger.debug("[celery_status] inprocess_schedule_entries failed: %s", e)
    return entries


def inprocess_active_runs_entries() -> list[dict[str, Any]]:
    """Return currently executing in-process cron runs from cron_progress."""
    out: list[dict[str, Any]] = []
    try:
        from backend.cron_progress import list_active_runs

        for item in list_active_runs():
            run_id = item.get("run_id")
            if run_id is None:
                continue
            started_at = item.get("started_at")
            out.append(
                {
                    "run_id": int(run_id),
                    "service_id": str(item["service_id"]) if item.get("service_id") else None,
                    "task": str(item["task"]) if item.get("task") else None,
                    "started_at": float(started_at) if isinstance(started_at, (int, float)) else None,
                }
            )
    except Exception as e:
        logger.debug("[celery_status] inprocess_active_runs_entries failed: %s", e)
    return out


def standard_ingest_summary() -> dict[str, int]:
    """Aggregate standard-mode ingested_files and quarantine counts across all configured services."""
    ingested_files = 0
    quarantined_items = 0
    try:
        from backend.config import list_configs
        from backend.core.metadata.base import get_con

        sids = list(dict.fromkeys(sid for svc in list_configs() if (sid := (svc.get("service_id") or svc.get("name")))))
        if sids:
            placeholders = ", ".join("?" for _ in sids)
            con = get_con(sids[0])
            cur = con.cursor()
            try:
                cur.execute(
                    f"SELECT COALESCE(SUM(file_count), 0) FROM ingested_files_summary WHERE source_name IN ({placeholders})",
                    tuple(sids),
                )
                rows = cur.fetchall()
                if rows and len(rows[0]) == 1 and isinstance(rows[0][0], (int, float)):
                    ingested_files = int(rows[0][0])
            except Exception:
                pass
            if ingested_files == 0:
                try:
                    cur.execute(
                        f"SELECT count(*) FROM ingested_files WHERE source_name IN ({placeholders})",
                        tuple(sids),
                    )
                    rows = cur.fetchall()
                    if rows and len(rows[0]) == 1 and isinstance(rows[0][0], (int, float)):
                        ingested_files = int(rows[0][0])
                except Exception:
                    pass
    except Exception as e:
        logger.debug("[celery_status] standard_ingest_summary failed: %s", e)
    return {
        "ingested_files": ingested_files,
        "quarantined_items": quarantined_items,
    }


def get_celery_status(timeout: float = 0.25) -> dict[str, Any]:
    """Full status payload for the admin endpoint / SSE feeder."""
    from backend import config as svcconfig
    from backend.celery_app import app

    is_high_scale = bool(svcconfig.is_high_scale_mode())
    deployment_mode = "high_scale" if is_high_scale else "standard"

    queues, broker_reachable = celery_queue_depths()

    workers_error: str | None = None
    active: dict[str, Any] = {}
    stats: dict[str, Any] = {}
    registered: dict[str, Any] = {}
    scheduled: dict[str, Any] = {}

    if not broker_reachable:
        workers_error = "Broker unreachable (Broker connection timeout)"
    else:
        try:
            i = app.control.inspect(timeout=timeout)
            stats = i.stats() or {}
            if stats:
                active = i.active() or {}
                registered = i.registered() or {}
                scheduled = i.scheduled() or {}
        except Exception as e:
            workers_error = str(e)[:200]

    ledger = ingest_ledger_summary()

    return {
        "deployment_mode": deployment_mode,
        "broker_reachable": broker_reachable,
        "workers": {
            "active_tasks": active,
            "stats": stats,
            "registered": registered,
            "scheduled": scheduled,
            "count": len(stats) if stats else 0,
            **({"error": workers_error} if workers_error else {}),
        },
        "queues": queues,
        "ledger": ledger,
        "inprocess_schedules": inprocess_schedule_entries(),
        "inprocess_active_runs": inprocess_active_runs_entries(),
        "standard_ingest": standard_ingest_summary(),
    }


_LEDGER_PROBLEM_STATUSES = ("dead_letter", "quarantined")
_LEDGER_VALID_STATUSES = ("discovered", "claimed", "committed", "quarantined", "dead_letter")
_LEDGER_DISPATCH_BATCH_SIZE = 25


def _is_rum_object_key(object_key: str) -> bool:
    return object_key.startswith("raw/rum/") or "/raw/rum/" in object_key


def _resolve_ledger_service_ids(service_id: str | None = None) -> list[str]:
    if service_id:
        return [service_id]
    from backend.config import list_configs

    seen: list[str] = []
    for svc in list_configs():
        sid = svc.get("service_id") or svc.get("name")
        if sid and sid not in seen:
            seen.append(sid)
    return seen


def list_ingest_ledger_entries(
    status: str | None = "problem",
    service_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List rows from ``ingest_ledger`` across configured services (or a single
    service), defaulting to problem rows (``dead_letter`` + ``quarantined``)."""
    from backend.core.metadata.base import get_con

    norm_status = (status or "problem").strip().lower()
    entries: list[dict[str, Any]] = []
    total = 0
    seen_keys: set[tuple[str, str]] = set()
    per_service_limit = max(1, int(offset) + int(limit))

    for sid in _resolve_ledger_service_ids(service_id):
        try:
            con = get_con(sid)
            cur = con.cursor()
            if norm_status == "all":
                where_sql = "WHERE service_id = ?"
                params: tuple[Any, ...] = (sid,)
            elif norm_status in _LEDGER_VALID_STATUSES:
                where_sql = "WHERE service_id = ? AND status = ?"
                params = (sid, norm_status)
            else:
                where_sql = "WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')"
                params = (sid,)

            cur.execute(f"SELECT count(*) FROM ingest_ledger {where_sql}", params)
            count_row = cur.fetchone()
            total += int(count_row[0]) if count_row and count_row[0] is not None else 0

            cur.execute(
                f"SELECT service_id, object_key, status, attempts, error_kind, last_error, "
                f"claimed_by, claimed_at, discovered_at, committed_at, next_attempt_at, "
                f"originating_task, originating_run_id, size_bytes "
                f"FROM ingest_ledger {where_sql} "
                f"ORDER BY COALESCE(claimed_at, discovered_at, committed_at, 0) DESC, object_key DESC "
                f"LIMIT ?",
                (*params, per_service_limit),
            )
            for row in cur.fetchall():
                row_sid = str(row[0])
                obj_key = str(row[1])
                if (row_sid, obj_key) in seen_keys:
                    continue
                seen_keys.add((row_sid, obj_key))
                entries.append(
                    {
                        "service_id": row_sid,
                        "object_key": obj_key,
                        "source_type": "rum" if _is_rum_object_key(obj_key) else "request",
                        "status": str(row[2]),
                        "attempts": int(row[3] or 0),
                        "error_kind": row[4],
                        "last_error": row[5],
                        "claimed_by": row[6],
                        "claimed_at": float(row[7]) if row[7] is not None else None,
                        "discovered_at": float(row[8]) if row[8] is not None else None,
                        "committed_at": float(row[9]) if row[9] is not None else None,
                        "next_attempt_at": float(row[10]) if row[10] is not None else None,
                        "originating_task": row[11],
                        "originating_run_id": int(row[12]) if row[12] is not None else None,
                        "size_bytes": int(row[13]) if row[13] is not None else None,
                    }
                )
        except Exception as e:
            logger.debug("[celery_status] list_ingest_ledger_entries failed for %s: %s", sid, e)
            continue

    entries.sort(
        key=lambda item: (
            float(item.get("claimed_at") or item.get("discovered_at") or item.get("committed_at") or 0.0),
            str(item.get("object_key") or ""),
        ),
        reverse=True,
    )
    return {
        "items": entries[offset : offset + limit],
        "total": total,
    }


def purge_ingest_ledger_entries(
    items: list[dict[str, str]] | None = None,
    status: str | None = None,
    service_id: str | None = None,
) -> dict[str, Any]:
    """Delete ``dead_letter`` / ``quarantined`` rows from ``ingest_ledger``.
    Never deletes ``discovered``, ``claimed``, or ``committed`` rows."""
    from backend.core.metadata.base import get_con

    affected = 0
    if items:
        by_service: dict[str, list[str]] = {}
        for item in items:
            sid = (item.get("service_id") or "").strip()
            key = (item.get("object_key") or "").strip()
            if sid and key:
                by_service.setdefault(sid, []).append(key)
        for sid, keys in by_service.items():
            try:
                con = get_con(sid)
                cur = con.cursor()
                for key in keys:
                    cur.execute(
                        "DELETE FROM ingest_ledger "
                        "WHERE service_id = ? AND object_key = ? "
                        "AND status IN ('dead_letter', 'quarantined')",
                        (sid, key),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                con.commit()
            except Exception as e:
                logger.warning("[celery_status] purge_ingest_ledger_entries failed for %s: %s", sid, e)
    else:
        norm_status = (status or "problem").strip().lower()
        for sid in _resolve_ledger_service_ids(service_id):
            try:
                con = get_con(sid)
                cur = con.cursor()
                if norm_status in _LEDGER_PROBLEM_STATUSES:
                    cur.execute(
                        "DELETE FROM ingest_ledger WHERE service_id = ? AND status = ?",
                        (sid, norm_status),
                    )
                else:
                    cur.execute(
                        "DELETE FROM ingest_ledger WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')",
                        (sid,),
                    )
                if cur.rowcount and cur.rowcount > 0:
                    affected += int(cur.rowcount)
                con.commit()
            except Exception as e:
                logger.warning("[celery_status] bulk purge_ingest_ledger_entries failed for %s: %s", sid, e)

    return {"ok": True, "affected": affected, "dispatched": 0}


def retry_ingest_ledger_entries(
    items: list[dict[str, str]] | None = None,
    status: str | None = None,
    service_id: str | None = None,
) -> dict[str, Any]:
    """Reset ``dead_letter`` / ``quarantined`` rows in ``ingest_ledger`` back to
    ``discovered`` (with ``attempts=0`` and cleared error fields) and best-effort
    dispatch Celery convert batches for immediate retry."""
    from backend.core.metadata.base import get_con

    requeued_by_service: dict[str, list[str]] = {}
    affected = 0

    if items:
        by_service: dict[str, list[str]] = {}
        for item in items:
            sid = (item.get("service_id") or "").strip()
            key = (item.get("object_key") or "").strip()
            if sid and key:
                by_service.setdefault(sid, []).append(key)
        for sid, keys in by_service.items():
            try:
                con = get_con(sid)
                cur = con.cursor()
                for key in keys:
                    cur.execute(
                        "UPDATE ingest_ledger "
                        "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                        "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                        "WHERE service_id = ? AND object_key = ? "
                        "AND status IN ('dead_letter', 'quarantined')",
                        (sid, key),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        affected += int(cur.rowcount)
                        requeued_by_service.setdefault(sid, []).append(key)
                con.commit()
            except Exception as e:
                logger.warning("[celery_status] retry_ingest_ledger_entries failed for %s: %s", sid, e)
    else:
        norm_status = (status or "problem").strip().lower()
        for sid in _resolve_ledger_service_ids(service_id):
            try:
                con = get_con(sid)
                cur = con.cursor()
                if norm_status in _LEDGER_PROBLEM_STATUSES:
                    cur.execute(
                        "SELECT object_key FROM ingest_ledger WHERE service_id = ? AND status = ?",
                        (sid, norm_status),
                    )
                    keys = [str(r[0]) for r in cur.fetchall()]
                    if keys:
                        cur.execute(
                            "UPDATE ingest_ledger "
                            "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                            "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                            "WHERE service_id = ? AND status = ?",
                            (sid, norm_status),
                        )
                        if cur.rowcount and cur.rowcount > 0:
                            affected += int(cur.rowcount)
                            requeued_by_service.setdefault(sid, []).extend(keys)
                        con.commit()
                else:
                    cur.execute(
                        "SELECT object_key FROM ingest_ledger "
                        "WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')",
                        (sid,),
                    )
                    keys = [str(r[0]) for r in cur.fetchall()]
                    if keys:
                        cur.execute(
                            "UPDATE ingest_ledger "
                            "SET status = 'discovered', attempts = 0, last_error = NULL, error_kind = NULL, "
                            "claimed_by = NULL, claimed_at = NULL, next_attempt_at = NULL "
                            "WHERE service_id = ? AND status IN ('dead_letter', 'quarantined')",
                            (sid,),
                        )
                        if cur.rowcount and cur.rowcount > 0:
                            affected += int(cur.rowcount)
                            requeued_by_service.setdefault(sid, []).extend(keys)
                        con.commit()
            except Exception as e:
                logger.warning("[celery_status] bulk retry_ingest_ledger_entries failed for %s: %s", sid, e)

    dispatched = 0
    if requeued_by_service:
        try:
            from backend.core.ingest import convert_batch_files, convert_batch_rum_files

            for sid, keys in requeued_by_service.items():
                req_keys = [k for k in keys if not _is_rum_object_key(k)]
                rum_keys = [k for k in keys if _is_rum_object_key(k)]
                for i in range(0, len(req_keys), _LEDGER_DISPATCH_BATCH_SIZE):
                    batch = req_keys[i : i + _LEDGER_DISPATCH_BATCH_SIZE]
                    convert_batch_files.delay(sid, batch)
                    dispatched += len(batch)
                for i in range(0, len(rum_keys), _LEDGER_DISPATCH_BATCH_SIZE):
                    batch = rum_keys[i : i + _LEDGER_DISPATCH_BATCH_SIZE]
                    convert_batch_rum_files.delay(sid, batch)
                    dispatched += len(batch)
        except Exception as e:
            logger.debug("[celery_status] best-effort Celery dispatch on ledger retry skipped: %s", e)

    return {"ok": True, "affected": affected, "dispatched": dispatched}
