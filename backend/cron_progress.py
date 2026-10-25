from __future__ import annotations

import logging
import os
import sys as _sys
import threading
import time
from typing import Any

from backend.celery_app import _InProcessPublisher
from backend.config import SSE_BACKPLANE
from backend.utils.dir_size import _get_dir_size

logger = logging.getLogger(__name__)

_progress: dict[int, list[dict]] = {}
_last_update: dict[int, float] = {}
_run_metadata: dict[int, dict] = {}
_lock = threading.Lock()

# Run IDs we've already confirmed are in a terminal DB state. Once a
# cron_runs row reads ``status IN ('success', 'error')`` it never goes
# back, so the SQLite check per run_id per list_active_runs() call is
# pure waste from the second invocation onwards. The /admin page polls
# at 5 s (was 1 s before perf item #12 landed) and the snapshot can
# contain 100+ candidates; the audit measured 422 cron_runs SELECTs
# per page load before this cache went in.
_terminal_run_ids: set[int] = set()
_TERMINAL_CACHE_CAP = 4096


def start_progress(run_id: int | None, service_id: str | None = None, task: str | None = None):
    # run_id can be None when start_cron_run failed to register the run
    # in per-service SQLite (e.g. table write contention). The cron job
    # still runs; progress tracking is a no-op for that run.
    if run_id is None:
        return
    with _lock:
        if run_id not in _progress:
            now = time.time()
            _progress[run_id] = []
            _last_update[run_id] = now
            _run_metadata[run_id] = {
                "service_id": service_id,
                "task": task,
                "started_at": now,
            }


_STALE_AFTER_SECONDS = 300  # 5 min — covers slow syncs, kills zombie entries


def list_active_runs() -> list[dict]:
    """Return metadata for runs that are GENUINELY in flight.

    A run is considered active when ALL of these hold:
      1. It's in ``_run_metadata`` (was started_progress'd)
      2. Its last progress event is NOT terminal (done/error)
      3. Its ``_last_update`` was within the last 5 minutes
      4. The persisted ``cron_runs.status`` is still ``'running'``

    Condition (4) is the DB-truth backstop: when an APScheduler
    watchdog abandons a worker thread (interpreter shutdown, OOM
    kill, executor recycle) or some other path completes ``log_cron_run``
    without firing the in-memory ``end_progress``, the in-memory dict
    falsely shows the run as in-flight even though the DB knows it
    succeeded. Production observed 13+ such ghosts on 2026-06-03
    after a backend restart — DB rows said ``status='success'`` with
    durations of 2-6 seconds while the in-memory dict held them as
    active for 100+ seconds. Cross-checking against the DB gives a
    correct answer regardless of what happened to the in-memory
    state.

    Condition (3) covers the residual: a run whose DB write also got
    skipped (something crashed before ``log_cron_run``). After 5 min
    of zero progress, we declare it a zombie regardless.
    """
    now = time.time()
    with _lock:
        candidates = []
        for run_id, meta in _run_metadata.items():
            events = _progress.get(run_id) or []
            if events and events[-1].get("type") in ("done", "error"):
                continue
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                continue
            candidates.append((run_id, meta))

    # DB cross-check happens OUTSIDE the lock so a slow SQLite call
    # doesn't block other progress operations. Short-circuit on the
    # _terminal_run_ids memo first — terminal status never reverts, so
    # the per-poll cron_runs SELECT only needs to run once per run_id
    # over the process's lifetime (the audit measured 422 of these per
    # /admin load before this).
    out = []
    for run_id, meta in candidates:
        if run_id in _terminal_run_ids:
            continue
        if _db_status_is_terminal(meta.get("service_id"), run_id):
            if len(_terminal_run_ids) < _TERMINAL_CACHE_CAP:
                _terminal_run_ids.add(run_id)
            continue
        entry = {"run_id": run_id}
        entry.update(meta)
        out.append(entry)
    return out


def active_task_for_service(service_id: str, tasks: tuple[str, ...]) -> str | None:
    """In-memory only: the first of ``tasks`` with a live run for ``service_id``,
    else None. Same liveness rule as ``list_active_runs`` minus its DB
    cross-check, so it is cheap enough for a cron to poll between work units;
    a ghost entry ages out after ``_STALE_AFTER_SECONDS`` without updates."""
    now = time.time()
    with _lock:
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") != service_id or meta.get("task") not in tasks:
                continue
            events = _progress.get(run_id) or []
            if events and events[-1].get("type") in ("done", "error"):
                continue
            if now - _last_update.get(run_id, now) > _STALE_AFTER_SECONDS:
                continue
            return meta["task"]
    return None


def _db_status_is_terminal(service_id: str | None, run_id: int) -> bool:
    """Return True if the cron_runs row for this run_id has a terminal
    status ('success' or 'error') in per-service SQLite.

    Best-effort: any DB error (missing service, table not yet created,
    SQLite locked) returns False so the in-memory truth still serves
    the badge (we'd rather show one false-in-flight than hide a
    genuinely running one).
    """
    if not service_id:
        return False
    try:
        from backend.core import metadata as metadata_db

        status = metadata_db.get_cron_run_status(service_id, run_id)
        return status in ("success", "error")
    except Exception:
        return False


def reap_zombie_runs() -> int:
    """Eagerly evict zombie run metadata from in-memory state.

    Mirrors list_active_runs' staleness check but actually mutates
    the dicts. Called from the scheduler's per-tick cleanup so
    /admin/health-snapshot doesn't drift by minutes between sync
    ticks. Returns the count evicted for log telemetry.
    """
    now = time.time()
    evicted = 0
    with _lock:
        for run_id in list(_run_metadata.keys()):
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                events = _progress.get(run_id) or []
                # Stale + no terminal event = zombie. Append a synthetic
                # error so any SSE subscriber sees the run ended.
                if not events or events[-1].get("type") not in ("done", "error"):
                    _progress.setdefault(run_id, []).append(
                        {"type": "error", "message": "scheduler reaped zombie cron (no progress in 5m)"}
                    )
                _run_metadata.pop(run_id, None)
                evicted += 1
        return evicted


def add_progress(run_id: int | None, event: dict):
    # See start_progress for why run_id can be None — no-op in that case.
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            _progress[run_id].append(event)
            _last_update[run_id] = time.time()


def get_progress(run_id: int | None, start_idx: int = 0, service_id: str | None = None) -> list[dict] | None:
    if run_id is None:
        return None
    with _lock:
        if run_id not in _progress:
            return None
        if service_id and _run_metadata.get(run_id, {}).get("service_id") != service_id:
            return None
        # Return a copy of the slice to avoid race conditions when the caller iterates over it
        return list(_progress[run_id][start_idx:])


def get_latest_progress_for_service(service_id: str) -> dict | None:
    """Return the latest progress event for any active run belonging to this service."""
    with _lock:
        # Find active runs for this service
        active_runs = []
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") == service_id and run_id in _progress:
                events = _progress[run_id]
                # If the last event is "done" or "error", this run is no longer active
                if events and events[-1].get("type") in ("done", "error"):
                    continue
                active_runs.append(run_id)

        if not active_runs:
            return None

        # Take the newest one
        run_id = max(active_runs)
        if _progress[run_id]:
            # Return the last event + the task type
            latest = _progress[run_id][-1].copy()
            latest["task"] = _run_metadata[run_id].get("task")
            return latest
        return {"task": _run_metadata[run_id].get("task")}


def end_progress(run_id: int | None, final_event: dict | None = None):
    """Mark a cron run as ended."""
    # See start_progress for why run_id can be None — no-op in that case.
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            events = _progress[run_id]
            last_type = events[-1].get("type") if events else None
            if final_event:
                _progress[run_id].append(final_event)
            elif last_type not in ("done", "error"):
                _progress[run_id].append({"type": "done"})
            _last_update[run_id] = time.time()


def cleanup_progress_and_reap():
    """Convenience helper that runs cleanup_progress + reap_zombie_runs."""
    cleanup_progress()
    return reap_zombie_runs()


def cleanup_progress():
    now = time.time()
    with _lock:
        expired = [k for k, v in _last_update.items() if now - v > 3600]  # 1 hour TTL
        for k in expired:
            _progress.pop(k, None)
            _last_update.pop(k, None)
            _run_metadata.pop(k, None)


# R-1: register the cross-test leak-surface globals so the autouse
# fixture in tests/conftest.py drains them via CacheRegistry.clear_all()
# rather than hand-clearing each one. _lock is left alone (not a
# cache, not state).
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron_progress._progress", _progress)
_CacheRegistry.register("cron_progress._last_update", _last_update)
_CacheRegistry.register("cron_progress._run_metadata", _run_metadata)
_CacheRegistry.register("cron_progress._terminal_run_ids", _terminal_run_ids)


# ── In-process pub/sub for cron-run state-change events (folded from cron_runs_publisher.py) ──


class CronRunsPublisher(_InProcessPublisher):
    def __init__(self) -> None:
        super().__init__(replay_size=0)


publisher: Any
if SSE_BACKPLANE == "valkey":
    from backend.utils.telemetry import ValkeyPublisher

    publisher = ValkeyPublisher("cron-runs")
else:
    publisher = CronRunsPublisher()


# ── Cached sync-status snapshot (folded from sync_status_snapshot.py) ─────────


def compute_sync_status_cached(service_id: str | None) -> dict | None:
    """Return the cached sync-status payload for ``service_id`` without
    grabbing a DuckDB connection.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        return None
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    from backend.high_scale.dashboard import header_metrics
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    high_scale_service = get_high_scale_service_registry().resolve(service_id)
    if high_scale_service is not None:
        metrics = header_metrics(high_scale_service)
        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            latest_crons = latest_cron_per_task(service_id)

            req_sync = None
            for t in ("log_discovery", "commit", "ledger_sweep"):
                if t in latest_crons and latest_crons[t].get("started_at"):
                    ts = latest_crons[t]["started_at"]
                    if req_sync is None or ts > req_sync:
                        req_sync = ts
            if req_sync and metrics.get("request"):
                metrics["request"]["last_sync_at"] = req_sync

            rum_sync = None
            for t in ("rum_discovery", "rum_sync", "ledger_rum_sweep"):
                if t in latest_crons and latest_crons[t].get("started_at"):
                    ts = latest_crons[t]["started_at"]
                    if rum_sync is None or ts > rum_sync:
                        rum_sync = ts
            if rum_sync and metrics.get("rum"):
                metrics["rum"]["last_sync_at"] = rum_sync
        except Exception:
            pass

        return {
            "configured": True,
            **metrics,
            "access_level": src.get("access_level", "read_write"),
            "storage_mode": src.get("storage_mode", "cloud"),
        }
    lookup_key = service_id or src.get("service_id") or src.get("name", "")
    cached_status = svcconfig.get_status(lookup_key) or (svcconfig.get_status(src["name"]) if "name" in src else {})
    if not cached_status:
        return None  # fall through to dedicated endpoint

    # Request metrics are queryable event-time extents, not raw-object times.
    # Prefer the current request snapshot even when it moves backwards or is
    # explicitly empty after a reset. Never substitute RUM or a filename.
    request_status = cached_status.get("request")
    if isinstance(request_status, dict):
        cached_status["latest_log_at"] = request_status.get("latest_log_at")

    # Real-time SQLite/PostgreSQL metadata lookup overlay to bypass stale on-disk config cache
    try:
        import re

        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(lookup_key)
        if not summary.get("latest_file_name") and "name" in src:
            summary = metadata_db.get_ingested_files_status_summary(src["name"]) or summary
        latest_file_name = summary.get("latest_file_name")
        total_rows = summary.get("total_rows") or 0

        try:
            from backend.core.metadata.base import get_con as _get_meta_con

            lrow = (
                _get_meta_con(service_id)
                .execute(
                    "SELECT object_key FROM ingest_ledger WHERE service_id = ? AND status = 'committed' "
                    "ORDER BY object_key DESC LIMIT 1",
                    (service_id,),
                )
                .fetchone()
            )
            ledger_latest = lrow[0] if lrow else None
            if ledger_latest and (
                not latest_file_name or ledger_latest.split("/")[-1] > latest_file_name.split("/")[-1]
            ):
                latest_file_name = ledger_latest
        except Exception:
            pass

        if latest_file_name:
            fname = latest_file_name.split("/")[-1]
            m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
            if m:
                latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"
                cached_status["latest_ingested_file_at"] = latest_ingested_file_at
                cached_status["latest_available_file_at"] = latest_ingested_file_at

        if request_status is None and total_rows > 0 and cached_status.get("local_rows") is None:
            cached_status["local_rows"] = total_rows
    except Exception as e:
        logger.debug("[compute_sync_status_cached] failed to overlay real-time SQLite metadata: %s", e)

    cached_status["access_level"] = src.get("access_level", "read_write")
    cached_status["storage_mode"] = src.get("storage_mode", "cloud")
    cached_status["configured"] = True

    db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0
    cache_size = _get_dir_size(_db._cache_dir(src))
    cached_status["duckdb_size_bytes"] = db_size + cache_size
    cached_status["duckdb_exists"] = db_exists

    active_run = get_latest_progress_for_service(service_id)
    if active_run:
        cached_status["active_run"] = active_run
        cached_status["busy"] = True

    cfg = svcconfig.load_config(service_id) or {}
    cached_status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")
    return cached_status


_self_mod = _sys.modules[__name__]
for _alias in (
    "backend.cron_runs_publisher",
    "backend.sync_status_snapshot",
):
    _sys.modules[_alias] = _self_mod
if "backend" in _sys.modules:
    _backend_pkg = _sys.modules["backend"]
    _backend_pkg.cron_runs_publisher = _self_mod  # type: ignore[attr-defined]
    _backend_pkg.sync_status_snapshot = _self_mod  # type: ignore[attr-defined]
