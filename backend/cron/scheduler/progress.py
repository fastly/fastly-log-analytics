"""Cron progress tracking, zombie run reaping, SSE publisher, and cached sync status."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from backend.celery_app import _InProcessPublisher
from backend.config import SSE_BACKPLANE
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.dir_size import _get_dir_size

logger = logging.getLogger("backend.scheduler")

_progress: dict[int, list[dict]] = {}
_last_update: dict[int, float] = {}
_run_metadata: dict[int, dict] = {}
_lock = threading.Lock()
_terminal_run_ids: set[int] = set()
_TERMINAL_CACHE_CAP = 4096


def start_progress(run_id: int | None, service_id: str | None = None, task: str | None = None):
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
    if not service_id:
        return False
    try:
        from backend.core import metadata as metadata_db

        status = metadata_db.get_cron_run_status(service_id, run_id)
        return status in ("success", "error")
    except Exception:
        return False


def reap_zombie_runs() -> int:
    now = time.time()
    evicted = 0
    with _lock:
        for run_id in list(_run_metadata.keys()):
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                events = _progress.get(run_id) or []
                if not events or events[-1].get("type") not in ("done", "error"):
                    _progress.setdefault(run_id, []).append(
                        {"type": "error", "message": "scheduler reaped zombie cron (no progress in 5m)"}
                    )
                _run_metadata.pop(run_id, None)
                evicted += 1
        return evicted


def add_progress(run_id: int | None, event: dict):
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
        return list(_progress[run_id][start_idx:])


def get_latest_progress_for_service(service_id: str) -> dict | None:
    with _lock:
        active_runs = []
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") == service_id and run_id in _progress:
                events = _progress[run_id]
                if events and events[-1].get("type") in ("done", "error"):
                    continue
                active_runs.append(run_id)
        if not active_runs:
            return None
        run_id = max(active_runs)
        if _progress[run_id]:
            latest = _progress[run_id][-1].copy()
            latest["task"] = _run_metadata[run_id].get("task")
            return latest
        return {"task": _run_metadata[run_id].get("task")}


def end_progress(run_id: int | None, final_event: dict | None = None):
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


_CacheRegistry.register("cron_progress._progress", _progress)
_CacheRegistry.register("cron_progress._last_update", _last_update)
_CacheRegistry.register("cron_progress._run_metadata", _run_metadata)
_CacheRegistry.register("cron_progress._terminal_run_ids", _terminal_run_ids)


class CronRunsPublisher(_InProcessPublisher):
    def __init__(self) -> None:
        super().__init__(replay_size=0)


publisher: Any
if SSE_BACKPLANE == "valkey":
    from backend.utils.telemetry import ValkeyPublisher

    publisher = ValkeyPublisher("cron-runs")
else:
    publisher = CronRunsPublisher()


def compute_sync_status_cached(service_id: str | None) -> dict | None:
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        return None
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    from backend.high_scale.dashboard import header_metrics
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    high_scale_service = get_high_scale_service_registry().resolve(service_id)
    if high_scale_service is not None:
        metrics = header_metrics(high_scale_service)
        try:
            from backend.core.metadata.usage_log import latest_cron_per_task

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
            "storage_mode": src.get("storage_mode", "cloud"),
        }
    lookup_key = service_id or src.get("service_id") or src.get("name", "")
    cached_status = svcconfig.get_status(lookup_key) or (svcconfig.get_status(src["name"]) if "name" in src else {})
    if not cached_status:
        return None  # fall through to dedicated endpoint
    request_status = cached_status.get("request")
    if isinstance(request_status, dict):
        cached_status["latest_log_at"] = request_status.get("latest_log_at")
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


__all__ = [
    "CronRunsPublisher",
    "SSE_BACKPLANE",
    "_CacheRegistry",
    "_InProcessPublisher",
    "_STALE_AFTER_SECONDS",
    "_TERMINAL_CACHE_CAP",
    "_db_status_is_terminal",
    "_get_dir_size",
    "_last_update",
    "_lock",
    "_progress",
    "_run_metadata",
    "_terminal_run_ids",
    "active_task_for_service",
    "add_progress",
    "cleanup_progress",
    "cleanup_progress_and_reap",
    "compute_sync_status_cached",
    "end_progress",
    "get_latest_progress_for_service",
    "get_progress",
    "list_active_runs",
    "logger",
    "publisher",
    "reap_zombie_runs",
    "start_progress",
    "time",
]
