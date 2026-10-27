"""Shared helpers for ingest-class and maintenance cron jobs."""

from __future__ import annotations

import logging
import os
import sys as _sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import closing
from typing import Any

from backend import config as svcconfig
from backend.cron.scheduler import (
    _log_and_add_progress,
    dev_mode_no_crons,
)

logger = logging.getLogger("backend.scheduler")


def _resolve_sync_hook(attr_name: str, current: Any, orig: Any) -> Any:
    """Return a monkeypatched attribute from ``sync``, ``_common``, or ``scheduler`` if overridden."""
    if current is not orig:
        return current
    for mod_name in ("backend.cron.jobs.sync", "backend.cron.jobs._common", "backend.cron.scheduler"):
        mod = _sys.modules.get(mod_name)
        if mod is not None and attr_name in mod.__dict__:
            candidate = mod.__dict__[attr_name]
            if candidate is not orig:
                return candidate
    return current


def _refuse_if_dev_no_crons(service_id: str, label: str) -> bool:
    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — %s refused.", service_id, label)
        return True
    return False


def _spawn_local_compaction(
    src: dict, service_id: str, *, compact_active_hour: bool = False, refresh_on_merge: bool = False
) -> None:
    def _worker() -> None:
        try:
            from backend.core import local_compaction as _lc  # type: ignore[attr-defined]

            res = (
                _lc.compact_local_partitions(src, compact_active_hour=True)
                if compact_active_hour
                else _lc.compact_local_partitions(src)
            )
            if refresh_on_merge and isinstance(res, dict) and res.get("files_merged", 0) > 0:
                refresh_fn = _resolve_sync_hook(
                    "refresh_view_and_warm_pool", refresh_view_and_warm_pool, _orig_refresh_view_and_warm_pool
                )
                refresh_fn(src, service_id, log_prefix="[compact-on-sync] ")
        except Exception as e:
            logger.warning("[scheduler] %s: post-sync local compaction failed: %s", service_id, e)

    try:
        threading.Thread(target=_worker, name=f"local-compact-on-sync:{service_id}", daemon=True).start()
    except Exception as e:
        logger.warning("[scheduler] %s: post-sync local compaction failed to launch: %s", service_id, e)


def refresh_view_and_warm_pool(
    source: dict,
    service_id: str,
    *,
    log_prefix: str,
    progress_log: Callable[[dict], None] | None = None,
) -> None:
    """Force-refresh the storage DuckDB view and warm the connection pool.

    Called on the cron writer thread after a commit or a sync tick so the
    next request-path checkout finds a pre-bound view (avoiding the
    slow-path rebuild under the per-service lock).

    Emits a single status message via ``progress_log`` ON SUCCESS ONLY.
    Failures are logged at WARNING through this module's logger; the
    progress feed stays quiet because the prior shape (sync.py) put the
    success message OUTSIDE the try/except and so reported "View refresh +
    warm: Xms" even when the work raised. Fixes that latent mis-log.
    """
    t0 = time.time()
    try:
        from backend.core import storage as _storage
        from backend.core.duckdb import get_connection as _get_conn
        from backend.core.duckdb_pool import warm_pool_for_service as _warm

        with closing(_get_conn(source=source, read_only=True)) as con_v:
            _storage.update_storage_view(con_v, source, force=True)
            _warm(service_id, source)
        if progress_log is not None:
            progress_log(
                {"type": "status", "message": f"{log_prefix}View refresh + warm: {int((time.time() - t0) * 1000)}ms"}
            )
    except Exception as e:
        logger.warning("[scheduler] %s: post-%s view refresh failed: %s", service_id, log_prefix.strip(": "), e)


def _refresh_status_after_commit(service_id: str, source: dict) -> None:
    """Refresh the cached service status and drop stale dashboard cache entries after a commit."""
    from backend.core.duckdb import refresh_config_status
    from backend.repositories.dashboard import invalidate_service

    refresh_config_status(service_id)
    invalidate_service(source["name"])


def finalize_cron_duration(
    src: dict,
    run_id: int | None,
    t_start: float,
    *,
    log_output: str | None = None,
    silent: bool = True,
    clock: Callable[[], float] = time.time,
) -> None:
    """Update the cron-run row's ``duration_s`` (and optionally ``log_output``)."""
    if run_id is None:
        return
    try:
        from backend.core.duckdb import update_cron_duration

        elapsed = clock() - t_start
        if log_output is not None:
            update_cron_duration(src, run_id, elapsed, log_output=log_output)
        else:
            update_cron_duration(src, run_id, elapsed)
    except Exception as e:
        if not silent:
            logger.warning("Failed to update full cron duration: %s", e)


def get_service_display_label(service_id: str, cfg: dict | None = None) -> str:
    """Return ``"{name} ({service_id})"`` when the service name differs from ``service_id``."""
    if cfg is None:
        cfg = svcconfig.load_config(service_id)
    svc_name = (cfg.get("service_name") or cfg.get("name") or service_id) if cfg else service_id
    return f"{svc_name} ({service_id})" if svc_name and svc_name != service_id else service_id


def has_sufficient_disk_space(
    path: str, job_label: str, service_id: str, *, min_free_bytes: int = 50 * 1024 * 1024
) -> tuple[bool, str]:
    """Return ``(ok, msg)`` checking that ``path`` (or ``.``) has at least ``min_free_bytes`` free."""
    import shutil

    try:
        free_bytes = shutil.disk_usage(path if os.path.isdir(path) else ".").free
        if free_bytes < min_free_bytes:
            msg = f"Disk space critically low (< {min_free_bytes // (1024 * 1024)}MB free: {free_bytes / (1024 * 1024):.1f}MB)"
            logger.warning("⚠️  [%s] %s: %s", job_label, service_id, msg)
            return False, msg
    except Exception as e:
        logger.warning("[%s] %s: failed to check disk space: %s", job_label, service_id, e)
    return True, ""


def publish_sync_status_safe(service_id: str, log_label: str = "scheduler") -> None:
    """Compute and publish the cached sync-status SSE snapshot for ``service_id`` (best-effort)."""
    try:
        from backend.celery_app import publisher as _sync_status_publisher
        from backend.cron.scheduler import compute_sync_status_cached

        if (snapshot := compute_sync_status_cached(service_id)) is not None:
            _sync_status_publisher.publish(service_id, snapshot)
    except Exception:
        logger.exception("[%s] %s: sync-status SSE publish failed", log_label, service_id)


def _buffer_result_error(result: dict, default_error: str | None = None) -> str | None:
    """Extract combined error and quarantined-file message from a ``commit_buffer`` result dict."""
    parts = [str(result["error"])] if result.get("error") else []
    if quarantined := int(result.get("quarantined_files", 0) or 0):
        parts.append(f"{quarantined} unreadable buffer file(s) quarantined")
    return "; ".join(parts) if parts else default_error


def apply_time_range_pinning(
    service_id: str,
    cfg: dict,
    src: dict,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    is_manual: bool = False,
    fallback_to_created_at: bool = False,
    pin_explicit_range: bool = False,
    get_scheduler_fn: Callable | None = None,
) -> tuple[str | None, str | None]:
    """Resolve and persist provisioning ``time_range`` bounds for sync/metadata jobs."""
    prov = cfg.get("provisioning", {})
    should_pin = False
    if not start_time and not is_manual:
        tr = prov.get("time_range")
        if tr and tr.get("start"):
            start_time = tr["start"]
            logger.info("[scheduler] %s: Using configured start_time limit: %s", service_id, start_time)
            should_pin = pin_explicit_range
        elif fallback_to_created_at and cfg.get("created_at"):
            start_time = cfg["created_at"]
            logger.info(
                "[scheduler] %s: Fallback to service creation time as start_time limit: %s", service_id, start_time
            )
            should_pin = True
    elif pin_explicit_range and (start_time or end_time):
        should_pin = True

    if should_pin and (start_time or end_time):

        def _pin_time_range(c: dict) -> None:
            time_range = c.setdefault("provisioning", {}).setdefault("time_range", {})
            if start_time:
                time_range["start"] = start_time
            if end_time:
                time_range["end"] = end_time

        _pin_time_range(cfg)
        svcconfig.mutate_config(service_id, _pin_time_range)
        src["time_range"] = cfg["provisioning"]["time_range"]
        if fallback_to_created_at:
            logger.info("[scheduler] %s: Saved service creation time as start_time limit to config.", service_id)
    elif is_manual and not start_time and not end_time and "time_range" in prov:
        del prov["time_range"]
        cfg["provisioning"] = prov
        svcconfig.mutate_config(service_id, lambda c: c.get("provisioning", {}).pop("time_range", None))
        src["time_range"] = None
        logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

    return start_time, end_time


_INGEST_ERROR_OUTCOME_KEYS = (
    "malformed_records",
    "corrupt_containers",
    "quarantine_capture_failures",
    "source_delete_failures",
    "objects_failed",
)


def _has_ingest_outcome_errors(outcome_counters: dict | None, corrupt_rows: int = 0) -> bool:
    return bool(
        corrupt_rows > 0 or (outcome_counters and any(outcome_counters.get(k, 0) for k in _INGEST_ERROR_OUTCOME_KEYS))
    )


def _finish_post_ingest_view_and_rollups(
    src: dict,
    service_id: str,
    run_id: int | None,
    job_name: str,
    done_event: dict,
    elapsed_fn: Callable[[], str],
    *,
    already_published: bool = False,
) -> None:
    if done_event.get("rows_inserted", 0) > 0 and not already_published:
        from backend.utils.telemetry import yield_to_api

        yield_to_api()
        refresh_fn = _resolve_sync_hook(
            "refresh_view_and_warm_pool", refresh_view_and_warm_pool, _orig_refresh_view_and_warm_pool
        )
        refresh_fn(
            src,
            service_id,
            log_prefix=f"{elapsed_fn()} ",
            progress_log=lambda ev: _log_and_add_progress(run_id, service_id, job_name=job_name, event=ev),
        )
    if touched_hours := done_event.get("touched_hours", []):
        from backend.cron.jobs.sync import schedule_post_ingest_rollups

        schedule_post_ingest_rollups(service_id, src, set(touched_hours))
        _log_and_add_progress(
            run_id,
            service_id,
            job_name=job_name,
            event={"type": "status", "message": f"{elapsed_fn()} Rollups queued for {len(touched_hours)} hour(s)"},
        )


def _ingest_with_adaptive_followups(
    ingest_fn: Callable[..., Iterator[dict]],
    *,
    adaptive: bool,
    after_pass: Callable[[], None] | None = None,
    **ingest_kwargs,
) -> Iterator[dict]:
    from backend.core.ingest import _new_object_outcome

    started = time.monotonic()
    pass_number = processed_files = pass_rows = pass_new_files = 0
    totals: dict[str, Any] = {
        "new_files": 0,
        "rows_inserted": 0,
        "corrupt_rows": 0,
        "deleted_files": 0,
        "quarantined_files": 0,
        "corrupt_details": [],
        "touched_hours": set(),
        "outcome_counters": _new_object_outcome(processed=0),
    }
    max_files = ingest_kwargs.get("max_files")
    adaptive_deadline_s = float(os.environ.get("INGEST_INCREMENTAL_ADAPTIVE_DEADLINE_S", "5.0"))
    backlog_deadline_s = float(os.environ.get("INGEST_BACKLOG_DRAIN_DEADLINE_S", "45.0"))

    while True:
        if pass_number:
            hit_cap = bool(max_files and pass_new_files >= max_files)
            sleep_time = 0.0 if hit_cap else 0.5
            effective_deadline_s = backlog_deadline_s if hit_cap else adaptive_deadline_s
            if not adaptive or pass_number >= 3 or time.monotonic() - started + sleep_time > effective_deadline_s:
                break
            # Publish this pass's rows now; the caller's final refresh only
            # runs after the whole polling window.
            if after_pass is not None and pass_rows > 0:
                after_pass()
            if time.monotonic() - started + sleep_time > effective_deadline_s:
                break
            if sleep_time > 0:
                time.sleep(sleep_time)
            yield {"type": "status", "message": "Adaptive polling: checking for newly arrived log files."}

        pass_done: dict = {}
        pass_new_files = 0
        for event in ingest_fn(**ingest_kwargs):
            event = dict(event)
            if event.get("type") == "file_done":
                event["current"] = processed_files + event.get("current", 0)
                event["total"] = processed_files + event.get("total", 0)
            elif event.get("type") == "done":
                pass_done = event
                pass_new_files = event.get("new_files", 0)
                pass_rows = event.get("rows_inserted", 0)
                processed_files += pass_new_files
                for key in ("new_files", "rows_inserted", "corrupt_rows", "deleted_files", "quarantined_files"):
                    totals[key] += event.get(key, 0)
                totals["corrupt_details"].extend(event.get("corrupt_details", []))
                totals["touched_hours"].update(event.get("touched_hours", []))
                for key, value in event.get("outcome_counters", {}).items():
                    totals["outcome_counters"][key] = totals["outcome_counters"].get(key, 0) + value
                event.update(totals)
                event["touched_hours"] = sorted(totals["touched_hours"])
            yield event
            if event.get("type") == "error":
                return

        if not pass_done or pass_new_files == 0:
            return
        pass_number += 1


_orig_refresh_view_and_warm_pool = refresh_view_and_warm_pool
_orig_refresh_status_after_commit = _refresh_status_after_commit
_orig_finalize_cron_duration = finalize_cron_duration
_orig_publish_sync_status_safe = publish_sync_status_safe
_orig_spawn_local_compaction = _spawn_local_compaction
_orig_finish_post_ingest_view_and_rollups = _finish_post_ingest_view_and_rollups
_orig_ingest_with_adaptive_followups = _ingest_with_adaptive_followups
