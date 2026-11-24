"""Shared helpers for ingest-class and maintenance cron jobs."""

from __future__ import annotations

import logging
import os
import sys as _sys
import time
from collections.abc import Callable
from typing import Any

from backend import config as svcconfig
from backend.cron.scheduler import dev_mode_no_crons

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
        from backend.core.fos_client import update_cron_duration

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


_orig_finalize_cron_duration = finalize_cron_duration
_orig_publish_sync_status_safe = publish_sync_status_safe
