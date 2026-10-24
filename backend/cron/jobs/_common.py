"""Shared helpers for cron job modules in ``backend.cron.jobs``."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


def refresh_view_and_warm_pool(
    source: dict,
    service_id: str,
    *,
    log_prefix: str,
    progress_log: Callable[[dict], None] | None = None,
) -> None:
    """Force-refresh the Iceberg DuckDB view and warm the connection pool.

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
        from backend.core import iceberg as _ice
        from backend.core.duckdb import get_connection as _get_conn
        from backend.core.duckdb_pool import warm_pool_for_service as _warm

        con_v = _get_conn(source=source, read_only=True)
        try:
            _ice.update_iceberg_view(con_v, source, force=True)
            _warm(service_id, source)
        finally:
            con_v.close()
        if progress_log is not None:
            progress_log(
                {"type": "status", "message": f"{log_prefix}View refresh + warm: {int((time.time() - t0) * 1000)}ms"}
            )
    except Exception as e:
        logger.warning("[scheduler] %s: post-%s view refresh failed: %s", service_id, log_prefix.strip(": "), e)


def finalize_cron_duration(
    src: dict,
    run_id: int | None,
    t_start: float,
    *,
    log_output: str | None = None,
    silent: bool = True,
    clock: Callable[[], float] = time.time,
) -> None:
    """Update the cron-run row's ``duration_s`` (and optionally ``log_output``).

    Shared by the five ``backend/cron/jobs/*`` modules whose ``finally:``
    blocks all ended in the same six-line ``if run_id is not None: try: ...
    update_cron_duration(...) except: pass``. The two variations live as
    keyword args: ``log_output`` is set on the sync job (the initial
    log_cron_run snapshot pre-dates phases 1.5-4), and ``silent`` controls
    whether a failed update logs a warning (sync) or stays quiet (commit,
    optimize, metadata).

    ``clock`` is injected so the metadata-cleanup site that times with
    ``time.monotonic()`` keeps that semantic without forcing every caller
    onto monotonic.
    """
    if run_id is None:
        return
    try:
        elapsed = clock() - t_start
        from backend.core.duckdb import update_cron_duration

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
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
    svc_name = (cfg.get("service_name") or cfg.get("name") or service_id) if cfg else service_id
    return f"{svc_name} ({service_id})" if svc_name and svc_name != service_id else service_id


def has_sufficient_disk_space(
    path: str,
    job_label: str,
    service_id: str,
    *,
    min_free_bytes: int = 50 * 1024 * 1024,
) -> tuple[bool, str]:
    """Return ``(ok, msg)`` checking that ``path`` (or ``.``) has at least ``min_free_bytes`` free."""
    import os
    import shutil

    check_dir = path if os.path.isdir(path) else "."
    try:
        free_bytes = shutil.disk_usage(check_dir).free
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
        from backend.sync_status_publisher import publisher as _sync_status_publisher
        from backend.sync_status_snapshot import compute_sync_status_cached

        snapshot = compute_sync_status_cached(service_id)
        if snapshot is not None:
            _sync_status_publisher.publish(service_id, snapshot)
    except Exception:
        logger.exception("[%s] %s: sync-status SSE publish failed", log_label, service_id)


def _buffer_result_error(result: dict, default_error: str | None = None) -> str | None:
    """Extract combined error and quarantined-file message from a ``commit_buffer`` result dict."""
    parts: list[str] = []
    if result.get("error"):
        parts.append(str(result["error"]))
    quarantined = int(result.get("quarantined_files", 0) or 0)
    if quarantined:
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
    from backend import config as svcconfig

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
    elif is_manual and not start_time and not end_time:
        if "time_range" in prov:
            del prov["time_range"]
            cfg["provisioning"] = prov
            svcconfig.mutate_config(service_id, lambda c: c.get("provisioning", {}).pop("time_range", None))
            src["time_range"] = None
            logger.info("[scheduler] %s: Manual sync-all, cleared time_range limit.", service_id)

    return start_time, end_time
