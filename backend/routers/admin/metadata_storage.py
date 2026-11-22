"""Metadata-retention, metadata-storage, metadata-cleanup, and reset-logs/rum admin endpoints."""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import Depends, HTTPException

from backend.core.request_context import get_source, require_admin
from backend.models.admin import (
    MetadataRetentionResponse,
    MetadataRetentionUpdate,
    MetadataRetentionValues,
    MetadataStorageResponse,
    ResetLogsRequest,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.router_utils import (
    make_error,
    require_json_content_type,
)

from ._helpers import service_id_from_source
from ._router import router

logger = logging.getLogger(__name__)

_METADATA_STORAGE_CACHE: BoundedTTLCache[str, MetadataStorageResponse] = BoundedTTLCache(maxsize=64, ttl_seconds=30.0)
CacheRegistry.register("metadata_storage._METADATA_STORAGE_CACHE", _METADATA_STORAGE_CACHE)


@router.patch(
    "/admin/metadata-retention",
    response_model=MetadataRetentionResponse,
    response_model_exclude_unset=True,
)
def update_metadata_retention(
    body: MetadataRetentionUpdate, source: dict[str, Any] = Depends(get_source)
) -> MetadataRetentionResponse:
    """Update the per-service ``metadata_retention`` config block."""
    from backend.core import metadata as _mdb
    from backend.core.metadata import DEFAULT_METADATA_RETENTION
    from backend.utils.router_utils import load_service_config, mutate_service_config

    service_id = source["name"]
    load_service_config(service_id)

    raw_body = body.model_dump(exclude_unset=True) if hasattr(body, "model_dump") else dict(body)
    updates: dict[str, int] = {}
    for key in ("usage_log_days", "ingested_files_days", "cron_runs_days"):
        if key in raw_body:
            try:
                v = int(raw_body[key])
            except (TypeError, ValueError):
                v = 0
            updates[key] = max(0, v)

    def _apply(cfg: dict[str, Any]) -> None:
        merged = dict(cfg.get("metadata_retention") or {})
        merged.update(updates)
        cfg["metadata_retention"] = merged

    current = mutate_service_config(service_id, _apply)["metadata_retention"]
    try:
        _mdb.record_audit(
            service_id=service_id,
            event_type="metadata_retention_update",
            details=current,
        )
    except Exception:
        logger.warning("update_metadata_retention: _mdb.record_audit failed (ignored)", exc_info=True)

    _METADATA_STORAGE_CACHE.clear()
    return MetadataRetentionResponse(retention=MetadataRetentionValues(**{**DEFAULT_METADATA_RETENTION, **current}))


@router.get(
    "/admin/metadata-storage",
    response_model=MetadataStorageResponse,
    response_model_exclude_unset=True,
)
def metadata_storage(source: dict[str, Any] = Depends(get_source)) -> MetadataStorageResponse:
    """Per-table row count + estimated bytes for this service's metadata."""
    from backend import config as svcconfig
    from backend.core.metadata import (
        DEFAULT_METADATA_RETENTION,
        get_metadata_storage_stats,
    )

    service_id = source["name"]
    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    cached = _METADATA_STORAGE_CACHE.get(service_id) if use_cache else None
    if cached is not None:
        return cached
    stats = get_metadata_storage_stats(service_id)
    cfg = svcconfig.load_config(service_id) or {}
    retention = {**DEFAULT_METADATA_RETENTION, **(cfg.get("metadata_retention") or {})}
    resp = MetadataStorageResponse(
        **stats,
        retention=MetadataRetentionValues(**retention),
    )
    if use_cache:
        _METADATA_STORAGE_CACHE.set(service_id, resp)
    return resp


@router.post("/admin/metadata-cleanup")
@router.post("/admin/metadata-cleanup/{service_id}")
def metadata_cleanup_now(
    service_id: str | None = None,
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> Any:
    """Trigger an immediate metadata cleanup, streaming progress as SSE."""
    import time as _t

    from backend import config as svcconfig
    from backend.core.fos_client import log_cron_run, start_cron_run
    from backend.core.metadata import cleanup_metadata
    from backend.utils.sse import stream_sse_from_worker

    service_id = service_id_from_source(source, service_id, prefer_name=True)
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    def worker(events: Any) -> None:
        started = _t.time()
        run_id = None
        try:
            run_id = start_cron_run(source, "metadata_cleanup")
            result = cleanup_metadata(service_id, retention, on_event=events.put)
        except Exception as e:
            err = str(e)
            events.put({"type": "error", "message": f"Cleanup failed: {err}"})
            log_cron_run(
                source,
                "metadata_cleanup",
                _t.time() - started,
                "error",
                error_message=err,
                summary=f"cleanup failed: {err}",
                run_id=run_id,
            )
            return

        total_deleted = sum(result["deleted"].values())
        if total_deleted:
            parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
            summary = (
                f"Trimmed {total_deleted:,} rows ({', '.join(parts)}). "
                f"VACUUM={'yes' if result['vacuumed'] else 'skipped'}."
            )
        else:
            summary = "No rows older than retention windows."
        try:
            log_cron_run(
                source,
                "metadata_cleanup",
                _t.time() - started,
                "success",
                summary=summary,
                rows_ingested=total_deleted,
                run_id=run_id,
            )
        finally:
            _METADATA_STORAGE_CACHE.clear()
            events.put({"type": "done", "message": summary, "result": result})

    return stream_sse_from_worker(worker)


def _stream_reset_sse(
    body: ResetLogsRequest,
    source: dict[str, Any],
    reset_fn: Any,
    label: str,
    **extra_kwargs: Any,
) -> Any:
    from backend.utils.sse import stream_sse_from_worker

    service_id = source["name"]
    if body.confirm != service_id:
        raise HTTPException(
            status_code=400,
            detail=make_error("confirm_mismatch", "`confirm` must equal the service id being deleted."),
        )

    def worker(events: Any) -> None:
        from backend.cron.scheduler import get_scheduler

        try:
            for event in reset_fn(
                service_id,
                delete_raw_logs=body.delete_raw_logs,
                reload_scheduler=lambda: get_scheduler().reload(),
                **extra_kwargs,
            ):
                events.put(event)
        except Exception as e:
            logger.exception("reset_%s_endpoint(%s) failed", label.lower(), service_id)
            events.put({"type": "error", "message": f"{label} deletion failed: {e}"})

    return stream_sse_from_worker(worker)


@router.post("/admin/reset-logs", dependencies=[Depends(require_json_content_type)])
def reset_logs_endpoint(body: ResetLogsRequest, source: dict[str, Any] = Depends(get_source)) -> Any:
    """Permanently delete this service's log data (cloud + local) over SSE."""
    from backend.core.reset import reset_service_logs

    return _stream_reset_sse(
        body,
        source,
        reset_service_logs,
        "Log",
        preserve_usage_history=body.preserve_usage_history,
    )


@router.post("/admin/reset-rum", dependencies=[Depends(require_json_content_type)])
def reset_rum_endpoint(body: ResetLogsRequest, source: dict[str, Any] = Depends(get_source)) -> Any:
    """Permanently delete this service's RUM telemetry data (cloud + local) over SSE."""
    from backend.core.reset import reset_service_rum

    return _stream_reset_sse(body, source, reset_service_rum, "RUM")
