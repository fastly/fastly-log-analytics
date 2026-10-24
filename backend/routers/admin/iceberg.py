"""Iceberg admin endpoints: info, calendar, commit, view-rebuild."""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import Depends, HTTPException

from backend.deps import get_source, require_admin
from backend.models.admin import IcebergTableInfoResponse, ResetLogsRequest, SyncStartResponse
from backend.models.admin_actions import IcebergCalendarResponse
from backend.utils.router_utils import (
    make_error,
    query_errors,
    require_json_content_type,
)

from ._router import router

logger = logging.getLogger(__name__)


@router.get("/admin/iceberg-info", response_model=IcebergTableInfoResponse)
@query_errors(status_code=500)
def iceberg_info_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict = Depends(get_source),
):
    """Return Iceberg table metadata: snapshots, data files, size, buffer status."""
    from backend.core import iceberg as db_iceberg

    result = db_iceberg.get_table_info(source, table_name=table_name)
    return IcebergTableInfoResponse.with_telemetry(**result)


@router.get("/admin/iceberg-calendar", response_model=IcebergCalendarResponse, response_model_exclude_unset=True)
@query_errors(status_code=500)
def iceberg_calendar_endpoint(
    table_name: Literal["logs", "client_vitals", "client_errors"] = "logs",
    source: dict = Depends(get_source),
):
    """Return per-date data file counts from Iceberg partition metadata."""
    from backend.core import iceberg as db_iceberg
    from backend.utils.telemetry import debug_envelope

    result = db_iceberg.get_snapshot_calendar(source, table_name=table_name)
    return debug_envelope(result, gated=True, is_cached=None, inject_calls=True)


@router.post(
    "/admin/commit-iceberg",
    status_code=202,
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
@router.post(
    "/admin/commit/{service_id}",
    status_code=202,
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def iceberg_commit_endpoint(
    service_id: str | None = None,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    """Manually flush the local buffer to the DuckLake table.

    Returns 202 Accepted — the commit runs asynchronously in a worker
    thread and the response body carries a ``run_id`` the caller can
    poll for completion. 200 (the prior status) misled clients that
    treated it as "done"."""
    from backend.cron.jobs.commit import _run_commit
    from backend.utils.router_utils import run_admin_cron_trigger

    return run_admin_cron_trigger(
        source,
        "commit",
        _run_commit,
        target_kwargs={"force": True},
        success_msg="Commit started.",
        in_progress_msg="Commit already running.",
        readonly_detail=make_error("read_only", "Read-only services cannot run DuckLake commit."),
        invalidate_cache=False,
    )


@router.post(
    "/admin/rebuild-local-view", status_code=202, response_model=SyncStartResponse, response_model_exclude_unset=True
)
def rebuild_local_view_endpoint(source: dict = Depends(get_source)):
    """One-button "fix it" for a stuck or stale local DuckDB view.

    Returns 202 Accepted — the rebuild runs asynchronously in a worker
    thread and the response body carries a ``run_id`` the caller can
    poll for completion.

    Clears the in-memory + on-disk caches that drive view SQL generation,
    then triggers a metadata_sync that re-pulls the catalog from the cloud
    and rebuilds the view. The local raw buffer is NOT touched —
    un-committed data is safe.

    When to use: after manually editing parquet files, after a catalog
    schema-mapping desync, or when "Sync All" already ran and the view
    still looks wrong. This is the nuclear-option version of refresh.
    """
    import threading

    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import start_cron_run
    from backend.cron.jobs.metadata import _run_metadata_sync
    from backend.cron_progress import start_progress

    service_id = source["name"]

    db_iceberg.clear_source_caches(service_id)

    try:
        # Deliberate divergence from router_utils.start_or_resume_cron: this
        # path does NOT resume an in-flight run on RuntimeError — it 503s
        # cron_busy instead. A rebuild is the nuclear "fix it" button, so
        # piggy-backing on an existing run_id would silently no-op the
        # cache-clearing the operator explicitly asked for.
        run_id = start_cron_run(source, "metadata_sync")
    except RuntimeError as e:
        logger.warning("cron_busy service_id=%s: %s", service_id, e)
        raise HTTPException(
            status_code=503,
            detail=make_error("cron_busy", "A sync run is already in progress", busy=True),
        ) from e

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    t = threading.Thread(target=_run_metadata_sync, args=(service_id,), kwargs={"run_id": run_id}, daemon=True)
    t.start()
    return {"ok": True, "message": "Local view rebuild started.", "run_id": run_id}


# response_model intentionally omitted: SSE progress stream (EventSourceResponse),
# not a JSON body — event shapes are documented in reset_service_logs's docstring.
@router.post("/admin/reset-logs", dependencies=[Depends(require_json_content_type)])
def reset_logs_endpoint(body: ResetLogsRequest, source: dict = Depends(get_source)):
    """Permanently delete this service's log data (cloud + local) over SSE.

    Wipes the cloud Iceberg log table, the local DuckDB file + cache, and
    the SQLite ingestion ledgers, then re-initializes an empty, queryable
    0-state. Preserves the service's configuration: saved views, alerts,
    source registration, audit history, and scoring labels/audit. See
    ``backend.core.reset.reset_service_logs`` for the full sequence.

    Fails closed before any work starts: 403 if this service isn't
    read_write (defense-in-depth — ``RemoteAccessMiddleware`` already blocks
    ``/api/admin/*`` for remote analysts), and 400 if ``confirm`` doesn't
    match the resolved service id.
    """
    from backend.core.reset import reset_service_logs
    from backend.utils.sse import stream_sse_from_worker

    service_id = source["name"]

    if source.get("access_level") != "read_write":
        raise HTTPException(status_code=403, detail=make_error("read_only", "This service is read-only."))
    if body.confirm != service_id:
        raise HTTPException(
            status_code=400,
            detail=make_error("confirm_mismatch", "`confirm` must equal the service id being deleted."),
        )

    def worker(events):
        from backend.cron.scheduler import get_scheduler

        try:
            for event in reset_service_logs(
                service_id,
                delete_raw_logs=body.delete_raw_logs,
                preserve_usage_history=body.preserve_usage_history,
                reload_scheduler=lambda: get_scheduler().reload(),
            ):
                events.put(event)
        except Exception as e:
            logger.exception("reset_logs_endpoint(%s) failed", service_id)
            events.put({"type": "error", "message": f"Log deletion failed: {e}"})

    return stream_sse_from_worker(worker)


@router.post("/admin/reset-rum", dependencies=[Depends(require_json_content_type)])
def reset_rum_endpoint(body: ResetLogsRequest, source: dict = Depends(get_source)):
    """Permanently delete this service's RUM telemetry data (cloud + local) over SSE.

    Wipes the cloud Iceberg RUM tables and the local RUM DuckDB file and cache
    while leaving logs data completely untouched.
    """
    from backend.core.reset import reset_service_rum
    from backend.utils.sse import stream_sse_from_worker

    service_id = source["name"]

    if source.get("access_level") != "read_write":
        raise HTTPException(status_code=403, detail=make_error("read_only", "This service is read-only."))
    if body.confirm != service_id:
        raise HTTPException(
            status_code=400,
            detail=make_error("confirm_mismatch", "`confirm` must equal the service id being deleted."),
        )

    def worker(events):
        from backend.cron.scheduler import get_scheduler

        try:
            for event in reset_service_rum(
                service_id,
                delete_raw_logs=body.delete_raw_logs,
                reload_scheduler=lambda: get_scheduler().reload(),
            ):
                events.put(event)
        except Exception as e:
            logger.exception("reset_rum_endpoint(%s) failed", service_id)
            events.put({"type": "error", "message": f"RUM deletion failed: {e}"})

    return stream_sse_from_worker(worker)
