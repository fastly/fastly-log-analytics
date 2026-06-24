"""Iceberg admin endpoints: info, calendar, commit, view-rebuild."""

from __future__ import annotations

import logging
import os

from fastapi import Depends, HTTPException

from backend.deps import get_source
from backend.models.admin import IcebergTableInfoResponse
from backend.utils.router_utils import make_error, query_errors, raise_internal

from ._router import router

logger = logging.getLogger(__name__)


@router.get("/admin/iceberg-info", response_model=IcebergTableInfoResponse)
@query_errors(status_code=500)
def iceberg_info_endpoint(source: dict = Depends(get_source)):
    """Return Iceberg table metadata: snapshots, data files, size, buffer status."""
    from backend.core import iceberg as db_iceberg

    result = db_iceberg.get_table_info(source)
    return IcebergTableInfoResponse.with_telemetry(**result)


@router.get("/admin/iceberg-calendar")
@query_errors(status_code=500)
def iceberg_calendar_endpoint(source: dict = Depends(get_source)):
    """Return per-date data file counts from Iceberg partition metadata."""
    from backend.core import iceberg as db_iceberg
    from backend.utils.telemetry import get_tracked_calls

    result = db_iceberg.get_snapshot_calendar(source)
    return {**result, "_debug_calls": get_tracked_calls()}


@router.post("/admin/commit-iceberg", status_code=202)
def iceberg_commit_endpoint(source: dict = Depends(get_source)):
    """Manually flush the local buffer to the Iceberg table.

    Returns 202 Accepted — the commit runs asynchronously in a worker
    thread and the response body carries a ``run_id`` the caller can
    poll for completion. 200 (the prior status) misled clients that
    treated it as "done"."""
    from backend.scheduler import _run_commit
    from backend.utils.router_utils import start_or_resume_cron

    return start_or_resume_cron(
        source,
        "commit",
        _run_commit,
        target_kwargs={"force": True},
        success_msg="Commit started.",
        in_progress_msg="Commit already running.",
    )


@router.post("/admin/rebuild-local-view", status_code=202)
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
    from backend.core.duckdb import _cache_dir, start_cron_run
    from backend.cron_progress import start_progress
    from backend.scheduler import _run_metadata_sync

    service_id = source["name"]

    db_iceberg.clear_source_caches(service_id)
    # The persistent cache file lives at cache/{bucket}/snapshot_files_cache.json
    # — deleting it forces sync_data to call tbl.scan().plan_files() against
    # the freshly-loaded catalog instead of trusting the previous snapshot's
    # cached file list.
    persistent_cache = os.path.join(_cache_dir(source), "snapshot_files_cache.json")
    if os.path.exists(persistent_cache):
        try:
            os.remove(persistent_cache)
        except OSError as e:
            raise_internal(logger, e, code="snapshot_cache_remove_failed", status=500)

    try:
        # Deliberate divergence from router_utils.start_or_resume_cron: this
        # path does NOT resume an in-flight run on RuntimeError — it 503s
        # cron_busy instead. A rebuild is the nuclear "fix it" button, so
        # piggy-backing on an existing run_id would silently no-op the
        # cache-clearing the operator explicitly asked for.
        run_id = start_cron_run(source, "metadata_sync")
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=make_error("cron_busy", str(e), busy=True)) from e

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    t = threading.Thread(target=_run_metadata_sync, args=(service_id,), kwargs={"run_id": run_id}, daemon=True)
    t.start()
    return {"ok": True, "message": "Local view rebuild started.", "run_id": run_id}
