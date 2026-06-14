"""Manual ingest trigger endpoint."""

from __future__ import annotations

from fastapi import Depends, Query

from backend.deps import get_source

from ._router import router


@router.post("/admin/ingest-logs")
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict = Depends(get_source),
) -> dict:
    import threading

    from fastapi import HTTPException

    from backend.core.duckdb import start_cron_run
    from backend.cron_progress import list_active_runs, start_progress
    from backend.repositories.dashboard import invalidate_service
    from backend.scheduler import _run_metadata_sync, _run_service_cron

    src = source
    invalidate_service(src["name"])
    is_readonly = source.get("access_level") == "read_only"

    if is_readonly:
        try:
            run_id = start_cron_run(source, "metadata_sync")
            start_progress(run_id, service_id=source["name"], task="metadata_sync")
            t = threading.Thread(
                target=_run_metadata_sync,
                args=(source["name"],),
                kwargs={"run_id": run_id, "start_time": start_time, "end_time": end_time},
                daemon=True,
            )
            t.start()
        except RuntimeError as e:
            run_id = None
            for entry in list_active_runs():
                if entry.get("service_id") == source["name"] and entry.get("task") == "metadata_sync":
                    run_id = entry["run_id"]
                    break
            if run_id is None:
                raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
            return {"ok": True, "message": "Metadata sync already running.", "run_id": run_id}

        return {"ok": True, "message": "Metadata sync started.", "run_id": run_id}

    else:
        try:
            run_id = start_cron_run(src, "sync")
            start_progress(run_id, service_id=src["name"], task="sync")
            t = threading.Thread(
                target=_run_service_cron,
                args=(src["name"],),
                kwargs={
                    "force": True,
                    "run_id": run_id,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                daemon=True,
            )
            t.start()
        except RuntimeError as e:
            run_id = None
            for entry in list_active_runs():
                if entry.get("service_id") == src["name"] and entry.get("task") == "sync":
                    run_id = entry["run_id"]
                    break
            if run_id is None:
                raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
            return {"ok": True, "message": "Ingestion already running.", "run_id": run_id}

        return {"ok": True, "message": "Ingestion started.", "run_id": run_id}
