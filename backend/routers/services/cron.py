from fastapi import APIRouter, Depends, HTTPException, Query

from backend.deps import get_source
from backend.repositories.cron import delete_cron_log, get_cron_logs, purge_cron_logs

router = APIRouter(prefix="/api/cron-runs", tags=["cron-runs"])


@router.get("")
def api_cron_logs(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="started_at"),
    dir: str = Query(default="DESC"),
):
    from backend.utils.telemetry import get_tracked_calls

    try:
        total, entries = get_cron_logs(source["name"], task, status, page, per_page, sort, dir)
        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "entries": entries,
            "_debug_queries": [],
            "_debug_calls": get_tracked_calls(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})


@router.delete("/{log_id}")
def api_cron_log_delete(log_id: int, source: dict = Depends(get_source)):
    try:
        delete_cron_log(source["name"], log_id)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})


@router.delete("")
def api_cron_logs_purge(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    days: int | None = Query(default=None),
):
    try:
        purge_cron_logs(source["name"], task, days)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"ok": False, "message": str(e)})
