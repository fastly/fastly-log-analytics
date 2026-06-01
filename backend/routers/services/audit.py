from fastapi import APIRouter, Depends, HTTPException, Query

from backend.core import metadata_db
from backend.deps import get_source

router = APIRouter(prefix="/api/audit-logs", tags=["audit-logs"])


@router.get("")
def api_audit_logs(
    source: dict = Depends(get_source),
    event_type: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="timestamp"),
    dir: str = Query(default="DESC"),
):
    from backend.utils.telemetry import get_tracked_calls

    try:
        total, entries = metadata_db.get_audit_logs(
            source["name"],
            event_type=event_type,
            page=page,
            per_page=per_page,
            sort_col=sort,
            sort_dir=dir,
        )

        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "entries": entries,
            "_debug_calls": get_tracked_calls(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})
