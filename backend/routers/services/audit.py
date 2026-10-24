import logging

from fastapi import APIRouter, Depends, Query

from backend.core import metadata as metadata_db
from backend.deps import get_source
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.status_reads import AuditLogsResponse
from backend.utils.router_utils import query_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/audit-logs", tags=["audit-logs"], responses=DEFAULT_ERROR_RESPONSES)


@router.get("", response_model=AuditLogsResponse, response_model_exclude_unset=True)
@query_errors(500, code="audit_logs_read_failed", logger_obj=logger)
def api_audit_logs(
    source: dict = Depends(get_source),
    event_type: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="timestamp"),
    dir: str = Query(default="DESC"),
):
    from backend.utils.telemetry import debug_envelope

    total, entries = metadata_db.get_audit_logs(
        source["name"],
        event_type=event_type,
        page=page,
        per_page=per_page,
        sort_col=sort,
        sort_dir=dir,
    )

    return debug_envelope(
        {"total": total, "page": page, "per_page": per_page, "entries": entries},
        gated=True,
        is_cached=None,
        inject_calls=True,
    )
