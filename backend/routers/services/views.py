"""Audit log and cron run history view endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query, Response

from backend.core import metadata as metadata_db
from backend.core.request_context import get_source
from backend.models.common import DEFAULT_ERROR_RESPONSES, AuditLogsResponse, CronRunsResponse
from backend.repositories.assets import delete_cron_log, get_cron_logs, purge_cron_logs
from backend.utils.router_utils import query_errors

logger = logging.getLogger(__name__)

audit_router = APIRouter(prefix="/api/audit-logs", tags=["audit-logs"], responses=DEFAULT_ERROR_RESPONSES)
cron_router = APIRouter(prefix="/api/cron-runs", tags=["cron-runs"], responses=DEFAULT_ERROR_RESPONSES)
router = APIRouter()


@audit_router.get("", response_model=AuditLogsResponse, response_model_exclude_unset=True)
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
        source["name"], event_type=event_type, page=page, per_page=per_page, sort_col=sort, sort_dir=dir
    )
    return debug_envelope(
        {"total": total, "page": page, "per_page": per_page, "entries": entries},
        gated=True,
        is_cached=None,
        inject_calls=True,
    )


@cron_router.get("", response_model=CronRunsResponse, response_model_exclude_unset=True)
@query_errors(500, code="cron_logs_read_failed", logger_obj=logger)
def api_cron_logs(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, le=1000),
    sort: str = Query(default="started_at"),
    dir: str = Query(default="DESC"),
    since_id: int | None = Query(default=None, ge=0),
    skip_total: bool = Query(default=False),
):
    total, entries = get_cron_logs(
        source["name"], task, status, page, per_page, sort, dir, since_id=since_id, skip_total=skip_total
    )
    return {"total": total, "page": page, "per_page": per_page, "entries": entries}


@cron_router.delete("/{log_id}", status_code=204)
@query_errors(500, code="cron_log_delete_failed", logger_obj=logger)
def api_cron_log_delete(log_id: int, source: dict = Depends(get_source)):
    delete_cron_log(source["name"], log_id)
    return Response(status_code=204)


@cron_router.delete("", status_code=204)
@query_errors(500, code="cron_logs_purge_failed", logger_obj=logger)
def api_cron_logs_purge(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    days: int | None = Query(default=None),
):
    purge_cron_logs(source["name"], task, days)
    return Response(status_code=204)


router.include_router(audit_router)
router.include_router(cron_router)
