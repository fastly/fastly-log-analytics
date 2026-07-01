import logging

from fastapi import APIRouter, Depends, Query, Response

from backend.deps import get_source
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.services import CronRunsResponse
from backend.repositories.cron import delete_cron_log, get_cron_logs, purge_cron_logs
from backend.utils.router_utils import raise_internal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cron-runs", tags=["cron-runs"], responses=DEFAULT_ERROR_RESPONSES)


@router.get("", response_model=CronRunsResponse, response_model_exclude_unset=True)
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
    try:
        total, entries = get_cron_logs(
            source["name"],
            task,
            status,
            page,
            per_page,
            sort,
            dir,
            since_id=since_id,
            skip_total=skip_total,
        )
        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "entries": entries,
        }
    except Exception as e:
        raise_internal(logger, e, code="cron_logs_read_failed")


@router.delete("/{log_id}", status_code=204)
def api_cron_log_delete(log_id: int, source: dict = Depends(get_source)):
    try:
        delete_cron_log(source["name"], log_id)
        return Response(status_code=204)
    except Exception as e:
        raise_internal(logger, e, code="cron_log_delete_failed")


@router.delete("", status_code=204)
def api_cron_logs_purge(
    source: dict = Depends(get_source),
    task: str | None = Query(default=None),
    days: int | None = Query(default=None),
):
    try:
        purge_cron_logs(source["name"], task, days)
        return Response(status_code=204)
    except Exception as e:
        raise_internal(logger, e, code="cron_logs_purge_failed")
