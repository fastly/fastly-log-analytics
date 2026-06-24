import json
import logging
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sse_starlette.sse import EventSourceResponse

from backend.cron_runs_publisher import publisher as cron_runs_publisher
from backend.deps import get_service_id, get_source
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories.cron import delete_cron_log, get_cron_logs, purge_cron_logs
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, bad_request, raise_internal
from backend.utils.sse_subscription import iter_with_disconnect_ping

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cron-runs", tags=["cron-runs"], responses=DEFAULT_ERROR_RESPONSES)


@router.get("")
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


@router.get("/stream")
async def cron_runs_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Push cron-run state changes (start / completion) to admin browsers.

    Replaces two polled queries that the /logs Recent Cron Activity
    table relied on (a 30 s table refetch + a 15 s delta poll for the
    floating dock toast). One tickle event per state change tells
    connected clients to invalidate their cached cron-logs queries —
    the refetch happens through React Query with the user's current
    task/status filter encoded in the key, so the server doesn't have
    to know about per-tab filter state.

    Payload shape is intentionally minimal — a notification, not a
    row. See backend/cron_runs_publisher.py for the publisher.

    Auth: ``/api/cron-runs`` is already in ``_ANALYST_BLOCKED_PREFIXES``
    (see backend/utils/remote_access.py), so this sibling inherits the
    admin-only block via the same prefix match that protects the
    existing per-run ``/api/cron-runs/{run_id}/stream`` endpoint.
    """
    if not service_id:
        raise HTTPException(status_code=400, detail=bad_request("x_service_id_required"))

    async def stream() -> AsyncIterator[str]:
        # No initial snapshot — the React Query cache is already seeded
        # from /api/cron-runs on page mount; this stream's only job is
        # to tickle invalidations as state changes. See
        # backend.utils.sse_subscription for the disconnect-ping rationale.
        async for payload in iter_with_disconnect_ping(
            cron_runs_publisher.subscribe(service_id), request, ping_seconds=15
        ):
            yield json.dumps(payload)

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)
