"""Admin endpoint exposing Celery worker/queue, APScheduler, and ledger status.

The computation lives in backend/celery_status.py (top-level) so the SSE
sampler and health snapshot can reuse it without importing through the
routers package. Admin-only via RemoteAccessMiddleware path gating.
"""

from fastapi import Depends, Query

from backend.celery_status import (
    get_celery_status,
    list_ingest_ledger_entries,
    purge_ingest_ledger_entries,
    retry_ingest_ledger_entries,
)
from backend.deps import require_admin
from backend.models.admin_status import (
    CeleryStatusResponse,
    IngestLedgerActionRequest,
    IngestLedgerActionResponse,
    IngestLedgerListResponse,
)
from backend.routers.admin._router import router


@router.get(
    "/admin/celery/status",
    response_model=CeleryStatusResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def api_celery_status():
    """Expose Celery queue depths, worker status, APScheduler schedule, and
    ingest-ledger summary."""
    res = dict(get_celery_status())
    from backend.models.common import _debug_responses_enabled

    if _debug_responses_enabled():
        from backend.utils.telemetry import get_sqlite_queries

        ds = list(get_sqlite_queries())
        res["_debug_sqlite"] = ds
        res["_debug_postgres"] = ds
    return res


@router.get(
    "/admin/celery/ledger",
    response_model=IngestLedgerListResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_list(
    status: str | None = Query(
        default="problem",
        description="Filter by ledger status ('problem', 'all', 'dead_letter', 'quarantined', 'discovered', 'claimed', 'committed')",
    ),
    service_id: str | None = Query(default=None, description="Optional service_id filter"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """List entries in ``ingest_ledger`` across services (defaults to dead_letter + quarantined)."""
    return list_ingest_ledger_entries(status=status, service_id=service_id, limit=limit, offset=offset)


@router.post(
    "/admin/celery/ledger/retry",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_retry(body: IngestLedgerActionRequest):
    """Reset ``dead_letter`` / ``quarantined`` ledger entries back to ``discovered`` and
    best-effort dispatch Celery convert batches."""
    items = [item.model_dump() for item in body.items] if body.items else None
    return retry_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)


@router.post(
    "/admin/celery/ledger/purge",
    response_model=IngestLedgerActionResponse,
    dependencies=[Depends(require_admin)],
)
def api_celery_ledger_purge(body: IngestLedgerActionRequest):
    """Delete ``dead_letter`` / ``quarantined`` entries from ``ingest_ledger``."""
    items = [item.model_dump() for item in body.items] if body.items else None
    return purge_ingest_ledger_entries(items=items, status=body.status, service_id=body.service_id)
