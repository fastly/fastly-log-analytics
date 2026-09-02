"""Admin endpoint exposing Celery worker/queue/RedBeat/ledger status.

The computation lives in backend/celery_status.py (top-level) so the SSE
sampler and health snapshot can reuse it without importing through the
routers package. Admin-only via RemoteAccessMiddleware path gating.
"""

from backend.celery_status import get_celery_status
from backend.routers.admin._router import router


@router.get("/admin/celery/status")
def api_celery_status():
    """Expose Celery queue depths, worker status, RedBeat schedule, and
    ingest-ledger summary."""
    return get_celery_status()
