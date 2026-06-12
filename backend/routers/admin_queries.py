"""Admin endpoints for the Live Query Monitor.

All routes sit under ``/api/admin/`` so :class:`RemoteAccessMiddleware`
gates them structurally — analyst sessions can never reach this surface.

Endpoints (see ``pending-docs/design_live_query_monitoring.md`` §6 for the
full schema):

- ``GET  /api/admin/queries``                 — incremental snapshot
- ``GET  /api/admin/queries/summary``         — cheap counts for the tab badge
- ``GET  /api/admin/queries/{qid}``           — full SQL for one row
- ``POST /api/admin/queries/{qid}/cancel``    — interrupt + audit log

The feature-flag (``QUERY_MONITOR_ENABLED``) flips every endpoint to 404 so
the frontend's nav-gating call sees the feature as absent. Default ON; flip
to 0 if the registry ever causes load pressure.
"""

from __future__ import annotations

import time
from collections import deque

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from backend.core.query_registry import query_registry
from backend.core.settings import Settings

router = APIRouter(prefix="/api/admin", tags=["admin", "query-monitor"])


def _enabled() -> bool:
    # Re-evaluate per request so an env flip (mostly for incident response)
    # takes effect without a restart. Settings() construction cost is in
    # the microseconds.
    try:
        return Settings().query_monitor_enabled
    except Exception:
        return True


def _ensure_enabled() -> None:
    if not _enabled():
        # 404 (not 503) so frontend feature-detection treats it as "missing"
        # rather than "temporarily broken".
        raise HTTPException(status_code=404, detail="query_monitor_disabled")


# ── Rate limiting (cancel endpoint only) ────────────────────────────────────

# Per-admin token bucket — 10 cancels/sec. The cancel endpoint is idempotent
# so a buggy frontend re-clicking is harmless; this just caps the audit-log
# spam and prevents accidentally hammering the SQLite/DuckDB interrupt path.
_CANCEL_RATE_PER_SEC = 10
_CANCEL_WINDOW_S = 1.0
_cancel_history: dict[str, deque[float]] = {}


def _check_cancel_rate(admin_id: str) -> bool:
    now = time.monotonic()
    history = _cancel_history.setdefault(admin_id, deque(maxlen=_CANCEL_RATE_PER_SEC * 2))
    cutoff = now - _CANCEL_WINDOW_S
    while history and history[0] < cutoff:
        history.popleft()
    if len(history) >= _CANCEL_RATE_PER_SEC:
        return False
    history.append(now)
    return True


def _admin_id_from_request(request: Request) -> str:
    """Same logic as :func:`backend.core.request_context._build_attribution_from_request`
    — keep them in sync if the admin-id derivation ever moves."""
    return (request.client.host if request.client else "unknown") or "admin"


# ── Response models ─────────────────────────────────────────────────────────


class SnapshotResponse(BaseModel):
    last_seq: int
    active: list[dict]
    completed: list[dict]


class SummaryResponse(BaseModel):
    active_total: int
    by_db_type: dict[str, int]
    longest_ms: float


class CancelResponse(BaseModel):
    state: str  # "cancelled" | "not_found" | "already_finished" | "connection_gone"
    query_id: int


# ── Endpoints ───────────────────────────────────────────────────────────────


@router.get("/queries", response_model=SnapshotResponse)
def list_queries(
    since_seq: int = Query(0, ge=0),
    include_completed: bool = Query(False),
) -> SnapshotResponse:
    _ensure_enabled()
    snap = query_registry.snapshot(
        since_seq=since_seq,
        full_sql=False,
        include_completed=include_completed,
    )
    return SnapshotResponse(**snap)


@router.get("/queries/summary", response_model=SummaryResponse)
def queries_summary() -> SummaryResponse:
    _ensure_enabled()
    return SummaryResponse(**query_registry.summary())


@router.get("/queries/{qid}")
def get_query(qid: int) -> dict:
    """Fetch the full SQL + attribution for a single in-flight query.

    Looks up the active row only — completed queries are returned via the
    snapshot endpoint with ``include_completed=true``."""
    _ensure_enabled()
    active = query_registry.get(qid)
    if active is None:
        raise HTTPException(status_code=404, detail="query_not_found")
    snap = query_registry.snapshot(since_seq=qid - 1, full_sql=True)
    row = next((r for r in snap["active"] if r["query_id"] == qid), None)
    if row is None:
        raise HTTPException(status_code=404, detail="query_not_found")
    return row


@router.post("/queries/{qid}/cancel", response_model=CancelResponse)
def cancel_query(qid: int, request: Request) -> CancelResponse:
    _ensure_enabled()
    admin_id = _admin_id_from_request(request)
    if not _check_cancel_rate(admin_id):
        raise HTTPException(
            status_code=429,
            detail="cancel rate-limit exceeded (10/sec)",
            headers={"Retry-After": "1"},
        )
    state = query_registry.cancel_query(qid, admin_id=admin_id)
    return CancelResponse(state=state, query_id=qid)


# ── App-config surface for frontend nav gating ──────────────────────────────


@router.get("/app-config/query-monitor")
def query_monitor_config() -> dict:
    """Tiny config endpoint the frontend hits on mount to decide whether to
    render the Live Query Monitor tab. Returns enabled=False (not 404) so
    the nav can render a stable shape regardless of the flag state."""
    return {"enabled": _enabled()}
