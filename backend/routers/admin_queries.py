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
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from backend.core import metadata as _meta_mod
from backend.core.query_registry import query_registry
from backend.core.settings import Settings
from backend.deps import get_service_id

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


@router.get("/slow-queries")
def list_persisted_slow_queries(
    service_id: str = Depends(get_service_id),
    since_hours: int = Query(24, ge=1, le=24 * 30),
    threshold_ms: float = Query(100.0, ge=0.0),
    kind: str | None = Query(None, pattern="^(analyst|admin|cron|system)$"),
    db_type: str | None = Query(None, pattern="^(DuckDB|SQLite)$"),
    sort: str = Query("recent", pattern="^(recent|duration)$"),
    limit: int = Query(200, ge=1, le=2000),
) -> dict[str, Any]:
    """Persistent slow-SQL history from the per-service ``slow_queries``
    SQLite table — the durable backing store for the Notable Slow
    Queries panel beyond the in-memory ring buffer's ~10-30 min /
    restart-bounded window.

    Server-side filters keep the response payload small:
    ``threshold_ms`` is applied at the SQL level (indexed scan),
    ``kind`` / ``db_type`` are equality filters on low-cardinality
    columns. ``limit`` clamped at 2000 so a runaway client query can't
    page the whole 7-day window in one shot.

    Sort: ``recent`` (started_at_utc DESC, the panel default) or
    ``duration`` (duration_ms DESC, the "what was slowest" variant).
    """
    _ensure_enabled()
    if not service_id:
        raise HTTPException(status_code=400, detail="service_id required")
    since_utc = time.time() - since_hours * 3600
    rows = _meta_mod.list_slow_queries(
        service_id,
        since_utc=since_utc,
        threshold_ms=threshold_ms,
        kind=kind,
        db_type=db_type,
        sort_by_duration=(sort == "duration"),
        limit=limit,
    )
    # Re-shape into the same dict layout the in-memory ``completed`` array
    # uses so the frontend can render them through the existing
    # ``CompletedRow`` type without a separate path. ``attribution`` is
    # nested to match ``_attribution_payload``'s shape.
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "query_id": r["query_id"],
                "db_type": r["db_type"],
                "service_id": r["service_id"],
                "sql_preview": r["sql_preview"],
                "sql": r["sql_full"],
                "sql_len": r["sql_len"],
                "started_at_utc": r["started_at_utc"],
                "ended_at_utc": r["ended_at_utc"],
                "duration_ms": r["duration_ms"],
                "outcome": r["outcome"],
                "error_type": r["error_type"],
                "error_message": r["error_message"],
                "peak_memory_mb": r["peak_memory_mb"],
                "attribution": {
                    "kind": r["attr_kind"],
                    "label": r["attr_label"],
                    "principal_id": r["attr_principal_id"],
                    "caller_qualname": r["attr_caller_qualname"],
                    "caller_file": r["attr_caller_file"],
                    "request_path": r["attr_request_path"],
                    "request_id": r["attr_request_id"],
                    "cron_job": r["attr_cron_job"],
                    "cron_run_id": r["attr_cron_run_id"],
                    "pool_slot": r["attr_pool_slot"],
                },
            }
        )
    return {"rows": out, "since_hours": since_hours, "threshold_ms": threshold_ms}


@router.get("/queries/{qid}")
def get_query(qid: int) -> dict[str, Any]:
    """Fetch the full SQL + attribution for a single in-flight query.

    Looks up the active row only — completed queries are returned via the
    snapshot endpoint with ``include_completed=true``."""
    _ensure_enabled()
    active = query_registry.get(qid)
    if active is None:
        raise HTTPException(status_code=404, detail="query_not_found")
    snap = query_registry.snapshot(since_seq=qid - 1, full_sql=True)
    row: dict[str, Any] | None = next((r for r in snap["active"] if r["query_id"] == qid), None)
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
