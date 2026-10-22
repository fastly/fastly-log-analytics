"""Client-side UX-event collector.

Lightweight cousin of /api/web-vitals. Where web-vitals captures the
SDK-typed performance metrics, this endpoint takes free-form UX events
the SPA emits to inform later decisions — e.g. which DataTable callers
users actually reorder columns on (so we can data-drive the
DataTableReadonly migration sweep instead of guessing).

Auth: analyst-safe — the same paths in the SPA that hit the DataTable
fire from share-mode sessions, so this endpoint is added to
``_ANALYST_ALLOWED_WRITE_PREFIXES`` in ``backend/utils/remote_access.py``.

Volume: bounded by user interaction (column drags, etc.) — orders of
magnitude lower than web-vitals' per-page-load cadence. structlog
emission so log aggregation can slice by event / pathname / cohort
without a new SQLite schema.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request

from backend.models.common import OkResponse, UxEventPayload
from backend.models.errors import DEFAULT_ERROR_RESPONSES

logger = logging.getLogger("backend.ux_events")

router = APIRouter(prefix="/api/ux-events", tags=["ux-events"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("", response_model=OkResponse, response_model_exclude_unset=True)
def report_ux_event(payload: UxEventPayload, request: Request) -> dict:
    """Log one UX event. Returns ``{"ok": true}``.

    Same structlog-extra shape as web_vitals so log slicing patterns
    transfer (filter by cohort / pathname / event)."""
    analyst_session = getattr(request.state, "analyst_session", None)
    cohort = "analyst" if analyst_session is not None else "admin"

    logger.info(
        "ux_event",
        extra={
            "ux_event": payload.event,
            "ux_pathname": payload.pathname,
            "ux_component_id": payload.component_id,
            "ux_details": payload.details,
            "ux_cohort": cohort,
        },
    )
    return {"ok": True}
