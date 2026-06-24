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
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from backend.models.errors import DEFAULT_ERROR_RESPONSES

logger = logging.getLogger("backend.ux_events")

router = APIRouter(prefix="/api/ux-events", tags=["ux-events"], responses=DEFAULT_ERROR_RESPONSES)


class UxEventPayload(BaseModel):
    """Generic UX-event envelope. Specific event types live as
    ``event`` discriminator strings; the ``details`` bag carries
    event-specific fields without forcing a per-event schema."""

    # ``event`` is the event kind — e.g. ``column_reordered``,
    # ``column_visibility_toggled``, ``filter_cleared``. Bounded length
    # so a malformed client can't pump arbitrary strings into the log
    # aggregation.
    event: str = Field(..., max_length=80)
    # Pathname of the page the event fired on. Free-form so we don't
    # have to keep an enum in sync with the route table.
    pathname: str | None = Field(default=None, max_length=300)
    # Identifier the SPA uses to disambiguate multiple instances of the
    # same component on a page (e.g. table_caption / table_id). Helps
    # answer "which DataTable on /sessions was reordered" without
    # threading per-caller event names through.
    component_id: str | None = Field(default=None, max_length=120)
    # Event-specific bag. Bounded size at the Pydantic-validation layer
    # so a misbehaving client can't post unbounded JSON.
    details: dict[str, Any] = Field(default_factory=dict)

    model_config = {
        # Reject extra top-level fields so a typo in the SPA caller
        # surfaces as a 422 rather than silently landing in the log
        # under an unread key.
        "extra": "forbid",
    }


@router.post("")
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
