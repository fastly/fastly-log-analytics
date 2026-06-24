"""Client-side Web Vitals collector.

Accepts the metric payloads the `web-vitals` library emits in the browser
(LCP / INP / CLS / FCP / TTFB / etc.). When collection is enabled it logs
each metric via structlog (so log aggregation can slice perf by route /
metric / rating) AND appends it to a JSONL sink for offline analysis by
``scripts/analyze_web_vitals.py``.

Collection is opt-in via the ``WEB_VITALS_COLLECT`` env var (default off);
see ``backend/core/web_vitals_store.py``. The frontend mirrors that flag
through ``/api/bootstrap`` and skips sending entirely when it's off, so a
disabled deployment sees zero web-vitals traffic. This endpoint re-checks
the flag as the authoritative gate in case a stale client still POSTs.

Auth: analyst-safe — the SPA mounts the reporter for every session,
including share-mode analysts. The endpoint is added to
``_ANALYST_ALLOWED_WRITE_PREFIXES`` in ``backend/utils/remote_access.py``.

Volume: ~5 metrics per page load per user. Cheap append path; no
rate-limit needed at the current scale. If a tenant ever ramps to
thousands of concurrent analysts, this is the natural place to add a
token-bucket per ``analyst_session_id``.
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from backend.core import web_vitals_store
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.utils.date_utils import iso_z_now

logger = logging.getLogger("backend.web_vitals")

router = APIRouter(prefix="/api/web-vitals", tags=["web-vitals"], responses=DEFAULT_ERROR_RESPONSES)


class WebVitalsPayload(BaseModel):
    """Mirrors the web-vitals SDK metric shape (v5 API).

    Field names match the JS Metric object so the frontend can pass it
    through without reshape. ``rating`` is computed by the SDK against
    Google's good/needs-improvement/poor thresholds.
    """

    # ``id`` is a stable per-page-load id the SDK uses for delta updates
    # (CLS / INP keep updating the same metric throughout a page's life).
    id: str = Field(..., max_length=120)
    name: Literal["CLS", "FCP", "FID", "INP", "LCP", "TTFB"]
    value: float
    rating: Literal["good", "needs-improvement", "poor"]
    # Optional context.
    pathname: str | None = Field(default=None, max_length=300)
    navigation_type: str | None = Field(default=None, max_length=40)
    delta: float | None = None


@router.post("")
def report_web_vitals(payload: WebVitalsPayload, request: Request) -> dict:
    """Record one Web Vitals metric event. Returns ``{"ok": true}``.

    When collection is disabled the sample is dropped quietly (still a
    200 so the fire-and-forget client never sees an error). When enabled
    it's both logged (structlog key/value emission for log-analysis) and
    appended to the JSONL sink for ``scripts/analyze_web_vitals.py``.
    """
    if not web_vitals_store.collection_enabled():
        return {"ok": True}

    # ``request.state.analyst_session`` is populated by
    # RemoteAccessMiddleware for share-mode callers and is None for admin.
    analyst_session = getattr(request.state, "analyst_session", None)
    cohort = "analyst" if analyst_session is not None else "admin"

    logger.info(
        "web_vitals",
        extra={
            "web_vitals_name": payload.name,
            "web_vitals_value": payload.value,
            "web_vitals_rating": payload.rating,
            "web_vitals_id": payload.id,
            "web_vitals_pathname": payload.pathname,
            "web_vitals_navigation_type": payload.navigation_type,
            "web_vitals_delta": payload.delta,
            "web_vitals_cohort": cohort,
        },
    )

    # Persist for offline analysis. Keys are flat + analyzer-friendly.
    web_vitals_store.append_sample(
        {
            "ts": iso_z_now(),
            "name": payload.name,
            "value": payload.value,
            "rating": payload.rating,
            "id": payload.id,
            "pathname": payload.pathname,
            "navigation_type": payload.navigation_type,
            "delta": payload.delta,
            "cohort": cohort,
        }
    )
    return {"ok": True}
