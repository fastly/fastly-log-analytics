"""Control Room router — Phase 0 stubs for the real-time control room.

All tab endpoints return canned data; mutation endpoints return 501.
The SSE endpoint emits synthetic heartbeat ticks every 5 seconds.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from backend.core.metadata.state import list_audit, record_audit
from backend.deps import ServiceId, require_admin, require_service_access
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, query_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["control-room"], responses=DEFAULT_ERROR_RESPONSES)

# ── Canned tab responses (Phase 0) ───────────────────────────────────────────

TAB_STUBS: dict[str, dict] = {
    "overview": {
        "tab": "overview",
        "status": "stub",
        "summary": {
            "requests_per_second": 0,
            "error_rate": 0.0,
            "cache_hit_ratio": 0.0,
            "bandwidth_mbps": 0.0,
        },
    },
    "performance": {
        "tab": "performance",
        "status": "stub",
        "latency_p50_ms": 0,
        "latency_p95_ms": 0,
        "latency_p99_ms": 0,
    },
    "origin": {
        "tab": "origin",
        "status": "stub",
        "origin_requests": 0,
        "origin_latency_ms": 0,
        "shield_hit_ratio": 0.0,
    },
    "security": {
        "tab": "security",
        "status": "stub",
        "threats_blocked": 0,
        "waf_events": 0,
        "rate_limited": 0,
    },
    "network": {
        "tab": "network",
        "status": "stub",
        "pop_count": 0,
        "healthy_pops": 0,
        "degraded_pops": 0,
    },
    "sessions": {
        "tab": "sessions",
        "status": "stub",
        "active_sessions": 0,
        "unique_visitors": 0,
    },
    "cost": {
        "tab": "cost",
        "status": "stub",
        "estimated_cost_usd": 0.0,
        "requests_billed": 0,
        "bandwidth_billed_gb": 0.0,
    },
    "insights": {
        "tab": "insights",
        "status": "stub",
        "active_insights": 0,
        "anomalies_detected": 0,
    },
    "admin_health": {
        "tab": "admin_health",
        "status": "stub",
        "sync_healthy": True,
        "last_sync_age_seconds": 0,
        "disk_usage_pct": 0.0,
    },
}


# ── Tab data endpoints (read) ────────────────────────────────────────────────


@router.get("/services/{service_id}/control-room/{tab}")
@query_errors()
def control_room_tab(
    tab: str,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
):
    """Return canned data for a control-room tab (Phase 0 stub)."""
    if tab not in TAB_STUBS:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail={"error": "unknown_tab", "tab": tab})
    return TAB_STUBS[tab]


# ── Mutation endpoints (admin-only, 501 stubs) ───────────────────────────────


class MutationStub(BaseModel):
    """Generic request body for Phase 0 mutation stubs."""

    action: str = ""
    payload: dict = {}


@router.post("/services/{service_id}/control-room/mitigations")
@query_errors()
def control_room_mitigations(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
    """Block/tarpit mitigation actions (Phase 0 stub)."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Mitigations not yet implemented."}
    )


@router.post("/services/{service_id}/control-room/rules")
@query_errors()
def control_room_rules(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
    """Create rules (Phase 0 stub)."""
    from fastapi import HTTPException

    raise HTTPException(status_code=501, detail={"error": "not_implemented", "message": "Rules not yet implemented."})


@router.post("/services/{service_id}/control-room/allowlist")
@query_errors()
def control_room_allowlist(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
    """Manage allowlist (Phase 0 stub)."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Allowlist not yet implemented."}
    )


@router.post("/services/{service_id}/control-room/big-red-button")
@query_errors()
def control_room_big_red_button(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
    """Emergency disable (Phase 0 stub)."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Big red button not yet implemented."}
    )


@router.post("/services/{service_id}/control-room/cost-governor")
@query_errors()
def control_room_cost_governor(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
    """Cost kill switch (Phase 0 stub)."""
    from fastapi import HTTPException

    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Cost governor not yet implemented."}
    )


# ── SSE real-time stream ─────────────────────────────────────────────────────


@router.get("/services/{service_id}/realtime-stream")
async def realtime_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> EventSourceResponse:
    """Synthetic metrics heartbeat every 5 seconds (Phase 0 stub).

    Emits ``metrics_tick`` events with zeroed counters. Phase 1 will wire
    this to rt.fastly.com for live data.
    """

    async def stream() -> AsyncIterator[str]:
        while True:
            if await request.is_disconnected():
                break
            tick = {
                "event": "metrics_tick",
                "event_schema_version": 1,
                "timestamp": datetime.now(UTC).isoformat(),
                "data": {
                    "requests_per_second": 0,
                    "error_rate": 0.0,
                    "cache_hit_ratio": 0.0,
                    "bandwidth_mbps": 0.0,
                },
            }
            yield json.dumps(tick)
            await asyncio.sleep(5)

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


# ── Onboarding wizard ────────────────────────────────────────────────────────


class WizardStepRequest(BaseModel):
    """Request body for recording a wizard step completion."""

    step: str
    details: dict = {}


@router.get("/services/{service_id}/control-room/wizard/state")
@query_errors()
def wizard_state(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
):
    """Return current wizard state from audit log."""
    entries = list_audit(service_id, limit=100)
    wizard_entries = [e for e in entries if e.get("event_type", "").startswith("wizard_")]
    completed_steps = [
        e["event_type"].replace("wizard_step_", "")
        for e in wizard_entries
        if e["event_type"].startswith("wizard_step_")
    ]
    return {
        "service_id": service_id,
        "completed_steps": completed_steps,
        "is_complete": any(e["event_type"] == "wizard_complete" for e in wizard_entries),
    }


@router.post("/services/{service_id}/control-room/wizard/step")
@query_errors()
def wizard_step(
    body: WizardStepRequest,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
):
    """Record a wizard step completion via audit log."""
    record_audit(
        service_id=service_id,
        event_type=f"wizard_step_{body.step}",
        details=body.details,
        actor="control_room_wizard",
    )
    return {"recorded": True, "step": body.step}
