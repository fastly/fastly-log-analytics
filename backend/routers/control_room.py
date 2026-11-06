"""Control Room router — Phase 1: passive observation with real data.

Overview and Cost tabs are backed by the rt.fastly.com polling stream.
Correlator and error-stream endpoints provide ClickHouse-driven data.
Mutation endpoints remain 501 stubs.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from backend.core.fos_client import get_source_for_service
from backend.core.request_context import ServiceId, require_admin, require_service_access
from backend.core.share_db import mask_ip
from backend.high_scale.ingest_controller import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models.common import (
    DEFAULT_ERROR_RESPONSES,
    ControlRoomActionResponse,
    ControlRoomTabResponse,
    CorrelateRequest,
    CorrelateResponse,
    CorrelatorFreshness,
    CorrelatorTopRow,
    MutationStub,
    RealtimeSeedResponse,
)
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import safe_iso
from backend.utils.router_utils import query_errors
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["control-room"], responses=DEFAULT_ERROR_RESPONSES)
control_room_router = router

# ClickHouse dialect; ``{service_id:String}`` is a ClickHouse bind parameter, not a format field.
RECENT_HIGH_SCALE_ERRORS = """
SELECT
    toString(event_id) AS event_id,
    event_timestamp AS timestamp,
    toInt32OrZero(custom_fields['status']) AS status,
    url,
    custom_fields['pop'] AS pop,
    client_ip
FROM request_facts
WHERE service_id={service_id:String}
  AND event_timestamp >= now() - INTERVAL 2 MINUTE
  AND toInt32OrZero(custom_fields['status']) BETWEEN 400 AND 599
  AND publication_state = 'visible'
ORDER BY event_timestamp DESC
LIMIT 20
"""

HIGH_SCALE_CORRELATE_TOP_VALUES = """
SELECT
    toString({dim_expr}) AS value,
    count() AS aggregate_count
FROM request_facts
WHERE service_id={service_id:String}
  AND event_timestamp >= {start:DateTime64(3)}
  AND event_timestamp <= {end:DateTime64(3)}
  AND toInt32OrZero(custom_fields['status']) BETWEEN 400 AND 599
  AND {fence}
GROUP BY value
HAVING value != ''
ORDER BY aggregate_count DESC, value ASC
LIMIT {limit:UInt32}
"""


def recent_high_scale_error_events(client: Any, service_id: str) -> list[dict[str, Any]]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) for a ClickHouse service."""
    rows = client.execute(RECENT_HIGH_SCALE_ERRORS, {"service_id": service_id})
    return [
        {
            "event_id": str(row["event_id"]),
            "timestamp": safe_iso(row["timestamp"]),
            "status": int(row["status"]),
            "url": str(row.get("url") or ""),
            "pop": str(row.get("pop") or ""),
            "client_ip": str(row.get("client_ip") or ""),
        }
        for row in rows
    ]


# ── Canned tab responses ────────────────────────────────────────────────────

TAB_STUBS: dict[str, dict[str, Any]] = {
    "overview": {
        "tab": "overview",
        "status": "live",
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


# ── Mutation endpoints (admin-only, 501 stubs) ───────────────────────────────


@control_room_router.post("/services/{service_id}/control-room/mitigations", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_mitigations(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
) -> ControlRoomActionResponse:
    """Block/tarpit mitigation actions (Phase 0 stub)."""
    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Mitigations not yet implemented."}
    )


@control_room_router.post("/services/{service_id}/control-room/rules", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_rules(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
) -> ControlRoomActionResponse:
    """Create rules (Phase 0 stub)."""
    raise HTTPException(status_code=501, detail={"error": "not_implemented", "message": "Rules not yet implemented."})


@control_room_router.post("/services/{service_id}/control-room/allowlist", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_allowlist(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
) -> ControlRoomActionResponse:
    """Manage allowlist (Phase 0 stub)."""
    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Allowlist not yet implemented."}
    )


@control_room_router.post(
    "/services/{service_id}/control-room/big-red-button", response_model=ControlRoomActionResponse
)
@query_errors()
def control_room_big_red_button(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
) -> ControlRoomActionResponse:
    """Emergency disable (Phase 0 stub)."""
    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Big red button not yet implemented."}
    )


@control_room_router.post("/services/{service_id}/control-room/cost-governor", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_cost_governor(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
) -> ControlRoomActionResponse:
    """Cost kill switch (Phase 0 stub)."""
    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Cost governor not yet implemented."}
    )


# ── Realtime seed (REST) ────────────────────────────────────────────────────


def _fetch_seed_ticks(service_id: str, count: int = 60) -> list[dict[str, Any]]:
    """Fetch historical per-second ticks directly from the RT API."""
    from backend import config
    from backend.core.realtime import transform_single_second

    api_key = config.get_fastly_api_key(service_id)
    fastly_service_id = config.get_fastly_service_id(service_id)
    if not api_key or not fastly_service_id:
        return []

    from backend.core.fastly.client import fastly_rt

    try:
        rt_json = fastly_rt(f"/v1/channel/{fastly_service_id}/ts/h?limit=120", token=api_key)
    except Exception:
        logger.warning("RT seed fetch failed for %s", service_id)
        return []

    data_points = rt_json.get("Data") or []
    if not data_points:
        return []

    cursor = rt_json.get("Timestamp", 0)
    base_ts = cursor - len(data_points)
    ticks = []
    for i, point in enumerate(data_points):
        try:
            ts = datetime.fromtimestamp(base_ts + i, tz=UTC).isoformat()
            ticks.append(transform_single_second(point, ts))
        except Exception:
            continue

    return ticks[-count:]


@control_room_router.get(
    "/services/{service_id}/realtime-seed", response_model=RealtimeSeedResponse, response_model_exclude_unset=True
)
@query_errors()
async def realtime_seed(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> RealtimeSeedResponse:
    """Return the last 60 metrics ticks for instant chart hydration."""
    from backend.core.realtime import poller
    from backend.core.realtime.publisher import publisher as rt_publisher

    ticks = await asyncio.to_thread(rt_publisher.get_recent_ticks, service_id, 60)
    if len(ticks) >= 60:
        return RealtimeSeedResponse(ticks=ticks)

    ticks = await asyncio.to_thread(_fetch_seed_ticks, service_id)

    poller.ensure_polling(service_id)

    return RealtimeSeedResponse(ticks=ticks)


# ── SSE real-time stream ─────────────────────────────────────────────────────


@control_room_router.get("/services/{service_id}/realtime-stream")
async def realtime_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> EventSourceResponse:
    """Real-time metrics stream powered by rt.fastly.com polling."""
    from backend.core.realtime import poller
    from backend.core.realtime.publisher import publisher as rt_publisher
    from backend.utils.sse import iter_with_disconnect_ping

    poller.ensure_polling(service_id)

    async def stream() -> AsyncIterator[str]:
        async for payload in iter_with_disconnect_ping(rt_publisher.subscribe(service_id), request, ping_seconds=15):
            yield json.dumps(payload)

    return sse_response(stream())


# ── Recent error stream ─────────────────────────────────────────────────────


def _recent_high_scale_errors(service: Any) -> list[dict[str, Any]]:
    return recent_high_scale_error_events(service.client, service.service_id)


def _mask_error_events(events: list[dict[str, Any]], request: Request) -> list[dict[str, Any]]:
    if not mask_ips_for(getattr(request.state, "analyst_session", None)):
        return events
    return [{**event, "client_ip": mask_ip(event["client_ip"])} for event in events]


@control_room_router.get("/services/{service_id}/control-room/error-stream")
async def control_room_error_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> EventSourceResponse:
    """Stream bounded recent 4xx/5xx request events."""
    high_scale_service = registry.resolve(service_id)

    async def stream() -> AsyncIterator[dict[str, Any]]:
        seen: set[str] = set()
        while not await request.is_disconnected():
            try:
                if high_scale_service is not None:
                    events = await asyncio.to_thread(_recent_high_scale_errors, high_scale_service)
                else:
                    events = []
                for event in reversed(_mask_error_events(events, request)):
                    event_id = event["event_id"]
                    if event_id in seen:
                        continue
                    seen.add(event_id)
                    if len(seen) > 200:
                        seen = set(list(seen)[-100:])
                    yield {"event": "error_event", "data": json.dumps(event)}
            except Exception:
                logger.warning("Control Room error stream query failed for %s", service_id, exc_info=True)
            await asyncio.sleep(2)

    return sse_response(stream())


# ── Correlator endpoint ─────────────────────────────────────────────────────


@control_room_router.post(
    "/services/{service_id}/control-room/correlate", response_model=CorrelateResponse, response_model_exclude_unset=True
)
@query_errors()
async def control_room_correlate(
    body: CorrelateRequest,
    service_id: ServiceId,
    request: Request,
    _access: str | None = Depends(require_service_access),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> dict[str, Any]:
    """Top-N breakdown by a log field within a recent time window."""
    from backend.core import log_fields as field_registry
    from backend.high_scale._common import visible_publication_fence_sql
    from backend.high_scale.aggregate_query import _FACT_DIMENSION_EXPRESSIONS

    field_def = field_registry.try_get(body.dimension)
    if field_def is None:
        raise HTTPException(
            status_code=400,
            detail={"error": "unknown_dimension", "dimension": body.dimension},
        )

    col = field_def.code
    window_minutes = body.window_minutes
    limit = body.limit

    high_scale_service = registry.resolve(service_id)
    if high_scale_service is None:
        source = await asyncio.to_thread(get_source_for_service, service_id)
        if source is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_data_source", "service_id": service_id},
            )
        raise HTTPException(
            status_code=503,
            detail={"error": "high_scale_unavailable", "service_id": service_id},
        )

    now_utc = datetime.now(UTC)
    start = now_utc - timedelta(minutes=window_minutes)
    aggregate_dimension = "client_ip" if col == "ip" else col
    dim_expr = _FACT_DIMENSION_EXPRESSIONS["request"].get(
        aggregate_dimension,
        "custom_fields[{dimension:String}]",
    )
    watermark = high_scale_service.watermark_for("request")
    watermark.validate()
    sql = HIGH_SCALE_CORRELATE_TOP_VALUES.replace("{dim_expr}", dim_expr).replace(
        "{fence}", visible_publication_fence_sql("request")
    )
    params: dict[str, Any] = {
        "service_id": service_id,
        "dimension": aggregate_dimension,
        "start": start,
        "end": now_utc,
        "limit": int(limit),
    }
    hs_rows = await asyncio.to_thread(high_scale_service.client.execute, sql, params)
    top_values = [
        (str(row["value"]), max(0, int(row.get("aggregate_count") or row.get("count") or 0)))
        for row in hs_rows
        if str(row.get("value") or "") != ""
    ][:limit]
    if col == "ip" and mask_ips_for(getattr(request.state, "analyst_session", None)):
        top_values = [(mask_ip(value), count) for value, count in top_values]
    freshness_lag = (
        max(0.0, (now_utc - watermark.coverage_end.astimezone(UTC)).total_seconds())
        if watermark.coverage_end is not None
        else 0.0
    )
    return CorrelateResponse(
        dimension=body.dimension,
        top=[CorrelatorTopRow(value=value, count=count) for value, count in top_values],
        freshness=CorrelatorFreshness(
            latest_log_at=(watermark.coverage_end.isoformat() if watermark.coverage_end else None),
            lag_seconds=int(freshness_lag),
        ),
    ).model_dump()


# ── Tab data endpoints (read) — registered AFTER static /control-room/* paths ─


@control_room_router.get(
    "/services/{service_id}/control-room/{tab}",
    response_model=ControlRoomTabResponse,
    response_model_exclude_unset=True,
)
@query_errors()
def control_room_tab(
    tab: str,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> ControlRoomTabResponse:
    """Return data for a control-room tab."""
    if tab not in TAB_STUBS:
        raise HTTPException(status_code=404, detail={"error": "unknown_tab", "tab": tab})
    return ControlRoomTabResponse(**TAB_STUBS[tab])


__all__ = [
    "HIGH_SCALE_CORRELATE_TOP_VALUES",
    "RECENT_HIGH_SCALE_ERRORS",
    "TAB_STUBS",
    "control_room_allowlist",
    "control_room_big_red_button",
    "control_room_correlate",
    "control_room_cost_governor",
    "control_room_error_stream",
    "control_room_mitigations",
    "control_room_router",
    "control_room_rules",
    "control_room_tab",
    "realtime_seed",
    "realtime_stream",
    "recent_high_scale_error_events",
    "router",
]
