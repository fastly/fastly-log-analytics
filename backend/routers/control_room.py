"""Control Room router — Phase 1: passive observation with real data.

Overview and Cost tabs are backed by the rt.fastly.com polling stream.
Correlator and error-stream endpoints provide DuckDB/ClickHouse-driven data.
Mutation endpoints remain 501 stubs.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from backend.core.share_db.validation import mask_ip
from backend.deps import ServiceId, require_admin, require_service_access
from backend.high_scale.aggregate_query import AggregateRequest, query_clickhouse_aggregate
from backend.high_scale.registry_bootstrap import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.common import (
    ControlRoomActionResponse,
    ControlRoomTabResponse,
    CorrelateRequest,
    CorrelateResponse,
    CorrelatorFreshness,
    CorrelatorTopRow,
    MutationStub,
    RealtimeSeedResponse,
)
from backend.repositories._base import record_duckdb_query
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import query_errors
from backend.utils.sse import sse_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["control-room"], responses=DEFAULT_ERROR_RESPONSES)
control_room_router = router

RECENT_STANDARD_ERRORS = """
SELECT
    md5(concat_ws('|', CAST(timestamp AS VARCHAR), CAST(status AS VARCHAR),
                  COALESCE(url, ''), COALESCE(pop, ''), COALESCE(ip, ''),
                  CAST(row_number() OVER (
                      ORDER BY timestamp DESC, status, url, pop, ip
                  ) AS VARCHAR))) AS event_id,
    timestamp,
    TRY_CAST(status AS INTEGER) AS status,
    COALESCE(url, '') AS url,
    COALESCE(pop, '') AS pop,
    COALESCE(ip, '') AS client_ip
FROM {view}
WHERE timestamp >= now() - INTERVAL 2 MINUTE
  AND TRY_CAST(status AS INTEGER) BETWEEN 400 AND 599
ORDER BY timestamp DESC
LIMIT 20
"""

# ClickHouse (high-scale) dialect; ``{service_id:String}`` is a ClickHouse bind parameter, not a format field.
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

CORRELATE_TOP_VALUES = """
SELECT {col} AS value, COUNT(*) AS count
FROM {view}
WHERE timestamp >= ?
GROUP BY 1
ORDER BY 2 DESC
LIMIT ?
"""

LATEST_TIMESTAMP_WINDOWED = "SELECT MAX(timestamp) AS latest FROM {view} WHERE timestamp >= ?"

LATEST_TIMESTAMP = "SELECT MAX(timestamp) AS latest FROM {view}"


def recent_standard_error_events(con: Any, view_name: str, *, service_id: str | None = None) -> list[dict[str, Any]]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) from the standard-mode request view."""
    q = RECENT_STANDARD_ERRORS.format(view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q).fetchall()
    record_duckdb_query(q, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)
    return [
        {
            "event_id": str(row[0]),
            "timestamp": row[1].isoformat(),
            "status": int(row[2]),
            "url": str(row[3]),
            "pop": str(row[4]),
            "client_ip": str(row[5]),
        }
        for row in rows
    ]


def recent_high_scale_error_events(client: Any, service_id: str) -> list[dict[str, Any]]:
    """Last-2-minute 4xx/5xx events (max 20, newest first) for a high-scale (ClickHouse) service."""
    rows = client.execute(RECENT_HIGH_SCALE_ERRORS, {"service_id": service_id})
    return [
        {
            "event_id": str(row["event_id"]),
            "timestamp": row["timestamp"].isoformat(),
            "status": int(row["status"]),
            "url": str(row.get("url") or ""),
            "pop": str(row.get("pop") or ""),
            "client_ip": str(row.get("client_ip") or ""),
        }
        for row in rows
    ]


def correlate_top_values(
    con: Any,
    view_name: str,
    col: str,
    since: datetime,
    limit: int,
    *,
    service_id: str | None = None,
) -> tuple[list[Any], Any]:
    """``(top [(value, count)] since ``since``, MAX(timestamp) row)`` for one allow-listed column."""
    q_top = CORRELATE_TOP_VALUES.format(col=col, view=view_name)
    t0 = time.perf_counter()
    rows = con.execute(q_top, [since, limit]).fetchall()
    record_duckdb_query(q_top, round((time.perf_counter() - t0) * 1000, 2), service_id=service_id)

    q_fresh = LATEST_TIMESTAMP_WINDOWED.format(view=view_name)
    t1 = time.perf_counter()
    freshness_row = con.execute(q_fresh, [since]).fetchone()
    record_duckdb_query(q_fresh, round((time.perf_counter() - t1) * 1000, 2), service_id=service_id)

    if (not freshness_row or freshness_row[0] is None) and not rows:
        q_fallback = LATEST_TIMESTAMP.format(view=view_name)
        t2 = time.perf_counter()
        freshness_row = con.execute(q_fallback).fetchone()
        record_duckdb_query(q_fallback, round((time.perf_counter() - t2) * 1000, 2), service_id=service_id)
    return rows, freshness_row


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


def _recent_standard_errors(source: dict[str, Any], service_id: str) -> list[dict[str, Any]]:
    from backend.core import duckdb as _db
    from backend.core.storage import execute_with_stale_view_retry
    from backend.repositories._base import _safe_table

    con = _db.get_connection(source=source, read_only=True)
    try:
        view_name = _safe_table(service_id.replace("-", "_"))
        return execute_with_stale_view_retry(
            con,
            source,
            recent_standard_error_events,
            view_name,
            service_id=service_id,
        )
    finally:
        con.close()


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
    from backend.core import duckdb as _db

    source = await asyncio.to_thread(_db.get_source_for_service, service_id)
    high_scale_service = registry.resolve(service_id)

    async def stream() -> AsyncIterator[dict[str, Any]]:
        seen: set[str] = set()
        while not await request.is_disconnected():
            try:
                if high_scale_service is not None:
                    events = await asyncio.to_thread(_recent_high_scale_errors, high_scale_service)
                elif source is not None:
                    events = await asyncio.to_thread(_recent_standard_errors, source, service_id)
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
    from backend.core import duckdb as _db
    from backend.core import field_registry
    from backend.repositories._base import _safe_table

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
    if high_scale_service is not None:
        start = datetime.now(UTC) - timedelta(minutes=window_minutes)
        aggregate_dimension = "client_ip" if col == "ip" else col
        response = await asyncio.to_thread(
            query_clickhouse_aggregate,
            high_scale_service.client,
            AggregateRequest(
                service_id=service_id,
                domain="request",
                start=start,
                end=datetime.now(UTC),
                dimension=aggregate_dimension,
            ),
            watermark=high_scale_service.watermark_for("request"),
        )
        top_values = list(response.top_values)[:limit]
        if col == "ip" and mask_ips_for(getattr(request.state, "analyst_session", None)):
            top_values = [(mask_ip(value), count) for value, count in top_values]
        return CorrelateResponse(
            dimension=body.dimension,
            top=[CorrelatorTopRow(value=value, count=count) for value, count in top_values],
            freshness=CorrelatorFreshness(
                latest_log_at=(
                    response.watermark.coverage_end.isoformat() if response.watermark.coverage_end else None
                ),
                lag_seconds=int(response.freshness_lag_seconds),
            ),
        ).model_dump()

    source = await asyncio.to_thread(_db.get_source_for_service, service_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "no_data_source", "service_id": service_id},
        )

    def _query() -> tuple[list[Any], Any]:
        from backend.core.storage import execute_with_stale_view_retry

        con = _db.get_connection(source=source, read_only=True)
        try:
            view_name = _safe_table(service_id.replace("-", "_"))
            return execute_with_stale_view_retry(
                con,
                source,
                correlate_top_values,
                view_name,
                col,
                datetime.now(UTC) - timedelta(minutes=window_minutes),
                limit,
                service_id=service_id,
            )
        finally:
            con.close()

    rows, freshness_row = await asyncio.to_thread(_query)

    latest_log_at = freshness_row[0] if freshness_row else None
    lag_seconds = 0
    if latest_log_at:
        lag_seconds = max(0, int((datetime.now(UTC) - latest_log_at).total_seconds()))

    top_items = [CorrelatorTopRow(value=str(r[0]), count=r[1]) for r in rows]
    if col == "ip" and mask_ips_for(getattr(request.state, "analyst_session", None)):
        top_items = [CorrelatorTopRow(value=mask_ip(str(item.value or "")), count=item.count) for item in top_items]

    return CorrelateResponse(
        dimension=body.dimension,
        top=top_items,
        freshness=CorrelatorFreshness(
            latest_log_at=latest_log_at.isoformat() if latest_log_at else None,
            lag_seconds=lag_seconds,
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


_LEGACY_REEXPORTS: dict[str, str] = {
    "alerts_router": "backend.routers.alerts",
    "validate_webhook_url": "backend.routers.alerts",
    "views_router": "backend.routers.alerts",
    "FEATURE_REQUIRED_FIELDS": "backend.routers.audit",
    "audit_router": "backend.routers.audit",
    "log_field_audit": "backend.routers.audit",
    "wizard_state": "backend.routers.audit",
    "wizard_step": "backend.routers.audit",
    "usage_router": "backend.routers.usage",
}


def __getattr__(name: str) -> Any:
    target_mod = _LEGACY_REEXPORTS.get(name)
    if target_mod is not None:
        return getattr(importlib.import_module(target_mod), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CORRELATE_TOP_VALUES",
    "LATEST_TIMESTAMP",
    "LATEST_TIMESTAMP_WINDOWED",
    "RECENT_HIGH_SCALE_ERRORS",
    "RECENT_STANDARD_ERRORS",
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
    "correlate_top_values",
    "realtime_seed",
    "realtime_stream",
    "recent_high_scale_error_events",
    "recent_standard_error_events",
    "router",
]
