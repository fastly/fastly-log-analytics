"""Control Room router — Phase 1: passive observation with real data.

Overview and Cost tabs are backed by the rt.fastly.com polling stream.
Log-field-audit and correlator endpoints provide DuckDB-driven data.
Mutation endpoints remain 501 stubs.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from backend.core.metadata.state import list_audit, record_audit
from backend.core.share_db.validation import mask_ip
from backend.deps import ServiceId, require_admin, require_service_access
from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.registry import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, query_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["control-room"], responses=DEFAULT_ERROR_RESPONSES)

# ── Canned tab responses ────────────────────────────────────────────────────

TAB_STUBS: dict[str, dict] = {
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


# ── Tab data endpoints (read) ────────────────────────────────────────────────


@router.get("/services/{service_id}/control-room/{tab}")
@query_errors()
def control_room_tab(
    tab: str,
    service_id: ServiceId,
):
    """Return data for a control-room tab."""
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


# ── Realtime seed (REST) ────────────────────────────────────────────────────


def _fetch_seed_ticks(service_id: str, count: int = 60) -> list[dict]:
    """Fetch historical per-second ticks directly from the RT API.

    Bypasses the poller thread entirely — no threading coordination needed.
    """
    from backend import config
    from backend.core.realtime.transform import transform_single_second

    api_key = config.get_fastly_api_key(service_id)
    fastly_service_id = config.get_fastly_service_id(service_id)
    if not api_key or not fastly_service_id:
        return []

    import requests as req_lib

    url = f"https://rt.fastly.com/v1/channel/{fastly_service_id}/ts/h?limit=120"
    try:
        resp = req_lib.get(url, headers={"Fastly-Key": api_key}, timeout=10)
        resp.raise_for_status()
        rt_json = resp.json()
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


@router.get("/services/{service_id}/realtime-seed")
@query_errors()
async def realtime_seed(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
):
    """Return the last 60 metrics ticks for instant chart hydration."""
    from backend.core.realtime.poller import poller
    from backend.core.realtime.publisher import publisher as rt_publisher

    # Sync Redis I/O in valkey-backplane mode — keep it off the event loop.
    ticks = await asyncio.to_thread(rt_publisher.get_recent_ticks, service_id, 60)
    if len(ticks) >= 60:
        return {"ticks": ticks}

    ticks = await asyncio.to_thread(_fetch_seed_ticks, service_id)

    poller.ensure_polling(service_id)

    return {"ticks": ticks}


# ── SSE real-time stream ─────────────────────────────────────────────────────


@router.get("/services/{service_id}/realtime-stream")
async def realtime_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> EventSourceResponse:
    """Real-time metrics stream powered by rt.fastly.com polling.

    Emits ``metrics_tick`` events with live counters. The poller starts
    on first subscriber connect and suspends when all subscribers disconnect.
    """
    from backend.core.realtime.poller import poller
    from backend.core.realtime.publisher import publisher as rt_publisher
    from backend.utils.sse_subscription import iter_with_disconnect_ping

    poller.ensure_polling(service_id)

    async def stream() -> AsyncIterator[str]:
        async for payload in iter_with_disconnect_ping(rt_publisher.subscribe(service_id), request, ping_seconds=15):
            yield json.dumps(payload)

    return EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)


# ── Recent error stream ─────────────────────────────────────────────────────


def _recent_standard_errors(source: dict, service_id: str) -> list[dict]:
    from backend.core import duckdb as _db
    from backend.repositories._base import _safe_table

    con = _db.get_connection(source=source, read_only=True)
    try:
        view_name = _safe_table(service_id.replace("-", "_"))
        rows = con.execute(
            f"""
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
            FROM {view_name}
            WHERE timestamp >= now() - INTERVAL 2 MINUTE
              AND TRY_CAST(status AS INTEGER) BETWEEN 400 AND 599
            ORDER BY timestamp DESC
            LIMIT 20
            """
        ).fetchall()
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
    finally:
        con.close()


def _recent_high_scale_errors(service: Any) -> list[dict]:
    rows = service.client.execute(
        """
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
        """,
        {"service_id": service.service_id},
    )
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


def _mask_error_events(events: list[dict], request: Request) -> list[dict]:
    if not mask_ips_for(getattr(request.state, "analyst_session", None)):
        return events
    return [{**event, "client_ip": mask_ip(event["client_ip"])} for event in events]


@router.get("/services/{service_id}/control-room/error-stream")
async def control_room_error_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
) -> EventSourceResponse:
    """Stream bounded recent 4xx/5xx request events.

    This analyst-reachable SSE path is explicitly listed in
    ``remote_access._ANALYST_SSE_ALLOWLIST``.
    """
    from backend.core import duckdb as _db

    source = _db.get_source_for_service(service_id)
    high_scale_service = registry.resolve(service_id)

    async def stream() -> AsyncIterator[dict]:
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

    return EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)


# ── Log-field-audit endpoint ────────────────────────────────────────────────

FEATURE_REQUIRED_FIELDS: dict[str, list[str]] = {
    "Dashboard": ["status", "cache_status", "url", "method"],
    "Performance": ["time_elapsed", "time_to_first_byte"],
    "Origin": ["is_cacheable", "origin_host"],
    "Security": ["client_ip", "geo_country", "user_agent"],
    "Sessions": ["session_id", "url"],
    "Network": ["pop", "geo_country"],
    "Insights": ["status", "url", "client_ip", "user_agent"],
}


@router.get("/services/{service_id}/log-field-audit")
@query_errors()
def log_field_audit(
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
):
    """Check which log fields are enabled and which features they support."""
    from backend import config as svcconfig
    from backend.core.log_fields import resolve_enabled_fields

    cfg = svcconfig.load_config(service_id)
    log_fields_cfg: dict | None = cfg.get("log_fields") if cfg else None
    enabled = resolve_enabled_fields(log_fields_cfg or {})
    enabled_ids = {f if isinstance(f, str) else f.get("id", f) for f in enabled}

    features = []
    for feature_name, required in FEATURE_REQUIRED_FIELDS.items():
        missing = [f for f in required if f not in enabled_ids]
        features.append(
            {
                "name": feature_name,
                "required_fields": required,
                "status": "ok" if not missing else "missing_fields",
                "missing_fields": missing,
            }
        )

    return {
        "service_id": service_id,
        "enabled_fields": sorted(enabled_ids),
        "features": features,
    }


# ── Correlator endpoint ─────────────────────────────────────────────────────


class CorrelateRequest(BaseModel):
    dimension: str
    window_minutes: int = Field(default=5, ge=1, le=60)
    limit: int = Field(default=10, ge=1, le=100)


@router.post("/services/{service_id}/control-room/correlate")
@query_errors()
async def control_room_correlate(
    body: CorrelateRequest,
    service_id: ServiceId,
    request: Request,
    _access: str | None = Depends(require_service_access),
    registry: HighScaleServiceRegistryProtocol = Depends(get_high_scale_service_registry),
):
    """Top-N breakdown by a log field within a recent time window."""
    from fastapi import HTTPException

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
        response = query_clickhouse_aggregate(
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
        return {
            "dimension": body.dimension,
            "top": [{"value": value, "count": count} for value, count in top_values],
            "freshness": {
                "latest_log_at": (
                    response.watermark.coverage_end.isoformat() if response.watermark.coverage_end else None
                ),
                "lag_seconds": int(response.freshness_lag_seconds),
            },
        }

    source = _db.get_source_for_service(service_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "no_data_source", "service_id": service_id},
        )

    def _query():
        con = _db.get_connection(source=source, read_only=True)
        try:
            view_name = _safe_table(service_id.replace("-", "_"))
            result = con.execute(
                f"""
                SELECT {col} AS value, COUNT(*) AS count
                FROM {view_name}
                WHERE timestamp >= ?
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT ?
                """,
                [datetime.now(UTC) - timedelta(minutes=window_minutes), limit],
            ).fetchall()
            freshness_row = con.execute(f"SELECT MAX(timestamp) AS latest FROM {view_name}").fetchone()
            return result, freshness_row
        finally:
            con.close()

    rows, freshness_row = await asyncio.to_thread(_query)

    latest_log_at = freshness_row[0] if freshness_row else None
    lag_seconds = 0
    if latest_log_at:
        lag_seconds = max(0, int((datetime.now(UTC) - latest_log_at).total_seconds()))

    top = [{"value": str(r[0]), "count": r[1]} for r in rows]
    if col == "ip" and mask_ips_for(getattr(request.state, "analyst_session", None)):
        top = [{"value": mask_ip(item["value"]), "count": item["count"]} for item in top]

    return {
        "dimension": body.dimension,
        "top": top,
        "freshness": {
            "latest_log_at": latest_log_at.isoformat() if latest_log_at else None,
            "lag_seconds": lag_seconds,
        },
    }


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
