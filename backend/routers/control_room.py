"""Control Room router — Phase 1: passive observation with real data.

Overview and Cost tabs are backed by the rt.fastly.com polling stream.
Log-field-audit and correlator endpoints provide DuckDB-driven data.
Mutation endpoints remain 501 stubs.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import sys as _sys
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlparse

import duckdb
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response
from sse_starlette.sse import EventSourceResponse

from backend.core.metadata.state import list_audit, record_audit
from backend.core.share_db.validation import mask_ip
from backend.deps import ServiceId, get_con, get_service_id, require_admin, require_service_access
from backend.high_scale.aggregate_query import AggregateRequest, query_clickhouse_aggregate
from backend.high_scale.registry_bootstrap import HighScaleServiceRegistryProtocol, get_high_scale_service_registry
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.common import (
    Alert,
    AlertEvaluateData,
    AlertEvaluateRequest,
    AlertEvaluateResponse,
    AlertEvaluateResultItem,
    AlertListResponse,
    AlertPreviewResponse,
    AlertResponse,
    AlertTestWebhookData,
    AlertTestWebhookRequest,
    AlertTestWebhookResponse,
    ControlRoomActionResponse,
    ControlRoomTabResponse,
    CorrelateRequest,
    CorrelateResponse,
    LogFieldAuditResponse,
    MutationStub,
    RealtimeSeedResponse,
    SavedView,
    SavedViewRecord,
    ViewSaveResponse,
    WizardStateResponse,
    WizardStepRequest,
    WizardStepResponse,
    _ToggleBody,
)
from backend.repositories import assets as repo
from backend.repositories._base import record_duckdb_query
from backend.utils.auth import (
    analyst_allowed_services as _analyst_allowed_services,
)
from backend.utils.auth import (
    mask_ips_for,
    require_owned_row,
    require_service_in_scope,
)
from backend.utils.router_utils import (
    not_found,
    query_errors,
    redact_error_text,
    require_service_id,
    sync_admin_state,
)
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


def recent_standard_error_events(con, view_name: str, *, service_id: str | None = None) -> list[dict]:
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


def recent_high_scale_error_events(client: Any, service_id: str) -> list[dict]:
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
    con,
    view_name: str,
    col: str,
    since: datetime,
    limit: int,
    *,
    service_id: str | None = None,
) -> tuple[list, Any]:
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


# ── Mutation endpoints (admin-only, 501 stubs) ───────────────────────────────


@control_room_router.post("/services/{service_id}/control-room/mitigations", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_mitigations(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
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
):
    """Create rules (Phase 0 stub)."""
    raise HTTPException(status_code=501, detail={"error": "not_implemented", "message": "Rules not yet implemented."})


@control_room_router.post("/services/{service_id}/control-room/allowlist", response_model=ControlRoomActionResponse)
@query_errors()
def control_room_allowlist(
    body: MutationStub,
    service_id: ServiceId,
    _admin: None = Depends(require_admin),
    _access: str | None = Depends(require_service_access),
):
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
):
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
):
    """Cost kill switch (Phase 0 stub)."""
    raise HTTPException(
        status_code=501, detail={"error": "not_implemented", "message": "Cost governor not yet implemented."}
    )


# ── Realtime seed (REST) ────────────────────────────────────────────────────


def _fetch_seed_ticks(service_id: str, count: int = 60) -> list[dict]:
    """Fetch historical per-second ticks directly from the RT API.

    Bypasses the poller thread entirely — no threading coordination needed.
    """
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
):
    """Return the last 60 metrics ticks for instant chart hydration."""
    from backend.core.realtime import poller
    from backend.core.realtime.publisher import publisher as rt_publisher

    # Sync Redis I/O in valkey-backplane mode — keep it off the event loop.
    ticks = await asyncio.to_thread(rt_publisher.get_recent_ticks, service_id, 60)
    if len(ticks) >= 60:
        return {"ticks": ticks}

    ticks = await asyncio.to_thread(_fetch_seed_ticks, service_id)

    poller.ensure_polling(service_id)

    return {"ticks": ticks}


# ── SSE real-time stream ─────────────────────────────────────────────────────


@control_room_router.get("/services/{service_id}/realtime-stream")
async def realtime_stream(
    request: Request,
    service_id: ServiceId,
    _access: str | None = Depends(require_service_access),
) -> EventSourceResponse:
    """Real-time metrics stream powered by rt.fastly.com polling.

    Emits ``metrics_tick`` events with live counters. The poller starts
    on first subscriber connect and suspends when all subscribers disconnect.
    """
    from backend.core.realtime import poller
    from backend.core.realtime.publisher import publisher as rt_publisher
    from backend.utils.sse import iter_with_disconnect_ping

    poller.ensure_polling(service_id)

    async def stream() -> AsyncIterator[str]:
        async for payload in iter_with_disconnect_ping(rt_publisher.subscribe(service_id), request, ping_seconds=15):
            yield json.dumps(payload)

    return sse_response(stream())


# ── Recent error stream ─────────────────────────────────────────────────────


def _recent_standard_errors(source: dict, service_id: str) -> list[dict]:
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


def _recent_high_scale_errors(service: Any) -> list[dict]:
    return recent_high_scale_error_events(service.client, service.service_id)


def _mask_error_events(events: list[dict], request: Request) -> list[dict]:
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
    """Stream bounded recent 4xx/5xx request events.

    This analyst-reachable SSE path is explicitly listed in
    ``remote_access._ANALYST_SSE_ALLOWLIST``.
    """
    from backend.core import duckdb as _db

    source = await asyncio.to_thread(_db.get_source_for_service, service_id)
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

    return sse_response(stream())


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


@control_room_router.get(
    "/services/{service_id}/log-field-audit", response_model=LogFieldAuditResponse, response_model_exclude_unset=True
)
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
):
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

    source = await asyncio.to_thread(_db.get_source_for_service, service_id)
    if source is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "no_data_source", "service_id": service_id},
        )

    def _query():
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


@control_room_router.get(
    "/services/{service_id}/control-room/wizard/state",
    response_model=WizardStateResponse,
    response_model_exclude_unset=True,
)
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


@control_room_router.post(
    "/services/{service_id}/control-room/wizard/step",
    response_model=WizardStepResponse,
    response_model_exclude_unset=True,
)
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
):
    """Return data for a control-room tab."""
    if tab not in TAB_STUBS:
        raise HTTPException(status_code=404, detail={"error": "unknown_tab", "tab": tab})
    return TAB_STUBS[tab]


# ── Alerts & Saved Views (folded from alerts.py) ─────────────────────────────

alerts_router = APIRouter(prefix="/api/alerts", tags=["alerts"], responses=DEFAULT_ERROR_RESPONSES)

AlertStatusFilter = Literal["all", "alerting", "ok", "muted"]

_BLOCKED_HOSTNAMES = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata",
        "ip6-localhost",
        "ip6-loopback",
    }
)


def validate_webhook_url(url: str) -> str:
    """Validate a webhook/notification channel URL to prevent SSRF."""
    cleaned = (url or "").strip()
    if not cleaned:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_webhook_url", "message": "Webhook URL must not be empty."},
        )
    parsed = urlparse(cleaned)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_webhook_url",
                "message": "Webhook URL must use http or https scheme with a valid hostname.",
            },
        )
    host = parsed.hostname.lower().rstrip(".")
    if host in _BLOCKED_HOSTNAMES or host.endswith(".internal") or host.endswith(".local"):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_webhook_url",
                "message": f"Webhook target hostname '{host}' is not allowed (SSRF protection).",
            },
        )
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None

    if ip is not None and (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_webhook_url",
                "message": f"Webhook target IP '{host}' is in a restricted range (SSRF protection).",
            },
        )
    return cleaned


def _validate_alert_urls(alert: Alert) -> None:
    if alert.webhook_url:
        validate_webhook_url(alert.webhook_url)
    for ch in alert.channels or []:
        ch_url = ch.get("url") if isinstance(ch, dict) else getattr(ch, "url", None)
        if ch_url:
            validate_webhook_url(ch_url)


def _require_admin_mutation(request: Request) -> None:
    """Reject mutations from Analyst Path B sessions (defense-in-depth behind RemoteAccessMiddleware)."""
    if _analyst_allowed_services(request) is not None:
        raise HTTPException(
            status_code=403,
            detail={"error": "admin_only", "message": "Alert mutations require admin access."},
        )


def _filter_alerts_by_status(alerts: list[dict], status: AlertStatusFilter | None) -> list[dict]:
    if not status or status == "all":
        return alerts
    if status == "muted":
        return [a for a in alerts if not a.get("enabled", True)]
    if status == "alerting":
        return [a for a in alerts if a.get("enabled", True) and bool(a.get("last_triggered_at"))]
    if status == "ok":
        return [a for a in alerts if a.get("enabled", True) and not a.get("last_triggered_at")]
    return alerts


def _alert_list(alerts):
    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


def _status_cache_has_data(src: dict) -> bool:
    """True when the per-service status cache reports ingested log data."""
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
    except Exception:
        return False
    if not cached:
        return False
    return bool(cached.get("local_rows", 0)) or cached.get("latest_log_at") is not None


@alerts_router.get("/", response_model=AlertListResponse)
@alerts_router.get("", response_model=AlertListResponse, include_in_schema=False)
def list_all_alerts(
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max alerts to return; oldest dropped."),
    status: AlertStatusFilter | None = Query(
        default=None,
        description="Optional filter by alert state: all, alerting, ok, or muted.",
    ),
):
    """Return alerts visible to the caller.

    Admin: every alert across every service. Analyst: only alerts for
    services in their invite's scope (security).

    Hard ``limit`` cap (default 500, max 2000) prevents the JSON
    payload from growing unbounded as a tenant accumulates alerts —
    the prior unbounded response was a slow-growth resource-exhaustion
    risk on the page-load path.
    """
    allowed = _analyst_allowed_services(request)
    alerts = repo.get_alerts(limit=limit)
    if allowed is not None:
        alerts = [a for a in alerts if a.get("service_id") in allowed]
    alerts = _filter_alerts_by_status(alerts, status)
    return _alert_list(alerts)


@alerts_router.get("/{service_id}", response_model=AlertListResponse)
def list_service_alerts(
    service_id: str,
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max alerts to return."),
    status: AlertStatusFilter | None = Query(
        default=None,
        description="Optional filter by alert state: all, alerting, ok, or muted.",
    ),
):
    """Return alerts for one service. Analyst gets 403 if the service
    isn't in their invite (security)."""
    require_service_in_scope(request, service_id)
    alerts = repo.get_alerts(service_id)[:limit]
    alerts = _filter_alerts_by_status(alerts, status)
    return _alert_list(alerts)


@alerts_router.post("/", response_model=AlertResponse, status_code=201)
@alerts_router.post("", response_model=AlertResponse, status_code=201, include_in_schema=False)
def create_alert(alert: Alert, request: Request):
    """Create an alert. Analyst can only create alerts for services in
    their invite scope (security). The Phase-1 analyst middleware
    also blocks POSTs on /api/alerts for analysts entirely (not in the
    _ANALYST_ALLOWED_WRITE_PREFIXES list), so this is defense-in-depth
    for the admin-impersonating-analyst case.

    Returns 201 Created — resource POSTs convention; the response body
    still carries the new alert so callers don't need a second GET to
    pick up the server-assigned id."""
    require_service_in_scope(request, alert.service_id)
    _require_admin_mutation(request)
    _validate_alert_urls(alert)
    res = repo.save_alert(alert)
    sync_admin_state(alert.service_id)
    return AlertResponse.with_telemetry(data=res)


@alerts_router.post("/evaluate", response_model=AlertEvaluateResponse)
def evaluate_alerts_endpoint(
    request: Request,
    body: AlertEvaluateRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    """Evaluate enabled alert rules on demand for the active service (Admin only)."""
    target_service_id = (body.service_id if body and body.service_id else None) or service_id
    target_service_id = require_service_id(target_service_id)

    require_service_in_scope(request, target_service_id)
    _require_admin_mutation(request)

    from backend.core.duckdb import get_source_for_service

    alerts = repo.get_alerts(target_service_id)
    if body and body.alert_id:
        alerts = [a for a in alerts if a.get("id") == body.alert_id]
    else:
        alerts = [a for a in alerts if a.get("enabled", True)]

    src = get_source_for_service(target_service_id)
    results: list[AlertEvaluateResultItem] = []
    triggered_count = 0

    for a in alerts:
        triggered = False
        triggered_ts: str | None = a.get("last_triggered_at")
        if src:
            trig, _wh_url, _payload, max_ts_str = repo.evaluate_alert(
                con,
                src,
                dict(a),
                service_id=target_service_id,
            )
            if trig:
                triggered = True
                triggered_count += 1
                triggered_ts = max_ts_str or datetime.now(UTC).isoformat()
                if a.get("id"):
                    repo.update_last_triggered(target_service_id, a["id"], triggered_ts)
        results.append(
            AlertEvaluateResultItem(
                id=str(a.get("id") or ""),
                name=str(a.get("name") or ""),
                metric=str(a.get("metric") or ""),
                triggered=triggered,
                last_triggered_at=triggered_ts,
            )
        )

    return AlertEvaluateResponse.with_telemetry(
        data=AlertEvaluateData(
            service_id=target_service_id,
            evaluated=len(results),
            triggered=triggered_count,
            results=results,
            evaluated_at=datetime.now(UTC).isoformat(),
        )
    )


@alerts_router.post("/preview", response_model=AlertPreviewResponse)
def preview_alert(
    alert: Alert,
    request: Request,
    lookback_hours: int = Query(default=24, ge=1),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    from backend.core.duckdb import _safe_table_name, get_source_for_service

    require_service_in_scope(request, alert.service_id)

    src = get_source_for_service(alert.service_id)
    if not src:
        raise HTTPException(status_code=404, detail=not_found("Service not found"))

    table_name = _safe_table_name(src["name"])

    try:
        res = repo.preview_alert_series(
            con, src, table_name, alert, lookback_hours, has_status_data=lambda: _status_cache_has_data(src)
        )
        return AlertPreviewResponse.with_telemetry(data=res)
    except Exception as e:
        logger.error(f"Preview failed: {e}")
        return AlertPreviewResponse.with_telemetry(error=redact_error_text(e)[:500])


@alerts_router.put("/{alert_id}", response_model=AlertResponse)
def update_alert(
    alert_id: str,
    alert: Alert,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    sid = alert.service_id or service_id
    sid = require_service_id(sid)
    require_service_in_scope(request, sid)
    _require_admin_mutation(request)
    _validate_alert_urls(alert)
    updated_alert = alert.model_copy(update={"id": alert_id, "service_id": sid})
    res = repo.save_alert(updated_alert)
    res.setdefault("service_id", sid)
    sync_admin_state(sid)
    return AlertResponse.with_telemetry(data=res)


@alerts_router.patch("/{alert_id}/enabled", response_model=AlertResponse)
def toggle_alert_enabled(
    alert_id: str,
    body: _ToggleBody,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    service_id = require_service_id(service_id)
    require_owned_row(request, service_id, alert_id, repo.get_alert_by_id)
    _require_admin_mutation(request)
    res = repo.toggle_alert(alert_id, body.enabled, service_id)
    res.setdefault("service_id", service_id)
    sync_admin_state(res.get("service_id"))
    return AlertResponse.with_telemetry(data=res)


@alerts_router.delete("/{alert_id}", status_code=204)
def delete_alert(
    alert_id: str,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    service_id = require_service_id(service_id)
    require_owned_row(request, service_id, alert_id, repo.get_alert_by_id)
    _require_admin_mutation(request)
    res = repo.delete_alert(alert_id, service_id)
    sync_admin_state(res.get("service_id") or service_id)
    return Response(status_code=204)


def _run_test_alert_webhook(
    alert_id: str,
    sid: str,
    body: AlertTestWebhookRequest | None,
) -> AlertTestWebhookResponse:
    from backend.cron.jobs.metadata import (
        _post_alert_webhook,
        _post_pagerduty_notification,
        _post_slack_notification,
    )

    existing = repo.get_alert_by_id(alert_id, sid)
    if not existing and not (body and body.webhook_url):
        raise HTTPException(status_code=404, detail=not_found("alert_not_found"))

    targets: list[tuple[str, str]] = []
    if body and body.webhook_url is not None:
        targets.append((body.channel_type, validate_webhook_url(body.webhook_url)))
    elif existing:
        if existing.get("webhook_url"):
            targets.append(("webhook", validate_webhook_url(str(existing["webhook_url"]))))
        for ch in existing.get("channels") or []:
            ch_url = ch.get("url") if isinstance(ch, dict) else getattr(ch, "url", None)
            ch_type = (ch.get("type") if isinstance(ch, dict) else getattr(ch, "type", "webhook")) or "webhook"
            if ch_url:
                targets.append((str(ch_type), validate_webhook_url(str(ch_url))))

    if not targets:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "no_webhook_configured",
                "message": "No webhook or notification channel URL configured for this alert.",
            },
        )

    if body and body.dry_run:
        return AlertTestWebhookResponse.with_telemetry(
            data=AlertTestWebhookData(
                alert_id=alert_id,
                service_id=sid,
                status="validated",
                targets_tested=len(targets),
                channel_type=targets[0][0],
                detail="Webhook URL validation succeeded.",
            )
        )

    alert_name = str(existing.get("name", alert_id)) if existing else alert_id
    metric = str(existing.get("metric", "requests")) if existing else "requests"
    operator = str(existing.get("operator", ">")) if existing else ">"
    threshold = float(existing.get("threshold") or 0.0) if existing else 0.0

    for ch_type, target_url in targets:
        if ch_type == "slack":
            _post_slack_notification(
                target_url,
                {
                    "text": f"🔔 Test alert notification: *{alert_name}* on service `{sid}` ({metric} {operator} {threshold})"
                },
            )
        elif ch_type == "pagerduty":
            _post_pagerduty_notification(
                target_url,
                alert_name=alert_name,
                metric=metric,
                msg_val=f"{operator} {threshold} (test)",
                service_name=sid,
            )
        else:
            _post_alert_webhook(
                target_url,
                {
                    "event": "test_alert",
                    "alert_id": alert_id,
                    "alert_name": alert_name,
                    "service_id": sid,
                    "metric": metric,
                    "operator": operator,
                    "threshold": threshold,
                    "triggered_at": datetime.now(UTC).isoformat(),
                },
            )

    return AlertTestWebhookResponse.with_telemetry(
        data=AlertTestWebhookData(
            alert_id=alert_id,
            service_id=sid,
            status="sent",
            targets_tested=len(targets),
            channel_type=targets[0][0],
            detail=f"Dispatched test notification to {len(targets)} target(s).",
        )
    )


@alerts_router.post("/{alert_id}/test", response_model=AlertTestWebhookResponse)
def test_alert_webhook(
    alert_id: str,
    request: Request,
    body: AlertTestWebhookRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
):
    sid = (body.service_id if body and body.service_id else None) or service_id
    sid = require_service_id(sid)
    require_service_in_scope(request, sid)
    _require_admin_mutation(request)
    return _run_test_alert_webhook(alert_id, sid, body)


@alerts_router.post("/{service_id}/{alert_id}/test", response_model=AlertTestWebhookResponse)
def test_alert_webhook_by_service(
    service_id: str,
    alert_id: str,
    request: Request,
    body: AlertTestWebhookRequest | None = Body(default=None),
):
    require_service_in_scope(request, service_id)
    _require_admin_mutation(request)
    return _run_test_alert_webhook(alert_id, service_id, body)


views_router = APIRouter(prefix="/api/views", tags=["views"], responses=DEFAULT_ERROR_RESPONSES)


@views_router.get(
    "/{service_id}",
    response_model=list[SavedViewRecord],
    response_model_exclude_unset=True,
)
def list_views(
    service_id: str,
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max saved views to return."),
):
    """Security: analyst can only list views for services in their
    scope. Without this gate an analyst scoped to ``svc-A`` could enumerate
    saved views for ``svc-B`` by typing /api/views/svc-B in their browser.

    Hard ``limit`` cap (default 500, max 2000) caps the payload as a
    tenant accumulates saved views.
    """
    require_service_in_scope(request, service_id)
    return repo.get_views(service_id)[:limit]


@views_router.post("/", status_code=201, response_model=ViewSaveResponse)
def create_view(view: SavedView, request: Request):
    """Security: analyst can only create views for services in
    their scope. Middleware already blocks POST on /api/views for
    analysts; this is defense-in-depth.

    Returns 201 Created — resource POST convention. Body still carries
    the saved view so callers don't need a follow-up GET for the id."""
    require_service_in_scope(request, view.service_id)
    res = repo.save_view(view)
    sync_admin_state(view.service_id)
    return res


@views_router.delete("/{view_id}", status_code=204)
def delete_view(view_id: str, request: Request, service_id: str | None = Depends(get_service_id)):
    service_id = require_service_id(service_id)
    require_owned_row(request, service_id, view_id, repo.get_view_by_id)
    res = repo.delete_view(view_id, service_id)
    sync_admin_state(res.get("service_id") or service_id)
    return Response(status_code=204)


# ── Usage endpoints (folded from backend.routers.usage) ───────────────────────

import logging
import urllib.parse

from fastapi import APIRouter, Depends, Query

logger = logging.getLogger(__name__)

from backend.config import get_fastly_api_key
from backend.core.fastly.client import api_key_or_mock
from backend.core.fastly.utils import FASTLY_LOG_FIELDS
from backend.deps import get_source
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin import (
    CurrentStorageResponse,
    PrefillRatesResponse,
    PrefillResponse,
    RumBreakdownPoint,
    UsageBandwidthResponse,
    UsageLogActivityResponse,
    UsageOperationsResponse,
    UsageRumBreakdownResponse,
)
from backend.repositories._base import SectionTimer
from backend.utils.date_utils import window_to_epoch
from backend.utils.router_utils import query_errors, raise_internal

usage_router = APIRouter(prefix="/api/usage", tags=["usage"], responses=DEFAULT_ERROR_RESPONSES)
# Short-TTL cache over the /stats/* Fastly hops. /usage cold-loads fan
# 3 endpoints (operations, bandwidth, log-activity) that hit the same
# `(service_id, by, from, to)` tuples with `from`/`to` rounded to the
# auto-set 24 h window. Without caching, tab-switching, second-tab
# admin sessions, and the playwright fan-out each pay ~210-330 ms per
# Fastly hop. Stats API records are debounced to hourly buckets, so a
# 60 s staleness window is invisible to the operator and absorbs every
# burst without changing chart numbers.
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

_FASTLY_STATS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
_STORAGE_INSPECTION_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=64, ttl_seconds=60.0)
_CacheRegistry.register("routers.usage._FASTLY_STATS_CACHE", _FASTLY_STATS_CACHE)
_CacheRegistry.register("routers.usage._STORAGE_INSPECTION_CACHE", _STORAGE_INSPECTION_CACHE)


def _fastly_api(path: str, api_key: str) -> dict:
    """Thin wrapper delegating to the central Fastly client. Kept as a
    function so the existing 3 call sites in this module read the same;
    the central wrapper adds retry-on-429/5xx + telemetry tracking that
    used to live in each caller. GET responses are TTL-cached on
    ``path`` (api_key never changes within a process — it's read from
    the same module-level config — so it is NOT part of the cache key)."""
    from backend.core.fastly.client import fastly

    cached = _FASTLY_STATS_CACHE.get(path)
    if cached is not None:
        return cached
    response = fastly("GET", path, token=api_key)
    _FASTLY_STATS_CACHE[path] = response
    return response


def _extract_fos_ops(record: dict) -> tuple[int, int]:
    """Read Class A / Class B counts from a Fastly /stats/aggregate record.

    The aggregate endpoint returns object_storage metrics flattened with an
    `object_storage_` prefix (object_storage_class_a_operations_count, etc.),
    not nested under an `object_storage` dict. We accept either shape.
    """

    from backend.core.fastly.client import StatsRecord

    return StatsRecord.model_validate(record).fos_ops()


def _compute_prefill_rates(source: dict) -> tuple[dict, dict]:
    """Local-config-only prefill payload (FAST path — no Fastly, no DuckDB).

    Returned shape:
      (result, prov)
    where ``result`` is the dict-shaped subset of PrefillResponse fields the
    cost-page stat cards need on first paint, and ``prov`` is the
    provisioning sub-dict the SLOW path reuses (saves a second
    ``load_service_config`` round-trip when /prefill calls this helper).
    """
    from backend import config as svcconfig

    global_rates = svcconfig.load_usage_logging_config()
    result: dict = {
        "avg_log_file_size_kb": None,
        "estimated_bytes_per_line": None,
        "data_days": 0,
        "log_period_seconds": None,
        "commit_interval_mins": 5,
        "sample_rate": 100,
        "edge_only": False,
        "compaction_enabled": True,
        "delete_after": True,
        "log_retention_days": 90,
        "class_a_rate_per_1k": float(global_rates.get("class_a_rate_per_1k", 0.005)),
        "class_b_rate_per_10k": float(global_rates.get("class_b_rate_per_10k", 0.01)),
        "cdn_egress_rate_per_gb": float(global_rates.get("cdn_egress_rate_per_gb", 0.12)),
        "storage_rate_per_gb_month": float(global_rates.get("storage_rate_per_gb_month", 0.02)),
        "min_billed_days": int(global_rates.get("min_billed_days", 30)),
        "billing_zone": str(global_rates.get("billing_zone", "MBZ200")),
        "rum_beacons_per_day": 0,
    }

    prov: dict = {}
    try:
        cfg = svcconfig.load_config(source["name"])
        if cfg:
            prov = cfg.get("provisioning", {})
            cron_sync = prov.get("cron_sync", {})
            result["sample_rate"] = int(prov.get("sample_rate", 100))
            result["edge_only"] = prov.get("edge_only", True)
            result["compaction_enabled"] = prov.get("cron_compact", {}).get("enabled", True)
            result["delete_after"] = bool(cron_sync.get("delete_after", True))
            result["commit_interval_mins"] = int(cron_sync.get("commit_interval_mins", 5))
            result["log_retention_days"] = int(cfg.get("log_retention_days", 90))
            try:
                from backend.core.log_fields import estimate_log_line_bytes

                lf_cfg = cfg.get("log_fields") or prov.get("log_fields", {})
                if not lf_cfg:
                    lf_cfg = {"groups": ["A", "B", "C", "D"], "field_overrides": {}}
                result["estimated_bytes_per_line"] = estimate_log_line_bytes(lf_cfg)
            except Exception:
                logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
            try:
                from backend.provision import parse_period

                period_val = cfg.get("log_period", prov.get("log_period", 60))
                period_secs = period_val if isinstance(period_val, int) else parse_period(str(period_val))
                result["log_period_seconds"] = period_secs
            except Exception:
                logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    try:
        cached_status = svcconfig.get_status(source["name"])
        if cached_status:
            avg_kb = cached_status.get("avg_log_size_kb")
            if avg_kb:
                result["avg_log_file_size_kb"] = round(float(avg_kb), 2)
            earliest = cached_status.get("earliest_log_at")
            latest = cached_status.get("latest_log_at")
            if earliest and latest:
                try:
                    from backend.utils.date_utils import parse_iso_utc

                    _now = datetime.now(UTC)
                    e_dt = parse_iso_utc(earliest) or _now
                    l_dt = parse_iso_utc(latest) or _now
                    result["data_days"] = max(1, (l_dt - e_dt).days + 1)
                except Exception:
                    logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    # Empirical node count analysis — SQLite-only, doesn't need a DuckDB hop.
    try:
        from backend.core import metadata as metadata_db

        avg = metadata_db.get_node_count_avg(source["name"])
        if avg:
            result["avg_nodes_per_flush"] = round(float(avg))
    except Exception:
        logger.debug("_compute_prefill_rates: step failed (ignored)", exc_info=True)

    # Extract historical RUM beacons per day for prefill.
    try:
        val = metadata_db.get_rum_beacons_avg_per_day(source["name"])
        result["rum_beacons_per_day"] = round(val) if val is not None else 0
    except Exception as e:
        logger.debug("Failed to calculate rum_beacons_per_day: %s", e)

    return result, prov


@usage_router.get("/prefill/rates", response_model=PrefillRatesResponse)
@query_errors()
def prefill_rates(source: dict = Depends(get_source)):
    """Instant prefill subset for stat cards — local config + cached
    status + node-count SQLite read only. No Fastly API calls, no
    DuckDB hop. Frontend pairs this with the lazy /prefill call inside
    the Cost Estimator viewport so the cards paint while the estimator
    is still scrolling into view."""
    result, _prov = _compute_prefill_rates(source)
    return PrefillRatesResponse.with_telemetry(**result)


@usage_router.get("/prefill", response_model=PrefillResponse)
@query_errors()
async def prefill(source: dict = Depends(get_source)):
    import asyncio
    import time as _time

    from backend import config as svcconfig
    from backend.config import get_fastly_logging_service_id

    # Per-phase timings — /api/usage/prefill clocks ~1.4 s p50 per perf
    # audit; section_timings shows whether the cost is in the Fastly
    # endpoint chain, the /stats fetch, or the DuckDB edge-ratio hop.
    timer = SectionTimer()
    section_timings = timer.entries

    fast_result, prov = await asyncio.to_thread(_compute_prefill_rates, source)
    result: dict = {
        "requests_per_day": None,
        "edge_requests_per_day": None,
        "edge_ratio": None,
        **fast_result,
    }

    # Pull real traffic numbers and live logging config from Fastly Stats API.
    api_key = api_key_or_mock(get_fastly_api_key(source["service_id"]))
    if api_key:
        from backend.core.fastly.client import fastly, find_condition, get_active_version

        # CDN service for bandwidth stats; logging service for S3 endpoint config lookup
        stats_svc_id = (source.get("cdn_service_id") or get_fastly_logging_service_id() or "").strip()
        logging_svc_id = (source.get("logging_service_id") or get_fastly_logging_service_id() or "").strip()
        svc_id = stats_svc_id
        now = datetime.now(UTC)
        from_ts = int((now - timedelta(days=3)).timestamp())
        to_ts = int(now.timestamp())
        by = "day"

        # M4 parallelisation: the version → endpoint → condition chain
        # (250 ms typical, fully serial because each step needs the
        # previous response) is independent of the /stats call (150 ms
        # typical) — neither uses the other's result. Run both as
        # asyncio tasks so the prefill wall-clock is bound by the slower
        # of the two instead of their sum. Each sync ``fastly()`` call
        # runs inside ``asyncio.to_thread`` so the existing retry, auth,
        # and telemetry machinery in ``backend/core/fastly/client.py``
        # is reused unchanged.

        async def _resolve_endpoint_chain() -> dict:
            """Returns {log_period_seconds?, sample_rate?, edge_only?}."""
            updates: dict = {}
            try:
                if not logging_svc_id:
                    return updates
                active_ver = await asyncio.to_thread(get_active_version, logging_svc_id, api_key)
                if not active_ver:
                    return updates
                endpoint_name = prov.get("endpoint_name", "Fastly Object Storage Logs")
                encoded_name = urllib.parse.quote(endpoint_name, safe="")
                current_ep = await asyncio.to_thread(
                    fastly,
                    "GET",
                    f"/service/{logging_svc_id}/version/{active_ver}/logging/s3/{encoded_name}",
                    token=api_key,
                )
                if "period" in current_ep:
                    updates["log_period_seconds"] = int(current_ep["period"])
                cond_name = current_ep.get("response_condition")
                if cond_name == "Log Sampling":
                    import re

                    cond = await asyncio.to_thread(find_condition, cond_name, logging_svc_id, active_ver, api_key)
                    if cond:
                        stmt = cond.get("statement", "")
                        m = re.search(r"randombool\((\d+),", stmt)
                        if m:
                            updates["sample_rate"] = int(m.group(1))
                        if "req.restarts == 0" in stmt:
                            updates["edge_only"] = True
            except Exception:
                logger.debug("_resolve_endpoint_chain: step failed (ignored)", exc_info=True)
            return updates

        async def _fetch_stats() -> dict | None:
            try:
                if svc_id:
                    return await asyncio.to_thread(
                        _fastly_api, f"/stats/service/{svc_id}?by={by}&from={from_ts}&to={to_ts}", api_key
                    )
                return await asyncio.to_thread(
                    _fastly_api, f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key
                )
            except Exception:
                return None

        try:
            _t = _time.perf_counter()
            chain_updates, payload = await asyncio.gather(_resolve_endpoint_chain(), _fetch_stats())
            timer.mark("fastly_chain_and_stats", _t)
            # Chain updates feed into the response shape's existing keys
            # — overrides any defaults set above and any cron_sync values
            # set from the local config, matching the prior precedence
            # (Fastly-resolved values win over local config).
            result.update(chain_updates)

            daily_reqs: dict[str, int] = {}
            daily_edge: dict[str, int] = {}
            if payload:
                for rec in payload.get("data", []):
                    ts = rec.get("start_time")
                    if ts is None:
                        continue
                    day = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")
                    daily_reqs[day] = daily_reqs.get(day, 0) + int(rec.get("requests") or 0)
                    daily_edge[day] = daily_edge.get(day, 0) + int(rec.get("edge_requests") or 0)
            today = now.strftime("%Y-%m-%d")
            complete_days = [d for d, v in daily_reqs.items() if v > 0 and d != today]
            avg_days = complete_days if complete_days else [d for d, v in daily_reqs.items() if v > 0]
            if avg_days:
                result["requests_per_day"] = round(sum(daily_reqs[d] for d in avg_days) / len(avg_days))
                result["edge_requests_per_day"] = round(sum(daily_edge[d] for d in avg_days) / len(avg_days))
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)

    # Skip the DuckDB hop entirely when the cached config status already
    # carries edge_ratio (steady state — the sync cron keeps it fresh).
    # Saves a connection-open + view-resolve on the hot path. Falls back
    # to a live get_con read only on the cold path.
    debug_queries: list = []
    cached_status = svcconfig.get_status(source["name"]) or {}
    cached_edge_ratio = cached_status.get("edge_ratio")
    if cached_edge_ratio is not None:
        result["edge_ratio"] = cached_edge_ratio
    else:
        try:
            from backend.core.duckdb import get_connection

            # read_only: get_edge_ratio is a SELECT against the view.
            # Wrapped in asyncio.to_thread so this sync I/O doesn't block
            # the event loop now that prefill is an async handler.
            def _edge_ratio_blocking() -> tuple:
                # EDGE_RATIO is a coarse count-filter; a few seconds of
                # view-resolution staleness is fine here, so skip the
                # rebind step on this code path (saves ~80-150 ms/call).
                con = get_connection(source=source, max_wait=5, read_only=True, skip_view_update=True)
                try:
                    return repo.get_edge_ratio(con, source)
                finally:
                    con.close()

            _t = _time.perf_counter()
            edge_ratio, debug_queries = await asyncio.to_thread(_edge_ratio_blocking)
            timer.mark("edge_ratio_query", _t)
            if edge_ratio is not None:
                result["edge_ratio"] = edge_ratio
        except Exception:
            logger.debug("prefill: step failed (ignored)", exc_info=True)

    # ``avg_nodes_per_flush`` already populated by _compute_prefill_rates above.
    return PrefillResponse.with_telemetry(debug_queries=debug_queries, section_timings=section_timings, **result)


@usage_router.get("/current-storage", response_model=CurrentStorageResponse)
@query_errors()
def usage_current_storage(
    start: str = Query(default=""),
    end: str = Query(default=""),
    source: dict = Depends(get_source),
):
    from backend.core.duckdb import _get_fos_client

    src = source
    if not src.get("bucket"):
        raise HTTPException(status_code=400, detail={"error": "Fastly Object Storage bucket not configured."})

    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import parse_date_window

    now = datetime.now(UTC)
    start_str, end_str = parse_date_window(start, end)

    try:
        from backend import config as svcconfig

        # Config lookup is keyed on service_id; fall back to {} when the
        # config file is missing (e.g. mid-teardown, or when name != service_id
        # for any reason). Defensive: a None cfg here would AttributeError
        # on the subsequent .get() calls and 500 the cost panel.
        cfg = svcconfig.load_config(src["name"]) or {}
        cron_sync = cfg.get("provisioning", {}).get("cron_sync", {})
        delete_after = cron_sync.get("delete_after", True)
        retention_days = int(cron_sync.get("log_retention_days", 30))
        retention_end_str = now.strftime("%Y-%m-%d %H:%M:%S")
        # When delete_after=True, raw files are deleted quickly but billed for Fastly's
        # 30-day minimum. Query exactly that window to capture all currently-billed files.
        MIN_BILLING_DAYS = 30
        query_days = MIN_BILLING_DAYS if delete_after else retention_days
        retention_start_str = (now - timedelta(days=query_days)).strftime("%Y-%m-%d %H:%M:%S")

        # get_storage_stats reads from metadata_db (file list + sizes),
        # not the DuckDB/DuckLake view — no DuckDB connection checkout needed.
        stats = repo.get_storage_stats(None, src, retention_start_str, retention_end_str)

        total_files = stats["total_files"]
        total_bytes = stats["total_bytes"]
        debug_queries = stats.get("debug_queries", [])

        # Prefer the cached metadata path: get_table_info reads manifest
        # summaries (with multi-layer local caching) instead of doing a
        # paginated FOS LIST on iceberg/. The undercount vs LIST is just
        # metadata-file bytes (manifests/snapshots, typically <1% of total),
        # and trading that for ~5s of cloud LIST per panel open is correct
        # per the standing "be a little behind the data rather than read
        # extra from the cloud" rule. The FOS-LIST fallback below still
        # covers the cold-start case where get_table_info errors out.
        cache_key = (src.get("name"), src.get("bucket"), src.get("prefix"))
        cached_insp = _STORAGE_INSPECTION_CACHE.get(cache_key)
        if cached_insp is not None:
            iceberg_bytes, iceberg_files, quarantine_bytes, rum_bytes = cached_insp
        else:
            iceberg_bytes = 0
            iceberg_files = 0
            iceberg_info_success = False
            # First try the cron-written cached values in svcconfig.status —
            # the heavy refresh_config_status tick already populated these,
            # so this endpoint stays sub-50 ms even on a cold-cache process.
            cached_status = svcconfig.get_status(src["name"]) or {}
            cached_ib = cached_status.get("iceberg_bytes")
            cached_if = cached_status.get("iceberg_files")
            if cached_ib is not None and cached_if is not None:
                iceberg_bytes = int(cached_ib)
                iceberg_files = int(cached_if)
                iceberg_info_success = True
            else:
                try:
                    from backend.core import storage as db_storage

                    iceberg_info = db_storage.get_table_info(src)
                    if not iceberg_info.get("error"):
                        iceberg_bytes = iceberg_info.get("size_bytes", 0)
                        iceberg_files = iceberg_info.get("data_files", 0)
                        iceberg_info_success = True
                except Exception:
                    logger.debug("usage_current_storage: step failed (ignored)", exc_info=True)

            if not iceberg_info_success:
                try:
                    s3 = _get_fos_client(src)
                    bucket = src["bucket"]
                    prefix = f"{src.get('prefix', '').strip().rstrip('/')}/" if src.get("prefix") else ""
                    iceberg_prefix = f"{prefix}iceberg/"
                    paginator = s3.get_paginator("list_objects_v2")
                    for page in paginator.paginate(Bucket=bucket, Prefix=iceberg_prefix.rstrip("/")):
                        for obj in page.get("Contents", []):
                            iceberg_bytes += obj.get("Size", 0)
                            iceberg_files += 1
                except Exception as e:
                    import logging

                    logging.error(f"FOS Scan failed: {e}")

            quarantine_bytes = 0
            try:
                from backend.core.metadata.ingest_log import get_quarantine_storage_total

                quarantine_bytes = get_quarantine_storage_total(src["name"])
            except Exception:
                logger.debug("usage_current_storage: get_quarantine_storage_total failed (ignored)", exc_info=True)

            # Calculate RUM storage separately from regular logs
            rum_bytes = 0
            try:
                s3 = _get_fos_client(src)
                bucket = src["bucket"]
                prefix = src.get("prefix", "").strip("/")
                rum_prefix = f"{prefix}/raw/rum/" if prefix else "raw/rum/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=rum_prefix):
                    for obj in page.get("Contents", []):
                        rum_bytes += obj.get("Size", 0)
            except Exception:
                logger.debug("usage_current_storage: step failed (ignored)", exc_info=True)

            _STORAGE_INSPECTION_CACHE[cache_key] = (
                iceberg_bytes,
                iceberg_files,
                quarantine_bytes,
                rum_bytes,
            )

        regular_log_bytes = total_bytes - rum_bytes

        if delete_after:
            live_bytes = iceberg_bytes + quarantine_bytes
            live_files = iceberg_files
            deleted_bytes = total_bytes
            deleted_files = total_files
        else:
            live_bytes = total_bytes + iceberg_bytes + quarantine_bytes
            live_files = total_files + iceberg_files
            deleted_bytes = 0
            deleted_files = 0

        billed_bytes = total_bytes + iceberg_bytes + quarantine_bytes
        # GB-hours using the 720-hour (30-day) basis so the frontend can divide by 720
        # to get GB-months cost. This correctly applies the 30-day minimum billing
        # floor: files deleted in < 30 days are still billed for their full minimum period.
        billed_gb = billed_bytes / (1024 * 1024 * 1024)
        gb_hours = billed_gb * (MIN_BILLING_DAYS * 24)

        return CurrentStorageResponse.with_telemetry(
            debug_queries=debug_queries,
            live_bytes=live_bytes,
            live_files=live_files,
            deleted_bytes=deleted_bytes,
            quarantine_bytes=quarantine_bytes,
            total_billed_bytes=billed_bytes,
            total_billed_gb_hours=gb_hours,
            total_files=total_files,
            total_bytes=total_bytes,
            rum_bytes=rum_bytes,
            regular_log_bytes=regular_log_bytes,
            start=start_str,
            end=end_str,
        )
    except Exception as e:
        raise_internal(logger, e, code="usage_storage_failed")


@usage_router.get("/operations", response_model=UsageOperationsResponse)
@query_errors()
def usage_operations(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict = Depends(get_source),
):
    # /stats/aggregate is account-wide and very slow at sub-hour granularity;
    # clamp to hour minimum. Track whether we clamped so the frontend can
    # surface the discrepancy ("you asked for 1m but the chart is 1h").
    requested_by = by
    if by not in ("hour", "day"):
        by = "hour"
    clamped_from_sub_hour = requested_by != by

    src = source
    api_key = api_key_or_mock(get_fastly_api_key(src["logging_service_id"]))
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    from datetime import UTC, datetime

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    agg: dict[str, dict] = {}
    fos_fields_found: set[str] = set()

    def _accumulate(records: list) -> None:
        for record in records:
            ts = record.get("start_time")
            if ts is None:
                continue
            for k in record:
                if "object_storage" in k:
                    fos_fields_found.add(k)
            sub = record.get("object_storage", {})
            if isinstance(sub, dict):
                for k in sub:
                    if "operations" in k or "class" in k:
                        fos_fields_found.add(f"object_storage.{k}")
            if by == "minute":
                fmt = "%Y-%m-%dT%H:%M"
            elif by == "hour":
                fmt = "%Y-%m-%dT%H:00"
            else:
                fmt = "%Y-%m-%d"
            date_str = datetime.fromtimestamp(ts, tz=UTC).strftime(fmt)
            class_a, class_b = _extract_fos_ops(record)
            if date_str not in agg:
                agg[date_str] = {"class_a": 0, "class_b": 0}
            agg[date_str]["class_a"] += class_a
            agg[date_str]["class_b"] += class_b

    try:
        # fastly() does telemetry tracking internally; the prior
        # explicit tracked_call wrapper was duplicating the entry.
        payload = _fastly_api(f"/stats/aggregate?by={by}&from={from_ts}&to={to_ts}", api_key)
        _accumulate(payload.get("data", []))
    except Exception as e:
        # fastly() raises RuntimeError("HTTP 502 GET /stats/aggregate ...")
        # on non-2xx, with the upstream body included. The body can contain
        # internal hostnames or token fragments, so log server-side and
        # return a generic code keyed by error_id for correlation.
        raise_internal(logger, e, code="fastly_stats_aggregate_failed", status=502)

    points = [{"date": d, **v} for d, v in sorted(agg.items())]
    total_a = sum(d["class_a"] for d in points)
    total_b = sum(d["class_b"] for d in points)
    note = (
        "Note: Object Storage operations are reported across your entire Fastly account "
        "and cannot be filtered by service. Estimates may be inflated if other services "
        "use Object Storage."
    )
    if clamped_from_sub_hour:
        note += (
            f" The chart interval you selected ({requested_by}) is finer than the Fastly "
            "Historical Stats API supports for /stats/aggregate — granularity is forced to "
            "hourly minimum (the API is account-wide and prohibitively slow at sub-hour)."
        )
    return UsageOperationsResponse.with_telemetry(
        data=points,
        total_class_a=total_a,
        total_class_b=total_b,
        granularity=by,
        note=note,
        fos_fields_found=sorted(fos_fields_found),
    )


@usage_router.get("/bandwidth", response_model=UsageBandwidthResponse)
@query_errors()
def usage_bandwidth(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
    source: dict = Depends(get_source),
):

    src = source
    api_key = api_key_or_mock(get_fastly_api_key(src["logging_service_id"]))
    if not api_key:
        raise HTTPException(status_code=403, detail={"error": "Fastly API key is not configured."})

    if by not in ("hour", "minute", "day"):
        by = "hour"

    from datetime import UTC, datetime

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    cdn_svc = src.get("cdn_service_id", "").strip()
    agg: dict[int, dict] = {}

    def _merge(payload):
        for record in payload.get("data", []):
            ts = record.get("start_time")
            if ts is None:
                continue
            if ts not in agg:
                agg[ts] = {"bandwidth_bytes": 0, "requests": 0}
            agg[ts]["bandwidth_bytes"] += int(record.get("bandwidth") or 0)
            agg[ts]["requests"] += int(record.get("requests") or 0)

    if cdn_svc:
        try:
            # tracked_call wrapper removed — _fastly_api → fastly() already
            # does telemetry internally; the double-wrap was producing
            # duplicate entries in /api/admin/usage-logging and inflating
            # the visible call count to 2x for this endpoint.
            payload = _fastly_api(f"/stats/service/{cdn_svc}?by={by}&from={from_ts}&to={to_ts}", api_key)
            _merge(payload)
        except Exception as e:
            raise_internal(logger, e, code="fastly_stats_service_failed", status=502)

    fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
    points = [{"time": datetime.fromtimestamp(ts, tz=UTC).strftime(fmt), **v} for ts, v in sorted(agg.items())]
    return UsageBandwidthResponse.with_telemetry(
        data=points,
        total_bytes=sum(p["bandwidth_bytes"] for p in points),
        total_log_bytes=0,
        granularity=by,
    )


@usage_router.get("/log-activity", response_model=UsageLogActivityResponse)
@query_errors()
def usage_log_activity(
    source: dict = Depends(get_source),
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="hour"),
):
    if by not in ("second", "minute", "hour", "day"):
        by = "hour"

    from datetime import UTC, datetime

    from backend.utils.date_utils import parse_date_window

    now = datetime.now(UTC)
    start_str, end_str = parse_date_window(start, end)

    # Dropped the Depends(get_con) — repo reads metadata SQLite only, never
    # touched the DuckDB connection. Saves one get_connection() lookup
    # + update_storage_view rebind per call.
    res = repo.get_log_activity(source, start_str, end_str, by)

    # Fetch Fastly API stats for the logging service to compare generated vs processed
    api_key = api_key_or_mock(get_fastly_api_key(source.get("logging_service_id", "")))
    logging_svc = source.get("logging_service_id", "").strip()

    if api_key and logging_svc and by in ("minute", "hour", "day"):
        from backend.utils.date_utils import parse_window_str_to_dt

        start_dt = parse_window_str_to_dt(start_str)
        end_dt = parse_window_str_to_dt(end_str)
        from_ts = int(start_dt.timestamp())
        to_ts = int(end_dt.timestamp())

        try:
            # tracked_call wrapper removed — see _fastly_api docstring;
            # double-wrap inflated the visible call count to 2x.
            payload = _fastly_api(f"/stats/service/{logging_svc}?by={by}&from={from_ts}&to={to_ts}", api_key)

            fmt = "%Y-%m-%dT%H:00" if by == "hour" else "%Y-%m-%dT%H:%M" if by == "minute" else "%Y-%m-%d"
            stats_lookup: dict[str, int] = {}
            log_records_lookup: dict[str, int] = {}
            for record in payload.get("data", []):
                ts = record.get("start_time")
                if ts is None:
                    continue
                time_key = datetime.fromtimestamp(ts, tz=UTC).strftime(fmt)
                stats_lookup[time_key] = stats_lookup.get(time_key, 0) + int(record.get("requests") or 0)
                # Pick the first non-zero candidate field for log records;
                # mirrors compute_log_accounting's probe-list strategy.
                log_count = 0
                for fname in FASTLY_LOG_FIELDS:
                    v = record.get(fname)
                    if v:
                        log_count = int(v)
                        break
                if log_count:
                    log_records_lookup[time_key] = log_records_lookup.get(time_key, 0) + log_count

            total_api_requests = 0
            total_log_records = 0
            existing_points = {p["time"]: p for p in res["data"]}
            all_times = sorted(set(existing_points.keys()) | set(stats_lookup.keys()))

            new_data = []
            for t in all_times:
                p = existing_points.get(t, {"time": t, "row_count": 0, "bytes": 0})
                api_reqs = stats_lookup.get(t, 0)
                p["api_requests"] = api_reqs
                total_api_requests += api_reqs
                log_recs = log_records_lookup.get(t, 0)
                p["fastly_log_records"] = log_recs
                total_log_records += log_recs
                new_data.append(p)

            res["data"] = new_data
            res["total_api_requests"] = total_api_requests
            res["total_fastly_log_records"] = total_log_records

        except Exception:
            logger.debug("usage_log_activity: step failed (ignored)", exc_info=True)

    return UsageLogActivityResponse.with_telemetry(**res)


@usage_router.get("/rum-breakdown", response_model=UsageRumBreakdownResponse)
@query_errors()
def usage_rum_breakdown(
    start: str = Query(default=""),
    end: str = Query(default=""),
    by: str = Query(default="day"),
    source: dict = Depends(get_source),
):
    """Calculate RUM beacon volume and estimated FOS Class A operations cost."""
    from backend.core import metadata as metadata_db

    requested_by = by
    if by not in ("hour", "day"):
        by = "day"
    clamped_from_sub_hour = requested_by != by

    src = source
    service_id = src.get("name") or src.get("service_id")
    if not service_id or not isinstance(service_id, str):
        return UsageRumBreakdownResponse.with_telemetry(
            data=[],
            total_beacons=0,
            total_estimated_class_a=0,
            total_estimated_cost_usd=0.0,
            class_a_rate_per_1k=0.0,
            average_beacons_per_day=0.0,
            note="Service not found.",
        )

    start_str, end_str, from_ts, to_ts = window_to_epoch(start, end)

    # Get class A rate from config
    from backend import config as svcconfig

    global_rates = svcconfig.load_usage_logging_config()
    class_a_rate = float(global_rates.get("class_a_rate_per_1k", 0.005))

    # Query RUM beacon count by date from metadata DB using the ingestion tracking
    try:
        start_date = start_str.split("T")[0]
        end_date = end_str.split("T")[0]
        beacons_by_date: dict[str, int] = metadata_db.get_rum_beacons_by_date(service_id, start_date, end_date)
    except Exception:
        beacons_by_date = {}

    # Build response with cost estimates (each beacon = 1 Class A operation)
    data: list[RumBreakdownPoint] = []
    total_beacons = 0
    total_class_a = 0
    total_cost = 0.0

    for date_str in sorted(beacons_by_date.keys()):
        beacon_count = beacons_by_date[date_str]
        class_a_ops = beacon_count  # Each beacon = 1 PUT to FOS = 1 Class A op
        cost = (class_a_ops / 1000.0) * class_a_rate  # class_a_rate is per 1000 ops

        data.append(
            RumBreakdownPoint(
                date=date_str,
                beacon_count=beacon_count,
                estimated_class_a_ops=class_a_ops,
                estimated_cost_usd=round(cost, 6),
            )
        )

        total_beacons += beacon_count
        total_class_a += class_a_ops
        total_cost += cost

    # Calculate average beacons per day
    days = (to_ts - from_ts) / 86400.0
    avg_beacons_per_day = total_beacons / days if days > 0 else 0

    note = (
        "RUM beacon volume is tracked separately. Each beacon is one FOS Class A operation. "
        "This estimate assumes current beacon volume; actual costs depend on user traffic. "
        "These operations are also included in the total FOS Class A operations shown elsewhere."
    )
    if clamped_from_sub_hour:
        note += (
            f" The chart interval you selected ({requested_by}) was adjusted to daily granularity "
            "for RUM data (FOS doesn't provide sub-daily beacon breakdown)."
        )

    return UsageRumBreakdownResponse.with_telemetry(
        data=data,
        total_beacons=total_beacons,
        total_estimated_class_a=total_class_a,
        total_estimated_cost_usd=round(total_cost, 6),
        class_a_rate_per_1k=class_a_rate,
        average_beacons_per_day=round(avg_beacons_per_day, 2),
        note=note,
    )


_self_mod = _sys.modules[__name__]
_sys.modules["backend.routers.alerts"] = _self_mod
_sys.modules["backend.routers.usage"] = _self_mod
_sys.modules["backend.routers.audit"] = _self_mod
_routers_pkg = _sys.modules.get("backend.routers")
if _routers_pkg is not None:
    _routers_pkg.alerts = _self_mod  # type: ignore[attr-defined]
    _routers_pkg.usage = _self_mod  # type: ignore[attr-defined]
    _routers_pkg.audit = _self_mod  # type: ignore[attr-defined]
