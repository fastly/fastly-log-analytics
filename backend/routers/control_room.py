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
    SavedView,
    SavedViewRecord,
    ViewSaveResponse,
    _ToggleBody,
)
from backend.models.services import (
    ControlRoomActionResponse,
    ControlRoomTabResponse,
    CorrelateRequest,
    CorrelateResponse,
    LogFieldAuditResponse,
    MutationStub,
    RealtimeSeedResponse,
    WizardStateResponse,
    WizardStepRequest,
    WizardStepResponse,
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
    from backend.core.realtime.transform import transform_single_second

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
    from backend.core.realtime.publisher import publisher as rt_publisher
    from backend.core.realtime.transform import poller

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
    from backend.core.realtime.publisher import publisher as rt_publisher
    from backend.core.realtime.transform import poller
    from backend.utils.sse import iter_with_disconnect_ping

    poller.ensure_polling(service_id)

    async def stream() -> AsyncIterator[str]:
        async for payload in iter_with_disconnect_ping(rt_publisher.subscribe(service_id), request, ping_seconds=15):
            yield json.dumps(payload)

    return sse_response(stream())


# ── Recent error stream ─────────────────────────────────────────────────────


def _recent_standard_errors(source: dict, service_id: str) -> list[dict]:
    from backend.core import duckdb as _db
    from backend.core.iceberg import execute_with_stale_view_retry
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
        from backend.core.iceberg import execute_with_stale_view_retry

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


_sys.modules.setdefault("backend.routers.alerts", _sys.modules[__name__])
_routers_pkg = _sys.modules.get("backend.routers")
if _routers_pkg is not None:
    _routers_pkg.alerts = _sys.modules[__name__]  # type: ignore[attr-defined]
