"""Alerts and Saved Views router."""

from __future__ import annotations

import ipaddress
import logging
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import urlparse

import duckdb
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response

from backend.core.request_context import get_con, get_service_id
from backend.models.common import (
    DEFAULT_ERROR_RESPONSES,
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
from backend.repositories import assets as repo
from backend.utils.auth import (
    analyst_allowed_services as _analyst_allowed_services,
)
from backend.utils.auth import (
    require_owned_row,
    require_service_in_scope,
)
from backend.utils.router_utils import (
    not_found,
    redact_error_text,
    require_service_id,
    sync_admin_state,
)

logger = logging.getLogger(__name__)

alerts_router = APIRouter(prefix="/api/alerts", tags=["alerts"], responses=DEFAULT_ERROR_RESPONSES)
router = alerts_router

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


def _filter_alerts_by_status(alerts: list[dict[str, Any]], status: AlertStatusFilter | None) -> list[dict[str, Any]]:
    if not status or status == "all":
        return alerts
    if status == "muted":
        return [a for a in alerts if not a.get("enabled", True)]
    if status == "alerting":
        return [a for a in alerts if a.get("enabled", True) and bool(a.get("last_triggered_at"))]
    if status == "ok":
        return [a for a in alerts if a.get("enabled", True) and not a.get("last_triggered_at")]
    return alerts


def _alert_list(alerts: list[dict[str, Any]]) -> AlertListResponse:
    resp: AlertListResponse = AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())
    return resp


def _status_cache_has_data(src: dict[str, Any]) -> bool:
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
) -> AlertListResponse:
    """Return alerts visible to the caller."""
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
) -> AlertListResponse:
    """Return alerts for one service. Analyst gets 403 if the service isn't in their invite."""
    require_service_in_scope(request, service_id)
    alerts = repo.get_alerts(service_id)[:limit]
    alerts = _filter_alerts_by_status(alerts, status)
    return _alert_list(alerts)


@alerts_router.post("/", response_model=AlertResponse, status_code=201)
@alerts_router.post("", response_model=AlertResponse, status_code=201, include_in_schema=False)
def create_alert(alert: Alert, request: Request) -> AlertResponse:
    """Create an alert."""
    require_service_in_scope(request, alert.service_id)
    _require_admin_mutation(request)
    _validate_alert_urls(alert)
    res = repo.save_alert(alert)
    sync_admin_state(alert.service_id)
    resp: AlertResponse = AlertResponse.with_telemetry(data=res)
    return resp


@alerts_router.post("/evaluate", response_model=AlertEvaluateResponse)
def evaluate_alerts_endpoint(
    request: Request,
    body: AlertEvaluateRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
) -> AlertEvaluateResponse:
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

    resp: AlertEvaluateResponse = AlertEvaluateResponse.with_telemetry(
        data=AlertEvaluateData(
            service_id=target_service_id,
            evaluated=len(results),
            triggered=triggered_count,
            results=results,
            evaluated_at=datetime.now(UTC).isoformat(),
        )
    )
    return resp


@alerts_router.post("/preview", response_model=AlertPreviewResponse)
def preview_alert(
    alert: Alert,
    request: Request,
    lookback_hours: int = Query(default=24, ge=1),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
) -> AlertPreviewResponse:
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
        resp: AlertPreviewResponse = AlertPreviewResponse.with_telemetry(data=res)
        return resp
    except Exception as e:
        logger.error(f"Preview failed: {e}")
        err_resp: AlertPreviewResponse = AlertPreviewResponse.with_telemetry(error=redact_error_text(e)[:500])
        return err_resp


@alerts_router.put("/{alert_id}", response_model=AlertResponse)
def update_alert(
    alert_id: str,
    alert: Alert,
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> AlertResponse:
    sid = alert.service_id or service_id
    sid = require_service_id(sid)
    require_service_in_scope(request, sid)
    _require_admin_mutation(request)
    _validate_alert_urls(alert)
    updated_alert = alert.model_copy(update={"id": alert_id, "service_id": sid})
    res = repo.save_alert(updated_alert)
    res.setdefault("service_id", sid)
    sync_admin_state(sid)
    resp: AlertResponse = AlertResponse.with_telemetry(data=res)
    return resp


@alerts_router.patch("/{alert_id}/enabled", response_model=AlertResponse)
def toggle_alert_enabled(
    alert_id: str,
    body: _ToggleBody,
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> AlertResponse:
    resolved_id = require_service_id(service_id)
    require_owned_row(request, resolved_id, alert_id, repo.get_alert_by_id)
    _require_admin_mutation(request)
    res = repo.toggle_alert(alert_id, body.enabled, resolved_id)
    res.setdefault("service_id", resolved_id)
    sync_admin_state(res.get("service_id"))
    resp: AlertResponse = AlertResponse.with_telemetry(data=res)
    return resp


@alerts_router.delete("/{alert_id}", status_code=204)
def delete_alert(
    alert_id: str,
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> Response:
    resolved_id = require_service_id(service_id)
    require_owned_row(request, resolved_id, alert_id, repo.get_alert_by_id)
    _require_admin_mutation(request)
    res = repo.delete_alert(alert_id, resolved_id)
    sync_admin_state(res.get("service_id") or resolved_id)
    return Response(status_code=204)


def _run_test_alert_webhook(
    alert_id: str,
    sid: str,
    body: AlertTestWebhookRequest | None,
) -> AlertTestWebhookResponse:
    import importlib

    metadata_jobs: Any = importlib.import_module("backend.cron.jobs.metadata")
    _post_alert_webhook = metadata_jobs._post_alert_webhook
    _post_pagerduty_notification = metadata_jobs._post_pagerduty_notification
    _post_slack_notification = metadata_jobs._post_slack_notification

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
        dry_resp: AlertTestWebhookResponse = AlertTestWebhookResponse.with_telemetry(
            data=AlertTestWebhookData(
                alert_id=alert_id,
                service_id=sid,
                status="validated",
                targets_tested=len(targets),
                channel_type=targets[0][0],
                detail="Webhook URL validation succeeded.",
            )
        )
        return dry_resp

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

    sent_resp: AlertTestWebhookResponse = AlertTestWebhookResponse.with_telemetry(
        data=AlertTestWebhookData(
            alert_id=alert_id,
            service_id=sid,
            status="sent",
            targets_tested=len(targets),
            channel_type=targets[0][0],
            detail=f"Dispatched test notification to {len(targets)} target(s).",
        )
    )
    return sent_resp


@alerts_router.post("/{alert_id}/test", response_model=AlertTestWebhookResponse)
def test_alert_webhook(
    alert_id: str,
    request: Request,
    body: AlertTestWebhookRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
) -> AlertTestWebhookResponse:
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
) -> AlertTestWebhookResponse:
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
) -> list[SavedViewRecord]:
    """Security: analyst can only list views for services in their scope."""
    require_service_in_scope(request, service_id)
    rows = repo.get_views(service_id)[:limit]
    return [SavedViewRecord(**r) if isinstance(r, dict) else r for r in rows]


@views_router.post("/", status_code=201, response_model=ViewSaveResponse)
def create_view(view: SavedView, request: Request) -> ViewSaveResponse:
    """Security: analyst can only create views for services in their scope."""
    require_service_in_scope(request, view.service_id)
    res = repo.save_view(view)
    sync_admin_state(view.service_id)
    if isinstance(res, ViewSaveResponse):
        return res
    return ViewSaveResponse(**res)


@views_router.delete("/{view_id}", status_code=204)
def delete_view(view_id: str, request: Request, service_id: str | None = Depends(get_service_id)) -> Response:
    resolved_id = require_service_id(service_id)
    require_owned_row(request, resolved_id, view_id, repo.get_view_by_id)
    res = repo.delete_view(view_id, resolved_id)
    sync_admin_state(res.get("service_id") or resolved_id)
    return Response(status_code=204)
