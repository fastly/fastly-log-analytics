"""Alerts router."""

from __future__ import annotations

import ipaddress
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlparse

import duckdb
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response

from backend.deps import get_con, get_service_id
from backend.models.alerts import (
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
    _ToggleBody,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import alerts as repo
from backend.routers._state_sync import sync_admin_state
from backend.utils.auth import analyst_allowed_services as _analyst_allowed_services
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import not_found

router = APIRouter(prefix="/api/alerts", tags=["alerts"], responses=DEFAULT_ERROR_RESPONSES)

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
    """Validate a webhook/notification channel URL to prevent SSRF.

    Rejects non-http(s) schemes, missing hostnames, localhost/internal
    hostnames, and loopback/private/link-local/cloud-metadata IP literals.
    """
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
    """True when the per-service status cache reports ingested log data.

    Reads the same status cache the dashboard self-heal consults (the
    direct parquet+buffer min/max+count snapshot written at ingest, which
    updates immediately — unlike the cached Iceberg view SQL). Used by the
    alerts-preview self-heal to distinguish "view is stale" from "service
    genuinely has no data" without an extra DuckDB scan.
    """
    try:
        from backend import config as svcconfig

        cached = svcconfig.get_status(src["name"])
    except Exception:
        return False
    if not cached:
        return False
    return bool(cached.get("local_rows", 0)) or cached.get("latest_log_at") is not None


@router.get("/", response_model=AlertListResponse)
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
    # Cross-service ATTACH+UNION inside the repo returns the globally-
    # newest ``limit`` rows in one query — no per-service connection
    # open or in-Python slice. The analyst filter still runs in Python
    # because the scope set isn't known at query-plan time.
    alerts = repo.get_alerts(limit=limit)
    if allowed is not None:
        alerts = [a for a in alerts if a.get("service_id") in allowed]
    alerts = _filter_alerts_by_status(alerts, status)
    return _alert_list(alerts)


@router.get("/{service_id}", response_model=AlertListResponse)
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


@router.post("/", response_model=AlertResponse, status_code=201)
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


@router.post("/evaluate", response_model=AlertEvaluateResponse)
def evaluate_alerts_endpoint(
    request: Request,
    body: AlertEvaluateRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    """Evaluate enabled alert rules on demand for the active service (Admin only)."""
    target_service_id = (body.service_id if body and body.service_id else None) or service_id
    if not target_service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})

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


@router.post("/preview", response_model=AlertPreviewResponse)
def preview_alert(
    alert: Alert,
    request: Request,
    lookback_hours: int = Query(default=24, ge=1),
    con: duckdb.DuckDBPyConnection = Depends(get_con),
):
    import datetime

    from backend.core.duckdb import _safe_table_name, get_source_for_service

    # Security: analyst can only preview alerts against their scoped
    # services. Without this an analyst could compose an Alert against
    # another tenant's service_id and read its time-series data.
    require_service_in_scope(request, alert.service_id)

    src = get_source_for_service(alert.service_id)
    if not src:
        raise HTTPException(status_code=404, detail=not_found("Service not found"))

    table_name = _safe_table_name(src["name"])
    metric = alert.metric
    window = alert.window_min
    eval_type = getattr(alert, "evaluation_type", "absolute")
    eval_scope = getattr(alert, "evaluation_scope", "all")
    comp_period = getattr(alert, "comparison_period_min", None)
    status_codes = getattr(alert, "status_codes", None)

    lookback_mins = lookback_hours * 60
    if eval_type in ("relative_increase", "relative_decrease") and comp_period:
        lookback_mins = max(lookback_mins, comp_period + window + 60)

    if lookback_mins <= 180:
        group_sql = "time_bucket(INTERVAL '1 minute', timestamp)"
    elif lookback_mins <= 1440:
        group_sql = "time_bucket(INTERVAL '15 minutes', timestamp)"
    else:
        group_sql = "time_bucket(INTERVAL '1 hour', timestamp)"

    from backend.core.metrics import get_metric_sql

    agg_sql = get_metric_sql(metric, status_codes)

    scope_filter = ""
    if eval_scope == "edge":
        scope_filter = " AND edge = true"
    elif eval_scope == "origin":
        scope_filter = " AND edge = false"

    q = f"""
        SELECT {group_sql} as ts, {agg_sql} as val
        FROM {table_name}
        WHERE timestamp >= (SELECT max(timestamp) FROM {table_name}) - INTERVAL '{lookback_mins} minutes' {scope_filter}
        GROUP BY 1
        ORDER BY 1 ASC
    """

    try:
        from backend.repositories._base import safe_iso
        from backend.utils.telemetry import track_query

        with track_query(con, q, [], "alerts_preview") as cursor:
            rows = cursor.fetchall()

        # View-lag self-heal: the window is anchored to ``(SELECT
        # max(timestamp) FROM table)``, which is NULL when the cached
        # Iceberg view SQL lags a fresh ingest (e.g. a single buffered log
        # before the next sync/commit cron rebuild; dev runs no crons).
        # That yields 0 preview rows even though the data exists. If the
        # preview is empty BUT the per-service status cache reports data,
        # the view MUST be stale — force ONE rebuild (local boolean, no
        # retry storm) and re-run. A genuinely empty service won't have a
        # status-cache row count, so this won't fire there.
        if not rows and _status_cache_has_data(src):
            with track_query(con, f"SELECT 1 FROM {table_name} LIMIT 1", [], "alerts_preview_check") as cursor:
                has_any = cursor.fetchone()
            if not has_any:
                from backend.repositories._base import force_rebuild_view

                force_rebuild_view(con, src)
                with track_query(con, q, [], "alerts_preview") as cursor:
                    rows = cursor.fetchall()

        times = [safe_iso(r[0]) for r in rows]
        values = [float(r[1] or 0) for r in rows]

        res = {
            "times": times,
            "values": values,
            "type": "anomaly_zscore" if eval_type == "anomaly_zscore" else "absolute",
        }

        if eval_type == "anomaly_zscore":
            days = getattr(alert, "baseline_period_days", 7) or 7
            zscore_thresh = getattr(alert, "zscore_threshold", 3.0) or 3.0

            try:
                probe = con.execute(f"SELECT * FROM {table_name} LIMIT 0").description or []
                existing_cols = {d[0] for d in probe}
            except Exception:
                existing_cols = set()

            baseline_start_expr = f"(SELECT max(timestamp) FROM {table_name}) - INTERVAL '{days} days'"
            baseline_end_expr = f"(SELECT max(timestamp) FROM {table_name}) - INTERVAL '1 hours'"

            baseline_where = f"timestamp >= {baseline_start_expr} AND timestamp <= {baseline_end_expr} {scope_filter}"

            try:
                with track_query(
                    con, f"SELECT max(timestamp) FROM {table_name}", [], "alerts_preview_max_ts"
                ) as cursor:
                    max_ts_row = cursor.fetchone()
                    max_ts_dt = max_ts_row[0] if max_ts_row else None
            except Exception:
                max_ts_dt = None

            if max_ts_dt:
                st_dt = max_ts_dt - datetime.timedelta(days=days)
                st_utc = st_dt.astimezone(datetime.UTC)
                if "dt" in existing_cols:
                    baseline_where += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"

            q_baseline = f"""
                SELECT
                    EXTRACT(hour FROM ts) AS hr,
                    avg(val) AS mean_val,
                    stddev(val) AS stddev_val
                FROM (
                    SELECT date_trunc('hour', timestamp) AS ts, {agg_sql} AS val
                    FROM {table_name}
                    WHERE {baseline_where}
                    GROUP BY 1
                )
                GROUP BY 1
            """

            try:
                with track_query(con, q_baseline, [], "alerts_preview_baseline") as cursor:
                    baseline_rows = cursor.fetchall()
                baseline_map = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in baseline_rows}
            except Exception as e:
                import logging

                logging.getLogger(__name__).error(f"Baseline fetch failed: {e}")
                baseline_map = {}

            baseline_means = []
            baseline_thresholds = []

            for r in rows:
                dt = r[0]
                hr = dt.hour if (dt and hasattr(dt, "hour")) else 0
                mean_val, stddev_val = baseline_map.get(hr, (0.0, 0.0))
                stddev_val = stddev_val or 0.0001

                baseline_means.append(mean_val)
                baseline_thresholds.append(mean_val + zscore_thresh * stddev_val)

            res["hist_values"] = baseline_means
            res["threshold_values"] = baseline_thresholds

        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
            res["type"] = "relative"
            q_hist = f"""
                 SELECT {group_sql} as ts, {agg_sql} as val
                 FROM {table_name}
                 WHERE timestamp >= (SELECT max(timestamp) FROM {table_name}) - INTERVAL '{lookback_mins + comp_period} minutes'
                   AND timestamp <= (SELECT max(timestamp) FROM {table_name}) - INTERVAL '{comp_period} minutes'
                   {scope_filter}
                 GROUP BY 1
                 ORDER BY 1 ASC
             """
            with track_query(con, q_hist, [], "alerts_preview") as cursor:
                hist_rows = cursor.fetchall()

            hist_map = {}
            for r in hist_rows:
                if r[0]:
                    shifted = r[0] + datetime.timedelta(minutes=comp_period)
                    hist_map[shifted.isoformat()] = float(r[1] or 0)

            hist_values = [hist_map.get(t, 0) for t in times]
            res["hist_values"] = hist_values

        return AlertPreviewResponse.with_telemetry(data=res)
    except Exception as e:
        import logging

        logging.getLogger(__name__).error(f"Preview failed: {e}")
        return AlertPreviewResponse.with_telemetry(error=str(e))


@router.put("/{alert_id}", response_model=AlertResponse)
def update_alert(
    alert_id: str,
    alert: Alert,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    sid = alert.service_id or service_id
    if not sid:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    require_service_in_scope(request, sid)
    _require_admin_mutation(request)
    _validate_alert_urls(alert)
    updated_alert = alert.model_copy(update={"id": alert_id, "service_id": sid})
    res = repo.save_alert(updated_alert)
    res.setdefault("service_id", sid)
    sync_admin_state(sid)
    return AlertResponse.with_telemetry(data=res)


@router.patch("/{alert_id}/enabled", response_model=AlertResponse)
def toggle_alert_enabled(
    alert_id: str,
    body: _ToggleBody,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    # Security: service_id is required (audit finding 018). The pre-fix
    # variant fell through to an O(N) cross-tenant scan when service_id
    # was absent.
    if not service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    # Security: pre-flight scope check BEFORE the mutation. Earlier
    # implementation toggled first and then 403'd on the result, so a
    # cross-tenant write would still land and the analyst would just see
    # an error after the fact. Now the toggle never runs for an
    # unauthorized session.
    allowed = _analyst_allowed_services(request)
    if allowed is not None:
        existing = repo.get_alert_by_id(alert_id, service_id)
        target_sid = existing.get("service_id") if existing else service_id
        if target_sid not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": target_sid},
            )
    _require_admin_mutation(request)
    res = repo.toggle_alert(alert_id, body.enabled, service_id)
    res.setdefault("service_id", service_id)
    sync_admin_state(res.get("service_id"))
    return AlertResponse.with_telemetry(data=res)


@router.delete("/{alert_id}", status_code=204)
def delete_alert(
    alert_id: str,
    request: Request,
    service_id: str | None = Depends(get_service_id),
):
    # Security: service_id is required (audit finding 018).
    if not service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    # Pre-flight scope check: look up the alert's service_id before
    # deleting so we don't leak the existence of cross-tenant alerts
    # via a delete-then-403 pattern.
    allowed = _analyst_allowed_services(request)
    if allowed is not None:
        existing = repo.get_alert_by_id(alert_id, service_id)
        target_sid = existing.get("service_id") if existing else service_id
        if target_sid not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": target_sid},
            )
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


@router.post("/{alert_id}/test", response_model=AlertTestWebhookResponse)
def test_alert_webhook(
    alert_id: str,
    request: Request,
    body: AlertTestWebhookRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
):
    sid = (body.service_id if body and body.service_id else None) or service_id
    if not sid:
        raise HTTPException(status_code=400, detail={"error": "service_id_required"})
    require_service_in_scope(request, sid)
    _require_admin_mutation(request)
    return _run_test_alert_webhook(alert_id, sid, body)


@router.post("/{service_id}/{alert_id}/test", response_model=AlertTestWebhookResponse)
def test_alert_webhook_by_service(
    service_id: str,
    alert_id: str,
    request: Request,
    body: AlertTestWebhookRequest | None = Body(default=None),
):
    require_service_in_scope(request, service_id)
    _require_admin_mutation(request)
    return _run_test_alert_webhook(alert_id, service_id, body)
