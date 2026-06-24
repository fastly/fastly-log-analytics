"""Alerts router."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from backend.deps import get_con, get_service_id
from backend.models.alerts import Alert, AlertListResponse, AlertPreviewResponse, AlertResponse, _ToggleBody
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import alerts as repo
from backend.routers._state_sync import sync_admin_state
from backend.utils.auth import analyst_allowed_services as _analyst_allowed_services
from backend.utils.auth import require_service_in_scope
from backend.utils.router_utils import not_found

router = APIRouter(prefix="/api/alerts", tags=["alerts"], responses=DEFAULT_ERROR_RESPONSES)


def _alert_list(alerts):
    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


@router.get("/", response_model=AlertListResponse)
def list_all_alerts(
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max alerts to return; oldest dropped."),
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
    return _alert_list(alerts)


@router.get("/{service_id}", response_model=AlertListResponse)
def list_service_alerts(
    service_id: str,
    request: Request,
    limit: int = Query(default=500, ge=1, le=2000, description="Max alerts to return."),
):
    """Return alerts for one service. Analyst gets 403 if the service
    isn't in their invite (security)."""
    require_service_in_scope(request, service_id)
    alerts = repo.get_alerts(service_id)[:limit]
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
    res = repo.save_alert(alert)
    sync_admin_state(alert.service_id)
    return AlertResponse.with_telemetry(data=res)


@router.post("/preview", response_model=AlertPreviewResponse)
def preview_alert(
    alert: Alert,
    request: Request,
    lookback_hours: int = 24,
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

        times = [safe_iso(r[0]) for r in rows]
        values = [float(r[1] or 0) for r in rows]

        res = {"times": times, "values": values, "type": "absolute"}

        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
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
            res["type"] = "relative"

        return AlertPreviewResponse.with_telemetry(data=res)
    except Exception as e:
        import logging

        logging.getLogger(__name__).error(f"Preview failed: {e}")
        return AlertPreviewResponse.with_telemetry(error=str(e))


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
        if existing and existing.get("service_id") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": existing.get("service_id")},
            )
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
        if existing and existing.get("service_id") not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": existing.get("service_id")},
            )
    res = repo.delete_alert(alert_id, service_id)
    sync_admin_state(res.get("service_id") or service_id)
    return Response(status_code=204)
