"""Alerts router."""

from __future__ import annotations

from datetime import UTC

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.deps import get_con, get_service_id
from backend.models.alerts import Alert, AlertListResponse, AlertPreviewResponse, AlertResponse
from backend.repositories import alerts as repo
from backend.utils.router_utils import sync_admin_state

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


def _analyst_allowed_services(request: Request) -> set[str] | None:
    """Return the set of service IDs the caller (analyst session) can see,
    or ``None`` for admin requests (no scope restriction).

    Security: every read / mutation on the alerts collection must
    filter by this set so an analyst scoped to ``svc-A`` cannot
    enumerate or modify ``svc-B``'s alerts via the cross-tenant pattern
    GET /api/alerts/ , GET /api/alerts/{other_id}, etc.
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None  # admin — unrestricted
    return set(analyst_session.service_ids or [])


@router.get("/", response_model=AlertListResponse)
def list_all_alerts(request: Request):
    """Return alerts visible to the caller.

    Admin: every alert across every service. Analyst: only alerts for
    services in their invite's scope (security).
    """
    allowed = _analyst_allowed_services(request)
    alerts = repo.get_alerts()
    if allowed is not None:
        alerts = [a for a in alerts if a.get("service_id") in allowed]
    from datetime import datetime

    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


@router.get("/{service_id}", response_model=AlertListResponse)
def list_service_alerts(service_id: str, request: Request):
    """Return alerts for one service. Analyst gets 403 if the service
    isn't in their invite (security)."""
    allowed = _analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(status_code=403, detail={"error": "service_not_authorized", "service": service_id})
    alerts = repo.get_alerts(service_id)
    from datetime import datetime

    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


@router.post("/", response_model=AlertResponse)
def create_alert(alert: Alert, request: Request):
    """Create an alert. Analyst can only create alerts for services in
    their invite scope (security). The Phase-1 analyst middleware
    also blocks POSTs on /api/alerts for analysts entirely (not in the
    _ANALYST_ALLOWED_WRITE_PREFIXES list), so this is defense-in-depth
    for the admin-impersonating-analyst case."""
    allowed = _analyst_allowed_services(request)
    if allowed is not None and alert.service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": alert.service_id},
        )
    res = repo.save_alert(alert)
    sync_admin_state(alert.service_id)
    return AlertPreviewResponse.with_telemetry(data=res)


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
    allowed = _analyst_allowed_services(request)
    if allowed is not None and alert.service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": alert.service_id},
        )

    src = get_source_for_service(alert.service_id)
    if not src:
        raise HTTPException(status_code=404, detail="Service not found")

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


class _ToggleBody(BaseModel):
    enabled: bool


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
    return AlertPreviewResponse.with_telemetry(data=res)


@router.delete("/{alert_id}", response_model=AlertResponse)
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
    res.setdefault("service_id", service_id)
    sync_admin_state(res.get("service_id"))
    return AlertPreviewResponse.with_telemetry(data=res)
