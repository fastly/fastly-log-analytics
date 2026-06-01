"""Alerts router."""

from __future__ import annotations

from datetime import UTC

import duckdb
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from backend.deps import get_con, get_service_id
from backend.models.alerts import Alert, AlertListResponse, AlertPreviewResponse, AlertResponse
from backend.repositories import alerts as repo
from backend.utils.router_utils import sync_admin_state

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("/", response_model=AlertListResponse)
def list_all_alerts():
    alerts = repo.get_alerts()
    from datetime import datetime

    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


@router.get("/{service_id}", response_model=AlertListResponse)
def list_service_alerts(service_id: str):
    alerts = repo.get_alerts(service_id)
    from datetime import datetime

    return AlertListResponse.with_telemetry(data=alerts, evaluated_at=datetime.now(UTC).isoformat())


@router.post("/", response_model=AlertResponse)
def create_alert(alert: Alert):
    res = repo.save_alert(alert)
    sync_admin_state(alert.service_id)
    return AlertPreviewResponse.with_telemetry(data=res)


@router.post("/preview", response_model=AlertPreviewResponse)
def preview_alert(alert: Alert, lookback_hours: int = 24, con: duckdb.DuckDBPyConnection = Depends(get_con)):
    import datetime

    from fastapi import HTTPException

    from backend.core.duckdb import _safe_table_name, get_source_for_service

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
def toggle_alert_enabled(alert_id: str, body: _ToggleBody, service_id: str | None = Depends(get_service_id)):
    res = repo.toggle_alert(alert_id, body.enabled, service_id_hint=service_id)
    sync_admin_state(res.get("service_id"))
    return AlertPreviewResponse.with_telemetry(data=res)


@router.delete("/{alert_id}", response_model=AlertResponse)
def delete_alert(alert_id: str, service_id: str | None = Depends(get_service_id)):
    res = repo.delete_alert(alert_id, service_id_hint=service_id)
    sync_admin_state(res.get("service_id"))
    return AlertPreviewResponse.with_telemetry(data=res)
