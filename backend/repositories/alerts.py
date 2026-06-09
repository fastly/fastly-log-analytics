"""Repository for alerts.

Storage lives in per-service SQLite via ``backend.core.metadata_db``. This
module is a thin domain wrapper plus the alert-evaluation logic that still
needs a DuckDB connection (to read the log table).
"""

from __future__ import annotations

from datetime import UTC

import duckdb

from backend import config as svcconfig
from backend.core import metadata_db
from backend.core.metrics import get_metric_sql
from backend.models.alerts import Alert
from backend.repositories._sql import alerts as SQL
from backend.utils.telemetry import track_query


def _all_service_ids() -> list[str]:
    return [c["service_id"] for c in svcconfig.list_configs() if c.get("service_id")]


def get_alerts(service_id: str | None = None) -> list[dict]:
    """Return alerts. With service_id, only that service's; otherwise all services'."""
    if service_id:
        return metadata_db.list_alerts(service_id, filter_service_id=service_id)
    out: list[dict] = []
    for sid in _all_service_ids():
        out.extend(metadata_db.list_alerts(sid, filter_service_id=sid))
    return out


def save_alert(alert: Alert) -> dict:
    return metadata_db.save_alert(alert.service_id, alert)


def _find_alert_service(alert_id: str) -> str | None:
    """Scan all per-service metadata DBs to find which service owns this alert."""
    for sid in _all_service_ids():
        for a in metadata_db.list_alerts(sid, filter_service_id=sid):
            if a["id"] == alert_id:
                return sid
    return None


def get_alert_by_id(alert_id: str) -> dict | None:
    """Return the alert row whose id matches ``alert_id`` (or None).

    Security (defense-in-depth): the cross-tenant scope check in
    ``backend/routers/alerts.py:delete_alert`` calls this to look up
    ``service_id`` BEFORE mutating, so an analyst attempting a
    cross-tenant delete gets 403 and the underlying row stays untouched.
    Without this helper that check is dead code and the gate falls
    through to the middleware (which already blocks DELETE on
    /api/alerts for analysts, but the router-level gate is the
    secondary belt-and-suspenders).
    """
    for sid in _all_service_ids():
        for a in metadata_db.list_alerts(sid, filter_service_id=sid):
            if a.get("id") == alert_id:
                return a
    return None


def toggle_alert(alert_id: str, enabled: bool, service_id_hint: str | None = None) -> dict:
    """Toggle an alert. ``service_id_hint`` (from request context) avoids the
    cross-service scan when known; falls back to scan when not provided."""
    sid = service_id_hint or _find_alert_service(alert_id)
    if not sid:
        return {"id": alert_id, "status": "not_found", "service_id": None}
    return metadata_db.toggle_alert(sid, alert_id, enabled)


def delete_alert(alert_id: str, service_id_hint: str | None = None) -> dict:
    sid = service_id_hint or _find_alert_service(alert_id)
    if not sid:
        return {"status": "not_found", "service_id": None}
    return metadata_db.delete_alert(sid, alert_id)


def update_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    metadata_db.update_alert_last_triggered(service_id, alert_id, triggered_ts)


def evaluate_alert(
    con: duckdb.DuckDBPyConnection, src: dict, alert: dict, display_name: str = "", service_id: str = ""
) -> tuple[bool, str | None, dict | None, str | None]:
    """Run one alert against the log table; returns (triggered, webhook_url, payload, max_ts)."""
    from datetime import datetime, timedelta

    from backend.core.duckdb import _safe_table_name

    table_name = _safe_table_name(src["name"])
    metric = alert["metric"]
    window = alert["window_min"]
    eval_type = alert.get("evaluation_type", "absolute")
    eval_scope = alert.get("evaluation_scope", "all")
    comp_period = alert.get("comparison_period_min")
    status_codes = alert.get("status_codes")

    try:
        max_ts_query = SQL.MAX_TIMESTAMP.format(table=table_name)
        with track_query(con, max_ts_query, [], "alerts") as cursor:
            max_ts = cursor.fetchone()[0]

        if not max_ts:
            return False, None, None, None

        now_utc = datetime.now(UTC)
        if hasattr(max_ts, "replace"):
            max_ts_aware = max_ts.replace(tzinfo=UTC) if max_ts.tzinfo is None else max_ts
            if (now_utc - max_ts_aware) > timedelta(minutes=30):
                return False, None, None, None

        def build_metric_query(window_start_expr: str, window_end_expr: str) -> str:
            agg_or_sel = get_metric_sql(metric, status_codes)
            where_clause = f"timestamp >= {window_start_expr} AND timestamp <= {window_end_expr}"

            if eval_scope == "edge":
                where_clause += " AND edge = true"
            elif eval_scope == "origin":
                where_clause += " AND edge = false"

            if agg_or_sel.startswith("SELECT"):
                if "WHERE" in agg_or_sel:
                    return f"{agg_or_sel} AND {where_clause}"
                return f"{agg_or_sel} WHERE {where_clause}"
            return f"SELECT {agg_or_sel} FROM {table_name} WHERE {where_clause}"

        current_start = SQL.WINDOW_OFFSET_EXPR.format(table=table_name, minutes_ago=window)
        current_end = SQL.MAX_TIMESTAMP_SUBQUERY_EXPR.format(table=table_name)
        q_current = build_metric_query(current_start, current_end)

        with track_query(con, q_current, [], "alerts") as cursor:
            val = cursor.fetchone()[0] or 0

        if metric != "requests":
            q_req = SQL.COUNT_REQUESTS_IN_WINDOW.format(
                table=table_name,
                window_start_expr=current_start,
                window_end_expr=current_end,
            )
            with track_query(con, q_req, [], "alerts") as cursor:
                req_count = cursor.fetchone()[0] or 0
        else:
            req_count = val

        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
            if req_count < 10:
                return False, None, None, None

            hist_start = SQL.WINDOW_OFFSET_EXPR.format(table=table_name, minutes_ago=comp_period + window)
            hist_end = SQL.WINDOW_OFFSET_EXPR.format(table=table_name, minutes_ago=comp_period)
            q_hist = build_metric_query(hist_start, hist_end)

            with track_query(con, q_hist, [], "alerts") as cursor:
                hist_val = cursor.fetchone()[0] or 0

            if hist_val == 0:
                return False, None, None, None

            if eval_type == "relative_increase":
                val = ((val - hist_val) / hist_val) * 100.0
            else:  # relative_decrease
                val = ((hist_val - val) / hist_val) * 100.0

    except Exception as e:
        import logging

        logging.getLogger(__name__).error("Failed to evaluate alert %s: %s", alert.get("id"), e)
        return False, None, None, None

    op = alert["operator"]
    thresh = alert["threshold"]

    triggered = False
    if op == ">":
        triggered = val > thresh
    elif op == "<":
        triggered = val < thresh
    elif op == ">=":
        triggered = val >= thresh
    elif op == "<=":
        triggered = val <= thresh

    if triggered and req_count < 50 and eval_type != "absolute":
        return False, None, None, None

    if triggered:
        if alert.get("last_triggered_at"):
            try:
                from backend.utils.date_utils import parse_iso_utc

                last = parse_iso_utc(alert["last_triggered_at"]) or datetime.fromtimestamp(0, tz=UTC)
                if last.tzinfo is None:
                    last = last.replace(tzinfo=UTC)
                if now_utc - last < timedelta(hours=1):
                    return False, None, None, None
            except ValueError:
                pass

        webhook_payload: dict | None = None
        if alert.get("webhook_url"):
            s_id = service_id or alert.get("service_id", "")
            disp = display_name or src.get("name", s_id)
            manage_link = f"https://manage.fastly.com/configure/services/{s_id}" if s_id else "Unknown"

            if eval_type == "relative_increase":
                msg_val = f"increased by {val:.1f}% vs {comp_period}m ago"
            elif eval_type == "relative_decrease":
                msg_val = f"decreased by {val:.1f}% vs {comp_period}m ago"
            else:
                msg_val = f"is {val:.2f}"

            webhook_payload = {
                "text": (
                    "🚨 *Fastly Alert Triggered*\n"
                    f"*Name:* {alert['name']}\n"
                    f"*Metric:* {metric} {msg_val} (Threshold: {op} {thresh})\n"
                    f"*Service:* {disp}\n"
                    f"*Manage:* {manage_link}\n"
                    f"*Window:* Last {window} minutes"
                )
            }

        if hasattr(max_ts, "replace"):
            max_ts_aware = max_ts.replace(tzinfo=UTC) if max_ts.tzinfo is None else max_ts
            max_ts_str = max_ts_aware.isoformat()
        else:
            max_ts_str = str(max_ts)

        return True, alert.get("webhook_url"), webhook_payload, max_ts_str

    return False, None, None, None
