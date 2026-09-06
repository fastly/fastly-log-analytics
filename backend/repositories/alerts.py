"""Repository for alerts.

Storage lives in per-service SQLite via ``backend.core.metadata``. This
module is a thin domain wrapper plus the alert-evaluation logic that still
needs a DuckDB connection (to read the log table).
"""

from __future__ import annotations

from datetime import UTC

import duckdb

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.core.metrics import get_metric_sql
from backend.models.alerts import Alert
from backend.repositories._sql import alerts as SQL
from backend.utils.telemetry import track_query


def get_alerts(service_id: str | None = None, *, limit: int = 500) -> list[dict]:
    """Return alerts. With service_id, only that service's (no limit applied —
    bounded by the per-service-bucket cap inside list_alerts). Without
    service_id, return the globally-newest ``limit`` alerts across all
    services via a single ATTACH+UNION query.

    Previously the cross-service path looped one connection-open + SELECT
    per service then sliced in Python — the slice gave per-service-bucket
    ordering rather than true cross-service newest-N, and the N
    connection opens dominated runtime on installs with many services.
    """
    if service_id:
        return metadata_db.list_alerts(service_id, filter_service_id=service_id)
    service_ids: list[str] = [sid for cfg in svcconfig.list_configs() if (sid := cfg.get("service_id"))]
    return metadata_db.list_alerts_cross_service(service_ids, limit=limit)


def save_alert(alert: Alert) -> dict:
    return metadata_db.save_alert(alert.service_id, alert)


def get_alert_by_id(alert_id: str, service_id: str) -> dict | None:
    """Return the alert row whose id matches ``alert_id`` in the given
    service (or None).

    Security (defense-in-depth): the cross-tenant scope check in
    ``backend/routers/alerts.py:delete_alert`` calls this to look up
    ``service_id`` BEFORE mutating, so an analyst attempting a
    cross-tenant delete gets 403 and the underlying row stays untouched.

    ``service_id`` is required — see audit finding 018 (same O(N)
    cross-tenant-scan vulnerability the views.py module had).
    """
    for a in metadata_db.list_alerts(service_id, filter_service_id=service_id):
        if a.get("id") == alert_id:
            return a
    return None


def toggle_alert(alert_id: str, enabled: bool, service_id: str) -> dict:
    """Toggle an alert. ``service_id`` is required — see audit finding 018."""
    return metadata_db.toggle_alert(service_id, alert_id, enabled)


def delete_alert(alert_id: str, service_id: str) -> dict:
    """Delete an alert. ``service_id`` is required — see audit finding 018."""
    return metadata_db.delete_alert(service_id, alert_id)


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

        from datetime import datetime, timedelta

        from backend.utils.date_utils import parse_iso_utc

        now_utc = datetime.now(UTC)

        # Ensure max_ts is a timezone-aware datetime
        max_ts_dt = max_ts
        if isinstance(max_ts_dt, str):
            max_ts_dt = parse_iso_utc(max_ts_dt)
        if hasattr(max_ts_dt, "tzinfo") and max_ts_dt.tzinfo is None:
            max_ts_dt = max_ts_dt.replace(tzinfo=UTC)

        if hasattr(max_ts, "replace"):
            max_ts_aware = max_ts.replace(tzinfo=UTC) if max_ts.tzinfo is None else max_ts
            if (now_utc - max_ts_aware) > timedelta(minutes=30):
                return False, None, None, None

        # Pre-compute the timestamp literal once so downstream window
        # expressions use it directly instead of re-running the
        # max(timestamp) subquery (up to 6x per alert).
        max_ts_literal = f"'{max_ts}'::TIMESTAMPTZ"

        # Safely query table columns to check for dt/timestamp_hour partition columns existence
        try:
            probe = con.execute(f"SELECT * FROM {table_name} LIMIT 0").description or []
            existing_cols = {d[0] for d in probe}
        except Exception:
            existing_cols = set()

        def _window_offset(minutes_ago: float) -> str:
            return f"{max_ts_literal} - INTERVAL '{minutes_ago} minutes'"

        def build_metric_query(
            window_start_expr: str, window_end_expr: str, start_dt: datetime, end_dt: datetime
        ) -> str:
            agg_or_sel = get_metric_sql(metric, status_codes)
            where_clause = f"timestamp >= {window_start_expr} AND timestamp <= {window_end_expr}"

            # Partition pruning via dt/timestamp_hour virtual columns (if present)
            st_utc = start_dt.astimezone(UTC)
            et_utc = end_dt.astimezone(UTC)
            if "dt" in existing_cols:
                where_clause += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"
                where_clause += f" AND dt <= '{et_utc.strftime('%Y-%m-%d')}'"
            if "timestamp_hour" in existing_cols:
                where_clause += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"
                where_clause += f" AND timestamp_hour <= '{et_utc.strftime('%Y-%m-%d-%H')}'"

            if eval_scope == "edge":
                where_clause += " AND edge = true"
            elif eval_scope == "origin":
                where_clause += " AND edge = false"

            if agg_or_sel.startswith("SELECT"):
                if "WHERE" in agg_or_sel:
                    return f"{agg_or_sel} AND {where_clause}"
                return f"{agg_or_sel} WHERE {where_clause}"
            return f"SELECT {agg_or_sel} FROM {table_name} WHERE {where_clause}"

        start_dt_current = max_ts_dt - timedelta(minutes=window)
        end_dt_current = max_ts_dt

        current_start = _window_offset(window)
        current_end = max_ts_literal
        q_current = build_metric_query(current_start, current_end, start_dt_current, end_dt_current)

        with track_query(con, q_current, [], "alerts") as cursor:
            val = cursor.fetchone()[0] or 0

        # Request count is only used for relative alerts to ensure minimum traffic floor
        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
            if metric != "requests":
                q_req_where = f"timestamp >= {current_start} AND timestamp <= {current_end}"
                st_utc = start_dt_current.astimezone(UTC)
                et_utc = end_dt_current.astimezone(UTC)
                if "dt" in existing_cols:
                    q_req_where += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"
                    q_req_where += f" AND dt <= '{et_utc.strftime('%Y-%m-%d')}'"
                if "timestamp_hour" in existing_cols:
                    q_req_where += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"
                    q_req_where += f" AND timestamp_hour <= '{et_utc.strftime('%Y-%m-%d-%H')}'"

                q_req = f"SELECT count(*) FROM {table_name} WHERE {q_req_where}"
                with track_query(con, q_req, [], "alerts") as cursor:
                    req_count = cursor.fetchone()[0] or 0
            else:
                req_count = val

            if req_count < 10:
                return False, None, None, None

            start_dt_hist = max_ts_dt - timedelta(minutes=comp_period + window)
            end_dt_hist = max_ts_dt - timedelta(minutes=comp_period)

            hist_start = _window_offset(comp_period + window)
            hist_end = _window_offset(comp_period)
            q_hist = build_metric_query(hist_start, hist_end, start_dt_hist, end_dt_hist)

            with track_query(con, q_hist, [], "alerts") as cursor:
                hist_val = cursor.fetchone()[0] or 0

            if hist_val == 0:
                return False, None, None, None

            if eval_type == "relative_increase":
                val = ((val - hist_val) / hist_val) * 100.0
            else:  # relative_decrease
                val = ((hist_val - val) / hist_val) * 100.0
        elif eval_type == "anomaly_zscore":
            if metric != "requests":
                q_req_where = f"timestamp >= {current_start} AND timestamp <= {current_end}"
                st_utc = start_dt_current.astimezone(UTC)
                et_utc = end_dt_current.astimezone(UTC)
                if "dt" in existing_cols:
                    q_req_where += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"
                if "timestamp_hour" in existing_cols:
                    q_req_where += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"

                q_req = f"SELECT count(*) FROM {table_name} WHERE {q_req_where}"
                with track_query(con, q_req, [], "alerts") as cursor:
                    req_count = cursor.fetchone()[0] or 0
            else:
                req_count = val

            days = alert.get("baseline_period_days") or 7
            zscore_thresh = alert.get("zscore_threshold") or 3.0

            baseline_start_dt = max_ts_dt - timedelta(days=days)
            baseline_end_dt = max_ts_dt - timedelta(hours=1)  # Exclude the active hour

            baseline_start_expr = f"{max_ts_literal} - INTERVAL '{days} days'"
            baseline_end_expr = f"{max_ts_literal} - INTERVAL '1 hours'"

            agg_or_sel = get_metric_sql(metric, status_codes)
            where_clause = (
                f"timestamp >= {baseline_start_expr} AND timestamp <= {baseline_end_expr} "
                f"AND EXTRACT(hour FROM timestamp) = EXTRACT(hour FROM {max_ts_literal})"
            )

            st_utc = baseline_start_dt.astimezone(UTC)
            et_utc = baseline_end_dt.astimezone(UTC)
            if "dt" in existing_cols:
                where_clause += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"

            if agg_or_sel.strip().lower().startswith("select"):
                agg_expr = agg_or_sel.replace(f"FROM {table_name}", "").replace("SELECT", "").strip()
                q_hist_series = f"SELECT {agg_expr} AS val FROM {table_name} WHERE {where_clause} GROUP BY date_trunc('hour', timestamp)"
            else:
                q_hist_series = f"SELECT {agg_or_sel} AS val FROM {table_name} WHERE {where_clause} GROUP BY date_trunc('hour', timestamp)"

            q_stats = f"WITH series AS ({q_hist_series}) SELECT avg(val), stddev(val) FROM series"

            with track_query(con, q_stats, [], "alerts") as cursor:
                mean_val, stddev_val = cursor.fetchone()

            if mean_val is None:
                return False, None, None, None

            stddev_val = stddev_val or 0.0001
            zscore = (val - mean_val) / stddev_val

            val = zscore
            alert["operator"] = ">"  # Z-score alerts trigger when exceeding threshold stddev
            alert["threshold"] = zscore_thresh
        else:
            req_count = val

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
