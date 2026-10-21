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
from backend.repositories._base import QueryRunner
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

        from backend.utils.date_utils import ensure_utc, parse_iso_utc

        now_utc = datetime.now(UTC)

        # Ensure max_ts is a timezone-aware datetime
        max_ts_dt = max_ts
        if isinstance(max_ts_dt, str):
            max_ts_dt = parse_iso_utc(max_ts_dt)
        if hasattr(max_ts_dt, "tzinfo") and max_ts_dt.tzinfo is None:
            max_ts_dt = ensure_utc(max_ts_dt)

        if hasattr(max_ts, "replace"):
            max_ts_aware = ensure_utc(max_ts)
            if (now_utc - max_ts_aware) > timedelta(minutes=30):
                return False, None, None, None

        # Pre-compute the timestamp literal once so downstream window
        # expressions use it directly instead of re-running the
        # max(timestamp) subquery (up to 6x per alert).
        max_ts_literal = f"'{max_ts}'::TIMESTAMPTZ"

        # Safely query table columns to check for dt/timestamp_hour partition columns existence
        try:
            existing_cols = set(QueryRunner(con, src).get_schema_cols())
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
                from backend.utils.date_utils import ensure_utc, parse_iso_utc

                last = parse_iso_utc(alert["last_triggered_at"]) or datetime.fromtimestamp(0, tz=UTC)
                last = ensure_utc(last)
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


def preview_alert_series(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    table_name: str,
    alert: Alert,
    lookback_hours: int,
    *,
    has_status_data,
) -> dict:
    """Time series (plus baseline / comparison overlays) behind ``POST /api/alerts/preview``.

    ``has_status_data`` is a zero-arg callable consulted only when the first query is empty:
    if the status cache reports ingested data the cached view must be stale, so ONE rebuild
    is forced and the series re-run (a genuinely empty service never triggers it).
    ``table_name`` is the caller's ``_safe_table_name`` result. Raises on query failure; the router converts that into the response's ``error`` field.
    """
    import datetime
    import logging

    from backend.repositories._base import safe_iso
    from backend.utils.telemetry import track_query

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

    agg_sql = get_metric_sql(metric, status_codes)

    base_scope_filter = ""
    if eval_scope == "edge":
        base_scope_filter = " AND edge = true"
    elif eval_scope == "origin":
        base_scope_filter = " AND edge = false"

    anchor_dt = None
    existing_cols: set[str] = set()
    if src:
        try:
            from backend.utils.date_utils import parse_iso_utc

            svc_status = svcconfig.get_status(src.get("name") or src.get("service_id") or "") or {}
            latest_iso = svc_status.get("latest_log_at")
            anchor_dt = parse_iso_utc(latest_iso) if latest_iso else None
            if anchor_dt:
                existing_cols = set(QueryRunner(con, src).get_schema_cols())
        except Exception:
            pass

    def _prune_clause(mins: int) -> str:
        if not anchor_dt or not existing_cols:
            return ""
        clause = ""
        prune_st = (anchor_dt - datetime.timedelta(minutes=mins + 60)).astimezone(datetime.UTC)
        if "dt" in existing_cols:
            clause += f" AND dt >= '{prune_st.strftime('%Y-%m-%d')}'"
        if "timestamp_hour" in existing_cols:
            clause += f" AND timestamp_hour >= '{prune_st.strftime('%Y-%m-%d-%H')}'"
        return clause

    max_ts_prune = _prune_clause(lookback_mins)
    scope_filter = base_scope_filter + max_ts_prune

    q = SQL.PREVIEW_SERIES.format(
        group_sql=group_sql,
        agg_sql=agg_sql,
        table=table_name,
        lookback_mins=lookback_mins,
        scope_filter=scope_filter,
    )
    if max_ts_prune:
        q = q.replace(
            f"(SELECT max(timestamp) FROM {table_name})",
            f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})",
        )

    with track_query(con, q, [], "alerts_preview") as cursor:
        rows = cursor.fetchall()

    # View-lag self-heal: the window is anchored to ``(SELECT max(timestamp) FROM table)``,
    # which is NULL when the cached Iceberg view SQL lags a fresh ingest (e.g. a single
    # buffered log before the next sync/commit cron rebuild; dev runs no crons). That yields
    # 0 preview rows even though the data exists. If the preview is empty BUT the per-service
    # status cache reports data, the view MUST be stale — force ONE rebuild (local boolean,
    # no retry storm) and re-run. A genuinely empty service won't have a status-cache row
    # count, so this won't fire there.
    if not rows and has_status_data():
        with track_query(con, SQL.PREVIEW_ANY_ROW.format(table=table_name), [], "alerts_preview_check") as cursor:
            has_any = cursor.fetchone()
        if not has_any:
            from backend.repositories._base import force_rebuild_view

            force_rebuild_view(con, src)
            with track_query(con, q, [], "alerts_preview") as cursor:
                rows = cursor.fetchall()

    times = [safe_iso(r[0]) for r in rows]
    values = [float(r[1] or 0) for r in rows]

    res: dict = {
        "times": times,
        "values": values,
        "type": "anomaly_zscore" if eval_type == "anomaly_zscore" else "absolute",
    }

    if eval_type == "anomaly_zscore":
        days = getattr(alert, "baseline_period_days", 7) or 7
        zscore_thresh = getattr(alert, "zscore_threshold", 3.0) or 3.0

        if not existing_cols:
            try:
                existing_cols = set(QueryRunner(con, src).get_schema_cols())
            except Exception:
                existing_cols = set()

        max_ts_subq = (
            f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})"
            if max_ts_prune
            else f"(SELECT max(timestamp) FROM {table_name})"
        )
        baseline_start_expr = f"{max_ts_subq} - INTERVAL '{days} days'"
        baseline_end_expr = f"{max_ts_subq} - INTERVAL '1 hours'"

        baseline_where = f"timestamp >= {baseline_start_expr} AND timestamp <= {baseline_end_expr} {base_scope_filter}"

        max_ts_dt = anchor_dt
        if not max_ts_dt:
            try:
                with track_query(
                    con, SQL.PREVIEW_MAX_TS.format(table=table_name), [], "alerts_preview_max_ts"
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
            if "timestamp_hour" in existing_cols:
                baseline_where += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"

        q_baseline = SQL.PREVIEW_BASELINE.format(agg_sql=agg_sql, table=table_name, baseline_where=baseline_where)

        try:
            with track_query(con, q_baseline, [], "alerts_preview_baseline") as cursor:
                baseline_rows = cursor.fetchall()
            baseline_map = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in baseline_rows}
        except Exception as e:
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
        hist_scope_filter = base_scope_filter + _prune_clause(lookback_mins + comp_period)
        q_hist = SQL.PREVIEW_HISTORY.format(
            group_sql=group_sql,
            agg_sql=agg_sql,
            table=table_name,
            start_minutes=lookback_mins + comp_period,
            comp_period=comp_period,
            scope_filter=hist_scope_filter,
        )
        if max_ts_prune:
            q_hist = q_hist.replace(
                f"(SELECT max(timestamp) FROM {table_name})",
                f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})",
            )
        with track_query(con, q_hist, [], "alerts_preview") as cursor:
            hist_rows = cursor.fetchall()

        hist_map = {}
        for r in hist_rows:
            if r[0]:
                shifted = r[0] + datetime.timedelta(minutes=comp_period)
                hist_map[shifted.isoformat()] = float(r[1] or 0)

        res["hist_values"] = [hist_map.get(t, 0) for t in times]

    return res
