"""Alerts and Saved Views router."""

from __future__ import annotations

import ipaddress
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, Response

from backend import config as svcconfig
from backend.core.clickhouse import get_clickhouse_client
from backend.core.fos_client import _safe_table_name, get_source_for_service
from backend.core.request_context import get_service_id
from backend.high_scale._common import visible_publication_fence_sql
from backend.high_scale.ingest_controller import get_high_scale_service_registry
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

    if ip is not None:
        if isinstance(ip, ipaddress.IPv6Address):
            if ip.ipv4_mapped is not None:
                ip = ip.ipv4_mapped
            elif ip.sixtofour is not None:
                ip = ip.sixtofour
            elif ip.teredo is not None:
                ip = ip.teredo[1] if ip.teredo[0].is_global else ip.teredo[0]
        if (
            not ip.is_global
            or ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
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


_ALERT_PREVIEW_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}


def _resolve_ch_client(service_id: str) -> Any:
    hs_service = get_high_scale_service_registry().resolve(service_id)
    if hs_service is not None:
        return hs_service.client
    return get_clickhouse_client()


def _ch_metric_expr(metric: str, status_codes: list[int] | None = None) -> str:
    codes = [int(c) for c in (status_codes or []) if isinstance(c, int)]
    codes_sql = f"({', '.join(str(c) for c in codes)})" if codes else "(0)"
    status_col = "toInt32OrZero(custom_fields['status'])"
    if metric == "requests":
        return "count()"
    if metric == "5xx":
        return f"countIf({status_col} >= 500)"
    if metric == "5xx_rate":
        return f"if(count() = 0, 0.0, countIf({status_col} >= 500) * 100.0 / count())"
    if metric == "4xx":
        return f"countIf({status_col} >= 400 AND {status_col} <= 499)"
    if metric == "4xx_rate":
        return f"if(count() = 0, 0.0, countIf({status_col} >= 400 AND {status_col} <= 499) * 100.0 / count())"
    if metric == "specific_status":
        return f"countIf({status_col} IN {codes_sql})" if codes else "0"
    if metric == "specific_status_rate":
        return f"if(count() = 0, 0.0, countIf({status_col} IN {codes_sql}) * 100.0 / count())" if codes else "0.0"
    if metric == "latency_p95":
        return "coalesce(quantile(0.95)(toFloat64OrNull(custom_fields['elapsed'])) / 1000.0, 0.0)"
    if metric == "hit_rate":
        return "if(count() = 0, 0.0, countIf(startsWith(upper(custom_fields['cache']), 'HIT')) * 100.0 / count())"
    if metric in ("throughput", "bandwidth"):
        return "coalesce(sum(toFloat64OrZero(custom_fields['resp_bytes'])), 0.0)"
    if metric == "ttfb":
        return "coalesce(quantile(0.95)(toFloat64OrNull(custom_fields['ottfb'])) / 1000.0, 0.0)"
    if metric == "req_size":
        return (
            "coalesce(avg(toFloat64OrZero(custom_fields['req_bytes']) "
            "+ toFloat64OrZero(custom_fields['req_header_bytes'])), 0.0)"
        )
    return "count()"


def _ch_scope_clause(eval_scope: str) -> str:
    if eval_scope == "edge":
        return " AND lower(custom_fields['edge']) IN ('true', '1')"
    if eval_scope == "origin":
        return " AND lower(custom_fields['edge']) IN ('false', '0')"
    return ""


def _evaluate_alert(
    service_id: str,
    src: dict[str, Any],
    alert: dict[str, Any],
    display_name: str = "",
) -> tuple[bool, str | None, dict[str, Any] | None, str | None]:
    """Evaluate one alert rule against ClickHouse ``request_facts``."""
    from backend.utils.date_utils import ensure_utc, parse_iso_utc

    metric = str(alert.get("metric") or "requests")
    window = float(alert.get("window_min") or 5)
    eval_type = str(alert.get("evaluation_type") or "absolute")
    eval_scope = str(alert.get("evaluation_scope") or "all")
    comp_period = alert.get("comparison_period_min")
    status_codes = alert.get("status_codes")

    try:
        client = _resolve_ch_client(service_id)
        fence = visible_publication_fence_sql("request", client=client, service_id=service_id)
        max_rows = client.execute(
            f"SELECT max(event_timestamp) AS max_ts, count() AS cnt FROM request_facts "
            f"WHERE service_id={{service_id:String}} AND {fence}",
            {"service_id": service_id},
        )
        if not max_rows or not int(max_rows[0].get("cnt") or 0):
            return False, None, None, None
        raw_max_ts = max_rows[0].get("max_ts")
        if not raw_max_ts:
            return False, None, None, None
        max_ts_dt = parse_iso_utc(str(raw_max_ts)) if isinstance(raw_max_ts, str) else ensure_utc(raw_max_ts)
        if max_ts_dt is None:
            return False, None, None, None
        now_utc = datetime.now(UTC)
        if (now_utc - max_ts_dt) > timedelta(minutes=30):
            return False, None, None, None

        agg_expr = _ch_metric_expr(metric, status_codes)
        scope_clause = _ch_scope_clause(eval_scope)

        def _query_window(start_dt: datetime, end_dt: datetime) -> tuple[float, int]:
            rows = client.execute(
                f"SELECT {agg_expr} AS val, count() AS req_count FROM request_facts "
                f"WHERE service_id={{service_id:String}} AND {fence} "
                f"AND event_timestamp >= {{start:DateTime64(3)}} AND event_timestamp <= {{end:DateTime64(3)}}"
                f"{scope_clause}",
                {"service_id": service_id, "start": start_dt, "end": end_dt},
            )
            row = rows[0] if rows else {}
            return float(row.get("val") or 0.0), int(row.get("req_count") or 0)

        val, req_count = _query_window(max_ts_dt - timedelta(minutes=window), max_ts_dt)

        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
            if req_count < 10:
                return False, None, None, None
            hist_val, _ = _query_window(
                max_ts_dt - timedelta(minutes=float(comp_period) + window),
                max_ts_dt - timedelta(minutes=float(comp_period)),
            )
            if hist_val == 0:
                return False, None, None, None
            if eval_type == "relative_increase":
                val = ((val - hist_val) / hist_val) * 100.0
            else:
                val = ((hist_val - val) / hist_val) * 100.0
        elif eval_type == "anomaly_zscore":
            days = int(alert.get("baseline_period_days") or 7)
            zscore_thresh = float(alert.get("zscore_threshold") or 3.0)
            base_rows = client.execute(
                f"SELECT avg(val) AS mean_val, stddevSamp(val) AS stddev_val FROM ("
                f"SELECT toStartOfHour(event_timestamp) AS hr, {agg_expr} AS val FROM request_facts "
                f"WHERE service_id={{service_id:String}} AND {fence} "
                f"AND event_timestamp >= {{start:DateTime64(3)}} AND event_timestamp <= {{end:DateTime64(3)}} "
                f"AND toHour(event_timestamp) = {{target_hour:UInt8}}{scope_clause} GROUP BY hr)",
                {
                    "service_id": service_id,
                    "start": max_ts_dt - timedelta(days=days),
                    "end": max_ts_dt - timedelta(hours=1),
                    "target_hour": max_ts_dt.hour,
                },
            )
            base_row = base_rows[0] if base_rows else {}
            mean_val = base_row.get("mean_val")
            if mean_val is None:
                return False, None, None, None
            stddev_val = float(base_row.get("stddev_val") or 0.0001) or 0.0001
            val = (val - float(mean_val)) / stddev_val
            alert["operator"] = ">"
            alert["threshold"] = zscore_thresh
    except Exception as e:
        logger.error("Failed to evaluate alert %s: %s", alert.get("id"), e)
        return False, None, None, None

    op = str(alert.get("operator") or ">")
    thresh = float(alert.get("threshold") or 0.0)
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

                last = parse_iso_utc(str(alert["last_triggered_at"])) or datetime.fromtimestamp(0, tz=UTC)
                last = ensure_utc(last)
                if now_utc - last < timedelta(hours=1):
                    return False, None, None, None
            except ValueError:
                pass

        webhook_payload: dict[str, Any] | None = None
        if alert.get("webhook_url"):
            s_id = service_id or str(alert.get("service_id") or "")
            disp = display_name or str(src.get("name") or s_id)
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
        max_ts_str = max_ts_dt.isoformat()
        return True, alert.get("webhook_url"), webhook_payload, max_ts_str

    return False, None, None, None


def _alert_preview_cache_key(
    src: dict[str, Any],
    table_name: str,
    alert: Alert,
    lookback_hours: int,
    anchor_iso: str | None = None,
) -> tuple[Any, ...]:
    if anchor_iso is None and src:
        try:
            svc_status = svcconfig.get_status(src.get("name") or src.get("service_id") or "") or {}
            anchor_iso = str(svc_status.get("latest_log_at") or "")
        except Exception:
            anchor_iso = ""
    status_codes = getattr(alert, "status_codes", None)
    return (
        str(src.get("name") or src.get("service_id") or table_name),
        str(table_name),
        str(anchor_iso or ""),
        int(lookback_hours),
        str(alert.metric),
        float(alert.window_min or 0),
        str(getattr(alert, "evaluation_type", "absolute")),
        str(getattr(alert, "evaluation_scope", "all")),
        float(getattr(alert, "comparison_period_min", None) or 0),
        tuple(sorted(status_codes)) if status_codes else (),
        int(getattr(alert, "baseline_period_days", 7) or 7),
        float(getattr(alert, "zscore_threshold", 3.0) or 3.0),
    )


def get_cached_alert_preview(
    src: dict[str, Any],
    table_name: str,
    alert: Alert,
    lookback_hours: int,
) -> dict[str, Any] | None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return None
    key = _alert_preview_cache_key(src, table_name, alert, lookback_hours)
    cached = _ALERT_PREVIEW_CACHE.get(key)
    if cached is None:
        return None
    return {k: (list(v) if isinstance(v, list) else v) for k, v in cached.items()}


def _preview_alert_series(
    service_id: str,
    src: dict[str, Any],
    table_name: str,
    alert: Alert,
    lookback_hours: int,
) -> dict[str, Any]:
    """Build alert preview series against ClickHouse ``request_facts``."""
    from backend.utils.date_utils import ensure_utc, parse_iso_utc, safe_iso

    cached = get_cached_alert_preview(src, table_name, alert, lookback_hours)
    if cached is not None:
        return cached

    metric = str(alert.metric)
    window = float(alert.window_min or 5)
    eval_type = str(getattr(alert, "evaluation_type", "absolute") or "absolute")
    eval_scope = str(getattr(alert, "evaluation_scope", "all") or "all")
    comp_period = getattr(alert, "comparison_period_min", None)
    status_codes = getattr(alert, "status_codes", None)

    lookback_mins = int(lookback_hours * 60)
    if eval_type in ("relative_increase", "relative_decrease") and comp_period:
        lookback_mins = max(lookback_mins, int(comp_period + window + 60))

    if lookback_mins <= 180:
        bucket_expr = "toStartOfMinute(event_timestamp)"
    elif lookback_mins <= 1440:
        bucket_expr = "toStartOfFifteenMinutes(event_timestamp)"
    else:
        bucket_expr = "toStartOfHour(event_timestamp)"

    agg_expr = _ch_metric_expr(metric, status_codes)
    scope_clause = _ch_scope_clause(eval_scope)

    client = _resolve_ch_client(service_id)
    fence = visible_publication_fence_sql("request", client=client, service_id=service_id)

    latest_iso: str | None = None
    anchor_dt: datetime | None = None
    try:
        svc_status = svcconfig.get_status(service_id) or {}
        latest_iso = svc_status.get("latest_log_at")
        anchor_dt = parse_iso_utc(latest_iso) if latest_iso else None
    except Exception:
        anchor_dt = None

    if anchor_dt is None:
        max_rows = client.execute(
            f"SELECT max(event_timestamp) AS max_ts, count() AS cnt FROM request_facts "
            f"WHERE service_id={{service_id:String}} AND {fence}",
            {"service_id": service_id},
        )
        if max_rows and int(max_rows[0].get("cnt") or 0) > 0 and max_rows[0].get("max_ts"):
            raw_max = max_rows[0]["max_ts"]
            anchor_dt = parse_iso_utc(str(raw_max)) if isinstance(raw_max, str) else ensure_utc(raw_max)

    if anchor_dt is None:
        return {
            "times": [],
            "values": [],
            "type": "anomaly_zscore" if eval_type == "anomaly_zscore" else "absolute",
        }

    series_rows = client.execute(
        f"SELECT {bucket_expr} AS ts, {agg_expr} AS val FROM request_facts "
        f"WHERE service_id={{service_id:String}} AND {fence} "
        f"AND event_timestamp >= {{start:DateTime64(3)}} AND event_timestamp <= {{end:DateTime64(3)}}"
        f"{scope_clause} GROUP BY ts ORDER BY ts ASC",
        {
            "service_id": service_id,
            "start": anchor_dt - timedelta(minutes=lookback_mins),
            "end": anchor_dt,
        },
    )
    times = [safe_iso(r.get("ts")) for r in series_rows]
    values = [float(r.get("val") or 0.0) for r in series_rows]
    res: dict[str, Any] = {
        "times": times,
        "values": values,
        "type": "anomaly_zscore" if eval_type == "anomaly_zscore" else "absolute",
    }

    if eval_type == "anomaly_zscore":
        days = int(getattr(alert, "baseline_period_days", 7) or 7)
        zscore_thresh = float(getattr(alert, "zscore_threshold", 3.0) or 3.0)
        base_rows = client.execute(
            f"SELECT toHour(ts) AS hr, avg(val) AS mean_val, stddevSamp(val) AS stddev_val FROM ("
            f"SELECT toStartOfHour(event_timestamp) AS ts, {agg_expr} AS val FROM request_facts "
            f"WHERE service_id={{service_id:String}} AND {fence} "
            f"AND event_timestamp >= {{start:DateTime64(3)}} AND event_timestamp <= {{end:DateTime64(3)}}"
            f"{scope_clause} GROUP BY ts) GROUP BY hr",
            {
                "service_id": service_id,
                "start": anchor_dt - timedelta(days=days),
                "end": anchor_dt - timedelta(hours=1),
            },
        )
        baseline_map = {
            int(r.get("hr") or 0): (float(r.get("mean_val") or 0.0), float(r.get("stddev_val") or 0.0))
            for r in (base_rows or [])
        }
        baseline_means: list[float] = []
        baseline_thresholds: list[float] = []
        for r in series_rows:
            dt = r.get("ts")
            hr = dt.hour if (dt and hasattr(dt, "hour")) else 0
            mean_val, stddev_val = baseline_map.get(hr, (0.0, 0.0))
            stddev_val = stddev_val or 0.0001
            baseline_means.append(mean_val)
            baseline_thresholds.append(mean_val + zscore_thresh * stddev_val)
        res["hist_values"] = baseline_means
        res["threshold_values"] = baseline_thresholds

    if eval_type in ("relative_increase", "relative_decrease") and comp_period:
        res["type"] = "relative"
        hist_rows = client.execute(
            f"SELECT {bucket_expr} AS ts, {agg_expr} AS val FROM request_facts "
            f"WHERE service_id={{service_id:String}} AND {fence} "
            f"AND event_timestamp >= {{start:DateTime64(3)}} AND event_timestamp <= {{end:DateTime64(3)}}"
            f"{scope_clause} GROUP BY ts ORDER BY ts ASC",
            {
                "service_id": service_id,
                "start": anchor_dt - timedelta(minutes=lookback_mins + float(comp_period)),
                "end": anchor_dt - timedelta(minutes=float(comp_period)),
            },
        )
        hist_map: dict[str, float] = {}
        for r in hist_rows or []:
            ts_val = r.get("ts")
            if isinstance(ts_val, datetime):
                shifted = ts_val + timedelta(minutes=float(comp_period))
                hist_map[shifted.isoformat()] = float(r.get("val") or 0.0)
        res["hist_values"] = [hist_map.get(t or "", 0.0) for t in times]

    if not os.getenv("PYTEST_CURRENT_TEST"):
        key = _alert_preview_cache_key(src, table_name, alert, lookback_hours, anchor_iso=latest_iso)
        _ALERT_PREVIEW_CACHE[key] = {k: (list(v) if isinstance(v, list) else v) for k, v in res.items()}
    return res


@alerts_router.post("/evaluate", response_model=AlertEvaluateResponse)
def evaluate_alerts_endpoint(
    request: Request,
    body: AlertEvaluateRequest | None = Body(default=None),
    service_id: str | None = Depends(get_service_id),
) -> AlertEvaluateResponse:
    """Evaluate enabled alert rules on demand for the active service (Admin only)."""
    target_service_id = (body.service_id if body and body.service_id else None) or service_id
    target_service_id = require_service_id(target_service_id)

    require_service_in_scope(request, target_service_id)
    _require_admin_mutation(request)

    alerts = repo.get_alerts(target_service_id)
    if body and body.alert_id:
        alerts = [a for a in alerts if a.get("id") == body.alert_id]
    else:
        alerts = [a for a in alerts if a.get("enabled", True)]

    src = get_source_for_service(target_service_id) if alerts else None
    results: list[AlertEvaluateResultItem] = []
    triggered_count = 0

    if alerts and src:
        for a in alerts:
            triggered = False
            triggered_ts: str | None = a.get("last_triggered_at")
            trig, _wh_url, _payload, max_ts_str = _evaluate_alert(
                target_service_id,
                src,
                dict(a),
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
    else:
        for a in alerts:
            results.append(
                AlertEvaluateResultItem(
                    id=str(a.get("id") or ""),
                    name=str(a.get("name") or ""),
                    metric=str(a.get("metric") or ""),
                    triggered=False,
                    last_triggered_at=a.get("last_triggered_at"),
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
) -> AlertPreviewResponse:
    require_service_in_scope(request, alert.service_id)

    src = get_source_for_service(alert.service_id)
    if not src:
        raise HTTPException(status_code=404, detail=not_found("Service not found"))

    table_name = _safe_table_name(src["name"])

    try:
        cached_res = get_cached_alert_preview(src, table_name, alert, lookback_hours)
        if cached_res is not None:
            cached_resp: AlertPreviewResponse = AlertPreviewResponse.with_telemetry(data=cached_res)
            return cached_resp
        res = _preview_alert_series(alert.service_id, src, table_name, alert, lookback_hours)
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
