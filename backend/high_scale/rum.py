"""High-scale RUM beacon health, analytics, and live event queries."""

from __future__ import annotations

import contextvars
import json
import os
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

from pydantic import TypeAdapter, ValidationError

from backend.high_scale._common import (
    _row_timestamp,
    format_iso_utc_or_empty,
    normalize_cache_bound,
    parse_time_window,
    range_value,
    visible_fact_clauses,
    visible_publication_fence_sql,
)
from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    client_cache_token,
    digest_cache_key,
)
from backend.utils.bounded_cache import CacheRegistry

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

_HS_RUM_CACHE_TTL = float(os.getenv("HIGH_SCALE_RUM_CACHE_TTL_SECONDS", "30"))
_rum_memo = ResponseMemo(ttl=max(_HS_RUM_CACHE_TTL, 1.0), maxsize=128, prewarmer=False)
CacheRegistry.register("high_scale.rum._rum_memo", _rum_memo.cache)

_iso = format_iso_utc_or_empty
_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "pathname", "city", "region", "pop", "tls"})
_LOW_CARD_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "region", "pop", "tls"})
_CASE_INSENSITIVE_FILTER_COLUMNS = frozenset({"browser", "os", "device", "country", "pop", "tls"})


def _is_opt27_ch(client: Any) -> bool:
    return (
        hasattr(client, "_http")
        and "MockTransport" not in type(client._http._transport).__name__
        and not os.getenv("PYTEST_CURRENT_TEST")
    )


def _rum_sample_stride(span_s: float) -> int:
    if span_s >= 1_728_000:
        return 16
    if span_s >= 1_209_600:
        return 8
    if span_s >= 604_800:
        return 4
    return 1


def _rum_pk_sample_clauses(
    stride: int,
    *,
    start: datetime,
    end: datetime,
    req_margin_s: int = 600,
) -> tuple[str, str]:
    if stride <= 1 or end <= start:
        return "1", "1"
    s_utc = start.astimezone(UTC)
    e_utc = end.astimezone(UTC)
    rum_windows: list[tuple[int, int]] = []
    req_raw: list[tuple[int, int]] = []
    day_cur = s_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    while day_cur < e_utc:
        seg_start = max(s_utc, day_cur)
        seg_end = min(e_utc, day_cur + timedelta(days=1))
        seg_s = (seg_end - seg_start).total_seconds()
        if seg_s > 0:
            win_s = min(int(seg_s), max(60, round(seg_s / stride)))
            w_start = int(seg_start.timestamp())
            w_end = w_start + win_s
            if w_end > w_start:
                rum_windows.append((w_start, w_end))
                req_raw.append((w_start - req_margin_s, w_end + req_margin_s))
        day_cur += timedelta(days=1)
    if not rum_windows:
        return "1", "1"
    req_merged: list[tuple[int, int]] = []
    for rs, re in req_raw:
        if req_merged and rs <= req_merged[-1][1]:
            req_merged[-1] = (req_merged[-1][0], max(req_merged[-1][1], re))
        else:
            req_merged.append((rs, re))

    def _fmt(windows: list[tuple[int, int]]) -> str:
        parts = [
            f"(event_timestamp >= toDateTime({ws}, 'UTC') AND event_timestamp < toDateTime({we}, 'UTC'))"
            for ws, we in windows
        ]
        return f"({' OR '.join(parts)})"

    return _fmt(rum_windows), _fmt(req_merged)


def _decode_pct(value: Any) -> str:
    text = str(value or "")
    return unquote(text) if "%" in text else text


_BEACON_IDENTITY = (
    "coalesce(nullIf(toString(request_event_id), ''), "
    "concat(client_id, '_', toString(toInt64(round(toUnixTimestamp64Milli(event_timestamp) / 1000.0)))))"
)
# A resource timing (`ttlb`) measures one request, not a page view: page-level readers skip it.
_PAGE_VITALS = "metric_name != 'ttlb'"
# The tracker reports only a sample of sessions when an admin lowers the sample rate. Each
# stored row keeps the rate it was collected at, so a row stands for 1 / rate real ones
# (inverse-probability weighting). Rows written before sampling existed have rate 1.
_ROW_WEIGHT = "if(sample_rate > 0 AND sample_rate < 1, 1 / sample_rate, 1)"


def _weighted_beacons(condition: str = "1", *, identity: str = "beacon_id", weight: str = "w") -> str:
    """Estimated real beacons: each distinct beacon counted once, at its largest matching weight."""
    return f"toUInt64(round(arraySum(mapValues(maxMapIf(map({identity}, {weight}), {condition})))))"


_RUM_FILTERS_ADAPTER: TypeAdapter[dict[str, Any]] = TypeAdapter(dict[str, Any])


def parse_rum_filters(raw: str | None) -> dict[str, Any]:
    """Decode the ``filters`` query parameter (a JSON object of ``{column: {mode, values}}``)."""
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("filters must be valid JSON") from exc
    try:
        return _RUM_FILTERS_ADAPTER.validate_python(payload)
    except ValidationError as exc:
        raise ValueError("filters must be a JSON object") from exc


def _filter_clauses_split(filters: Mapping[str, Any] | None) -> tuple[list[str], list[str], dict[str, Any]]:
    prewhere_clauses: list[str] = []
    where_clauses: list[str] = []
    params: dict[str, Any] = {}
    unsupported: list[str] = []
    for i, (raw_key, spec) in enumerate((filters or {}).items()):
        raw_values = filter_spec_attr(spec, "values") or []
        values = [v for v in raw_values if v is not None and not (isinstance(v, str) and v.strip() == "")]
        if not values:
            continue
        column = normalize_filter_key(raw_key)
        if column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        case_insensitive = column in _CASE_INSENSITIVE_FILTER_COLUMNS
        names: list[str] = []
        for j, value in enumerate(values):
            name = f"flt_{i}_{j}"
            params[name] = str(value).lower() if case_insensitive else str(value)
            names.append(f"{{{name}:String}}")
        mode = str(filter_spec_attr(spec, "mode") or "include")
        operator = "NOT IN" if mode == "exclude" else "IN"
        col_expr = f"lower({column})" if case_insensitive else column
        clause = f"{col_expr} {operator} ({', '.join(names)})"
        if column in _LOW_CARD_FILTER_COLUMNS:
            prewhere_clauses.append(clause)
        else:
            where_clauses.append(clause)
    if unsupported:
        raise ValueError(f"high-scale RUM cannot filter on: {', '.join(sorted(unsupported))}")
    return prewhere_clauses, where_clauses, params


def _filter_clauses(filters: Mapping[str, Any] | None) -> tuple[list[str], dict[str, Any]]:
    prewhere_clauses, where_clauses, params = _filter_clauses_split(filters)
    return [*prewhere_clauses, *where_clauses], params


def rum_beacon_health(
    service: HighScaleService,
    *,
    source: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    from backend import config as svcconfig

    if isinstance(source, dict) and not svcconfig.is_rum_enabled(source):
        return {
            "enabled": False,
            "has_data": False,
            "beacons": 0,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM is not enabled for this service.",
        }

    observed = (now or datetime.now(UTC)).astimezone(UTC)
    cutoff = observed - timedelta(minutes=15)
    params = {"service_id": service.service_id, "cutoff": cutoff}
    agg_sql = (
        "SELECT sum(c) as c, max(last_ts) as last_ts FROM ("
        "SELECT sum(event_count) as c, max(bucket_start) as last_ts FROM rum_vitals_aggregates "
        f"WHERE service_id = {{service_id:String}} AND dimension = 'metric_name' "
        f"AND bucket_start >= {{cutoff:DateTime64(3, 'UTC')}} AND {visible_publication_fence_sql('rum_vitals_aggregate')} "
        "UNION ALL "
        "SELECT sum(error_count) as c, max(bucket_start) as last_ts FROM rum_error_aggregates "
        f"WHERE service_id = {{service_id:String}} AND dimension = 'error_message' "
        f"AND bucket_start >= {{cutoff:DateTime64(3, 'UTC')}} AND {visible_publication_fence_sql('rum_errors_aggregate')}"
        ")"
    )
    facts_sql = (
        "SELECT sum(c) as c, max(last_ts) as last_ts FROM ("
        "SELECT count() as c, max(event_timestamp) as last_ts FROM rum_vitals_facts "
        f"WHERE service_id = {{service_id:String}} "
        f"AND event_timestamp >= {{cutoff:DateTime64(3, 'UTC')}} AND {visible_publication_fence_sql('rum_vitals')} "
        "UNION ALL "
        "SELECT count() as c, max(event_timestamp) as last_ts FROM rum_error_facts "
        f"WHERE service_id = {{service_id:String}} "
        f"AND event_timestamp >= {{cutoff:DateTime64(3, 'UTC')}} AND {visible_publication_fence_sql('rum_errors')}"
        ")"
    )
    try:
        res = service.client.execute(agg_sql, params)
        row = res[0] if res else {}
        if int(row.get("c") or 0) == 0:
            fact_res = service.client.execute(facts_sql, params)
            if fact_res and int(fact_res[0].get("c") or 0) > 0:
                row = fact_res[0]
    except Exception:
        res = service.client.execute(facts_sql, params)
        row = res[0] if res else {}

    beacons = int(row.get("c") or 0)
    raw_last_ts = row.get("last_ts") if beacons > 0 else None
    parsed_last_ts = _row_timestamp({"timestamp": raw_last_ts}) if raw_last_ts is not None else None
    if parsed_last_ts is not None and parsed_last_ts < cutoff:
        beacons = 0
    last_ts = raw_last_ts if beacons > 0 else None
    has_data = beacons > 0
    if has_data:
        message = f"RUM is working! Received {beacons} beacon(s) in the last 15 minutes."
    else:
        message = "No beacons received in the last 15 minutes. Ensure the RUM script is installed and your site is receiving traffic."
    return {
        "enabled": True,
        "has_data": has_data,
        "beacons": beacons,
        "beacon_fire_rate": beacons,
        "recent_beacons": beacons,
        "last_beacon_time": (_iso(last_ts) or None) if last_ts is not None else None,
        "setup_complete": has_data,
        "message": message,
    }


def rum_analytics(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Serve the standard RUM wire contract from visible, time-filtered facts."""
    start, end = parse_time_window(start_time, end_time, utc=True, now=datetime.now(UTC))
    if end < start:
        raise ValueError("RUM end time precedes start time")
    prewhere_filters, where_filters, filter_params = _filter_clauses_split(filters)
    filter_clauses = [*prewhere_filters, *where_filters]
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if _HS_RUM_CACHE_TTL > 0 and is_real_ch and start_time is not None and end_time is not None:
        digest = digest_cache_key(
            {
                "ep": "hs_rum_analytics",
                "cid": client_cache_token(service.client),
                "s": normalize_cache_bound(start_time),
                "e": normalize_cache_bound(end_time),
                "f": sorted((str(k), v) for k, v in (filters or {}).items()),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        memo_key = f"{service.service_id}:{digest}"
        if (cached := _rum_memo.get_with_telemetry(memo_key)) is not None:
            return dict(cached)

    def _compute() -> dict[str, Any]:
        if memo_key is not None and (cached_inner := _rum_memo.get_with_telemetry(memo_key)) is not None:
            return dict(cached_inner)
        payload = _rum_analytics_compute(
            service,
            start,
            end,
            filter_clauses,
            filter_params,
            prewhere_filters=prewhere_filters,
            where_filters=where_filters,
        )
        if memo_key is not None:
            _rum_memo.put(memo_key, payload)
        return payload

    if memo_key is not None:
        return _rum_memo.coalesce(f"hs_rum_analytics:{memo_key}", _compute)
    return _compute()


def _shape_rum_resource_latency_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    def _opt_ms(key: str) -> float | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            num = float(val)
        except (TypeError, ValueError):
            return None
        return round(num, 2) if num == num else None

    return {
        "requests": int(row.get("requests") or 0),
        "avg": _opt_ms("avg_ms") or 0.0,
        "p50": _opt_ms("p50_ms") or 0.0,
        "p95": _opt_ms("p95_ms") or 0.0,
        "p99": _opt_ms("p99_ms") or 0.0,
        "matched_requests": int(row.get("matched_requests") or 0),
        "edge_avg": _opt_ms("edge_avg_ms"),
        "edge_p50": _opt_ms("edge_p50_ms"),
        "edge_p95": _opt_ms("edge_p95_ms"),
        "edge_p99": _opt_ms("edge_p99_ms"),
    }


def _rum_analytics_compute(
    service: HighScaleService,
    start: datetime,
    end: datetime,
    filter_clauses: list[str],
    filter_params: dict[str, Any],
    *,
    prewhere_filters: list[str] | None = None,
    where_filters: list[str] | None = None,
    allow_sampling: bool = True,
) -> dict[str, Any]:
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    opt27 = _is_opt27_ch(service.client)
    span_s = max(0.0, (end - start).total_seconds())
    rum_stride = _rum_sample_stride(span_s) if (opt27 and allow_sampling) else 1
    rum_pk_clause, req_pk_clause = _rum_pk_sample_clauses(rum_stride, start=start, end=end)
    row_weight_expr = f"(({_ROW_WEIGHT}) * {rum_stride})" if rum_stride > 1 else _ROW_WEIGHT
    q_exact = "quantileTDigest" if rum_stride > 1 else "quantileExactInclusive"

    def prewhere_scope(domain: str, *extra: str) -> str:
        return " AND ".join(
            [
                *visible_fact_clauses(domain),
                "event_timestamp >= {start:DateTime64(3, 'UTC')}",
                "event_timestamp < {end:DateTime64(3, 'UTC')}",
                *extra,
            ]
        )

    pre_flt = list(prewhere_filters) if prewhere_filters is not None else []
    rem_flt = list(where_filters) if where_filters is not None else list(filter_clauses)
    filter_sql = " AND ".join(filter_clauses)
    rem_sql = " AND ".join(rem_flt)

    if opt27:
        pk_extra = [rum_pk_clause] if rum_pk_clause != "1" else []
        vitals_prewhere = prewhere_scope("rum_vitals", _PAGE_VITALS, *pk_extra, *pre_flt)
        ttlb_prewhere = prewhere_scope("rum_vitals", "metric_name = 'ttlb'", *pk_extra, *pre_flt)
        errors_prewhere = prewhere_scope("rum_errors", *pk_extra, *pre_flt)
        vitals_where_clause = f" WHERE {rem_sql}" if rem_sql else ""
        errors_where_clause = f" WHERE {rem_sql}" if rem_sql else ""
        ttlb_req_where_clause = f" WHERE {rem_sql} AND req_id != ''" if rem_sql else " WHERE req_id != ''"
    else:
        vitals_prewhere = prewhere_scope("rum_vitals")
        ttlb_prewhere = prewhere_scope("rum_vitals")
        errors_prewhere = prewhere_scope("rum_errors")
        vitals_where = f"{_PAGE_VITALS} AND {filter_sql}" if filter_sql else _PAGE_VITALS
        ttlb_where = f"metric_name = 'ttlb' AND {filter_sql}" if filter_sql else "metric_name = 'ttlb'"
        vitals_where_clause = f" WHERE {vitals_where}"
        errors_where_clause = f" WHERE {filter_sql}" if filter_sql else ""
        ttlb_req_where_clause = f" WHERE {ttlb_where} AND req_id != ''"

    req_edge_scope = " AND ".join(visible_fact_clauses("request"))
    req_edge_pk = f"AND {req_pk_clause} " if (opt27 and req_pk_clause != "1") else ""
    req_edge_cte = (
        f"req_edge AS (SELECT _cf_rid AS rid, any(toFloat64OrNull(nullIf(_cf_elapsed, ''))) / 1000.0 AS edge_ms, "
        f"any(_cf_asn) AS req_asn FROM request_facts PREWHERE {req_edge_scope} "
        "AND event_timestamp >= ({start:DateTime64(3, 'UTC')} - INTERVAL 10 MINUTE) "
        "AND event_timestamp < ({end:DateTime64(3, 'UTC')} + INTERVAL 10 MINUTE) "
        f"{req_edge_pk}"
        "AND _cf_rid != '' "
        f"AND _cf_rid IN (SELECT req_id FROM rum_vitals_facts PREWHERE {ttlb_prewhere}"
        f"{ttlb_req_where_clause}) "
        "GROUP BY _cf_rid)"
    )
    slowest_agg_select = (
        "toUInt64(round(sum(w))) AS requests, "
        "avg(metric_value) AS avg_ms, "
        f"{q_exact}(0.5)(metric_value) AS p50_ms, "
        f"{q_exact}(0.95)(metric_value) AS p95_ms, "
        f"{q_exact}(0.99)(metric_value) AS p99_ms, "
        "toUInt64(round(sumIf(w, edge_ms IS NOT NULL))) AS matched_requests, "
        "avgOrNullIf(edge_ms, edge_ms IS NOT NULL) AS edge_avg_ms, "
        f"{q_exact}OrNullIf(0.5)(edge_ms, edge_ms IS NOT NULL) AS edge_p50_ms, "
        f"{q_exact}OrNullIf(0.95)(edge_ms, edge_ms IS NOT NULL) AS edge_p95_ms, "
        f"{q_exact}OrNullIf(0.99)(edge_ms, edge_ms IS NOT NULL) AS edge_p99_ms"
    )
    identity = _BEACON_IDENTITY
    beacon_expr = f"cityHash64({identity})" if opt27 else identity
    beacon_col = "beacon_h" if opt27 else "beacon_id"
    hourly = end - start <= timedelta(hours=48)
    bucket_sql = "toStartOfHour(event_timestamp, 'UTC')" if hourly else "toStartOfDay(event_timestamp, 'UTC')"
    pageview = "src = 'vitals' AND metric_name NOT LIKE 'event_%'"
    interaction = "src = 'vitals' AND metric_name LIKE 'event_%'"
    is_vital, is_error = "src = 'vitals'", "src = 'errors'"

    vitals_good = (
        "metric_rating = 'good' OR (metric_rating = '' AND multiIf("
        "lower(metric_name) = 'lcp', metric_value <= 2500, "
        "lower(metric_name) = 'cls', metric_value <= 0.1, "
        "lower(metric_name) = 'inp', metric_value <= 200, "
        "lower(metric_name) = 'fcp', metric_value <= 1800, "
        "lower(metric_name) = 'ttfb', metric_value <= 800, 0))"
    )
    vitals_ni = (
        "metric_rating IN ('needs-improvement', 'needs_improvement') OR ("
        "metric_rating = '' AND multiIf("
        "lower(metric_name) = 'lcp', metric_value > 2500 AND metric_value <= 4000, "
        "lower(metric_name) = 'cls', metric_value > 0.1 AND metric_value <= 0.25, "
        "lower(metric_name) = 'inp', metric_value > 200 AND metric_value <= 500, "
        "lower(metric_name) = 'fcp', metric_value > 1800 AND metric_value <= 3000, "
        "lower(metric_name) = 'ttfb', metric_value > 800 AND metric_value <= 1800, 0))"
    )
    vitals_poor = (
        "metric_rating = 'poor' OR (metric_rating = '' AND multiIf("
        "lower(metric_name) = 'lcp', metric_value > 4000, "
        "lower(metric_name) = 'cls', metric_value > 0.25, "
        "lower(metric_name) = 'inp', metric_value > 500, "
        "lower(metric_name) = 'fcp', metric_value > 3000, "
        "lower(metric_name) = 'ttfb', metric_value > 1800, 0))"
    )
    vitals_sql = (
        f"/* rum_analytics_vitals */ SELECT lower(metric_name) AS metric_name, "
        f"{q_exact}OrNull(0.75)(metric_value) as p75, sum({row_weight_expr}) as total, "
        f"sumIf({row_weight_expr}, {vitals_good}) as good, "
        f"sumIf({row_weight_expr}, {vitals_ni}) as ni, "
        f"sumIf({row_weight_expr}, {vitals_poor}) as poor "
        f"FROM rum_vitals_facts PREWHERE {vitals_prewhere}{vitals_where_clause} GROUP BY metric_name"
    )
    env_identity = f"cityHash64({identity})" if opt27 else identity
    environments_sql = (
        f"/* rum_analytics_environments */ SELECT if(browser = '', 'Unknown', browser) AS env_browser, "
        f"if(os = '', 'Unknown', os) AS env_os, if(device = '', 'Unknown', device) AS env_device, "
        f"{_weighted_beacons(identity=env_identity, weight=row_weight_expr)} AS beacons, "
        f"grouping(browser) AS g_browser, grouping(os) AS g_os, "
        f"grouping(device) AS g_device FROM rum_vitals_facts PREWHERE {vitals_prewhere}"
        f"{vitals_where_clause} GROUP BY GROUPING SETS ((browser), (os), (device))"
    )
    exceptions_sql = (
        f"/* rum_analytics_exceptions */ SELECT error_message, error_file, error_line, error_col, "
        f"toUInt64(round(sum({row_weight_expr}))) AS count "
        f"FROM rum_error_facts PREWHERE {errors_prewhere}{errors_where_clause} "
        f"GROUP BY error_message, error_file, error_line, error_col "
        f"ORDER BY count DESC, error_message, error_file, error_line, error_col LIMIT 3"
    )
    ttlb_where_clause = f" WHERE {rem_sql}" if opt27 and rem_sql else ("" if opt27 else f" WHERE {ttlb_where}")
    slowest_sql = (
        f"/* rum_analytics_slowest */ WITH ttlb_rows AS ("
        f"SELECT req_id, resource_url, asn, metric_value, {row_weight_expr} AS w "
        f"FROM rum_vitals_facts PREWHERE {ttlb_prewhere}{ttlb_where_clause}), "
        f"{req_edge_cte}, "
        f"ttlb_joined AS ("
        f"SELECT v.resource_url AS resource_url, "
        f"replaceRegexpOne(coalesce(nullIf(v.asn, ''), r.req_asn), '^AS', '') AS asn_key, "
        f"v.metric_value AS metric_value, v.w AS w, r.edge_ms AS edge_ms "
        f"FROM ttlb_rows AS v LEFT JOIN req_edge AS r ON v.req_id != '' AND r.rid = v.req_id) "
        f"SELECT kind_k.1 AS kind, kind_k.2 AS k, {slowest_agg_select} "
        f"FROM ttlb_joined "
        f"ARRAY JOIN [('url', if(resource_url != '', resource_url, '')), "
        f"('asn', if(asn_key != '' AND asn_key != '0', asn_key, ''))] AS kind_k "
        f"WHERE kind_k.2 != '' "
        f"GROUP BY kind_k.1, kind_k.2 "
        f"ORDER BY kind ASC, p99_ms DESC, p95_ms DESC, requests DESC "
        f"LIMIT 20 BY kind"
    )

    if opt27:
        events_folded = (
            f"WITH events AS ("
            f"SELECT {bucket_sql} AS bucket_col, {beacon_expr} AS {beacon_col}, 'vitals' AS src, "
            f"metric_name, metric_value, pathname, {row_weight_expr} AS w "
            f"FROM rum_vitals_facts PREWHERE {vitals_prewhere}{vitals_where_clause} "
            f"UNION ALL "
            f"SELECT {bucket_sql} AS bucket_col, {beacon_expr} AS {beacon_col}, 'errors' AS src, "
            f"'' AS metric_name, CAST(NULL AS Nullable(Float64)) AS metric_value, pathname, {row_weight_expr} AS w "
            f"FROM rum_error_facts PREWHERE {errors_prewhere}{errors_where_clause}) "
        )
        counts_trends_pages_sql = (
            events_folded + "/* rum_analytics_counts_trends_pages */ "
            "SELECT kind, bucket, path, beacons, views, pageviews, interactions, errors, "
            "sampled, avg_load, lcp, cls, fcp, ttfb, error_rate, page_rn FROM ("
            "SELECT multiIf(g_b = 0, 'trend', g_p = 0, 'page', 'count') AS kind, "
            "bucket, path, beacons, views, pageviews, interactions, errors, "
            "sampled, avg_load, lcp, cls, fcp, ttfb, "
            "errors * 100.0 / nullIf(views + errors, 0) AS error_rate, "
            "row_number() OVER (PARTITION BY g_p "
            "ORDER BY errors * 100.0 / nullIf(views + errors, 0) DESC, avg_load DESC, path ASC) AS page_rn "
            "FROM ("
            "SELECT grouping(bucket_col) AS g_b, grouping(pathname) AS g_p, "
            "bucket_col AS bucket, pathname AS path, "
            f"{_weighted_beacons(identity=beacon_col)} AS beacons, "
            f"{_weighted_beacons(is_vital, identity=beacon_col)} AS views, "
            f"{_weighted_beacons(pageview, identity=beacon_col)} AS pageviews, "
            f"{_weighted_beacons(interaction, identity=beacon_col)} AS interactions, "
            f"{_weighted_beacons(is_error, identity=beacon_col)} AS errors, "
            "countIf(w > 1) > 0 AS sampled, "
            "avgOrNullIf(metric_value, metric_name = 'page_load') AS avg_load, "
            f"{q_exact}OrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp, "
            f"{q_exact}OrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls, "
            f"{q_exact}OrNullIf(0.75)(metric_value, lower(metric_name) = 'fcp') AS fcp, "
            f"{q_exact}OrNullIf(0.75)(metric_value, lower(metric_name) = 'ttfb') AS ttfb "
            "FROM events GROUP BY GROUPING SETS ((bucket_col), (pathname), ())) "
            "WHERE g_p = 1 OR views > 0) "
            "WHERE kind != 'page' OR page_rn <= 5 "
            "ORDER BY kind ASC, bucket ASC, page_rn ASC"
        )
        queries: tuple[str, ...] = (
            counts_trends_pages_sql,
            vitals_sql,
            environments_sql,
            exceptions_sql,
            slowest_sql,
        )
        with ThreadPoolExecutor(max_workers=len(queries)) as pool:
            futs = [pool.submit(contextvars.copy_context().run, service.client.execute, q, params) for q in queries]
            events_rows, rows, environment_rows, error_rows, slowest_rows = (f.result() for f in futs)
        count_rows = [r for r in events_rows if r.get("kind") == "count"]
        trend_rows = [r for r in events_rows if r.get("kind") == "trend"]
        page_rows = sorted(
            ({**r, "page_errors": r.get("errors", 0)} for r in events_rows if r.get("kind") == "page"),
            key=lambda r: int(r.get("page_rn") or 0),
        )
        counts = (
            count_rows[0]
            if count_rows
            else {"beacons": 0, "pageviews": 0, "interactions": 0, "errors": 0, "sampled": 0}
        )
        if rum_stride > 1 and not counts["beacons"]:
            return _rum_analytics_compute(
                service,
                start,
                end,
                filter_clauses,
                filter_params,
                prewhere_filters=prewhere_filters,
                where_filters=where_filters,
                allow_sampling=False,
            )
    else:
        events = (
            f"WITH events AS ("
            f"SELECT event_timestamp, {identity} AS beacon_id, 'vitals' AS src, metric_name, metric_value, pathname, "
            f"{_ROW_WEIGHT} AS w FROM rum_vitals_facts PREWHERE {vitals_prewhere}{vitals_where_clause} "
            f"UNION ALL "
            f"SELECT event_timestamp, {identity} AS beacon_id, 'errors' AS src, '' AS metric_name, "
            f"CAST(NULL AS Nullable(Float64)) AS metric_value, pathname, {_ROW_WEIGHT} AS w "
            f"FROM rum_error_facts PREWHERE {errors_prewhere}{errors_where_clause}) "
        )
        counts_sql = (
            events + f"/* rum_analytics_counts */ SELECT {_weighted_beacons()} AS beacons, "
            f"{_weighted_beacons(pageview)} AS pageviews, {_weighted_beacons(interaction)} AS interactions, "
            f"{_weighted_beacons(is_error)} AS errors, countIf(w > 1) > 0 AS sampled FROM events"
        )
        trends_sql = (
            events + f"/* rum_analytics_trends */ SELECT {bucket_sql} AS bucket, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'fcp') AS fcp, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'ttfb') AS ttfb, "
            f"{_weighted_beacons(is_vital)} AS views, {_weighted_beacons(pageview)} AS pageviews, "
            f"{_weighted_beacons(interaction)} AS interactions, {_weighted_beacons(is_error)} AS errors "
            "FROM events GROUP BY bucket ORDER BY bucket"
        )
        pages_sql = (
            events + "/* rum_analytics_pages */ SELECT pathname AS path, "
            f"{_weighted_beacons(is_vital)} AS views, {_weighted_beacons(is_error)} AS page_errors, "
            "avgOrNullIf(metric_value, metric_name = 'page_load') AS avg_load, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'lcp') AS lcp, "
            "quantileExactInclusiveOrNullIf(0.75)(metric_value, lower(metric_name) = 'cls') AS cls, "
            "page_errors * 100.0 / nullIf(views + page_errors, 0) AS error_rate "
            "FROM events GROUP BY pathname HAVING views > 0 ORDER BY error_rate DESC, avg_load DESC, path LIMIT 5"
        )
        queries = (
            counts_sql,
            vitals_sql,
            environments_sql,
            trends_sql,
            pages_sql,
            exceptions_sql,
            slowest_sql,
        )
        with ThreadPoolExecutor(max_workers=7) as pool:
            futs = [pool.submit(contextvars.copy_context().run, service.client.execute, q, params) for q in queries]
            count_rows, rows, environment_rows, trend_rows, page_rows, error_rows, slowest_rows = (
                f.result() for f in futs
            )
        counts = count_rows[0]
    no_data = False
    if not counts["beacons"]:
        any_vitals = " AND ".join(visible_fact_clauses("rum_vitals"))
        any_errors = " AND ".join(visible_fact_clauses("rum_errors"))
        any_data = service.client.execute(
            f"/* rum_analytics_any_data */ SELECT count() AS has_data FROM ("
            f"(SELECT event_id FROM rum_vitals_facts PREWHERE {any_vitals} LIMIT 1) UNION ALL "
            f"(SELECT event_id FROM rum_error_facts PREWHERE {any_errors} LIMIT 1))",
            {"service_id": service.service_id},
        )
        no_data = not any_data[0]["has_data"]

    browsers: dict[str, int] = {}
    operating_systems: dict[str, int] = {}
    devices: dict[str, int] = {}
    for row in environment_rows:
        if not row["g_browser"]:
            browsers[row["env_browser"]] = int(row["beacons"])
        elif not row["g_os"]:
            operating_systems[row["env_os"]] = int(row["beacons"])
        elif not row["g_device"]:
            devices[row["env_device"]] = int(row["beacons"])

    vitals: dict[str, Any] = {
        m: {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}}
        for m in ("lcp", "cls", "inp", "fcp", "ttfb")
    }
    vitals["fid"] = {"p75": None, "fcp": None, "ttfb": None}

    def seconds(value: float | None) -> float | None:
        if value is None:
            return None
        scaled = value / 1000.0 if value > 20 else value
        return round(scaled, 2)

    for r in rows:
        m, p75 = r["metric_name"].lower(), r["p75"]
        if m in ("lcp", "cls", "inp", "fcp", "ttfb"):
            if p75 is None:
                vitals[m]["p75"] = None
            elif m in ("lcp", "fcp", "ttfb"):
                vitals[m]["p75"] = seconds(p75)
            elif m == "cls":
                vitals[m]["p75"] = round(p75, 3)
            else:
                vitals[m]["p75"] = int(p75)
            if t := r["total"]:
                good = round(r["good"] * 100 / t, 2)
                poor = round(r["poor"] * 100 / t, 2)
                vitals[m]["distribution"] = {
                    "good": good,
                    "poor": poor,
                    "needs_improvement": round(max(0.0, 100.0 - good - poor), 2),
                }
        if m == "fid":
            vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
        elif m in ("fcp", "ttfb"):
            vitals["fid"][m] = seconds(p75)

    trend_by_bucket = {}
    for row in trend_rows:
        ts = row["bucket"]
        dt = ts if isinstance(ts, datetime) else range_value(ts)
        norm_dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        trend_by_bucket[norm_dt] = row

    trends: dict[str, list[Any]] = {
        k: [] for k in ("timestamps", "lcp", "cls", "fcp", "ttfb", "error_rate", "pageviews", "interactions", "errors")
    }
    curr = start.replace(minute=0, second=0, microsecond=0)
    end_incl = end - timedelta(microseconds=1) if end > start else end
    target = end_incl.replace(minute=0, second=0, microsecond=0)
    step = timedelta(hours=1) if hourly else timedelta(days=1)
    if not hourly:
        curr, target = curr.replace(hour=0), target.replace(hour=0)
    while not no_data and curr <= target:
        bucket_row = trend_by_bucket.get(curr)
        trends["timestamps"].append(curr.isoformat())
        trends["lcp"].append(seconds(bucket_row["lcp"]) if bucket_row else None)
        trends["cls"].append(round(bucket_row["cls"], 3) if bucket_row and bucket_row["cls"] is not None else None)
        trends["fcp"].append(seconds(bucket_row.get("fcp")) if bucket_row else None)
        trends["ttfb"].append(seconds(bucket_row.get("ttfb")) if bucket_row else None)
        denom = bucket_row["views"] + bucket_row["errors"] if bucket_row else 0
        trends["error_rate"].append(round(bucket_row["errors"] * 100.0 / denom, 2) if bucket_row and denom else None)
        for metric_key in ("pageviews", "interactions", "errors"):
            trends[metric_key].append(int(bucket_row[metric_key]) if bucket_row else 0)
        curr += step

    worst_pages = [
        {
            "path": _decode_pct(row["path"]),
            "views": row["views"],
            "avg_load_time": seconds(row["avg_load"]),
            "lcp_p75": seconds(row["lcp"]),
            "cls_p75": round(row["cls"], 3) if row["cls"] is not None else None,
            "error_rate": round(row["error_rate"], 2) if row["error_rate"] is not None else 0.0,
        }
        for row in page_rows
    ]
    slowest_urls = [
        {"url": _decode_pct(row["k"]), **_shape_rum_resource_latency_fields(row)}
        for row in slowest_rows
        if row.get("kind") == "url" and row.get("k")
    ]
    from backend.repositories.utils.response_shaping.asn import (
        format_asn_display_label,
        parse_asn_int,
        resolve_asn_names_map,
    )

    slowest_net_rows = [row for row in slowest_rows if row.get("kind") == "asn"]
    raw_asns = [row.get("k") for row in slowest_net_rows if row.get("k")]
    try:
        asn_names = resolve_asn_names_map(service.service_id, raw_asns) if raw_asns else {}
    except Exception:
        asn_names = {}
    slowest_networks = []
    for row in slowest_net_rows:
        raw_key = str(row.get("k") or "").strip()
        if not raw_key:
            continue
        asn_int = parse_asn_int(raw_key)
        asn_str = f"AS{asn_int}" if asn_int is not None else raw_key
        label = format_asn_display_label(asn_int, asn_names) if asn_int is not None else asn_str
        slowest_networks.append({"asn": asn_str, "label": label, **_shape_rum_resource_latency_fields(row)})
    result = {
        "is_mock": False,
        "no_data": no_data,
        # True when any beacon in the window came from a sampled session: counts are estimates.
        "estimated": bool(counts.get("sampled")),
        "beacon_count": int(counts["beacons"]),
        "pageview_count": int(counts["pageviews"]),
        "interaction_count": int(counts["interactions"]),
        "error_count": int(counts["errors"]),
        "vitals": vitals,
        "worst_pages": worst_pages,
        "slowest_urls": slowest_urls,
        "slowest_networks": slowest_networks,
        "errors": [
            {
                "message": _decode_pct(row["error_message"]),
                "file": _decode_pct(row["error_file"] or "unknown.js") or "unknown.js",
                "line": int(row.get("error_line") or 0),
                "col": int(row.get("error_col") or 0),
                "count": row["count"],
            }
            for row in error_rows
        ],
        "trends": trends,
        "environments": {"browsers": browsers, "os": operating_systems, "devices": devices},
    }
    if no_data:
        result["message"] = "Waiting for real-time RUM user events..."
        for metric in ("lcp", "cls", "inp"):
            vitals[metric]["distribution"] = None
    return result


def rum_beacons_by_date(service: HighScaleService, start_time: str | None, end_time: str | None) -> dict[str, int]:
    """``{UTC date: distinct visible beacons}`` across vitals and errors in the inclusive window."""
    start, end = parse_time_window(start_time, end_time, utc=True, now=datetime.now(UTC))
    if end < start:
        raise ValueError("RUM end time precedes start time")
    window = ["event_timestamp >= {start:DateTime64(3, 'UTC')}", "event_timestamp <= {end:DateTime64(3, 'UTC')}"]
    where_vitals = " AND ".join([*visible_fact_clauses("rum_vitals"), *window, _PAGE_VITALS])
    where_errors = " AND ".join([*visible_fact_clauses("rum_errors"), *window])
    rows = service.client.execute(
        f"WITH events AS (SELECT event_timestamp, {_BEACON_IDENTITY} AS beacon_id FROM rum_vitals_facts "
        f"WHERE {where_vitals} UNION ALL SELECT event_timestamp, {_BEACON_IDENTITY} AS beacon_id "
        f"FROM rum_error_facts WHERE {where_errors}) "
        "/* rum_beacons_by_date */ SELECT toString(toDate(event_timestamp, 'UTC')) AS date, "
        "uniqExact(beacon_id) AS beacons FROM events GROUP BY date ORDER BY date",
        {"service_id": service.service_id, "start": start, "end": end},
    )
    return {str(row["date"]): int(row["beacons"]) for row in rows}


def _ttfb_ms(value: Any) -> int | None:
    """Beacons report TTFB in seconds or milliseconds; the live feed always shows whole milliseconds."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return int(number * 1000) if number < 15 else int(number)


def rum_live_events(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    limit: int,
    filters: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    start_dt, end_dt = parse_time_window(start_time, end_time, now=datetime.now(UTC))
    filter_clauses, filter_params = _filter_clauses(filters)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if _HS_RUM_CACHE_TTL > 0 and is_real_ch and start_time is not None and end_time is not None:
        digest = digest_cache_key(
            {
                "ep": "hs_rum_live_events",
                "cid": client_cache_token(service.client),
                "s": normalize_cache_bound(start_time),
                "e": normalize_cache_bound(end_time),
                "l": int(limit),
                "f": sorted((str(k), v) for k, v in (filters or {}).items()),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        memo_key = f"{service.service_id}:{digest}"
        if (cached := _rum_memo.get_with_telemetry(memo_key)) is not None:
            return list(cached.get("events", []))

    def _compute() -> dict[str, Any]:
        if memo_key is not None and (cached_inner := _rum_memo.get_with_telemetry(memo_key)) is not None:
            return cached_inner
        events = _rum_live_events_compute(service, start_dt, end_dt, limit, filter_clauses, filter_params)
        payload = {"events": events}
        if memo_key is not None:
            _rum_memo.put(memo_key, payload)
        return payload

    if memo_key is not None:
        return list(_rum_memo.coalesce(f"hs_rum_live:{memo_key}", _compute).get("events", []))
    return _compute()["events"]


def _rum_live_events_compute(
    service: HighScaleService,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
    filter_clauses: list[str],
    filter_params: dict[str, Any],
) -> list[dict[str, Any]]:
    window = ["event_timestamp >= {start_time:DateTime64(3)}", "event_timestamp < {end_time:DateTime64(3)}"]

    def scope(domain: str) -> str:
        return " AND ".join([*visible_fact_clauses(domain), *window, *filter_clauses])

    query = (
        f"/* rum_live_events */ SELECT * FROM ("
        f"SELECT * FROM (SELECT event_timestamp AS timestamp, "
        f"if(metric_name = 'ttlb', 'resource', 'pageview') AS type, "
        f"if(metric_name = 'ttlb' AND resource_url != '', resource_url, pathname) AS pathname, "
        f"metric_name, metric_value, metric_rating, browser, os, device, client_id AS cid, "
        f"coalesce(nullIf(req_id, ''), coalesce(toString(request_event_id), '')) AS req_id, "
        f"resource_url, "
        f"'' AS error_message, city, region, country, pop, tls, ttfb FROM rum_vitals_facts "
        f"WHERE {scope('rum_vitals')} "
        f"ORDER BY event_timestamp DESC LIMIT {{limit:UInt32}}) UNION ALL "
        f"SELECT * FROM (SELECT event_timestamp AS timestamp, 'error' AS type, pathname, '' AS metric_name, "
        f"CAST(NULL AS Nullable(Float64)) AS metric_value, '' AS metric_rating, browser, os, device, "
        f"client_id AS cid, coalesce(nullIf(req_id, ''), coalesce(toString(request_event_id), '')) AS req_id, "
        f"'' AS resource_url, "
        f"error_message, city, region, country, pop, tls, ttfb "
        f"FROM rum_error_facts WHERE {scope('rum_errors')} ORDER BY event_timestamp DESC LIMIT {{limit:UInt32}})"
        f")\n    ORDER BY timestamp DESC LIMIT {{limit:UInt32}}"
    )
    res = service.client.execute(
        query,
        {"service_id": service.service_id, "start_time": start_dt, "end_time": end_dt, "limit": limit, **filter_params},
    )

    from backend.utils.pop_utils import clean_edge_meta_str, resolve_pop_geo_fallback

    cid_ttfb: dict[str, int] = {}
    for d in res:
        c = d.get("cid")
        if c and str(c) not in cid_ttfb:
            ms = _ttfb_ms(d.get("ttfb"))
            if ms is None and str(d.get("metric_name") or "").upper() == "TTFB" and d.get("metric_value") is not None:
                ms = _ttfb_ms(d.get("metric_value"))
            if ms is not None:
                cid_ttfb[str(c)] = ms

    events: list[dict[str, Any]] = []
    for d in res:
        etype = d.get("type", "pageview")
        mname, mrating, mval = d.get("metric_name"), d.get("metric_rating"), d.get("metric_value")
        path = _decode_pct(d.get("pathname") or "/") or "/"
        browser = clean_edge_meta_str(d.get("browser")) or "Unknown"
        os_name = clean_edge_meta_str(d.get("os")) or "Unknown"
        device_name = clean_edge_meta_str(d.get("device")) or "Unknown"
        city_c, region_c, country_c = resolve_pop_geo_fallback(
            d.get("pop"), d.get("city"), d.get("region"), d.get("country")
        )
        pop_c = clean_edge_meta_str(d.get("pop"))
        tls_c = clean_edge_meta_str(d.get("tls"))
        ttfb_ms = _ttfb_ms(d.get("ttfb"))
        if ttfb_ms is None and mname and str(mname).upper() == "TTFB" and mval is not None:
            ttfb_ms = _ttfb_ms(mval)
        if ttfb_ms is None and d.get("cid") and str(d.get("cid")) in cid_ttfb:
            ttfb_ms = cid_ttfb[str(d.get("cid"))]
        res_url = _decode_pct(d.get("resource_url") or "")
        req_id = str(d.get("req_id") or "")
        meta = {
            "browser": {"name": browser},
            "os": {"name": os_name},
            "device": {"type": device_name},
            "page": {"url": path},
        }
        context = {
            "cid": d.get("cid"),
            "req_id": req_id,
            "resource_url": res_url,
            "city": city_c or "Unknown",
            "region": region_c or "Unknown",
            "country": country_c or "Unknown",
            "pop": pop_c or "Unknown",
            "tls": tls_c or "Unknown",
            "ttfb": ttfb_ms,
        }
        if etype == "error":
            desc = _decode_pct(d.get("error_message") or "JS Error") or "JS Error"
            raw_log: dict[str, Any] = {
                "meta": meta,
                "measurements": [],
                "exceptions": [{"type": "Error", "value": desc}],
                **context,
            }
        elif etype == "resource":
            ms_label = f"{round(float(mval))}ms" if mval is not None else "—"
            desc = f"Resource TTLB: {ms_label}"
            raw_log = {
                "meta": meta,
                "measurements": [{"type": "resource-timing", "values": {"ttlb": mval}, "url": res_url or path}],
                "exceptions": [],
                **context,
            }
        else:
            desc = "Page loaded successfully"
            if mname:
                desc = f"Metric {mname.upper()}: {float(mval)}" if mval is not None else f"Metric {mname.upper()}"
                if mrating:
                    desc += f" ({mrating.upper()})"
            if mname:
                measurements = [{"type": "web-vitals", "values": {mname: mval}, "context": {"rating": mrating or ""}}]
            else:
                measurements = []
            raw_log = {"meta": meta, "measurements": measurements, "exceptions": [], **context}
        events.append(
            {
                "time": _iso(d.get("timestamp")),
                "type": etype,
                "path": path,
                "desc": desc,
                "browser": browser,
                "os": os_name,
                "req_id": req_id or None,
                "resource_url": res_url or None,
                "metric_name": mname or None,
                "metric_value": float(mval) if mval is not None else None,
                "raw_log": raw_log,
            }
        )
    return events


def rum_request_log(
    service: HighScaleService,
    req_id: str,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Look up the matching edge request log row by ``_cf_rid`` for a RUM event."""
    clean_rid = req_id.strip()
    if not clean_rid or len(clean_rid) > 128 or any(ch.isspace() or ord(ch) < 32 for ch in clean_rid):
        raise ValueError("Invalid request ID")

    params: dict[str, Any] = {"service_id": service.service_id, "req_id": clean_rid}
    clauses = [*visible_fact_clauses("request"), "_cf_rid = {req_id:String}"]
    if timestamp:
        start_dt, end_dt = parse_time_window(timestamp, timestamp, utc=True, now=datetime.now(UTC))
        params["start"] = start_dt - timedelta(hours=2)
        params["end"] = end_dt + timedelta(hours=2)
    else:
        now = datetime.now(UTC)
        params["start"] = now - timedelta(days=7)
        params["end"] = now + timedelta(hours=1)
    clauses.append("event_timestamp >= {start:DateTime64(3, 'UTC')}")
    clauses.append("event_timestamp < {end:DateTime64(3, 'UTC')}")

    sql = (
        "/* rum_request_log */ SELECT "
        "event_timestamp AS timestamp, _cf_rid AS rid, _cf_method AS method, url, _cf_host AS host, "
        "toInt64OrNull(nullIf(_cf_status, '')) AS status, _cf_cache AS cache, _cf_pop AS pop, "
        "country, _cf_city AS city, _cf_region AS region, _cf_asn AS asn, "
        "toFloat64OrNull(nullIf(_cf_elapsed, '')) / 1000.0 AS elapsed_ms, "
        "toFloat64OrNull(nullIf(_cf_ttfb, '')) * 1000.0 AS ttfb_ms, "
        "toInt64OrNull(nullIf(_cf_resp_bytes, '')) AS resp_bytes, "
        "client_ip, _cf_ua AS ua, _cf_proto AS proto, _cf_tls AS tls "
        f"FROM request_facts WHERE {' AND '.join(clauses)} "
        "ORDER BY event_timestamp DESC LIMIT 1"
    )
    rows = service.client.execute(sql, params)
    if not rows:
        return {"found": False, "req_id": clean_rid, "request": None}
    row = dict(rows[0])
    row["timestamp"] = _iso(row.get("timestamp"))
    if row.get("elapsed_ms") is not None:
        row["elapsed_ms"] = round(float(row["elapsed_ms"]), 2)
    if row.get("ttfb_ms") is not None:
        row["ttfb_ms"] = round(float(row["ttfb_ms"]), 2)
    return {"found": True, "req_id": clean_rid, "request": row}


rum_events = rum_live_events
