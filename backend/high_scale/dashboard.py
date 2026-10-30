"""High-scale dashboard header metrics, time series, and panel aggregates."""

from __future__ import annotations

import sys
from datetime import datetime
from typing import TYPE_CHECKING, Any

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import (
    _FIELD_DIMENSIONS,
    _column,
    inclusive_minute_end,
    request_filter_clauses,
    visible_fact_clauses,
)
from backend.high_scale._common import (
    range_value as _range_value,
)
from backend.high_scale.aggregate_query import (
    AggregateRequest,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.models.analytics import (
    AggregatesRequest,
)
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    client_cache_token,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    chart_interval_seconds,
    enrich_asn_rows,
    normalize_chart_interval,
)

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

# ── Folded from backend/high_scale/dashboard.py ──────────────────────────────

import concurrent.futures
import os
from functools import partial

from backend.models.analytics import (
    AggregatesResponse,
    FieldAggregate,
    FieldTopEntry,
    FieldValuesRequest,
    FieldValuesResponse,
    MapPoint,
    RawRequest,
    TimeSeriesPoint,
)
from backend.models.common import ResponseCacheObservation
from backend.repositories.utils.response_cache import cache_result
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import ensure_utc, parse_iso_utc, safe_iso

_HS_DASHBOARD_CACHE_TTL = float(os.getenv("HS_DASHBOARD_CACHE_TTL_SECONDS", "30"))
_dashboard_memo = ResponseMemo(ttl=_HS_DASHBOARD_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_dashboard_aggregates_memo", _dashboard_memo.cache)

_HEADER_TABLES = {"request": "request_facts", "rum_vitals": "rum_vitals_facts", "rum_errors": "rum_error_facts"}


def _normalize_extent_iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value).isoformat()
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = parse_iso_utc(raw)
        return parsed.isoformat() if parsed is not None else raw
    except ValueError:
        return raw


def _watermark_extent(value: datetime | None) -> str | None:
    return ensure_utc(value).isoformat() if isinstance(value, datetime) else None


def _query_source_extents(
    client: Any,
    source_id: str,
    table_name: str,
    *,
    domain: str,
    count_expr: str = "count()",
    ts_col: str = "event_timestamp",
) -> tuple[int, str | None, str | None]:
    rows = client.execute(
        f"SELECT {count_expr} AS total_rows, min({ts_col}) AS earliest_log_at, max({ts_col}) AS latest_log_at "
        f"FROM {table_name} "
        "WHERE service_id={service_id:String} "
        "AND publication_state = 'visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={domain:String} "
        "AND publication_state = 'visible'"
        ")",
        {"service_id": source_id, "domain": domain},
    )
    row = rows[0] if rows else {}
    total_rows = int(row.get("total_rows") or 0)
    if total_rows == 0:
        return 0, None, None
    return (
        total_rows,
        _normalize_extent_iso(row.get("earliest_log_at")),
        _normalize_extent_iso(row.get("latest_log_at")),
    )


def header_metrics(service: HighScaleService) -> dict[str, Any]:
    """Return visible ClickHouse row counts and event-time extents for the header."""
    totals: dict[str, int] = {}
    earliest: dict[str, str | None] = {}
    latest: dict[str, str | None] = {}
    for domain, table in _HEADER_TABLES.items():
        totals[domain], earliest[domain], latest[domain] = _query_source_extents(
            service.client, service.service_id, table, domain=domain
        )

    if totals.get("rum_errors", 0) == 0:
        agg_total, agg_earliest, agg_latest = _query_source_extents(
            service.client,
            service.service_id,
            "rum_error_aggregates",
            domain="rum_errors_aggregate",
            count_expr="sum(error_count)",
            ts_col="bucket_start",
        )
        if agg_total > 0:
            totals["rum_errors"] = agg_total
            earliest["rum_errors"] = agg_earliest
            latest["rum_errors"] = agg_latest

    rum_total = totals["rum_vitals"] + totals["rum_errors"]
    rum_earliest_values = [earliest["rum_vitals"], earliest["rum_errors"]]
    rum_latest_values = [latest["rum_vitals"], latest["rum_errors"]]
    rum_earliest = min((value for value in rum_earliest_values if value is not None), default=None)
    rum_latest = max((value for value in rum_latest_values if value is not None), default=None)
    request_total = totals["request"]
    request_earliest = earliest["request"]
    request_latest = latest["request"]

    req_sync: str | None = None
    rum_sync: str | None = None
    try:
        from backend.core.metadata.usage_log import latest_cron_per_task

        latest_crons = latest_cron_per_task(service.service_id)
        req_sync = max(
            (
                str(latest_crons[t]["started_at"])
                for t in ("log_discovery", "commit", "ledger_sweep")
                if t in latest_crons and latest_crons[t].get("started_at")
            ),
            default=None,
        )
        rum_sync = max(
            (
                str(latest_crons[t]["started_at"])
                for t in ("rum_discovery", "rum_sync", "ledger_rum_sweep")
                if t in latest_crons and latest_crons[t].get("started_at")
            ),
            default=None,
        )
    except Exception:
        pass

    return {
        "request": {
            "earliest_log_at": request_earliest,
            "latest_log_at": request_latest,
            "total_rows": request_total,
            "last_sync_at": req_sync,
        },
        "rum": {
            "earliest_log_at": rum_earliest,
            "latest_log_at": rum_latest,
            "total_rows": rum_total,
            "last_sync_at": rum_sync,
        },
        "earliest_log_at": request_earliest,
        "latest_log_at": request_latest,
        "local_rows": request_total + rum_total,
    }


def _aggregate(service: HighScaleService, *, start_time: str | None, end_time: str | None, dimension: str, watermark):
    start = _range_value(start_time)
    end = _range_value(end_time)
    return query_clickhouse_aggregate(
        service.client,
        AggregateRequest(service_id=service.service_id, domain="request", start=start, end=end, dimension=dimension),
        watermark=watermark,
    )


_normalize_chart_interval = normalize_chart_interval


def _time_series(
    service: HighScaleService, start_time: str | None, end_time: str | None, chart_interval: str
) -> list[TimeSeriesPoint]:
    start = _range_value(start_time)
    end = _range_value(end_time)
    clauses = [
        "service_id={service_id:String}",
        "dimension='url'",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain='request_aggregate' "
        "AND publication_state='visible')",
    ]
    interval_norm = _normalize_chart_interval(chart_interval, default="1 minute")
    bucket_seconds = chart_interval_seconds(interval_norm, default=60)
    params: dict[str, Any] = {"service_id": service.service_id, "bucket_seconds": bucket_seconds}
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start < {end:DateTime64(3)}")
        params["end"] = end
    rows = service.client.execute(
        "SELECT toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND) AS chart_bucket, "
        "sum(request_count) AS value "
        "FROM request_aggregates "
        f"WHERE {' AND '.join(clauses)} "
        "GROUP BY chart_bucket ORDER BY chart_bucket",
        params,
    )
    return [
        TimeSeriesPoint(time=safe_iso(row["chart_bucket"]) or str(row["chart_bucket"]), value=float(row["value"]))
        for row in rows
    ]


def _panels(req: AggregatesRequest) -> tuple[bool, bool, bool]:
    """``(time_series, map_data, top_n)`` gates, derived exactly as the standard router derives them."""
    if req.sections:
        sections = set(req.sections)
        return "core" in sections, "core" in sections, "topten" in sections
    return req.include_time_series is not False, req.include_map_data is not False, True


def _field_sql_column(field: str) -> str:
    col_name = _FIELD_DIMENSIONS[field]
    if col_name in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
        return col_name
    if col_name in {"age", "ttl"}:
        return f"toString(toInt32(round(toFloat64OrZero(custom_fields['{col_name}']))))"
    return f"custom_fields['{col_name}']"


def _fields_to_fetch(req: AggregatesRequest, want_map: bool, want_top_n: bool) -> tuple[list[str], list[str]]:
    """``(card_fields, fields_to_query)``; the map is the country card's top values, so it may need an extra field."""
    requested_fields = list(_FIELD_DIMENSIONS) if req.fields is None else req.fields
    card_fields = [field for field in requested_fields if field in _FIELD_DIMENSIONS]
    top_fields = card_fields if want_top_n else []
    to_query = list(top_fields)
    if want_map and "country" not in to_query:
        to_query.append("country")
    return top_fields, to_query


def _facts_time_series(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    norm_interval: str,
    metric: str,
    where_clause: str,
    params: dict[str, Any],
) -> list[TimeSeriesPoint]:
    interval_sql = "toStartOfMinute(event_timestamp)"
    if norm_interval == "1 second":
        interval_sql = "toStartOfSecond(event_timestamp)"
    elif norm_interval == "5 minutes":
        interval_sql = "toStartOfFiveMinutes(event_timestamp)"
    elif norm_interval == "1 hour":
        interval_sql = "toStartOfInterval(event_timestamp, INTERVAL 1 hour)"
    elif norm_interval == "1 day":
        interval_sql = "toStartOfDay(event_timestamp)"

    if metric == "5xx":
        metric_sql = "countIf(custom_fields['status'] >= '500' AND custom_fields['status'] <= '599')"
    elif metric == "4xx":
        metric_sql = "countIf(custom_fields['status'] >= '400' AND custom_fields['status'] <= '499')"
    elif metric == "hit_rate":
        metric_sql = "round(100.0 * countIf(custom_fields['cache'] IN ('HIT', 'HIT-CLUSTER', 'HIT-STALE')) / nullIf(count(), 0), 2)"
    else:
        metric_sql = "count()"

    ts_rows = service.client.execute(
        f"SELECT {interval_sql} AS bucket_start, {metric_sql} AS value FROM request_facts "
        f"WHERE {where_clause} GROUP BY bucket_start ORDER BY bucket_start",
        params,
    )
    return [
        TimeSeriesPoint(time=safe_iso(r["bucket_start"]) or str(r["bucket_start"]), value=float(r["value"]))
        for r in ts_rows
    ]


def _filtered_aggregates(
    service: HighScaleService,
    req: AggregatesRequest,
    start_time: str | None,
    end_time: str | None,
    *,
    trace: HighScaleSections | None = None,
    request_watermark: Any = None,
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    if trace is None:
        trace = HighScaleSections(service.service_id, "dashboard")
    if request_watermark is None:
        request_watermark = trace.run("watermark", lambda: service.watermark_for("request"), activity="overhead")
    want_time_series, want_map, want_top_n = _panels(req)

    if not req.filters:
        filter_dict = {}
    elif hasattr(req.filters, "model_dump"):
        filter_dict = req.filters.model_dump()
    else:
        filter_dict = req.filters
    filter_clauses, filter_params = request_filter_clauses(filter_dict)

    clauses = [*visible_fact_clauses("request"), *filter_clauses]
    params: dict[str, Any] = {"service_id": service.service_id, **filter_params}

    if start is not None and end is not None:
        end_upper = end
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    where_clause = " AND ".join(clauses)

    top_fields, fields_to_query = _fields_to_fetch(req, want_map, want_top_n)

    def fetch_field(field: str):
        sql_col = _field_sql_column(field)
        rows = service.client.execute(
            f"SELECT {sql_col} AS v, count() AS c FROM request_facts WHERE {where_clause} AND {sql_col} != '' "
            "GROUP BY v ORDER BY c DESC LIMIT 10",
            params,
        )
        return field, [(str(r["v"]), int(r["c"])) for r in rows]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = [trace.submit(ex, f"top_n.{f}", partial(fetch_field, f), source="fact") for f in fields_to_query]
        field_results = dict(f.result() for f in futs)

    total_rows_result = trace.run(
        "count",
        lambda: service.client.execute(f"SELECT count() AS c FROM request_facts WHERE {where_clause}", params),
        source="fact",
    )
    total_count = int(total_rows_result[0]["c"]) if total_rows_result else 0

    data = {
        field: FieldAggregate(top=[FieldTopEntry(value=v, count=c) for v, c in field_results[field]], total=total_count)
        for field in top_fields
    }

    map_data = [MapPoint(country=v, count=c) for v, c in field_results.get("country", [])] if want_map else []

    norm_interval = _normalize_chart_interval(req.chart_interval)
    time_series = (
        trace.run(
            "time_series",
            lambda: _facts_time_series(
                service, start_time, end_time, norm_interval, req.chart_metric or "requests", where_clause, params
            ),
            source="fact",
        )
        if want_time_series
        else []
    )

    return AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse (filtered)",
        section_timings=trace.timings(),
        interval=norm_interval,
        metric=req.chart_metric,
        total_rows=total_count,
        total_rows_total=total_count,
        earliest_log_at=_watermark_extent(request_watermark.coverage_start),
        latest_log_at=_watermark_extent(request_watermark.coverage_end),
    )


def _aggregates_batch(
    service: HighScaleService, *, start_time: str | None, end_time: str | None, fields: list[str], watermark
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    dims = [_FIELD_DIMENSIONS[f] for f in fields]
    by_dim = query_clickhouse_aggregates_batch(
        service.client,
        service_id=service.service_id,
        domain="request",
        dimensions=dims,
        start=start,
        end=end,
        watermark=watermark,
    )
    return {f: by_dim[_FIELD_DIMENSIONS[f]] for f in fields}


def _cache_agg_resp(cache_key: str | None, resp: AggregatesResponse) -> AggregatesResponse:
    if cache_key is not None:
        _dashboard_memo.put(
            cache_key,
            {
                "data": resp.data,
                "time_series": resp.time_series,
                "map_data": resp.map_data,
                "where_clause": resp.where_clause,
                "interval": resp.interval,
                "metric": resp.metric,
                "total_rows": resp.total_rows,
                "total_rows_total": resp.total_rows_total,
                "earliest_log_at": resp.earliest_log_at,
                "latest_log_at": resp.latest_log_at,
                "section_timings": resp.section_timings,
            },
        )
    resp.response_cache = ResponseCacheObservation.model_validate(
        _dashboard_memo.result({}, state="miss" if cache_key is not None else "bypass")["_response_cache"]
    )
    return resp


def dashboard_aggregates(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
):
    norm_interval = _normalize_chart_interval(req.chart_interval)
    start = _range_value(start_time)
    end = _range_value(end_time)
    trace = HighScaleSections(service.service_id, "dashboard")
    request_watermark = trace.run("watermark", lambda: service.watermark_for("request"), activity="overhead")
    cache_key: str | None = None
    if _HS_DASHBOARD_CACHE_TTL > 0 and start_time is not None and end_time is not None:
        raw_filters = (
            req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
        )
        cache_key = digest_cache_key(
            {
                "ep": "hs_dashboard_aggregates",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "wm": (_watermark_extent(request_watermark.coverage_end), request_watermark.last_visible_event_id),
                "s": start.isoformat() if start is not None else None,
                "e": end.isoformat() if end is not None else None,
                "f": serialize_filters_for_key(raw_filters),
                "ci": norm_interval,
                "cm": req.chart_metric,
                "fl": sorted(req.fields) if req.fields else None,
                "sec": sorted(req.sections) if req.sections else None,
                "its": req.include_time_series,
                "icr": req.include_conn_requests,
                "imd": req.include_map_data,
            },
            {"service_id": service.service_id},
        )
        cached = _dashboard_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return AggregatesResponse.with_telemetry(**cached)

    partial_minutes = (
        start is not None
        and end is not None
        and any(bound.second != 0 or bound.microsecond != 0 for bound in (start, end))
    )
    if req.filters or norm_interval == "1 second" or partial_minutes:
        resp = _filtered_aggregates(
            service, req, start_time, end_time, trace=trace, request_watermark=request_watermark
        )
        return _cache_agg_resp(cache_key, resp)

    want_time_series, want_map, want_top_n = _panels(req)
    top_fields, fields_to_query = _fields_to_fetch(req, want_map, want_top_n)
    all_fields_to_query = list(fields_to_query)
    if "url" not in all_fields_to_query:
        all_fields_to_query.append("url")
    responses = trace.run(
        "aggregates_batch",
        lambda: _aggregates_batch(
            service, start_time=start_time, end_time=end_time, fields=all_fields_to_query, watermark=request_watermark
        ),
        source="projection",
    )
    total = responses["url"]

    data = {
        field: FieldAggregate(
            top=[FieldTopEntry(value=value, count=count) for value, count in responses[field].top_values],
            total=responses[field].request_count,
        )
        for field in top_fields
    }
    map_response = responses.get("country")
    map_data = (
        [MapPoint(country=value, count=count) for value, count in map_response.top_values]
        if want_map and map_response is not None
        else []
    )
    if not want_time_series:
        time_series = []
    elif req.chart_metric and req.chart_metric != "requests":
        start = _range_value(start_time)
        end = _range_value(end_time)
        clauses = list(visible_fact_clauses("request"))
        params: dict[str, Any] = {"service_id": service.service_id}
        if start is not None and end is not None:
            end_upper = end
            clauses.append("event_timestamp >= {start:DateTime64(3)}")
            clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
            params.update({"start": start, "end_upper": end_upper})
        time_series = trace.run(
            "time_series",
            lambda: _facts_time_series(
                service, start_time, end_time, norm_interval, req.chart_metric, " AND ".join(clauses), params
            ),
            source="fact",
        )
    else:
        time_series = trace.run(
            "time_series", lambda: _time_series(service, start_time, end_time, norm_interval), source="projection"
        )
    resp = AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse",
        section_timings=trace.timings(),
        interval=norm_interval,
        metric=req.chart_metric,
        total_rows=total.request_count,
        total_rows_total=total.request_count,
        earliest_log_at=_watermark_extent(request_watermark.coverage_start),
        latest_log_at=_watermark_extent(request_watermark.coverage_end),
    )
    return _cache_agg_resp(cache_key, resp)


aggregates = dashboard_aggregates


def bundle(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    from backend.high_scale import security as hs_security
    from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

    sections = set(req.sections) if req.sections else None
    want_aggregates = sections is None or bool(sections & {"core", "topten"})
    want_bots = sections is None or "bots" in sections
    res_aggregates = aggregates(service, req, start_time, end_time) if want_aggregates else None
    res_top_bots = hs_security.top_bots(service, req, start_time, end_time) if want_bots else None

    if res_aggregates is not None and hasattr(res_aggregates, "debug_queries"):
        res_aggregates.debug_queries = []
        res_aggregates.debug_calls = []
    if res_top_bots is not None and hasattr(res_top_bots, "debug_queries"):
        res_top_bots.debug_queries = []
        res_top_bots.debug_calls = []

    combined_timings = [
        t for r in (res_aggregates, res_top_bots) if r is not None for t in (getattr(r, "section_timings", None) or [])
    ]
    return {
        **cache_result({}, state="bypass", prewarmer=False),
        "aggregates": res_aggregates,
        "top_bots": res_top_bots,
        "section_timings": combined_timings,
        "debug_queries": get_queries(),
        "debug_calls": get_tracked_calls(),
        "debug_sqlite": list(get_sqlite_queries()),
    }


def field_values(
    service: HighScaleService, req: FieldValuesRequest, start_time: str | None, end_time: str | None
) -> FieldValuesResponse:
    """Return top values for a single field in high-scale ClickHouse mode."""
    from backend.repositories.utils.filters import normalize_filter_key

    clean_field = "".join(ch for ch in req.field if ch.isalnum() or ch == "_")
    if not clean_field:
        raise ValueError("Invalid field name")

    key = normalize_filter_key(clean_field)
    if not key:
        raise ValueError("Invalid field name")

    raw_filters = (
        req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
    )
    filters_excl = {k: v for k, v in raw_filters.items() if k != req.field and normalize_filter_key(k) != key}
    filter_clauses, filter_params = request_filter_clauses(filters_excl)

    limit = max(1, min(int(req.limit or 100), 1000))
    start = _range_value(start_time)
    end = _range_value(end_time)

    if not req.search and not filter_clauses and key in _FIELD_DIMENSIONS:
        agg = _aggregate(
            service,
            start_time=start_time,
            end_time=end_time,
            dimension=_FIELD_DIMENSIONS[key],
            watermark=service.watermark_for("request"),
        )
        vals: list[dict[str, Any]] = [{"value": str(val), "count": int(cnt)} for val, cnt in agg.top_values[:limit]]
        if key == "asn" and vals:
            try:
                enrich_asn_rows(vals, service.service_id, asn_key="value")
            except Exception:
                pass
        return FieldValuesResponse.with_telemetry(values=[FieldTopEntry(**v) for v in vals], field=req.field)

    if key in {"_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind", "edge_score_reason_ind"}:
        return FieldValuesResponse.with_telemetry(values=[], field=req.field)

    params: dict[str, Any] = {"service_id": service.service_id, "limit": limit, **filter_params}
    sql_col = _column(key, params, "target_field")
    clauses = [*visible_fact_clauses("request"), *filter_clauses, f"{sql_col} != ''"]

    if start is not None and end is not None:
        end_upper = inclusive_minute_end(end)
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    if req.search:
        params["search_pat"] = f"%{req.search}%"
        clauses.append(f"{sql_col} ILIKE {{search_pat:String}}")

    where_clause = " AND ".join(clauses)
    rows = service.client.execute(
        f"SELECT {sql_col} AS v, count() AS c FROM request_facts "
        f"WHERE {where_clause} GROUP BY v ORDER BY c DESC LIMIT {{limit:UInt32}}",
        params,
    )
    vals_facts: list[dict[str, Any]] = [{"value": str(r["v"]), "count": int(r["c"])} for r in rows]
    if key == "asn" and vals_facts:
        try:
            enrich_asn_rows(vals_facts, service.service_id, asn_key="value")
        except Exception:
            pass
    return FieldValuesResponse.with_telemetry(values=[FieldTopEntry(**v) for v in vals_facts], field=req.field)


def raw_df(
    service: HighScaleService, req: RawRequest, start_time: str | None, end_time: str | None, *, limit: int = 50000
):
    """Fetch raw rows from ClickHouse ``request_facts`` as a pandas DataFrame for CSV export."""
    import pandas as pd

    from backend.utils.bot_sources import enrich_bot_metadata

    raw_filters = (
        req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
    )
    filter_clauses, filter_params = request_filter_clauses(raw_filters)
    clauses = [*visible_fact_clauses("request"), *filter_clauses]
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "limit": max(1, min(int(limit), 50000)),
        **filter_params,
    }

    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None and end is not None:
        end_upper = inclusive_minute_end(end)
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    where_clause = " AND ".join(clauses)
    rows = service.client.execute(
        "SELECT event_timestamp AS timestamp, client_ip AS ip, url, country, custom_fields "
        f"FROM request_facts WHERE {where_clause} "
        "ORDER BY event_timestamp DESC LIMIT {limit:UInt32}",
        params,
    )
    if not rows:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for r in rows:
        cf = r.get("custom_fields")
        custom = dict(cf) if isinstance(cf, dict) else {}
        ts = r.get("timestamp")
        records.append(
            {
                **custom,
                "timestamp": safe_iso(ts) if isinstance(ts, datetime) else str(ts or ""),
                "ip": str(r.get("ip") or ""),
                "url": str(r.get("url") or ""),
                "country": str(r.get("country") or ""),
            }
        )

    df = pd.DataFrame(records)
    enrich_bot_metadata(df)

    if req.columns:
        existing_cols = [c for c in req.columns if c in df.columns]
        if "_bot_name" in req.columns and "_bot_name" not in existing_cols:
            df["_bot_name"] = "null"
            existing_cols.append("_bot_name")
        if "_ngwaf_bot_name" in req.columns and "_ngwaf_bot_name" not in existing_cols:
            df["_ngwaf_bot_name"] = None
            existing_cols.append("_ngwaf_bot_name")
        df = df[existing_cols]

    return df


if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "dashboard", sys.modules[__name__])
