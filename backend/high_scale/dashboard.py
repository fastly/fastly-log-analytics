"""High-scale dashboard header metrics, time series, and panel aggregates."""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import (
    _FIELD_DIMENSIONS,
    _column,
    normalize_cache_bound,
    request_filter_clauses,
    visible_fact_clauses,
)
from backend.high_scale._common import (
    range_value as _range_value,
)
from backend.high_scale.aggregate_query import (
    AggregateRequest,
    _hourly_edge_time_clause,
    can_use_hourly_rollup,
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
import contextvars
import copy
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
from backend.repositories.utils.response_cache import cache_result, coalesce
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import ensure_utc, parse_iso_utc, safe_iso

_HS_DASHBOARD_CACHE_TTL = float(os.getenv("HS_DASHBOARD_CACHE_TTL_SECONDS", "60"))
_dashboard_memo = ResponseMemo(ttl=_HS_DASHBOARD_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_dashboard_aggregates_memo", _dashboard_memo.cache)

_HS_HEADER_METRICS_CACHE_TTL = float(os.getenv("HS_HEADER_METRICS_CACHE_TTL_SECONDS", "15"))
_header_metrics_cache: BoundedTTLCache[tuple[str, int], dict[str, Any]] = BoundedTTLCache(
    maxsize=128, ttl_seconds=_HS_HEADER_METRICS_CACHE_TTL
)
CacheRegistry.register("hs_header_metrics_cache", _header_metrics_cache)

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
    params = {"service_id": source_id, "domain": domain}
    where_sql = (
        "service_id={service_id:String} "
        "AND publication_state = 'visible' "
        "AND batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={domain:String} "
        "AND publication_state = 'visible'"
        ")"
    )
    if hasattr(client, "rewrite_publication_fence"):
        where_sql = client.rewrite_publication_fence(where_sql, params)
    rows = client.execute(
        f"SELECT {count_expr} AS total_rows, min({ts_col}) AS earliest_log_at, max({ts_col}) AS latest_log_at "
        f"FROM {table_name} "
        f"WHERE {where_sql}",
        params,
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
    cache_key = (service.service_id, client_cache_token(service.client))
    use_cache = not hasattr(service.client, "responses")
    if use_cache and (cached := _header_metrics_cache.get(cache_key)) is not None:
        return copy.deepcopy(cached)

    totals: dict[str, int] = {}
    earliest: dict[str, str | None] = {}
    latest: dict[str, str | None] = {}
    if use_cache:
        from backend.utils.telemetry.metrics import get_queries

        get_queries()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(_HEADER_TABLES)) as pool:
            futs = {
                domain: pool.submit(
                    contextvars.copy_context().run,
                    _query_source_extents,
                    service.client,
                    service.service_id,
                    table,
                    domain=domain,
                )
                for domain, table in _HEADER_TABLES.items()
            }
            for domain in _HEADER_TABLES:
                totals[domain], earliest[domain], latest[domain] = futs[domain].result()
    else:
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

    try:
        from backend.core.metadata.base import get_con

        con = get_con(service.service_id)
        cursor_rows = con.execute(
            "SELECT domain, MAX(updated_at) FROM high_scale_source_cursors WHERE service_id = ? GROUP BY domain",
            (service.service_id,),
        ).fetchall()
        for dom, updated_at in cursor_rows:
            ts_iso = _normalize_extent_iso(updated_at)
            if not ts_iso:
                continue
            if dom == "request":
                req_sync = max(req_sync, ts_iso) if req_sync else ts_iso
            elif str(dom).startswith("rum"):
                rum_sync = max(rum_sync, ts_iso) if rum_sync else ts_iso
    except Exception:
        pass

    result = {
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
    if use_cache:
        _header_metrics_cache.set(cache_key, copy.deepcopy(result))
    return result


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
    rows: list[dict[str, Any]] = []
    hourly_bounds = (
        can_use_hourly_rollup(service.client, "request", start, end)
        if bucket_seconds >= 3600 and bucket_seconds % 3600 == 0
        else None
    )
    if hourly_bounds is not None and start is not None and end is not None:
        h_start, h_end = hourly_bounds
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        h_params = {**params, "h_start": h_start, "h_end": h_end}
        edge_clause = _hourly_edge_time_clause(start_utc, end_utc, h_start, h_end)
        union_sql = (
            "SELECT bucket_start, sum(request_count) AS request_count FROM request_aggregates_1h "
            "WHERE service_id={service_id:String} AND dimension='url' "
            "AND bucket_start >= {h_start:DateTime64(3)} AND bucket_start < {h_end:DateTime64(3)} "
            "GROUP BY bucket_start"
        )
        if edge_clause is not None:
            union_sql += (
                " UNION ALL SELECT bucket_start, sum(request_count) AS request_count FROM request_aggregates "
                "WHERE service_id={service_id:String} AND dimension='url' "
                f"AND {edge_clause} AND publication_state='visible' AND batch_id IN ("
                "SELECT batch_id FROM high_scale_batch_publications FINAL "
                "WHERE service_id={service_id:String} AND domain='request_aggregate' "
                "AND publication_state='visible') "
                "GROUP BY bucket_start"
            )
        try:
            rows = service.client.execute(
                "SELECT toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND) AS chart_bucket, "
                f"sum(request_count) AS value FROM ({union_sql}) "
                "GROUP BY chart_bucket ORDER BY chart_bucket",
                h_params,
            )
        except Exception:
            rows = []
    if not rows:
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


def _facts_time_series_with_count(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    norm_interval: str,
    metric: str,
    where_clause: str,
    params: dict[str, Any],
) -> tuple[list[TimeSeriesPoint], int]:
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
        f"SELECT {interval_sql} AS bucket_start, {metric_sql} AS value, count() AS _bucket_cnt FROM request_facts "
        f"WHERE {where_clause} GROUP BY bucket_start ORDER BY bucket_start",
        params,
    )
    total_cnt = sum(int(r.get("_bucket_cnt") or r.get("value") or 0) for r in ts_rows)
    return (
        [
            TimeSeriesPoint(time=safe_iso(r["bucket_start"]) or str(r["bucket_start"]), value=float(r["value"]))
            for r in ts_rows
        ],
        total_cnt,
    )


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

    clauses = [*visible_fact_clauses("request", client=service.client, service_id=service.service_id), *filter_clauses]
    params: dict[str, Any] = {"service_id": service.service_id, **filter_params}

    if start is not None and end is not None:
        end_upper = end
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp <= {end_upper:DateTime64(3)}")
        params.update({"start": start, "end_upper": end_upper})

    where_clause = " AND ".join(clauses)

    top_fields, fields_to_query = _fields_to_fetch(req, want_map, want_top_n)
    ua_bot_rows: list[tuple[str, int]] = []
    ngwaf_bot_rows: list[tuple[str, int]] = []

    def fetch_field(field: str):
        sql_col = _field_sql_column(field)
        rows = service.client.execute(
            f"SELECT {sql_col} AS v, count() AS c FROM request_facts WHERE {where_clause} AND {sql_col} != '' "
            "GROUP BY v ORDER BY c DESC LIMIT 10",
            params,
        )
        return field, [(str(r["v"]), int(r["c"])) for r in rows]

    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    norm_interval = _normalize_chart_interval(req.chart_interval)
    eff_where = (
        service.client.rewrite_publication_fence(where_clause, params)
        if is_real_ch and hasattr(service.client, "rewrite_publication_fence")
        else where_clause
    )
    use_approx_top_k = is_real_ch and "SELECT" not in eff_where
    inline_ts_buckets: list[TimeSeriesPoint] = []
    chunk_total_counts: list[int] = []

    def fetch_field_chunk(
        chunk: list[str], *, include_bots: bool = False, include_ts: bool = False
    ) -> dict[str, list[tuple[str, int]]]:
        if use_approx_top_k:
            exprs: list[str] = ["count() AS __total_count__"]
            for f in chunk:
                col = _field_sql_column(f)
                if include_bots and f == "ua":
                    exprs.append(f"approx_top_kIf(1000, 3000)({col}, {col} != '') AS `{f}`")
                else:
                    exprs.append(f"approx_top_kIf(15, 250)({col}, {col} != '') AS `{f}`")
            if include_bots:
                exprs.append(
                    "approx_top_kIf(200, 1000)("
                    "concat(custom_fields['ngwaf_bot'], '\x1f', custom_fields['ngwaf_bot_category']), "
                    "custom_fields['ngwaf_bot'] != '') AS `__ngwaf_bot__`"
                )
            if include_ts:
                interval_sql = "toStartOfMinute(event_timestamp)"
                if norm_interval == "5 minutes":
                    interval_sql = "toStartOfFiveMinutes(event_timestamp)"
                elif norm_interval == "1 hour":
                    interval_sql = "toStartOfInterval(event_timestamp, INTERVAL 1 hour)"
                elif norm_interval == "1 day":
                    interval_sql = "toStartOfDay(event_timestamp)"
                exprs.append(f"approx_top_k(1500, 3000)({interval_sql}) AS `__ts_buckets__`")
            sql = f"SELECT {', '.join(exprs)} FROM request_facts PREWHERE {eff_where}"
            rows = service.client.execute(sql, params)
            row = rows[0] if rows else {}
            if "__total_count__" in row:
                chunk_total_counts.append(int(row["__total_count__"] or 0))
            out: dict[str, list[tuple[str, int]]] = {}
            for f in chunk:
                items = row.get(f) or []
                sorted_items = sorted(items, key=lambda x: (-int(x["count"]), str(x["item"])))
                out[f] = [(str(x["item"]), int(x["count"])) for x in sorted_items[:10]]
                if include_bots and f == "ua":
                    for x in sorted_items:
                        ua_bot_rows.append((str(x["item"]), int(x["count"])))
            if include_bots:
                for x in row.get("__ngwaf_bot__") or []:
                    ngwaf_bot_rows.append((str(x["item"]), int(x["count"])))
            if include_ts:
                ts_items = row.get("__ts_buckets__") or []
                sorted_ts = sorted(ts_items, key=lambda x: str(x["item"]))
                for x in sorted_ts:
                    b_str = safe_iso(x["item"]) or str(x["item"])
                    inline_ts_buckets.append(TimeSeriesPoint(time=b_str, value=float(x["count"])))
            return out

        pair_exprs = [f"('{f}', {_field_sql_column(f)})" for f in chunk]
        if include_bots:
            pair_exprs.append(
                "('__ngwaf_bot__', if(custom_fields['ngwaf_bot'] != '', "
                "concat(custom_fields['ngwaf_bot'], '\x1f', custom_fields['ngwaf_bot_category']), ''))"
            )
        pairs_sql = ", ".join(pair_exprs)
        limit_by = 1000 if include_bots else 10
        if "SELECT" not in eff_where:
            sql = (
                f"SELECT pair.1 AS field_name, pair.2 AS v, count() AS c "
                f"FROM (SELECT arrayFilter(p -> p.2 != '', [{pairs_sql}]) AS pairs "
                f"FROM request_facts PREWHERE {eff_where}) "
                f"ARRAY JOIN pairs AS pair "
                f"GROUP BY field_name, v ORDER BY field_name ASC, c DESC, v ASC LIMIT {limit_by} BY field_name"
            )
        else:
            sql = (
                f"SELECT pair.1 AS field_name, pair.2 AS v, count() AS c "
                f"FROM request_facts ARRAY JOIN [{pairs_sql}] AS pair "
                f"WHERE {eff_where} AND v != '' "
                f"GROUP BY field_name, v ORDER BY field_name ASC, c DESC, v ASC LIMIT {limit_by} BY field_name"
            )
        rows = service.client.execute(sql, params)
        out = {f: [] for f in chunk}
        for r in rows:
            fn = str(r.get("field_name") or "")
            val_str = str(r["v"])
            cnt_int = int(r["c"])
            if fn in out and len(out[fn]) < 10:
                out[fn].append((val_str, cnt_int))
            if include_bots:
                if fn == "ua":
                    ua_bot_rows.append((val_str, cnt_int))
                elif fn == "__ngwaf_bot__":
                    ngwaf_bot_rows.append((val_str, cnt_int))
        return out

    if is_real_ch and len(fields_to_query) > 2:
        # Group custom_fields that share the same ClickHouse Map bucket (cityHash64(key) % 64)
        # into the same chunk so no Map bucket is decompressed across multiple chunks:
        #   bucket 9: pop, ttl, ua  |  bucket 25: digest, hits  |  bucket 28: city, waf_ms  |  bucket 39: ja3, proto
        bucket_groups = (
            (
                "ip",
                "country",
                "url",
                "digest",
                "hits",
                "city",
                "waf_ms",
                "ja3",
                "proto",
                "method",
                "status",
                "cache",
                "tls",
                "tls_cipher",
            ),
            (
                "asn",
                "region",
                "conn_speed",
                "host",
                "content_type",
                "shield",
                "edge",
                "h2_stream",
                "error_code",
                "beresp",
                "waf_action",
                "waf_block",
                "waf_failures",
                "waf_tags",
            ),
            (
                "pop",
                "ttl",
                "ua",
                "age",
                "ja4",
                "ja4h",
                "ddos_action",
                "bot_score",
                "edge_score_action",
                "edge_score_reason",
                "edge_sid",
                "ngwaf_bot_category",
            ),
        )
        req_set = set(fields_to_query)
        chunks = [[f for f in grp if f in req_set] for grp in bucket_groups]
        assigned = {f for c in chunks for f in c}
        leftover = [f for f in fields_to_query if f not in assigned]
        for idx_l, f in enumerate(leftover):
            chunks[idx_l % len(chunks)].append(f)
        chunks = [c for c in chunks if c]

        can_inline_ts = (
            use_approx_top_k
            and want_time_series
            and (req.chart_metric or "requests") == "requests"
            and norm_interval in {"1 minute", "5 minutes", "1 hour", "1 day"}
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            chunk_futs = [
                trace.submit(
                    ex,
                    f"top_n_chunk.{idx}",
                    partial(
                        fetch_field_chunk,
                        c,
                        include_bots=("ua" in c),
                        include_ts=(can_inline_ts and idx == len(chunks) - 1),
                    ),
                    source="fact",
                )
                for idx, c in enumerate(chunks)
            ]
            cnt_fut = (
                None
                if (want_time_series or use_approx_top_k)
                else trace.submit(
                    ex,
                    "count",
                    lambda: service.client.execute(
                        f"SELECT count() AS c FROM request_facts WHERE {where_clause}", params
                    ),
                    source="fact",
                )
            )
            ts_fut = (
                trace.submit(
                    ex,
                    "time_series",
                    lambda: _facts_time_series_with_count(
                        service,
                        start_time,
                        end_time,
                        norm_interval,
                        req.chart_metric or "requests",
                        where_clause,
                        params,
                    ),
                    source="fact",
                )
                if (want_time_series and not can_inline_ts)
                else None
            )
            field_results: dict[str, list[tuple[str, int]]] = {}
            for cf in chunk_futs:
                field_results.update(cf.result())
            if ts_fut is not None:
                time_series, total_count = ts_fut.result()
            elif can_inline_ts:
                time_series = inline_ts_buckets
                total_count = chunk_total_counts[0] if chunk_total_counts else int(sum(p.value for p in time_series))
            elif use_approx_top_k and chunk_total_counts:
                time_series = []
                total_count = chunk_total_counts[0]
            else:
                time_series = []
                total_rows_result = cnt_fut.result() if cnt_fut is not None else []
                total_count = int(total_rows_result[0]["c"]) if total_rows_result else 0
        if "ua" in fields_to_query and start_time is not None and end_time is not None:
            from backend.high_scale import security as hs_security

            sec_scope = hs_security._scope(service, req, start_time, end_time)
            bot_memo_key = digest_cache_key(
                {
                    "ep": "hs_sec_top_bots_inner",
                    "cid": client_cache_token(service.client),
                    "s": sec_scope.start.isoformat(),
                    "e": sec_scope.end.isoformat(),
                    "n": 10,
                    "w": sec_scope.where,
                    "p": sorted((k, str(v)) for k, v in sec_scope.params.items()),
                },
                {"name": service.service_id, "service_id": service.service_id},
            )
            ngwaf_counts: dict[str, int] = {}
            ngwaf_cats: dict[str, str] = {}
            for raw_v, cnt in ngwaf_bot_rows:
                bot_name, _, bot_cat = raw_v.partition("\x1f")
                if not bot_name:
                    continue
                ngwaf_counts[bot_name] = ngwaf_counts.get(bot_name, 0) + cnt
                if bot_cat and not ngwaf_cats.get(bot_name):
                    ngwaf_cats[bot_name] = bot_cat
            sorted_ngwaf = sorted(ngwaf_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
            hs_security._security_memo.put(
                bot_memo_key,
                {
                    "bots": hs_security.classify_ua_counts(ua_bot_rows, 10),
                    "ngwaf_bots": [
                        hs_security.shape_security_ngwaf_top_bot_row(b_name, ngwaf_cats.get(b_name, ""), b_cnt)
                        for b_name, b_cnt in sorted_ngwaf
                    ],
                },
            )
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futs = [trace.submit(ex, f"top_n.{f}", partial(fetch_field, f), source="fact") for f in fields_to_query]
            field_results = dict(f.result() for f in futs)

        total_rows_result = trace.run(
            "count",
            lambda: service.client.execute(f"SELECT count() AS c FROM request_facts WHERE {where_clause}", params),
            source="fact",
        )
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

        total_count = int(total_rows_result[0]["c"]) if total_rows_result else 0

    data = {
        field: FieldAggregate(
            top=[FieldTopEntry(value=v, count=c) for v, c in field_results.get(field, [])], total=total_count
        )
        for field in top_fields
    }

    map_data = [MapPoint(country=v, count=c) for v, c in field_results.get("country", [])] if want_map else []

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


def _extract_fact_field_value(row: Mapping[str, Any], field: str) -> str:
    col_name = _FIELD_DIMENSIONS[field]
    if col_name in {"client_ip", "country", "url", "cmcd"}:
        return str(row.get(col_name) or "")
    cf = row.get("custom_fields")
    raw_val = str(cf.get(col_name) or "") if isinstance(cf, Mapping) else str(row.get(col_name) or "")
    if not raw_val:
        return ""
    if col_name in {"age", "ttl"}:
        try:
            return str(int(round(float(raw_val))))
        except (ValueError, TypeError):
            return "0"
    return raw_val


def _exact_end_fact_rows(
    service: HighScaleService,
    start: datetime | None,
    end: datetime | None,
    fields: Iterable[str] = (),
    *,
    watermark: Any = None,
) -> list[dict[str, Any]]:
    if end is None or (start is not None and end < start):
        return []
    if watermark is not None and getattr(watermark, "coverage_end", None) is not None and end > watermark.coverage_end:
        return []
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    if (
        is_real_ch
        and start is not None
        and (end - start) >= timedelta(hours=24)
        and end.second == 0
        and end.microsecond == 0
    ):
        return []
    unique_fields = [f for f in dict.fromkeys(fields) if f in _FIELD_DIMENSIONS]
    select_cols = ["event_timestamp AS exact_end_ts"]
    for f in unique_fields:
        select_cols.append(f"{_field_sql_column(f)} AS f_{f}")
    clauses = [*visible_fact_clauses("request"), "event_timestamp = {end_exact:DateTime64(3)}"]
    params: dict[str, Any] = {"service_id": service.service_id, "end_exact": end}
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    try:
        rows = service.client.execute(
            f"SELECT {', '.join(select_cols)} FROM request_facts WHERE {' AND '.join(clauses)}",
            params,
        )
    except Exception:
        return []
    matched: list[dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, Mapping):
            continue
        if "exact_end_ts" in r:
            matched.append(dict(r))
        elif "event_timestamp" in r:
            raw_ts = r["event_timestamp"]
            ts = raw_ts if isinstance(raw_ts, datetime) else _range_value(str(raw_ts))
            if ts == end:
                matched.append(dict(r))
    return matched


def _bucket_start_for_interval(ts: datetime, norm_interval: str) -> datetime:
    dt = ts.astimezone(UTC)
    if norm_interval == "1 second":
        return dt.replace(microsecond=0)
    if norm_interval == "5 minutes":
        return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
    if norm_interval == "1 hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if norm_interval == "1 day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(second=0, microsecond=0)


def _merge_top_values_with_exact_end(
    base_top: Iterable[tuple[str, int]], exact_rows: list[dict[str, Any]], field: str, limit: int = 10
) -> list[tuple[str, int]]:
    counts: dict[str, int] = {str(val): int(cnt) for val, cnt in base_top}
    alias_key = f"f_{field}"
    for row in exact_rows:
        if alias_key in row:
            val = str(row.get(alias_key) or "")
        else:
            val = _extract_fact_field_value(row, field)
        if val != "":
            counts[val] = counts.get(val, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


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
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    start = _range_value(start_time)
    end = _range_value(end_time)
    norm_interval = _normalize_chart_interval(req.chart_interval)
    if (
        (req.chart_interval is None or "chart_interval" not in getattr(req, "model_fields_set", set()))
        and is_real_ch
        and start is not None
        and end is not None
        and (end - start) >= timedelta(hours=24)
    ):
        norm_interval = "1 hour"
    trace = HighScaleSections(service.service_id, "dashboard")
    request_watermark = trace.run("watermark", lambda: service.watermark_for("request"), activity="overhead")
    cache_key: str | None = None
    if _HS_DASHBOARD_CACHE_TTL > 0 and start is not None and end is not None:
        raw_filters = (
            req.filters.model_dump() if req.filters and hasattr(req.filters, "model_dump") else (req.filters or {})
        )
        cache_key = digest_cache_key(
            {
                "ep": "hs_dashboard_aggregates",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "wm": (_watermark_extent(request_watermark.coverage_end), request_watermark.last_visible_event_id),
                "s": normalize_cache_bound(start),
                "e": normalize_cache_bound(end),
                "f": serialize_filters_for_key(raw_filters),
                "ci": norm_interval,
                "cm": req.chart_metric,
                "fl": sorted(req.fields) if req.fields is not None else None,
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

    def _compute_agg() -> AggregatesResponse:
        if cache_key is not None and (cached_inner := _dashboard_memo.get_with_telemetry(cache_key)) is not None:
            return AggregatesResponse.with_telemetry(**cached_inner)
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

        def _run_agg_batch() -> tuple[dict[str, Any], list[dict[str, Any]]]:
            if hasattr(service.client, "responses") or (
                is_real_ch
                and start is not None
                and end is not None
                and (end - start) >= timedelta(hours=24)
                and end.second == 0
                and end.microsecond == 0
            ):
                return (
                    _aggregates_batch(
                        service,
                        start_time=start_time,
                        end_time=end_time,
                        fields=all_fields_to_query,
                        watermark=request_watermark,
                    ),
                    _exact_end_fact_rows(service, start, end, all_fields_to_query, watermark=request_watermark),
                )
            from backend.utils.telemetry.metrics import get_queries

            get_queries()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as inner_pool:
                agg_fut = inner_pool.submit(
                    contextvars.copy_context().run,
                    partial(
                        _aggregates_batch,
                        service,
                        start_time=start_time,
                        end_time=end_time,
                        fields=all_fields_to_query,
                        watermark=request_watermark,
                    ),
                )
                exact_fut = inner_pool.submit(
                    contextvars.copy_context().run,
                    partial(
                        _exact_end_fact_rows, service, start, end, all_fields_to_query, watermark=request_watermark
                    ),
                )
                return agg_fut.result(), exact_fut.result()

        parallel_projection_ts = want_time_series and not (req.chart_metric and req.chart_metric != "requests")
        if parallel_projection_ts and not hasattr(service.client, "responses"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                batch_fut = trace.submit(pool, "aggregates_batch", _run_agg_batch, source="projection")
                ts_fut = trace.submit(
                    pool,
                    "time_series",
                    partial(_time_series, service, start_time, end_time, norm_interval),
                    source="projection",
                )
                responses, exact_end_rows = batch_fut.result()
                raw_projection_ts = ts_fut.result()
        else:
            responses, exact_end_rows = trace.run("aggregates_batch", _run_agg_batch, source="projection")
            raw_projection_ts = None

        extra_end_count = len(exact_end_rows)
        total_request_count = responses["url"].request_count + extra_end_count

        data = {
            field: FieldAggregate(
                top=[
                    FieldTopEntry(value=value, count=count)
                    for value, count in (
                        _merge_top_values_with_exact_end(responses[field].top_values, exact_end_rows, field)
                        if extra_end_count
                        else responses[field].top_values
                    )
                ],
                total=responses[field].request_count + extra_end_count,
            )
            for field in top_fields
        }
        map_response = responses.get("country")
        map_top_values = (
            _merge_top_values_with_exact_end(map_response.top_values, exact_end_rows, "country")
            if map_response is not None and extra_end_count
            else (map_response.top_values if map_response is not None else ())
        )
        map_data = (
            [MapPoint(country=value, count=count) for value, count in map_top_values]
            if want_map and map_response is not None
            else []
        )
        if not want_time_series:
            time_series = []
        elif req.chart_metric and req.chart_metric != "requests":
            start_ts = _range_value(start_time)
            end_ts = _range_value(end_time)
            clauses = list(visible_fact_clauses("request"))
            params: dict[str, Any] = {"service_id": service.service_id}
            if start_ts is not None and end_ts is not None:
                clauses.append("event_timestamp >= {start:DateTime64(3)}")
                clauses.append("event_timestamp <= {end_upper:DateTime64(3)}")
                params.update({"start": start_ts, "end_upper": end_ts})
            time_series = trace.run(
                "time_series",
                lambda: _facts_time_series(
                    service, start_time, end_time, norm_interval, req.chart_metric, " AND ".join(clauses), params
                ),
                source="fact",
            )
        else:
            time_series = (
                raw_projection_ts
                if raw_projection_ts is not None
                else trace.run(
                    "time_series",
                    lambda: _time_series(service, start_time, end_time, norm_interval),
                    source="projection",
                )
            )
            if extra_end_count and end is not None:
                end_bucket_dt = _bucket_start_for_interval(end, norm_interval)
                end_bucket_iso = safe_iso(end_bucket_dt) or end_bucket_dt.isoformat()
                merged_ts: list[TimeSeriesPoint] = []
                matched_bucket = False
                for pt in time_series:
                    if pt.time == end_bucket_iso:
                        merged_ts.append(TimeSeriesPoint(time=pt.time, value=float(pt.value) + float(extra_end_count)))
                        matched_bucket = True
                    else:
                        merged_ts.append(pt)
                if not matched_bucket:
                    merged_ts.append(TimeSeriesPoint(time=end_bucket_iso, value=float(extra_end_count)))
                time_series = merged_ts
        resp = AggregatesResponse.with_telemetry(
            data=data,
            time_series=time_series,
            map_data=map_data,
            where_clause="high-scale ClickHouse",
            section_timings=trace.timings(),
            interval=norm_interval,
            metric=req.chart_metric,
            total_rows=total_request_count,
            total_rows_total=total_request_count,
            earliest_log_at=_watermark_extent(request_watermark.coverage_start),
            latest_log_at=_watermark_extent(request_watermark.coverage_end),
        )
        return _cache_agg_resp(cache_key, resp)

    if is_real_ch and cache_key is not None:
        resp_out, is_leader = coalesce(f"hs_dashboard_agg:{cache_key}", _compute_agg)
        if not is_leader:
            return resp_out.model_copy(update={"is_cached": True})
        return resp_out
    return _compute_agg()


aggregates = dashboard_aggregates


def bundle(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    from backend.high_scale import security as hs_security
    from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

    sections = set(req.sections) if req.sections else None
    want_aggregates = sections is None or bool(sections & {"core", "topten"})
    want_bots = sections is None or "bots" in sections
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    res_aggregates: AggregatesResponse | None
    res_top_bots: Any
    if want_aggregates and want_bots and not hasattr(service.client, "responses") and not (is_real_ch and req.filters):
        get_queries()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            agg_fut = pool.submit(contextvars.copy_context().run, aggregates, service, req, start_time, end_time)
            bots_fut = pool.submit(
                contextvars.copy_context().run, hs_security.top_bots, service, req, start_time, end_time
            )
            res_aggregates = agg_fut.result()
            res_top_bots = bots_fut.result()
    else:
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
    all_cached = (
        (not want_aggregates or bool(getattr(res_aggregates, "is_cached", False)))
        and (not want_bots or bool(getattr(res_top_bots, "is_cached", False)))
        and (want_aggregates or want_bots)
    )
    bundle_obs = (
        cache_result({}, state="hit", prewarmer=False)
        if all_cached
        else cache_result({}, state="bypass", prewarmer=False)
    )
    return {
        **bundle_obs,
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

    partial_minutes = (
        start is not None
        and end is not None
        and any(bound.second != 0 or bound.microsecond != 0 for bound in (start, end))
    )
    if not req.search and not filter_clauses and key in _FIELD_DIMENSIONS and not partial_minutes:
        agg = _aggregate(
            service,
            start_time=start_time,
            end_time=end_time,
            dimension=_FIELD_DIMENSIONS[key],
            watermark=service.watermark_for("request"),
        )
        exact_end_rows = _exact_end_fact_rows(service, start, end, [key])
        merged_top = (
            _merge_top_values_with_exact_end(agg.top_values, exact_end_rows, key, limit=limit)
            if exact_end_rows
            else agg.top_values[:limit]
        )
        vals: list[dict[str, Any]] = [{"value": str(val), "count": int(cnt)} for val, cnt in merged_top]
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

    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("event_timestamp <= {end_upper:DateTime64(3)}")
        params["end_upper"] = end

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
    if start is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("event_timestamp <= {end_upper:DateTime64(3)}")
        params["end_upper"] = end

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
