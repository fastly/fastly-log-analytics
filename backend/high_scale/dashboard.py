"""Dashboard-shaped reads for services owned by the high-scale data plane."""

from __future__ import annotations

import concurrent.futures
from datetime import datetime
from typing import Any

from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import (
    AggregatesRequest,
    AggregatesResponse,
    FieldAggregate,
    FieldTopEntry,
    MapPoint,
    TimeSeriesPoint,
)
from backend.models.security import SecurityTopBotsResponse
from backend.utils.date_utils import safe_iso

_FIELD_DIMENSIONS = {
    "url": "url",
    "country": "country",
    "ip": "client_ip",
    "asn": "asn",
    "host": "host",
    "method": "method",
    "status": "status",
    "cache": "cache",
    "proto": "proto",
    "ua": "ua",
    "referer": "referer",
    "cookie_session": "cookie_session",
    "resp_header_content_encoding": "resp_header_content_encoding",
    "ttl": "ttl",
    "age": "age",
    "hits": "hits",
    "digest": "digest",
    "city": "city",
    "region": "region",
    "metro": "metro",
    "transport": "transport",
    "c_speed": "c_speed",
    "c_type": "c_type",
    "pop": "pop",
    "backend": "backend",
    "edge": "edge",
    "server_region": "server_region",
    "tls": "tls",
    "is_ipv6": "is_ipv6",
    "conn_requests": "conn_requests",
    "waf": "waf",
    "waf_resp": "waf_resp",
    "waf_ms": "waf_ms",
    "p_type": "p_type",
    "p_desc": "p_desc",
    "ja3": "ja3",
    "ja4": "ja4",
    "tls_ciphers_sha": "tls_ciphers_sha",
    "io_input_format": "io_input_format",
    "io_output_format": "io_output_format",
}
_HEADER_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
}


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def header_metrics(service: HighScaleService) -> dict[str, Any]:
    """Return visible ClickHouse row counts and event-time extents for the header."""
    totals: dict[str, int] = {}
    earliest: dict[str, str | None] = {}
    latest: dict[str, str | None] = {}
    for domain, table in _HEADER_TABLES.items():
        rows = service.client.execute(
            f"SELECT count() AS total_rows, min(event_timestamp) AS earliest_log_at, max(event_timestamp) AS latest_log_at "
            f"FROM {table} "
            "WHERE service_id={service_id:String} "
            "AND publication_state = 'visible' "
            "AND batch_id IN ("
            "SELECT batch_id FROM high_scale_batch_publications FINAL "
            "WHERE service_id={service_id:String} AND domain={domain:String} "
            "AND publication_state = 'visible'"
            ")",
            {"service_id": service.service_id, "domain": domain},
        )
        row = rows[0] if rows else {}
        total_rows = int(row.get("total_rows") or 0)
        earliest_value: object = row.get("earliest_log_at")
        latest_value: object = row.get("latest_log_at")
        if total_rows == 0:
            earliest_value = None
            latest_value = None
        if isinstance(earliest_value, datetime):
            earliest_value = earliest_value.isoformat()
        if isinstance(latest_value, datetime):
            latest_value = latest_value.isoformat()
        totals[domain] = total_rows
        earliest[domain] = str(earliest_value) if earliest_value is not None else None
        latest[domain] = str(latest_value) if latest_value is not None else None

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
        from backend.core.metadata.cron_log import latest_cron_per_task

        latest_crons = latest_cron_per_task(service.service_id)
        for t in ("log_discovery", "commit", "ledger_sweep"):
            if t in latest_crons and latest_crons[t].get("started_at"):
                ts = str(latest_crons[t]["started_at"])
                if req_sync is None or ts > req_sync:
                    req_sync = ts
        for t in ("rum_discovery", "rum_sync", "ledger_rum_sweep"):
            if t in latest_crons and latest_crons[t].get("started_at"):
                ts = str(latest_crons[t]["started_at"])
                if rum_sync is None or ts > rum_sync:
                    rum_sync = ts
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


def _aggregate(
    service: HighScaleService,
    *,
    start_time: str | None,
    end_time: str | None,
    dimension: str,
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    return query_clickhouse_aggregate(
        service.client,
        AggregateRequest(
            service_id=service.service_id,
            domain="request",
            start=start,
            end=end,
            dimension=dimension,
        ),
        watermark=service.watermark_for("request"),
    )


def _time_series(service: HighScaleService, start_time: str | None, end_time: str | None) -> list[TimeSeriesPoint]:
    start = _range_value(start_time)
    end = _range_value(end_time)
    clauses = [
        "service_id={service_id:String}",
        "1=1",
    ]
    params: dict[str, Any] = {"service_id": service.service_id}
    if start is not None and end is not None:
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", "bucket_start < {end:DateTime64(3)}"])
        params.update({"start": start, "end": end})
    rows = service.client.execute(
        "SELECT bucket_start, sum(request_count) AS value "
        "FROM request_aggregates "
        f"WHERE {' AND '.join(clauses)} "
        "GROUP BY bucket_start ORDER BY bucket_start",
        params,
    )
    return [
        TimeSeriesPoint(time=safe_iso(row["bucket_start"]) or str(row["bucket_start"]), value=float(row["value"]))
        for row in rows
    ]


def _build_clickhouse_filters(filters: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if not filters:
        return "1=1", {}

    clauses = []
    params = {}

    for i, (field, config) in enumerate(filters.items()):
        if isinstance(config, dict):
            mode = config.get("mode", "include")
            values = config.get("values", [])
        else:
            mode = getattr(config, "mode", "include")
            values = getattr(config, "values", [])

        if values is None or not values:
            continue

        col_name = _FIELD_DIMENSIONS.get(field)
        if not col_name:
            continue

        if col_name in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            sql_col = col_name
        elif col_name in {"age", "ttl"}:
            sql_col = f"CAST(CAST(ROUND(toFloat64OrZero(custom_fields['{col_name}'])) AS INTEGER) AS VARCHAR)"
        else:
            sql_col = f"custom_fields['{col_name}']"

        op = "IN" if mode == "include" else "NOT IN"
        keys = []
        for j, val in enumerate(values):
            p_name = f"filter_{i}_{j}"
            params[p_name] = str(val)
            keys.append(f"{{{p_name}:String}}")

        if keys:
            clauses.append(f"{sql_col} {op} ({', '.join(keys)})")
        else:
            clauses.append("1=0" if mode == "include" else "1=1")

    where_sql = " AND ".join(clauses) if clauses else "1=1"
    return where_sql, params


def _filtered_aggregates(
    service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None
):
    start = _range_value(start_time)
    end = _range_value(end_time)

    if not req.filters:
        filter_dict = {}
    elif hasattr(req.filters, "model_dump"):
        filter_dict = req.filters.model_dump()
    elif hasattr(req.filters, "dict"):
        filter_dict = req.filters.dict()
    else:
        filter_dict = req.filters
    where_sql, filter_params = _build_clickhouse_filters(filter_dict)

    clauses = ["service_id={service_id:String}", "1=1", where_sql]

    params: dict[str, Any] = {"service_id": service.service_id, **filter_params}

    if start is not None and end is not None:
        clauses.append("event_timestamp >= {start:DateTime64(3)}")
        clauses.append("event_timestamp < {end:DateTime64(3)}")
        params.update({"start": start, "end": end})

    where_clause = " AND ".join(clauses)

    requested_fields = req.fields or list(_FIELD_DIMENSIONS)

    def fetch_field(field: str):
        if field not in _FIELD_DIMENSIONS:
            return field, []
        col_name = _FIELD_DIMENSIONS[field]

        if col_name in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            sql_col = col_name
        elif col_name in {"age", "ttl"}:
            sql_col = f"CAST(CAST(ROUND(toFloat64OrZero(custom_fields['{col_name}'])) AS INTEGER) AS VARCHAR)"
        else:
            sql_col = f"custom_fields['{col_name}']"

        q = f"SELECT {sql_col} AS v, count() AS c FROM request_facts WHERE {where_clause} AND {sql_col} != '' GROUP BY v ORDER BY c DESC LIMIT 10"
        try:
            rows = service.client.execute(q, params)
            return field, [(str(r["v"]), int(r["c"])) for r in rows]
        except Exception as e:
            import logging

            logging.getLogger(__name__).error("FILTER ERROR field %s: %s", field, e)
            return field, []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        field_results = dict(ex.map(fetch_field, requested_fields))

    try:
        total_rows_result = service.client.execute(
            f"SELECT count() AS c FROM request_facts WHERE {where_clause}", params
        )
        total_count = int(total_rows_result[0]["c"]) if total_rows_result else 0
    except Exception as e:
        import logging

        logging.getLogger(__name__).error("FILTER ERROR total: %s", e)
        total_count = 0

    data = {
        field: FieldAggregate(
            top=[FieldTopEntry(value=v, count=c) for v, c in top_values],
            total=total_count,
        )
        for field, top_values in field_results.items()
    }

    map_data = [MapPoint(country=v, count=c) for v, c in field_results.get("country", [])]

    time_series = []
    if req.include_time_series is not False:
        interval_sql = "toStartOfMinute(event_timestamp)"
        if req.chart_interval in ("1 second", "1 seconds"):
            interval_sql = "toStartOfSecond(event_timestamp)"
        elif req.chart_interval in ("5 minute", "5 minutes"):
            interval_sql = "toStartOfFiveMinutes(event_timestamp)"
        elif req.chart_interval in ("1 hour", "1 hours"):
            interval_sql = "toStartOfInterval(event_timestamp, INTERVAL 1 hour)"
        elif req.chart_interval in ("1 day", "1 days"):
            interval_sql = "toStartOfDay(event_timestamp)"

        metric = req.chart_metric or "requests"
        if metric == "5xx":
            metric_sql = "countIf(custom_fields['status'] >= '500' AND custom_fields['status'] <= '599')"
        elif metric == "4xx":
            metric_sql = "countIf(custom_fields['status'] >= '400' AND custom_fields['status'] <= '499')"
        elif metric == "hit_rate":
            metric_sql = "round(100.0 * countIf(custom_fields['cache'] IN ('HIT', 'HIT-CLUSTER', 'HIT-STALE')) / nullIf(count(), 0), 2)"
        else:
            metric_sql = "count()"

        ts_q = f"SELECT {interval_sql} AS bucket_start, {metric_sql} AS value FROM request_facts WHERE {where_clause} GROUP BY bucket_start ORDER BY bucket_start"
        try:
            ts_rows = service.client.execute(ts_q, params)
            time_series = [
                TimeSeriesPoint(time=safe_iso(r["bucket_start"]) or str(r["bucket_start"]), value=float(r["value"]))
                for r in ts_rows
            ]
        except Exception:
            time_series = []

    return AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse (filtered)",
        interval=req.chart_interval,
        metric=req.chart_metric,
        total_rows=total_count,
        total_rows_total=total_count,
        earliest_log_at=None,
        latest_log_at=None,
    )


def aggregates(service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None):
    if req.filters:
        return _filtered_aggregates(service, req, start_time, end_time)

    requested_fields = req.fields or list(_FIELD_DIMENSIONS)
    responses = {
        field: _aggregate(
            service,
            start_time=start_time,
            end_time=end_time,
            dimension=_FIELD_DIMENSIONS[field],
        )
        for field in requested_fields
        if field in _FIELD_DIMENSIONS
    }
    total = responses.get("url")
    if total is None:
        total = _aggregate(service, start_time=start_time, end_time=end_time, dimension="url")

    data = {
        field: FieldAggregate(
            top=[FieldTopEntry(value=value, count=count) for value, count in response.top_values],
            total=response.request_count,
        )
        for field, response in responses.items()
    }
    map_response = responses.get("country")
    map_data = (
        [MapPoint(country=value, count=count) for value, count in map_response.top_values]
        if map_response is not None
        else []
    )
    time_series = _time_series(service, start_time, end_time)
    return AggregatesResponse.with_telemetry(
        data=data,
        time_series=time_series,
        map_data=map_data,
        where_clause="high-scale ClickHouse",
        interval=req.chart_interval,
        metric=req.chart_metric,
        total_rows=total.request_count,
        total_rows_total=total.request_count,
        earliest_log_at=None,
        latest_log_at=None,
    )


def bundle(
    service: HighScaleService,
    req: AggregatesRequest,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

    res_aggregates = aggregates(service, req, start_time, end_time)
    res_top_bots = SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[])

    return {
        "aggregates": res_aggregates,
        "top_bots": res_top_bots,
        "debug_queries": get_queries(),
        "debug_calls": get_tracked_calls(),
        "debug_sqlite": list(get_sqlite_queries()),
    }
