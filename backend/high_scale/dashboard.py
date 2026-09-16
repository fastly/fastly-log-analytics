"""Dashboard-shaped reads for services owned by the high-scale data plane."""

from __future__ import annotations

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
    latest: dict[str, str | None] = {}
    for domain, table in _HEADER_TABLES.items():
        rows = service.client.execute(
            f"SELECT count() AS total_rows, max(event_timestamp) AS latest_log_at "
            f"FROM {table} "
            "WHERE service_id={service_id:String} "
            "AND publication_state='visible' "
            "AND batch_id IN ("
            "SELECT batch_id FROM high_scale_batch_publications FINAL "
            "WHERE service_id={service_id:String} AND domain={domain:String} "
            "AND publication_state='visible'"
            ")",
            {"service_id": service.service_id, "domain": domain},
        )
        row = rows[0] if rows else {}
        total_rows = int(row.get("total_rows") or 0)
        latest_value: object = row.get("latest_log_at")
        if isinstance(latest_value, datetime):
            latest_value = latest_value.isoformat()
        totals[domain] = total_rows
        latest[domain] = str(latest_value) if latest_value is not None else None

    rum_total = totals["rum_vitals"] + totals["rum_errors"]
    rum_latest_values = [latest["rum_vitals"], latest["rum_errors"]]
    rum_latest = max((value for value in rum_latest_values if value is not None), default=None)
    request_total = totals["request"]
    request_latest = latest["request"]
    return {
        "request": {
            "latest_log_at": request_latest,
            "total_rows": request_total,
            "last_sync_at": None,
        },
        "rum": {
            "latest_log_at": rum_latest,
            "total_rows": rum_total,
            "last_sync_at": None,
        },
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
        "publication_state='visible'",
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
    return [TimeSeriesPoint(time=str(row["bucket_start"]), value=float(row["value"])) for row in rows]


def aggregates(service: HighScaleService, req: AggregatesRequest, start_time: str | None, end_time: str | None):
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
    return {
        "aggregates": aggregates(service, req, start_time, end_time),
        "top_bots": SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[]),
    }
