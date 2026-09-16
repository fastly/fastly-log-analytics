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
}


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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
