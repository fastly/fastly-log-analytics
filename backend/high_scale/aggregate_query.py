"""ClickHouse-backed triage aggregate reads for the isolated high-scale plane."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from backend.high_scale.aggregates import AggregateRequest, AggregateResponse
from backend.high_scale.archive_models import ServingWatermark


class AggregateQueryClient(Protocol):
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...


_DOMAIN_TABLES = {
    "request": ("request_aggregates", "request_count"),
    "rum_vitals": ("rum_vitals_aggregates", "event_count"),
    "rum_errors": ("rum_error_aggregates", "error_count"),
    "cmcd": ("cmcd_aggregates", "event_count"),
}
_DEFAULT_DIMENSIONS = {
    "request": "url",
    "rum_vitals": "metric_name",
    "rum_errors": "error_message",
    "cmcd": "cmcd_session",
}


def query_clickhouse_aggregate(
    client: AggregateQueryClient,
    request: AggregateRequest,
    *,
    watermark: ServingWatermark,
    now: datetime | None = None,
) -> AggregateResponse:
    """Read visible aggregate rows while preserving the aggregate contract."""

    request.validate()
    watermark.validate()
    if watermark.service_id != request.service_id or watermark.domain != request.domain:
        raise ValueError("watermark does not belong to this service and domain")
    table, metric = _DOMAIN_TABLES[request.domain]
    dimension = request.dimension or _DEFAULT_DIMENSIONS[request.domain]
    if not dimension or any(char not in "abcdefghijklmnopqrstuvwxyz_" for char in dimension):
        raise ValueError("aggregate dimension must be an internal identifier")

    clauses = [
        "service_id={service_id:String}",
        "dimension={dimension:String}",
        "publication_state='visible'",
    ]
    params: dict[str, Any] = {
        "service_id": request.service_id,
        "dimension": dimension,
    }
    if request.start is not None and request.end is not None:
        start = request.start.astimezone(UTC)
        end = request.end.astimezone(UTC)
        clauses.extend(["bucket_start >= {start:DateTime}", "bucket_start < {end:DateTime}"])
        params.update({"start": start, "end": end})

    rows = client.execute(
        f"SELECT value, sum({metric}) AS aggregate_count "
        f"FROM {table} WHERE {' AND '.join(clauses)} "
        "GROUP BY value ORDER BY aggregate_count DESC, value ASC",
        params,
    )
    values = tuple((str(row["value"]), max(0, int(row["aggregate_count"]))) for row in rows)
    request_count = sum(count for _, count in values)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
    return AggregateResponse(
        service_id=request.service_id,
        domain=request.domain,
        request_count=request_count,
        top_values=values,
        coverage=1.0 if values or watermark.coverage_end is not None else 0.0,
        watermark=watermark,
        freshness_lag_seconds=freshness,
        exact=True,
        approximation_error=None,
        counters=((f"{request.domain}_events", request_count),),
    )
