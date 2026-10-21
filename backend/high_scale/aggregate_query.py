"""ClickHouse-backed triage aggregate reads for the isolated high-scale plane."""

from __future__ import annotations

import re
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
_INTERNAL_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
DEFAULT_AGGREGATE_DIMENSIONS = {
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
    dimension = request.dimension or DEFAULT_AGGREGATE_DIMENSIONS[request.domain]
    if not dimension or _INTERNAL_IDENTIFIER.fullmatch(dimension) is None:
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
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", "bucket_start <= {end:DateTime64(3)}"])
        params.update({"start": start, "end": end})

    from backend.core.clickhouse_client import ClickHouseError

    try:
        totals = client.execute(
            f"SELECT sum({metric}) AS total_count FROM {table} WHERE {' AND '.join(clauses)}",
            params,
        )
        rows = client.execute(
            f"SELECT value, sum({metric}) AS aggregate_count "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            "GROUP BY value ORDER BY aggregate_count DESC, value ASC LIMIT 10",
            params,
        )
    except ClickHouseError as e:
        raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")
    values = tuple((str(row["value"]), max(0, int(row["aggregate_count"]))) for row in rows)
    total_row = totals[0] if totals else {}
    request_count = max(0, int(total_row.get("total_count") or 0))
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


def query_clickhouse_aggregates_batch(
    client: AggregateQueryClient,
    *,
    service_id: str,
    domain: str,
    dimensions: list[str] | tuple[str, ...],
    start: datetime | None,
    end: datetime | None,
    watermark: ServingWatermark,
    now: datetime | None = None,
) -> dict[str, AggregateResponse]:
    """Read visible aggregate rows for multiple dimensions in two batched ClickHouse queries."""
    unique_dims = list(dict.fromkeys(dimensions))
    if not unique_dims:
        return {}
    if len(unique_dims) == 1:
        dim = unique_dims[0]
        return {
            dim: query_clickhouse_aggregate(
                client,
                AggregateRequest(
                    service_id=service_id,
                    domain=domain,
                    start=start,
                    end=end,
                    dimension=dim,
                ),
                watermark=watermark,
                now=now,
            )
        }

    watermark.validate()
    if watermark.service_id != service_id or watermark.domain != domain:
        raise ValueError("watermark does not belong to this service and domain")
    if domain not in _DOMAIN_TABLES:
        raise ValueError(f"unsupported aggregate domain: {domain}")
    table, metric = _DOMAIN_TABLES[domain]

    dim_placeholders: list[str] = []
    params: dict[str, Any] = {"service_id": service_id}
    for idx, dim in enumerate(unique_dims):
        if not dim or _INTERNAL_IDENTIFIER.fullmatch(dim) is None:
            raise ValueError("aggregate dimension must be an internal identifier")
        key = f"dim_{idx}"
        dim_placeholders.append(f"{{{key}:String}}")
        params[key] = dim

    clauses = [
        "service_id={service_id:String}",
        f"dimension IN ({', '.join(dim_placeholders)})",
        "publication_state='visible'",
    ]
    if start is not None and end is not None:
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", "bucket_start <= {end:DateTime64(3)}"])
        params.update({"start": start_utc, "end": end_utc})

    from backend.core.clickhouse_client import ClickHouseError

    try:
        totals_rows = client.execute(
            f"SELECT dimension, sum({metric}) AS total_count "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            "GROUP BY dimension",
            params,
        )
        if totals_rows and "dimension" not in totals_rows[0]:
            return {
                dim: query_clickhouse_aggregate(
                    client,
                    AggregateRequest(
                        service_id=service_id,
                        domain=domain,
                        start=start,
                        end=end,
                        dimension=dim,
                    ),
                    watermark=watermark,
                    now=now,
                )
                for dim in unique_dims
            }
        top_rows = client.execute(
            f"SELECT dimension, value, sum({metric}) AS aggregate_count "
            f"FROM {table} WHERE {' AND '.join(clauses)} "
            "GROUP BY dimension, value "
            "ORDER BY dimension ASC, aggregate_count DESC, value ASC "
            "LIMIT 10 BY dimension",
            params,
        )
        if top_rows and "dimension" not in top_rows[0]:
            return {
                dim: query_clickhouse_aggregate(
                    client,
                    AggregateRequest(
                        service_id=service_id,
                        domain=domain,
                        start=start,
                        end=end,
                        dimension=dim,
                    ),
                    watermark=watermark,
                    now=now,
                )
                for dim in unique_dims
            }
    except ClickHouseError as e:
        raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")

    totals_by_dim: dict[str, int] = {}
    for row in totals_rows:
        d = str(row.get("dimension") or "")
        totals_by_dim[d] = max(0, int(row.get("total_count") or 0))

    tops_by_dim: dict[str, list[tuple[str, int]]] = {d: [] for d in unique_dims}
    for row in top_rows:
        d = str(row.get("dimension") or "")
        if d in tops_by_dim and len(tops_by_dim[d]) < 10:
            tops_by_dim[d].append((str(row["value"]), max(0, int(row["aggregate_count"]))))

    observed = (now or datetime.now(UTC)).astimezone(UTC)
    freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0

    out: dict[str, AggregateResponse] = {}
    for dim in unique_dims:
        values = tuple(tops_by_dim.get(dim, ()))
        req_count = totals_by_dim.get(dim, 0)
        out[dim] = AggregateResponse(
            service_id=service_id,
            domain=domain,
            request_count=req_count,
            top_values=values,
            coverage=1.0 if values or watermark.coverage_end is not None else 0.0,
            watermark=watermark,
            freshness_lag_seconds=freshness,
            exact=True,
            approximation_error=None,
            counters=((f"{domain}_events", req_count),),
        )
    return out
