"""ClickHouse-backed triage aggregate reads for the isolated high-scale plane."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from backend.high_scale._common import visible_publication_fence_sql
from backend.high_scale.archive_models import ServingWatermark


@dataclass(frozen=True)
class AggregateResponse:
    service_id: str
    domain: str
    request_count: int
    top_values: tuple[tuple[str, int], ...]
    coverage: float
    watermark: ServingWatermark
    freshness_lag_seconds: float
    exact: bool
    approximation_error: float | None
    counters: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class AggregateRequest:
    service_id: str
    domain: str
    start: datetime | None = None
    end: datetime | None = None
    dimension: str | None = None

    def validate(self) -> None:
        if not self.service_id.strip():
            raise ValueError("service id is required")
        if self.domain not in {"request", "rum_vitals", "rum_errors", "cmcd"}:
            raise ValueError("unsupported aggregate domain")
        if (self.start is None) != (self.end is None):
            raise ValueError("aggregate range must include both start and end")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("aggregate range is inverted")


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
_FACT_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}
_REQUEST_CUSTOM_FIELD_DIMENSIONS = (
    "asn",
    "host",
    "method",
    "status",
    "cache",
    "proto",
    "ua",
    "referer",
    "cookie_session",
    "resp_header_content_encoding",
    "ttl",
    "age",
    "hits",
    "digest",
    "city",
    "region",
    "metro",
    "transport",
    "c_speed",
    "c_type",
    "pop",
    "backend",
    "edge",
    "server_region",
    "tls",
    "is_ipv6",
    "conn_requests",
    "waf",
    "waf_resp",
    "waf_ms",
    "p_type",
    "p_desc",
    "ja3",
    "ja4",
    "tls_ciphers_sha",
    "io_input_format",
    "io_output_format",
)
_FACT_DIMENSION_EXPRESSIONS = {
    "request": {
        "url": "url",
        "country": "country",
        "client_ip": "client_ip",
        **{dimension: f"custom_fields['{dimension}']" for dimension in _REQUEST_CUSTOM_FIELD_DIMENSIONS},
        "cmcd_session": "cmcd['sid']",
    },
    "rum_vitals": {"metric_name": "metric_name"},
    "rum_errors": {"error_message": "error_message"},
    "cmcd": {"cmcd_session": "cmcd['sid']"},
}


def _is_partial_minute_window(start: datetime | None, end: datetime | None) -> bool:
    if start is None or end is None:
        return False
    return any(value.astimezone(UTC).second != 0 or value.astimezone(UTC).microsecond != 0 for value in (start, end))


def _query_fact_aggregates(
    client: AggregateQueryClient,
    *,
    service_id: str,
    domain: str,
    dimensions: list[str],
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    table = _FACT_TABLES[domain]
    expressions = _FACT_DIMENSION_EXPRESSIONS[domain]
    if any(dimension not in expressions for dimension in dimensions):
        raise ValueError("aggregate dimension cannot be read exactly from event-time facts")

    dimension_values: list[str] = []
    params: dict[str, Any] = {
        "service_id": service_id,
        "start": start.astimezone(UTC),
        "end": end.astimezone(UTC),
    }
    for idx, dimension in enumerate(dimensions):
        key = f"fact_dimension_{idx}"
        dimension_values.append(f"tuple({{{key}:String}}, toString({expressions[dimension]}))")
        params[key] = dimension

    end_operator = "<" if domain == "request" else "<="
    clauses = [
        "service_id={service_id:String}",
        "event_timestamp >= {start:DateTime64(3)}",
        f"event_timestamp {end_operator} {{end:DateTime64(3)}}",
        visible_publication_fence_sql(domain),
    ]
    fact_values = (
        "SELECT tupleElement(dimension_value, 1) AS dimension, "
        "tupleElement(dimension_value, 2) AS value "
        f"FROM {table} ARRAY JOIN [{', '.join(dimension_values)}] AS dimension_value "
        f"WHERE {' AND '.join(clauses)}"
    )
    totals = client.execute(
        f"SELECT dimension, count() AS total_count FROM ({fact_values}) GROUP BY dimension",
        params,
    )
    rows = client.execute(
        f"SELECT dimension, value, count() AS aggregate_count FROM ({fact_values}) "
        "GROUP BY dimension, value "
        "ORDER BY dimension ASC, aggregate_count DESC, value ASC "
        "LIMIT 10 BY dimension",
        params,
    )
    return totals, rows


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
        visible_publication_fence_sql(f"{request.domain}_aggregate"),
    ]
    params: dict[str, Any] = {
        "service_id": request.service_id,
        "dimension": dimension,
    }
    partial_minute_window = _is_partial_minute_window(request.start, request.end)
    if partial_minute_window:
        if dimension not in _FACT_DIMENSION_EXPRESSIONS[request.domain]:
            raise ValueError("aggregate dimension cannot be read exactly from event-time facts")
        assert request.start is not None and request.end is not None
    elif request.start is not None and request.end is not None:
        start = request.start.astimezone(UTC)
        end = request.end.astimezone(UTC)
        end_operator = "<" if request.domain == "request" else "<="
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", f"bucket_start {end_operator} {{end:DateTime64(3)}}"])
        params.update({"start": start, "end": end})

    from backend.core.clickhouse_client import ClickHouseError

    try:
        if partial_minute_window:
            assert request.start is not None and request.end is not None
            totals, rows = _query_fact_aggregates(
                client,
                service_id=request.service_id,
                domain=request.domain,
                dimensions=[dimension],
                start=request.start,
                end=request.end,
            )
        else:
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
    partial_minute_window = _is_partial_minute_window(start, end)
    from backend.core.clickhouse_client import ClickHouseError

    dim_placeholders: list[str] = []
    params: dict[str, Any] = {"service_id": service_id}
    for idx, dim in enumerate(unique_dims):
        if not dim or _INTERNAL_IDENTIFIER.fullmatch(dim) is None:
            raise ValueError("aggregate dimension must be an internal identifier")
        key = f"dim_{idx}"
        dim_placeholders.append(f"{{{key}:String}}")
        params[key] = dim

    if partial_minute_window:
        assert start is not None and end is not None

        try:
            totals_rows, top_rows = _query_fact_aggregates(
                client,
                service_id=service_id,
                domain=domain,
                dimensions=unique_dims,
                start=start,
                end=end,
            )
        except ClickHouseError as e:
            raise ValueError(f"ClickHouse server query failed (S3/FOS access credentials may be expired): {e}")

        fact_totals_by_dim = {
            str(row.get("dimension") or ""): max(0, int(row.get("total_count") or 0)) for row in totals_rows
        }
        fact_tops_by_dim: dict[str, list[tuple[str, int]]] = {dimension: [] for dimension in unique_dims}
        for row in top_rows:
            dimension = str(row.get("dimension") or "")
            if dimension in fact_tops_by_dim and len(fact_tops_by_dim[dimension]) < 10:
                fact_tops_by_dim[dimension].append((str(row["value"]), max(0, int(row["aggregate_count"]))))

        observed = (now or datetime.now(UTC)).astimezone(UTC)
        freshness = max(0.0, (observed - watermark.coverage_end).total_seconds()) if watermark.coverage_end else 0.0
        return {
            dimension: AggregateResponse(
                service_id=service_id,
                domain=domain,
                request_count=fact_totals_by_dim.get(dimension, 0),
                top_values=tuple(fact_tops_by_dim.get(dimension, ())),
                coverage=1.0 if fact_tops_by_dim.get(dimension) or watermark.coverage_end is not None else 0.0,
                watermark=watermark,
                freshness_lag_seconds=freshness,
                exact=True,
                approximation_error=None,
                counters=((f"{domain}_events", fact_totals_by_dim.get(dimension, 0)),),
            )
            for dimension in unique_dims
        }

    clauses = [
        "service_id={service_id:String}",
        f"dimension IN ({', '.join(dim_placeholders)})",
        visible_publication_fence_sql(f"{domain}_aggregate"),
    ]
    if start is not None and end is not None:
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        end_operator = "<" if domain == "request" else "<="
        clauses.extend(["bucket_start >= {start:DateTime64(3)}", f"bucket_start {end_operator} {{end:DateTime64(3)}}"])
        params.update({"start": start_utc, "end": end_utc})

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
