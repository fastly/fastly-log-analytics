from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.performance import PerformanceAggregatesResponse


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _where(
    service_id: str,
    domain: str,
    start_time: str | None,
    end_time: str | None,
    *,
    dimension: str | None = None,
) -> tuple[str, dict[str, Any]]:
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={publication_domain:String} "
        "AND publication_state='visible')",
    ]
    params: dict[str, Any] = {
        "service_id": service_id,
        "publication_domain": domain,
    }
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start < {end:DateTime64(3)}")
        params["end"] = end
    if dimension is not None:
        clauses.append("dimension={dimension:String}")
        params["dimension"] = dimension
    return " AND ".join(clauses), params


def _weighted(column: str, count_column: str = "latency_count") -> str:
    return f"sum({column} * {count_column}) / nullIf(sum(if({column} IS NOT NULL, {count_column}, 0)), 0)"


def _dimension_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    select: str,
    order_by: str,
    limit: int | None = None,
    having: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    where, params = _where(
        service.service_id,
        "performance_dimensions",
        start_time,
        end_time,
        dimension=dimension,
    )
    suffix = f" HAVING {having}" if having else ""
    params.update(extra_params or {})
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " ORDER BY " + order_by + " LIMIT {limit:UInt32}"
    else:
        suffix += " ORDER BY " + order_by
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, {select} "
        f"FROM performance_minute_dimensions WHERE {where} GROUP BY value{suffix}",
        params,
    )


def _aggregate(
    service: HighScaleService,
    *,
    start_time: str | None,
    end_time: str | None,
    dimension: str,
):
    from backend.high_scale.aggregate_query import query_clickhouse_aggregate
    from backend.high_scale.aggregates import AggregateRequest

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


def performance_aggregates(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> PerformanceAggregatesResponse:
    ttl_response = _aggregate(service, start_time=start_time, end_time=end_time, dimension="ttl")

    buckets = {
        "0s": 0,
        "<10s": 0,
        "<30s": 0,
        "<1m": 0,
        "<5m": 0,
        "<10m": 0,
        "<1h": 0,
        "<24h": 0,
        ">24h": 0,
    }

    for val_str, count in ttl_response.top_values:
        try:
            ttl = int(val_str)
        except ValueError:
            continue

        if ttl <= 0:
            buckets["0s"] += count
        elif ttl <= 10:
            buckets["<10s"] += count
        elif ttl <= 30:
            buckets["<30s"] += count
        elif ttl <= 60:
            buckets["<1m"] += count
        elif ttl <= 300:
            buckets["<5m"] += count
        elif ttl <= 600:
            buckets["<10m"] += count
        elif ttl <= 3600:
            buckets["<1h"] += count
        elif ttl <= 86400:
            buckets["<24h"] += count
        else:
            buckets[">24h"] += count

    ttl_dist = [{"bucket": k, "count": v} for k, v in buckets.items() if v > 0]

    top_urls_rows = _dimension_rows(
        service,
        marker="top_urls",
        dimension="url",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(requests) AS requests, "
            "sum(latency_sum_ms) / nullIf(sum(requests), 0) AS avg_ms, "
            f"{_weighted('latency_p50_ms')} AS p50_ms, "
            f"{_weighted('latency_p95_ms')} AS p95_ms, "
            f"{_weighted('latency_p99_ms')} AS p99_ms"
        ),
        having="sum(requests) > 5",
        order_by="p99_ms DESC",
        limit=20,
    )
    top_urls = [
        {
            "url": row["value"],
            "requests": int(row["requests"]),
            "avg": float(row["avg_ms"]) if row["avg_ms"] is not None else 0.0,
            "p50": float(row["p50_ms"]) if row["p50_ms"] is not None else 0.0,
            "p95": float(row["p95_ms"]) if row["p95_ms"] is not None else 0.0,
            "p99": float(row["p99_ms"]) if row["p99_ms"] is not None else 0.0,
        }
        for row in top_urls_rows
    ]

    top_asns_rows = _dimension_rows(
        service,
        marker="top_asns",
        dimension="asn",
        start_time=start_time,
        end_time=end_time,
        select=(
            "sum(requests) AS requests, "
            "sum(latency_sum_ms) / nullIf(sum(requests), 0) AS avg_ms, "
            f"{_weighted('latency_p50_ms')} AS p50_ms, "
            f"{_weighted('latency_p95_ms')} AS p95_ms, "
            f"{_weighted('latency_p99_ms')} AS p99_ms"
        ),
        having="sum(requests) > 5",
        order_by="p99_ms DESC",
        limit=20,
    )
    top_asns = [
        {
            "asn": row["value"],
            "requests": int(row["requests"]),
            "avg": float(row["avg_ms"]) if row["avg_ms"] is not None else 0.0,
            "p50": float(row["p50_ms"]) if row["p50_ms"] is not None else 0.0,
            "p95": float(row["p95_ms"]) if row["p95_ms"] is not None else 0.0,
            "p99": float(row["p99_ms"]) if row["p99_ms"] is not None else 0.0,
        }
        for row in top_asns_rows
    ]

    return PerformanceAggregatesResponse.with_telemetry(
        top_urls=top_urls,
        top_asns=top_asns,
        ttl_dist=ttl_dist,
        scatter=[],
        waterfall={},
        _approx=True,
    )
