from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.registry import HighScaleService
from backend.models.performance import PerformanceAggregatesResponse


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

    return PerformanceAggregatesResponse.with_telemetry(
        top_urls=[],
        top_asns=[],
        ttl_dist=ttl_dist,
        scatter=[],
        waterfall={},
        _approx=True,
    )
