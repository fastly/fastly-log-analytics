"""Pure request-event projection for bounded high-scale Origin queries."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.high_scale._projection_utils import (
    bucket as _bucket,
)
from backend.high_scale._projection_utils import (
    finite_float as _finite_float,
)
from backend.high_scale._projection_utils import (
    percentiles as _percentiles,
)
from backend.high_scale._projection_utils import (
    text as _text,
)


@dataclass(frozen=True)
class OriginProjection:
    summary_rows: tuple[dict[str, Any], ...]
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _Accumulator:
    requests: int = 0
    misses: int = 0
    passes: int = 0
    origin_5xx: int = 0
    status_count: int = 0
    origin_bytes: int = 0
    latencies: list[float] = field(default_factory=list)
    ttlb_values: list[float] = field(default_factory=list)
    overhead_values: list[float] = field(default_factory=list)
    byte_values: list[float] = field(default_factory=list)


def build_origin_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> OriginProjection:
    summaries: dict[datetime, _Accumulator] = defaultdict(_Accumulator)
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        latency = _origin_latency_us(row)
        status = _integer(row.get("ost"))
        origin_bytes = _non_negative_integer(row.get("obytes"))
        if latency is not None:
            summary = summaries[bucket]
            _add_latency_metrics(summary, row, latency, status, origin_bytes)

        if status is not None:
            status_value = str(status if 100 <= status <= 599 else -1)
            status_acc = dimensions[(bucket, "status", status_value)]
            status_acc.requests += 1
            status_acc.origin_5xx += int(500 <= status < 600)

        for dimension, value in _latency_dimensions(row):
            if latency is None:
                continue
            acc = dimensions[(bucket, dimension, value)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

        origin_ip = _text(row.get("oip"))
        if origin_ip and status is not None:
            acc = dimensions[(bucket, "oip", origin_ip)]
            acc.requests += 1
            _add_dimension_metrics(acc, latency, status, origin_bytes)

    return OriginProjection(
        summary_rows=tuple(_summary_row(bucket, acc) for bucket, acc in sorted(summaries.items())),
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
    )


def _add_latency_metrics(
    acc: _Accumulator,
    row: Mapping[str, Any],
    latency: float,
    status: int | None,
    origin_bytes: int | None,
) -> None:
    acc.requests += 1
    acc.latencies.append(latency)
    cache = _text(row.get("cache")).upper()
    acc.misses += int(cache.startswith("MISS"))
    acc.passes += int(cache.startswith("PASS"))
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    acc.status_count += int(status is not None)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes
        acc.byte_values.append(float(origin_bytes))
    ttlb = _finite_float(row.get("ottlb"))
    if ttlb is not None:
        acc.ttlb_values.append(ttlb)
        elapsed = _finite_float(row.get("elapsed"))
        if elapsed is not None:
            acc.overhead_values.append(elapsed - ttlb)


def _add_dimension_metrics(
    acc: _Accumulator,
    latency: float | None,
    status: int | None,
    origin_bytes: int | None,
) -> None:
    if latency is not None:
        acc.latencies.append(latency)
    acc.origin_5xx += int(status is not None and 500 <= status < 600)
    if origin_bytes is not None:
        acc.origin_bytes += origin_bytes


def _summary_row(bucket: datetime, acc: _Accumulator) -> dict[str, Any]:
    lat_p50, lat_p75, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.75, 0.95, 0.99))
    ttlb_p50, ttlb_p95 = _percentiles(acc.ttlb_values, (0.50, 0.95))
    (overhead_p50,) = _percentiles(acc.overhead_values, (0.50,))
    (bytes_p50,) = _percentiles(acc.byte_values, (0.50,))
    return {
        "bucket_start": bucket.isoformat(),
        "requests": acc.requests,
        "misses": acc.misses,
        "passes": acc.passes,
        "origin_5xx": acc.origin_5xx,
        "status_count": acc.status_count,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "ttlb_count": len(acc.ttlb_values),
        "overhead_count": len(acc.overhead_values),
        "origin_bytes_count": len(acc.byte_values),
        "latency_p50_us": lat_p50,
        "latency_p75_us": lat_p75,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
        "ttlb_p50_us": ttlb_p50,
        "ttlb_p95_us": ttlb_p95,
        "cdn_overhead_p50_us": overhead_p50,
        "origin_bytes_p50": bytes_p50,
    }


def _dimension_row(
    bucket: datetime,
    dimension: str,
    value: str,
    acc: _Accumulator,
) -> dict[str, Any]:
    lat_p50, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.95, 0.99))
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "origin_5xx": acc.origin_5xx,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "latency_p50_us": lat_p50,
        "latency_p95_us": lat_p95,
        "latency_p99_us": lat_p99,
    }


def _latency_dimensions(row: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    values: list[tuple[str, str]] = []
    for dimension, source_field in (("url", "url"), ("pop", "pop")):
        value = _text(row.get(source_field))
        if value:
            values.append((dimension, value))
    edge = row.get("edge")
    if isinstance(edge, bool):
        values.append(("edge", "true" if edge else "false"))
    return tuple(values)


def _origin_latency_us(row: Mapping[str, Any]) -> float | None:
    ottfb = _finite_float(row.get("ottfb"))
    if ottfb is not None:
        return ottfb
    ttfb = _finite_float(row.get("ttfb"))
    return ttfb * 1_000_000.0 if ttfb is not None else None


def _integer(value: object) -> int | None:
    parsed = _finite_float(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _non_negative_integer(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None
