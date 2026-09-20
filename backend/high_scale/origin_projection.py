"""Pure request-event projection for bounded high-scale Origin queries."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


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
        "latency_p50_us": _percentile(acc.latencies, 0.50),
        "latency_p75_us": _percentile(acc.latencies, 0.75),
        "latency_p95_us": _percentile(acc.latencies, 0.95),
        "latency_p99_us": _percentile(acc.latencies, 0.99),
        "ttlb_p50_us": _percentile(acc.ttlb_values, 0.50),
        "ttlb_p95_us": _percentile(acc.ttlb_values, 0.95),
        "cdn_overhead_p50_us": _percentile(acc.overhead_values, 0.50),
        "origin_bytes_p50": _percentile(acc.byte_values, 0.50),
    }


def _dimension_row(
    bucket: datetime,
    dimension: str,
    value: str,
    acc: _Accumulator,
) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "origin_5xx": acc.origin_5xx,
        "origin_bytes": acc.origin_bytes,
        "latency_count": len(acc.latencies),
        "latency_p50_us": _percentile(acc.latencies, 0.50),
        "latency_p95_us": _percentile(acc.latencies, 0.95),
        "latency_p99_us": _percentile(acc.latencies, 0.99),
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


def _bucket(value: object) -> datetime | None:
    if isinstance(value, datetime):
        timestamp = value.astimezone(UTC)
    elif isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
    else:
        return None
    return timestamp.replace(second=0, microsecond=0)


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _integer(value: object) -> int | None:
    parsed = _finite_float(value)
    if parsed is None or not parsed.is_integer():
        return None
    return int(parsed)


def _non_negative_integer(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
