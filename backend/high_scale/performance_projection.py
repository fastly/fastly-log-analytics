"""Pure request-event projection for bounded high-scale Performance queries."""

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
    percentile as _percentile,
)
from backend.high_scale._projection_utils import (
    text as _text,
)


@dataclass(frozen=True)
class PerformanceProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _Accumulator:
    requests: int = 0
    latencies: list[float] = field(default_factory=list)


def build_performance_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> PerformanceProjection:
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)

    for row in rows:
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue

        elapsed = _finite_float(row.get("elapsed"))

        # URL dimension
        url = _text(row.get("url"))
        if url:
            acc = dimensions[(bucket, "url", url)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)

        # ASN dimension
        asn = _text(row.get("asn"))
        if asn:
            acc = dimensions[(bucket, "asn", asn)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)

        # Top Bots? (Wait, user said "bot detection metrics")
        # NGWAF Bots might use "ngwaf_bot_name"
        # Let's add ASN and URL for now, maybe bot later or add it now if we need.
        bot = _text(row.get("_ngwaf_bot_name"))
        if bot:
            acc = dimensions[(bucket, "bot", bot)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)

    return PerformanceProjection(
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
    )


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
        "latency_count": len(acc.latencies),
        "latency_sum_ms": sum(acc.latencies) if acc.latencies else None,
        "latency_p50_ms": _percentile(acc.latencies, 0.50),
        "latency_p95_ms": _percentile(acc.latencies, 0.95),
        "latency_p99_ms": _percentile(acc.latencies, 0.99),
    }
