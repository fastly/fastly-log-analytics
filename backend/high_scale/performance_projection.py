"""Pure request-event projection for bounded high-scale Performance queries."""

from __future__ import annotations

import hashlib
import heapq
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

# Matches the standard path's ``USING SAMPLE 300``. Each batch keeps its lowest
# ``sample_key`` rows, so the lowest keys across any union of batches are a
# uniform sample of the union's rows.
SCATTER_SAMPLE_SIZE = 300


@dataclass(frozen=True)
class PerformanceProjection:
    dimension_rows: tuple[dict[str, Any], ...]
    summary_rows: tuple[dict[str, Any], ...] = ()
    scatter_rows: tuple[dict[str, Any], ...] = ()


@dataclass
class _Accumulator:
    requests: int = 0
    latencies: list[float] = field(default_factory=list)


@dataclass
class _WaterfallSums:
    count: int = 0
    edge_processing: float = 0.0
    origin_wait: float = 0.0
    origin_download: float = 0.0
    client_download: float = 0.0


def build_performance_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> PerformanceProjection:
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)
    waterfalls: dict[datetime, _WaterfallSums] = defaultdict(_WaterfallSums)
    scatter_candidates: list[tuple[int, int, dict[str, Any]]] = []

    for index, row in enumerate(rows):
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue

        elapsed = _finite_float(row.get("elapsed"))
        _add_waterfall_and_scatter(row, index, bucket, elapsed, waterfalls, scatter_candidates)

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
        summary_rows=tuple(_summary_row(bucket, sums) for bucket, sums in sorted(waterfalls.items())),
        scatter_rows=tuple(
            row for _, _, row in heapq.nsmallest(SCATTER_SAMPLE_SIZE, scatter_candidates, key=lambda c: c[:2])
        ),
    )


def _add_waterfall_and_scatter(
    row: Mapping[str, Any],
    index: int,
    bucket: datetime,
    elapsed_us: float | None,
    waterfalls: dict[datetime, _WaterfallSums],
    scatter_candidates: list[tuple[int, int, dict[str, Any]]],
) -> None:
    """Mirror the standard ``components`` CTE: rows need both ``ttfb`` and ``elapsed``."""
    ttfb_s = _finite_float(row.get("ttfb"))
    if elapsed_us is None or ttfb_s is None:
        return
    elapsed_ms = elapsed_us / 1000.0
    ttfb_ms = ttfb_s * 1000.0
    ottfb_ms = (_finite_float(row.get("ottfb")) or 0.0) / 1000.0
    ottlb_ms = (_finite_float(row.get("ottlb")) or 0.0) / 1000.0

    sums = waterfalls[bucket]
    sums.count += 1
    sums.edge_processing += max(0.0, ttfb_ms - ottfb_ms)
    sums.origin_wait += ottfb_ms
    sums.origin_download += max(0.0, ottlb_ms - ottfb_ms)
    sums.client_download += max(0.0, elapsed_ms - max(ottlb_ms, ttfb_ms))

    edge_ms = elapsed_ms - ttfb_ms
    if edge_ms < 0:
        return
    sample_key = _sample_key(row, index)
    scatter_candidates.append(
        (
            sample_key,
            index,
            {
                "bucket_start": bucket.isoformat(),
                "sample_key": sample_key,
                "origin_ms": ttfb_ms,
                "edge_ms": edge_ms,
                "cache": _text(row.get("cache")),
            },
        )
    )


def _sample_key(row: Mapping[str, Any], index: int) -> int:
    identity = _text(row.get("event_id")) or f"{row.get('timestamp')}|{row.get('url')}|{index}"
    return int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")


def _summary_row(bucket: datetime, sums: _WaterfallSums) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "component_count": sums.count,
        "edge_processing_sum_ms": sums.edge_processing,
        "origin_wait_sum_ms": sums.origin_wait,
        "origin_download_sum_ms": sums.origin_download,
        "client_download_sum_ms": sums.client_download,
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
        "latency_count": len(acc.latencies),
        "latency_sum_ms": sum(acc.latencies) if acc.latencies else None,
        "latency_p50_ms": lat_p50,
        "latency_p95_ms": lat_p95,
        "latency_p99_ms": lat_p99,
    }
