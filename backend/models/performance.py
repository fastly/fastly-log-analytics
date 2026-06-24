from __future__ import annotations

from typing import Any

from pydantic import Field

from backend.models.common import BaseResponse


class PerformanceAggregatesResponse(BaseResponse):
    top_urls: list[dict[str, Any]] = []
    top_asns: list[dict[str, Any]] = []
    ttl_dist: list[dict[str, Any]] = []
    scatter: list[dict[str, Any]] = []
    waterfall: dict[str, dict[str, float]] = {}
    # True when top_urls / top_asns were served from the perf_latency rollup
    # (>= 48 h unfiltered windows): the cross-hour percentile combine is a
    # request-weighted average, so the values are approximate. Emitted as
    # ``_approx`` (same key the origin Aggregates badge reads).
    approx: bool = Field(default=False, serialization_alias="_approx")
