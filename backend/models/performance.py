from __future__ import annotations

from pydantic import ConfigDict, Field

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse


class PerformanceUrlRow(WireSafeModel):
    url: str
    requests: int
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class PerformanceAsnRow(WireSafeModel):
    """``asn`` is an int, or the raw dimension string when the value is not numeric; ``label``
    is the resolved ASN name on every path."""

    asn: int | str
    label: str | None = None
    requests: int
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class PerformanceTtlBucket(WireSafeModel):
    bucket: str
    count: int


class PerformanceScatterPoint(WireSafeModel):
    origin: float | None = None
    edge: float | None = None
    cache: str | None = None


class PerformanceWaterfallAvg(WireSafeModel):
    edge_processing: float
    origin_wait: float
    origin_download: float
    client_download: float


class PerformanceWaterfall(WireSafeModel):
    """Empty (``{}``) when scatter/waterfall were not requested or could not be computed."""

    avg: PerformanceWaterfallAvg | None = None


class PerformanceAggregatesResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    top_urls: list[PerformanceUrlRow] = []
    top_asns: list[PerformanceAsnRow] = []
    ttl_dist: list[PerformanceTtlBucket] = []
    scatter: list[PerformanceScatterPoint] = []
    waterfall: PerformanceWaterfall = Field(default_factory=PerformanceWaterfall)
    # True when top_urls / top_asns were served from the perf_latency rollup
    # (>= 48 h unfiltered windows): the cross-hour percentile combine is a
    # request-weighted average, so the values are approximate. Emitted as
    # ``_approx`` (same key the origin Aggregates badge reads).
    approx: bool = Field(default=False, alias="_approx")
