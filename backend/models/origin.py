from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.models._base import WireSafeModel
from backend.models.common import (
    BaseResponse,
    FilteredRequest,
    HasDataMixin,
    Limit100,
    Limit200,
    Limit1440,
    LimitedRequest,
    RangeKeyedRequest,
)
from backend.models.network import ShieldingRow


class OriginSlowUrlRow(WireSafeModel):
    url: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None


class OriginStatusCodeRow(WireSafeModel):
    """``status == -1`` buckets any origin status outside 100-599."""

    status: int
    count: int
    pct: float | None = None


class OriginPathRow(WireSafeModel):
    edge: bool | None = None
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None


class OriginPopLatencyRow(WireSafeModel):
    pop: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    elevated: bool


class OriginIpHealthRow(WireSafeModel):
    oip: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    error_pct: float | None = None


class OriginDrilldownRow(WireSafeModel):
    key: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    error_rate: float
    bytes: int | None = None


class OriginTimeseriesPoint(WireSafeModel):
    """``edge`` is only present when the series is split by leg."""

    time: str
    miss_count: int
    value: float | None = None
    edge: bool | None = None


class OriginDrilldownResponse(HasDataMixin, BaseResponse):
    dimension: str = "host"
    rows: list[OriginDrilldownRow] = []


class _HasDataOnlyMixin(BaseModel):
    has_data: bool = False


class _OriginSummaryMetricsMixin(BaseModel):
    total_misses: int | None = None
    total_passes: int | None = None
    ottfb_p50_ms: float | None = None
    ottfb_p75_ms: float | None = None
    ottfb_p95_ms: float | None = None
    ottfb_p99_ms: float | None = None
    ottlb_p50_ms: float | None = None
    ottlb_p95_ms: float | None = None
    cdn_overhead_p50_ms: float | None = None
    origin_error_rate: float | None = None
    obytes_p50: float | None = None


class OriginSummaryResponse(_OriginSummaryMetricsMixin, HasDataMixin, BaseResponse):
    pass


class OriginTimeseriesResponse(HasDataMixin, BaseResponse):
    series: list[OriginTimeseriesPoint] = []


class OriginSlowUrlsResponse(HasDataMixin, BaseResponse):
    rows: list[OriginSlowUrlRow] = []


class OriginStatusCodesResponse(HasDataMixin, BaseResponse):
    rows: list[OriginStatusCodeRow] = []


class OriginPathBreakdownResponse(HasDataMixin, BaseResponse):
    shielding_detected: bool = False
    rows: list[OriginPathRow] = []


class OriginPopLatencyResponse(HasDataMixin, BaseResponse):
    requires_group_c: bool = False
    median_p95_ms: float | int | None = None
    rows: list[OriginPopLatencyRow] = []


class OriginIpHealthResponse(HasDataMixin, BaseResponse):
    rows: list[OriginIpHealthRow] = []


class OriginShieldingAnalysisResponse(HasDataMixin, BaseResponse):
    requires_fields: list[str] = []
    edge_only: bool = False
    rows: list[ShieldingRow] = []
    # M1 (shielding audit 2026-06-30): full distinct-pair count + a flag
    # set when the returned rows are a (volume ∪ overhead) subset of all
    # routes. Declared so FastAPI doesn't strip them on the
    # /api/origin/shielding-analysis success path.
    total_routes: int | None = None
    truncated: bool | None = None
    # M2: handler-level failure sentinel so a broken analysis is
    # distinguishable from "no shield data" (the network router sets this).
    error: bool = False


# ── Composite sections ────────────────────────────────────────────────────────
#
# Plain ``WireSafeModel`` payloads (not ``BaseResponse``: a nested telemetry envelope would
# add ``_debug_*`` keys inside each section). ``_approx`` is set by the rollup/high-scale
# readers on weighted-percentile panels; ``_approx_note`` carries the producer's own
# description of the approximation so the UI never has to guess which method applies.


class OriginSummarySection(_OriginSummaryMetricsMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginTimeseriesSection(WireSafeModel):
    has_data: bool = False
    series: list[OriginTimeseriesPoint] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginSlowUrlsSection(WireSafeModel):
    has_data: bool = False
    rows: list[OriginSlowUrlRow] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginStatusCodesSection(WireSafeModel):
    has_data: bool = False
    rows: list[OriginStatusCodeRow] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginPathBreakdownSection(WireSafeModel):
    has_data: bool = False
    shielding_detected: bool = False
    rows: list[OriginPathRow] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginPopLatencySection(WireSafeModel):
    has_data: bool = False
    requires_group_c: bool = False
    median_p95_ms: float | int | None = None
    rows: list[OriginPopLatencyRow] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginIpHealthSection(WireSafeModel):
    has_data: bool = False
    rows: list[OriginIpHealthRow] = []
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginAggregatesResponse(HasDataMixin, BaseResponse):
    """Composite of every origin card on the /origin page.

    One CREATE TEMP TABLE filtered to the requested window populates a
    `t_origin` projection; six sub-queries run against that single
    materialization. Shielding analysis is NOT included here — it lives
    in /api/network-health post item 13 (the join semantics overlap with
    network-level shielding metadata).

    Granular endpoints (/api/origin/summary, /timeseries, etc.) stay
    alive behind the same router so the frontend can flip back during a
    rollback without a backend redeploy.
    """

    summary: OriginSummarySection = Field(default_factory=OriginSummarySection)
    timeseries: OriginTimeseriesSection = Field(default_factory=OriginTimeseriesSection)
    slow_urls: OriginSlowUrlsSection = Field(default_factory=OriginSlowUrlsSection)
    status_codes: OriginStatusCodesSection = Field(default_factory=OriginStatusCodesSection)
    path_breakdown: OriginPathBreakdownSection = Field(default_factory=OriginPathBreakdownSection)
    pop_latency: OriginPopLatencySection = Field(default_factory=OriginPopLatencySection)
    ip_health: OriginIpHealthSection = Field(default_factory=OriginIpHealthSection)


# ── Request models ────────────────────────────────────────────────────────────

# Section selector — names mirror OriginAggregatesResponse fields one-to-one so
# the FE can request a subset.
SectionName = Literal[
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
]


class OriginRequest(FilteredRequest):
    pass


class OriginTimeseriesRequest(FilteredRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
    metric: Literal["ttfb", "ttlb"] = "ttfb"
    percentile: Literal["p50", "p95", "p99"] = "p95"


class OriginSlowUrlsRequest(FilteredRequest):
    limit: Limit100 = 20
    min_requests: int = 10


class OriginPopLatencyRequest(LimitedRequest):
    pass


class OriginIpHealthRequest(LimitedRequest):
    pass


class OriginShieldingAnalysisRequest(FilteredRequest):
    limit: Limit200 = 50


class OriginDrilldownRequest(FilteredRequest):
    dimension: Literal["host", "shield_pop", "datacenter", "protocol", "pop", "oip"] = "host"
    limit: Limit100 = 30


class OriginAggregatesRequest(RangeKeyedRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
    timeseries_metric: Literal["ttfb", "ttlb"] = "ttfb"
    timeseries_percentile: Literal["p50", "p95", "p99"] = "p95"
    slow_urls_limit: Limit100 = 20
    slow_urls_min_requests: int = 10
    ip_health_limit: Limit100 = 30
    pop_latency_limit: Limit100 = 30
    sections: list[SectionName] | None = None
