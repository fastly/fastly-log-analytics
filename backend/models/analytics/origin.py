from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.models.analytics._common import (
    ShieldingRow,
    _move_fields,
    _UrlRequestsWireBase,
)
from backend.models.common import (
    BaseResponse,
    FilteredRequest,
    HasDataMixin,
    Limit100,
    Limit200,
    Limit1440,
    LimitedRequest,
    RangeKeyedRequest,
    WireSafeModel,
)


class _OriginLatencyTailMixin(WireSafeModel):
    p50_ms: float | None = None
    p95_ms: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _move_fields(cls, "p50_ms", "p95_ms", "p99_ms", after="requests")


class _OriginLatencyP99TailMixin(_OriginLatencyTailMixin):
    p99_ms: float | None = None


class OriginSlowUrlRow(_UrlRequestsWireBase, _OriginLatencyP99TailMixin):
    pass


class OriginStatusCodeRow(WireSafeModel):
    status: int
    count: int
    pct: float | None = None


class OriginPathRow(_OriginLatencyTailMixin):
    edge: bool | None = None
    requests: int


class OriginPopLatencyRow(_OriginLatencyTailMixin):
    pop: str
    requests: int
    elevated: bool


class OriginIpHealthRow(_OriginLatencyTailMixin):
    oip: str
    requests: int
    error_pct: float | None = None


class OriginDrilldownRow(_OriginLatencyP99TailMixin):
    key: str
    requests: int
    error_rate: float
    bytes: int | None = None


class OriginTimeseriesPoint(WireSafeModel):
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


class _OriginTimeseriesMixin(BaseModel):
    series: list[OriginTimeseriesPoint] = []


class _OriginSlowUrlsMixin(BaseModel):
    rows: list[OriginSlowUrlRow] = []


class _OriginStatusCodesMixin(BaseModel):
    rows: list[OriginStatusCodeRow] = []


class _OriginPathBreakdownMixin(BaseModel):
    shielding_detected: bool = False
    rows: list[OriginPathRow] = []


class _OriginPopLatencyMixin(BaseModel):
    requires_group_c: bool = False
    median_p95_ms: float | int | None = None
    rows: list[OriginPopLatencyRow] = []


class _OriginIpHealthMixin(BaseModel):
    rows: list[OriginIpHealthRow] = []


class OriginTimeseriesResponse(_OriginTimeseriesMixin, HasDataMixin, BaseResponse):
    pass


class OriginSlowUrlsResponse(_OriginSlowUrlsMixin, HasDataMixin, BaseResponse):
    pass


class OriginStatusCodesResponse(_OriginStatusCodesMixin, HasDataMixin, BaseResponse):
    pass


class OriginPathBreakdownResponse(_OriginPathBreakdownMixin, HasDataMixin, BaseResponse):
    pass


class OriginPopLatencyResponse(_OriginPopLatencyMixin, HasDataMixin, BaseResponse):
    pass


class OriginIpHealthResponse(_OriginIpHealthMixin, HasDataMixin, BaseResponse):
    pass


class OriginShieldingAnalysisResponse(HasDataMixin, BaseResponse):
    requires_fields: list[str] = []
    edge_only: bool = False
    rows: list[ShieldingRow] = []
    total_routes: int | None = None
    truncated: bool | None = None
    error: bool = False


class _OriginSectionApproxMixin(BaseModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginSummarySection(_OriginSectionApproxMixin, _OriginSummaryMetricsMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginTimeseriesSection(_OriginSectionApproxMixin, _OriginTimeseriesMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginSlowUrlsSection(_OriginSectionApproxMixin, _OriginSlowUrlsMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginStatusCodesSection(_OriginSectionApproxMixin, _OriginStatusCodesMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginPathBreakdownSection(
    _OriginSectionApproxMixin, _OriginPathBreakdownMixin, _HasDataOnlyMixin, WireSafeModel
):
    pass


class OriginPopLatencySection(_OriginSectionApproxMixin, _OriginPopLatencyMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginIpHealthSection(_OriginSectionApproxMixin, _OriginIpHealthMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginAggregatesResponse(HasDataMixin, BaseResponse):
    summary: OriginSummarySection = Field(default_factory=OriginSummarySection)
    timeseries: OriginTimeseriesSection = Field(default_factory=OriginTimeseriesSection)
    slow_urls: OriginSlowUrlsSection = Field(default_factory=OriginSlowUrlsSection)
    status_codes: OriginStatusCodesSection = Field(default_factory=OriginStatusCodesSection)
    path_breakdown: OriginPathBreakdownSection = Field(default_factory=OriginPathBreakdownSection)
    pop_latency: OriginPopLatencySection = Field(default_factory=OriginPopLatencySection)
    ip_health: OriginIpHealthSection = Field(default_factory=OriginIpHealthSection)


SectionName = Literal[
    "summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"
]


class OriginRequest(FilteredRequest):
    pass


class _OriginBucketLegMixin(BaseModel):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False


class OriginTimeseriesRequest(_OriginBucketLegMixin, FilteredRequest):
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


class OriginAggregatesRequest(_OriginBucketLegMixin, RangeKeyedRequest):
    timeseries_metric: Literal["ttfb", "ttlb"] = "ttfb"
    timeseries_percentile: Literal["p50", "p95", "p99"] = "p95"
    slow_urls_limit: Limit100 = 20
    slow_urls_min_requests: int = 10
    ip_health_limit: Limit100 = 30
    pop_latency_limit: Limit100 = 30
    sections: list[SectionName] | None = None
