from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.models.analytics._common import (
    _move_fields,
    _UrlRequestsWireBase,
)
from backend.models.common import (
    BaseResponse,
    RangeKeyedRequest,
    WireSafeModel,
)

PerformanceSectionName = Literal["waterfall", "top_urls", "top_asns", "ttl_dist", "scatter"]


class PerformanceRequest(RangeKeyedRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"
    sections: list[PerformanceSectionName] | None = None


class _PerformanceLatencyTailMixin(WireSafeModel):
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _move_fields(cls, "avg", "p50", "p95", "p99")


class PerformanceUrlRow(_UrlRequestsWireBase, _PerformanceLatencyTailMixin):
    pass


class PerformanceAsnRow(_PerformanceLatencyTailMixin):
    asn: int | str
    label: str | None = None
    requests: int


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
    avg: PerformanceWaterfallAvg | None = None


class PerformanceAggregatesResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    top_urls: list[PerformanceUrlRow] = []
    top_asns: list[PerformanceAsnRow] = []
    ttl_dist: list[PerformanceTtlBucket] = []
    scatter: list[PerformanceScatterPoint] = []
    waterfall: PerformanceWaterfall = Field(default_factory=PerformanceWaterfall)
    approx: bool = Field(default=False, alias="_approx")


class AssetTypeBreakdownRow(BaseModel):
    asset_type: str
    requests: int
    egress_bytes: int
    cache_hit_ratio: float
    compression_rate: float


class CachePerformanceRow(BaseModel):
    asset_type: str
    cache_status: str
    requests: int
    bytes: int


class CompressionPerformanceRow(BaseModel):
    asset_type: str
    content_encoding: str
    requests: int
    bytes: int


class _UrlRequestsBase(BaseModel):
    url: str
    requests: int


class LargeUncompressedAssetRow(_UrlRequestsBase):
    avg_bytes: float
    total_bytes: int
    status: int


class LowTtlAssetRow(_UrlRequestsBase):
    avg_ttl: float
    asset_type: str


class AssetsAggregatesResponse(BaseResponse):
    asset_type_breakdown: list[AssetTypeBreakdownRow] = []
    cache_performance: list[CachePerformanceRow] = []
    compression_performance: list[CompressionPerformanceRow] = []
    large_uncompressed_assets: list[LargeUncompressedAssetRow] = []
    low_ttl_assets: list[LowTtlAssetRow] = []
    missing_fields: list[str] = Field(default_factory=list)


class AssetsRequest(RangeKeyedRequest):
    pass
