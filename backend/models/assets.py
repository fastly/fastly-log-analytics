from __future__ import annotations

from pydantic import BaseModel, Field

from backend.models.common import BaseResponse, RangeKeyedRequest


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


class LargeUncompressedAssetRow(BaseModel):
    url: str
    requests: int
    avg_bytes: float
    total_bytes: int
    status: int


class LowTtlAssetRow(BaseModel):
    url: str
    requests: int
    avg_ttl: float
    asset_type: str


class AssetsAggregatesResponse(BaseResponse):
    """Composite of assets & shield performance cards."""

    asset_type_breakdown: list[AssetTypeBreakdownRow] = []
    cache_performance: list[CachePerformanceRow] = []
    compression_performance: list[CompressionPerformanceRow] = []
    large_uncompressed_assets: list[LargeUncompressedAssetRow] = []
    low_ttl_assets: list[LowTtlAssetRow] = []
    missing_fields: list[str] = Field(default_factory=list)


class AssetsRequest(RangeKeyedRequest):
    """Assets analytics request schema supporting relative-range resolving."""
