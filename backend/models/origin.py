from __future__ import annotations

from typing import Any

from backend.models.common import BaseResponse, HasDataMixin


class OriginSummaryResponse(HasDataMixin, BaseResponse):
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
    by_leg: list[dict[str, Any]] = []


class OriginTimeseriesResponse(HasDataMixin, BaseResponse):
    series: list[dict[str, Any]] = []


class OriginSlowUrlsResponse(HasDataMixin, BaseResponse):
    rows: list[dict[str, Any]] = []


class OriginStatusCodesResponse(HasDataMixin, BaseResponse):
    rows: list[dict[str, Any]] = []


class OriginPathBreakdownResponse(HasDataMixin, BaseResponse):
    shielding_detected: bool = False
    rows: list[dict[str, Any]] = []


class OriginPopLatencyResponse(HasDataMixin, BaseResponse):
    requires_group_c: bool = False
    median_p95_ms: float | None = None
    rows: list[dict[str, Any]] = []


class OriginIpHealthResponse(HasDataMixin, BaseResponse):
    rows: list[dict[str, Any]] = []


class OriginShieldingAnalysisResponse(HasDataMixin, BaseResponse):
    requires_fields: list[str] = []
    edge_only: bool = False
    rows: list[dict[str, Any]] = []
