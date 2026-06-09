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

    summary: dict[str, Any] = {}
    timeseries: dict[str, Any] = {}
    slow_urls: dict[str, Any] = {}
    status_codes: dict[str, Any] = {}
    path_breakdown: dict[str, Any] = {}
    pop_latency: dict[str, Any] = {}
    ip_health: dict[str, Any] = {}
