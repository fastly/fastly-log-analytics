from __future__ import annotations

from typing import Any, Literal

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


class OriginDrilldownResponse(HasDataMixin, BaseResponse):
    dimension: str = "host"
    rows: list[dict[str, Any]] = []


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


class OriginTimeseriesResponse(HasDataMixin, BaseResponse):
    series: list[dict[str, Any]] = []


class _OriginRowsResponse(HasDataMixin, BaseResponse):
    rows: list[dict[str, Any]] = []


class OriginSlowUrlsResponse(_OriginRowsResponse):
    pass


class OriginStatusCodesResponse(_OriginRowsResponse):
    pass


class OriginPathBreakdownResponse(HasDataMixin, BaseResponse):
    shielding_detected: bool = False
    rows: list[dict[str, Any]] = []


class OriginPopLatencyResponse(HasDataMixin, BaseResponse):
    requires_group_c: bool = False
    median_p95_ms: float | None = None
    rows: list[dict[str, Any]] = []


class OriginIpHealthResponse(_OriginRowsResponse):
    pass


class OriginShieldingAnalysisResponse(HasDataMixin, BaseResponse):
    requires_fields: list[str] = []
    edge_only: bool = False
    rows: list[dict[str, Any]] = []
    # M1 (shielding audit 2026-06-30): full distinct-pair count + a flag
    # set when the returned rows are a (volume ∪ overhead) subset of all
    # routes. Declared so FastAPI doesn't strip them on the
    # /api/origin/shielding-analysis success path.
    total_routes: int | None = None
    truncated: bool | None = None
    # M2: handler-level failure sentinel so a broken analysis is
    # distinguishable from "no shield data" (the network router sets this).
    error: bool = False


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
