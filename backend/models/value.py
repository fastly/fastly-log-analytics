"""Pydantic models for the Fastly Value executive summary."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from backend.models.common import BaseResponse, FilteredRequest

ValueSectionName = Literal[
    "overview",
    "caching",
    "security",
    "bots",
    "performance",
    "network",
    "io",
]


class ValueSummaryRequest(FilteredRequest):
    sections: list[ValueSectionName] | None = None
    chart_interval: str = "1 day"
    range_token: str | None = None
    anchor: str | None = None


class TimeSeriesPoint(BaseModel):
    time: str
    value: float
    category: str | None = None


class OverviewMetrics(BaseModel):
    origin_offload_pct: float | None = None
    threats_blocked: int | None = None
    cache_acceleration_factor: float | None = None
    total_requests: int | None = None
    total_bandwidth_bytes: int | None = None
    edge_hit_requests: int | None = None
    origin_requests: int | None = None


class CachingMetrics(BaseModel):
    origin_offload_pct: float | None = None
    bandwidth_saved_bytes: int | None = None
    shield_effectiveness_pct: float | None = None
    total_requests: int | None = None
    hit_requests: int | None = None
    miss_requests: int | None = None
    pass_requests: int | None = None
    synth_requests: int | None = None
    offload_time_series: list[TimeSeriesPoint] = []
    cache_state_time_series: list[TimeSeriesPoint] = []


class SecurityMetrics(BaseModel):
    waf_blocked: int | None = None
    waf_logged: int | None = None
    waf_passed: int | None = None
    total_requests: int | None = None
    threat_time_series: list[TimeSeriesPoint] = []
    top_waf_signals: list[dict[str, Any]] = []


class BotMetrics(BaseModel):
    bot_requests: int | None = None
    verified_bots: int | None = None
    total_requests: int | None = None
    top_bots: list[dict[str, Any]] = []


class PerformanceMetrics(BaseModel):
    cache_accel_factor: float | None = None
    avg_hit_latency_ms: float | None = None
    avg_miss_latency_ms: float | None = None
    p99_latency_ms: float | None = None
    total_requests: int | None = None
    latency_time_series: list[TimeSeriesPoint] = []


class NetworkMetrics(BaseModel):
    http3_pct: float | None = None
    tls_pct: float | None = None
    ipv6_pct: float | None = None
    h2_pct: float | None = None
    total_requests: int | None = None
    protocol_time_series: list[TimeSeriesPoint] = []


class IOFormatBreakdown(BaseModel):
    format: str
    count: int
    pct: float


class IOMetrics(BaseModel):
    io_transforms: int | None = None
    io_bandwidth_bytes: int | None = None
    io_shield_bandwidth_bytes: int | None = None
    total_requests: int | None = None
    io_time_series: list[TimeSeriesPoint] = []
    io_pct_of_traffic: float | None = None
    io_estimated_cost_usd: float | None = None
    format_distribution: list[IOFormatBreakdown] = []
    modern_format_pct: float | None = None
    io_bandwidth_time_series: list[TimeSeriesPoint] = []
    optimization_opportunities: list[dict[str, Any]] = []
    io_actual_bandwidth_saved_bytes: int | None = None
    io_actual_compression_ratio: float | None = None
    io_format_conversion_pairs: list[dict[str, Any]] = []
    io_compression_time_series: list[TimeSeriesPoint] = []
    image_request_count: int | None = None
    image_bandwidth_bytes: int | None = None
    estimated_savings_bytes: int | None = None


class ValueSummaryResponse(BaseResponse):
    overview: OverviewMetrics | None = None
    caching: CachingMetrics | None = None
    security: SecurityMetrics | None = None
    bots: BotMetrics | None = None
    performance: PerformanceMetrics | None = None
    network: NetworkMetrics | None = None
    io: IOMetrics | None = None
