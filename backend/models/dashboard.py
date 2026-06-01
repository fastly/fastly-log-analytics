"""Pydantic models for dashboard, sessions, insights, and query endpoints."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from backend.models.common import BaseResponse, FilteredRequest, Limit500, PaginationMixin

# ── Dashboard aggregates ──────────────────────────────────────────────────────

ChartMetric = Literal[
    "requests",
    "5xx",
    "4xx",
    "hit_rate",
    "p50_latency",
    "p95_latency",
    "p99_latency",
    "throughput",
    "req_size",
    "ttfb",
]


class AggregatesRequest(FilteredRequest):
    chart_interval: str = "1 minute"
    chart_metric: ChartMetric = "requests"


class FieldTopEntry(BaseModel):
    value: Any
    count: int
    label: str | None = None


class FieldAggregate(BaseModel):
    top: list[FieldTopEntry]
    total: int


class TimeSeriesPoint(BaseModel):
    time: str
    value: float
    category: str | None = None
    baseline: float | None = None


class MapPoint(BaseModel):
    country: str
    count: int


class AggregatesResponse(BaseResponse):
    data: dict[str, FieldAggregate]
    time_series: list[TimeSeriesPoint]
    map_data: list[MapPoint]
    where_clause: str
    interval: str
    metric: str
    total_rows: int
    total_rows_total: int
    earliest_log_at: str | None = None
    latest_log_at: str | None = None


# ── Dashboard raw ─────────────────────────────────────────────────────────────


class RawRequest(FilteredRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_col: str | None = None
    columns: list[str] = []


class RawResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, Any]]
    total_rows: int
    total_rows_total: int
    page: int
    limit: int
    earliest_log_at: str | None = None
    latest_log_at: str | None = None


# ── Dashboard field values ────────────────────────────────────────────────────


class FieldValuesRequest(FilteredRequest):
    field: str
    search: str = ""
    limit: Limit500 = 100


class FieldValuesResponse(BaseResponse):
    values: list[FieldTopEntry]
    field: str


# ── Insights ──────────────────────────────────────────────────────────────────


class InsightsRequest(FilteredRequest):
    window_size_hrs: float = 1.0
    baseline_hours: float = 168.0


class InsightItem(BaseModel):
    label: str
    current_val: float | None = None
    baseline_val: float | None = None
    baseline_label: str = "baseline"
    unit: str | None = None
    severity: str = "clean"
    meta: dict[str, Any] = {}
    investigate_url: str | None = None
    tooltip: str | None = None


class InsightCard(BaseModel):
    id: str
    title: str
    description: str
    severity: str
    summary: str
    items: list[InsightItem]


class InsightsResponse(BaseResponse):
    insights: list[InsightCard]
    window_start: str
    window_end: str
    baseline_start: str
    baseline_end: str
    computed_at: str
    window_hours: float
    baseline_hours: float


class InsightAvailability(BaseModel):
    id: str
    title: str
    description: str
    missing_fields: list[str] | None = None
    missing_groups: list[str] | None = None
    enable_url: str | None = None
    required_fields: list[str] | None = None
    required_groups: list[str] | None = None
    available: bool


class InsightsAvailabilityResponse(BaseModel):
    available: bool | list[str]
    insights: list[InsightAvailability] | None = None
    unavailable: list[InsightAvailability] | None = None


# ── Sessions ──────────────────────────────────────────────────────────────────


class SessionsRequest(FilteredRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_by: str = "session_start"
    flagged_only: bool = False
    min_reqs_flag: int | None = None
    min_4xx_pct_flag: float | None = None


class Session(BaseModel):
    ip: str
    ua: str | None = None
    ja4: str | None = None
    country: str | None = None
    asn: int | None = None
    session_start: str
    session_end: str
    req_count: int
    edge_count: int | None = None
    shield_count: int | None = None
    unique_urls: int | None = None
    reqs_4xx: int | None = None
    reqs_5xx: int | None = None
    total_bytes: int | None = None
    median_rtt_ms: float | None = None
    flagged: bool


class SessionsResponse(BaseResponse):
    sessions: list[Session]
    total: int
    page: int
    limit: int
    has_rtt: bool
    has_ja4: bool
    has_edge: bool
    min_reqs_flag: int
    min_4xx_pct_flag: float


class SessionDetailRequest(BaseModel):
    ip: str
    ja4: str | None = None
    start_time: str
    end_time: str


class SessionDetailResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, Any]]


# ── Query ─────────────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    sql: str
    max_rows: int = 500
    explain: bool = False


class QueryResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, Any]]
    row_count: int
    total_rows: int
    truncated: bool
    elapsed_ms: int
    explain_plan: str | None = None


class PresetQuery(BaseModel):
    name: str
    description: str
    sql: str
