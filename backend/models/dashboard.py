"""Pydantic models for dashboard, sessions, insights, and query endpoints."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.models.common import BaseResponse, FilteredRequest, Limit500, LogExtentsMixin, PaginationMixin
from backend.models.security import SecurityTopBotsResponse

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


# Selector for /api/dashboard/aggregates + /api/dashboard/bundle. Mirrors
# the security + network slices: callers pass ``sections=[...]`` to fetch
# only the panels they render. The 3-name shape collapses the existing
# 3-flag knob (include_time_series / include_conn_requests / include_map_data)
# + a new top-N gate into a single uniform API across pages.
#
# Coupling: ``core`` ⟶ time_series + conn_requests histogram + map_data
# (the above-fold cluster shares the live-temp build); ``topten`` ⟶ all
# ~85 per-field top-N cards (one merged execute_top_n_rollups scan); ``bots``
# ⟶ the bundle's second-connection top_bots branch only (no effect on the
# dedicated /aggregates endpoint).
DashboardSectionName = Literal["core", "topten", "bots"]


class AggregatesRequest(FilteredRequest):
    chart_interval: str = "1 minute"
    chart_metric: ChartMetric = "requests"
    fields: list[str] | None = None
    # Page-shape flags. Optional so existing callers (which never pass
    # them) keep working; ``None`` is treated as ``True`` server-side to
    # preserve the /dashboard contract. /charts (which only renders the
    # per-field top cards) passes False for all three to skip the
    # time_series query, conn_requests histogram, and map_data scan it
    # never reads.
    include_time_series: bool | None = None
    include_conn_requests: bool | None = None
    include_map_data: bool | None = None
    # Higher-level selector. When set, the router expands it into the
    # include_* flags above + an internal include_top_n gate. None →
    # preserves today's full behavior (every panel runs). The selector and
    # the include_* flags are NOT mutually exclusive — when both are
    # provided the selector wins (it's the canonical surface; the include_*
    # knobs stay for backwards compat with the /charts callers that already
    # pass them).
    sections: list[DashboardSectionName] | None = None
    # Relative-range wire contract (additive, optional) — see
    # backend/routers/origin.py / backend/utils/time_window.py. When
    # ``range_token`` is recognized the SERVER resolves the scan window from
    # (token, anchor), ignoring FE-supplied start/end, then clamps to the invite
    # ceiling. Consumed by /dashboard/bundle + /dashboard/aggregates so the FE
    # first-paint key is server-reproducible (origin SSR-seed contract). The
    # dashboard response memo is hard-disabled (DASHBOARD_CACHE_TTL=0) so the
    # token does not interact with it. Absent/unknown token → legacy path.
    range_token: str | None = None
    anchor: str | None = None


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


class AggregatesResponse(BaseResponse, LogExtentsMixin):
    data: dict[str, FieldAggregate]
    time_series: list[TimeSeriesPoint]
    map_data: list[MapPoint]
    where_clause: str
    interval: str
    metric: str
    total_rows: int
    total_rows_total: int


class BundleResponse(BaseResponse):
    """Composite response for /api/dashboard/bundle (finding 013).

    Without an explicit response_model the endpoint emitted an untyped dict
    that bypassed the BaseResponse._strip_debug_when_disabled serializer,
    leaking internal SQL queries + execution timings to clients regardless
    of the DEBUG_RESPONSES flag. Declaring the shape with nested typed
    sub-responses re-engages Pydantic's serialization lifecycle so the
    same redaction rules apply here as on the dedicated /aggregates and
    /top-bots endpoints.

    Sub-responses are Optional so the selector (``sections=[...]``) can
    omit a branch: a ``sections=['core']`` call returns ``top_bots: None``
    instead of paying for the second pool connection + bot SQL.
    """

    aggregates: AggregatesResponse | None = None
    top_bots: SecurityTopBotsResponse | None = None


# ── Dashboard raw ─────────────────────────────────────────────────────────────


class RawRequest(FilteredRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_col: str | None = None
    columns: list[str] = []


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
    # M2: bound both windows. Unbounded, these fed
    # ``baseline_start = now - (baseline_hours + window_size_hrs)`` into a
    # temp-table scan — a value like 8_760_000 reached ~1000 years back and
    # scanned every retained row (DoS + time-scope bypass). 168h / 2160h
    # (7d / 90d) cover every legitimate insight selection.
    window_size_hrs: float = Field(default=1.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)


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


# ── Cache Collapse Detail ─────────────────────────────────────────────────────


class CacheCollapseDetailRequest(BaseModel):
    url: str
    window_size_hrs: float = Field(default=1.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)


class CacheCollapseTimelinePoint(BaseModel):
    bucket: str
    expected_hits: float
    real_hits: int
    misses: int
    total_requests: int
    hit_rate: float


class CacheMissPoint(BaseModel):
    timestamp: str
    cache: str
    pop: str | None = None
    ip: str | None = None
    status: int | None = None


class CacheBreakdown(BaseModel):
    # Window-scoped request counts by cache disposition.
    hits: int
    misses: int
    passes: int
    other: int


class CacheCollapseDetailResponse(BaseResponse):
    url: str
    timeline: list[CacheCollapseTimelinePoint]
    recent_misses: list[CacheMissPoint]
    breakdown: CacheBreakdown
    # Cacheable hit ratio HIT/(HIT+MISS); PASS excluded.
    baseline_hit_rate: float
    window_hit_rate: float
    # Uncacheable share PASS/total.
    baseline_pass_rate: float
    window_pass_rate: float


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
    # asn_label retained as an optional carry-through so old frontend builds
    # that still read row.asn_label don't fall back to "AS<n>" mid-deploy.
    # New backends leave it unset; the labelled string lives in the top-level
    # ``asn_names`` map keyed by ``asn``.
    asn_label: str | None = None
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
    edge_sid: str | None = None
    flagged: bool
    # Opaque AES-GCM token sealing the real (ip, ja4, session_start,
    # session_end) tuple, minted server-side per row. The detail endpoint
    # unseals it to run the exact-match lookup, so a PII-masking analyst can
    # drill into a session without the real IP ever leaving the server or the
    # masked `ip` being round-tripped as a never-matching lookup key. Opaque
    # ciphertext, so it is NOT an IP-family field and passes masking untouched.
    session_token: str | None = None


class SessionsResponse(BaseResponse):
    sessions: list[Session]
    # Hoisted ASN-label map (``{asn_int: "AS<n> Name"}``) — replaces per-row
    # asn_label inlining. JSON serialises int keys as strings, so the
    # frontend looks up ``asn_names[String(row.asn)]``. Sessions sharing an
    # ASN now reference one map entry instead of repeating the label
    # ~30 bytes per row.
    asn_names: dict[int, str] = {}
    total: int
    page: int
    limit: int
    has_rtt: bool
    has_ja4: bool
    has_edge: bool
    has_edge_sid: bool = False
    min_reqs_flag: int
    min_4xx_pct_flag: float


class SessionDetailRequest(BaseModel):
    # ``session_token`` is the preferred lookup key — an opaque, server-minted
    # seal of the real session tuple (see Session.session_token). When present
    # the server derives ip/ja4/window from it and ignores the fields below. A
    # raw ``ip`` is still accepted for admin / non-masking analysts (legacy
    # path); a masking analyst supplying a raw ``ip`` is rejected (the masked
    # value can never match, and accepting raw IPs here would be a presence
    # oracle the PII filter-lock otherwise closes).
    session_token: str | None = None
    ip: str | None = None
    ja4: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class SessionDetailResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, Any]]


# ── Query ─────────────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    sql: str
    # M1: bound max_rows so an analyst can't request tens of millions of rows
    # and OOM the worker (it's interpolated into ``LIMIT max_rows+1`` and the
    # result is fully materialized via ``to_arrow_table().to_pylist()`` before
    # truncation). The Pydantic clamp is the first line of defense; the repo's
    # ``execute_query`` re-clamps against ``MAX_QUERY_ROWS`` so internal callers
    # that bypass the model can't exceed it either.
    max_rows: int = Field(default=500, ge=1, le=10_000)
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
