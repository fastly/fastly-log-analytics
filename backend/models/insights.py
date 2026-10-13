"""Pydantic models for automated insights and anomaly detection endpoints."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.models.common import BaseResponse, FilteredRequest


class InsightsRequest(FilteredRequest):
    """Request payload for /api/insights."""

    # M2: bound both windows. Unbounded, these fed
    # ``baseline_start = now - (baseline_hours + window_size_hrs)`` into a
    # temp-table scan — a value like 8_760_000 reached ~1000 years back and
    # scanned every retained row (DoS + time-scope bypass). 168h / 2160h
    # (7d / 90d) cover every legitimate insight selection.
    window_size_hrs: float = Field(default=24.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)
    force_refresh: bool = False


class InsightItem(BaseModel):
    label: str
    current_val: float | None = None
    baseline_val: float | None = None
    baseline_label: str = "baseline"
    unit: str | None = None
    severity: str = "clean"
    meta: dict[str, Any] = Field(default_factory=dict)
    investigate_url: str | None = None
    tooltip: str | None = None


class InsightCard(BaseModel):
    id: str
    title: str
    description: str
    severity: str
    summary: str
    items: list[InsightItem]
    # Thematic section key ({security, origin, edge, traffic, network}); drives the
    # sectioned Anomaly Detection page (frontend/lib/insight-sections.ts).
    # Optional-with-default, NOT required: the /api/insights cache stores the
    # raw pre-serialisation dict, so a stale pre-deploy entry that predates
    # this field must serialise as ``null`` instead of raising a
    # ValidationError for the whole TTL window. The field is load-bearing —
    # a ``category`` key on the repo dict alone is silently dropped on the
    # wire (Pydantic ``extra='ignore'``), the same way window_total_requests is.
    category: str | None = None


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
    # Same thematic section key as InsightCard.category, mirrored here so the
    # availability-derived skeleton cards group into the identical sections
    # (CLS: the loading layout must match the loaded layout). Sourced from the
    # INSIGHT_DEFINITIONS dicts in backend/core/_log_fields_data.py — a
    # SEPARATE subsystem from the runtime registry, so category is set in both.
    category: str | None = None
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
    window_size_hrs: float = Field(default=24.0, gt=0, le=168)
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
    hits: int = 0
    misses: int = 0
    passes: int = 0
    other: int = 0


class CacheCollapseDetailResponse(BaseResponse):
    url: str = ""
    timeline: list[CacheCollapseTimelinePoint] = Field(default_factory=list)
    recent_misses: list[CacheMissPoint] = Field(default_factory=list)
    breakdown: CacheBreakdown = Field(default_factory=CacheBreakdown)
    # Cacheable hit ratio HIT/(HIT+MISS); PASS excluded.
    baseline_hit_rate: float = 0.0
    window_hit_rate: float = 0.0
    # Uncacheable share PASS/total.
    baseline_pass_rate: float = 0.0
    window_pass_rate: float = 0.0
    has_data: bool = True
    clusters: list[Any] = Field(default_factory=list)
