from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from backend.models.common import BaseResponse


class NetworkWorstEntry(BaseModel):
    label: str
    score: float | None = None


class NetworkCityEntry(BaseModel):
    name: str
    lat: float | None = None
    lon: float | None = None


class NetworkHealthSummary(BaseModel):
    global_health_score: float
    avg_rtt_ms: float
    total_reqs: int
    worst_asn: NetworkWorstEntry | None = None
    worst_country: NetworkWorstEntry | None = None


class NetworkHealthResponse(BaseResponse):
    has_data: bool = True
    reason: str | None = None
    available: bool
    metric: str | None = None
    bucket_seconds: int | None = None
    buckets: list[str] = []
    heatmap: list[dict[str, Any]] = []
    map_buckets: list[dict[str, Any]] = []
    # Deduplicated city lookup for map_buckets. Each entry in
    # map_buckets[i].cities references a city by integer ``city_idx``
    # that resolves to ``cities[city_idx]`` (``{name, lat, lon}``). Hoisting
    # lat/lon up here lets the per-cell dicts drop both fields entirely;
    # interning keys by (name, lat, lon) so distinct geocenter entries for
    # the same name (e.g. duplicate "Boston" rows from MaxMind) stay distinct.
    cities: list[NetworkCityEntry] = []
    leaderboard: list[dict[str, Any]] = []
    metro_leaderboard: list[dict[str, Any]] = []
    summary: NetworkHealthSummary | None = None
    countries: list[str] = []
    has_metro: bool = False
    # Phase 3 item 13 — shielding-analysis is conceptually network-level
    # (edge → shield latency arcs). Folding it into the network-health
    # response lets the /network page get both shapes in one round-trip
    # instead of fanning to /api/origin/shielding-analysis.
    shielding_analysis: dict[str, Any] | None = None


class NetworkQualityResponse(BaseResponse):
    available: bool
    by_country: list[dict[str, Any]] = []
    by_asn: list[dict[str, Any]] = []
    by_region: list[dict[str, Any]] = []
    region_country: str | None = None
    by_pop: list[dict[str, Any]] = []
    scatter: list[dict[str, Any]] = []
    countries: list[str] = []
