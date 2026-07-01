from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, model_serializer

from backend.models.common import BaseResponse


class NetworkWorstEntry(BaseModel):
    label: str
    score: float | None = None


class ShieldingRow(BaseModel):
    """One edge→shield POP pair in the transit map / analysis table.

    Every field is populated by ``origin._enrich_with_distance`` on the
    happy path (coords resolve to None when a POP is absent from the
    pop-location map, but the keys are always present).
    """

    edge_pop: str | None = None
    shield_pop: str | None = None
    requests: int | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    distance_km: float | None = None
    light_speed_rtt_ms: float | None = None
    efficiency_ratio: float | None = None
    anomaly_static: bool = False
    # The latency verdict *independent* of sample size: efficiency > 3× the
    # light-speed floor AND ≥20ms absolute overhead. ``anomaly_static`` is just
    # ``anomaly_eligible and not low_sample`` at the server's default floor.
    # Surfacing it lets the FE recompute the flag against a user-chosen
    # min-requests threshold without duplicating the (3×/20ms) latency rule.
    anomaly_eligible: bool = False
    # True when ``requests`` is below the anomaly-flag floor — too few samples
    # for the percentiles to be reliable. The route is still shown; the FE
    # mutes its colouring and suppresses the anomaly treatment.
    low_sample: bool = False
    edge_lat: float | None = None
    edge_lon: float | None = None
    shield_lat: float | None = None
    shield_lon: float | None = None


class ShieldingAnalysis(BaseModel):
    """Typed sub-payload for ``NetworkHealthResponse.shielding_analysis``.

    Deliberately a plain ``BaseModel`` (NOT ``BaseResponse``): nesting a
    ``BaseResponse`` here would emit its telemetry envelope keys
    (``_debug_queries`` / ``_debug_calls`` / ``_section_timings`` /
    ``_is_cached``) *inside* this object, a wire change. The network
    router already strips ``debug_``-prefixed keys before assigning this.

    ``extra="allow"`` keeps the typing additive/wire-safe — any key the
    repository emits that isn't declared here (e.g. the always-zero
    ``total`` from ``empty_schema_response``) passes through untouched.
    The serializer drops the optional discriminator keys when they're
    None so the emitted bytes match the pre-typing dict exactly (the repo
    only sets the keys relevant to each branch).
    """

    model_config = ConfigDict(extra="allow")

    has_data: bool = False
    rows: list[ShieldingRow] = []
    edge_only: bool | None = None
    requires_fields: list[str] | None = None
    # M1: full distinct-pair count + truncation flag for "Top N of M".
    total_routes: int | None = None
    truncated: bool | None = None
    # M2: handler-level failure sentinel (broken analysis ≠ "no data").
    error: bool | None = None

    @model_serializer(mode="wrap")
    def _drop_none_discriminators(self, handler):
        data = handler(self)
        for key in ("edge_only", "requires_fields", "total_routes", "truncated", "error"):
            if data.get(key) is None:
                data.pop(key, None)
        return data


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
    # instead of fanning to /api/origin/shielding-analysis. Typed (was
    # ``dict[str, Any]``) so a repository key rename is a compile-time /
    # schema-drift error rather than a silent strip — shielding audit
    # 2026-06-30 L6.
    shielding_analysis: ShieldingAnalysis | None = None


class NetworkQualityResponse(BaseResponse):
    available: bool
    by_country: list[dict[str, Any]] = []
    by_asn: list[dict[str, Any]] = []
    by_region: list[dict[str, Any]] = []
    region_country: str | None = None
    by_pop: list[dict[str, Any]] = []
    scatter: list[dict[str, Any]] = []
    countries: list[str] = []
