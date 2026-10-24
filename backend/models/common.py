"""Shared Pydantic models used across multiple routers."""

from __future__ import annotations

import os as _os
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, AliasChoices, BaseModel, Field, field_validator, model_serializer

from backend.models._base import WireSafeModel  # noqa: F401 (re-exported)
from backend.models.log_fields_catalog import LogFieldsCatalogResponse
from backend.models.query_observation import ClickHouseQueryObservation

# ── Annotated clamping types (replaces repeated @field_validator boilerplate) ──


def _clamp_to(hi: int):
    def _clamp(v: int) -> int:
        return max(1, min(hi, int(v)))

    _clamp.__name__ = f"clamp_to_{hi}"
    return _clamp


def _clamp_to_float(hi: float, lo: float = 1.0 / 3600.0):
    """Like ``_clamp_to`` but preserves fractional values.

    The ``int(v)`` cast in ``_clamp_to`` silently truncated sub-1 floats to
    0 (then clamped to 1), which broke sub-minute bucket sizes for the
    timeseries endpoints — a request for 1-second buckets
    (``bucket_minutes = 1/60 ≈ 0.0167``) became 1 minute on the backend.
    Default ``lo`` is one-second-in-minutes so the smallest meaningful
    bucket survives the clamp.
    """

    def _clamp(v: float) -> float:
        return max(lo, min(hi, float(v)))

    _clamp.__name__ = f"clamp_float_to_{hi}"
    return _clamp


Limit100 = Annotated[int, AfterValidator(_clamp_to(100))]
Limit200 = Annotated[int, AfterValidator(_clamp_to(200))]
Limit500 = Annotated[int, AfterValidator(_clamp_to(500))]
# Float so callers can request sub-minute bucket sizes (e.g. ``1/60``
# for a 1-second granularity). The backend's ``get_timeseries`` handles
# ``bucket_minutes < 1`` by switching to a seconds-based INTERVAL.
Limit1440 = Annotated[float, AfterValidator(_clamp_to_float(1440.0))]
Seconds14400 = Annotated[int, AfterValidator(_clamp_to(14400))]

Page = Annotated[int, AfterValidator(lambda v: max(1, int(v)))]


def _valid_sort_dir(v: str) -> str:
    return v.upper() if v.upper() in ("ASC", "DESC") else "DESC"


SortDir = Annotated[str, AfterValidator(_valid_sort_dir)]


# ── Filter models ─────────────────────────────────────────────────────────────


class FilterSpec(BaseModel):
    """Multi-value filter for a single column.

    Example POST body:
        {"country": {"mode": "include", "values": ["US", "CA"]}}
    """

    mode: Literal["include", "exclude"] = "include"
    values: list[Any] = []


# The frontend sends filters as {column_name: {mode, values}} — a plain dict.
# We use a type alias so routers can annotate request bodies cleanly.
FiltersDict = dict[str, FilterSpec]


# ── Shared request fragments ──────────────────────────────────────────────────


class DateRangeMixin(BaseModel):
    """Mixin for endpoints that accept a start/end time window."""

    start_time: str | None = None
    end_time: str | None = None


class FilteredRequest(DateRangeMixin):
    """Base for POST bodies that carry both a date range and column filters."""

    filters: FiltersDict = {}


class RangeKeyedRequest(FilteredRequest):
    """``FilteredRequest`` plus the relative-range wire contract (additive, optional).

    When ``range_token`` is a recognized token the SERVER resolves the scan
    window from (token, ``anchor``) and ignores FE-supplied ``start_time`` /
    ``end_time`` (see ``RequestContext.resolve_window``), then clamps to the
    invite ceiling, so an analyst can't widen past their invite by picking
    ``"30d"``. The token also makes the FE first-paint key server-reproducible
    (the origin SSR-seed contract) and, on network-health, stabilizes the
    response-memo key. Absent/unknown token falls back to the legacy absolute
    bounds path. See ``backend/utils/time_window.py``.
    """

    range_token: str | None = None
    anchor: str | None = None


class LimitedRequest(FilteredRequest):
    """``FilteredRequest`` plus a row ``limit`` (clamped to 100, default 30)."""

    limit: Limit100 = 30


class PaginationMixin(BaseModel):
    """Mixin for endpoints that page/sort their results.

    Caps ``page`` at >=1 and validates ``sort_dir`` to ASC/DESC. ``limit`` is
    intentionally not part of this mixin — endpoints differ in their cap
    (Limit100 / Limit200 / Limit500) and should declare it explicitly.
    """

    page: Page = 1
    sort_dir: SortDir = "desc"


# ── Common response fragments ─────────────────────────────────────────────────


class DebugQuery(BaseModel):
    clickhouse: ClickHouseQueryObservation | None = Field(default=None, exclude_if=lambda value: value is None)
    sql: str
    time_ms: float
    is_cached: bool = False
    engine: str | None = None
    # Bind parameters, when a producer records them. No current producer does, so
    # the key is dropped from the wire unless it was set (see ``_drop_unset_params``).
    params: list[Any] | None = None

    @model_serializer(mode="wrap")
    def _drop_unset_params(self, handler):
        data = handler(self)
        if "params" not in self.model_fields_set:
            data.pop("params", None)
        return data


class DebugCall(BaseModel):
    service: str
    method: str
    path: str
    time_ms: float
    status: str | int | None = None
    details: str | None = None
    caller: str | None = None


class HasDataMixin(BaseModel):
    """Mixin for responses that report whether the query returned any rows."""

    has_data: bool = False
    total: int = 0


class LogExtentsMixin(BaseModel):
    """Mixin for responses that expose the per-service log time-extents.

    ``earliest_log_at`` / ``latest_log_at`` appear together on four
    response models (admin status + dashboard variants) — the audit
    flagged them as a one-shape pair worth co-locating so a new field
    on this pair (e.g. ``coverage_pct``) lands in one place.
    """

    earliest_log_at: str | None = None
    latest_log_at: str | None = None


class OkResponse(BaseModel):
    """Mixin for "ack" endpoints that only return ``{"ok": True}``.

    Six share-auth response models all carry ``ok: bool = True`` as
    their first field — promoted here so the field's default + name
    can't drift across the set.
    """

    ok: bool = True


# 038: telemetry payloads (raw SQL + outbound API URL/timing) are useful
# during development and incident response but they're an information-leak
# surface in normal operation — every analyst dashboard fetch echoes the
# server's internal SQL and the FOS object keys it touched. Gate inclusion
# on a process-level ``DEBUG_RESPONSES`` env var so production
# deployments default to "telemetry excluded from API responses" and an
# operator who needs the debug panel during triage can flip the flag and
# restart the process. The frontend DebugPanel reads ``_debug_queries`` /
# ``_debug_calls`` via optional-chain access so a missing field renders
# as an empty panel rather than throwing.
#
# Implementation uses ``model_serializer`` (not ``Field(exclude=...)``)
# so the OpenAPI schema continues to describe the fields — keeps the
# committed snapshot stable regardless of which mode the deployment
# is running in, and avoids per-deployment frontend type drift.
def _debug_responses_enabled() -> bool:
    return _os.getenv("DEBUG_RESPONSES", "").lower() in ("1", "true", "yes")


# Both spellings of every envelope key: the serialization aliases and the bare field names.
_DEBUG_ENVELOPE_KEYS = (
    "_debug_queries",
    "_debug_calls",
    "_debug_sqlite",
    "_debug_postgres",
    "debug_queries",
    "debug_calls",
    "debug_sqlite",
    "debug_postgres",
)


class ResponseCacheObservation(BaseModel):
    state: Literal["hit", "miss", "bypass", "unknown"]
    ttl_seconds: float | None = Field(default=None, gt=0)
    prewarmer: bool | None = None
    single_flight: Literal["leader", "follower"] | None = None
    scope: Literal["self", "subtree"] | None = None
    wait_ms: float | None = Field(default=None, ge=0)

    @model_serializer(mode="wrap")
    def _omit_absent(self, handler):
        return {key: value for key, value in handler(self).items() if value is not None or key == "prewarmer"}


class SectionTimingEntry(WireSafeModel):
    """Per-phase wall-clock timing entry in ``BaseResponse.section_timings``."""

    section: str | None = None
    section_id: str | None = None
    name: str | None = None
    time_ms: float | None = None
    duration_ms: float | None = None
    cache: str | None = None


class BaseResponse(BaseModel):
    """Base response that automatically includes telemetry if present."""

    debug_queries: list[DebugQuery] = Field(default_factory=list, serialization_alias="_debug_queries")
    debug_calls: list[DebugCall] = Field(default_factory=list, serialization_alias="_debug_calls")
    # SQLite statements executed while serving THIS request (page-scoped
    # Postgres/metadata SQL statements captured for this request.
    debug_sqlite: list[dict] = Field(default_factory=list, serialization_alias="_debug_sqlite")
    debug_postgres: list[dict] = Field(default_factory=list, serialization_alias="_debug_postgres")
    is_cached: bool = Field(default=False, serialization_alias="_is_cached")
    response_cache: ResponseCacheObservation | None = Field(
        default=None,
        validation_alias=AliasChoices("response_cache", "_response_cache"),
        serialization_alias="_response_cache",
    )
    # Per-phase wall-clock timing for the handler. Always emitted as
    # _section_timings under serialization. Default empty so endpoints
    # that don't instrument get a benign empty list. Safe to surface in
    # prod — phase names + millisecond timings are operational metadata,
    # not SQL/URLs.
    section_timings: list[SectionTimingEntry] = Field(default_factory=list, serialization_alias="_section_timings")

    @model_serializer(mode="wrap")
    def _strip_debug_when_disabled(self, handler):
        data = handler(self)
        if self.response_cache is None:
            data.pop("_response_cache", None)
            data.pop("response_cache", None)
        if not _debug_responses_enabled():
            for key in _DEBUG_ENVELOPE_KEYS:
                data.pop(key, None)
        return data

    @classmethod
    def with_telemetry(cls, **data):
        """Helper to create a response with context-local telemetry."""
        from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

        dq = data.pop("debug_queries", None) or get_queries()
        ds = data.pop("debug_postgres", None) or data.pop("debug_sqlite", None) or list(get_sqlite_queries())
        dc = data.pop("debug_calls", None) or get_tracked_calls()

        return cls(**data, debug_queries=dq, debug_calls=dc, debug_sqlite=ds, debug_postgres=ds)


class BootstrapService(WireSafeModel):
    service_id: str
    name: str | None = None
    access_level: str | None = None
    cmcd_enabled: bool | None = None
    rum_enabled: bool | None = None
    rum_log_period: int | None = None
    analyst_path_a_supported: bool = True
    analyst_path_a_reason: str | None = None
    is_high_scale: bool | None = None


class BootstrapSettings(WireSafeModel):
    """Typed settings envelope returned by ``GET /api/bootstrap``."""

    initializing: bool | None = None
    access_level: str | None = None
    storage_mode: str | None = None
    is_remote_analyst: bool | None = None
    needs_login: bool | None = None
    analyst_email: str | None = None
    analyst_name: str | None = None
    mask_ips: bool | None = None
    web_vitals_enabled: bool | None = None


class BootstrapPopGeo(WireSafeModel):
    """One PoP's label parts, parsed from the cached ``/datacenters`` list
    (``get_pop_location_map``); any part may be an empty string."""

    city: str
    region: str
    country: str


class BootstrapDebugState(WireSafeModel):
    """Diagnostics capability and audience settings for authenticated clients."""

    debug_responses_enabled: bool
    query_debug_visibility: str
    api_call_debug_visibility: str


class CustomCard(WireSafeModel):
    """Custom dashboard card entry in BootstrapResponse."""

    id: str
    label: str


class LastSyncInfo(WireSafeModel):
    """Latest non-running sync cron run summary in BootstrapResponse."""

    started_at: str | None = None
    status: str | None = None
    duration_s: float | None = None


from backend.models._base import SchemaColumn  # noqa: E402
from backend.models.admin import (  # noqa: E402
    LogAccountingResponse,
    LogExtentsResponse,
    ShareHeaderBadgeResponse,
    SyncStatusResponse,
)
from backend.models.admin_queries import (  # noqa: E402
    SlowQueriesCountResponse,
    SummaryResponse,
)
from backend.models.services import CronRunsPageResponse  # noqa: E402
from backend.models.session_scoring import ScoringLabelsResponse  # noqa: E402
from backend.models.share_admin import ShareStatusBannerResponse  # noqa: E402
from backend.models.views import SavedView  # noqa: E402


class OpsOverviewResponse(WireSafeModel):
    """Seed for OperationsOverview admin cards in BootstrapResponse."""

    queries_summary: SummaryResponse | None = None
    slow_queries_count: SlowQueriesCountResponse | None = None
    log_accounting: LogAccountingResponse | None = None


class BootstrapResponse(BaseResponse):
    active_service_id: str | None
    services: list[BootstrapService]
    schema_: list[SchemaColumn] | None = Field(default=None, alias="schema")
    # _safe_table_name(active_source.name) — the same value /api/schema
    # returns alongside its schema list. Folded in so the frontend can
    # seed its ['admin', 'schema', service_id] React Query cache with the
    # full {schema, table_name} payload and skip the dedicated round-trip
    # on /query nav. None when no active service / status not populated.
    table_name: str | None = None
    countries: dict[str, str] | None = None
    pops: dict[str, tuple[float | None, float | None]] | None = None
    # Per-PoP {city, region, country} for the shared PoP label
    # ("DEN (Denver, CO - USA)"). Parsed from the cached /datacenters list;
    # the frontend formats + renders it (see frontend/lib/pop.ts). Separate
    # from `pops` (lat/lon) so the world-map consumer is untouched.
    pop_geo: dict[str, BootstrapPopGeo] | None = None
    settings: BootstrapSettings | None = None
    custom_dashboard_cards: list[CustomCard] = Field(default_factory=list)
    active_log_field_ids: list[str] = Field(default_factory=list)
    ngwaf_configured: bool = False
    # Saved views for the active service, folded in so the frontend can
    # render ViewSelector and rehydrate from URL view params without a
    # second /api/views/{service_id} round-trip on every page nav.
    views: list[SavedView] = Field(default_factory=list)
    # Full log-fields catalog (same payload as /api/log-fields/catalog)
    # for the active service. Folded in so the frontend can seed its
    # ['log-fields-catalog', service_id] React Query cache and skip the
    # 35-KB round-trip on every cold page load (perf audit Phase D).
    # None when no active service.
    log_fields_catalog: LogFieldsCatalogResponse | None = None
    # Cached sync-status (same fast-path payload /api/sync-status?skip_fos=true
    # returns). Folded in so SyncStatusBadge / logs page hit cache on
    # first mount. ADMIN ONLY — None for analyst sessions (matches the
    # dedicated endpoint's 403 for analysts).
    sync_status: SyncStatusResponse | None = None
    # Lean share-status banner ({sharing_active, public_url}). Folded
    # in so the global share banner has its initial state on first
    # render and skips the first /api/admin/share/banner poll.
    # ADMIN ONLY — analysts don't manage sharing.
    share_banner: ShareStatusBannerResponse | None = None
    # Analyst-safe sibling of sync_status, projected down to the two
    # fields the global SyncStatusBadge renders (latest_log_at,
    # local_rows). Available to BOTH admin AND analyst sessions so the
    # badge shows on prod for analyst-shared instances too.
    header_badge: ShareHeaderBadgeResponse | None = None
    # Analyst-safe log extents (same shape as /api/log-extents): the
    # earliest + latest log timestamps the FilterBar uses for its
    # auto-range snap-to-extents. Folded in so the FilterBar's first
    # render skips the dedicated round-trip; the existing 3-s
    # not-yet-populated poll continues from useFilterBar for new
    # services where extents land later.
    log_extents: LogExtentsResponse | None = None
    # Admin DiagnosticsPanel reads backend capability; both roles use the
    # audience settings to honor operator-enabled diagnostics. The response
    # middleware independently enforces analyst visibility.
    debug_state: BootstrapDebugState | None = None
    # Seed for the OperationsOverview admin cards:
    # ``{queries_summary, log_accounting, slow_queries_count}``.
    # The card-level useQueries (10-s poll, same queryKeys) hit cache
    # on first paint so the cards render with real values instead of
    # "—" placeholders. ADMIN ONLY.
    ops_overview: OpsOverviewResponse | None = None
    # Seed for the /logs cron tab: mirrors the lean delta-poll shape of
    # /api/cron-runs?per_page=10 with ``with_total=False`` so the
    # count(*) precount stays off the cold-path WAL writer. The heavy
    # 500-row cron-history pull is intentionally NOT seeded — it's
    # tab-gated and a session-3 lesson (commit bbbd381) showed seeding
    # expensive payloads dominates the bootstrap hot path. The cron
    # schedule itself is a lazy-load on the cron tab (P1#5).
    # ADMIN ONLY — analysts don't reach /logs.
    cron_runs_first_page: CronRunsPageResponse | None = None
    # Seed for useLastSync (the "Last Sync: Xs ago" header badge).
    # Shape: ``{started_at, status, duration_s}`` of the latest non-running
    # sync cron run, derived from ``latest_cron_per_task("sync")``. Without
    # this seed every admin page-load fired one mandatory
    # /api/cron-runs?task=sync request + 1-2 SSE-invalidation-driven
    # refetches before the 5-min poll TTL kicked in. ADMIN ONLY.
    last_sync: LastSyncInfo | None = None
    # Seed for useScoringLabels — TopFlaggedTable + admin Labels tab +
    # dashboard Flag column all read from ``['scoring-labels', sid]``.
    # Without this seed every cold load fires
    # GET /api/services/{sid}/scoring/labels (p95 311 ms admin / 702 ms
    # analyst). Shape mirrors the endpoint: ``{labels: [...], counts: {...}}``.
    # ADMIN ONLY.
    scoring_labels: ScoringLabelsResponse | None = None
    # section_timings is inherited from BaseResponse.


class UxEventPayload(BaseModel):
    """Generic UX-event envelope. Specific event types live as
    ``event`` discriminator strings; the ``details`` bag carries
    event-specific fields without forcing a per-event schema."""

    event: str = Field(..., max_length=80)
    pathname: str | None = Field(default=None, max_length=300)
    component_id: str | None = Field(default=None, max_length=120)
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("details")
    @classmethod
    def validate_details(cls, v: dict[str, Any]) -> dict[str, Any]:
        if len(str(v)) > 4096:
            raise ValueError("Details payload too large")

        def _check(obj: Any, depth: int = 0) -> None:
            if depth > 5:
                raise ValueError("Details too deep")
            if isinstance(obj, dict):
                if len(obj) > 50:
                    raise ValueError("Too many items")
                for val in obj.values():
                    _check(val, depth + 1)
            elif isinstance(obj, list):
                if len(obj) > 50:
                    raise ValueError("Too many items")
                for item in obj:
                    _check(item, depth + 1)

        _check(v)
        return v

    model_config = {
        "extra": "forbid",
    }


class WebVitalsPayload(BaseModel):
    """Mirrors the web-vitals SDK metric shape (v5 API)."""

    id: str = Field(..., max_length=120)
    name: Literal["CLS", "FCP", "FID", "INP", "LCP", "TTFB"]
    value: float
    rating: Literal["good", "needs-improvement", "poor"]
    pathname: str | None = Field(default=None, max_length=300)
    navigation_type: str | None = Field(default=None, max_length=40)
    delta: float | None = None
