"""Shared Pydantic models used across multiple routers."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel

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
    sql: str
    time_ms: float
    is_cached: bool = False


class DebugCall(BaseModel):
    service: str
    method: str
    path: str
    time_ms: float
    status: str | int | None = None
    details: str | None = None
    caller: str | None = None


import os as _os

from pydantic import Field, model_serializer


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


class BaseResponse(BaseModel):
    """Base response that automatically includes telemetry if present."""

    debug_queries: list[DebugQuery] = Field(default_factory=list, serialization_alias="_debug_queries")
    debug_calls: list[DebugCall] = Field(default_factory=list, serialization_alias="_debug_calls")
    # SQLite statements executed while serving THIS request (page-scoped
    # sibling of debug_queries). Plain dicts — the wire shape is pinned by
    # backend/models/debug.py::SqliteProfilerEntry, which the ring-buffer
    # endpoint already exposes; duplicating the model here as a typed field
    # would force common.py → debug.py imports for zero runtime validation
    # gain on a debug-only envelope.
    debug_sqlite: list[dict] = Field(default_factory=list, serialization_alias="_debug_sqlite")
    is_cached: bool = Field(default=False, serialization_alias="_is_cached")
    # Per-phase wall-clock timing for the handler. Always emitted as
    # _section_timings under serialization. Default empty so endpoints
    # that don't instrument get a benign empty list. Safe to surface in
    # prod — phase names + millisecond timings are operational metadata,
    # not SQL/URLs.
    section_timings: list[dict] = Field(default_factory=list, serialization_alias="_section_timings")

    @model_serializer(mode="wrap")
    def _strip_debug_when_disabled(self, handler):
        data = handler(self)
        if not _debug_responses_enabled():
            data.pop("_debug_queries", None)
            data.pop("_debug_calls", None)
            data.pop("_debug_sqlite", None)
            data.pop("debug_queries", None)
            data.pop("debug_calls", None)
            data.pop("debug_sqlite", None)
        return data

    @classmethod
    def with_telemetry(cls, **data):
        """Helper to create a response with context-local telemetry."""
        from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

        dq = data.pop("debug_queries", None) or get_queries()
        # Snapshot-copy: get_tracked_calls() below may itself run a SQLite
        # SELECT (the usage_log iothread augmentation), and the collector
        # returns a live list — copying first keeps that debug-induced
        # statement out of the page's view.
        ds = data.pop("debug_sqlite", None) or list(get_sqlite_queries())
        dc = data.pop("debug_calls", None) or get_tracked_calls()

        return cls(**data, debug_queries=dq, debug_calls=dc, debug_sqlite=ds)


class BootstrapService(BaseModel):
    service_id: str
    name: str | None = None
    access_level: str | None = None


class BootstrapResponse(BaseResponse):
    active_service_id: str | None
    services: list[BootstrapService]
    schema_: list[dict] | None = Field(default=None, alias="schema")
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
    pop_geo: dict[str, dict[str, str]] | None = None
    settings: dict[str, str | bool | None] | None = None
    custom_dashboard_cards: list[dict] = Field(default_factory=list)
    active_log_field_ids: list[str] = Field(default_factory=list)
    # Saved views for the active service, folded in so the frontend can
    # render ViewSelector and rehydrate from URL view params without a
    # second /api/views/{service_id} round-trip on every page nav.
    views: list[dict] = Field(default_factory=list)
    # Full log-fields catalog (same payload as /api/log-fields/catalog)
    # for the active service. Folded in so the frontend can seed its
    # ['log-fields-catalog', service_id] React Query cache and skip the
    # 35-KB round-trip on every cold page load (perf audit Phase D).
    # None when no active service.
    log_fields_catalog: dict | None = None
    # Cached sync-status (same fast-path payload /api/sync-status?skip_fos=true
    # returns). Folded in so SyncStatusBadge / logs page hit cache on
    # first mount. ADMIN ONLY — None for analyst sessions (matches the
    # dedicated endpoint's 403 for analysts).
    sync_status: dict | None = None
    # Lean share-status banner ({sharing_active, public_url}). Folded
    # in so the global share banner has its initial state on first
    # render and skips the first /api/admin/share/banner poll.
    # ADMIN ONLY — analysts don't manage sharing.
    share_banner: dict | None = None
    # Analyst-safe sibling of sync_status, projected down to the two
    # fields the global SyncStatusBadge renders (latest_log_at,
    # local_rows). Available to BOTH admin AND analyst sessions so the
    # badge shows on prod for analyst-shared instances too.
    header_badge: dict | None = None
    # Analyst-safe log extents (same shape as /api/log-extents): the
    # earliest + latest log timestamps the FilterBar uses for its
    # auto-range snap-to-extents. Folded in so the FilterBar's first
    # render skips the dedicated round-trip; the existing 3-s
    # not-yet-populated poll continues from useFilterBar for new
    # services where extents land later.
    log_extents: dict | None = None
    # Whether the backend will populate ``_debug_queries`` /
    # ``_debug_calls`` envelopes on responses (gated by the
    # ``DEBUG_RESPONSES`` env var). Folded in so the admin
    # DiagnosticsPanel can dim the "Query debugging" / "API call"
    # toggles on first paint instead of paying a separate
    # /api/debug/state round-trip. ADMIN ONLY — analysts never see
    # the diagnostics panel and the toggles aren't user-facing.
    debug_state: dict | None = None
    # Seed for the OperationsOverview admin cards:
    # ``{queries_summary, log_accounting, slow_queries_count}``.
    # The card-level useQueries (10-s poll, same queryKeys) hit cache
    # on first paint so the cards render with real values instead of
    # "—" placeholders. ADMIN ONLY.
    ops_overview: dict | None = None
    # Seed for the /logs cron tab: mirrors the lean delta-poll shape of
    # /api/cron-runs?per_page=10 with ``with_total=False`` so the
    # count(*) precount stays off the cold-path WAL writer. The heavy
    # 500-row cron-history pull is intentionally NOT seeded — it's
    # tab-gated and a session-3 lesson (commit bbbd381) showed seeding
    # expensive payloads dominates the bootstrap hot path. The cron
    # schedule itself is a lazy-load on the cron tab (P1#5).
    # ADMIN ONLY — analysts don't reach /logs.
    cron_runs_first_page: dict | None = None
    # Seed for useLastSync (the "Last Sync: Xs ago" header badge).
    # Shape: ``{started_at, status, duration_s}`` of the latest non-running
    # sync cron run, derived from ``latest_cron_per_task("sync")``. Without
    # this seed every admin page-load fired one mandatory
    # /api/cron-runs?task=sync request + 1-2 SSE-invalidation-driven
    # refetches before the 5-min poll TTL kicked in. ADMIN ONLY.
    last_sync: dict | None = None
    # Seed for useScoringLabels — TopFlaggedTable + admin Labels tab +
    # dashboard Flag column all read from ``['scoring-labels', sid]``.
    # Without this seed every cold load fires
    # GET /api/services/{sid}/scoring/labels (p95 311 ms admin / 702 ms
    # analyst). Shape mirrors the endpoint: ``{labels: [...], counts: {...}}``.
    # ADMIN ONLY.
    scoring_labels: dict | None = None
    # section_timings is inherited from BaseResponse.
