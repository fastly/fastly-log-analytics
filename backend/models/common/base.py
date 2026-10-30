"""Core base models, request/response mixins, clamping types, and telemetry envelopes."""

from __future__ import annotations

import os as _os
from enum import StrEnum
from typing import Annotated, Any, Literal

from fastapi.encoders import jsonable_encoder
from pydantic import (
    AfterValidator,
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    model_serializer,
)


class MetricType(StrEnum):
    """Available metrics for timeseries and analysis endpoints."""

    REQUESTS = "requests"
    ERRORS_5XX = "5xx"
    ERRORS_4XX = "4xx"
    HIT_RATE = "hit_rate"
    LATENCY_P95 = "p95_latency"
    THROUGHPUT = "throughput"
    REQ_SIZE = "req_size"
    TTFB = "ttfb"
    SPECIFIC_STATUS = "specific_status"


class ClickHouseQueryObservation(BaseModel):
    query_id: str
    request_id: str | None = None
    page_load_id: str | None = None
    service_id: str | None = None
    component: str | None = None
    section: str | None = None
    section_id: str | None = None
    parent_section_id: str | None = None
    source: Literal["fact", "projection", "projected_fact", "mixed", "unknown"] = "unknown"
    outcome: Literal["success", "error", "timeout"]
    client_elapsed_ms: float = Field(ge=0, allow_inf_nan=False)
    admission_wait_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    network_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    decode_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    server_elapsed_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    server_elapsed_source: Literal["statistics.elapsed"] | None = None
    server_cpu_ms: None = None
    timing_kind: Literal["client_operation_wall"] = "client_operation_wall"


class PassthroughModel(BaseModel):
    """Read model that tolerates unknown keys (``extra="allow"``)."""

    model_config = ConfigDict(extra="allow")

    def __getitem__(self, key: str) -> Any:
        if key in type(self).model_fields:
            return getattr(self, key)
        extra = self.__pydantic_extra__ or {}
        if key in extra:
            return extra[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        extra = self.__pydantic_extra__ or {}
        return (
            key in self.model_fields_set
            or key in extra
            or (key in type(self).model_fields and getattr(self, key) is not None)
        )


class WireSafeModel(PassthroughModel):
    """``extra="allow"`` model whose serialization omits never-set fields.

    Nested use inside a model whose route does not pass
    ``response_model_exclude_unset=True`` would otherwise emit every declared
    optional field as ``null``. Dropping unset declared fields here keeps the
    wire payload byte-identical to the dict the producer built.
    """

    @model_serializer(mode="wrap")
    def _drop_unset(self, handler):
        data = handler(self)
        fields = type(self).model_fields
        for name in fields.keys() - self.model_fields_set:
            data.pop(name, None)
            info = fields[name]
            for alias in (info.alias, info.serialization_alias):
                if alias:
                    data.pop(alias, None)
        return data


class SchemaColumn(WireSafeModel):
    column: str | None = None
    name: str = ""
    type: str
    min: str | None = None
    max: str | None = None
    approx_unique: int | None = None
    null_percentage: float | None = None
    count: int | None = None


# One cell of a raw-log row: VARCHAR, the integer/float widths and BOOLEAN pass through; TIMESTAMP
# columns arrive already serialized to ISO strings.
ScalarCell = str | int | float | bool | None

# DB/cache-native values (Decimal, tz-aware datetimes, bytes) encode differently under
# pydantic's JSON mode than under ``jsonable_encoder`` (the pre-typing wire path, e.g.
# ``+00:00`` vs ``Z``). These aliases keep the old encoding while the OpenAPI schema
# still advertises the row/dict shape.
JsonRows = Annotated[
    list[dict[str, Any]], PlainSerializer(jsonable_encoder, return_type=list[dict[str, Any]], when_used="json")
]
JsonAny = Annotated[Any, PlainSerializer(jsonable_encoder, when_used="json")]
JsonDict = Annotated[dict[str, Any], PlainSerializer(jsonable_encoder, return_type=dict[str, Any], when_used="json")]


class ErrorDetail(PassthroughModel):
    """Inner detail object for the canonical error envelope.

    Allows extra fields because a handful of historical 403 sites attach
    context (``service``, ``invite_id``, etc.) alongside the canonical
    ``error`` code. Locking the shape down would force a coordinated
    rewrite of those handlers without changing observable behavior — the
    frontend's ``extractApiError`` already keys on ``detail.error`` and
    ignores extras.
    """

    error: str
    """Machine-readable code the frontend keys on (e.g. ``"not_found"``)."""
    error_id: str | None = None
    """Server-side correlation id; populated by ``raise_internal`` on 5xx."""
    messages: list[str] | None = None
    """Optional human-readable messages, populated by ``validation_failed``."""


class ErrorEnvelope(BaseModel):
    """Canonical FastAPI HTTPException envelope shape."""

    detail: ErrorDetail


# ── Default responses= mapping ────────────────────────────────────────────────
#
# Apply to every router via ``APIRouter(responses=DEFAULT_ERROR_RESPONSES)``
# so the OpenAPI schema documents every error code the router tree actually
# raises. Per-endpoint ``responses=`` keyword arguments still override these
# entries when an endpoint needs a more specific shape (e.g. the 415 from
# provision.check_fos which uses an HTTP-standard reason phrase).
DEFAULT_ERROR_RESPONSES: dict[int | str, dict] = {
    400: {"model": ErrorEnvelope, "description": "Bad request"},
    401: {"model": ErrorEnvelope, "description": "Unauthenticated"},
    403: {"model": ErrorEnvelope, "description": "Forbidden"},
    404: {"model": ErrorEnvelope, "description": "Not found"},
    422: {"model": ErrorEnvelope, "description": "Validation failed"},
    429: {"model": ErrorEnvelope, "description": "Rate limited"},
    500: {"model": ErrorEnvelope, "description": "Internal error"},
    502: {"model": ErrorEnvelope, "description": "Upstream error"},
    503: {"model": ErrorEnvelope, "description": "Service unavailable"},
}


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
    "_component_observations",
    "component_observations",
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
        if not _debug_responses_enabled():
            return cls(**data)

        from backend.utils.telemetry import get_queries, get_sqlite_queries, get_tracked_calls

        dq = data.pop("debug_queries", None) or get_queries()
        ds = data.pop("debug_postgres", None) or data.pop("debug_sqlite", None) or list(get_sqlite_queries())
        dc = data.pop("debug_calls", None) or get_tracked_calls()

        return cls(**data, debug_queries=dq, debug_calls=dc, debug_sqlite=ds, debug_postgres=ds)
