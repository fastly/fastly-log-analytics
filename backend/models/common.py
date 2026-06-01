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


from pydantic import Field


class HasDataMixin(BaseModel):
    """Mixin for responses that report whether the query returned any rows."""

    has_data: bool = False
    total: int = 0


class BaseResponse(BaseModel):
    """Base response that automatically includes telemetry if present."""

    debug_queries: list[DebugQuery] = Field(default_factory=list, serialization_alias="_debug_queries")
    debug_calls: list[DebugCall] = Field(default_factory=list, serialization_alias="_debug_calls")
    is_cached: bool = Field(default=False, serialization_alias="_is_cached")

    @classmethod
    def with_telemetry(cls, **data):
        """Helper to create a response with context-local telemetry."""
        from backend.utils.telemetry import get_queries, get_tracked_calls

        dq = data.pop("debug_queries", None) or get_queries()
        dc = data.pop("debug_calls", None) or get_tracked_calls()

        return cls(**data, debug_queries=dq, debug_calls=dc)


class RowsResponse(BaseResponse):
    """Base for endpoints that return a has_data flag and a list of row dicts.

    Subclass and add endpoint-specific fields; inherit the telemetry fields
    from BaseResponse for free.
    """

    has_data: bool = False
    rows: list[dict] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: str
    busy: bool = False
    no_service: bool = False


class BootstrapService(BaseModel):
    service_id: str
    name: str | None = None
    access_level: str | None = None


class BootstrapResponse(BaseResponse):
    active_service_id: str | None
    services: list[BootstrapService]
    schema_: list[dict] | None = Field(default=None, alias="schema")
    countries: dict[str, str] | None = None
    pops: dict[str, tuple[float | None, float | None]] | None = None
    settings: dict[str, str | bool | None] | None = None
    custom_dashboard_cards: list[dict] = Field(default_factory=list)
    custom_fields_catalog: list[dict] = Field(default_factory=list)
    active_log_field_ids: list[str] = Field(default_factory=list)
