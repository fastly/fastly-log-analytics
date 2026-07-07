"""Pydantic models for the admin Live Query Monitor.

Carved out of ``backend.routers.admin_queries`` so the schemas live with
the other ``backend.models.*`` modules and OpenAPI generation has one
canonical location to find them.

Row shapes are derived from the PRODUCERS in
``backend.core.query_registry`` (``_row_for_active`` /
``_row_for_completed`` / ``_attribution_payload``) and the
``/slow-queries`` re-shape loop in ``backend.routers.admin_queries`` —
which intentionally mirrors the completed-row layout. The row models set
``extra="allow"`` so a future producer key passes through verbatim, and
every field is Optional so validation can never 500 a monitoring
endpoint. Producers emit every key on every row (with ``None`` where
absent), so serialization without ``exclude_unset`` stays byte-identical.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class _QueryRow(BaseModel):
    """Base for registry row payloads — passes undeclared keys through."""

    model_config = ConfigDict(extra="allow")


class QueryAttribution(_QueryRow):
    """``attribution`` block from ``_attribution_payload``."""

    kind: str | None = None
    label: str | None = None
    principal_id: str | None = None
    caller_qualname: str | None = None
    caller_file: str | None = None
    request_path: str | None = None
    request_id: str | None = None
    cron_job: str | None = None
    cron_run_id: str | None = None
    pool_slot: str | None = None


class ActiveQueryRow(_QueryRow):
    """One in-flight query (``_row_for_active``). ``sql`` is only populated
    when the caller asked for ``full_sql``; ``started_at_utc`` is epoch
    seconds (``time.time()``)."""

    query_id: int | None = None
    db_type: str | None = None
    sql_preview: str | None = None
    sql: str | None = None
    sql_len: int | None = None
    attribution: QueryAttribution | None = None
    service_id: str | None = None
    started_at_utc: float | None = None
    duration_ms: float | None = None
    cancellable: bool | None = None
    cancelled_at: float | None = None


class CompletedQueryRow(_QueryRow):
    """One finished query (``_row_for_completed``); also the exact layout
    the persisted ``/slow-queries`` rows are re-shaped into."""

    query_id: int | None = None
    db_type: str | None = None
    sql_preview: str | None = None
    sql: str | None = None
    sql_len: int | None = None
    attribution: QueryAttribution | None = None
    service_id: str | None = None
    started_at_utc: float | None = None
    ended_at_utc: float | None = None
    duration_ms: float | None = None
    outcome: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    peak_memory_mb: float | None = None


class SnapshotResponse(BaseModel):
    """Incremental snapshot of the in-process query registry.

    ``last_seq`` is the high-water mark the client should send back as
    ``since_seq`` on its next poll so the registry can stream only new rows.
    """

    last_seq: int
    active: list[ActiveQueryRow]
    completed: list[CompletedQueryRow]


class SummaryResponse(BaseModel):
    """Cheap counts that power the live-monitor tab badge."""

    active_total: int
    by_db_type: dict[str, int]
    longest_ms: float


class CancelResponse(BaseModel):
    """Outcome of ``POST /api/admin/queries/{qid}/cancel``."""

    state: Literal["cancelled", "not_found", "already_finished", "connection_gone"]
    query_id: int


class SlowQueriesCountResponse(BaseModel):
    """Aggregate row-count for the operations-overview card."""

    count: int
    since_hours: int
    threshold_ms: float


class SlowQueriesResponse(BaseModel):
    """Persisted slow-SQL history (``GET /api/admin/slow-queries``)."""

    rows: list[CompletedQueryRow]
    since_hours: int
    threshold_ms: float


class QueryMonitorConfigResponse(BaseModel):
    """Nav-gating flag for the Live Query Monitor tab."""

    enabled: bool
