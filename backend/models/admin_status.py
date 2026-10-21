"""Response models for admin status reads that previously returned bare dicts.

Producers: ``backend/celery_status.get_celery_status``, the duckdb status route
in ``routers/admin/duckdb.py``, ``utils/ngwaf_bot_cache.get_cache_stats`` and
``utils/rdns_cache.get_stats``. Inner dicts come from Celery/DuckDB/SQLite and
may hold non-JSON natives, so they keep the ``jsonable_encoder`` encoding.
"""

from __future__ import annotations

from typing import Any, Literal

from backend.models._base import JsonAny, JsonDict, WireSafeModel


class CeleryActiveTask(WireSafeModel):
    """One entry of Celery ``inspect().active()``; other inspect keys ride as extras."""

    id: str
    name: str
    args: list[Any]
    time_start: int | float


class CeleryWorkers(WireSafeModel):
    """``workers`` block of ``get_celery_status``. ``count`` is ``len(stats)``;
    ``error`` is present only when the inspect call failed."""

    active_tasks: dict[str, list[CeleryActiveTask]]
    stats: JsonDict
    registered: dict[str, list[str]]
    scheduled: JsonDict
    count: int
    error: str | None = None


class CeleryScheduleEntry(WireSafeModel):
    name: str
    task: str


class CeleryStatusResponse(WireSafeModel):
    """``queues`` is ``{queue: depth}``; ``ledger`` is ``{ingest_ledger.status: count}``
    summed across services, so both keep open string keys."""

    broker_reachable: bool | None = None
    workers: CeleryWorkers | None = None
    queues: dict[str, int] | None = None
    schedules: list[CeleryScheduleEntry] | None = None
    ledger: dict[str, int] | None = None


class IngestLedgerEntry(WireSafeModel):
    """Single row from ``ingest_ledger`` exposed on ``/api/admin/celery/ledger``."""

    service_id: str
    object_key: str
    source_type: Literal["request", "rum"]
    status: str
    attempts: int = 0
    error_kind: str | None = None
    last_error: str | None = None
    claimed_by: str | None = None
    claimed_at: float | None = None
    discovered_at: float | None = None
    committed_at: float | None = None
    next_attempt_at: float | None = None
    originating_task: str | None = None
    originating_run_id: int | None = None
    size_bytes: int | None = None


class IngestLedgerListResponse(WireSafeModel):
    items: list[IngestLedgerEntry]
    total: int


class IngestLedgerItemTarget(WireSafeModel):
    service_id: str
    object_key: str


class IngestLedgerActionRequest(WireSafeModel):
    """Target either specific ``items`` or all rows matching ``status`` (optionally
    scoped to ``service_id``)."""

    items: list[IngestLedgerItemTarget] | None = None
    status: Literal["dead_letter", "quarantined", "problem"] | None = None
    service_id: str | None = None


class IngestLedgerActionResponse(WireSafeModel):
    ok: bool = True
    affected: int = 0
    dispatched: int = 0


class DuckdbStatusResponse(WireSafeModel):
    ok: bool
    memory: JsonDict | None = None
    barrier: JsonDict | None = None
    pools: JsonAny = None
    retired_pools: JsonAny = None


class NgwafStatusResponse(WireSafeModel):
    """``total_cached_bots`` / ``workspaces`` / ``top_bots`` come from the cache stats;
    ``service_workspace_id`` / ``service_configured`` only when ``service_id`` was passed."""

    ok: bool
    total_cached_bots: int | None = None
    workspaces: dict[str, str | None] | None = None
    top_bots: dict[str, int] | None = None
    service_workspace_id: str | None = None
    service_configured: bool | None = None


class RdnsStatsResponse(WireSafeModel):
    ok: bool
    stats: JsonDict | None = None
