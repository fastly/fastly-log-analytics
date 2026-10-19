"""Response models for admin status reads that previously returned bare dicts.

Producers: ``backend/celery_status.get_celery_status``, the duckdb status route
in ``routers/admin/duckdb.py``, ``utils/ngwaf_bot_cache.get_cache_stats`` and
``utils/rdns_cache.get_stats``. Inner dicts come from Celery/DuckDB/SQLite and
may hold non-JSON natives, so they keep the ``jsonable_encoder`` encoding.
"""

from __future__ import annotations

from typing import Any

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
