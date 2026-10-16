"""Response models for admin status reads that previously returned bare dicts.

Producers: ``backend/celery_status.get_celery_status``, the duckdb status route
in ``routers/admin/duckdb.py``, ``utils/ngwaf_bot_cache.get_cache_stats`` and
``utils/rdns_cache.get_stats``. Inner dicts come from Celery/DuckDB/SQLite and
may hold non-JSON natives, so they keep the ``jsonable_encoder`` encoding.
"""

from __future__ import annotations

from backend.models._base import JsonAny, JsonDict, JsonRows, WireSafeModel


class CeleryStatusResponse(WireSafeModel):
    broker_reachable: bool | None = None
    workers: JsonDict | None = None
    queues: dict[str, int] | None = None
    schedules: JsonRows | None = None
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
