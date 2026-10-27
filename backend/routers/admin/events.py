"""Multiplexed admin SSE channel (`/api/admin/events/stream`)."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator
from typing import Any, cast

from fastapi import Depends, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

import backend.celery_status_sampler as celery_status_sampler
from backend.cron_runs_publisher import publisher as cron_runs_publisher
from backend.deps import get_service_id
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.sync_status_snapshot import compute_sync_status_cached
from backend.utils.router_utils import make_error
from backend.utils.sse import iter_with_disconnect_ping, sse_response

from ._router import router
from .health import sample_system_metrics_cached

logger = logging.getLogger(__name__)

_ORIG_COMPUTE_SYNC_STATUS_CACHED = compute_sync_status_cached
_ORIG_SAMPLE_SYSTEM_METRICS_CACHED = sample_system_metrics_cached

_ADMIN_EVENT_CHANNELS = frozenset({"sync-status", "cron-runs", "system-metrics", "share", "celery-status"})

_METRICS_SAMPLE_INTERVAL_SECONDS = 10.0
_SHARE_SAMPLE_INTERVAL_SECONDS = 10.0
_CELERY_SAMPLE_INTERVAL_SECONDS = 5.0

_FANIN_MAXSIZE = 16


def _offer(q: asyncio.Queue[dict[str, Any]], env: dict[str, Any]) -> None:
    """Non-blocking enqueue with drop-oldest."""
    if q.full():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        q.put_nowait(env)
    except asyncio.QueueFull:
        pass


def _resolve_sync_status_fn() -> Any:
    if compute_sync_status_cached is not _ORIG_COMPUTE_SYNC_STATUS_CACHED:
        return compute_sync_status_cached
    for mod_name in ("backend.routers.admin", "backend.routers.admin.sync_status", "backend.routers.admin.health"):
        mod = sys.modules.get(mod_name)
        if mod is not None:
            fn = getattr(mod, "compute_sync_status_cached", None)
            if fn is not None and fn is not _ORIG_COMPUTE_SYNC_STATUS_CACHED:
                return fn
    return compute_sync_status_cached


def _resolve_metrics_sampler() -> Any:
    if sample_system_metrics_cached is not _ORIG_SAMPLE_SYSTEM_METRICS_CACHED:
        return sample_system_metrics_cached
    health_mod = sys.modules.get("backend.routers.admin.health")
    if health_mod is not None:
        fn = getattr(health_mod, "sample_system_metrics_cached", None)
        if fn is not None and fn is not _ORIG_SAMPLE_SYSTEM_METRICS_CACHED:
            return fn
    return sample_system_metrics_cached


async def _sync_status_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    fn = _resolve_sync_status_fn()
    initial = await asyncio.to_thread(fn, service_id)
    if initial is not None:
        _offer(q, {"channel": "sync-status", "data": initial})
    async for payload in sync_status_publisher.subscribe(service_id):
        _offer(q, {"channel": "sync-status", "data": payload})


async def _cron_runs_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str) -> None:
    async for payload in cron_runs_publisher.subscribe(service_id):
        _offer(q, {"channel": "cron-runs", "data": payload})


async def _system_metrics_feeder(q: asyncio.Queue[dict[str, Any]], service_id: str | None) -> None:
    initial = await _resolve_metrics_sampler()(service_id)
    _offer(q, {"channel": "system-metrics", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_METRICS_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await _resolve_metrics_sampler()(service_id)
        except Exception:
            logger.exception("system-metrics sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "system-metrics", "data": payload})
            last_payload = payload


async def _celery_status_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    last_payload = None
    try:
        last_payload = await celery_status_sampler.sample_celery_status_cached()
        _offer(q, {"channel": "celery-status", "data": last_payload})
    except Exception:
        logger.exception("celery-status initial sample failed; will retry next tick")
    while True:
        await asyncio.sleep(_CELERY_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await celery_status_sampler.sample_celery_status_cached()
        except Exception:
            logger.exception("celery-status sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "celery-status", "data": payload})
            last_payload = payload


async def _share_feeder(q: asyncio.Queue[dict[str, Any]]) -> None:
    from backend.utils.tunnel import build_share_live_payload

    initial = build_share_live_payload()
    _offer(q, {"channel": "share", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_SHARE_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = build_share_live_payload()
        except Exception:
            logger.exception("share sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "share", "data": payload})
            last_payload = payload


@router.get("/admin/events/stream")
async def admin_events_stream(
    request: Request,
    channels: str = Query(..., description="Comma-separated admin channels to multiplex."),
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Multiplex the requested admin channels onto one SSE connection."""
    requested = [c.strip() for c in channels.split(",") if c.strip()]
    if not requested:
        raise HTTPException(status_code=422, detail=make_error("channels_required"))
    unknown = sorted(c for c in requested if c not in _ADMIN_EVENT_CHANNELS)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=make_error("unknown_channel", f"unknown channel(s): {', '.join(unknown)}"),
        )
    wanted = set(requested)

    async def stream() -> AsyncIterator[str]:
        out_q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_FANIN_MAXSIZE)
        feeders: list[asyncio.Task[Any]] = []
        if "system-metrics" in wanted:
            feeders.append(asyncio.ensure_future(_system_metrics_feeder(out_q, service_id)))
        if "share" in wanted:
            feeders.append(asyncio.ensure_future(_share_feeder(out_q)))
        if "celery-status" in wanted:
            feeders.append(asyncio.ensure_future(_celery_status_feeder(out_q)))
        if service_id:
            if "sync-status" in wanted:
                feeders.append(asyncio.ensure_future(_sync_status_feeder(out_q, service_id)))
            if "cron-runs" in wanted:
                feeders.append(asyncio.ensure_future(_cron_runs_feeder(out_q, service_id)))

        async def _q_iter() -> AsyncIterator[dict[str, Any]]:
            while True:
                yield await out_q.get()

        try:
            async for env in iter_with_disconnect_ping(_q_iter(), request, ping_seconds=15):
                yield json.dumps(env)
        finally:
            for t in feeders:
                t.cancel()
            await asyncio.gather(*feeders, return_exceptions=True)

    return cast(EventSourceResponse, sse_response(stream()))
