"""Multiplexed admin SSE channel.

Collapses the three always-on admin streams that used to mount separately
in the page header — ``/api/sync-status/stream``,
``/api/cron-runs/stream`` and ``/api/admin/system-metrics/stream`` — into
ONE ``EventSourceResponse`` so an admin tab holds a single connection
instead of three.

Why this matters: the admin tunnel (``localhost:3001`` → Next on :3000)
is HTTP/1.1, where browsers cap ~6 connections per origin. Three
long-lived SSE streams alone consumed half the budget, leaving the
bootstrap + every panel fetch queued as "pending". One multiplexed
stream restores the budget and cuts per-admin server load (fewer
connections, fewer SQLite poll loops) so more admins can use the site
concurrently.

Wire format: each frame is a JSON envelope ``{"channel": <name>,
"data": <payload>}``. The frontend ``useAdminEventStream`` hook demuxes
on ``channel`` and dispatches to the same per-channel apply logic the old
single-purpose hooks used (``setQueryData`` / coalesced invalidations),
so downstream consumers are unchanged.

Sources are merged via a bounded fan-in queue fed by independent feeder
tasks — two re-use the existing publishers (``sync_status_publisher``,
``cron_runs_publisher``); ``system-metrics`` polls
``sample_system_metrics_cached`` (a per-connection loop whose sample is
deduped process-wide, so N admin tabs cost one recompute per window not
N); ``share`` polls the in-memory tunnel-manager snapshot. The
drop-oldest ``_offer`` mirrors ``_InProcessPublisher._enqueue`` so one
slow/bursty channel can never stall or starve another's feeder.

Auth: lives under ``/api/admin/`` so RemoteAccessMiddleware
(``_ANALYST_BLOCKED_PREFIXES``) auto-403s analysts — all three channels
are admin-only. ``_is_sse_route`` (path ends ``/stream``) keeps it exempt
from the analyst idle-timer. Analysts keep their own single
``/api/log-extents/stream`` (untouched).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from fastapi import Depends, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.cron_runs_publisher import publisher as cron_runs_publisher
from backend.deps import get_service_id
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.system_metrics_sampler import sample_system_metrics_cached
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error
from backend.utils.sse_subscription import iter_with_disconnect_ping

from ._router import router
from .sync_status import compute_sync_status_cached

logger = logging.getLogger(__name__)

# Channels a single admin connection may request. All admin-only — the
# analyst-safe log-extents projection keeps its own dedicated endpoint.
_ADMIN_EVENT_CHANNELS = frozenset({"sync-status", "cron-runs", "system-metrics", "share"})

_METRICS_SAMPLE_INTERVAL_SECONDS = 10.0
_SHARE_SAMPLE_INTERVAL_SECONDS = 10.0

# Fan-in buffer across all channels. Bounded + drop-oldest so a bursty
# channel can't grow memory without bound; 16 is comfortably above the
# per-channel maxsize=4 publisher queues so cross-channel coalescing only
# kicks in under genuine backpressure.
_FANIN_MAXSIZE = 16


def _offer(q: asyncio.Queue, env: dict) -> None:
    """Non-blocking enqueue with drop-oldest.

    Mirrors ``_InProcessPublisher._enqueue``: a full queue drops its
    oldest frame so the newest always lands, and a feeder never blocks
    waiting on the drain side (which would let one channel starve the
    others or park past an undetected disconnect)."""
    if q.full():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            pass
    try:
        q.put_nowait(env)
    except asyncio.QueueFull:
        pass


async def _sync_status_feeder(q: asyncio.Queue, service_id: str) -> None:
    # Initial cached snapshot so a freshly mounted badge doesn't
    # blank-flash. compute_sync_status_cached touches disk → off-thread.
    initial = await asyncio.to_thread(compute_sync_status_cached, service_id)
    if initial is not None:
        _offer(q, {"channel": "sync-status", "data": initial})
    async for payload in sync_status_publisher.subscribe(service_id):
        _offer(q, {"channel": "sync-status", "data": payload})


async def _cron_runs_feeder(q: asyncio.Queue, service_id: str) -> None:
    # No initial snapshot — the cron-logs React Query cache is seeded
    # from /api/cron-runs on mount; this channel only tickles invalidations.
    async for payload in cron_runs_publisher.subscribe(service_id):
        _offer(q, {"channel": "cron-runs", "data": payload})


async def _system_metrics_feeder(q: asyncio.Queue, service_id: str | None) -> None:
    # Per-connection loop, but the sample itself is deduped process-wide:
    # sample_system_metrics_cached collapses N connections' aligned/overlapping
    # ticks (within the 10s TTL) into ONE real recompute. Accepts a None
    # service_id — the sampler degrades to global slices. last_payload is
    # per-connection so each tab still emits its own change deltas off the
    # (never-mutated) shared snapshot.
    initial = await sample_system_metrics_cached(service_id)
    _offer(q, {"channel": "system-metrics", "data": initial})
    last_payload = initial
    while True:
        await asyncio.sleep(_METRICS_SAMPLE_INTERVAL_SECONDS)
        try:
            payload = await sample_system_metrics_cached(service_id)
        except Exception:
            logger.exception("system-metrics sample failed; will retry next tick")
            continue
        if payload != last_payload:
            _offer(q, {"channel": "system-metrics", "data": payload})
            last_payload = payload


async def _share_feeder(q: asyncio.Queue) -> None:
    # Global-admin (no service scope). The payload is pure in-memory
    # tunnel-manager getters (microseconds), so per-connection sampling is
    # fine — no publisher-binding lifecycle, no to_thread. Sourced from
    # backend.utils.tunnel (not the share router) so import-linter keeps the
    # routers.admin → routers.share_admin edge banned.
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
    """Multiplex the requested admin channels onto one SSE connection.

    ``channels`` is a comma-separated subset of
    ``sync-status,cron-runs,system-metrics,share``. Unknown or empty → 422.

    Service scoping is the union (optionalService): ``system-metrics`` and
    ``share`` are global-admin and always stream (global/serviceless OK);
    ``sync-status`` + ``cron-runs`` are service-scoped and only stream when
    a service is resolved. The client reconnects on service switch
    (``useServiceStream``), so a serviceless connection that later gains a
    service simply re-opens with the service-scoped feeders active.
    """
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
        out_q: asyncio.Queue = asyncio.Queue(maxsize=_FANIN_MAXSIZE)
        feeders: list[asyncio.Task] = []
        # system-metrics + share are global-admin (tolerate a None service);
        # sync-status + cron-runs are service-scoped and skipped until a
        # service is resolved.
        if "system-metrics" in wanted:
            feeders.append(asyncio.ensure_future(_system_metrics_feeder(out_q, service_id)))
        if "share" in wanted:
            feeders.append(asyncio.ensure_future(_share_feeder(out_q)))
        if service_id:
            if "sync-status" in wanted:
                feeders.append(asyncio.ensure_future(_sync_status_feeder(out_q, service_id)))
            if "cron-runs" in wanted:
                feeders.append(asyncio.ensure_future(_cron_runs_feeder(out_q, service_id)))

        async def _q_iter() -> AsyncIterator[dict]:
            while True:
                yield await out_q.get()

        try:
            async for env in iter_with_disconnect_ping(_q_iter(), request, ping_seconds=15):
                yield json.dumps(env)
        finally:
            for t in feeders:
                t.cancel()
            await asyncio.gather(*feeders, return_exceptions=True)

    return EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)
