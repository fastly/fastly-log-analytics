"""Bundled SSE channel for the admin overview's system-metrics cards.

Replaces seven independent polls (health-snapshot every 10s,
metric-history/batch every 60s, queries/summary every 10s,
slow-queries/count every 10s, log-accounting every 30s,
metadata-storage every 60s, system-jobs every 30s) with a single
EventSourceResponse that pushes the bundled payload only when it
changes.

Per-subscriber sampler loop (not a process-wide publisher): the
sampling cost is dominated by stdlib calls (load avg, meminfo, disk
usage) plus a few cheap SQLite COUNTs, which is fine to run per
connection. Admin count is small (1–2 tabs typically). Keeps the code
shape much simpler than a publisher + binding lifecycle.

Change-only push: a shallow dict-equality compare against the last
emitted payload suppresses redundant frames. If anything jitters (e.g.
load average updates every cycle), we'll push every tick — no worse
than polling, just no better.

Frontend dispatch: the bundled payload is exploded into the same
React Query slice keys the cards already read from
(``['admin', 'health-snapshot']``, etc.) via ``setQueryData`` in the
``useSystemMetricsStream`` hook, so the components themselves don't
change.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from fastapi import Depends, Request
from sse_starlette.sse import EventSourceResponse

from backend.deps import get_service_id
from backend.system_metrics_sampler import sample_system_metrics
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS

from ._router import router

logger = logging.getLogger(__name__)

_SAMPLE_INTERVAL_SECONDS = 10.0


@router.get("/admin/system-metrics/stream")
async def system_metrics_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Push the bundled admin-metrics snapshot when it changes.

    Admin-only via the ``/api/admin/`` prefix gate in
    ``backend/utils/remote_access.py`` — analysts 403 at the
    middleware before reaching this handler, so no extra gating here.

    Serviceless connections are allowed: on a fresh install there is no
    service yet, but the admin still wants live host/process status.
    ``sample_system_metrics(None)`` degrades gracefully — the four
    global slices (health, metric-history, queries-summary, system-jobs)
    populate and the three service-scoped slices (slow-queries-count,
    log-accounting, metadata-storage) come back ``null``, which the
    frontend fan-out already skips. The change-only diff, ping, and
    disconnect handling are identical either way.
    """

    async def stream() -> AsyncIterator[str]:
        # Initial snapshot off-thread so the sampling I/O (stdlib disk
        # syscalls, SQLite COUNTs) doesn't stall the event loop.
        last_payload: dict | None = None
        initial = await asyncio.to_thread(sample_system_metrics, service_id)
        yield json.dumps(initial)
        last_payload = initial

        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(_SAMPLE_INTERVAL_SECONDS)
            try:
                payload = await asyncio.to_thread(sample_system_metrics, service_id)
            except Exception:
                logger.exception("system-metrics sample failed; will retry next tick")
                continue
            if payload != last_payload:
                yield json.dumps(payload)
                last_payload = payload

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)
