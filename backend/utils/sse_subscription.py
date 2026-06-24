"""SSE consumer helper that races publisher events against disconnect detection.

Used by `/api/sync-status/stream`, `/api/cron-runs/stream`, and any other
endpoint that subscribes to a publisher (`backend.sync_status_publisher`,
`backend.cron_runs_publisher`) and yields server-sent events to a FastAPI
``Request``-bound client.

Why this exists: the natural shape ::

    async for payload in publisher.subscribe(service_id):
        if await request.is_disconnected():
            break
        yield json.dumps(payload)

leaves the subscriber parked on the publisher's ``q.get()`` for the full
inter-event interval after the client disconnects — the subscriber stays
registered and accumulates payloads until the EventSourceResponse ping
write eventually fails and tears the generator down. A naive timeout via
``asyncio.wait_for(sub.__anext__(), timeout=ping)`` works once, but
``wait_for`` cancels the underlying coroutine on timeout. For an async
generator iterator that suspended inside ``await q.get()``, that
cancellation propagates out of the generator body and closes it — the
next ``__anext__()`` returns ``StopAsyncIteration`` and the SSE
subscriber is silently terminated after one ping interval of idle.

The fix keeps a single long-lived ``__anext__()`` task alive across
timeouts using ``asyncio.shield`` so ``wait_for``'s timeout cancels the
shield wrapper, not the wrapped task. The publisher's queue stays parked
between disconnect checks; the subscriber survives.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator

from fastapi import Request


async def iter_with_disconnect_ping[T](
    source: AsyncIterable[T],
    request: Request,
    *,
    ping_seconds: float = 15,
) -> AsyncIterator[T]:
    """Yield each payload from ``source`` until the request disconnects.

    Polls ``request.is_disconnected()`` every ``ping_seconds`` regardless
    of whether ``source`` is producing — so idle publishers don't leave
    a disconnected client's subscriber registered. The ``source``
    iterator's ``__anext__()`` task is shielded across timeouts so it
    stays parked on the publisher's queue between disconnect checks.
    """
    aiter = source.__aiter__()
    pending: asyncio.Future[T] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(aiter.__anext__())
            try:
                payload = await asyncio.wait_for(asyncio.shield(pending), timeout=ping_seconds)
            except TimeoutError:
                if await request.is_disconnected():
                    return
                continue
            except StopAsyncIteration:
                return
            pending = None
            if await request.is_disconnected():
                return
            yield payload
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
