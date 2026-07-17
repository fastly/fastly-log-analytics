"""Shared in-process pub/sub primitive for SSE fan-out.

Cron ticks (APScheduler worker threads) call ``publish()`` with a payload;
SSE endpoint handlers iterate ``subscribe()`` to receive those payloads and
forward them to connected browsers.

Why in-process: cron + FastAPI run in the same Python process (APScheduler
``BackgroundScheduler`` in ``backend/cron/scheduler.py``), so a process-local
channel is the smallest possible primitive — no Redis, no LISTEN/NOTIFY, no
file watcher.

Semantics: each subscriber gets a bounded queue (maxsize=4). When the queue
is full at publish time, the oldest item is dropped (last-write-wins). For
state-snapshot channels a fresh tick always supersedes the previous; for
distinct-event channels a subscriber draining within ~4 items still sees
every event.

Thread-safety: ``publish()`` is safe to call from any thread; queue inserts
are scheduled onto the asyncio loop via ``loop.call_soon_threadsafe``.
``subscribe()`` must be called from the asyncio loop (FastAPI request handler
context), and ``bind_loop()`` must run first (FastAPI lifespan startup) —
cross-thread ``publish()`` calls before binding are silently dropped.

Subclasses (``SyncStatusPublisher``, ``CronRunsPublisher``) are kept as
distinct types + singletons on purpose: a bug in one channel cannot stall the
other, and their semantic intent differs even though the mechanism is shared.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict, deque
from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

REPLAY_BUFFER_SIZE = 60


class _InProcessPublisher:
    """Per-``service_id`` fan-out of dict payloads to async subscribers."""

    def __init__(self, replay_size: int = REPLAY_BUFFER_SIZE) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()
        self._replay: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=replay_size))

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind the publisher to the FastAPI asyncio loop.

        Must be called from inside a running event loop (typically the
        FastAPI ``lifespan`` startup phase). Cross-thread ``publish()`` calls
        before this binding are silently dropped.
        """
        self._loop = loop

    def publish(self, service_id: str, payload: dict) -> None:
        """Fan ``payload`` out to every subscriber for ``service_id``.

        Safe to call from any thread (APScheduler workers, threads spawned by
        the cron, etc.). Returns immediately; queue inserts happen on the
        asyncio loop.
        """
        loop = self._loop
        if loop is None:
            return
        with self._lock:
            self._replay[service_id].append(payload)
            queues = list(self._subscribers.get(service_id, ()))
        for q in queues:
            try:
                loop.call_soon_threadsafe(self._enqueue, q, payload)
            except RuntimeError:
                return

    @staticmethod
    def _enqueue(q: asyncio.Queue, payload: dict) -> None:
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    async def subscribe(self, service_id: str) -> AsyncIterator[dict]:
        """Async iterator that yields each published payload for ``service_id``
        until the caller stops iterating (e.g. SSE client disconnects → the
        endpoint generator is closed)."""
        q: asyncio.Queue = asyncio.Queue(maxsize=4)
        with self._lock:
            replay = list(self._replay.get(service_id, ()))
            self._subscribers[service_id].add(q)
        try:
            for item in replay:
                yield item
            while True:
                yield await q.get()
        finally:
            with self._lock:
                subs = self._subscribers.get(service_id)
                if subs is not None:
                    subs.discard(q)
                    if not subs:
                        self._subscribers.pop(service_id, None)

    def subscriber_count(self, service_id: str) -> int:
        """Inspection helper for tests / debug surfaces."""
        with self._lock:
            return len(self._subscribers.get(service_id, ()))
