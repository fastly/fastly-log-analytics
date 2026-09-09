"""Thread-safe pub/sub for real-time metrics ticks.

The poller thread publishes ticks to a ring buffer.  SSE subscribers are
notified via per-subscriber ``asyncio.Event`` objects signalled through
``call_soon_threadsafe``.  Each publish → deliver cycle is a single
event-loop callback, avoiding the compounding latency of repeated
``asyncio.sleep`` polls that plagued the previous design.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict, deque
from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

BUFFER_SIZE = 120


class RealtimeMetricsPublisher:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buffers: dict[str, deque[tuple[int, dict]]] = defaultdict(lambda: deque(maxlen=BUFFER_SIZE))
        self._cursors: dict[str, int] = defaultdict(int)
        self._subscriber_counts: dict[str, int] = defaultdict(int)
        self._notify_events: dict[str, list[asyncio.Event]] = defaultdict(list)
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:  # type: ignore[override]
        self._loop = loop

    def publish(self, service_id: str, payload: dict) -> None:
        with self._lock:
            seq = self._cursors[service_id]
            self._buffers[service_id].append((seq, payload))
            self._cursors[service_id] = seq + 1
            events = list(self._notify_events.get(service_id, []))

        loop = self._loop
        if loop is not None and events:
            for event in events:
                try:
                    loop.call_soon_threadsafe(event.set)
                except RuntimeError:
                    pass

    def subscribe(self, service_id: str) -> AsyncIterator[dict]:
        notify = asyncio.Event()

        with self._lock:
            self._subscriber_counts[service_id] += 1
            self._notify_events[service_id].append(notify)
            buf = self._buffers[service_id]
            if buf:
                start_seq = buf[0][0]
                snapshot = list(buf)
            else:
                start_seq = self._cursors[service_id]
                snapshot = []

        async def _generator() -> AsyncIterator[dict]:
            cursor = start_seq
            for seq, payload in snapshot:
                yield payload
                cursor = seq + 1

            try:
                while True:
                    notify.clear()
                    with self._lock:
                        buf = self._buffers[service_id]
                        new_items = [(s, p) for s, p in buf if s >= cursor]

                    if new_items:
                        for seq, payload in new_items:
                            yield payload
                            cursor = seq + 1
                    else:
                        try:
                            await asyncio.wait_for(notify.wait(), timeout=2.0)
                        except TimeoutError:
                            pass
            finally:
                with self._lock:
                    self._subscriber_counts[service_id] -= 1
                    try:
                        self._notify_events[service_id].remove(notify)
                    except ValueError:
                        pass

        return _generator()

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict]:
        with self._lock:
            buf = self._buffers.get(service_id)
            if not buf:
                return []
            items = list(buf)
            items = items[-count:]
            return [payload for _, payload in items]

    def subscriber_count(self, service_id: str) -> int:
        with self._lock:
            return self._subscriber_counts.get(service_id, 0)


from backend.config import SSE_BACKPLANE

if SSE_BACKPLANE == "valkey":
    from backend.utils.valkey_publisher import ValkeyPublisher

    publisher: ValkeyPublisher | RealtimeMetricsPublisher = ValkeyPublisher("realtime", history_size=BUFFER_SIZE)
else:
    publisher = RealtimeMetricsPublisher()
