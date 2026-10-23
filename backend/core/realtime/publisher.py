"""Thread-safe pub/sub for real-time metrics ticks.

The poller thread publishes ticks to a ring buffer.  SSE subscribers are
notified via per-subscriber ``asyncio.Event`` objects signalled through
``call_soon_threadsafe``.  Each publish → deliver cycle is a single
event-loop callback, avoiding the compounding latency of repeated
``asyncio.sleep`` polls that plagued the previous design.
"""

from __future__ import annotations

import logging

from backend._in_process_publisher import _InProcessPublisher

logger = logging.getLogger(__name__)

BUFFER_SIZE = 120


class RealtimeMetricsPublisher(_InProcessPublisher):
    """In-process pub/sub hub for real-time Fastly metrics."""

    def __init__(self) -> None:
        super().__init__(replay_size=BUFFER_SIZE)

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict]:
        with self._lock:
            buf = self._replay.get(service_id)
            if not buf:
                return []
            return list(buf)[-count:]


from backend.config import SSE_BACKPLANE

if SSE_BACKPLANE == "valkey":
    from backend.utils.valkey_publisher import ValkeyPublisher

    publisher: ValkeyPublisher | RealtimeMetricsPublisher = ValkeyPublisher("realtime", history_size=BUFFER_SIZE)
else:
    publisher = RealtimeMetricsPublisher()
