"""Thread-safe pub/sub for real-time metrics ticks.

The poller thread publishes ticks to a ring buffer.  SSE subscribers are
notified via per-subscriber ``asyncio.Event`` objects signalled through
``call_soon_threadsafe``.  Each publish → deliver cycle is a single
event-loop callback, avoiding the compounding latency of repeated
``asyncio.sleep`` polls that plagued the previous design.
"""

from __future__ import annotations

import logging
import sys
import types
from typing import Any

from backend._in_process_publisher import _InProcessPublisher
from backend.config import SSE_BACKPLANE

logger = logging.getLogger(__name__)

BUFFER_SIZE = 120


class RealtimeMetricsPublisher(_InProcessPublisher):
    """In-process pub/sub hub for real-time Fastly metrics."""

    publisher: Any = None

    def __init__(self) -> None:
        super().__init__(replay_size=BUFFER_SIZE)

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict]:
        with self._lock:
            buf = self._replay.get(service_id)
            if not buf:
                return []
            return list(buf)[-count:]


if SSE_BACKPLANE == "valkey":
    from backend.utils.telemetry import ValkeyPublisher

    publisher: ValkeyPublisher | RealtimeMetricsPublisher = ValkeyPublisher("realtime", history_size=BUFFER_SIZE)
    setattr(publisher, "publisher", None)
else:
    publisher = RealtimeMetricsPublisher()


class _PublisherShimModule(types.ModuleType):
    def __getattr__(self, name: str):
        if name == "publisher":
            override = publisher.__dict__.get("publisher")
            return override if override is not None else publisher
        return getattr(sys.modules[__name__], name)


sys.modules[f"{__name__}.publisher"] = _PublisherShimModule(f"{__name__}.publisher")


def __getattr__(name: str) -> Any:
    if name in ("poller", "manager"):
        import importlib

        mod = importlib.import_module("backend.core.realtime.transform")
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
