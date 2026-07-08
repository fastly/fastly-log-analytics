"""In-process pub/sub for real-time metrics ticks.

The RealtimePoller publishes transformed rt.fastly.com payloads here;
the SSE endpoint ``GET /api/services/{id}/realtime-stream`` subscribes.
Same fan-out primitive as SyncStatusPublisher / CronRunsPublisher.
"""

from __future__ import annotations

from backend._in_process_publisher import _InProcessPublisher


class RealtimeMetricsPublisher(_InProcessPublisher):
    pass


publisher = RealtimeMetricsPublisher()
