"""In-process pub/sub for cron-run state-change events.

Cron lifecycle hooks (``start_cron_run`` and ``log_cron_run`` in
``backend/core/metadata/cron_log.py``) call ``publisher.publish()`` with a
tiny tickle payload describing the run that changed; the SSE endpoint
``GET /api/cron-runs/stream`` iterates ``publisher.subscribe()`` to forward
those events to connected admin browsers. Consumers (``useCronRunsStream``
in the frontend) react by invalidating React Query keys — the table on
``/logs`` and the "Last Sync" header badge refetch their own data with the
user's current filter / pagination instead of merging a single row into a
cached list.

Direct sibling of ``backend/sync_status_publisher.py``. Two reasons it isn't
just one publisher object:

- Semantic intent: badge payloads are state snapshots (last-write-wins is
  correct); cron events are distinct lifecycle notifications. The bounded
  queue mechanism is shared (maxsize=4) but keeping the channels separate
  prevents future divergence from leaking across.
- Isolation: a bug in one channel cannot stall the other.

Mechanism + thread-safety live in :class:`backend._in_process_publisher`.
"""

from __future__ import annotations

from backend._in_process_publisher import _InProcessPublisher


class CronRunsPublisher(_InProcessPublisher):
    def __init__(self) -> None:
        # Cron events are distinct lifecycle notifications. The React cache
        # is seeded on mount, so we only need to stream live/future events;
        # replaying stale events would trigger a storm of redundant query refetches.
        super().__init__(replay_size=0)


publisher = CronRunsPublisher()
