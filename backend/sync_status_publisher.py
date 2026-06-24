"""In-process pub/sub for sync-status updates.

Cron ticks (APScheduler worker threads) call ``publisher.publish()`` with
the latest cached sync-status snapshot; SSE endpoint handlers
(``GET /api/sync-status/stream``) iterate ``publisher.subscribe()`` to
receive those snapshots and forward them to connected browsers.

Sync-status payloads are state snapshots — last-write-wins on a full queue
is correct because a fresh tick always supersedes the previous, and we never
need to deliver an old payload after a newer one is ready.

Mechanism + thread-safety live in :class:`backend._in_process_publisher`.
Kept as its own type + singleton (rather than sharing one publisher with the
cron-runs channel) so a bug in one channel can't stall the other.
"""

from __future__ import annotations

from backend._in_process_publisher import _InProcessPublisher


class SyncStatusPublisher(_InProcessPublisher):
    pass


publisher = SyncStatusPublisher()
