"""Lease keep-alive for one cron invocation, as a context manager.

``CronHeartbeat`` owns the heartbeat thread's whole lifecycle: it starts the thread on enter and,
however the block exits, sets the stop event and joins on exit. The thread is a fresh
``threading.Thread`` per invocation (never a pooled worker), so it also hands its
``get_con()`` connection back to the bounded Postgres metadata pool before it ends; a connection
left pinned per tick exhausts the pool and every later metadata call raises ``PoolTimeout``.
"""

from __future__ import annotations

import logging
import threading
import time
from types import TracebackType

logger = logging.getLogger("backend.scheduler")

DEFAULT_INTERVAL_S = 10.0
_JOIN_TIMEOUT_S = 1.0


class CronHeartbeat:
    """Refresh ``job_runs.heartbeat_at`` for ``(service_id, job_name)`` while the block runs.

    Only the job's own lease is refreshed: an unscoped refresh would keep every running lease for
    the service alive, so a wedged job whose own heartbeat died would never be reaped while any
    healthy job kept ticking (the orphaned-sync-row ingestion stall). With no ``job_name`` the
    loop refreshes nothing.
    """

    def __init__(self, service_id: str, job_name: str | None, *, interval_s: float | None = None) -> None:
        self._service_id = service_id
        self._job_name = job_name
        self._interval_s = DEFAULT_INTERVAL_S if interval_s is None else interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"cron-heartbeat-{service_id}")

    def __enter__(self) -> CronHeartbeat:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._thread.join(timeout=_JOIN_TIMEOUT_S)

    def _run(self) -> None:
        from backend.core.metadata.base import get_con, release_thread_connection

        try:
            while not self._stop.wait(self._interval_s):
                try:
                    con = get_con(self._service_id)
                    if self._job_name:
                        con.execute(
                            "UPDATE job_runs SET heartbeat_at = ? "
                            "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                            (time.time(), self._service_id, self._job_name),
                        )
                    con.commit()
                except Exception:
                    logger.debug("cron heartbeat refresh failed for %s", self._service_id, exc_info=True)
        finally:
            release_thread_connection()
