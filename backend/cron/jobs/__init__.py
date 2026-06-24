"""Per-concern cron job implementations.

Each module under this package exposes one or more ``@cron_task``-decorated
functions. The :class:`backend.cron.scheduler.Scheduler` registers them with
APScheduler at startup; the shim at ``backend/scheduler.py`` re-exports them
so historical ``from backend.scheduler import _run_*`` callers keep working.
"""

from __future__ import annotations
