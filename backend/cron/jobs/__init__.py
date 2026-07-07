"""Per-concern cron job implementations.

Each module under this package exposes one or more ``@cron_task``-decorated
functions. The :class:`backend.cron.scheduler.Scheduler` registers them with
APScheduler at startup; callers import the ``_run_*`` bodies directly from
their home module (e.g. ``from backend.cron.jobs.metadata import
_run_metadata_sync``).
"""

from __future__ import annotations
