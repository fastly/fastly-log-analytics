"""In-process APScheduler package.

Split out of the old monolithic ``backend/scheduler.py`` so each cron job
type lives in its own module under :mod:`backend.cron.jobs`. The lifecycle
(``get_scheduler`` / ``Scheduler``) lives in :mod:`backend.cron.scheduler`;
the ``@cron_task`` decorator in :mod:`backend.cron.decorators`.
"""

from __future__ import annotations
