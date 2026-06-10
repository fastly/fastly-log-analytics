"""In-process APScheduler package.

Split out of the old monolithic ``backend/scheduler.py`` so each cron job
type lives in its own module under :mod:`backend.cron.jobs`. The public
surface is preserved through the thin shim at ``backend/scheduler.py``
which re-exports every symbol callers historically imported.
"""

from __future__ import annotations
