"""Lightweight in-memory status store for global (non-service) scheduler jobs."""

from __future__ import annotations

import threading

from backend.utils.date_utils import iso_z_now

_lock = threading.Lock()
_status: dict[str, dict] = {}


def record_job_run(job_id: str, status: str, duration_s: float, detail: str = "") -> None:
    """Record the outcome of a completed global job run."""
    with _lock:
        _status[job_id] = {
            "last_run_at": iso_z_now(),
            "status": status,
            "duration_s": round(duration_s, 2),
            "detail": detail,
        }


def get_system_job_status() -> dict[str, dict]:
    """Return a snapshot of all recorded job statuses."""
    with _lock:
        return dict(_status)


# R-1: drain the job-status dict between tests so an earlier test's
# record_job_run doesn't bleed into the next test's snapshot read.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.system_jobs._status", _status)
