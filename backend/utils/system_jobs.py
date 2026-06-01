"""Lightweight in-memory status store for global (non-service) scheduler jobs."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

_lock = threading.Lock()
_status: dict[str, dict] = {}


def record_job_run(job_id: str, status: str, duration_s: float, detail: str = "") -> None:
    """Record the outcome of a completed global job run."""
    with _lock:
        _status[job_id] = {
            "last_run_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": status,
            "duration_s": round(duration_s, 2),
            "detail": detail,
        }


def get_system_job_status() -> dict[str, dict]:
    """Return a snapshot of all recorded job statuses."""
    with _lock:
        return dict(_status)
