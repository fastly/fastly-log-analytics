"""Atomic in-flight-request counter + cron-defer helper.

The heavy crons (sync / local_compact / optimize) all hit the DuckDB pool
+ FOS in ways that contend with cold-cache API requests. When the
``telemetry_middleware`` is processing one or more API calls, we'd rather
let those finish before letting a sync tick fire and steal the pool slot
+ Fastly bandwidth. The cron schedulers call :func:`should_defer_cron` at
the top of each tick; if the active count is non-zero AND the job hasn't
been deferred for more than ``max_defer_secs``, the tick returns early
and APScheduler requeues it on the next firing.

Starvation safeguard: each (job, service) tracks its first-deferred-at
timestamp. Once the deferral window exceeds ``max_defer_secs`` (default
30 s), the job runs anyway and emits a WARN — a sustained-traffic service
must still get sync ticks eventually or local data falls behind.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_active_count = 0
_defer_started_at: dict[tuple[str, str], float] = {}


def increment_active_requests() -> None:
    global _active_count
    with _lock:
        _active_count += 1


def decrement_active_requests() -> None:
    global _active_count
    with _lock:
        if _active_count > 0:
            _active_count -= 1


def active_request_count() -> int:
    """Snapshot read of the in-flight count. Mostly useful for tests."""
    with _lock:
        return _active_count


def should_defer_cron(job_name: str, service_id: str, *, max_defer_secs: float = 30.0) -> bool:
    """Returns True if the cron tick should be SKIPPED for now (requests
    are in flight and we haven't been deferring for too long). False means
    the cron should proceed.

    Tracks per (job_name, service_id) so concurrent services don't share
    a starvation clock.
    """
    key = (job_name, service_id)
    with _lock:
        if _active_count == 0:
            # Quiet moment — clear any pending defer window so the next
            # contention round starts its own 30 s clock.
            _defer_started_at.pop(key, None)
            return False
        now = time.monotonic()
        started = _defer_started_at.get(key)
        if started is None:
            _defer_started_at[key] = now
            return True
        if (now - started) >= max_defer_secs:
            # Starvation safeguard — let it run, reset the window.
            _defer_started_at.pop(key, None)
            logger.warning(
                "[active-request-gate] %s/%s ran after %.1fs of active-request deferrals — "
                "sustained traffic on this service; consider raising max_defer_secs.",
                job_name,
                service_id,
                now - started,
            )
            return False
        return True


def yield_to_api(*, max_wait_secs: float = 1.0, poll_interval: float = 0.1) -> float:
    """Cooperative yield: if API requests are in flight, sleep briefly to
    let them make progress before the caller resumes.

    Sister to ``should_defer_cron``, which gates the START of a cron tick.
    Once a tick is running, ``should_defer_cron`` no longer fires — but a
    dashboard query that arrives mid-tick still needs CPU. Heavy cron
    paths (per-chunk ingest, post-ingest rollup recompute) call this
    helper at natural boundaries so the API thread can drain before
    the next stage of CPU work starts.

    Bounded by ``max_wait_secs`` (default 1 s) so sustained API traffic
    can't starve sync indefinitely — under load the caller proceeds even
    if requests are still in flight. The starvation safeguard at the
    top-of-tick gate stays the backstop.

    Returns total seconds slept (useful for telemetry and tests).
    """
    if active_request_count() == 0:
        return 0.0
    deadline = time.monotonic() + max_wait_secs
    slept = 0.0
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        slept += poll_interval
        if active_request_count() == 0:
            return slept
    return slept


def _reset_for_tests() -> None:
    """Tests: clear counter + defer windows between cases."""
    global _active_count
    with _lock:
        _active_count = 0
        _defer_started_at.clear()


# R-1: thin adapter so CacheRegistry can drain the bare-int counter
# (which lacks a .clear()) alongside every other module cache. Removes
# the active_requests special case from tests/conftest.py.
class _ActiveRequestsResetAdapter:
    """Exposes ``.clear()`` → ``_reset_for_tests()`` for CacheRegistry."""

    @staticmethod
    def clear() -> None:
        _reset_for_tests()


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.active_requests", _ActiveRequestsResetAdapter())
