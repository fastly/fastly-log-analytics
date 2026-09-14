"""Bounded per-service admission for DuckLake writers."""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S = float(os.getenv("DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S", "30"))
_POLL_S = 0.1


class DuckLakeAdmissionTimeout(TimeoutError):
    """A DuckLake writer could not acquire the service admission in time."""


_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()
_stats: dict[str, dict[str, float]] = {}
_stats_guard = threading.Lock()


def _local_lock(service_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(service_id, threading.RLock())


def _advisory_key(service_id: str) -> int:
    # PostgreSQL advisory locks take signed BIGINT values.
    value = int.from_bytes(hashlib.sha256(service_id.encode()).digest()[:8], "big", signed=False)
    return value - (1 << 64) if value >= (1 << 63) else value


def _record_stat(service_id: str, key: str, value: float = 1.0) -> None:
    with _stats_guard:
        bucket = _stats.setdefault(
            service_id,
            {"acquisitions": 0.0, "timeouts": 0.0, "wait_ms_total": 0.0, "hold_ms_total": 0.0},
        )
        bucket[key] += value


def get_admission_stats() -> dict[str, dict[str, float]]:
    """Return cumulative writer-admission counters for diagnostics."""
    with _stats_guard:
        return {service_id: dict(values) for service_id, values in _stats.items()}


@contextmanager
def ducklake_write_admission(
    service_id: str,
    *,
    timeout_s: float = DUCKLAKE_WRITE_ADMISSION_TIMEOUT_S,
) -> Iterator[None]:
    """Serialize one service's DuckLake writer across threads and processes.

    SQLite/local sync uses only the process-local reentrant lock. When metadata
    is Postgres-backed, the same service also takes a PostgreSQL advisory lock,
    fencing writers in separate worker processes/pods.
    """
    lock = _local_lock(service_id)
    wait_started = time.monotonic()
    if not lock.acquire(timeout=max(0.0, timeout_s)):
        _record_stat(service_id, "timeouts")
        raise DuckLakeAdmissionTimeout(f"DuckLake writer admission timed out for {service_id}")
    _record_stat(service_id, "wait_ms_total", (time.monotonic() - wait_started) * 1000.0)

    pg_con = None
    advisory = False
    deadline = time.monotonic() + max(0.0, timeout_s)
    key = _advisory_key(service_id)
    hold_started = time.monotonic()
    try:
        from backend.core.metadata.pg_connection import get_pg_thread_connection, is_postgres

        if is_postgres():
            pg_con = get_pg_thread_connection()
            assert pg_con is not None
            while time.monotonic() <= deadline:
                if pg_con.execute("SELECT pg_try_advisory_lock(?)", (key,)).fetchone()[0]:
                    advisory = True
                    break
                time.sleep(_POLL_S)
            if not advisory:
                _record_stat(service_id, "timeouts")
                raise DuckLakeAdmissionTimeout(f"DuckLake advisory lock timed out for {service_id}")
        yield
    finally:
        if advisory and pg_con is not None:
            try:
                pg_con.execute("SELECT pg_advisory_unlock(?)", (key,))
            except Exception:
                logger.exception("Failed to release DuckLake advisory lock for %s", service_id)
        lock.release()
        _record_stat(service_id, "acquisitions")
        _record_stat(service_id, "hold_ms_total", (time.monotonic() - hold_started) * 1000.0)


def _reset_for_tests() -> None:
    with _locks_guard:
        _locks.clear()
    with _stats_guard:
        _stats.clear()
