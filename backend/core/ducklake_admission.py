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


def _local_lock(service_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(service_id, threading.RLock())


def _advisory_key(service_id: str) -> int:
    # PostgreSQL advisory locks take signed BIGINT values.
    value = int.from_bytes(hashlib.sha256(service_id.encode()).digest()[:8], "big", signed=False)
    return value - (1 << 64) if value >= (1 << 63) else value


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
    if not lock.acquire(timeout=max(0.0, timeout_s)):
        raise DuckLakeAdmissionTimeout(f"DuckLake writer admission timed out for {service_id}")

    pg_con = None
    advisory = False
    deadline = time.monotonic() + max(0.0, timeout_s)
    key = _advisory_key(service_id)
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
                raise DuckLakeAdmissionTimeout(f"DuckLake advisory lock timed out for {service_id}")
        yield
    finally:
        if advisory and pg_con is not None:
            try:
                pg_con.execute("SELECT pg_advisory_unlock(?)", (key,))
            except Exception:
                logger.exception("Failed to release DuckLake advisory lock for %s", service_id)
        lock.release()


def _reset_for_tests() -> None:
    with _locks_guard:
        _locks.clear()
