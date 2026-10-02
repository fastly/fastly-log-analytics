"""Per-service DuckLake writer admission tests."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from backend.core.ducklake_admission import (
    DuckLakeAdmissionTimeout,
    _reset_for_tests,
    ducklake_write_admission,
    get_admission_stats,
)


@pytest.fixture(autouse=True)
def reset_admission_state():
    _reset_for_tests()
    yield
    _reset_for_tests()


def test_local_admission_serializes_same_service():
    entered = threading.Event()
    release = threading.Event()
    order: list[str] = []

    def first() -> None:
        with ducklake_write_admission("admission-local"):
            order.append("first")
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=first)
    worker.start()
    assert entered.wait(timeout=2)

    with pytest.raises(DuckLakeAdmissionTimeout):
        with ducklake_write_admission("admission-local", timeout_s=0.02):
            pass

    release.set()
    worker.join(timeout=2)
    assert order == ["first"]
    stats = get_admission_stats()["admission-local"]
    assert stats["acquisitions"] == 1
    assert stats["timeouts"] == 1
    assert stats["hold_ms_total"] >= 0


def test_different_services_do_not_share_local_admission():
    entered = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with ducklake_write_admission("admission-service-a"):
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=holder)
    worker.start()
    assert entered.wait(timeout=2)

    started = time.monotonic()
    with ducklake_write_admission("admission-service-b", timeout_s=0.2):
        elapsed = time.monotonic() - started

    release.set()
    worker.join(timeout=2)
    assert elapsed < 0.15


def test_postgres_admission_uses_fair_blocking_lock():
    # Fairness: block in PostgreSQL's FIFO lock-wait queue (pg_advisory_lock),
    # bounded by lock_timeout, instead of busy-polling pg_try_advisory_lock
    # (which has no queue and starves whichever writer keeps losing the race).
    calls: list[tuple[str, object]] = []

    class Cursor:
        def execute(self, sql, params=None):
            calls.append((sql, params))
            return self

        def fetchone(self):
            return (True,)

    fake_con = Cursor()
    with (
        patch("backend.core.metadata.pg_connection.is_postgres", return_value=True),
        patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=fake_con),
    ):
        with ducklake_write_admission("admission-postgres", timeout_s=0.2):
            pass

    sqls = [c[0] for c in calls]
    assert not any("pg_try_advisory_lock" in s for s in sqls)
    # lock_timeout is armed before the blocking acquire and cleared afterward.
    assert any(s.startswith("SET lock_timeout") for s in sqls)
    acquire_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_lock(" in s)
    unlock_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_unlock(" in s)
    assert acquire_idx < unlock_idx
    acquire_key = calls[acquire_idx][1][0]
    unlock_key = calls[unlock_idx][1][0]
    assert acquire_key == unlock_key


def test_postgres_admission_timeout_raises_on_lock_not_available():
    import psycopg

    calls: list[str] = []

    class Cursor:
        def execute(self, sql, params=None):
            calls.append(sql)
            if "pg_advisory_lock(" in sql:
                raise psycopg.errors.LockNotAvailable("canceling statement due to lock timeout")
            return self

        def fetchone(self):
            return (True,)

    fake_con = Cursor()
    with (
        patch("backend.core.metadata.pg_connection.is_postgres", return_value=True),
        patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=fake_con),
    ):
        with pytest.raises(DuckLakeAdmissionTimeout):
            with ducklake_write_admission("admission-pg-timeout", timeout_s=0.2):
                pass

    # lock_timeout is reset even when the acquire times out, and no unlock is
    # issued for a lock that was never held.
    assert any(s == "SET lock_timeout = 0" for s in calls)
    assert not any("pg_advisory_unlock(" in s for s in calls)
    assert get_admission_stats()["admission-pg-timeout"]["timeouts"] == 1


def test_admission_stats_capture_wait_and_hold_time():
    with ducklake_write_admission("admission-stats"):
        time.sleep(0.005)

    stats = get_admission_stats()["admission-stats"]
    assert stats["acquisitions"] == 1
    assert stats["timeouts"] == 0
    assert stats["wait_ms_total"] >= 0
    assert stats["hold_ms_total"] >= 5
