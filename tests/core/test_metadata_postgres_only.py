"""Pins the precondition that makes every ``is_postgres()`` SQLite arm dead code.

ADR-22: metadata lives in Postgres only. ``get_con`` / ``get_con_readonly`` (and the
``usage_log_db`` twins) cannot hand out a connection without ``METADATA_DSN``, and
``validate_deployment_mode`` refuses to boot a deployment without a Postgres DSN. So any code
that already holds a metadata connection is on Postgres, and a ``not is_postgres()`` branch
beside it can never run.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from backend import config
from backend.core.metadata import base, ingest_log, pg_connection, usage_log_db
from backend.core.metadata.base import InvalidServiceIdError
from backend.core.share_db import connection as share_db_connection


def _in_fresh_thread(fn):
    """Run ``fn`` on a thread with no cached thread-local connection; return its exception."""
    box: dict[str, BaseException | None] = {"exc": None}

    def run():
        try:
            fn()
        except BaseException as e:  # noqa: BLE001 — the exception is the assertion target
            box["exc"] = e

    t = threading.Thread(target=run)
    t.start()
    t.join()
    return box["exc"]


@pytest.fixture
def no_dsn(monkeypatch):
    pg_connection.reset_pg_pool_for_tests()
    monkeypatch.delenv("METADATA_DSN", raising=False)
    yield
    pg_connection.reset_pg_pool_for_tests()


@pytest.mark.parametrize(
    "opener",
    [base.get_con, base.get_con_readonly, usage_log_db.get_con, usage_log_db.open_readonly],
    ids=["base.get_con", "base.get_con_readonly", "usage_log_db.get_con", "usage_log_db.open_readonly"],
)
def test_connection_openers_raise_without_dsn(no_dsn, opener):
    exc = _in_fresh_thread(lambda: opener("svc-1"))
    assert isinstance(exc, RuntimeError)
    assert "METADATA_DSN is not set" in str(exc)


def test_deployment_validation_refuses_a_non_postgres_metadata_dsn(monkeypatch):
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", "postgresql://u:p@h/db")
    monkeypatch.setenv("METADATA_DSN", "sqlite:///tmp/x.db")
    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        config.validate_deployment_mode()


def test_usage_log_ddl_still_comes_from_usage_log_db_schema():
    from backend.core.metadata import pg_schema

    statements = pg_schema.pg_schema_statements()
    joined = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS usage_log (" in joined
    assert "CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (" in joined
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    assert "AUTOINCREMENT" not in joined


@pytest.fixture
def postgres_mode(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://fake/fake")
    yield


def test_get_con_routes_to_postgres_when_dsn_set(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=sentinel) as mock_get:
        result = base.get_con("svc-1")
    assert result is sentinel
    mock_get.assert_called_once_with()


def test_get_con_still_validates_service_id_under_postgres(postgres_mode):
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=MagicMock()):
        with pytest.raises(InvalidServiceIdError):
            base.get_con("not a valid id / has slash")


def test_latest_ingest_timestamp_uses_postgres_compatible_aggregate(postgres_mode):
    cursor = MagicMock()
    cursor.fetchone.return_value = {"latest": "2026-09-08T21:34:32Z"}
    connection = MagicMock()

    def execute(sql, params):
        if "datetime(" in sql:
            raise AssertionError("SQLite datetime() is not valid PostgreSQL")
        return cursor

    connection.execute.side_effect = execute
    with patch.object(ingest_log, "get_con", return_value=connection):
        result = ingest_log.get_latest_ingest_ts("svc-1")

    assert result == "2026-09-08T21:34:32Z"


def test_get_con_readonly_routes_to_postgres(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_readonly_connection", return_value=sentinel) as mock_get:
        result = base.get_con_readonly("svc-1")
    assert result is sentinel
    mock_get.assert_called_once_with()


def test_release_thread_connection_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "release_pg_thread_connection") as mock_release:
        base.release_thread_connection()
    mock_release.assert_called_once_with()


def test_close_all_connections_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "close_all_pg_connections") as mock_close:
        base.close_all_connections()
    mock_close.assert_called_once()


def test_teardown_is_noop_under_postgres(postgres_mode):
    """No per-service file exists under Postgres — teardown clears the cache without error."""
    base.teardown("svc-1")


def test_get_global_share_con_routes_to_postgres_thread_connection():
    sentinel = MagicMock()
    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as mock_pg:
        result = share_db_connection.get_global_share_con()
    assert result is sentinel
    mock_pg.assert_called_once_with()


def test_stale_thread_wrapper_is_replaced_after_close_all_from_another_thread():
    first_ready = threading.Event()
    closed = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        results["first"] = pg_connection.get_pg_thread_connection()
        first_ready.set()
        closed.wait(10)
        second = pg_connection.get_pg_thread_connection()
        results["second"] = second
        results["value"] = second.execute("SELECT 1").fetchone()[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert first_ready.wait(10)
    pg_connection.close_all_pg_connections()
    closed.set()
    t.join(15)

    assert results["second"] is not results["first"]
    assert results["value"] == 1


def test_close_all_leaves_a_connection_with_a_query_in_flight_to_its_owner():
    """Returning a busy connection from another thread freed it under the running query (worker segfault)."""
    started = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        con = pg_connection.get_pg_thread_connection()
        results["wrapper"] = con
        started.set()
        results["value"] = con.execute("SELECT 1 FROM pg_sleep(1)").fetchone()[0]
        results["returned_after_query"] = con._returned[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert started.wait(10)
    wrapper = results["wrapper"]
    assert isinstance(wrapper, pg_connection.PgConnectionWrapper)
    for _ in range(500):
        if not wrapper._busy.acquire(blocking=False):
            break
        wrapper._busy.release()
        time.sleep(0.01)
    else:
        raise AssertionError("worker never started its query")
    pg_connection.close_all_pg_connections()
    t.join(15)

    assert results["value"] == 1
    assert results["returned_after_query"] is False


def test_return_to_pool_once_is_atomic_under_contention():
    """Two threads racing to return the same connection must putconn it exactly once."""
    pool = MagicMock()
    returned = [False]
    start = threading.Barrier(8)

    def go() -> None:
        start.wait(5)
        pg_connection._return_to_pool_once(object(), returned, pool)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert pool.putconn.call_count == 1
