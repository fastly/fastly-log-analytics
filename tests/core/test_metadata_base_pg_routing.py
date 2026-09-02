"""metadata.base routes to the Postgres backend when METADATA_DSN is set.

Unit-level: mocks backend.core.metadata.pg_connection's functions so no
real Postgres pool is touched. Verifies the routing decision, not the
Postgres wire behavior itself (see test_pg_connection.py for that).
"""

from unittest.mock import MagicMock, patch

import pytest

from backend.core.metadata import base
from backend.core.metadata.base import InvalidServiceIdError


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


def test_get_con_uses_sqlite_pool_when_dsn_unset(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with patch.object(base.pg_connection, "get_pg_thread_connection") as mock_get:
        base.get_con("svc-1")
    mock_get.assert_not_called()


def test_get_con_readonly_routes_to_postgres(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_readonly_connection", return_value=sentinel) as mock_get:
        result = base.get_con_readonly("svc-1")
    assert result is sentinel
    mock_get.assert_called_once_with()


def test_close_all_connections_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "close_all_pg_connections") as mock_close:
        with patch.object(base._pool, "close_all") as mock_sqlite_close:
            base.close_all_connections()
    mock_close.assert_called_once()
    mock_sqlite_close.assert_not_called()


def test_close_all_connections_uses_sqlite_when_dsn_unset(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with patch.object(base.pg_connection, "close_all_pg_connections") as mock_close:
        with patch.object(base._pool, "close_all") as mock_sqlite_close:
            base.close_all_connections()
    mock_close.assert_not_called()
    mock_sqlite_close.assert_called_once()


def test_teardown_is_noop_under_postgres(postgres_mode):
    """No per-service file exists under Postgres — teardown must not touch
    the SQLite pool's teardown path (which would try to delete a file that
    was never created)."""
    with patch.object(base._pool, "teardown") as mock_teardown:
        base.teardown("svc-1")
    mock_teardown.assert_not_called()


def test_teardown_deletes_sqlite_file_when_dsn_unset(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with patch.object(base._pool, "teardown") as mock_teardown:
        base.teardown("svc-1")
    mock_teardown.assert_called_once_with("svc-1")
