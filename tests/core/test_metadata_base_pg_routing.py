"""metadata.base routes to the Postgres backend when METADATA_DSN is set.

Unit-level: mocks backend.core.metadata.pg_connection's functions so no
real Postgres pool is touched. Verifies the routing decision, not the
Postgres wire behavior itself (see test_pg_connection.py for that).
"""

from unittest.mock import MagicMock, patch

import pytest

from backend.core.metadata import base, ingest_log
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

