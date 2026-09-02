"""share_db.connection.get_global_share_con routes to the Postgres backend
when METADATA_DSN is set.

Pins the fix for a previously-broken path: the old code wrapped
``get_pg_pool().connection()`` — a bare contextmanager, not a connection —
in ``PgConnectionWrapper`` directly, which would have raised AttributeError
the first time Postgres mode was actually exercised. No test ever ran that
line, which is how it shipped broken.
"""

from unittest.mock import MagicMock, patch

from backend.core.share_db import connection as share_db_connection


def test_get_global_share_con_uses_sqlite_pool_by_default(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with patch.object(share_db_connection, "get_pg_thread_connection") as mock_pg:
        with patch.object(share_db_connection._pool, "get", return_value=MagicMock()) as mock_sqlite:
            share_db_connection.get_global_share_con()
    mock_pg.assert_not_called()
    mock_sqlite.assert_called_once_with("__global_share__")


def test_get_global_share_con_routes_to_postgres_thread_connection(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://fake/fake")
    sentinel = MagicMock()
    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as mock_pg:
        with patch.object(share_db_connection._pool, "get") as mock_sqlite:
            result = share_db_connection.get_global_share_con()
    assert result is sentinel
    mock_pg.assert_called_once_with()
    mock_sqlite.assert_not_called()
