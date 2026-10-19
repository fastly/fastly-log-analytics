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


def test_get_global_share_con_routes_to_postgres_thread_connection():
    sentinel = MagicMock()
    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as mock_pg:
        result = share_db_connection.get_global_share_con()
    assert result is sentinel
    mock_pg.assert_called_once_with()
