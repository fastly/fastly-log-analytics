"""Tests for pure helpers in backend/core/duckdb.py."""

from unittest.mock import patch

import pytest

from backend.core.duckdb import DBBusyError, _safe_table_name, format_asn_label, get_connection


class TestFormatAsnLabel:
    def test_name_and_number(self):
        assert format_asn_label(7922, "Comcast Cable Communications") == "Comcast Cable Communications (7922)"

    def test_empty_name_returns_as_prefix(self):
        assert format_asn_label(1234, "") == "AS1234"

    def test_name_already_as_prefixed_returns_as_prefix(self):
        # "AS7922" is a bare ASN alias, not a human name — collapse it.
        assert format_asn_label(7922, "AS7922") == "AS7922"

    def test_name_with_special_chars(self):
        result = format_asn_label(1, "O'Reilly & Associates")
        assert result == "O'Reilly & Associates (1)"

    def test_large_asn(self):
        assert format_asn_label(4294967295, "Max ASN") == "Max ASN (4294967295)"

    def test_asn_zero(self):
        assert format_asn_label(0, "") == "AS0"


class TestSafeTableName:
    def test_simple_name(self):
        assert _safe_table_name("my_service") == "logs_my_service"

    def test_strips_special_chars(self):
        # The impl strips trailing underscores, so trailing ! becomes _ then is stripped.
        assert _safe_table_name("my-service!") == "logs_my_service"

    def test_lowercases(self):
        assert _safe_table_name("MyService") == "logs_myservice"

    def test_default_name(self):
        assert _safe_table_name("default") == "logs"

    def test_leading_trailing_underscores_stripped(self):
        result = _safe_table_name("_my_service_")
        assert not result[len("logs_") :].startswith("_")
        assert not result.endswith("_")

    def test_spaces_become_underscores(self):
        assert _safe_table_name("my service") == "logs_my_service"

    def test_dots_become_underscores(self):
        result = _safe_table_name("service.name.v2")
        assert result == "logs_service_name_v2"


class TestGetConnectionLockHandling:
    """``get_connection`` treats certain DuckDB error messages as transient
    locks and retries; everything else propagates immediately.

    Regression scope: the "different configuration" message (DuckDB raises
    it when a connection already exists with a different ``read_only``
    flag — e.g. a cron is writing while a dashboard request opens
    read-only) must be treated as a lock. Without that mapping, the
    dashboard request 500s instead of waiting for the cron to finish.
    """

    def test_classic_locked_error_retries_then_raises_db_busy(self):
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("database is locked"),
        ):
            with pytest.raises(DBBusyError, match=r"locked by another process.*path=/tmp/_test\.duckdb"):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_lock_timeout_logs_topology_context(self, caplog):
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("database is locked"),
        ):
            with caplog.at_level("ERROR", logger="backend.core.duckdb"):
                with pytest.raises(DBBusyError):
                    get_connection(
                        source={"logging_service_id": "svc-1", "duckdb_path": "/tmp/_test.duckdb"},
                        max_wait=0.1,
                        read_only=True,
                    )

        record = next(r for r in caplog.records if r.getMessage().startswith("duckdb_connection_lock_timeout"))
        assert record.args["service_id"] == "svc-1"
        assert record.args["db_path"] == "/tmp/_test.duckdb"
        assert record.args["read_only"] is True
        assert record.args["ingest_mode"] == "sync"

    def test_conflict_error_retries_then_raises_db_busy(self):
        """``"conflict"`` in the message is the original lock signal —
        pinned alongside the new "different configuration" case so the
        deny-list doesn't regress."""
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("write-write conflict on table"),
        ):
            with pytest.raises(DBBusyError):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_different_configuration_error_treated_as_transient_lock(self):
        """REGRESSION: DuckDB raises ``"Can't open a connection to same
        database file with a different configuration..."`` when an existing
        connection has a different ``read_only`` flag. Before the fix this
        message wasn't in the lock-detection deny-list, so it propagated
        immediately as a 500. Now it retries and converts to DBBusyError
        — the request layer maps DBBusyError → 503 → React Query keeps
        cached data instead of clearing the UI."""
        err_msg = (
            "Connection Error: Can't open a connection to same database file "
            "with a different configuration than existing connections"
        )
        with patch("backend.core.duckdb.duckdb.connect", side_effect=Exception(err_msg)):
            with pytest.raises(DBBusyError):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)

    def test_unexpected_error_propagates_immediately(self):
        """Non-lock errors (perms, missing parent dir, OOM) must propagate
        immediately — pinned because retrying through these wastes max_wait
        seconds and the actual error message gets shadowed by DBBusyError."""
        with patch(
            "backend.core.duckdb.duckdb.connect",
            side_effect=Exception("Permission denied: /var/duckdb"),
        ):
            with pytest.raises(Exception, match="Permission denied"):
                get_connection(source={"duckdb_path": "/tmp/_test.duckdb"}, max_wait=0.1)
