"""Tests for the iceberg.execute_with_stale_view_retry helper.

The helper exists so background-job code paths (rdns_cache discovery,
rollups DESCRIBE) — which open raw DuckDB connections instead of going
through QueryRunner — can recover from the same buffer-deletion race
that QueryRunner.execute already handles. Production incident
2026-06-10 (~8 hours of 100%-failing rdns discovery runs spamming
the log) is the regression these tests pin against.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.core.iceberg import execute_with_stale_view_retry, is_stale_view_error


class TestIsStaleViewError:
    @pytest.mark.parametrize(
        "msg",
        [
            'IO Error: No files found that match the pattern "cache/foo/batch_abc.parquet"',
            "Catalog Error: Table with name logs_xyz does not exist",
            "No such file or directory: /tmp/buf.parquet",
            # DuckLake's brief DETACH window during commit/optimize: a
            # reader mid-checkout hits this exact CatalogException text.
            'Catalog Error: Table with name "lake.logs" does not exist because schema "lake" does not exist.',
        ],
    )
    def test_recognises_known_messages(self, msg: str) -> None:
        assert is_stale_view_error(Exception(msg)) is True

    def test_returns_false_for_unrelated_errors(self) -> None:
        assert is_stale_view_error(Exception("Syntax error at line 1")) is False
        assert is_stale_view_error(Exception("Permission denied")) is False
        assert is_stale_view_error(Exception("Connection refused")) is False

    def test_recognises_real_duckdb_exception_from_detached_ducklake_catalog(self) -> None:
        """Not just a string fixture — a REAL DuckDB error from the exact
        DuckLake DETACH-then-query race (commit_batch/optimize briefly
        detach 'lake' on a connection other readers share), confirming the
        matcher isn't accidentally coupled to a specific error phrasing
        that a future DuckDB version could change."""
        import duckdb as _duckdb

        con = _duckdb.connect()
        con.execute("ATTACH ':memory:' AS lake")
        con.execute("CREATE TABLE lake.logs (x INT)")
        con.execute("DETACH lake")
        try:
            con.execute("SELECT * FROM lake.logs")
        except Exception as e:
            assert is_stale_view_error(e) is True
        else:
            pytest.fail("expected querying a detached catalog to raise")


class TestExecuteWithStaleViewRetry:
    def test_passthrough_when_fn_succeeds(self) -> None:
        """No retry / no cache bust when the first call succeeds."""
        con = MagicMock()
        src = {"name": "svc-a"}
        fn = MagicMock(return_value="ok")

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        fn.assert_called_once_with(con)
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_retries_once_after_stale_view_error(self) -> None:
        """First call raises a stale-view error → bust caches + force rebind + retry."""
        con = MagicMock()
        src = {"name": "svc-b"}
        fn = MagicMock(
            side_effect=[
                Exception('IO Error: No files found that match the pattern "buffer/batch_x.parquet"'),
                ["row1", "row2"],
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == ["row1", "row2"]
        assert fn.call_count == 2
        # Both calls receive the same con — i.e. retry isn't allocating a new connection.
        for call in fn.call_args_list:
            assert call.args[0] is con
        mock_clear.assert_called_once_with("svc-b", keep_snapshot_cache=True)
        mock_update.assert_called_once()
        # update_iceberg_view called with (con, src, force=True)
        upd_call = mock_update.call_args
        assert upd_call.args[0] is con
        assert upd_call.args[1] is src
        assert upd_call.kwargs.get("force") is True

    def test_non_stale_error_propagates_without_retry(self) -> None:
        """A non-stale error must surface immediately — don't waste a rebind on a real failure."""
        con = MagicMock()
        src = {"name": "svc-c"}
        fn = MagicMock(side_effect=ValueError("bad SQL"))

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
        ):
            with pytest.raises(ValueError, match="bad SQL"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 1
        mock_clear.assert_not_called()
        mock_update.assert_not_called()

    def test_second_attempt_failure_propagates(self) -> None:
        """If the retry itself fails, propagate — caller chooses fallback (log + skip vs raise).

        Pins the contract so a tempting "also swallow on retry failure" change
        wouldn't slip past silently — the caller-side `except duckdb.Error`
        block stays in charge of the user-visible behaviour.
        """
        con = MagicMock()
        src = {"name": "svc-d"}
        fn = MagicMock(
            side_effect=[
                Exception("No files found"),
                RuntimeError("still broken"),
            ]
        )

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            with pytest.raises(RuntimeError, match="still broken"):
                execute_with_stale_view_retry(con, src, fn)

        assert fn.call_count == 2

    def test_passes_through_extra_args_and_kwargs(self) -> None:
        """The helper must forward *args and **kwargs to ``fn``."""
        con = MagicMock()
        src = {"name": "svc-e"}
        fn = MagicMock(return_value=42)

        with (
            patch("backend.core.iceberg._core.clear_source_caches"),
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn, "a", 1, key="value")

        assert result == 42
        fn.assert_called_once_with(con, "a", 1, key="value")

    def test_default_source_key_when_name_missing(self) -> None:
        """``src`` without a ``name`` key falls back to ``"default"`` for the
        cache-bust call — never raises KeyError on the retry path."""
        con = MagicMock()
        src: dict = {}
        fn = MagicMock(side_effect=[Exception("No files found"), "ok"])

        with (
            patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
            patch("backend.core.iceberg._core.update_iceberg_view"),
        ):
            result = execute_with_stale_view_retry(con, src, fn)

        assert result == "ok"
        mock_clear.assert_called_once_with("default", keep_snapshot_cache=True)

    def test_load_table_cached_heals_missing_metadata_file(self, tmp_path) -> None:
        """When _load_table_cached receives a FileNotFoundError, it removes the table from the local SQL catalog."""
        import sqlite3

        from pyiceberg.exceptions import NoSuchTableError

        from backend.core.iceberg._core import _load_table_cached

        src = {"name": "svc-heal"}
        identifier = ("default", "logs")

        # Set up a mock sqlite database simulating the catalog db path
        cat_db = tmp_path / "iceberg_catalog.db"
        with sqlite3.connect(cat_db) as conn:
            conn.execute("CREATE TABLE iceberg_tables (table_namespace TEXT, table_name TEXT, metadata_location TEXT)")
            conn.execute("INSERT INTO iceberg_tables VALUES ('default', 'logs', 's3://some/stale.json')")
            conn.commit()

        # Mock load_table to raise FileNotFoundError
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = FileNotFoundError("[Errno 2] No such file or directory")

        with (
            patch("backend.core.iceberg._core._catalog_db_path", return_value=str(cat_db)),
            patch("backend.core.iceberg._core._read_metadata_pointer", return_value=None),
            patch("backend.core.iceberg._core._get_cached_table", return_value=None),
            patch("backend.core.iceberg._core._invalidate_cached_table") as mock_invalidate,
        ):
            with pytest.raises(NoSuchTableError, match="metadata is missing from S3"):
                _load_table_cached(src, identifier, mock_catalog)

        # Verify entry was deleted from local SQLite catalog
        with sqlite3.connect(cat_db) as conn:
            rows = conn.execute("SELECT * FROM iceberg_tables").fetchall()
            assert len(rows) == 0

        mock_invalidate.assert_called_once_with(src, identifier)
