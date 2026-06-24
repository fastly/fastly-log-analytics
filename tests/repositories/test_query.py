"""Tests for backend/repositories/query.py — execute_query and get_presets."""

import duckdb
import pytest

from backend.repositories.query import execute_query, get_presets


class TestLogsTableAlias:
    """execute_query rewrites 'logs' to the real table name when the source name differs."""

    def _con_with_table(self, table_name: str) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE {table_name} (id INTEGER, val VARCHAR)")
        con.execute(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
        return con

    def test_rewrites_logs_to_actual_table_name(self):
        con = self._con_with_table("logs_myservice")
        src = {"name": "myservice"}
        result = execute_query(con, src, "SELECT * FROM logs ORDER BY id", max_rows=100, want_explain=False)
        assert result["row_count"] == 2
        assert result["data"][0]["val"] == "a"

    def test_no_rewrite_when_table_is_already_logs(self):
        con = self._con_with_table("logs")
        src = {"name": "default"}
        result = execute_query(con, src, "SELECT * FROM logs ORDER BY id", max_rows=100, want_explain=False)
        assert result["row_count"] == 2

    def test_no_rewrite_when_src_is_none(self):
        con = self._con_with_table("logs")
        result = execute_query(con, None, "SELECT * FROM logs", max_rows=100, want_explain=False)
        assert result["row_count"] == 2

    def test_rewrite_is_case_insensitive(self):
        con = self._con_with_table("logs_svc")
        src = {"name": "svc"}
        result = execute_query(con, src, "select * from LOGS order by id", max_rows=100, want_explain=False)
        assert result["row_count"] == 2

    def test_logs_in_cte_alias_also_rewritten_consistently(self):
        """WITH logs AS (...) queries still work — the CTE name and references are rewritten together."""
        con = self._con_with_table("logs_svc2")
        src = {"name": "svc2"}
        sql = "WITH logs AS (SELECT id FROM logs WHERE id = 1) SELECT * FROM logs"
        result = execute_query(con, src, sql, max_rows=100, want_explain=False)
        assert result["row_count"] == 1

    def test_logs_word_boundary_not_matched_in_longer_names(self):
        """'my_logs' or 'logs_extra' must not be rewritten by the \\blogs\\b
        regex (the rewriter must respect word boundaries).

        Fix 5 (audit F-8/9/10) layers a per-service allowlist on top —
        the SQL validator now rejects references to tables outside
        ``{"logs", "logs_<active_service_id>"}`` to close cross-tenant
        catalog leakage on /api/query. So a query against ``my_logs``
        with src={"name": "svc3"} is now rejected as a foreign table
        even though the regex correctly leaves the identifier alone.
        The PermissionError shape from execute_query is what the route
        handler translates into HTTP 403.
        """
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc3 (id INTEGER)")
        con.execute("CREATE TABLE my_logs (id INTEGER)")
        con.execute("INSERT INTO logs_svc3 VALUES (1)")
        con.execute("INSERT INTO my_logs VALUES (99)")
        src = {"name": "svc3"}
        with pytest.raises(PermissionError, match="not in the allowed set"):
            execute_query(con, src, "SELECT * FROM my_logs", max_rows=100, want_explain=False)


class TestGetPresets:
    def test_presets_use_actual_table_name(self, in_memory_duckdb, test_service_source):
        table_name = "logs_test_service"
        in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMPTZ, val INTEGER)")
        presets = get_presets(test_service_source, in_memory_duckdb)
        assert presets, "should return at least one preset"
        for p in presets:
            assert "logs_test_service" in p["sql"], f"preset SQL should use real table name: {p['sql']}"
            assert "FROM logs " not in p["sql"] and not p["sql"].endswith("FROM logs"), (
                f"preset SQL must not use bare 'logs': {p['sql']}"
            )

    def test_sample_rows_preset_has_no_order_by(self, in_memory_duckdb, test_service_source):
        """The 'Sample rows' preview must NOT force a full sort. With 1.6M+ rows
        the ORDER BY timestamp DESC pre-fix made the preset take many seconds —
        and worse, the same default text leaked into the analyst's textarea where
        editing ``*`` to ``COUNT(*)`` produced a Binder error."""
        in_memory_duckdb.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, val INTEGER)")
        presets = get_presets(test_service_source, in_memory_duckdb)
        sample = next(p for p in presets if p["name"] == "Sample rows")
        assert "ORDER BY" not in sample["sql"].upper(), (
            f"Sample rows preset must not include ORDER BY (forces a 1.6M-row sort + COUNT()/aggregate footgun): {sample['sql']}"
        )


class TestAutoLimitPushdown:
    """Auto-apply LIMIT max_rows+1 when the query doesn't already have one.

    Without this, ``SELECT * FROM logs ORDER BY timestamp DESC`` materialises
    the entire table before the API truncates — a first-byte 503 timeout on
    the dashboard side. The +1 trick lets us still report ``truncated`` and
    DuckDB's top-k optimiser handles ORDER BY ... LIMIT efficiently.
    """

    def _con(self, rows: int = 50) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER, val VARCHAR)")
        con.executemany("INSERT INTO logs VALUES (?, ?)", [(i, f"v{i}") for i in range(rows)])
        return con

    def test_limit_pushdown_truncates_without_full_materialisation(self):
        con = self._con(rows=50)
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id", max_rows=10, want_explain=False)
        assert result["row_count"] == 10
        assert result["truncated"] is True
        # Total is reported as -1 sentinel (unknown) since we don't pay for COUNT(*).
        assert result["total_rows"] == -1

    def test_no_pushdown_when_query_already_has_limit(self):
        con = self._con(rows=50)
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id LIMIT 5", max_rows=100, want_explain=False)
        assert result["row_count"] == 5
        assert result["truncated"] is False
        assert result["total_rows"] == 5

    def test_non_truncated_select_reports_exact_total(self):
        con = self._con(rows=5)
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id", max_rows=100, want_explain=False)
        assert result["row_count"] == 5
        assert result["truncated"] is False
        assert result["total_rows"] == 5

    def test_summarize_is_rejected_at_validator(self):
        """SUMMARIZE parses as SELECT_NODE with from_table.type == SHOW_REF,
        so it slips past the statement-type whitelist. Fix 5 (audit
        F-8/9/10) closes that bypass — SUMMARIZE / SHOW / DESCRIBE are now
        rejected on the user-query path because they leak foreign service
        table names + schemas via the pooled DuckDB catalog. Admins who
        need catalog introspection should use an admin-only endpoint."""
        con = self._con(rows=10)
        with pytest.raises(PermissionError, match="SHOW / DESCRIBE / SUMMARIZE"):
            execute_query(con, None, "SUMMARIZE logs", max_rows=100, want_explain=False)

    def test_count_star_is_not_wrapped(self):
        """Pure aggregates already return 1 row — wrapping is no-op but
        should still produce the right answer."""
        con = self._con(rows=42)
        result = execute_query(con, None, "SELECT COUNT(*) AS n FROM logs", max_rows=100, want_explain=False)
        assert result["row_count"] == 1
        assert result["data"][0]["n"] == 42
        assert result["truncated"] is False

    def test_limit_pushdown_with_prepended_comment(self):
        """Finding 015: Verify that SQL comments (e.g., /* comment */) prepended to a query
        do not bypass the automatic SELECT statement limit wrapping logic."""
        con = self._con(rows=50)
        sql = "/* This is a comment */ SELECT * FROM logs"
        result = execute_query(con, None, sql, max_rows=10, want_explain=False)
        assert result["row_count"] == 10
        assert result["truncated"] is True


class TestAnalystTimeWindow:
    """H1: ``/api/query`` enforces the analyst's clamped time window.

    The enforcement rebinds the per-service log table to a temp view filtered
    to ``[start, end)`` (the finding's primary recommended fix) rather than
    wrapping the OUTPUT in ``WHERE timestamp BETWEEN`` — the latter cannot
    bound aggregates / ``count(*)`` (no timestamp column in the result) and is
    defeated by aliasing the column away. ``time_filter=None`` is the admin
    path = full retained range.
    """

    # _safe_table("svc") -> "logs_svc"; rows straddle the window below.
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, ?)",
            [
                ("2026-06-17T10:00:00+00:00", "1.2.3.4", "/a"),  # in window
                ("2026-06-17T10:30:00+00:00", "5.6.7.8", "/b"),  # in window
                ("2026-06-01T00:00:00+00:00", "9.9.9.9", "/old"),  # outside window
            ],
        )
        return con

    def test_admin_no_filter_sees_full_range(self):
        con = self._con()
        result = execute_query(con, self.SRC, "SELECT * FROM logs ORDER BY timestamp", max_rows=100, want_explain=False)
        assert result["row_count"] == 3

    def test_analyst_window_filters_out_of_range_rows(self):
        con = self._con()
        result = execute_query(
            con,
            self.SRC,
            "SELECT * FROM logs ORDER BY timestamp",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert result["row_count"] == 2
        assert all("9.9.9.9" not in str(row) for row in result["data"])

    def test_analyst_window_bounds_aggregates(self):
        """The temp-view rebind bounds ``count(*)`` too — an output-level
        ``WHERE timestamp`` wrapper could not (the result has no timestamp
        column) and would raise a Binder error here instead."""
        con = self._con()
        result = execute_query(
            con, self.SRC, "SELECT count(*) AS n FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW
        )
        assert result["data"][0]["n"] == 2

    def test_analyst_cannot_widen_window_via_own_where(self):
        """An analyst WHERE reaching for older rows is ANDed with the window
        (their filter is on the outer query, ours is in the source view) — it
        can narrow but never widen."""
        con = self._con()
        sql = "SELECT * FROM logs WHERE timestamp >= TIMESTAMPTZ '1970-01-01T00:00:00+00:00'"
        result = execute_query(con, self.SRC, sql, max_rows=100, want_explain=False, time_filter=self.WINDOW)
        assert result["row_count"] == 2

    def test_analyst_window_with_cte_named_logs(self):
        """``WITH logs AS (...)`` still parses AND stays windowed. The rebind
        substitutes a plain view name (not a subquery), so a CTE that shadows
        the table resolves correctly instead of producing a syntax error."""
        con = self._con()
        sql = "WITH logs AS (SELECT * FROM logs WHERE url LIKE '/%') SELECT count(*) AS n FROM logs"
        result = execute_query(con, self.SRC, sql, max_rows=100, want_explain=False, time_filter=self.WINDOW)
        assert result["data"][0]["n"] == 2

    def test_default_service_table_named_logs_is_windowed(self):
        """When the service table IS literally ``logs`` (the 'default' service)
        the first ``logs``→table rewrite is skipped — the rebind must still
        window it."""
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR)")
        con.executemany(
            "INSERT INTO logs VALUES (?, ?)",
            [("2026-06-17T10:00:00+00:00", "1.1.1.1"), ("2026-06-01T00:00:00+00:00", "2.2.2.2")],
        )
        result = execute_query(
            con,
            {"name": "default"},
            "SELECT count(*) AS n FROM logs",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert result["data"][0]["n"] == 1

    def test_window_view_is_dropped_after_call(self):
        """The reserved temp view must not linger on the pooled connection —
        the next request (different session / service / window) reuses it."""
        from backend.repositories.query import _ANALYST_WINDOW_VIEW

        con = self._con()
        execute_query(con, self.SRC, "SELECT * FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW)
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()

    def test_window_view_dropped_even_when_query_fails(self):
        """A failing user query must not leave the window view behind."""
        from backend.repositories.query import _ANALYST_WINDOW_VIEW

        con = self._con()
        with pytest.raises(Exception):  # noqa: B017 — Binder error on a bad column
            execute_query(
                con, self.SRC, "SELECT no_such_col FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW
            )
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()


class TestValueShapeMasking:
    """H2: ``mask_ips`` masks result IPs by VALUE, not by output column name."""

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.execute("INSERT INTO logs VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', '/path')")
        return con

    def test_mask_ips_survives_column_aliasing(self):
        """``SELECT ip AS addr`` defeats key-name masking but NOT value-shape
        masking — this is the H2 bypass scenario."""
        con = self._con()
        result = execute_query(
            con, None, "SELECT ip AS addr, url FROM logs", max_rows=100, want_explain=False, mask_ips=True
        )
        row = result["data"][0]
        assert row["addr"] == "1.2.3.xxx"
        assert row["url"] == "/path"  # non-IP cell untouched

    def test_mask_ips_false_returns_raw_ip(self):
        con = self._con()
        result = execute_query(
            con, None, "SELECT ip AS addr FROM logs", max_rows=100, want_explain=False, mask_ips=False
        )
        assert result["data"][0]["addr"] == "1.2.3.4"


class TestMaxRowsClamp:
    """M1: execute_query re-clamps max_rows to MAX_QUERY_ROWS regardless of the
    requested value (defense-in-depth for callers that bypass the model)."""

    def test_max_rows_clamped_to_ceiling(self):
        from backend.repositories.query import MAX_QUERY_ROWS

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER)")
        con.executemany("INSERT INTO logs VALUES (?)", [(i,) for i in range(MAX_QUERY_ROWS + 50)])
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id", max_rows=10**9, want_explain=False)
        assert result["row_count"] == MAX_QUERY_ROWS
        assert result["truncated"] is True
