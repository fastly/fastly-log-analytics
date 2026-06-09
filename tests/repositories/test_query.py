"""Tests for backend/repositories/query.py — execute_query and get_presets."""

import duckdb

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
        """'my_logs' or 'logs_extra' must not be rewritten."""
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc3 (id INTEGER)")
        con.execute("CREATE TABLE my_logs (id INTEGER)")
        con.execute("INSERT INTO logs_svc3 VALUES (1)")
        con.execute("INSERT INTO my_logs VALUES (99)")
        src = {"name": "svc3"}
        result = execute_query(con, src, "SELECT * FROM my_logs", max_rows=100, want_explain=False)
        assert result["data"][0]["id"] == 99


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

    def test_summarize_is_not_wrapped(self):
        """SUMMARIZE returns a fixed-shape stats result; wrapping with LIMIT
        would either error or change semantics. Must execute as-is."""
        con = self._con(rows=10)
        result = execute_query(con, None, "SUMMARIZE logs", max_rows=100, want_explain=False)
        # SUMMARIZE returns one row per column (2 columns → 2 rows).
        assert result["row_count"] == 2

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
