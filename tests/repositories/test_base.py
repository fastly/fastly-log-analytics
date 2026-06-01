"""Tests for shared repository utilities in backend/repositories/_base.py."""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from backend.repositories._base import QueryRunner, _is_stale_view_error, optional_col, safe_iso

# ── safe_iso ──────────────────────────────────────────────────────────────────


class TestSafeIso:
    def test_none_returns_none(self):
        assert safe_iso(None) is None

    def test_aware_datetime(self):
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        result = safe_iso(dt)
        assert isinstance(result, str)
        assert "2024-06-15" in result

    def test_naive_datetime_appends_z(self):
        dt = datetime(2024, 6, 15, 12, 0, 0)
        result = safe_iso(dt)
        assert result.endswith("Z")

    def test_string_passthrough(self):
        s = "2024-06-15T12:00:00Z"
        assert safe_iso(s) == s

    def test_arbitrary_object_stringified(self):
        result = safe_iso(42)
        assert result == "42"


# ── _is_stale_view_error ──────────────────────────────────────────────────────


class TestIsStaleViewError:
    @pytest.mark.parametrize(
        "msg",
        [
            "No files found in the given path",
            "Catalog Error: Table with name foo does not exist",
            "does not exist in this context",
            "No such file or directory: /tmp/buf.parquet",
        ],
    )
    def test_stale_messages_return_true(self, msg):
        assert _is_stale_view_error(Exception(msg)) is True

    @pytest.mark.parametrize(
        "msg",
        [
            "Syntax error near SELECT",
            "Column 'foo' not found",
            "Out of memory",
            "",
        ],
    )
    def test_non_stale_messages_return_false(self, msg):
        assert _is_stale_view_error(Exception(msg)) is False


# ── QueryRunner ───────────────────────────────────────────────────────────────


class TestQueryRunner:
    def test_execute_tracks_query(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        initial_count = len(runner.debug_queries)

        runner.execute("SELECT 1")

        assert len(runner.debug_queries) == initial_count + 1
        last_q = runner.debug_queries[-1]
        assert "SELECT 1" in last_q["sql"]
        assert "time_ms" in last_q
        assert isinstance(last_q["time_ms"], float)

    def test_execute_returns_cursor(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        result = runner.execute("SELECT 42 AS n")
        assert result.fetchone()[0] == 42

    def test_execute_with_retry_succeeds_on_first_try(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        result = runner.execute_with_retry("SELECT 99 AS n")
        assert result is not None
        assert result.fetchone()[0] == 99

    def test_execute_with_retry_retries_on_stale_view(self, in_memory_duckdb, test_service_source):
        """On a stale-view error, the runner refreshes the view and retries."""
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        stale_error = Exception("No files found in the given path")

        call_count = {"n": 0}
        original_execute = runner.execute

        def flaky_execute(sql, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise stale_error
            return original_execute(sql, params)

        with (
            patch.object(runner, "execute", side_effect=flaky_execute),
            patch("backend.core.iceberg.update_iceberg_view"),
        ):
            result = runner.execute_with_retry("SELECT 1")

        assert call_count["n"] == 2

    def test_execute_with_retry_reraises_non_stale_error(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        with patch.object(runner, "execute", side_effect=Exception("Syntax error near SELECT")):
            with pytest.raises(Exception, match="Syntax error"):
                runner.execute_with_retry("SELECT bad syntax !!!")

    def test_create_temp_table_returns_true_on_success(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        ok = runner.create_temp_table("CREATE TEMP TABLE _test_runner_tmp AS SELECT 1 AS x")
        # Cleanup
        in_memory_duckdb.execute("DROP TABLE IF EXISTS _test_runner_tmp")
        assert ok is True

    def test_create_temp_table_returns_false_on_permanent_stale_failure(self, in_memory_duckdb, test_service_source):
        """Returns False (not raises) when both attempts fail with stale-view errors."""
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        stale_error = Exception("No files found in the given path")

        with (
            patch.object(runner, "execute", side_effect=stale_error),
            patch("backend.core.iceberg.update_iceberg_view"),
        ):
            ok = runner.create_temp_table("CREATE TEMP TABLE _unreachable AS SELECT 1")

        assert ok is False

    def test_get_schema_cols_returns_list(self, in_memory_duckdb, test_service_source):
        """Returns a list (possibly empty) — never raises."""
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        cols = runner.get_schema_cols()
        assert isinstance(cols, list)


# ── optional_col ──────────────────────────────────────────────────────────────


class TestOptionalCol:
    def test_returns_quoted_col_when_present(self):
        assert optional_col("ottlb", {"ottlb", "ottfb"}) == '"ottlb"'

    def test_returns_default_when_absent(self):
        assert optional_col("obytes", {"ottlb"}) == "NULL"

    def test_custom_default(self):
        assert optional_col("elapsed", set(), default="0") == "0"

    def test_works_with_list_actual_cols(self):
        assert optional_col("ottfb", ["ottfb", "ottlb"]) == '"ottfb"'

    def test_missing_from_list_returns_default(self):
        assert optional_col("obytes", ["ottlb"]) == "NULL"


# ── create_filtered_temp_table ────────────────────────────────────────────────


class TestCreateFilteredTempTable:
    def test_creates_table_and_returns_name(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE src_test (ts VARCHAR, status INT)")
        in_memory_duckdb.execute("INSERT INTO src_test VALUES ('2024-01-01', 200)")
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        name = runner.create_filtered_temp_table(
            cols=["ts", "status"],
            actual_cols=["ts", "status"],
            source_table="src_test",
            where_clause="1=1",
        )

        assert name is not None
        assert name.startswith("t_")
        row = in_memory_duckdb.execute(f"SELECT status FROM {name}").fetchone()
        assert row[0] == 200
        in_memory_duckdb.execute("DROP TABLE src_test")

    def test_returns_none_when_no_cols_match(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        name = runner.create_filtered_temp_table(
            cols=["nonexistent"],
            actual_cols=["ts", "status"],
            source_table="any_table",
            where_clause="1=1",
        )
        assert name is None

    def test_filters_cols_against_actual(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE src_test2 (ts VARCHAR, status INT)")
        in_memory_duckdb.execute("INSERT INTO src_test2 VALUES ('t', 404)")
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        name = runner.create_filtered_temp_table(
            cols=["ts", "status", "missing_col"],
            actual_cols=["ts", "status"],
            source_table="src_test2",
            where_clause="1=1",
        )

        assert name is not None
        cols = [d[0] for d in in_memory_duckdb.execute(f"DESCRIBE {name}").fetchall()]
        assert "ts" in cols
        assert "status" in cols
        assert "missing_col" not in cols
        in_memory_duckdb.execute("DROP TABLE src_test2")


# ── execute_top_n_batch ───────────────────────────────────────────────────────


class TestExecuteTopNBatchIntegerAggregation:
    """The Top-N path rounds FLOAT 'integer-semantic' fields (ttl, age) to
    integer before GROUP BY. Without this, Fastly's `obj.ttl`/`obj.age`
    serialization jitter splits a single logical TTL value into many tiny
    buckets (3600.027, 3600.028, …) and the dashboard shows 10 rows of
    near-identical floats instead of one row aggregating them all."""

    def test_ttl_float_jitter_collapses_to_integer_bucket(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_ttl (ttl FLOAT)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_ttl VALUES (3600.027), (3600.028), (3600.029), (3600.030), (3601.001)"
        )
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        rows, order = runner.execute_top_n_batch(
            fields=["ttl"],
            table_name="logs_ttl",
            actual_cols=["ttl"],
            schema_types={"ttl": "FLOAT"},
        )
        in_memory_duckdb.execute("DROP TABLE logs_ttl")

        # All 5 rows are FLOAT but should collapse to two integer buckets:
        # 3600 (rounds the .027/.028/.029/.030 — banker's rounding may pull
        # 3600.5 either way but 3600.030 rounds to 3600) and 3601.
        assert order == ["ttl"]
        buckets = {value: count for (_field, value, count) in rows}
        assert "3600" in buckets
        assert "3601" in buckets
        # The 4 sub-integer values should all roll into 3600
        assert buckets["3600"] == 4
        assert buckets["3601"] == 1
        # No fractional keys
        assert all("." not in v for v in buckets)

    def test_age_float_jitter_also_collapses(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_age (age FLOAT)")
        in_memory_duckdb.execute("INSERT INTO logs_age VALUES (0.0), (0.0), (1.0), (1.0), (1.0), (2.0)")
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        rows, _ = runner.execute_top_n_batch(
            fields=["age"],
            table_name="logs_age",
            actual_cols=["age"],
            schema_types={"age": "FLOAT"},
        )
        in_memory_duckdb.execute("DROP TABLE logs_age")

        buckets = {value: count for (_field, value, count) in rows}
        # Whole-number floats stay distinct (correct) but render w/o ".0"
        assert buckets == {"0": 2, "1": 3, "2": 1}

    def test_non_int_aggregate_float_field_is_not_rounded(self, in_memory_duckdb, test_service_source):
        """Other FLOAT fields (e.g. tcp_rtt, elapsed) keep their fractional
        precision — rounding would lose meaningful sub-second resolution."""
        in_memory_duckdb.execute("CREATE TABLE logs_rtt (tcp_rtt DOUBLE)")
        in_memory_duckdb.execute("INSERT INTO logs_rtt VALUES (0.012), (0.013), (0.013)")
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        rows, _ = runner.execute_top_n_batch(
            fields=["tcp_rtt"],
            table_name="logs_rtt",
            actual_cols=["tcp_rtt"],
            schema_types={"tcp_rtt": "DOUBLE"},
        )
        in_memory_duckdb.execute("DROP TABLE logs_rtt")

        buckets = {value: count for (_field, value, count) in rows}
        # Should preserve fractional values, not collapse to 0
        assert "." in next(iter(buckets))
        assert buckets.get("0.013") == 2
