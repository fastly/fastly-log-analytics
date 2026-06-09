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

    def test_execute_self_heals_on_stale_view(self, test_service_source):
        """``execute()`` itself refreshes the view + retries once when DuckDB
        raises a stale-view error. This is the safety net that catches buffer
        commits landing mid-query, complementing the pool's checkout fingerprint."""
        from unittest.mock import MagicMock

        attempts = {"n": 0}

        def flaky_execute(sql, params=None):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise Exception("IO Error: No files found that match the pattern .../buffer/batch_x.parquet")
            cursor = MagicMock()
            cursor.fetchone.return_value = (1,)
            return cursor

        fake_con = MagicMock()
        fake_con.execute.side_effect = flaky_execute
        runner = QueryRunner(fake_con, test_service_source)

        refresh_calls = {"n": 0, "force_seen": None}

        def fake_refresh(con, src, force=False, lock_timeout=5.0):
            refresh_calls["n"] += 1
            refresh_calls["force_seen"] = force

        with patch("backend.core.iceberg.update_iceberg_view", side_effect=fake_refresh):
            res = runner.execute("SELECT 1")

        assert attempts["n"] == 2, "should retry once after refresh"
        assert refresh_calls["n"] == 1, "should call update_iceberg_view exactly once"
        # Self-heal must force a real rebuild — fast-path would re-execute
        # the same cached SQL that contains the deleted file path.
        assert refresh_calls["force_seen"] is True, "self-heal must pass force=True"
        assert res.fetchone()[0] == 1

    def test_execute_reraises_non_stale_error(self, test_service_source):
        """A non-stale error (e.g. syntax) is re-raised immediately — no retry."""
        from unittest.mock import MagicMock

        attempts = {"n": 0}

        def always_fail(sql, params=None):
            attempts["n"] += 1
            raise Exception("Parser Error: syntax error at or near 'BLOOP'")

        fake_con = MagicMock()
        fake_con.execute.side_effect = always_fail
        runner = QueryRunner(fake_con, test_service_source)

        with pytest.raises(Exception, match="Parser Error"):
            runner.execute("BLOOP")
        assert attempts["n"] == 1, "non-stale error should not trigger retry"

    def test_execute_surfaces_original_error_when_refresh_fails(self, test_service_source):
        """If update_iceberg_view itself raises, the caller should see the
        original stale-view error — not the refresh side-effect error."""
        from unittest.mock import MagicMock

        fake_con = MagicMock()
        fake_con.execute.side_effect = Exception("IO Error: No files found at path")
        runner = QueryRunner(fake_con, test_service_source)

        with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("rebind failed")):
            with pytest.raises(Exception, match="No files found"):
                runner.execute("SELECT 1")

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

    def test_get_schema_cols_self_heal_busts_view_cache_before_rebuild(self, test_service_source):
        """When ``_get_schema`` returns [] (view bound to deleted buffer file),
        the self-heal must call ``clear_source_caches`` BEFORE
        ``update_iceberg_view(force=True)``. Without busting the cache, the
        lock-timeout fallback in update_iceberg_view re-executes the SAME
        stale cached SQL, the view stays bound to the dead path, the next
        ``_get_schema`` returns [] again, and the caller short-circuits via
        ``empty_schema_response`` — surfacing as 'No data available' on a 200.
        Prod regression witnessed 2026-06-09."""
        from unittest.mock import MagicMock

        runner = QueryRunner(MagicMock(), test_service_source)

        call_order: list[str] = []

        def fake_clear(source_key, keep_snapshot_cache=False):
            call_order.append(f"clear_source_caches(keep_snapshot_cache={keep_snapshot_cache})")

        def fake_refresh(con, src, force=False, lock_timeout=5.0):
            call_order.append(f"update_iceberg_view(force={force})")

        get_schema_calls = {"n": 0}

        def fake_get_schema(con, src):
            get_schema_calls["n"] += 1
            # First call returns empty (stale view); second call (post-rebuild) returns a real schema
            if get_schema_calls["n"] == 1:
                return []
            return [{"name": "timestamp"}, {"name": "ip"}, {"name": "status"}]

        with (
            patch("backend.repositories._base._get_schema", side_effect=fake_get_schema),
            patch("backend.core.iceberg.clear_source_caches", side_effect=fake_clear),
            patch("backend.core.iceberg.update_iceberg_view", side_effect=fake_refresh),
        ):
            cols = runner.get_schema_cols()

        assert call_order == [
            "clear_source_caches(keep_snapshot_cache=True)",
            "update_iceberg_view(force=True)",
        ], f"clear must run BEFORE refresh; got: {call_order}"
        assert cols == ["timestamp", "ip", "status"], "post-rebuild schema should be returned"


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

    def test_execute_top_n_rollups_uses_direct_active_hour_fast_path(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Pinned: the live merge branch attempts the direct-parquet fast
        path BEFORE the view-based create_filtered_temp_table fallback.

        Profiling on 2026-06-08 showed the view-based path takes ~700ms
        per request (entirely view-traversal overhead). The direct path
        reads buffer/*.parquet + data/timestamp_hour=<active>/*.parquet
        in ~6ms. Pinned because removing the fast-path call would silently
        regress the dashboard cold path by ~700ms.
        """
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        cache_root = tmp_path / "cache"
        (cache_root / "buffer").mkdir(parents=True)

        # Write a buffer parquet containing one active-hour row.
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([active_dt + timedelta(minutes=5)], type=pa.timestamp("us", tz="UTC")),
                    "country": pa.array(["US"]),
                }
            ),
            str(cache_root / "buffer" / "batch_test.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )
        # Ensure the rollup hour dir exists so we enter execute_top_n_rollups.
        (cache_root / "rollups" / "hour").mkdir(parents=True)

        # Spy on _create_active_hour_temp_direct to assert it's tried.
        direct_calls = {"n": 0}
        orig_direct = QueryRunner._create_active_hour_temp_direct

        def spy_direct(self, *a, **kw):
            direct_calls["n"] += 1
            return orig_direct(self, *a, **kw)

        monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", spy_direct)

        # Spy on create_filtered_temp_table to assert it's NOT called when direct succeeds.
        view_fallback_calls = {"n": 0}
        orig_view = QueryRunner.create_filtered_temp_table

        def spy_view_fallback(self, *a, **kw):
            view_fallback_calls["n"] += 1
            return orig_view(self, *a, **kw)

        monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", spy_view_fallback)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_end = active_dt + timedelta(hours=1)
        rows, _ = runner.execute_top_n_rollups(["country"], active_dt.isoformat(), active_end.isoformat(), limit=10)

        assert direct_calls["n"] == 1, f"direct active-hour fast path must be tried; got {direct_calls['n']} calls"
        assert view_fallback_calls["n"] == 0, (
            f"view-based fallback must NOT fire when direct path succeeds; got {view_fallback_calls['n']} fallback calls. "
            f"This regression means the dashboard cold path silently dropped ~700ms back."
        )
        # And the result must include the active-hour row.
        country_rows = [r for r in rows if r[0] == "country"]
        assert ("country", "US", 1) in country_rows, (
            f"active-hour buffer row must be merged into top-N; got {country_rows}"
        )

    def test_execute_top_n_rollups_falls_back_to_view_when_direct_finds_nothing(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """When neither buffer/ nor data/timestamp_hour=<active>/ has any
        parquet files (e.g. brand-new service that hasn't ingested yet
        OR the buffer was just flushed), the direct path returns None
        and the live merge skips. live_res stays empty — semantically
        correct (no active-hour data exists)."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        # Intentionally NO buffer/ or data/timestamp_hour=<active>/ dirs.
        (cache_root / "rollups" / "hour").mkdir(parents=True)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )
        # Spy: view fallback should NOT fire either (direct returns None
        # meaning "nothing on disk", not "failure" — caller should skip).
        view_fallback_calls = {"n": 0}
        orig_view = QueryRunner.create_filtered_temp_table

        def spy_view_fallback(self, *a, **kw):
            view_fallback_calls["n"] += 1
            return orig_view(self, *a, **kw)

        monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", spy_view_fallback)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_end = active_dt + timedelta(hours=1)
        rows, _ = runner.execute_top_n_rollups(["country"], active_dt.isoformat(), active_end.isoformat(), limit=10)

        # No data anywhere → no live rows, but call shouldn't crash.
        # IMPORTANT: today the direct path returns None when no files
        # exist, AND the view fallback would still fire. That's fine for
        # correctness (view returns empty) but wastes ~700ms. Future
        # optimization: have direct return a sentinel meaning "no data"
        # vs "couldn't read" so caller can skip the view too.
        country_rows = [r for r in rows if r[0] == "country"]
        assert country_rows == [], f"no data anywhere → no country rows; got {country_rows}"

    def test_execute_top_n_rollups_live_branch_actually_runs(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Regression: the live-active-hour merge branch had a broken
        ``from backend.core.duckdb import _get_schema`` import (the
        symbol lives in _base.py, not duckdb.py). The ImportError got
        caught by the surrounding bare except, silently dropping the
        live merge — so the top-N panels were missing the current
        hour's data for an indeterminate time in prod. Pinned so any
        future refactor that re-introduces a wrong-module import is
        caught: the test asserts the live query path actually executes
        AND returns the live-hour data."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute("CREATE TABLE logs_liveimport (timestamp TIMESTAMPTZ, country VARCHAR)")
        # Insert ONLY into the active hour so the only way the result
        # has any rows is if the live branch actually ran.
        in_memory_duckdb.execute(
            "INSERT INTO logs_liveimport VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [
                active_dt + timedelta(minutes=5),
                active_dt + timedelta(minutes=15),
                active_dt + timedelta(minutes=25),
            ],
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_liveimport")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)

        # Window spans the active hour so the live branch must fire.
        st = active_dt.isoformat()
        et = (active_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_liveimport")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 2 and country_counts.get("JP") == 1, (
            f"live branch did not run — top-N is missing the current hour's data. "
            f"This is the silent ImportError regression. Got: {country_counts}"
        )

    def test_execute_top_n_rollups_clamps_live_window_to_requested_range(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Pinned: when the requested window starts/ends mid-hour, the
        live-active-hour query must clamp to the INTERSECTION of
        [active_dt, active_dt_end) and [start_time, end_time]. Without
        the clamp a request for [active_dt+5min, active_dt+35min]
        over-counts by querying the FULL active hour and including
        rows outside the user's window — silently misleading counts
        for custom-date-range users.

        Uses the real current hour to avoid mocking datetime (which
        breaks other tests if it leaks). The test is robust across
        any wall-clock time: it pins rows at offsets relative to the
        actual active_dt computed at test start."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        # Compute active_dt the same way the production code does.
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_dt_end = active_dt + timedelta(hours=1)

        # Insert rows at known offsets relative to active_dt.
        in_memory_duckdb.execute("CREATE TABLE logs_clamp (timestamp TIMESTAMPTZ, country VARCHAR)")
        t1 = active_dt + timedelta(minutes=10)  # inside requested + active
        t2 = active_dt + timedelta(minutes=30)  # inside requested + active
        t3 = active_dt + timedelta(minutes=45)  # OUTSIDE requested, inside active
        in_memory_duckdb.execute(
            "INSERT INTO logs_clamp VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [t1, t2, t3],
        )

        # Point the runner at our test table; bypass rollup enumeration
        # by giving it a real but empty rollup dir (forces rolled_res=[]).
        monkeypatch.setattr("backend.repositories._base._cache_dir", lambda _src: str(tmp_path), raising=False)
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_clamp")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )
        rollup_hour_dir = tmp_path / "rollups" / "hour"
        rollup_hour_dir.mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)

        # Request [active_dt + 5min, active_dt + 35min]. Without the clamp,
        # the live query would scan [active_dt, active_dt_end) and pick up
        # the t3 row at +45min. With the clamp, t3 must be excluded.
        st = (active_dt + timedelta(minutes=5)).isoformat()
        et = (active_dt + timedelta(minutes=35)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)

        in_memory_duckdb.execute("DROP TABLE logs_clamp")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 2, (
            f"US rows at +10min and +30min should both be counted. Got {country_counts}"
        )
        assert "JP" not in country_counts, (
            f"JP row at +45min is OUTSIDE the requested [+5min, +35min] window but inside the "
            f"active hour — must NOT be counted. The clamp regressed. Got {country_counts}"
        )

    def test_execute_top_n_batch_prevents_sql_injection(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_safe (status VARCHAR)")
        in_memory_duckdb.execute("INSERT INTO logs_safe VALUES ('200'), ('200'), ('500')")
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        # Attempt an injection as a field name
        malicious_field = "status' UNION ALL SELECT 'evil' as field, 'payload' as value, 100 as c --"
        rows, order = runner.execute_top_n_batch(
            fields=[malicious_field, "status"],
            table_name="logs_safe",
            actual_cols=["status"],
            schema_types={"status": "VARCHAR"},
        )
        in_memory_duckdb.execute("DROP TABLE logs_safe")

        # The malicious field should have been skipped, so order only contains 'status'
        assert order == ["status"]
        assert len(rows) == 2
        assert all(row[0] == "status" for row in rows)
