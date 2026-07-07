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
        "msg,exc_cls",
        [
            ("No files found in the given path", "IOException"),
            ("No such file or directory: /tmp/buf.parquet", "IOException"),
            ("Catalog Error: Table with name foo does not exist", "CatalogException"),
            ("does not exist in this context", "CatalogException"),
        ],
    )
    def test_stale_messages_return_true(self, msg, exc_cls):
        """Genuine stale-view shapes (IO + Catalog DuckDB exceptions with one
        of the canonical phrases) trigger the retry/rebuild path."""
        import duckdb

        exc = getattr(duckdb, exc_cls)(msg)
        assert _is_stale_view_error(exc) is True

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
        """Non-stale error messages must not trigger retry, even when
        raised through DuckDB IO/Catalog exceptions."""
        import duckdb

        assert _is_stale_view_error(duckdb.IOException(msg)) is False
        assert _is_stale_view_error(Exception(msg)) is False

    def test_substring_in_non_duckdb_exception_does_not_trigger_rebuild(self):
        """Finding 005 (2026-06-15): an attacker who can influence a filter
        value or column name can embed a canonical stale-view phrase
        into the message of a non-IO/non-Catalog DuckDB exception (e.g.
        ``ConversionException`` includes the offending input verbatim).
        Hammered, the prior substring-only detector treated each spoofed
        error as a stale view and triggered the expensive synchronous
        rebuild + catalog refresh — a credentialed-DoS vector against
        the per-service iceberg lock. The class check now rejects."""
        import duckdb

        spoofed = duckdb.ConversionException("Conversion failed: value 'No files found' is not a valid INTEGER")
        assert _is_stale_view_error(spoofed) is False, (
            "ConversionException with attacker-injected substring must not trigger the stale-view rebuild path"
        )

        # Another high-traffic exception class commonly seen with user-supplied
        # filter values — BinderException — must also be rejected even when
        # its message embeds a canonical phrase.
        binder = duckdb.BinderException("Binder Error: Table 'fake' does not exist")
        assert _is_stale_view_error(binder) is False


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

        import duckdb as _duckdb_mod

        def flaky_execute(sql, params=None):
            attempts["n"] += 1
            if attempts["n"] == 1:
                # Finding 005: the stale-view detector now requires a real
                # IOException / CatalogException (not a bare Exception with a
                # matching substring) so attacker-controlled ConversionException
                # messages can't spoof the rebuild path.
                raise _duckdb_mod.IOException(
                    "IO Error: No files found that match the pattern .../buffer/batch_x.parquet"
                )
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

        import duckdb as _duckdb_mod

        fake_con = MagicMock()
        fake_con.execute.side_effect = _duckdb_mod.IOException("IO Error: No files found at path")
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
        import duckdb as _duckdb_mod

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        stale_error = _duckdb_mod.IOException("No files found in the given path")

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
        import duckdb as _duckdb_mod

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        stale_error = _duckdb_mod.IOException("No files found in the given path")

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

    def test_execute_top_n_rollups_live_skips_nonrendered_identifier_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Lever B: the live active-hour top-up must skip fields in
        ``_LIVE_TOPN_SKIP_FIELDS`` (non-rendered per-request identifiers /
        raw metrics) while still merging current-hour data for rendered
        facet fields. ``rid`` is in the skip set; ``country`` is not.

        With an empty rollup dir, the ONLY source of data is the live
        branch. So a rendered field (country) must show the current hour,
        and a skipped field (rid) must be absent entirely — proving the
        live merge ran AND that it excluded the skip-set field (rather than
        skipping the whole branch)."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS, QueryRunner

        assert "rid" in _LIVE_TOPN_SKIP_FIELDS and "country" not in _LIVE_TOPN_SKIP_FIELDS

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute("CREATE TABLE logs_liveskip (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_liveskip VALUES (?, 'US', 'r1'), (?, 'US', 'r2'), (?, 'JP', 'r3')",
            [
                active_dt + timedelta(minutes=5),
                active_dt + timedelta(minutes=15),
                active_dt + timedelta(minutes=25),
            ],
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_liveskip")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country", "rid"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
                {"name": "rid", "type": "VARCHAR"},
            ],
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = active_dt.isoformat()
        et = (active_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country", "rid"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_liveskip")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        rid_values = [value for (field, value, count) in rows if field == "rid"]
        assert country_counts.get("US") == 2 and country_counts.get("JP") == 1, (
            f"rendered field 'country' lost its current-hour data — live merge regressed. Got {country_counts}"
        )
        assert rid_values == [], (
            f"skip-set field 'rid' must NOT be computed in the live top-up (no rollup data here either), "
            f"but got live rid values: {rid_values}"
        )

    def test_execute_top_n_rollups_rollup_path_still_returns_skip_field(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Lever B coverage (rollup direction): ``_LIVE_TOPN_SKIP_FIELDS``
        narrows the LIVE active-hour top-up ONLY — the rollup path still
        computes every requested field from parquet (it iterates
        ``safe_fields``, not the live-filtered list). So a skip-set field
        that HAS rollup data must still be returned, carrying its closed-hour
        rollup counts but NOT the current-hour live increment.

        Complements ``..._live_skips_nonrendered_identifier_fields`` (which
        seeds an empty rollup dir, so the skip-set field is absent entirely):
        here the rollup HAS data, pinning that the skip set never suppresses
        the rollup read. ``rid`` is in the skip set; ``country`` is not.

        The ``country`` merge (rollup + live) is also a vacuity guard: the
        live branch swallows exceptions (``except Exception: pass``), so a
        broken live setup would silently yield rollup-only counts — the
        ``country == 7`` assertion fails loudly if the live merge didn't run."""
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS, QueryRunner

        assert "rid" in _LIVE_TOPN_SKIP_FIELDS and "country" not in _LIVE_TOPN_SKIP_FIELDS

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_hour_dt = active_dt - timedelta(hours=1)  # fully-closed hour inside the window
        closed_hour_str = closed_hour_dt.strftime("%Y-%m-%d-%H")

        # Closed-hour rollup parquets (per-field per-hour layout) for BOTH a
        # skip-set field (rid) and a rendered field (country).
        for fld, val, cnt in [("rid", "r_rolled", 7), ("country", "US", 5)]:
            d = tmp_path / "rollups" / "hour" / f"field={fld}" / f"hour={closed_hour_str}"
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table({"value": [val], "count": pa.array([cnt], type=pa.int64())}),
                str(d / "compacted.parquet"),
            )

        # Live current-hour rows for BOTH fields, served via the view fallback
        # (no buffer/ dir → the direct fast path returns None).
        in_memory_duckdb.execute("CREATE TABLE logs_rollupkeep (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_rollupkeep VALUES (?, 'US', 'r_live'), (?, 'US', 'r_live')",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_rollupkeep")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country", "rid"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
                {"name": "rid", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = closed_hour_dt.isoformat()
        et = (active_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country", "rid"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_rollupkeep")

        by_field: dict[str, dict] = {}
        for fld, val, cnt in rows:
            by_field.setdefault(fld, {})[val] = cnt

        # country: rollup (5) merged with the live current-hour rows (2) = 7.
        assert by_field.get("country", {}).get("US") == 7, (
            f"rendered field 'country' must merge rollup (5) + live current-hour (2) = 7; got {by_field.get('country')}"
        )
        # rid: rollup data survives — the skip set never touches the rollup read...
        assert by_field.get("rid", {}).get("r_rolled") == 7, (
            f"skip-set field 'rid' must still return its closed-hour rollup count (7) — the skip set narrows the "
            f"LIVE top-up only, not the rollup path; got {by_field.get('rid')}"
        )
        # ...but its current-hour live increment must NOT appear (live top-up skipped it).
        assert "r_live" not in by_field.get("rid", {}), (
            f"skip-set field 'rid' must NOT pick up the current-hour live increment (live top-up skips it); "
            f"got {by_field.get('rid')}"
        )

    def test_execute_top_n_rollups_skip_field_with_no_data_does_not_warn(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Lever B side-effect guard: a ``_LIVE_TOPN_SKIP_FIELDS`` field that
        ends up with no merged data (live top-up skips it, rollup has none)
        must NOT emit the '[top_n_rollups] empty result' warning — that's an
        expected outcome for a non-rendered field, not a backfill gap. A
        rendered field that is genuinely empty MUST still warn, so the
        operator signal isn't lost.

        ``rid`` is in the skip set; ``status`` is rendered and here has only
        NULLs (no top-N values), so it exercises the real empty-warning path."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories import _base
        from backend.repositories._base import _EMPTY_ROLLUP_WARN_TS, _LIVE_TOPN_SKIP_FIELDS, QueryRunner

        assert "rid" in _LIVE_TOPN_SKIP_FIELDS and "status" not in _LIVE_TOPN_SKIP_FIELDS
        # Clear cross-test rate-limit state so prior runs can't suppress our warning.
        _EMPTY_ROLLUP_WARN_TS.clear()
        # The rate-limit compares time.monotonic() to the (cleared → 0.0) last-warn
        # stamp: `now - 0.0 >= _EMPTY_ROLLUP_WARN_INTERVAL_S` (300s). On a freshly
        # booted CI runner monotonic() can itself be < 300, so the check is False
        # and the warning is suppressed — an uptime-dependent flake (green locally
        # and on warm runners, red on cold ones). Drop the interval to 0 so a
        # genuinely-empty rendered field always warns here.
        monkeypatch.setattr(_base, "_EMPTY_ROLLUP_WARN_INTERVAL_S", 0.0)

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute(
            "CREATE TABLE logs_skipwarn (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR, status VARCHAR)"
        )
        # country has live data; rid+status do not (status column is all-NULL).
        in_memory_duckdb.execute(
            "INSERT INTO logs_skipwarn VALUES (?, 'US', NULL, NULL), (?, 'JP', NULL, NULL)",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_skipwarn")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country", "rid", "status"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
                {"name": "rid", "type": "VARCHAR"},
                {"name": "status", "type": "VARCHAR"},
            ],
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = active_dt.isoformat()
        et = (active_dt + timedelta(hours=1)).isoformat()
        # Capture the warning by spying on the module logger directly rather than
        # via caplog. Under `-n auto`, a concurrent test can leave global logging
        # state altered (level / propagation / logging.disable), which made caplog
        # intermittently capture zero records here — green locally and on the push
        # CI run, red on the PR run of the same commit. A direct spy is immune to
        # global logging state and preserves the exact assertions below.
        captured: list[str] = []

        def _spy_warning(msg, *args, **_kwargs):
            captured.append(msg % args if args else str(msg))

        monkeypatch.setattr(_base._logger, "warning", _spy_warning)
        runner.execute_top_n_rollups(["country", "rid", "status"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_skipwarn")

        warnings = [m for m in captured if "empty result for field=" in m]
        assert not any("field='rid'" in m for m in warnings), (
            f"skip-set field 'rid' must NOT warn when it has no merged data (it's non-rendered by design), "
            f"but a warning fired: {warnings}"
        )
        assert any("field='status'" in m for m in warnings), (
            f"rendered field 'status' is genuinely empty and MUST still warn so the operator signal isn't lost, "
            f"but no warning fired: {warnings}"
        )

    def test_live_topn_skip_fields_are_not_rendered_dashboard_panels(self):
        """Drift guard: every field in ``_LIVE_TOPN_SKIP_FIELDS`` is dropped
        from the LIVE active-hour top-up, so if one were ALSO rendered as a
        dashboard facet panel that panel would silently lose current-hour
        freshness (it'd show data only through the last closed hour) with no
        test failure. Assert the skip set stays disjoint from the categorized
        card IDs the frontend renders (frontend/app/dashboard/_sections/
        categories.ts → CARD_CATEGORIES).

        Limitation: this covers the *static* categorized cards only. Custom
        cards (bootstrap ``show_in_dashboard`` fields not in CATEGORIZED_CARD_IDS)
        are runtime config and can't be checked here — but the skip set is
        non-rendered identifiers / raw metrics that an admin wouldn't surface,
        and a category collision is the realistic drift this catches."""
        import re
        from pathlib import Path

        from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS

        repo_root = Path(__file__).resolve().parents[2]
        categories_ts = repo_root / "frontend" / "app" / "dashboard" / "_sections" / "categories.ts"
        assert categories_ts.is_file(), f"expected dashboard categories file at {categories_ts}"

        text = categories_ts.read_text()
        rendered: set[str] = set()
        # cardIds arrays may span multiple lines; capture each [ ... ] block.
        for block in re.findall(r"cardIds:\s*\[(.*?)\]", text, re.DOTALL):
            rendered.update(re.findall(r"'([^']+)'", block))

        # Guard against a parse regression silently passing the test.
        assert "country" in rendered and len(rendered) > 30, (
            f"categories.ts parse looks wrong (found {len(rendered)} card ids) — "
            f"fix the parser before trusting this guard"
        )

        collisions = _LIVE_TOPN_SKIP_FIELDS & rendered
        assert not collisions, (
            f"{sorted(collisions)} are in _LIVE_TOPN_SKIP_FIELDS but ALSO rendered as dashboard "
            f"facet panels (categories.ts) — those panels would silently lose current-hour freshness. "
            f"Either remove them from the skip set or stop rendering them."
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

    def test_execute_top_n_rollups_skips_day_file_on_partial_window(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Partial-day windows (start or end mid-day) must NOT include the
        boundary day's per-day rollup file — it covers the full 24 hours
        and would surface values from outside the user's window. Reader
        must fall back to per-hour rollups for the in-window hours.

        Pinned because the symptom is a phantom top-N value: user sees
        ``edge_score=50, count=154`` on a 24h window starting at 17:36,
        clicks it, and ``/query`` returns zero rows because the matching
        rows are actually at 05:00 (12 hours before the window).
        """
        import uuid
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        def _write_per_hour(field: str, hour: str, rows: list[tuple]) -> None:
            d = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={hour}"
            d.mkdir(parents=True, exist_ok=True)
            table = pa.table(
                {
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            )
            pq.write_table(table, str(d / f"compacted_{uuid.uuid4().hex[:8]}.parquet"))

        def _write_per_day(field: str, day: str, rows: list[tuple]) -> None:
            d = cache_root / "rollups" / "day" / f"field={field}" / f"day={day}"
            d.mkdir(parents=True, exist_ok=True)
            table = pa.table(
                {
                    "field": pa.array([field for _ in rows]),
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            )
            pq.write_table(table, str(d / "compacted.parquet"))

        # Anchor relative to the active hour so we don't have to mock
        # datetime. Boundary day D is two days before today (so it's
        # always closed). Window: [D 17:36, D+1 17:36).
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_plus_1 = day_d + timedelta(days=1)
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = day_d_plus_1.isoformat()

        # Per-day file for boundary day D contains BOTH:
        #   "in_window_val" (count=10, would be at hour 20 — inside window)
        #   "out_of_window_val" (count=99, would be at hour 05 — outside window)
        # If the reader uses this day file, BOTH values surface in top-N.
        # With the fix the day file is skipped and only per-hour rollups
        # for the in-window hours of D contribute — so out_of_window_val
        # never appears.
        _write_per_day(
            "edge_score",
            day_d_str,
            [("in_window_val", 10), ("out_of_window_val", 99)],
        )
        # Per-hour rollups for D's in-window hours only have in_window_val.
        for h in range(18, 24):
            _write_per_hour("edge_score", f"{day_d_str}-{h:02d}", [("in_window_val", 1)])
        # The boundary hour 17 also exists with in_window_val; the
        # 00:00-17:36 portion of D is intentionally NOT in any per-hour
        # file present (mirrors the user repro where out_of_window_val
        # only lives in the early-morning hours of the day rollup).
        _write_per_hour("edge_score", f"{day_d_str}-17", [("in_window_val", 1)])

        # D+1 is the active or end-day side. Per-day must NOT cover it
        # (active-day guard) and its per-hour files contribute in-window
        # contents.
        for h in range(0, 18):
            _write_per_hour("edge_score", f"{day_d_plus_1_str}-{h:02d}", [("in_window_val", 1)])

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "edge_score"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "edge_score", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = (datetime.combine(day_d, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)).isoformat()
        et = (
            datetime.combine(day_d_plus_1, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)
        ).isoformat()
        rows, _ = runner.execute_top_n_rollups(["edge_score"], st, et, limit=10)

        values = {value: count for (field, value, count) in rows if field == "edge_score"}
        assert "out_of_window_val" not in values, (
            f"out_of_window_val (count=99) lives only in the boundary day's per-day rollup. "
            f"It MUST NOT appear when the request window starts mid-day — that's the partial-day "
            f"over-inclusion bug. Got {values}."
        )
        assert values.get("in_window_val", 0) > 0, (
            f"in_window_val must be surfaced from per-hour rollups for the boundary days; got {values}"
        )

    def test_execute_top_n_rollups_uses_day_file_when_window_fully_contains_day(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Companion to the partial-window test: when the window FULLY
        contains a closed day (hour-aligned [D 00:00, D+1 00:00)), the
        per-day rollup IS used — preserving the ~24x file-open
        reduction it was built for."""
        import uuid
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = (day_d + timedelta(days=1)).isoformat()

        # Day file says count=42; if it's not used, per-hour file (count=1)
        # would surface instead and the count would be wrong.
        d = cache_root / "rollups" / "day" / "field=edge_score" / f"day={day_d_str}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"field": ["edge_score"], "value": ["v"], "count": pa.array([42], type=pa.int64())}),
            str(d / "compacted.parquet"),
        )
        # Stub per-hour to a different count so a wrong-source read would
        # be visible.
        h = cache_root / "rollups" / "hour" / "field=edge_score" / f"hour={day_d_str}-12"
        h.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"value": ["v"], "count": pa.array([1], type=pa.int64())}),
            str(h / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "edge_score"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "edge_score", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = f"{day_d_str}T00:00:00+00:00"
        et = f"{day_d_plus_1_str}T00:00:00+00:00"
        rows, _ = runner.execute_top_n_rollups(["edge_score"], st, et, limit=10)

        values = {value: count for (field, value, count) in rows if field == "edge_score"}
        assert values.get("v") == 42, (
            f"hour-aligned window fully containing day D must use the per-day rollup (count=42), "
            f"not the per-hour rollup (count=1). Got {values}."
        )

    def test_execute_top_n_rollups_no_day_vs_bundled_double_count(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """When both a per-day rollup AND per-hour-bundled files exist for
        the same closed day, the reader must NOT include both — the
        UNION ALL would sum the same data twice. The bundled-hour walk
        should skip hours whose day is already covered by a usable
        per-day file for at least one safe field.

        Pre-fix: a 24h hour-aligned closed-day window returned 2x counts
        because the day file aggregated the day AND each of the 24
        bundled-hour files (containing the same data) were also UNION'd."""
        import uuid
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = (day_d + timedelta(days=1)).isoformat()

        # Per-day file: edge_score = "v" with count=100
        d = cache_root / "rollups" / "day" / "field=edge_score" / f"day={day_d_str}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"field": ["edge_score"], "value": ["v"], "count": pa.array([100], type=pa.int64())}),
            str(d / "compacted.parquet"),
        )
        # Per-hour-bundled file for one hour of D containing the same
        # underlying counts. If the reader includes both day file AND
        # this bundled file, we'd see >100.
        bd = cache_root / "rollups" / "hour_bundled" / f"hour={day_d_str}-05"
        bd.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"field": ["edge_score"], "value": ["v"], "count": pa.array([100], type=pa.int64())}),
            str(bd / "all_fields.parquet"),
        )
        # And a per-field per-hour file too, to ensure the per-field walk
        # also correctly defers to the day file (existing behavior).
        h = cache_root / "rollups" / "hour" / "field=edge_score" / f"hour={day_d_str}-05"
        h.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"value": ["v"], "count": pa.array([100], type=pa.int64())}),
            str(h / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "edge_score"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "edge_score", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = f"{day_d_str}T00:00:00+00:00"
        et = f"{day_d_plus_1_str}T00:00:00+00:00"
        rows, _ = runner.execute_top_n_rollups(["edge_score"], st, et, limit=10)

        values = {value: count for (field, value, count) in rows if field == "edge_score"}
        assert values.get("v") == 100, (
            f"hour-aligned closed-day window must return day-file count (100), not double-counted "
            f"day+bundled (200) or day+bundled+per-field (300). Got {values}."
        )

    def test_execute_top_n_rollups_bundled_branch_scopes_to_requested_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """The bundled branches carry EVERY field's rows; a single-field
        caller (e.g. the security UA-rollup read) must still get exactly its
        field back. Guards the ``WHERE field IN (...)`` scoping added to the
        bundled read — a malformed IN-list would fail the whole rollup read
        (rolled_res == []) and this asserts through that seam."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        from datetime import UTC, datetime, timedelta

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()

        # The reader requires the per-field hour root to exist (it early-
        # returns otherwise); the data itself lives in the bundle below.
        (cache_root / "rollups" / "hour").mkdir(parents=True, exist_ok=True)
        # One bundled hour carrying TWO fields' rows.
        bd = cache_root / "rollups" / "hour_bundled" / f"hour={day_d_str}-05"
        bd.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "field": ["ua", "ua", "country"],
                    "value": ["bot-a", "bot-b", "US"],
                    "count": pa.array([7, 3, 99], type=pa.int64()),
                }
            ),
            str(bd / "all_fields.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "ua", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "ua", "type": "VARCHAR"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = f"{day_d_str}T05:00:00+00:00"
        et = f"{day_d_str}T06:00:00+00:00"
        rows, _ = runner.execute_top_n_rollups(["ua"], st, et, limit=50000, per_field_limits={"ua": 50000})

        assert {(f, v, c) for (f, v, c) in rows} == {("ua", "bot-a", 7), ("ua", "bot-b", 3)}, (
            f"single-field bundled read must return exactly the requested field's rows. Got {rows}."
        )

    def test_execute_top_n_rollups_bundled_still_used_when_no_day_file_for_field(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """When a closed day has a day file for ONE field but not ANOTHER,
        the bundled-hour file is still skipped (to avoid double-counting
        the field with a day file) and the field WITHOUT a day file falls
        back to per-field per-hour. Pinned because the new bundled-skip
        check is global (any field with a day file), so the cost of
        avoiding the double-count is per-field per-hour for the
        uncovered field — must still produce correct counts."""
        import uuid
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = (day_d + timedelta(days=1)).isoformat()

        # Field A: has a per-day file (count=50)
        da = cache_root / "rollups" / "day" / "field=field_a" / f"day={day_d_str}"
        da.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"field": ["field_a"], "value": ["a1"], "count": pa.array([50], type=pa.int64())}),
            str(da / "compacted.parquet"),
        )
        # Field B: NO day file, only per-field per-hour rollups (newly-
        # added custom field that compaction hasn't run for yet).
        for h_idx in range(24):
            h = cache_root / "rollups" / "hour" / "field=field_b" / f"hour={day_d_str}-{h_idx:02d}"
            h.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table({"value": ["b1"], "count": pa.array([3], type=pa.int64())}),
                str(h / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
            )
        # Field A also has a per-field hour dir (the day file was
        # compacted from it) — must NOT also surface or A double-counts.
        for h_idx in range(24):
            h = cache_root / "rollups" / "hour" / "field=field_a" / f"hour={day_d_str}-{h_idx:02d}"
            h.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table({"value": ["a1"], "count": pa.array([2], type=pa.int64())}),
                str(h / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
            )
        # Bundled hour for every hour of D (covering both fields).
        for h_idx in range(24):
            bd = cache_root / "rollups" / "hour_bundled" / f"hour={day_d_str}-{h_idx:02d}"
            bd.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table(
                    {
                        "field": ["field_a", "field_b"],
                        "value": ["a1", "b1"],
                        "count": pa.array([2, 3], type=pa.int64()),
                    }
                ),
                str(bd / "all_fields.parquet"),
            )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "dummy")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "field_a", "field_b"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "field_a", "type": "VARCHAR"},
                {"name": "field_b", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = f"{day_d_str}T00:00:00+00:00"
        et = f"{day_d_plus_1_str}T00:00:00+00:00"
        rows, _ = runner.execute_top_n_rollups(["field_a", "field_b"], st, et, limit=10)

        by_field: dict[str, dict] = {}
        for f, v, c in rows:
            by_field.setdefault(f, {})[v] = c
        assert by_field.get("field_a", {}).get("a1") == 50, (
            f"field_a must use its day file (50) without double-counting bundled or per-field per-hour. "
            f"Got {by_field.get('field_a')}."
        )
        assert by_field.get("field_b", {}).get("b1") == 24 * 3, (
            f"field_b has no day file — must fall back to per-field per-hour (24 hours × 3 = 72). "
            f"Got {by_field.get('field_b')}."
        )

    # ── execute_ip_spread_rollups ────────────────────────────────────────────

    @staticmethod
    def _write_per_field_ip_spread(cache_root, field, hour, rows):
        """Helper for the ip_spread reader tests: write a per-(field, hour)
        IP-spread parquet that matches the live writer's schema. Rows are
        dicts with keys (value, ip_sketch, ip_count_observed, sample_capped)."""
        import os
        import uuid

        import pyarrow as pa
        import pyarrow.parquet as pq

        d = os.path.join(str(cache_root), "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
        os.makedirs(d, exist_ok=True)
        table = pa.table(
            {
                "value": pa.array([r["value"] for r in rows]),
                "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
                "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
                "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
            }
        )
        p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
        pq.write_table(table, p)
        return p

    def test_execute_ip_spread_rollups_returns_empty_when_no_files(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Cold pool (no ip_spread tree at all) returns ({}, {}) — the
        caller's signal to fall back to live SQL. Pinned because the
        security FE relies on this empty-result being distinguishable
        from a real "zero IPs for any fingerprint" answer."""
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha", "ja3"],
            "2026-05-15T00:00:00Z",
            "2026-05-15T01:00:00Z",
        )

        assert counts == {}
        assert meta == {}

    def test_execute_ip_spread_rollups_merges_per_field_across_hours(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Happy path: per-field IP-spread parquets across N closed hours
        merge into a single distinct-IP estimate per (field, value).

        Two hours, same fingerprint, two overlapping IP sets — the merged
        HLL must estimate the UNION, not the sum (which would double-count
        the overlap). Pinned because this is the load-bearing invariant
        the security fingerprint cards rely on (and the entire point of
        the HLL rollup vs naive per-hour-summing)."""
        from backend.utils.hll import HyperLogLog

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        # Hour A: IPs 1-100 saw fingerprint "abc"
        # Hour B: IPs 51-150 saw fingerprint "abc" (overlap on 51-100)
        # Union = 150 distinct IPs.
        hll_a = HyperLogLog()
        hll_a.update([f"1.1.1.{i}" for i in range(1, 101)])
        hll_b = HyperLogLog()
        hll_b.update([f"1.1.1.{i}" for i in range(51, 151)])

        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-10",
            [
                {
                    "value": "abc",
                    "ip_sketch": hll_a.to_bytes(),
                    "ip_count_observed": 100,
                    "sample_capped": False,
                }
            ],
        )
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-11",
            [
                {
                    "value": "abc",
                    "ip_sketch": hll_b.to_bytes(),
                    "ip_count_observed": 100,
                    "sample_capped": False,
                }
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha"],
            "2026-05-15T10:00:00Z",
            "2026-05-15T12:00:00Z",
        )

        assert ("tls_ciphers_sha", "abc") in counts
        merged_estimate = counts[("tls_ciphers_sha", "abc")]
        # HLL ~6.5% standard error at p=8; allow 15% bound for test stability.
        # Naive per-hour-sum would yield 200; correct union is 150. The
        # estimate MUST be closer to 150 than 200 — that's the whole point.
        assert abs(merged_estimate - 150) < abs(merged_estimate - 200), (
            f"merged estimate {merged_estimate} is closer to the naive-sum 200 "
            f"than to the true-union 150 — the HLL merge isn't reducing the overlap"
        )
        assert abs(merged_estimate - 150) <= 25  # within HLL bound

        # Meta carries per-field coverage info.
        assert "tls_ciphers_sha" in meta
        assert meta["tls_ciphers_sha"]["coverage_hours"] == 2
        assert meta["tls_ciphers_sha"]["capped_values"] == 0

    def test_execute_ip_spread_rollups_skips_active_hour(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """The active (current UTC) hour must NOT contribute to the merged
        estimate — the writer hasn't materialized it yet, and including
        a stale active-hour parquet would silently miss in-flight IPs."""
        from datetime import UTC, datetime, timedelta

        from backend.utils.hll import HyperLogLog

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        active_hour = active_dt.strftime("%Y-%m-%d-%H")
        closed_hour = (active_dt - timedelta(hours=1)).strftime("%Y-%m-%d-%H")

        # Active hour has a sketch — must be ignored.
        hll_active = HyperLogLog()
        hll_active.update([f"9.9.9.{i}" for i in range(1, 51)])
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            active_hour,
            [
                {
                    "value": "abc",
                    "ip_sketch": hll_active.to_bytes(),
                    "ip_count_observed": 50,
                    "sample_capped": False,
                }
            ],
        )
        # Closed hour has its own sketch — must be the ONLY contributor.
        hll_closed = HyperLogLog()
        hll_closed.update([f"1.1.1.{i}" for i in range(1, 11)])
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            closed_hour,
            [
                {
                    "value": "abc",
                    "ip_sketch": hll_closed.to_bytes(),
                    "ip_count_observed": 10,
                    "sample_capped": False,
                }
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        # Window covers both hours — but active should be skipped.
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha"],
            (active_dt - timedelta(hours=2)).isoformat(),
            (active_dt + timedelta(hours=1)).isoformat(),
        )

        estimate = counts.get(("tls_ciphers_sha", "abc"), 0)
        # If the active hour leaked in, the union would be ~60. Skip working
        # → only the closed-hour 10 IPs contribute.
        assert estimate < 25, (
            f"active hour appears to have leaked in: estimate {estimate} suggests "
            f"the closed-hour IPs were merged with the active-hour ones"
        )

    def test_execute_ip_spread_rollups_propagates_capped_flag(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """When any input sketch is marked sample_capped=True, the merged
        result's per-field meta must surface that — so the caller / FE
        can render an "approximate (capped)" hint. Pinned because losing
        this propagation would silently hide the writer's cap-flag
        signal."""
        from backend.utils.hll import HyperLogLog

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        hll = HyperLogLog()
        hll.update([f"1.1.1.{i}" for i in range(10)])
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-10",
            [
                {
                    "value": "capped-fingerprint",
                    "ip_sketch": hll.to_bytes(),
                    "ip_count_observed": 5000,
                    "sample_capped": True,
                },
                {
                    "value": "uncapped-fingerprint",
                    "ip_sketch": hll.to_bytes(),
                    "ip_count_observed": 10,
                    "sample_capped": False,
                },
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        _, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha"],
            "2026-05-15T10:00:00Z",
            "2026-05-15T11:00:00Z",
        )

        assert meta["tls_ciphers_sha"]["capped_values"] == 1

    def test_execute_ip_spread_rollups_filters_unsafe_field_names(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Defense in depth: fields failing _is_safe_ident are dropped so
        a caller can't inject through the IN-list in the SQL filter.
        Same guard execute_top_n_rollups uses on its safe_fields list."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        # Field name with single quote attempting to break out of the IN list.
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha'; DROP TABLE foo; --"],
            "2026-05-15T10:00:00Z",
            "2026-05-15T11:00:00Z",
        )

        # All fields failed the safelist → no parquet read attempted.
        assert counts == {}
        assert meta == {}

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

    def test_execute_top_n_rollups_live_heals_unrolled_closed_hour(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Missing-hour live heal (2026-07-06): a CLOSED hour with rows in
        the base table but NO rollup coverage (no bundle, no per-field
        file) must be live-queried and merged — previously those rows
        silently vanished from every top-N panel until the nightly deep
        pass, making field_total disagree with total_rows by the whole
        day's traffic on bursty services."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        in_memory_duckdb.execute("SET TimeZone='UTC'")
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        in_memory_duckdb.execute("CREATE TABLE logs_healtest (timestamp TIMESTAMPTZ, country VARCHAR)")
        # Rows ONLY in a closed hour: the active-hour live branch can't
        # source them, and there are no rollup files — the heal is the
        # only path that can return them.
        in_memory_duckdb.execute(
            "INSERT INTO logs_healtest VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [
                closed_dt + timedelta(minutes=5),
                closed_dt + timedelta(minutes=15),
                closed_dt + timedelta(minutes=25),
            ],
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_healtest")
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

        # Window is ENTIRELY closed hours (ends before the active hour), so
        # the active-hour live branch never fires.
        st = (active_dt - timedelta(hours=3)).isoformat()
        et = (active_dt - timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healtest")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 2 and country_counts.get("JP") == 1, (
            f"missing-hour heal did not run — closed un-rolled hours are invisible to top-N. Got: {country_counts}"
        )

    def test_execute_top_n_rollups_heal_does_not_double_count_bundled_hours(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Steady state: an hour served by its bundle must NOT also be
        live-healed — the merge would double-count. The bundle count (5)
        deliberately differs from the raw rows (3) so a double-read is
        unambiguous."""
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        in_memory_duckdb.execute("SET TimeZone='UTC'")
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        hour_token = closed_dt.strftime("%Y-%m-%d-%H")
        in_memory_duckdb.execute("CREATE TABLE logs_healbundle (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healbundle VALUES (?, 'US'), (?, 'US'), (?, 'US')",
            [
                closed_dt + timedelta(minutes=5),
                closed_dt + timedelta(minutes=15),
                closed_dt + timedelta(minutes=25),
            ],
        )
        bundle_dir = tmp_path / "rollups" / "hour_bundled" / f"hour={hour_token}"
        bundle_dir.mkdir(parents=True)
        pq.write_table(
            pa.table({"field": ["country"], "value": ["US"], "count": pa.array([5], type=pa.int64())}),
            str(bundle_dir / "all_fields.parquet"),
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_healbundle")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = closed_dt.isoformat()
        et = (closed_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healbundle")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 5, (
            f"bundled hour was double-counted by the live heal (expected bundle-only count 5). Got: {country_counts}"
        )

    def test_execute_top_n_rollups_heal_skips_day_compacted_days(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Boundary hours of an already-day-compacted day are a KNOWN,
        bounded read gap (their per-hour files were deleted by design) —
        the heal must NOT live-scan them, or every mid-day window edge
        would pay a per-request day scan forever."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories._base import QueryRunner

        in_memory_duckdb.execute("SET TimeZone='UTC'")
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        # A closed hour ~26h back — on a different, closed UTC day for any
        # active_dt (26 > 24), i.e. the day-compacted-day scenario.
        compacted_hour_dt = active_dt - timedelta(hours=26)
        day_token = compacted_hour_dt.strftime("%Y-%m-%d")
        in_memory_duckdb.execute("CREATE TABLE logs_healcompacted (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healcompacted VALUES (?, 'US')",
            [compacted_hour_dt + timedelta(minutes=10)],
        )
        # Day-rollup presence: a REAL day parquet proves the day was
        # compacted (its hour files were deleted by design). The window
        # below only PARTIALLY covers the day, so the day file is rightly
        # skipped by the rollup read too — only the heal could surface the
        # raw row, and it must decline. (An EMPTY day dir is the crashed-
        # compaction case and is deliberately healable — see the companion
        # test below.) The count (99) differs from the raw row so a
        # wrong-source read would be visible.
        import pyarrow as pa
        import pyarrow.parquet as pq

        day_dir = tmp_path / "rollups" / "day" / "field=country" / f"day={day_token}"
        day_dir.mkdir(parents=True)
        pq.write_table(
            pa.table({"value": ["US"], "count": pa.array([99], type=pa.int64())}),
            str(day_dir / "compacted.parquet"),
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: "logs_healcompacted")
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        # Window cuts INTO the compacted day (not fully containing it), so
        # the day file (if it had data) would be skipped too — the exact
        # boundary-sliver case the heal must leave alone.
        st = (compacted_hour_dt - timedelta(hours=1)).isoformat()
        et = (compacted_hour_dt + timedelta(hours=2)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healcompacted")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert "US" not in country_counts, (
            f"heal live-scanned a day-compacted day's boundary hours — that turns every mid-day "
            f"window edge into a per-request day scan. Got: {country_counts}"
        )

    def _heal_test_setup(self, in_memory_duckdb, tmp_path, monkeypatch, table):
        """Shared monkeypatch seam for the missing-hour heal tests below."""
        from backend.repositories._base import QueryRunner

        in_memory_duckdb.execute("SET TimeZone='UTC'")
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: table)
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: ["timestamp", "country"])
        monkeypatch.setattr(
            "backend.repositories._base._get_schema",
            lambda _con, _src: [
                {"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"},
                {"name": "country", "type": "VARCHAR"},
            ],
        )
        (tmp_path / "rollups" / "hour").mkdir(parents=True, exist_ok=True)
        return QueryRunner

    def test_execute_top_n_rollups_heal_midhour_start_excludes_out_of_window_rows(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """A custom range starting MID-HOUR over an un-rolled closed hour:
        the heal's start clamp must exclude rows before st even though the
        hour token is in the missing set."""
        from datetime import UTC, datetime, timedelta

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        in_memory_duckdb.execute("CREATE TABLE logs_mh1 (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_mh1 VALUES (?, 'US'), (?, 'US'), (?, 'US'), (?, 'JP')",
            [
                closed_dt + timedelta(minutes=5),  # OUTSIDE window (before st)
                closed_dt + timedelta(minutes=15),  # OUTSIDE window (before st)
                closed_dt + timedelta(minutes=35),  # inside
                closed_dt + timedelta(minutes=45),  # inside
            ],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_mh1")
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        st = (closed_dt + timedelta(minutes=30)).isoformat()
        et = (closed_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_mh1")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert counts.get("US") == 1 and counts.get("JP") == 1, (
            f"mid-hour start clamp failed — heal returned rows outside [st, et). Got {counts}"
        )

    def test_execute_top_n_rollups_heal_midhour_end_excludes_out_of_window_rows(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Companion end-clamp pin: a window ending mid-hour must not pick
        up the un-rolled hour's rows after et."""
        from datetime import UTC, datetime, timedelta

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        in_memory_duckdb.execute("CREATE TABLE logs_mh2 (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_mh2 VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [
                closed_dt + timedelta(minutes=5),  # inside
                closed_dt + timedelta(minutes=35),  # OUTSIDE window (after et)
                closed_dt + timedelta(minutes=45),  # OUTSIDE window (after et)
            ],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_mh2")
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        st = closed_dt.isoformat()
        et = (closed_dt + timedelta(minutes=30)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_mh2")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert counts.get("US") == 1 and "JP" not in counts, (
            f"mid-hour end clamp failed — heal returned rows outside [st, et). Got {counts}"
        )

    def test_execute_top_n_rollups_heal_cap_bounds_lookback(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Un-rolled hours OLDER than _MISSING_HOUR_HEAL_CAP are NOT healed
        — the cap is the sole bound on the heal's live scan, so removing
        (or breaking) it would let a wiped rollup tree turn every request
        into an unbounded historical scan."""
        from datetime import UTC, datetime, timedelta

        from backend.repositories import _base as base_mod

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        beyond_cap_dt = active_dt - timedelta(hours=base_mod._MISSING_HOUR_HEAL_CAP + 2)
        in_memory_duckdb.execute("CREATE TABLE logs_healcap (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healcap VALUES (?, 'US')",
            [beyond_cap_dt + timedelta(minutes=10)],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healcap")
        runner = QueryRunner(in_memory_duckdb, test_service_source)

        st = (beyond_cap_dt - timedelta(hours=1)).isoformat()
        et = (beyond_cap_dt + timedelta(hours=2)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healcap")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert "US" not in counts, (
            f"heal scanned an hour beyond the {base_mod._MISSING_HOUR_HEAL_CAP}h cap. Got {counts}"
        )

    def test_execute_top_n_rollups_heal_does_not_double_count_per_field_hours(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """An hour covered by a PER-FIELD hour file (the pre-bundle
        intermediate state) must not also be live-healed. The file count
        (5) deliberately differs from the raw rows (3) so a double-read
        is unambiguous."""
        import uuid
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        hour_token = closed_dt.strftime("%Y-%m-%d-%H")
        in_memory_duckdb.execute("CREATE TABLE logs_healpf (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healpf VALUES (?, 'US'), (?, 'US'), (?, 'US')",
            [
                closed_dt + timedelta(minutes=5),
                closed_dt + timedelta(minutes=15),
                closed_dt + timedelta(minutes=25),
            ],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healpf")
        pf_dir = tmp_path / "rollups" / "hour" / "field=country" / f"hour={hour_token}"
        pf_dir.mkdir(parents=True)
        pq.write_table(
            pa.table({"value": ["US"], "count": pa.array([5], type=pa.int64())}),
            str(pf_dir / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = closed_dt.isoformat()
        et = (closed_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healpf")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert counts.get("US") == 5, (
            f"per-field-covered hour was double-counted by the live heal (expected 5). Got {counts}"
        )

    def test_execute_top_n_rollups_heal_failure_never_raises(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """Best-effort contract: a heal-internal failure degrades to the
        pre-fallback behavior (rollups + active hour only) — it must never
        propagate and 500 the aggregates request."""
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner as _QR

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        bundled_dt = active_dt - timedelta(hours=3)
        in_memory_duckdb.execute("CREATE TABLE logs_healfail (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healfail VALUES (?, 'US')",
            [closed_dt + timedelta(minutes=10)],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healfail")
        # A bundled hour so the rollup path has something to return even
        # when the heal explodes.
        bundle_dir = tmp_path / "rollups" / "hour_bundled" / f"hour={bundled_dt.strftime('%Y-%m-%d-%H')}"
        bundle_dir.mkdir(parents=True)
        pq.write_table(
            pa.table({"field": ["country"], "value": ["JP"], "count": pa.array([7], type=pa.int64())}),
            str(bundle_dir / "all_fields.parquet"),
        )
        monkeypatch.setattr(
            _QR,
            "create_filtered_temp_table",
            lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("temp build exploded")),
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = bundled_dt.isoformat()
        et = (closed_dt + timedelta(hours=1)).isoformat()
        # Must not raise despite the heal's temp builder exploding.
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healfail")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert counts.get("JP") == 7, f"rollup results must survive a failed heal. Got {counts}"
        assert "US" not in counts, "the failed heal cannot have contributed rows"

    def test_execute_top_n_rollups_sentinel_bundle_suppresses_heal(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """A verified-empty hour (empty sentinel bundle stamped by the heal
        cron) counts as COVERED: the reader must not classify it as a
        writer gap, so no heal temp table is ever built. This is the
        steady-state zero-cost contract for quiet hours on bursty
        services."""
        from datetime import UTC, datetime, timedelta

        from backend.core.rollups.hour_bundles import stamp_empty_hour_sentinels
        from backend.repositories._base import QueryRunner as _QR

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        empty_dt = active_dt - timedelta(hours=2)
        in_memory_duckdb.execute("CREATE TABLE logs_healsent (timestamp TIMESTAMPTZ, country VARCHAR)")
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healsent")
        stamped = stamp_empty_hour_sentinels(
            "test_service",
            {"name": "test_service"},
            [empty_dt.strftime("%Y-%m-%d-%H")],
        )
        assert stamped == 1

        calls: list = []
        _orig = _QR.create_filtered_temp_table
        monkeypatch.setattr(
            _QR,
            "create_filtered_temp_table",
            lambda self, *a, **k: (calls.append(a), _orig(self, *a, **k))[1],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = empty_dt.isoformat()
        et = (empty_dt + timedelta(hours=1)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healsent")

        assert calls == [], "sentinel-covered hour must not trigger the heal's temp build"
        assert not [r for r in rows if r[0] == "country"], "sentinel hour contributes zero rows"

    def test_execute_top_n_rollups_heal_rescues_crashed_compaction_day_dir(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        """An EMPTY day dir (crashed compaction left the dir but no
        parquet) must NOT count as day-compacted — dir presence alone
        would permanently exclude the whole day from the heal and
        re-create the silent undercount this fallback exists to close."""
        from datetime import UTC, datetime, timedelta

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        crashed_hour_dt = active_dt - timedelta(hours=26)
        day_token = crashed_hour_dt.strftime("%Y-%m-%d")
        in_memory_duckdb.execute("CREATE TABLE logs_healcrash (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healcrash VALUES (?, 'US')",
            [crashed_hour_dt + timedelta(minutes=10)],
        )
        QueryRunner = self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healcrash")
        # Dir exists, NO parquet inside — the crashed-compaction state.
        (tmp_path / "rollups" / "day" / "field=country" / f"day={day_token}").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = (crashed_hour_dt - timedelta(hours=1)).isoformat()
        et = (crashed_hour_dt + timedelta(hours=2)).isoformat()
        rows, _ = runner.execute_top_n_rollups(["country"], st, et, limit=10)
        in_memory_duckdb.execute("DROP TABLE logs_healcrash")

        counts = {value: count for (field, value, count) in rows if field == "country"}
        assert counts.get("US") == 1, (
            f"empty day dir wrongly treated as compacted — the heal must rescue it. Got {counts}"
        )


class TestActiveHourDirectTempSchemaDrift:
    def test_mixed_buffer_schemas_fall_back_to_union_by_name(self, in_memory_duckdb, tmp_path, monkeypatch):
        """The fast path reads multi-file parquet WITHOUT union_by_name (the
        per-file full-schema reconciliation dominated the temp create on
        prod). When buffer files drift — e.g. a new column mid-deploy —
        DuckDB raises rather than mis-binding, and the retry with
        union_by_name=true must still build the temp with every row."""
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir(parents=True)
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        ts = active_start + timedelta(minutes=5)

        pq.write_table(
            pa.table({"timestamp": pa.array([ts], type=pa.timestamp("us", tz="UTC"))}),
            str(buffer_dir / "old_schema.parquet"),
        )
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([ts, ts], type=pa.timestamp("us", tz="UTC")),
                    "brand_new_col": pa.array(["x", "y"], type=pa.string()),
                }
            ),
            str(buffer_dir / "new_schema.parquet"),
        )

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        runner = QueryRunner(in_memory_duckdb, {"name": "drift_svc"})
        tmp = runner._create_active_hour_temp_direct([], ["timestamp"], active_start, active_start + timedelta(hours=1))

        assert tmp is not None, "drifted buffer schemas must retry with union_by_name, not fail"
        assert in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0] == 3
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')


class TestActiveHourDirectTempTombstones:
    def test_tombstoned_buffer_files_are_not_double_counted(self, in_memory_duckdb, tmp_path, monkeypatch):
        """A tombstoned buffer parquet's rows were ALREADY committed into the
        hourly partition this read also scans — the file only lingers for the
        sweep grace window. The direct read must count those rows exactly
        ONCE (via the hourly partition), never twice. Pre-fix, every
        active-hour live slice double-counted up to ~10 min of the freshest
        rows (prod 2026-07-07: 40 of 55 buffer files were tombstoned)."""
        from datetime import UTC, datetime, timedelta

        import pyarrow as pa
        import pyarrow.parquet as pq

        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        buffer_dir = cache_root / "buffer"
        buffer_dir.mkdir(parents=True)
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hourly_dir = cache_root / "data" / f"timestamp_hour={active_start.strftime('%Y-%m-%d-%H')}"
        hourly_dir.mkdir(parents=True)
        ts = active_start + timedelta(minutes=5)

        def _write(path, n):
            pq.write_table(
                pa.table({"timestamp": pa.array([ts] * n, type=pa.timestamp("us", tz="UTC"))}),
                str(path),
            )

        # Live (un-tombstoned) buffer file: 2 rows.
        _write(buffer_dir / "batch_live.parquet", 2)
        # Tombstoned buffer file: 3 rows + its .consumed-<ts> marker...
        _write(buffer_dir / "batch_committed.parquet", 3)
        (buffer_dir / "batch_committed.parquet.consumed-1234567890").touch()
        # ...whose rows the commit already landed in the hourly partition.
        _write(hourly_dir / "00000-0-committed.parquet", 3)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        runner = QueryRunner(in_memory_duckdb, {"name": "tombstone_svc"})
        tmp = runner._create_active_hour_temp_direct([], ["timestamp"], active_start, active_start + timedelta(hours=1))

        assert tmp is not None
        n = in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0]
        assert n == 5, f"expected live(2) + committed-via-hourly(3) = 5 rows, got {n} (8 = double count)"
        # Telemetry counts only the files actually opened (2, not 3).
        assert runner._last_active_direct_n_files == 2
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')
