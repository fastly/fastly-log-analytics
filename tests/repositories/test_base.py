"""Tests for shared repository utilities in backend/repositories/_base.py."""

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import time_machine

from backend.repositories import _base
from backend.repositories._base import (
    _EMPTY_ROLLUP_WARN_TS,
    _LIVE_TOPN_SKIP_FIELDS,
    QueryRunner,
    _is_stale_view_error,
    optional_col,
    safe_iso,
)
from backend.utils.hll import HyperLogLog


class TestSafeIso:
    @pytest.mark.parametrize(
        "val,check",
        [
            (None, lambda r: r is None),
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), lambda r: isinstance(r, str) and "2024-06-15" in r),
            (datetime(2024, 6, 15, 12, 0, 0), lambda r: isinstance(r, str) and r.endswith("Z")),
            ("2024-06-15T12:00:00Z", lambda r: r == "2024-06-15T12:00:00Z"),
            (42, lambda r: r == "42"),
        ],
    )
    def test_safe_iso_variants(self, val, check):
        assert check(safe_iso(val))


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
        assert _is_stale_view_error(getattr(duckdb, exc_cls)(msg)) is True

    @pytest.mark.parametrize(
        "msg",
        ["Syntax error near SELECT", "Column 'foo' not found", "Out of memory", ""],
    )
    def test_non_stale_messages_return_false(self, msg):
        assert _is_stale_view_error(duckdb.IOException(msg)) is False
        assert _is_stale_view_error(Exception(msg)) is False

    def test_substring_in_non_duckdb_exception_does_not_trigger_rebuild(self):
        spoofed = duckdb.ConversionException("Conversion failed: value 'No files found' is not a valid INTEGER")
        assert _is_stale_view_error(spoofed) is False
        binder = duckdb.BinderException("Binder Error: Table 'fake' does not exist")
        assert _is_stale_view_error(binder) is False


class TestQueryRunner:
    def test_conn_requests_hist_uses_all_fields_rollup_when_specialized_bundle_is_missing(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        path = tmp_path / "all_fields.parquet"
        pq.write_table(
            pa.table(
                {
                    "field": ["conn_requests", "conn_requests", "conn_requests"],
                    "value": ["1", "7", "22"],
                    "count": [3, 2, 4],
                }
            ),
            path,
        )
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        paths = iter([None, [str(path)]])
        monkeypatch.setattr(runner, "_collect_rollup_paths", lambda *args, **kwargs: next(paths))

        result = runner.try_conn_requests_hist_from_rollup(
            "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", has_filters=False, actual_cols=["conn_requests"]
        )
        assert result == {
            "top": [{"value": "1", "count": 3}, {"value": "6–20", "count": 2}, {"value": "21+", "count": 4}],
            "total": 9,
        }

    def test_execute_tracks_query_and_returns_cursor(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        initial_count = len(runner.debug_queries)
        res = runner.execute("SELECT 42 AS n")
        assert res.fetchone()[0] == 42
        assert len(runner.debug_queries) == initial_count + 1
        last_q = runner.debug_queries[-1]
        assert "SELECT 42" in last_q["sql"] and isinstance(last_q["time_ms"], float)

    def test_execute_self_heals_on_stale_view(self, test_service_source):
        attempts = {"n": 0}

        def flaky_execute(sql, params=None):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise duckdb.IOException("IO Error: No files found that match the pattern .../buffer/batch_x.parquet")
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
        assert attempts["n"] == 2 and refresh_calls["n"] == 1 and refresh_calls["force_seen"] is True
        assert res.fetchone()[0] == 1

    def test_execute_reraises_non_stale_and_refresh_failure_errors(self, test_service_source):
        attempts = {"n": 0}

        def always_fail(sql, params=None):
            attempts["n"] += 1
            raise Exception("Parser Error: syntax error at or near 'BLOOP'")

        fake_con = MagicMock()
        fake_con.execute.side_effect = always_fail
        runner = QueryRunner(fake_con, test_service_source)
        with pytest.raises(Exception, match="Parser Error"):
            runner.execute("BLOOP")
        assert attempts["n"] == 1

        fake_con.execute.side_effect = duckdb.IOException("IO Error: No files found at path")
        with patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("rebind failed")):
            with pytest.raises(Exception, match="No files found"):
                runner.execute("SELECT 1")

    def test_execute_with_retry_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert runner.execute_with_retry("SELECT 99 AS n").fetchone()[0] == 99

        stale_error = duckdb.IOException("No files found in the given path")
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
            runner.execute_with_retry("SELECT 1")
        assert call_count["n"] == 2

        with patch.object(runner, "execute", side_effect=Exception("Syntax error near SELECT")):
            with pytest.raises(Exception, match="Syntax error"):
                runner.execute_with_retry("SELECT bad syntax !!!")

    def test_create_temp_table_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        ok = runner.create_temp_table("CREATE TEMP TABLE _test_runner_tmp AS SELECT 1 AS x")
        in_memory_duckdb.execute("DROP TABLE IF EXISTS _test_runner_tmp")
        assert ok is True

        stale_error = duckdb.IOException("No files found in the given path")
        with (
            patch.object(runner, "execute", side_effect=stale_error),
            patch("backend.core.iceberg.update_iceberg_view"),
        ):
            assert runner.create_temp_table("CREATE TEMP TABLE _unreachable AS SELECT 1") is False

    def test_get_schema_cols_and_self_heal_cache_bust(self, in_memory_duckdb, test_service_source):
        assert isinstance(QueryRunner(in_memory_duckdb, test_service_source).get_schema_cols(), list)

        runner = QueryRunner(MagicMock(), test_service_source)
        call_order: list[str] = []
        get_schema_calls = {"n": 0}

        def fake_get_schema(con, src):
            get_schema_calls["n"] += 1
            return [] if get_schema_calls["n"] == 1 else [{"name": "timestamp"}, {"name": "ip"}, {"name": "status"}]

        with (
            patch("backend.repositories._base._get_schema", side_effect=fake_get_schema),
            patch(
                "backend.core.iceberg.clear_source_caches",
                side_effect=lambda _k, keep_snapshot_cache=False: call_order.append(
                    f"clear_source_caches(keep_snapshot_cache={keep_snapshot_cache})"
                ),
            ),
            patch(
                "backend.core.iceberg.update_iceberg_view",
                side_effect=lambda con, src, force=False, lock_timeout=5.0: call_order.append(
                    f"update_iceberg_view(force={force})"
                ),
            ),
        ):
            cols = runner.get_schema_cols()

        assert call_order == ["clear_source_caches(keep_snapshot_cache=True)", "update_iceberg_view(force=True)"]
        assert cols == ["timestamp", "ip", "status"]


class TestOptionalColAndFilteredTempTable:
    @pytest.mark.parametrize(
        "col,actual,default,expected",
        [
            ("ottlb", {"ottlb", "ottfb"}, "NULL", '"ottlb"'),
            ("obytes", {"ottlb"}, "NULL", "NULL"),
            ("elapsed", set(), "0", "0"),
            ("ottfb", ["ottfb", "ottlb"], "NULL", '"ottfb"'),
            ("obytes", ["ottlb"], "NULL", "NULL"),
        ],
    )
    def test_optional_col(self, col, actual, default, expected):
        assert optional_col(col, actual, default=default) == expected

    def test_create_filtered_temp_table_behaviors(self, in_memory_duckdb, test_service_source):
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert (
            runner.create_filtered_temp_table(
                cols=["nonexistent"], actual_cols=["ts", "status"], source_table="any_table", where_clause="1=1"
            )
            is None
        )

        in_memory_duckdb.execute("CREATE TABLE src_test (ts VARCHAR, status INT)")
        in_memory_duckdb.execute("INSERT INTO src_test VALUES ('2024-01-01', 200)")
        try:
            name = runner.create_filtered_temp_table(
                cols=["ts", "status", "missing_col"],
                actual_cols=["ts", "status"],
                source_table="src_test",
                where_clause="1=1",
            )
            assert name is not None and name.startswith("t_")
            assert in_memory_duckdb.execute(f"SELECT status FROM {name}").fetchone()[0] == 200
            cols = [d[0] for d in in_memory_duckdb.execute(f"DESCRIBE {name}").fetchall()]
            assert "ts" in cols and "status" in cols and "missing_col" not in cols
        finally:
            in_memory_duckdb.execute("DROP TABLE src_test")


class TestExecuteTopNBatchTypesAndDrift:
    @pytest.mark.parametrize(
        "col,col_type,schema_type,values_sql,expected_buckets",
        [
            (
                "ttl",
                "FLOAT",
                "FLOAT",
                "(3600.027), (3600.028), (3600.029), (3600.030), (3601.001)",
                {"3600": 4, "3601": 1},
            ),
            ("age", "FLOAT", "FLOAT", "(0.0), (0.0), (1.0), (1.0), (1.0), (2.0)", {"0": 2, "1": 3, "2": 1}),
            ("status", "INTEGER", "VARCHAR", "(200), (200), (404), (500), (NULL)", {"200": 2, "404": 1, "500": 1}),
            ("country", "VARCHAR", "VARCHAR", "('US'), ('US'), (''), (NULL), ('CA')", {"US": 2, "CA": 1}),
        ],
    )
    def test_top_n_batch_rounding_and_schema_drift(
        self, in_memory_duckdb, test_service_source, col, col_type, schema_type, values_sql, expected_buckets
    ):
        tbl = f"logs_{col}_test"
        in_memory_duckdb.execute(f"CREATE TABLE {tbl} ({col} {col_type})")
        in_memory_duckdb.execute(f"INSERT INTO {tbl} VALUES {values_sql}")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, order = runner.execute_top_n_batch(
                fields=[col], table_name=tbl, actual_cols=[col], schema_types={col: schema_type}
            )
            assert order == [col]
            buckets = {value: count for (_field, value, count) in rows}
            assert buckets == expected_buckets
        finally:
            in_memory_duckdb.execute(f"DROP TABLE {tbl}")

    def test_non_int_aggregate_float_field_is_not_rounded(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_rtt (tcp_rtt DOUBLE)")
        in_memory_duckdb.execute("INSERT INTO logs_rtt VALUES (0.012), (0.013), (0.013)")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_batch(
                fields=["tcp_rtt"], table_name="logs_rtt", actual_cols=["tcp_rtt"], schema_types={"tcp_rtt": "DOUBLE"}
            )
            buckets = {value: count for (_field, value, count) in rows}
            assert "." in next(iter(buckets)) and buckets.get("0.013") == 2
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_rtt")


class TestExecuteTopNBatchPerFieldLimits:
    @staticmethod
    def _patch_rollup_env(
        monkeypatch,
        cache_root,
        cols: list | tuple = ("country",),
        *,
        safe_table: str = "dummy",
        disable_direct: bool = False,
        ensure_hour_dir: bool = True,
    ) -> None:
        schema_defs: list[dict[str, str]] = [{"name": "timestamp", "type": "TIMESTAMP WITH TIME ZONE"}]
        col_names: list[str] = ["timestamp"]
        for item in cols:
            if isinstance(item, tuple):
                col_names.append(item[0])
                schema_defs.append({"name": item[0], "type": item[1]})
            elif item != "timestamp":
                col_names.append(item)
                schema_defs.append({"name": item, "type": "VARCHAR"})
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        monkeypatch.setattr("backend.core.rollups._safe_table_for", lambda _src: safe_table)
        monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: list(col_names))
        monkeypatch.setattr("backend.repositories._base._get_schema", lambda _con, _src: list(schema_defs))
        if disable_direct:
            monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
        if ensure_hour_dir:
            (cache_root / "rollups" / "hour").mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _seed_partial_hour_country_fixture(tmp_path, active_dt, active_hour: str) -> None:
        hourly_dir = tmp_path / "data" / f"timestamp_hour={active_hour}"
        hourly_dir.mkdir(parents=True)
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone='UTC';")
            con.execute("CREATE TABLE t (timestamp TIMESTAMP, country VARCHAR)")
            con.executemany(
                "INSERT INTO t VALUES (?, ?)",
                [(active_dt + timedelta(minutes=1), "DE"), (active_dt + timedelta(minutes=1), "DE")],
            )
            con.execute(f"COPY t TO '{hourly_dir / 'batch1.parquet'}' (FORMAT PARQUET)")
        finally:
            con.close()
        watermark_time = (active_dt + timedelta(minutes=15)).timestamp()
        os.utime(hourly_dir / "batch1.parquet", (watermark_time, watermark_time))

    @staticmethod
    def _write_per_hour(cache_root: Path, field: str, hour: str, rows: list[tuple]) -> None:
        d = cache_root / "rollups" / "hour" / f"field={field}" / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            ),
            str(d / f"compacted_{uuid.uuid4().hex[:8]}.parquet"),
        )

    @staticmethod
    def _write_per_day(cache_root: Path, field: str, day: str, rows: list[tuple]) -> None:
        d = cache_root / "rollups" / "day" / f"field={field}" / f"day={day}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "field": pa.array([field for _ in rows]),
                    "value": pa.array([v for v, _ in rows]),
                    "count": pa.array([c for _, c in rows], type=pa.int64()),
                }
            ),
            str(d / "compacted.parquet"),
        )

    @staticmethod
    def _write_bundled_hour(cache_root: Path, hour: str, rows: list[tuple[str, str, int]]) -> None:
        bd = cache_root / "rollups" / "hour_bundled" / f"hour={hour}"
        bd.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "field": [f for f, _, _ in rows],
                    "value": [v for _, v, _ in rows],
                    "count": pa.array([c for _, _, c in rows], type=pa.int64()),
                }
            ),
            str(bd / "all_fields.parquet"),
        )

    @pytest.mark.parametrize(
        "limit,per_field_limits,expected_country,expected_ua",
        [
            (2, {"country": 5}, 5, 2),
            (3, None, 3, 3),
        ],
    )
    def test_per_field_limits_behavior(
        self, in_memory_duckdb, test_service_source, limit, per_field_limits, expected_country, expected_ua
    ):
        in_memory_duckdb.execute("CREATE TABLE logs_pfl (country VARCHAR, ua VARCHAR)")
        rows_sql = ", ".join(f"('c{i}', 'u{i}')" for i in range(10))
        in_memory_duckdb.execute(f"INSERT INTO logs_pfl VALUES {rows_sql}")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, order = runner.execute_top_n_batch(
                fields=["country", "ua"],
                table_name="logs_pfl",
                actual_cols=["country", "ua"],
                schema_types={"country": "VARCHAR", "ua": "VARCHAR"},
                limit=limit,
                per_field_limits=per_field_limits,
            )
            assert order == ["country", "ua"]
            assert len([r for r in rows if r[0] == "country"]) == expected_country
            assert len([r for r in rows if r[0] == "ua"]) == expected_ua
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_pfl")

    @pytest.mark.parametrize(
        "cols",
        [
            ["country"],
            ["country", "timestamp_hour", "dt"],
        ],
    )
    def test_execute_top_n_rollups_uses_direct_active_hour_fast_path(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, cols
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        cache_root = tmp_path / "cache"
        (cache_root / "buffer").mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([active_dt + timedelta(minutes=5)], type=pa.timestamp("us", tz="UTC")),
                    "country": pa.array(["US"]),
                }
            ),
            str(cache_root / "buffer" / "batch_test.parquet"),
        )
        self._patch_rollup_env(monkeypatch, cache_root, cols)

        direct_calls = {"n": 0}
        orig_direct = QueryRunner._create_active_hour_temp_direct
        monkeypatch.setattr(
            QueryRunner,
            "_create_active_hour_temp_direct",
            lambda self, *a, **kw: (direct_calls.__setitem__("n", direct_calls["n"] + 1), orig_direct(self, *a, **kw))[
                1
            ],
        )
        view_fallback_calls = {"n": 0}
        orig_view = QueryRunner.create_filtered_temp_table
        monkeypatch.setattr(
            QueryRunner,
            "create_filtered_temp_table",
            lambda self, *a, **kw: (
                view_fallback_calls.__setitem__("n", view_fallback_calls["n"] + 1),
                orig_view(self, *a, **kw),
            )[1],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        rows, _ = runner.execute_top_n_rollups(
            ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
        )
        assert direct_calls["n"] == 1 and view_fallback_calls["n"] == 0
        assert ("country", "US", 1) in [r for r in rows if r[0] == "country"]

    def test_shared_active_hour_temps_reused_between_count_and_top_n(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        cache_root = tmp_path / "cache"
        (cache_root / "buffer").mkdir(parents=True)
        pq.write_table(
            pa.table(
                {
                    "timestamp": pa.array([active_dt + timedelta(minutes=5)], type=pa.timestamp("us", tz="UTC")),
                    "country": pa.array(["US"]),
                    "status": pa.array([200]),
                }
            ),
            str(cache_root / "buffer" / "batch_test.parquet"),
        )
        self._patch_rollup_env(monkeypatch, cache_root, ["country", ("status", "INTEGER"), "timestamp_hour", "dt"])
        (cache_root / "rollups" / "hour_bundled").mkdir(parents=True)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_end = active_dt + timedelta(hours=1)
        runner.begin_shared_active_hour_temps()
        try:
            cnt = runner.try_count_from_rollup(
                active_dt.isoformat(),
                active_end.isoformat(),
                "dummy",
                "1=1",
                [],
                unfiltered_window=True,
                projected_fields=["country", "status"],
                actual_cols=["timestamp", "country", "status", "timestamp_hour", "dt"],
            )
            assert cnt == 1 and runner._shared_active_temps is not None and len(runner._shared_active_temps) == 1
            shared_table_name = runner._shared_active_temps[0][3]

            view_fallback_calls = {"n": 0}
            orig_view = QueryRunner.create_filtered_temp_table
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **kw: (
                    view_fallback_calls.__setitem__("n", view_fallback_calls["n"] + 1),
                    orig_view(self, *a, **kw),
                )[1],
            )

            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                active_dt.isoformat(),
                active_end.isoformat(),
                limit=10,
                actual_cols=["timestamp", "country", "status", "timestamp_hour", "dt"],
            )
            assert len(runner._shared_active_temps) == 1 and runner._shared_active_temps[0][3] == shared_table_name
            assert runner._last_active_direct_n_files == 0 and view_fallback_calls["n"] == 0
            assert ("country", "US", 1) in [r for r in rows if r[0] == "country"]
        finally:
            runner.end_shared_active_hour_temps()

    def test_execute_top_n_rollups_falls_back_to_view_when_direct_finds_nothing(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        self._patch_rollup_env(monkeypatch, cache_root, ["country"])
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        rows, _ = runner.execute_top_n_rollups(
            ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
        )
        assert [r for r in rows if r[0] == "country"] == []

    def test_execute_top_n_rollups_live_branch_and_clamp_and_skip_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        assert "rid" in _LIVE_TOPN_SKIP_FIELDS and "country" not in _LIVE_TOPN_SKIP_FIELDS
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute("CREATE TABLE logs_live (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_live VALUES (?, 'US', 'r1'), (?, 'US', 'r2'), (?, 'JP', 'r3')",
            [active_dt + timedelta(minutes=10), active_dt + timedelta(minutes=30), active_dt + timedelta(minutes=45)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid"], safe_table="logs_live", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # Full active hour: live branch runs, rid skipped
            rows_full, _ = runner.execute_top_n_rollups(
                ["country", "rid"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            country_full = {v: c for f, v, c in rows_full if f == "country"}
            assert country_full == {"US": 2, "JP": 1}
            assert [v for f, v, _ in rows_full if f == "rid"] == []

            # Clamped sub-window [+5m, +35m]: excludes +45m JP row
            rows_clamped, _ = runner.execute_top_n_rollups(
                ["country"],
                (active_dt + timedelta(minutes=5)).isoformat(),
                (active_dt + timedelta(minutes=35)).isoformat(),
                limit=10,
            )
            country_clamped = {v: c for f, v, c in rows_clamped if f == "country"}
            assert country_clamped == {"US": 2} and "JP" not in country_clamped
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_live")

    @pytest.mark.parametrize("seed_pre_watermark_de", [False, True])
    def test_execute_top_n_rollups_partial_hour_merge_and_no_double_count(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, seed_pre_watermark_de
    ):
        from backend.core.rollups import partial_hour as ph

        frozen_now = datetime(2026, 6, 20, 10, 20, 0, tzinfo=UTC)
        active_dt = datetime(2026, 6, 20, 10, 0, 0, tzinfo=UTC)
        active_hour = "2026-06-20-10"
        tbl = "logs_pht_test"

        self._patch_rollup_env(monkeypatch, tmp_path, ["country"], safe_table=tbl, disable_direct=True)
        self._seed_partial_hour_country_fixture(tmp_path, active_dt, active_hour)

        with time_machine.travel(frozen_now, tick=False):
            stats = ph.merge_partial_hour("svc-a", test_service_source, ["country"])
            assert stats["new_files"] == 1

            in_memory_duckdb.execute(f"CREATE TABLE {tbl} (timestamp TIMESTAMPTZ, country VARCHAR)")
            try:
                now = datetime.now(UTC)
                if seed_pre_watermark_de:
                    in_memory_duckdb.execute(
                        f"INSERT INTO {tbl} VALUES (?, 'DE'), (?, 'DE'), (?, 'US'), (?, 'US')",
                        [
                            active_dt + timedelta(seconds=1),
                            active_dt + timedelta(seconds=1),
                            now + timedelta(minutes=1),
                            now + timedelta(minutes=1),
                        ],
                    )
                else:
                    in_memory_duckdb.execute(
                        f"INSERT INTO {tbl} VALUES (?, 'US'), (?, 'US')",
                        [now + timedelta(minutes=1), now + timedelta(minutes=1)],
                    )

                runner = QueryRunner(in_memory_duckdb, test_service_source)
                rows, _ = runner.execute_top_n_rollups(
                    ["country"], active_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
                )
            finally:
                in_memory_duckdb.execute(f"DROP TABLE {tbl}")

        country_counts = {value: count for (field, value, count) in rows if field == "country"}
        assert country_counts.get("US") == 2 and country_counts.get("DE") == 2

    def test_execute_top_n_rollups_rollup_path_still_returns_skip_field(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_hour_dt = active_dt - timedelta(hours=1)
        closed_hour_str = closed_hour_dt.strftime("%Y-%m-%d-%H")

        for fld, val, cnt in [("rid", "r_rolled", 7), ("country", "US", 5)]:
            self._write_per_hour(tmp_path, fld, closed_hour_str, [(val, cnt)])

        in_memory_duckdb.execute("CREATE TABLE logs_rollupkeep (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_rollupkeep VALUES (?, 'US', 'r_live'), (?, 'US', 'r_live')",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid"], safe_table="logs_rollupkeep", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_rollups(
                ["country", "rid"], closed_hour_dt.isoformat(), (active_dt + timedelta(hours=1)).isoformat(), limit=10
            )
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_rollupkeep")

        by_field: dict[str, dict] = {}
        for fld, val, cnt in rows:
            by_field.setdefault(fld, {})[val] = cnt
        assert by_field.get("country", {}).get("US") == 7
        assert by_field.get("rid", {}).get("r_rolled") == 7
        assert "r_live" not in by_field.get("rid", {})

    def test_execute_top_n_rollups_skip_field_with_no_data_does_not_warn(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        _EMPTY_ROLLUP_WARN_TS.clear()
        monkeypatch.setattr(_base, "_EMPTY_ROLLUP_WARN_INTERVAL_S", 0.0)
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        in_memory_duckdb.execute(
            "CREATE TABLE logs_skipwarn (timestamp TIMESTAMPTZ, country VARCHAR, rid VARCHAR, status VARCHAR)"
        )
        in_memory_duckdb.execute(
            "INSERT INTO logs_skipwarn VALUES (?, 'US', NULL, NULL), (?, 'JP', NULL, NULL)",
            [active_dt + timedelta(minutes=5), active_dt + timedelta(minutes=15)],
        )
        try:
            self._patch_rollup_env(
                monkeypatch, tmp_path, ["country", "rid", "status"], safe_table="logs_skipwarn", disable_direct=True
            )
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            captured: list[str] = []
            monkeypatch.setattr(
                _base._logger, "debug", lambda msg, *args, **_kw: captured.append(msg % args if args else str(msg))
            )
            runner.execute_top_n_rollups(
                ["country", "rid", "status"],
                active_dt.isoformat(),
                (active_dt + timedelta(hours=1)).isoformat(),
                limit=10,
            )
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_skipwarn")

        warnings = [m for m in captured if "empty result for field=" in m]
        assert not any("field='rid'" in m for m in warnings)
        assert any("field='status'" in m for m in warnings)

    def test_live_topn_skip_fields_are_not_rendered_dashboard_panels(self):
        repo_root = Path(__file__).resolve().parents[2]
        sections_dir = repo_root / "frontend" / "app" / "dashboard" / "_sections"
        categories_ts = sections_dir / "categories.ts"
        target_file = (
            categories_ts if categories_ts.is_file() else repo_root / "frontend" / "hooks" / "useDashboardCards.ts"
        )
        assert target_file.is_file()
        text = target_file.read_text()
        rendered: set[str] = set()
        for block in re.findall(r"cardIds:\s*\[(.*?)\]", text, re.DOTALL):
            rendered.update(re.findall(r"'([^']+)'", block))
        assert "country" in rendered and len(rendered) > 30
        assert not (_LIVE_TOPN_SKIP_FIELDS & rendered)

    def test_execute_top_n_rollups_skips_day_file_on_partial_window(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_plus_1 = day_d + timedelta(days=1)
        day_d_str, day_d_plus_1_str = day_d.isoformat(), day_d_plus_1.isoformat()

        self._write_per_day(cache_root, "edge_score", day_d_str, [("in_window_val", 10), ("out_of_window_val", 99)])
        for h in range(17, 24):
            self._write_per_hour(cache_root, "edge_score", f"{day_d_str}-{h:02d}", [("in_window_val", 1)])
        for h in range(0, 18):
            self._write_per_hour(cache_root, "edge_score", f"{day_d_plus_1_str}-{h:02d}", [("in_window_val", 1)])

        self._patch_rollup_env(monkeypatch, cache_root, ["edge_score"], ensure_hour_dir=False)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st = (datetime.combine(day_d, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)).isoformat()
        et = (
            datetime.combine(day_d_plus_1, datetime.min.time(), tzinfo=UTC) + timedelta(hours=17, minutes=36)
        ).isoformat()
        rows, _ = runner.execute_top_n_rollups(["edge_score"], st, et, limit=10)
        values = {value: count for (field, value, count) in rows if field == "edge_score"}
        assert "out_of_window_val" not in values and values.get("in_window_val", 0) > 0

    def test_execute_top_n_rollups_day_vs_bundled_and_per_field_resolution(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d = (active_dt - timedelta(days=2)).date()
        day_d_str = day_d.isoformat()
        day_d_plus_1_str = (day_d + timedelta(days=1)).isoformat()

        # Field A has day file (50); Field B has no day file, only per-hour (24 * 3 = 72); both have bundled-hour (2, 3)
        self._write_per_day(cache_root, "field_a", day_d_str, [("a1", 50)])
        for h_idx in range(24):
            self._write_per_hour(cache_root, "field_b", f"{day_d_str}-{h_idx:02d}", [("b1", 3)])
            self._write_per_hour(cache_root, "field_a", f"{day_d_str}-{h_idx:02d}", [("a1", 2)])
            self._write_bundled_hour(
                cache_root, f"{day_d_str}-{h_idx:02d}", [("field_a", "a1", 2), ("field_b", "b1", 3)]
            )

        self._patch_rollup_env(monkeypatch, cache_root, ["field_a", "field_b"], ensure_hour_dir=False)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st, et = f"{day_d_str}T00:00:00+00:00", f"{day_d_plus_1_str}T00:00:00+00:00"

        # Single-field day-contained read uses day file without double-counting bundled or per-hour
        rows_a, _ = runner.execute_top_n_rollups(["field_a"], st, et, limit=10)
        assert {v: c for f, v, c in rows_a if f == "field_a"} == {"a1": 50}

        # Multi-field read where field_a has day file and field_b falls back to per-field per-hour
        rows_both, _ = runner.execute_top_n_rollups(["field_a", "field_b"], st, et, limit=10)
        by_field: dict[str, dict] = {}
        for f, v, c in rows_both:
            by_field.setdefault(f, {})[v] = c
        assert by_field.get("field_a", {}).get("a1") == 50
        assert by_field.get("field_b", {}).get("b1") == 72

    def test_execute_top_n_rollups_bundled_branch_scoping_and_per_field_qualify(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        day_d_str = (active_dt - timedelta(days=2)).date().isoformat()

        (cache_root / "rollups" / "hour").mkdir(parents=True, exist_ok=True)
        self._write_bundled_hour(
            cache_root, f"{day_d_str}-05", [("ua", "bot-a", 7), ("ua", "bot-b", 3), ("country", "US", 99)]
        )
        self._patch_rollup_env(monkeypatch, cache_root, ["ua", "country"], ensure_hour_dir=False)

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        st, et = f"{day_d_str}T05:00:00+00:00", f"{day_d_str}T06:00:00+00:00"

        # Single-field bundled read scopes to requested field
        rows_ua, _ = runner.execute_top_n_rollups(["ua"], st, et, limit=50000, per_field_limits={"ua": 50000})
        assert {(f, v, c) for f, v, c in rows_ua} == {("ua", "bot-a", 7), ("ua", "bot-b", 3)}

        # Multi-field with per_field_limits uses CASE field QUALIFY
        captured_queries: list[str] = []
        orig_execute = runner.execute
        monkeypatch.setattr(
            runner, "execute", lambda q, *a, **kw: (captured_queries.append(q), orig_execute(q, *a, **kw))[1]
        )
        runner.execute_top_n_rollups(["country", "ua"], st, et, limit=10, per_field_limits={"country": 500})
        rolled_queries = [q for q in captured_queries if "GROUP BY field, value QUALIFY" in q]
        assert rolled_queries
        q = rolled_queries[0]
        assert "CASE field" in q and "THEN 500" in q and "ELSE 100 END" in q and q.count("THEN") == 1

    # ── execute_ip_spread_rollups ────────────────────────────────────────────

    @staticmethod
    def _write_per_field_ip_spread(cache_root, field, hour, rows):
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

    def test_execute_ip_spread_rollups_empty_and_unsafe_fields(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(tmp_path))
        monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
        runner = QueryRunner(in_memory_duckdb, test_service_source)
        assert runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha", "ja3"], "2026-05-15T00:00:00Z", "2026-05-15T01:00:00Z"
        ) == ({}, {})
        assert runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha'; DROP TABLE foo; --"], "2026-05-15T10:00:00Z", "2026-05-15T11:00:00Z"
        ) == ({}, {})

    def test_execute_ip_spread_rollups_merge_and_capped_and_active_skip(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))

        hll_a, hll_b = HyperLogLog(), HyperLogLog()
        hll_a.update([f"1.1.1.{i}" for i in range(1, 101)])
        hll_b.update([f"1.1.1.{i}" for i in range(51, 151)])
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-10",
            [
                {"value": "abc", "ip_sketch": hll_a.to_bytes(), "ip_count_observed": 100, "sample_capped": False},
                {"value": "capped-fp", "ip_sketch": hll_a.to_bytes(), "ip_count_observed": 5000, "sample_capped": True},
            ],
        )
        self._write_per_field_ip_spread(
            cache_root,
            "tls_ciphers_sha",
            "2026-05-15-11",
            [{"value": "abc", "ip_sketch": hll_b.to_bytes(), "ip_count_observed": 100, "sample_capped": False}],
        )

        runner = QueryRunner(in_memory_duckdb, test_service_source)
        counts, meta = runner.execute_ip_spread_rollups(
            ["tls_ciphers_sha"], "2026-05-15T10:00:00Z", "2026-05-15T12:00:00Z"
        )
        merged_estimate = counts[("tls_ciphers_sha", "abc")]
        assert abs(merged_estimate - 150) < abs(merged_estimate - 200) and abs(merged_estimate - 150) <= 25
        assert meta["tls_ciphers_sha"]["coverage_hours"] == 2
        assert meta["tls_ciphers_sha"]["capped_values"] == 1

        # Active hour skip check
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hll_active, hll_closed = HyperLogLog(), HyperLogLog()
        hll_active.update([f"9.9.9.{i}" for i in range(1, 51)])
        hll_closed.update([f"1.1.1.{i}" for i in range(1, 11)])
        self._write_per_field_ip_spread(
            cache_root,
            "ja3",
            active_dt.strftime("%Y-%m-%d-%H"),
            [{"value": "x", "ip_sketch": hll_active.to_bytes(), "ip_count_observed": 50, "sample_capped": False}],
        )
        self._write_per_field_ip_spread(
            cache_root,
            "ja3",
            (active_dt - timedelta(hours=1)).strftime("%Y-%m-%d-%H"),
            [{"value": "x", "ip_sketch": hll_closed.to_bytes(), "ip_count_observed": 10, "sample_capped": False}],
        )
        counts_act, _ = runner.execute_ip_spread_rollups(
            ["ja3"], (active_dt - timedelta(hours=2)).isoformat(), (active_dt + timedelta(hours=1)).isoformat()
        )
        assert counts_act.get(("ja3", "x"), 0) < 25

    def test_execute_top_n_batch_prevents_sql_injection(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_safe (status VARCHAR)")
        in_memory_duckdb.execute("INSERT INTO logs_safe VALUES ('200'), ('200'), ('500')")
        try:
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            malicious_field = "status' UNION ALL SELECT 'evil' as field, 'payload' as value, 100 as c --"
            rows, order = runner.execute_top_n_batch(
                fields=[malicious_field, "status"],
                table_name="logs_safe",
                actual_cols=["status"],
                schema_types={"status": "VARCHAR"},
            )
            assert order == ["status"] and len(rows) == 2 and all(row[0] == "status" for row in rows)
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_safe")

    # ── Missing-hour live heal ───────────────────────────────────────────────

    def _heal_test_setup(self, in_memory_duckdb, tmp_path, monkeypatch, table):
        in_memory_duckdb.execute("SET TimeZone='UTC'")
        self._patch_rollup_env(monkeypatch, tmp_path, ["country"], safe_table=table, disable_direct=True)
        return QueryRunner

    def test_execute_top_n_rollups_missing_hour_heal_matrix(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)

        # 1. Unrolled closed hour is healed; mid-hour start/end clamps exclude out-of-window rows
        in_memory_duckdb.execute("CREATE TABLE logs_heal (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_heal VALUES (?, 'US'), (?, 'US'), (?, 'JP')",
            [closed_dt + timedelta(minutes=5), closed_dt + timedelta(minutes=15), closed_dt + timedelta(minutes=45)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_heal")
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # Full closed window
            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                (active_dt - timedelta(hours=3)).isoformat(),
                (active_dt - timedelta(hours=1)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows if f == "country"} == {"US": 2, "JP": 1}

            # Mid-hour start (+30m to +60m): only +45m JP inside
            rows_st, _ = runner.execute_top_n_rollups(
                ["country"],
                (closed_dt + timedelta(minutes=30)).isoformat(),
                (closed_dt + timedelta(hours=1)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows_st if f == "country"} == {"JP": 1}

            # Mid-hour end (+0m to +30m): only +5m and +15m US inside
            rows_et, _ = runner.execute_top_n_rollups(
                ["country"],
                closed_dt.isoformat(),
                (closed_dt + timedelta(minutes=30)).isoformat(),
                limit=10,
            )
            assert {v: c for f, v, c in rows_et if f == "country"} == {"US": 2}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_heal")

    @pytest.mark.parametrize("coverage_mode", ["bundled", "per_field"])
    def test_execute_top_n_rollups_heal_does_not_double_count_covered_hours(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, coverage_mode
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        closed_dt = active_dt - timedelta(hours=2)
        hour_token = closed_dt.strftime("%Y-%m-%d-%H")
        in_memory_duckdb.execute("CREATE TABLE logs_healcov (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healcov VALUES (?, 'US'), (?, 'US'), (?, 'US')",
            [closed_dt + timedelta(minutes=5), closed_dt + timedelta(minutes=15), closed_dt + timedelta(minutes=25)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healcov")
            if coverage_mode == "bundled":
                self._write_bundled_hour(tmp_path, hour_token, [("country", "US", 5)])
            else:
                self._write_per_hour(tmp_path, "country", hour_token, [("US", 5)])

            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_rollups(
                ["country"], closed_dt.isoformat(), (closed_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            assert {v: c for f, v, c in rows if f == "country"} == {"US": 5}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healcov")

    @pytest.mark.parametrize("has_day_parquet,expected_us", [(True, None), (False, 1)])
    def test_execute_top_n_rollups_heal_day_compacted_vs_crashed_empty_dir(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch, has_day_parquet, expected_us
    ):
        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        compacted_hour_dt = active_dt - timedelta(hours=26)
        day_token = compacted_hour_dt.strftime("%Y-%m-%d")
        in_memory_duckdb.execute("CREATE TABLE logs_healday (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healday VALUES (?, 'US')", [compacted_hour_dt + timedelta(minutes=10)]
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healday")
            day_dir = tmp_path / "rollups" / "day" / "field=country" / f"day={day_token}"
            day_dir.mkdir(parents=True)
            if has_day_parquet:
                pq.write_table(
                    pa.table({"value": ["US"], "count": pa.array([99], type=pa.int64())}),
                    str(day_dir / "compacted.parquet"),
                )
            runner = QueryRunner(in_memory_duckdb, test_service_source)
            rows, _ = runner.execute_top_n_rollups(
                ["country"],
                (compacted_hour_dt - timedelta(hours=1)).isoformat(),
                (compacted_hour_dt + timedelta(hours=2)).isoformat(),
                limit=10,
            )
            counts = {v: c for f, v, c in rows if f == "country"}
            assert counts.get("US") == expected_us
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healday")

    def test_execute_top_n_rollups_heal_cap_and_failure_and_sentinel(
        self, in_memory_duckdb, test_service_source, tmp_path, monkeypatch
    ):
        from backend.core.rollups.hour_bundles import stamp_empty_hour_sentinels

        active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        beyond_cap_dt = active_dt - timedelta(hours=_base._MISSING_HOUR_HEAL_CAP + 2)
        closed_dt = active_dt - timedelta(hours=2)
        bundled_dt = active_dt - timedelta(hours=3)

        in_memory_duckdb.execute("CREATE TABLE logs_healedge (timestamp TIMESTAMPTZ, country VARCHAR)")
        in_memory_duckdb.execute(
            "INSERT INTO logs_healedge VALUES (?, 'US'), (?, 'US')",
            [beyond_cap_dt + timedelta(minutes=10), closed_dt + timedelta(minutes=10)],
        )
        try:
            self._heal_test_setup(in_memory_duckdb, tmp_path, monkeypatch, "logs_healedge")
            runner = QueryRunner(in_memory_duckdb, test_service_source)

            # 1. Beyond _MISSING_HOUR_HEAL_CAP is not healed
            rows_cap, _ = runner.execute_top_n_rollups(
                ["country"],
                (beyond_cap_dt - timedelta(hours=1)).isoformat(),
                (beyond_cap_dt + timedelta(hours=2)).isoformat(),
                limit=10,
            )
            assert "US" not in {v: c for f, v, c in rows_cap if f == "country"}

            # 2. Sentinel bundle suppresses heal temp creation
            from backend.utils.bounded_cache import CacheRegistry

            empty_dt = active_dt - timedelta(hours=4)
            assert (
                stamp_empty_hour_sentinels("test_service", {"name": "test_service"}, [empty_dt.strftime("%Y-%m-%d-%H")])
                == 1
            )
            CacheRegistry.clear_all()
            calls: list = []
            orig_temp = QueryRunner.create_filtered_temp_table
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **k: (calls.append(a), orig_temp(self, *a, **k))[1],
            )
            rows_sent, _ = runner.execute_top_n_rollups(
                ["country"], empty_dt.isoformat(), (empty_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            assert calls == [] and not [r for r in rows_sent if r[0] == "country"]

            # 3. Heal failure never raises and preserves bundled rollup results
            self._write_bundled_hour(tmp_path, bundled_dt.strftime("%Y-%m-%d-%H"), [("country", "JP", 7)])
            CacheRegistry.clear_all()
            monkeypatch.setattr(
                QueryRunner,
                "create_filtered_temp_table",
                lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("temp build exploded")),
            )
            rows_fail, _ = runner.execute_top_n_rollups(
                ["country"], bundled_dt.isoformat(), (closed_dt + timedelta(hours=1)).isoformat(), limit=10
            )
            counts_fail = {v: c for f, v, c in rows_fail if f == "country"}
            assert counts_fail == {"JP": 7}
        finally:
            in_memory_duckdb.execute("DROP TABLE logs_healedge")


class TestActiveHourDirectTempSchemaDriftAndTombstones:
    def test_mixed_buffer_schemas_fall_back_to_union_by_name(self, in_memory_duckdb, tmp_path, monkeypatch):
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

        assert tmp is not None
        assert in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0] == 3
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')

    def test_tombstoned_buffer_files_are_not_double_counted(self, in_memory_duckdb, tmp_path, monkeypatch):
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

        _write(buffer_dir / "batch_live.parquet", 2)
        _write(buffer_dir / "batch_committed.parquet", 3)
        (buffer_dir / "batch_committed.parquet.consumed-1234567890").touch()
        _write(hourly_dir / "00000-0-committed.parquet", 3)

        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: str(cache_root))
        runner = QueryRunner(in_memory_duckdb, {"name": "tombstone_svc"})
        tmp = runner._create_active_hour_temp_direct([], ["timestamp"], active_start, active_start + timedelta(hours=1))

        assert tmp is not None
        assert in_memory_duckdb.execute(f'SELECT COUNT(*) FROM "{tmp}"').fetchone()[0] == 5
        assert runner._last_active_direct_n_files == 2
        in_memory_duckdb.execute(f'DROP TABLE IF EXISTS "{tmp}"')
