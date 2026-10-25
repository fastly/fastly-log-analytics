"""Unit tests for the top-N window math and filesystem path planning."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NoReturn
from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.core import rollups as rr
from backend.repositories import rollup_coverage
from backend.repositories._base import QueryRunner
from backend.repositories.rollup_coverage import collect_hourly_bundle_paths, heal_where_clause
from backend.repositories.top_n_rollups import (
    SchemaSeeds,
    TopNPaths,
    TopNWindow,
    build_rolled_sql,
    find_unrolled_hours,
    fold_rows,
    live_topn_fields,
    plan_top_n_paths,
    qualify_limit_expr,
    truncate_top_n,
)

# ── Merged from tests/repositories/test_top_n_rollup_plan.py ──
NOW = datetime(2026, 3, 15, 10, 20, tzinfo=UTC)


def _dt(day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 3, day, hour, minute, tzinfo=UTC)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def _rel(root: Path, paths: list[str]) -> list[str]:
    return sorted(os.path.relpath(p, root) for p in paths)


@pytest.fixture
def _fresh_listdir() -> None:
    rollup_coverage._listdir_cache.clear()


@pytest.mark.usefixtures("_fresh_listdir")
class TestTopNWindow:
    def test_bounds_and_active_hour(self) -> None:
        win = TopNWindow.build(_dt(12), _dt(15), NOW)
        assert win.active_str == "2026-03-15-10"
        assert win.active_dt == _dt(15, 10)
        assert win.active_dt_end == _dt(15, 11)
        assert (win.st_floor, win.et_floor) == ("2026-03-12-00", "2026-03-14-23")

    def test_unbounded_sides(self) -> None:
        win = TopNWindow.build(None, None, NOW)
        assert (win.st_floor, win.et_floor) == (None, None)
        assert win.hour_in_range("2000-01-01-00")

    def test_mid_hour_end_keeps_its_hour(self) -> None:
        assert TopNWindow.build(None, _dt(14, 5, 30), NOW).et_floor == "2026-03-14-05"

    def test_active_and_out_of_range_hours_excluded(self) -> None:
        win = TopNWindow.build(_dt(14), _dt(16), NOW)
        assert win.hour_in_range("2026-03-14-00")
        assert not win.hour_in_range("2026-03-13-23")
        assert not win.hour_in_range("2026-03-15-10")
        assert not win.hour_in_range("2026-03-15-11")

    def test_day_fully_in_window(self) -> None:
        win = TopNWindow.build(_dt(12, 6), _dt(14, 12), NOW)
        assert not win.day_fully_in_window("2026-03-12")
        assert win.day_fully_in_window("2026-03-13")
        assert not win.day_fully_in_window("2026-03-14")
        assert not win.day_fully_in_window("garbage")

    def test_day_in_range(self) -> None:
        win = TopNWindow.build(_dt(12, 6), _dt(14, 12), NOW)
        days = ("2026-03-11", "2026-03-12", "2026-03-14", "2026-03-15")
        assert [win.day_in_range(d) for d in days] == [False, True, True, False]


@pytest.mark.usefixtures("_fresh_listdir")
class TestPlanTopNPaths:
    def test_nothing_on_disk(self, tmp_path: Path) -> None:
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(12), _dt(14), NOW))
        assert not plan.paths
        assert plan.bundled_hours == plan.bundled_days == plan.per_field_hours_seen == set()

    def test_per_field_hours_in_window_only(self, tmp_path: Path) -> None:
        for hour in ("2026-03-13-23", "2026-03-14-00", "2026-03-14-01", "2026-03-14-02", "2026-03-15-10"):
            _touch(tmp_path / "rollups/hour/field=a" / f"hour={hour}" / "p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(14), _dt(14, 2), NOW))
        assert _rel(tmp_path, plan.paths.hour_paths) == [
            "rollups/hour/field=a/hour=2026-03-14-00/p.parquet",
            "rollups/hour/field=a/hour=2026-03-14-01/p.parquet",
        ]
        assert plan.per_field_hours_seen == {"2026-03-14-00", "2026-03-14-01"}

    def test_bundled_day_beats_hours_and_day_files(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day_bundled/day=2026-03-13/all_fields.parquet")
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        _touch(tmp_path / "rollups/hour_bundled/hour=2026-03-13-05/all_fields.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-13-05/p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert _rel(tmp_path, plan.paths.bundled_day_paths) == ["rollups/day_bundled/day=2026-03-13/all_fields.parquet"]
        assert not (plan.paths.day_paths or plan.paths.hour_paths or plan.paths.bundled_hour_paths)
        assert plan.bundled_days == {"2026-03-13"}

    @pytest.mark.parametrize(
        "rel",
        ["day=2026-03-13/day_bundle_2026-03-13.parquet", "day_bundle_2026-03-13.parquet"],
    )
    def test_bundled_day_alternate_spellings(self, tmp_path: Path, rel: str) -> None:
        _touch(tmp_path / "rollups/day_bundled" / rel)
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert _rel(tmp_path, plan.paths.bundled_day_paths) == [f"rollups/day_bundled/{rel}"]

    def test_day_file_for_one_field_suppresses_hour_bundle_for_the_day(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-13-05/p.parquet")
        _touch(tmp_path / "rollups/hour/field=b/hour=2026-03-13-05/p.parquet")
        _touch(tmp_path / "rollups/hour_bundled/hour=2026-03-13-05/all_fields.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a", "b"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert _rel(tmp_path, plan.paths.day_paths) == ["rollups/day/field=a/day=2026-03-13/p.parquet"]
        assert not plan.paths.bundled_hour_paths
        # b has no day file so it still reads its own hour; a's hour is covered by its day file.
        assert _rel(tmp_path, plan.paths.hour_paths) == ["rollups/hour/field=b/hour=2026-03-13-05/p.parquet"]

    def test_boundary_day_file_ignored_in_favour_of_hours(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-13-20/p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13, 6), _dt(14), NOW))
        assert not plan.paths.day_paths
        assert len(plan.paths.hour_paths) == 1
        assert plan.days_with_day_files_any == {"2026-03-13"}

    def test_tmp_files_and_empty_day_dirs_do_not_count(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/.tmp_p.parquet")
        (tmp_path / "rollups/day/field=b/day=2026-03-13").mkdir(parents=True)
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-13-05/p.parquet")
        _touch(tmp_path / "rollups/hour/field=b/hour=2026-03-13-05/p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a", "b"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert not plan.paths.day_paths
        assert len(plan.paths.hour_paths) == 2
        assert plan.days_with_day_files_any == {"2026-03-13"}

    def test_unrequested_fields_are_never_listed(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/hour/field=other/hour=2026-03-13-05/p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert not plan.paths


def _unrolled(root: Path, win: TopNWindow, cap: int = 48) -> list[str]:
    return find_unrolled_hours(plan_top_n_paths(str(root), ["a"], win), win, ["a"], cap)


@pytest.mark.usefixtures("_fresh_listdir")
class TestFindUnrolledHours:
    def test_every_hour_is_missing_on_an_empty_tree_up_to_the_cap(self, tmp_path: Path) -> None:
        got = _unrolled(tmp_path, TopNWindow.build(_dt(14, 6), _dt(14, 9), NOW))
        assert got == ["2026-03-14-06", "2026-03-14-07", "2026-03-14-08"]

    def test_cap_bounds_the_lookback_from_the_active_hour(self, tmp_path: Path) -> None:
        got = _unrolled(tmp_path, TopNWindow.build(_dt(1), _dt(15, 10), NOW), cap=3)
        assert got == ["2026-03-15-07", "2026-03-15-08", "2026-03-15-09"]

    def test_open_ended_window_stops_before_the_active_hour(self, tmp_path: Path) -> None:
        got = _unrolled(tmp_path, TopNWindow.build(_dt(15, 7), None, NOW))
        assert got == ["2026-03-15-07", "2026-03-15-08", "2026-03-15-09"]

    def test_bundle_or_per_field_file_counts_as_coverage(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/hour_bundled/hour=2026-03-14-06/all_fields.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-14-07/p.parquet")
        assert _unrolled(tmp_path, TopNWindow.build(_dt(14, 6), _dt(14, 9), NOW)) == ["2026-03-14-08"]

    def test_mid_hour_start_includes_its_hour_and_end_is_half_open(self, tmp_path: Path) -> None:
        got = _unrolled(tmp_path, TopNWindow.build(_dt(14, 6, 30), _dt(14, 8), NOW))
        assert got == ["2026-03-14-06", "2026-03-14-07"]

    def test_compacted_day_is_excluded_but_empty_day_dir_is_not(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        (tmp_path / "rollups/day/field=a/day=2026-03-14").mkdir(parents=True)
        got = _unrolled(tmp_path, TopNWindow.build(_dt(13, 22), _dt(14, 2), NOW))
        assert got == ["2026-03-14-00", "2026-03-14-01"]

    def test_bundled_day_marks_the_day_compacted(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day_bundled/day=2026-03-13/all_fields.parquet")
        assert _unrolled(tmp_path, TopNWindow.build(_dt(13, 22), _dt(14), NOW)) == []


@pytest.mark.usefixtures("_fresh_listdir")
class TestHealWhereClause:
    def test_range_is_clamped_to_the_request_window(self) -> None:
        got = heal_where_clause(["2026-03-14-06", "2026-03-14-08"], _dt(14, 6, 30), _dt(14, 8, 15))
        assert got == (
            "timestamp >= '2026-03-14T06:30:00+00:00' AND timestamp < '2026-03-14T08:15:00+00:00' "
            "AND strftime(timestamp, '%Y-%m-%d-%H') IN ('2026-03-14-06', '2026-03-14-08')"
        )

    def test_unbounded_window_uses_hour_edges(self) -> None:
        got = heal_where_clause(["2026-03-14-06"], None, None)
        assert got.startswith("timestamp >= '2026-03-14T06:00:00+00:00' AND timestamp < '2026-03-14T07:00:00+00:00'")


@pytest.mark.usefixtures("_fresh_listdir")
def test_partial_hour_adjusted_live_start_narrows_and_returns_total():
    from backend.repositories._base import QueryRunner

    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_hour = active_hour_dt.strftime("%Y-%m-%d-%H")
    watermark_dt = active_hour_dt + timedelta(minutes=20)
    window_end = datetime(2027, 1, 1, tzinfo=UTC)

    with (
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_watermark",
            return_value=watermark_dt.timestamp(),
        ),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_all_fields",
            return_value=[("country", "US", 42)],
        ),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_total",
            return_value=42,
        ),
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        runner.con = None
        live_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            active_hour_dt, active_hour, window_end
        )
        # The watermark (hour_start + 20min) is later than the naive window
        # start passed in (hour_start), so it should win via max(). No skew
        # margin is applied (see I2 investigation in the method's docstring
        # — subtracting one is unsafe for this writer).
        assert live_start == watermark_dt
        assert partial_rows == [("country", "US", 42)]
        # C1: the total comes from the writer's __total__ row, NOT from
        # summing partial_rows across every field.
        assert partial_total == 42


@pytest.mark.usefixtures("_fresh_listdir")
def test_partial_hour_adjusted_live_start_skips_when_window_starts_mid_hour():
    """I1: a caller whose window starts AFTER the active hour's own start
    can't safely use the partial rollup (it always covers the WHOLE
    [hour_start, watermark) span) — the seam must fall back to an
    unmodified live scan instead of over-counting."""
    from backend.repositories._base import QueryRunner

    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_hour = active_hour_dt.strftime("%Y-%m-%d-%H")
    mid_hour_start = active_hour_dt + timedelta(minutes=15)
    window_end = datetime(2027, 1, 1, tzinfo=UTC)

    with patch(
        "backend.core.rollups.partial_hour.read_partial_hour_watermark",
        return_value=(active_hour_dt + timedelta(minutes=20)).timestamp(),
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        runner.con = None
        live_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            mid_hour_start, active_hour, window_end
        )

    assert live_start == mid_hour_start
    assert partial_rows == []
    assert partial_total == 0


@pytest.mark.usefixtures("_fresh_listdir")
def test_collect_hourly_bundle_paths_trusts_durable_once_ready(tmp_path):
    rr.reset_rollup_coverage_ready()
    src = {"service_id": "svc-durable", "name": "svc-durable", "deployment_mode": "high_scale"}

    with (
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.repositories.rollup_coverage.cached_listdir", return_value=[]) as mock_listdir,
    ):
        assert rollup_coverage.collect_hourly_bundle_paths(src, None, None, str(tmp_path), "x.parquet") is None
        mock_listdir.assert_not_called()

    rr.mark_rollup_coverage_ready("svc-durable")

    with (
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.repositories.rollup_coverage.cached_listdir", return_value=[]) as mock_listdir,
    ):
        rollup_coverage.collect_hourly_bundle_paths(
            src,
            datetime(2026, 9, 6, 12, tzinfo=UTC),
            datetime(2026, 9, 6, 13, tzinfo=UTC),
            str(tmp_path),
            "x.parquet",
        )
        mock_listdir.assert_called()

    rr.reset_rollup_coverage_ready("svc-durable")


# ── Merged from tests/repositories/test_top_n_rollup_units.py ──
SKIP = frozenset({"rid", "ttfb"})


class TestLiveTopnFields:
    def test_drops_skip_fields_and_keeps_order(self) -> None:
        assert live_topn_fields(["country", "rid", "city", "ttfb"], SKIP, None) == ["country", "city"]

    def test_optional_prefixes_need_a_custom_field(self) -> None:
        fields = ["cmcd_x", "edge_a", "rum_b", "fastly_req_id", "country"]
        assert live_topn_fields(fields, SKIP, None) == ["country"]

    def test_enabled_dashboard_custom_field_is_kept(self) -> None:
        cfg = {
            "custom_fields": [
                {"name": "edge_a"},
                {"name": "rum_b", "enabled": False},
                {"name": "cmcd_x", "show_in_dashboard": False},
            ]
        }
        assert live_topn_fields(["cmcd_x", "edge_a", "rum_b"], SKIP, cfg) == ["edge_a"]

    def test_custom_field_does_not_override_skip(self) -> None:
        assert live_topn_fields(["rid"], SKIP, {"custom_fields": [{"name": "rid"}]}) == []


class TestFoldRows:
    def test_sums_across_row_sets(self) -> None:
        got = fold_rows([("a", "x", 1), ("a", "y", 2)], [("a", "x", 4), ("b", "z", 1)], [])
        assert got == {"a": {"x": 5, "y": 2}, "b": {"z": 1}}

    def test_no_rows(self) -> None:
        assert fold_rows() == {}
        assert fold_rows([], []) == {}


class TestTruncateTopN:
    BY_FIELD = {"a": {"x": 5, "y": 9, "z": 1}, "b": {"p": 2}, "c": {}}

    def test_limit_applies_per_field_in_request_order(self) -> None:
        rows, empty = truncate_top_n(self.BY_FIELD, ["b", "a"], 2, None)
        assert rows == [("b", "p", 2), ("a", "y", 9), ("a", "x", 5)]
        assert empty == []

    def test_per_field_limit_widens_only_its_field(self) -> None:
        rows, _ = truncate_top_n(self.BY_FIELD, ["a", "b"], 1, {"a": 3})
        assert [r for r in rows if r[0] == "a"] == [("a", "y", 9), ("a", "x", 5), ("a", "z", 1)]
        assert [r for r in rows if r[0] == "b"] == [("b", "p", 2)]

    def test_missing_and_empty_fields_are_reported(self) -> None:
        rows, empty = truncate_top_n(self.BY_FIELD, ["ghost", "c", "b"], 5, None)
        assert rows == [("b", "p", 2)]
        assert empty == ["ghost", "c"]

    def test_ties_keep_insertion_order_when_not_truncated(self) -> None:
        rows, _ = truncate_top_n({"a": {"x": 1, "y": 1}}, ["a"], 5, None)
        assert [r[1] for r in rows] == ["x", "y"]


class TestSchemaSeeds:
    def test_seeds_win_and_nothing_is_fetched(self) -> None:
        seeds = SchemaSeeds(["a"], {"a": "VARCHAR"})
        seeds.ensure(self._boom, self._boom)
        assert (seeds.cols, seeds.types) == (["a"], {"a": "VARCHAR"})

    def test_fetches_once_and_reuses(self) -> None:
        calls: list[str] = []
        seeds = SchemaSeeds()

        def cols() -> list[str]:
            calls.append("cols")
            return ["a"]

        def types() -> dict[str, str]:
            calls.append("types")
            return {"a": "INT"}

        seeds.ensure(cols, types)
        seeds.ensure(cols, types)
        assert calls == ["cols", "types"]
        assert (seeds.cols, seeds.types) == (["a"], {"a": "INT"})

    def test_failed_column_lookup_skips_types_and_is_retried_by_the_next_step(self) -> None:
        calls: list[str] = []
        seeds = SchemaSeeds()

        def bad_cols() -> list[str]:
            calls.append("cols")
            raise RuntimeError("nope")

        def types() -> dict[str, str]:
            calls.append("types")
            return {}

        with pytest.raises(RuntimeError):
            seeds.ensure(bad_cols, types)
        with pytest.raises(RuntimeError):
            seeds.ensure(bad_cols, types)
        assert calls == ["cols", "cols"]

    def test_types_failure_keeps_the_columns_already_fetched(self) -> None:
        seeds = SchemaSeeds()

        def bad_types() -> dict[str, str]:
            raise RuntimeError("nope")

        with pytest.raises(RuntimeError):
            seeds.ensure(lambda: ["a"], bad_types)
        assert seeds.cols == ["a"]

    @staticmethod
    def _boom() -> NoReturn:
        raise AssertionError("must not be called")


class TestQualifyLimitExpr:
    def test_plain_limit(self) -> None:
        assert qualify_limit_expr(["a", "b"], 10, None) == "10"

    def test_case_only_for_requested_fields(self) -> None:
        got = qualify_limit_expr(["a", "b"], 10, {"a": 500, "ghost": 9})
        assert got == "CASE field WHEN 'a' THEN 500 ELSE 10 END"

    def test_overrides_for_unread_fields_collapse_to_plain_limit(self) -> None:
        assert qualify_limit_expr(["a"], 3, {"ghost": 9}) == "3"

    def test_quote_in_field_is_doubled(self) -> None:
        assert "WHEN 'o''k' THEN 5" in qualify_limit_expr(["o'k"], 1, {"o'k": 5})


class TestBuildRolledSql:
    def test_nothing_to_read(self) -> None:
        assert build_rolled_sql(TopNPaths(), ["a"], 10, None) is None
        assert not TopNPaths()

    def test_each_source_shape_is_its_own_branch_in_order(self) -> None:
        paths = TopNPaths(["/d.parquet"], ["/h.parquet"], ["/bh.parquet"], ["/bd.parquet"])
        sql = build_rolled_sql(paths, ["a", "b"], 10, None)
        assert sql is not None
        assert sql.count(" UNION ALL ") == 3
        order = [sql.index(p) for p in ("/d.parquet", "/h.parquet", "/bh.parquet", "/bd.parquet")]
        assert order == sorted(order)
        assert sql.count("hive_partitioning=1") == 2
        assert sql.count("hive_partitioning=0") == 2

    def test_only_bundles_are_field_filtered(self) -> None:
        sql = build_rolled_sql(TopNPaths(["/d.parquet"], [], ["/bh.parquet"], []), ["a", "o'k"], 10, None)
        assert sql is not None
        assert sql.count("WHERE field IN ('a', 'o''k')") == 1
        assert sql.index("WHERE field IN") > sql.index("/bh.parquet")

    def test_qualify_uses_per_field_case(self) -> None:
        sql = build_rolled_sql(TopNPaths(["/d.parquet"]), ["a", "b"], 10, {"a": 50})
        assert sql is not None
        assert sql.endswith("<= CASE field WHEN 'a' THEN 50 ELSE 10 END")


# ── Merged from tests/repositories/test_durable_rollup_fallbacks.py ──
def _durable_source() -> dict:
    return {
        "name": "durable_rollup_svc",
        "service_id": "durable_rollup_svc",
        "deployment_mode": "high_scale",
    }


def _enable_durable(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://catalog/ducklake")


def test_durable_rollup_path_is_unavailable_even_when_local_bundle_exists(monkeypatch, tmp_path):
    _enable_durable(monkeypatch)
    src = _durable_source()
    root = tmp_path / "rollups" / "hour_bundled"
    (root / "hour=2026-09-06-12").mkdir(parents=True)
    bundle = root / "hour=2026-09-06-12" / "time_series.parquet"
    bundle.touch()

    out = collect_hourly_bundle_paths(
        src,
        datetime(2026, 9, 6, 12, tzinfo=UTC),
        datetime(2026, 9, 6, 13, tzinfo=UTC),
        str(root),
        "time_series.parquet",
    )

    assert out is None


def test_durable_top_n_rollup_returns_unavailable_without_querying_local_files(monkeypatch):
    _enable_durable(monkeypatch)
    con = MagicMock()
    runner = QueryRunner(con, _durable_source())

    rows, fields = runner.execute_top_n_rollups(["country"], None, None)

    assert rows == []
    assert fields == ["country"]
    con.execute.assert_not_called()


def test_durable_ip_spread_rollup_returns_unavailable(monkeypatch):
    _enable_durable(monkeypatch)
    runner = QueryRunner(MagicMock(), _durable_source())

    counts, metadata = runner.execute_ip_spread_rollups(["tls_ciphers_sha"], None, None)

    assert counts == {}
    assert metadata == {}


def test_durable_slow_url_rollup_returns_unavailable(monkeypatch):
    _enable_durable(monkeypatch)
    runner = QueryRunner(MagicMock(), _durable_source())

    result = runner.try_slow_urls_from_rollup(
        "2026-09-01T00:00:00Z",
        "2026-09-07T00:00:00Z",
        has_filters=False,
        min_requests=1,
        limit=10,
    )

    assert result is None


def test_durable_network_health_uses_ducklake_fallback_when_rollups_are_missing(
    monkeypatch, in_memory_duckdb, test_service_source
):
    _enable_durable(monkeypatch)
    src = {**test_service_source, "name": "durable_network_svc", "deployment_mode": "high_scale"}
    table = src["name"]
    in_memory_duckdb.execute(
        f'CREATE TABLE "{table}" ('
        "timestamp TIMESTAMPTZ, asn INTEGER, country VARCHAR, city VARCHAR, "
        "lat DOUBLE, lon DOUBLE, tcp_rtt DOUBLE, rtt_min DOUBLE, "
        "rtt_var DOUBLE, ploss DOUBLE, status INTEGER, cache VARCHAR, "
        "elapsed DOUBLE, resp_bytes BIGINT, c_speed DOUBLE)"
    )
    in_memory_duckdb.execute(
        f'INSERT INTO "{table}" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            datetime(2026, 9, 6, 12, tzinfo=UTC),
            7922,
            "US",
            "Denver",
            39.7,
            -104.9,
            20_000,
            10_000,
            100,
            0.0,
            200,
            "HIT",
            5.0,
            100,
            1000,
        ],
    )
    monkeypatch.setattr(
        QueryRunner,
        "get_schema_cols",
        lambda _self: [
            "timestamp",
            "asn",
            "country",
            "city",
            "lat",
            "lon",
            "tcp_rtt",
            "rtt_min",
            "rtt_var",
            "ploss",
            "status",
            "cache",
            "elapsed",
            "resp_bytes",
            "c_speed",
        ],
    )
    in_memory_duckdb.execute(f'CREATE TEMP TABLE durable_network_temp AS SELECT * FROM "{table}"')
    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", lambda *_args, **_kwargs: "durable_network_temp")
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda *_args, **_kwargs: {})

    with patch.object(QueryRunner, "try_network_heatmap_from_rollup", return_value=None) as heatmap:
        from backend.repositories.network import get_health

        result = get_health(
            lambda: in_memory_duckdb,
            src,
            "2026-09-06T00:00:00Z",
            "2026-09-07T00:00:00Z",
            {},
            sections={"heatmap"},
        )

    heatmap.assert_called_once()
    assert result["available"] is not False
    assert result["heatmap"]


def test_durable_log_activity_reads_ducklake_instead_of_local_rollup_or_metadata(monkeypatch):
    _enable_durable(monkeypatch)
    src = _durable_source()
    con = MagicMock()
    describe_result = MagicMock()
    describe_result.fetchall.return_value = [("timestamp",), ("resp_bytes",)]
    query_result = MagicMock()
    query_result.fetchall.return_value = [
        (datetime(2026, 9, 6, 12, tzinfo=UTC), 3, 1200),
    ]
    con.execute.side_effect = [describe_result, query_result]

    with (
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.repositories.usage._log_activity_fallback") as local_fallback,
    ):
        from backend.repositories.usage import get_log_activity

        result = get_log_activity(
            src,
            "2026-09-06T00:00:00Z",
            "2026-09-07T00:00:00Z",
            "hour",
        )

    local_fallback.assert_not_called()
    con.close.assert_called_once()
    assert result["data"] == [{"time": "2026-09-06T12:00", "row_count": 3, "bytes": 1200}]
    assert result["total_rows"] == 3
    assert result["total_bytes"] == 1200
