"""Unit tests for the top-N window math and filesystem path planning."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from backend.repositories import rollup_coverage
from backend.repositories.rollup_coverage import heal_where_clause
from backend.repositories.top_n_rollups import TopNWindow, find_unrolled_hours, plan_top_n_paths

NOW = datetime(2026, 3, 15, 10, 20, tzinfo=UTC)


def _dt(day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 3, day, hour, minute, tzinfo=UTC)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def _rel(root: Path, paths: list[str]) -> list[str]:
    return sorted(os.path.relpath(p, root) for p in paths)


@pytest.fixture(autouse=True)
def _fresh_listdir() -> None:
    rollup_coverage.clear_listdir_cache()


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
