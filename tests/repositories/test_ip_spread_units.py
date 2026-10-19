"""Unit tests for the pieces ``execute_ip_spread_rollups`` is built from."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME
from backend.repositories.ip_spread_rollups import (
    IpSpreadPlan,
    build_ip_spread_sql,
    fold_sketches,
    plan_ip_spread_paths,
    spread_meta,
)
from backend.repositories.top_n_rollups import TopNWindow
from backend.utils.hll import HyperLogLog

NOW = datetime(2026, 3, 15, 10, 20, tzinfo=UTC)


def _win(st_day: int = 14, et_day: int = 15) -> TopNWindow:
    return TopNWindow.build(datetime(2026, 3, st_day, tzinfo=UTC), datetime(2026, 3, et_day, tzinfo=UTC), NOW)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def _sketch(items: range, precision: int = 8) -> bytes:
    h = HyperLogLog(precision=precision)
    h.update(f"ip{i}" for i in items)
    return h.to_bytes()


class TestPlanIpSpreadPaths:
    def test_no_trees_is_none(self, tmp_path: Path) -> None:
        assert plan_ip_spread_paths(str(tmp_path / "b"), str(tmp_path / "p"), ["a"], _win()) is None

    def test_tree_without_in_window_files_is_a_falsy_plan(self, tmp_path: Path) -> None:
        (tmp_path / "b").mkdir()
        plan = plan_ip_spread_paths(str(tmp_path / "b"), str(tmp_path / "p"), ["a"], _win())
        assert plan is not None
        assert not plan

    def test_bundle_hours_suppress_per_field_files_for_the_same_hour(self, tmp_path: Path) -> None:
        b, p = tmp_path / "b", tmp_path / "p"
        _touch(b / "hour=2026-03-14-05" / IP_SPREAD_BUNDLE_FILENAME)
        _touch(p / "field=a" / "hour=2026-03-14-05" / "x.parquet")
        _touch(p / "field=a" / "hour=2026-03-14-06" / "x.parquet")
        _touch(p / "field=other" / "hour=2026-03-14-06" / "x.parquet")
        plan = plan_ip_spread_paths(str(b), str(p), ["a"], _win())
        assert plan is not None
        assert plan.bundled_hours == {"2026-03-14-05"}
        assert [Path(x).parent.name for x in plan.per_field_paths] == ["hour=2026-03-14-06"]
        assert plan.per_field_hours_by_field == {"a": {"2026-03-14-06"}}

    def test_window_active_hour_tmp_and_nonparquet_are_ignored(self, tmp_path: Path) -> None:
        b, p = tmp_path / "b", tmp_path / "p"
        _touch(b / "hour=2026-03-13-23" / IP_SPREAD_BUNDLE_FILENAME)
        _touch(b / "hour=2026-03-15-10" / IP_SPREAD_BUNDLE_FILENAME)
        _touch(b / "hour=2026-03-14-01" / "all_fields.parquet")
        _touch(p / "field=a" / "hour=2026-03-14-02" / ".tmp_x.parquet")
        _touch(p / "field=a" / "hour=2026-03-14-02" / "x.json")
        plan = plan_ip_spread_paths(str(b), str(p), ["a"], _win(14, 16))
        assert plan is not None
        assert not plan


class TestBuildIpSpreadSql:
    def test_each_tree_is_its_own_filtered_branch(self) -> None:
        plan = IpSpreadPlan(bundled_paths=["/b.parquet"], per_field_paths=["/p.parquet"])
        sql = build_ip_spread_sql(plan, ["a", "o'k"])
        assert sql.count(" UNION ALL ") == 1
        assert sql.count("WHERE field IN ('a', 'o''k')") == 2
        assert "[" in sql and sql.index("/b.parquet") < sql.index("/p.parquet")
        assert sql.index("hive_partitioning=0") < sql.index("hive_partitioning=1")

    def test_single_branch_has_no_union(self) -> None:
        sql = build_ip_spread_sql(IpSpreadPlan(per_field_paths=["/p.parquet"]), ["a"])
        assert " UNION ALL " not in sql
        assert sql.startswith("(SELECT field, value, ip_sketch, sample_capped FROM read_parquet(")


class TestFoldSketches:
    def test_merges_by_key_and_ors_the_capped_flag(self) -> None:
        rows = [
            ("a", "x", _sketch(range(0, 50)), False),
            ("a", "x", _sketch(range(25, 100)), True),
            ("a", "y", _sketch(range(0, 5)), False),
        ]
        merged, capped = fold_sketches(rows)
        assert set(merged) == {("a", "x"), ("a", "y")}
        assert capped == {("a", "x"): True, ("a", "y"): False}
        assert 85 <= merged[("a", "x")].count() <= 115

    def test_bad_blobs_are_skipped_not_fatal(self) -> None:
        rows = [("a", "x", None, False), ("a", "x", b"", False), ("a", "x", b"garbage", True)]
        assert fold_sketches(rows) == ({}, {})

    def test_precision_mismatch_row_is_dropped_but_flag_still_ors(self) -> None:
        rows = [("a", "x", _sketch(range(10)), False), ("a", "x", _sketch(range(10), precision=10), True)]
        merged, capped = fold_sketches(rows)
        assert merged[("a", "x")].precision == 8
        assert capped[("a", "x")] is True


class TestSpreadMeta:
    def test_coverage_is_bundle_hours_plus_the_fields_own_hours(self) -> None:
        plan = IpSpreadPlan(bundled_hours={"h1", "h2"}, per_field_hours_by_field={"a": {"h2", "h3"}, "b": set()})
        meta = spread_meta(["a", "b"], plan, {("a", "x"): True, ("a", "y"): False, ("a", "z"): True})
        assert meta == {
            "a": {"coverage_hours": 3, "capped_values": 2},
            "b": {"coverage_hours": 2, "capped_values": 0},
        }
