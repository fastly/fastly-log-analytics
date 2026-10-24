"""Golden characterization of ``QueryRunner.execute_ip_spread_rollups``.

Same approach as ``test_top_n_rollups_golden``: a matrix of bundled / per-field
ip-spread layouts, window shapes and sketch payloads (merge, capped flags,
malformed and precision-mismatched blobs, read failures), clock frozen, with the
returned ``(counts, meta)``, executed SQL digests, ordered parquet paths, phase
sections and filesystem probe counts pinned in ``golden/ip_spread_rollups.json``.
Regenerate deliberately with ``UPDATE_ROLLUP_GOLDEN=1``.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pytest
import time_machine

from backend.core import rollup_readiness as rr
from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME
from backend.repositories import rollup_coverage
from backend.repositories._base import QueryRunner
from backend.utils.hll import HyperLogLog
from tests._helpers.rollups import RollupGoldenLayout as Layout
from tests._helpers.rollups import build_rollup_golden_layout
from tests._helpers.sources import make_source
from tests.repositories.test_rollup_reader_coverage_golden import _compact, _digest, _Norm, _Probe

GOLDEN_PATH = Path(__file__).parent / "golden" / "ip_spread_rollups.json"
_STAT_FNS = ("isfile", "isdir", "exists")
FROZEN = datetime(2026, 3, 15, 10, 20, 0, tzinfo=UTC)
H = FROZEN.replace(minute=0, second=0, microsecond=0)
FIELDS = ["country", "ua"]


def _sketch(items: range, precision: int = 8) -> bytes:
    h = HyperLogLog(precision=precision)
    h.update(f"10.0.0.{i}" for i in items)
    return h.to_bytes()


ROWS_NORMAL = [
    ("country", "US", _sketch(range(0, 60)), False),
    ("country", "US", _sketch(range(40, 120)), True),
    ("country", "DE", _sketch(range(0, 10)), False),
    ("ua", "curl", _sketch(range(0, 5)), False),
    ("ua", "curl", _sketch(range(3, 9)), False),
]
ROWS_DIRTY = [
    ("country", "US", _sketch(range(0, 30)), False),
    ("country", "US", _sketch(range(0, 30), precision=10), True),
    ("country", "XX", b"", False),
    ("country", "XX", None, True),
    ("country", "YY", b"garbage-blob", False),
    ("ua", "bot", bytearray(_sketch(range(0, 7))), True),
    ("ua", "bot", _sketch(range(5, 15)), False),
]


@dataclass(frozen=True)
class Case:
    name: str
    layout: Callable[[Layout], None]
    fields: tuple[str, ...] = tuple(FIELDS)
    st: float | None = -24
    et: float | None = 0
    rows: str = "normal"  # normal | dirty | empty | boom
    durable: str | None = None
    phase_log: bool = True


def bundles(lo: int = -24, hi: int = 0, skip: tuple[int, ...] = (), fname: str = IP_SPREAD_BUNDLE_FILENAME):
    def b(lay: Layout) -> None:
        lay.bundles([k for k in range(lo, hi) if k not in skip], fname)

    return b


def perfield(lo: int = -24, hi: int = 0, fields: list[str] | None = None, skip: tuple[int, ...] = ()):
    def b(lay: Layout) -> None:
        lay.perfield_hours(fields or FIELDS, [k for k in range(lo, hi) if k not in skip])

    return b


def combo(*fns: Callable[[Layout], None]):
    def b(lay: Layout) -> None:
        for fn in fns:
            fn(lay)

    return b


def nothing(lay: Layout) -> None:
    lay.root.mkdir(parents=True, exist_ok=True)


CASES: tuple[Case, ...] = (
    Case("bundled_full", bundles()),
    Case("perfield_full", perfield()),
    Case("bundled_and_perfield_overlap", combo(bundles(-24, -12), perfield())),
    Case("bundled_and_perfield_disjoint", combo(bundles(-24, -12), perfield(-12, 0))),
    Case("perfield_one_field_only", perfield(fields=["country"])),
    Case("perfield_gap_hours", perfield(skip=(-5, -6, -7))),
    Case("bundled_gap_hours", bundles(skip=(-5, -6, -7))),
    Case("bundled_wrong_filename", bundles(fname="all_fields.parquet")),
    Case("perfield_tmp_files", lambda lay: lay.perfield_hours(FIELDS, range(-24, 0), ".tmp_part.parquet")),
    Case("perfield_nonparquet", lambda lay: lay.perfield_hours(FIELDS, range(-24, 0), "part-0.json")),
    Case("perfield_unrequested_field", perfield(fields=["other"])),
    Case("no_trees", nothing),
    Case("empty_bundled_root", lambda lay: lay.bundled.mkdir(parents=True)),
    Case("empty_perfield_root", lambda lay: lay.perfield.mkdir(parents=True)),
    Case("active_hour_excluded", combo(bundles(-24, 1), perfield(-24, 1)), st=-24, et=0.5),
    Case("window_start_mid_hour", bundles(), st=-10.5, et=-2),
    Case("window_end_mid_hour", bundles(), st=-10, et=-2.25),
    Case("window_end_on_boundary", bundles(), st=-10, et=-2),
    Case("window_no_start", bundles()),
    Case("window_no_end", bundles(), st=-5, et=None),
    Case("window_no_bounds", bundles(), st=None, et=None),
    Case("window_after_active", bundles(-24, 3), st=1.2, et=2),
    Case("window_single_hour", bundles(), st=-5, et=-4),
    Case("shape_single_field", combo(bundles(), perfield()), fields=("country",)),
    Case("shape_unsafe_mixed", combo(bundles(), perfield()), fields=("country", "bad'field", "has space")),
    Case("shape_all_unsafe", bundles(), fields=("bad'field",)),
    Case("shape_empty_fields", bundles(), fields=()),
    Case("shape_no_phase_log", bundles(), phase_log=False),
    Case("rows_dirty_bundled", bundles(), rows="dirty"),
    Case("rows_dirty_mixed", combo(bundles(-24, -12), perfield()), rows="dirty"),
    Case("rows_empty", bundles(), rows="empty"),
    Case("rows_boom", bundles(), rows="boom"),
    Case("rows_boom_perfield", perfield(), rows="boom"),
    Case("meta_coverage_union", combo(bundles(-24, -20), perfield(-20, 0, fields=["country"])), rows="normal"),
    Case("durable_not_ready", bundles(), durable="not_ready"),
    Case("durable_ready", bundles(), durable="ready"),
)


class _Rows:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


def _iso(off: float | None) -> str | None:
    return None if off is None else (H + timedelta(hours=off)).isoformat()


def _jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {(" | ".join(k) if isinstance(k, tuple) else k): _jsonable(x) for k, x in v.items()}
    return v


def _run(case: Case, tmp: Path, probe: _Probe) -> tuple[dict, dict[str, str]]:
    root = tmp / case.name
    build_rollup_golden_layout(root, case.layout, anchor=H)
    root.mkdir(parents=True, exist_ok=True)
    norm = _Norm(H, root, H)
    src = make_source(str(root), name="svc", service_id="svc")
    con = duckdb.connect()
    executed: list[tuple[str, Any]] = []
    phases: list[dict] = []
    try:
        runner = QueryRunner(con, src)

        def fake_execute(q: str, p: list | None = None) -> _Rows:
            executed.append((q, p))
            if case.rows == "boom":
                raise duckdb.Error("boom")
            return _Rows({"normal": ROWS_NORMAL, "dirty": ROWS_DIRTY, "empty": []}[case.rows])

        runner.execute = fake_execute  # type: ignore[method-assign]
        rollup_coverage._listdir_cache.clear()
        rr.reset_rollup_coverage_ready()
        probe.counts = {}
        patches = []
        if case.durable:
            patches.append(patch("backend.config.is_durable_serving_mode", return_value=True))
            if case.durable == "ready":
                rr.mark_rollup_coverage_ready("svc")
        for p in patches:
            p.start()
        kwargs: dict[str, Any] = {"_phase_log": phases} if case.phase_log else {}
        probe.on = True
        try:
            res = runner.execute_ip_spread_rollups(list(case.fields), _iso(case.st), _iso(case.et), **kwargs)
            raised = None
        except Exception as e:
            res, raised = None, type(e).__name__
        finally:
            probe.on = False
            for p in patches:
                p.stop()
            rr.reset_rollup_coverage_ready()
    finally:
        con.close()

    sql_texts = [norm.sql(q) for q, _ in executed]
    out = {
        "counts": None if res is None else _jsonable(res[0]),
        "meta": None if res is None else res[1],
        "raised": raised,
        "executes": len(executed),
        "sql": [_digest(t) for t in sql_texts],
        "paths": [_compact(norm.paths(q)) for q, _ in executed],
        "phases": [p["section"] for p in phases],
        "probes": dict(sorted(probe.counts.items())),
    }
    return out, {_digest(t): t for t in sql_texts}


def _load() -> dict:
    return json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else {}


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_ip_spread_matches_golden(case: Case, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _Probe()
    for name in _STAT_FNS:
        monkeypatch.setattr(os.path, name, probe.wrap(name, getattr(os.path, name)))
    real_listdir = os.listdir
    # Directory order is filesystem-dependent; pin it so the recorded path order is portable.
    monkeypatch.setattr(os, "listdir", probe.wrap("listdir", lambda p: sorted(real_listdir(p))))
    with time_machine.travel(FROZEN, tick=False):
        got, texts = _run(case, tmp_path, probe)
    monkeypatch.undo()
    shutil.rmtree(tmp_path / case.name, ignore_errors=True)
    got = json.loads(json.dumps(got, sort_keys=True))

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        golden = _load()
        golden[case.name] = got
        golden["_sql"] = {**golden.get("_sql", {}), **texts}
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return

    want = _load().get(case.name)
    assert want is not None, f"no golden for {case.name}; run with UPDATE_ROLLUP_GOLDEN=1"
    g_probes, w_probes = got.pop("probes"), want.pop("probes")
    if got != want:
        sqls = _load().get("_sql", {})
        drift = "\n".join(f"got: {texts.get(h)}\nwant: {sqls.get(h)}" for h in got["sql"] if h not in want["sql"])
        raise AssertionError(f"{case.name}: behavior drifted from golden\n{got}\n{want}\n{drift}")
    for k, v in g_probes.items():
        assert v <= w_probes.get(k, 0), f"{case.name}: {k} probes {v} > golden {w_probes.get(k, 0)}"


# ── Merged from test_ip_spread_units.py ──

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

NOW = datetime(2026, 3, 15, 10, 20, tzinfo=UTC)


def _win(st_day: int = 14, et_day: int = 15) -> TopNWindow:
    return TopNWindow.build(datetime(2026, 3, st_day, tzinfo=UTC), datetime(2026, 3, et_day, tzinfo=UTC), NOW)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def _unit_sketch(items: range, precision: int = 8) -> bytes:
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
            ("a", "x", _unit_sketch(range(0, 50)), False),
            ("a", "x", _unit_sketch(range(25, 100)), True),
            ("a", "y", _unit_sketch(range(0, 5)), False),
        ]
        merged, capped = fold_sketches(rows)
        assert set(merged) == {("a", "x"), ("a", "y")}
        assert capped == {("a", "x"): True, ("a", "y"): False}
        assert 85 <= merged[("a", "x")].count() <= 115

    def test_bad_blobs_are_skipped_not_fatal(self) -> None:
        rows = [("a", "x", None, False), ("a", "x", b"", False), ("a", "x", b"garbage", True)]
        assert fold_sketches(rows) == ({}, {})

    def test_precision_mismatch_row_is_dropped_but_flag_still_ors(self) -> None:
        rows = [("a", "x", _unit_sketch(range(10)), False), ("a", "x", _unit_sketch(range(10), precision=10), True)]
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
