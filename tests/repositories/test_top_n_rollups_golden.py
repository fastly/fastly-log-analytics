"""Golden characterization of ``QueryRunner.execute_top_n_rollups``.

Runs the function against a matrix of on-disk rollup layouts (per-field hours,
bundled hours, per-field days, bundled days incl. the alternate file names,
day+hour mixes, gaps with and without coverage, empty sentinels, crashed
compactions, the active hour, heal-triggering windows), field/limit shapes and
durable-serving states, with the clock frozen so the closed/active-hour seam is
deterministic. For every case it records:

* the returned ``(field, value, count)`` rows and the echoed field list,
* every SQL string executed (digest; paths/timestamps normalised) and the exact
  ordered parquet paths handed to each ``read_parquet``,
* the live-hour / heal collaborator calls (temp-table builds, batch queries),
* the phase-log sections, emitted log lines, and an upper bound on filesystem
  probes (these may only go down).

The snapshot ``golden/top_n_rollups.json`` was captured from the pre-refactor
implementation. Regenerate deliberately with ``UPDATE_ROLLUP_GOLDEN=1``.
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

import backend.core.rollups.recompute as rollup_coverage
from backend.core import rollups as rr
from backend.repositories import _base
from backend.repositories._base import QueryRunner
from tests._helpers.rollups import RollupGoldenLayout as Layout
from tests._helpers.rollups import build_rollup_golden_layout
from tests._helpers.sources import make_source
from tests.repositories.test_rollup_reader_coverage_golden import _compact, _digest, _Norm, _Probe

GOLDEN_PATH = Path(__file__).parent / "golden" / "top_n_rollups.json"
_STAT_FNS = ("isfile", "isdir", "exists")
FROZEN = datetime(2026, 3, 15, 10, 20, 0, tzinfo=UTC)
H = FROZEN.replace(minute=0, second=0, microsecond=0)  # active hour start
COLS = ["timestamp", "country", "city", "status", "cmcd_x", "edge_custom", "rid", "ttfb"]
F3 = ["country", "city", "status"]

ROLLED_ROWS = [
    ("country", "US", 50),
    ("country", "DE", 30),
    ("country", "FR", 30),
    ("country", "GB", 20),
    ("country", "JP", 10),
    ("country", "BR", 9),
    ("country", "IN", 8),
    ("city", "NYC", 40),
    ("city", "BER", 40),
    ("city", "PAR", 5),
    ("status", "200", 100),
    ("status", "404", 7),
    ("status", "500", 1),
]
LIVE_ROWS = [("country", "US", 5), ("country", "ES", 3), ("city", "NYC", 2), ("status", "200", 4)]
HEAL_ROWS = [("country", "ES", 40), ("city", "MAD", 3), ("status", "503", 2)]
PARTIAL_ROWS = [("country", "US", 7), ("status", "301", 1)]


# ── cases ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Case:
    name: str
    layout: Callable[[Layout], None]
    fields: tuple[str, ...] = tuple(F3)
    limit: int = 10
    pfl: dict[str, int] | None = None
    st: float | None = -72  # hours relative to the active hour start
    et: float | None = 0
    live: str = "empty"  # empty | rows | none_fallback | zero_rows | boom
    seeds: bool = False
    partial: str = "none"  # none | rows | narrow
    durable: str | None = None
    rolled: str = "rows"  # rows | boom
    heal: str = "rows"  # rows | boom | none_tmp
    phase_log: bool = True
    custom_fields: bool = False
    st_raw: str | None = None


def nothing(lay: Layout) -> None:
    lay.root.mkdir(parents=True, exist_ok=True)


def hours_pf(lo: int = -72, hi: int = 0, fields: list[str] | None = None, skip: tuple[int, ...] = ()):
    def b(lay: Layout) -> None:
        lay.field_hours(fields or F3, [k for k in range(lo, hi) if k not in skip])

    return b


def hours_bundled(lo: int = -72, hi: int = 0, skip: tuple[int, ...] = ()):
    def b(lay: Layout) -> None:
        lay.bundled_hours([k for k in range(lo, hi) if k not in skip])

    return b


def combo(*fns: Callable[[Layout], None]):
    def b(lay: Layout) -> None:
        for fn in fns:
            fn(lay)

    return b


def days_pf(ks: list[int], fields: list[str] | None = None):
    def b(lay: Layout) -> None:
        lay.field_days(fields or F3, ks)

    return b


def days_b(ks: list[int], variant: str = "std"):
    def b(lay: Layout) -> None:
        lay.bundled_days(ks, variant)

    return b


W3D = {"st": -82, "et": -10}  # 03-12 00:00 .. 03-15 00:00: three whole closed days
LIVE = {"st": -72, "et": 0.5}
D = [-3, -2, -1]  # day offsets vs 03-15


def _c(name: str, layout: Callable[[Layout], None], **kw: Any) -> Case:
    return Case(name, layout, **kw)


CASES: tuple[Case, ...] = (
    # ── field / limit shapes on a full per-field hourly tree ──
    _c("shape_single_field", hours_pf(), fields=("country",)),
    _c("shape_three_limit3_pfl_country5", hours_pf(), limit=3, pfl={"country": 5}),
    _c("shape_pfl_unrequested_field", hours_pf(), limit=2, pfl={"nonexistent": 99, "city": 1}),
    _c("shape_pfl_all_fields", hours_pf(), limit=2, pfl={"country": 4, "city": 3, "status": 1}),
    _c("shape_limit1", hours_pf(), limit=1),
    _c("shape_unsafe_mixed", hours_pf(), fields=("country", "bad'field", "has space", "city")),
    _c("shape_all_unsafe", hours_pf(), fields=("bad'field", "x y")),
    _c("shape_empty_fields", hours_pf(), fields=()),
    _c("shape_field_without_rows", hours_pf(), fields=("country", "ghost")),
    _c(
        "shape_skip_fields_live",
        hours_pf(),
        fields=("country", "rid", "ttfb", "cmcd_x", "edge_custom"),
        **LIVE,
        live="rows",
    ),
    _c(
        "shape_custom_fields_live",
        hours_pf(),
        fields=("country", "rid", "cmcd_x", "edge_custom"),
        custom_fields=True,
        **LIVE,
        live="rows",
    ),
    _c("shape_no_phase_log", hours_pf(), phase_log=False),
    _c("shape_no_bounds", hours_pf(), st=None, et=None),
    _c("shape_no_start", hours_pf(), st=None, et=0),
    _c("shape_no_end", hours_pf(-24, 0), st=-24, et=None, live="rows"),
    _c("shape_bad_iso_start", hours_pf(), st=None, st_raw="not-a-date"),
    # ── per-field hourly / bundled hourly / mixes ──
    _c("hours_perfield_full", hours_pf()),
    _c("hours_perfield_partial_fields", hours_pf(fields=["country"])),
    _c("hours_bundled_full", hours_bundled()),
    _c("hours_bundled_single_field_filter", hours_bundled(), fields=("country",)),
    _c("hours_bundled_plus_perfield_overlap", combo(hours_bundled(-72, -36), hours_pf())),
    _c("hours_bundled_plus_perfield_disjoint", combo(hours_bundled(-72, -36), hours_pf(-36, 0))),
    _c("hours_bundled_active_hour_ignored", combo(hours_bundled(-72, 1), hours_pf(-72, 1)), **LIVE, live="rows"),
    _c("hours_tmp_files_perfield", lambda lay: lay.field_hours(F3, range(-72, 0), ".tmp_part.parquet")),
    _c("hours_nonparquet_files", lambda lay: lay.field_hours(F3, range(-72, 0), "part-0.json")),
    _c("hours_window_mid_hour", hours_pf(), st=-71.5, et=-0.5),
    _c("hours_window_1h", hours_pf(), st=-1, et=0),
    _c("hours_window_single_hour_bundled", hours_bundled(), st=-5, et=-4),
    _c("no_root", nothing),
    _c("no_files", lambda lay: lay.hour.mkdir(parents=True), st=-20, et=0),
    # ── per-field days ──
    _c("days_perfield", days_pf(D), **W3D),
    _c("days_perfield_plus_hours", combo(days_pf(D), hours_pf(-82, -10)), **W3D),
    _c("days_perfield_residual_hours", combo(days_pf([-3, -2]), hours_pf(-34, -10)), **W3D),
    _c(
        "days_perfield_country_only_with_bundled_hours",
        combo(days_pf(D, ["country"]), hours_bundled(-82, -10)),
        **W3D,
    ),
    _c(
        "days_perfield_country_only_with_perfield_hours",
        combo(days_pf(D, ["country"]), hours_pf(-82, -10)),
        **W3D,
    ),
    _c("days_boundary_start_partial", combo(days_pf(D), hours_pf(-82, -10)), st=-70, et=-10),
    _c("days_boundary_end_partial", combo(days_pf(D), hours_pf(-82, -10)), st=-82, et=-12),
    _c("days_boundary_mid_hour_start", combo(days_pf(D), hours_pf(-82, -10)), st=-81.5, et=-10),
    _c("days_tmp_only", lambda lay: lay.field_days(F3, D, ".tmp_part.parquet"), **W3D),
    _c("days_active_day_ignored", combo(days_pf([0]), hours_pf(-82, 0)), st=-82, et=0.5, live="rows"),
    _c("days_empty_dirs_with_hours", combo(lambda lay: lay.empty_day_dirs(F3, D), hours_pf(-82, -10)), **W3D),
    _c("days_empty_dirs_no_hours_heal", lambda lay: lay.empty_day_dirs(F3, D), st=-30, et=-10),
    _c(
        "days_bad_len_entry",
        lambda lay: (lay.day / "field=country" / "day=2026-3-1").mkdir(parents=True),
        **W3D,
    ),
    # ── bundled days ──
    _c("bdays_std", days_b(D), **W3D),
    _c("bdays_nested_alt", days_b(D, "nested_alt"), **W3D),
    _c("bdays_flat_alt", days_b(D, "flat_alt"), **W3D),
    _c("bdays_plus_bundled_hours", combo(days_b(D), hours_bundled(-82, -10)), **W3D),
    _c("bdays_plus_perfield_days", combo(days_b(D), days_pf(D)), **W3D),
    _c("bdays_plus_perfield_hours", combo(days_b([-3, -2]), hours_pf(-82, -10)), **W3D),
    _c("bdays_boundary_start_partial", combo(days_b(D), hours_bundled(-82, -10)), st=-70, et=-10),
    _c("bdays_single_field", days_b(D), fields=("city",), **W3D),
    _c("bdays_active_day_ignored", combo(days_b([0]), hours_pf(-82, 0)), st=-82, et=0.5, live="rows"),
    _c(
        "bdays_std_and_alt_same_day",
        combo(days_b([-2]), days_b([-2], "nested_alt"), days_b([-2], "flat_alt")),
        st=-58,
        et=-34,
    ),
    # ── missing hours / heal ──
    _c("heal_gap_perfield_dirs_covered", combo(hours_pf(skip=(-10,)), lambda lay: lay.field_hour_dirs(F3, [-10]))),
    _c("heal_gap_no_marker", hours_pf(skip=(-10,))),
    _c("heal_gap_covered_by_bundle_sentinel", combo(hours_pf(skip=(-10,)), hours_bundled(-10, -9))),
    _c("heal_gap_recent", hours_pf(skip=(-3,))),
    _c("heal_gap_beyond_cap", hours_pf(-120, 0, skip=(-100,)), st=-120),
    _c("heal_gap_consecutive", hours_pf(skip=(-12, -11, -10, -5))),
    _c("heal_gap_mid_hour_window", hours_pf(skip=(-10, -9)), st=-10.5, et=-8.25),
    _c("heal_gap_bounded_by_start", hours_pf(skip=(-60,)), st=-72),
    _c("heal_gap_no_end", hours_pf(-24, 0, skip=(-5,)), st=-24, et=None, live="rows"),
    _c("heal_gap_with_live", hours_pf(skip=(-10,)), **LIVE, live="rows"),
    _c("heal_gap_in_compacted_day", combo(days_pf(D), hours_pf(-82, -10, skip=(-20,))), st=-82, et=-6),
    _c("heal_gap_in_bundled_compacted_day", combo(days_b(D), hours_bundled(-82, -10, skip=(-20,))), st=-82, et=-6),
    _c(
        "heal_gap_in_crashed_compaction_day",
        combo(lambda lay: lay.empty_day_dirs(F3, [-1]), hours_pf(-34, -10, skip=(-20,))),
        st=-34,
        et=-6,
    ),
    _c("heal_whole_window_empty", lambda lay: lay.hour.mkdir(parents=True), st=-30, et=0),
    _c("heal_whole_window_empty_beyond_cap", lambda lay: lay.hour.mkdir(parents=True), st=-100, et=0),
    _c("heal_boom", hours_pf(skip=(-10,)), heal="boom"),
    _c("heal_none_tmp", hours_pf(skip=(-10,)), heal="none_tmp"),
    _c("heal_with_seeds", hours_pf(skip=(-10,)), seeds=True),
    _c("heal_schema_boom", hours_pf(skip=(-10,)), live="boom"),
    # ── live active hour ──
    _c("live_rows", hours_pf(), **LIVE, live="rows"),
    _c("live_rows_seeds", hours_pf(), **LIVE, live="rows", seeds=True),
    _c("live_empty_sentinel", hours_pf(), **LIVE, live="empty"),
    _c("live_zero_rows", hours_pf(), **LIVE, live="zero_rows"),
    _c("live_direct_none_fallback", hours_pf(), **LIVE, live="none_fallback"),
    _c("live_schema_boom", hours_pf(), **LIVE, live="boom"),
    _c("live_partial_rows", hours_pf(), **LIVE, live="rows", partial="rows"),
    _c("live_partial_narrow", hours_pf(), **LIVE, live="none_fallback", partial="narrow"),
    _c("live_window_ends_at_active_start", hours_pf(), st=-72, et=0, live="rows"),
    _c("live_window_starts_after_active", hours_pf(), st=1.2, et=2, live="rows"),
    _c("live_window_mid_active_hour", hours_pf(), st=0.1, et=0.25, live="rows"),
    _c("live_limits_pfl", hours_pf(), limit=2, pfl={"country": 3}, **LIVE, live="rows", partial="rows"),
    # ── rolled-read failure ──
    _c("rolled_boom", hours_pf(), rolled="boom"),
    _c("rolled_boom_live", hours_pf(), rolled="boom", **LIVE, live="rows"),
    # ── durable serving ──
    _c("durable_not_ready", hours_pf(), durable="not_ready", **LIVE, live="rows"),
    _c("durable_ready", hours_pf(), durable="ready"),
)


# ── harness ─────────────────────────────────────────────────────────────────


class _Rows:
    def __init__(self, rows: list[tuple], one: tuple | None = None) -> None:
        self._rows, self._one = rows, one

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self) -> tuple | None:
        return self._one


class _Log:
    def __init__(self, norm: _Norm) -> None:
        self.norm = norm
        self.lines: list[list[str]] = []

    def _rec(self, level: str, msg: str, args: tuple) -> None:
        try:
            text = msg % args if args else msg
        except TypeError:
            text = msg
        self.lines.append([level, self.norm.text(text)])

    def warning(self, msg: str, *a: Any, **k: Any) -> None:
        self._rec("warning", msg, a)

    def debug(self, msg: str, *a: Any, **k: Any) -> None:
        self._rec("debug", msg, a)

    def exception(self, msg: str, *a: Any, **k: Any) -> None:
        self._rec("exception", msg, a)

    def info(self, msg: str, *a: Any, **k: Any) -> None:
        self._rec("info", msg, a)


def _iso(off: float | None) -> str | None:
    return None if off is None else (H + timedelta(hours=off)).isoformat()


def _run(case: Case, tmp: Path, probe: _Probe) -> tuple[dict, dict[str, str]]:
    root = tmp / case.name
    build_rollup_golden_layout(root, case.layout, anchor=H)
    root.mkdir(parents=True, exist_ok=True)

    st = case.st_raw if case.st_raw is not None else _iso(case.st)
    et = _iso(case.et)
    norm = _Norm(H, root, H)
    src = make_source(str(root), name="svc", service_id="svc")
    if case.custom_fields:
        src["log_fields"] = {"custom_fields": [{"name": "edge_custom", "enabled": True, "show_in_dashboard": True}]}
    con = duckdb.connect()
    executed: list[tuple[str, Any]] = []
    batch: list[dict] = []
    ftmp: list[dict] = []
    direct: list[dict] = []
    partial_calls: list[dict] = []
    released: list[str] = []
    schema_calls = {"cols": 0, "types": 0}
    log = _Log(norm)
    phases: list[dict] = []
    try:
        runner = QueryRunner(con, src)

        def fake_execute(q: str, p: list | None = None) -> _Rows:
            executed.append((q, p))
            if q.startswith("SELECT field, value, SUM(count)"):
                if case.rolled == "boom":
                    raise duckdb.Error("boom")
                return _Rows(ROLLED_ROWS)
            if q.startswith("SELECT COUNT(*) FROM"):
                return _Rows([], (0 if case.live == "zero_rows" else 5,))
            return _Rows([], (0,))

        def get_cols() -> list[str]:
            schema_calls["cols"] += 1
            if case.live == "boom":
                raise RuntimeError("schema boom")
            return list(COLS)

        def get_schema(_con: Any, _src: Any) -> list[dict]:
            schema_calls["types"] += 1
            return [{"name": c, "type": "VARCHAR"} for c in COLS]

        def make_direct(fields: list[str], cols: list[str], ls: datetime, le: datetime) -> str | None:
            direct.append(
                {
                    "fields": list(fields),
                    "cols": len(cols),
                    "start": norm.text(ls.isoformat()),
                    "end": norm.text(le.isoformat()),
                }
            )
            return {"empty": "__empty__", "none_fallback": None}.get(case.live, "tmp_live")

        def make_filtered(fields: list[str], cols: list[str], table: str, where: str) -> str | None:
            ftmp.append({"fields": list(fields), "cols": len(cols), "table": table, "where": norm.text(where)})
            if "strftime" in where:
                if case.heal == "boom":
                    raise RuntimeError("heal boom")
                return None if case.heal == "none_tmp" else "tmp_heal"
            return "tmp_livefb"

        def fake_batch(fields: list[str], table: str, cols: list[str], types: dict, **kw: Any) -> tuple[list, Any]:
            batch.append({"fields": list(fields), "table": table, "cols": len(cols), "types": len(types), **kw})
            return (HEAL_ROWS if table == "tmp_heal" else LIVE_ROWS), None

        def fake_partial(live_start: datetime, token: str, end: datetime) -> tuple[datetime, list, int]:
            partial_calls.append(
                {
                    "start": norm.text(live_start.isoformat()),
                    "token": norm.text(token),
                    "end": norm.text(end.isoformat()),
                }
            )
            if case.partial == "narrow":
                return live_start + timedelta(minutes=10), list(PARTIAL_ROWS), 0
            return live_start, (list(PARTIAL_ROWS) if case.partial == "rows" else []), 0

        runner.execute = fake_execute  # type: ignore[method-assign]
        runner.get_schema_cols = get_cols  # type: ignore[method-assign]
        runner._create_active_hour_temp_direct = make_direct  # type: ignore[method-assign]
        runner.create_filtered_temp_table = make_filtered  # type: ignore[method-assign]
        runner.execute_top_n_batch = fake_batch  # type: ignore[method-assign]
        runner._partial_hour_adjusted_live_start = fake_partial  # type: ignore[method-assign]
        runner.release_active_direct_temp = released.append  # type: ignore[method-assign]

        rollup_coverage._listdir_cache.clear()
        rr.reset_rollup_coverage_ready()
        _base._EMPTY_ROLLUP_WARN_TS.clear()
        _base._MISSING_HOUR_HEAL_WARN_TS.clear()
        probe.counts = {}
        patches = [patch.object(_base, "_get_schema", get_schema), patch.object(_base, "_logger", log)]
        if case.durable:
            patches.append(patch("backend.config.is_durable_serving_mode", return_value=True))
            if case.durable == "ready":
                rr.mark_rollup_coverage_ready("svc")
        for p in patches:
            p.start()
        kwargs: dict[str, Any] = {"per_field_limits": case.pfl}
        if case.phase_log:
            kwargs["_phase_log"] = phases
        if case.seeds:
            kwargs["actual_cols"] = list(COLS)
            kwargs["schema_types"] = {c: "VARCHAR" for c in COLS}
        probe.on = True
        try:
            res = runner.execute_top_n_rollups(list(case.fields), st, et, limit=case.limit, **kwargs)
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
    texts = {_digest(t): t for t in sql_texts}
    out = {
        "result": None if res is None else [[f, v, c] for f, v, c in res[0]],
        "echoed_fields": None if res is None else res[1],
        "raised": raised,
        "executes": len(executed),
        "sql": [_digest(t) for t in sql_texts],
        "paths": [_compact(norm.paths(q)) for q, _ in executed],
        "direct": direct,
        "filtered_tmp": ftmp,
        "batch": batch,
        "partial_calls": partial_calls,
        "released": released,
        "schema_calls": schema_calls,
        "phases": [[p["section"], p["time_ms"] if ":n_" in p["section"] else None] for p in phases],
        "logs": log.lines,
        "probes": dict(sorted(probe.counts.items())),
    }
    return out, texts


def _load() -> dict:
    return json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else {}


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_top_n_matches_golden(case: Case, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_first_heal_warning_fires_on_a_freshly_booted_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # time.monotonic() is host uptime; a never-warned service must warn even when uptime is
    # below the throttle interval (a cold CI runner or freshly rebooted VM).
    case = next(c for c in CASES if c.name == "no_root")
    monkeypatch.setattr(_base.time, "monotonic", lambda: 5.0)
    with time_machine.travel(FROZEN, tick=False):
        got, _ = _run(case, tmp_path, _Probe())

    assert any("live-healing" in line[1] for line in got["logs"])


# ── IP spread rollups golden + unit tests (merged from test_ip_spread_rollups_golden.py) ──

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME  # noqa: E402
from backend.repositories.top_n_rollups import (  # noqa: E402
    IpSpreadPlan,
    TopNWindow,
    build_ip_spread_sql,
    fold_sketches,
    plan_ip_spread_paths,
    spread_meta,
)
from backend.utils.hll import HyperLogLog  # noqa: E402

IP_SPREAD_GOLDEN_PATH = Path(__file__).parent / "golden" / "ip_spread_rollups.json"
IP_SPREAD_FIELDS = ["country", "ua"]


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
class IpSpreadCase:
    name: str
    layout: Callable[[Layout], None]
    fields: tuple[str, ...] = tuple(IP_SPREAD_FIELDS)
    st: float | None = -24
    et: float | None = 0
    rows: str = "normal"  # normal | dirty | empty | boom
    durable: str | None = None
    phase_log: bool = True


def _ip_spread_bundles(lo: int = -24, hi: int = 0, skip: tuple[int, ...] = (), fname: str = IP_SPREAD_BUNDLE_FILENAME):
    def b(lay: Layout) -> None:
        lay.bundles([k for k in range(lo, hi) if k not in skip], fname)

    return b


def _ip_spread_perfield(lo: int = -24, hi: int = 0, fields: list[str] | None = None, skip: tuple[int, ...] = ()):
    def b(lay: Layout) -> None:
        lay.perfield_hours(fields or IP_SPREAD_FIELDS, [k for k in range(lo, hi) if k not in skip])

    return b


def _ip_spread_combo(*fns: Callable[[Layout], None]):
    def b(lay: Layout) -> None:
        for fn in fns:
            fn(lay)

    return b


def _ip_spread_nothing(lay: Layout) -> None:
    lay.root.mkdir(parents=True, exist_ok=True)


IP_SPREAD_CASES: tuple[IpSpreadCase, ...] = (
    IpSpreadCase("bundled_full", _ip_spread_bundles()),
    IpSpreadCase("perfield_full", _ip_spread_perfield()),
    IpSpreadCase("bundled_and_perfield_overlap", _ip_spread_combo(_ip_spread_bundles(-24, -12), _ip_spread_perfield())),
    IpSpreadCase(
        "bundled_and_perfield_disjoint",
        _ip_spread_combo(_ip_spread_bundles(-24, -12), _ip_spread_perfield(-12, 0)),
    ),
    IpSpreadCase("perfield_one_field_only", _ip_spread_perfield(fields=["country"])),
    IpSpreadCase("perfield_gap_hours", _ip_spread_perfield(skip=(-5, -6, -7))),
    IpSpreadCase("bundled_gap_hours", _ip_spread_bundles(skip=(-5, -6, -7))),
    IpSpreadCase("bundled_wrong_filename", _ip_spread_bundles(fname="all_fields.parquet")),
    IpSpreadCase(
        "perfield_tmp_files",
        lambda lay: lay.perfield_hours(IP_SPREAD_FIELDS, range(-24, 0), ".tmp_part.parquet"),
    ),
    IpSpreadCase(
        "perfield_nonparquet",
        lambda lay: lay.perfield_hours(IP_SPREAD_FIELDS, range(-24, 0), "part-0.json"),
    ),
    IpSpreadCase("perfield_unrequested_field", _ip_spread_perfield(fields=["other"])),
    IpSpreadCase("no_trees", _ip_spread_nothing),
    IpSpreadCase("empty_bundled_root", lambda lay: lay.bundled.mkdir(parents=True)),
    IpSpreadCase("empty_perfield_root", lambda lay: lay.perfield.mkdir(parents=True)),
    IpSpreadCase(
        "active_hour_excluded",
        _ip_spread_combo(_ip_spread_bundles(-24, 1), _ip_spread_perfield(-24, 1)),
        st=-24,
        et=0.5,
    ),
    IpSpreadCase("window_start_mid_hour", _ip_spread_bundles(), st=-10.5, et=-2),
    IpSpreadCase("window_end_mid_hour", _ip_spread_bundles(), st=-10, et=-2.25),
    IpSpreadCase("window_end_on_boundary", _ip_spread_bundles(), st=-10, et=-2),
    IpSpreadCase("window_no_start", _ip_spread_bundles()),
    IpSpreadCase("window_no_end", _ip_spread_bundles(), st=-5, et=None),
    IpSpreadCase("window_no_bounds", _ip_spread_bundles(), st=None, et=None),
    IpSpreadCase("window_after_active", _ip_spread_bundles(-24, 3), st=1.2, et=2),
    IpSpreadCase("window_single_hour", _ip_spread_bundles(), st=-5, et=-4),
    IpSpreadCase(
        "shape_single_field",
        _ip_spread_combo(_ip_spread_bundles(), _ip_spread_perfield()),
        fields=("country",),
    ),
    IpSpreadCase(
        "shape_unsafe_mixed",
        _ip_spread_combo(_ip_spread_bundles(), _ip_spread_perfield()),
        fields=("country", "bad'field", "has space"),
    ),
    IpSpreadCase("shape_all_unsafe", _ip_spread_bundles(), fields=("bad'field",)),
    IpSpreadCase("shape_empty_fields", _ip_spread_bundles(), fields=()),
    IpSpreadCase("shape_no_phase_log", _ip_spread_bundles(), phase_log=False),
    IpSpreadCase("rows_dirty_bundled", _ip_spread_bundles(), rows="dirty"),
    IpSpreadCase(
        "rows_dirty_mixed", _ip_spread_combo(_ip_spread_bundles(-24, -12), _ip_spread_perfield()), rows="dirty"
    ),
    IpSpreadCase("rows_empty", _ip_spread_bundles(), rows="empty"),
    IpSpreadCase("rows_boom", _ip_spread_bundles(), rows="boom"),
    IpSpreadCase("rows_boom_perfield", _ip_spread_perfield(), rows="boom"),
    IpSpreadCase(
        "meta_coverage_union",
        _ip_spread_combo(_ip_spread_bundles(-24, -20), _ip_spread_perfield(-20, 0, fields=["country"])),
        rows="normal",
    ),
    IpSpreadCase("durable_not_ready", _ip_spread_bundles(), durable="not_ready"),
    IpSpreadCase("durable_ready", _ip_spread_bundles(), durable="ready"),
)


class _IpSpreadRows:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


def _jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {(" | ".join(k) if isinstance(k, tuple) else k): _jsonable(x) for k, x in v.items()}
    return v


def _run_ip_spread(case: IpSpreadCase, tmp: Path, probe: _Probe) -> tuple[dict, dict[str, str]]:
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

        def fake_execute(q: str, p: list | None = None) -> _IpSpreadRows:
            executed.append((q, p))
            if case.rows == "boom":
                raise duckdb.Error("boom")
            return _IpSpreadRows({"normal": ROWS_NORMAL, "dirty": ROWS_DIRTY, "empty": []}[case.rows])

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


def _load_ip_spread_golden() -> dict:
    return json.loads(IP_SPREAD_GOLDEN_PATH.read_text()) if IP_SPREAD_GOLDEN_PATH.exists() else {}


@pytest.mark.parametrize("case", IP_SPREAD_CASES, ids=[c.name for c in IP_SPREAD_CASES])
def test_ip_spread_matches_golden(case: IpSpreadCase, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _Probe()
    for name in _STAT_FNS:
        monkeypatch.setattr(os.path, name, probe.wrap(name, getattr(os.path, name)))
    real_listdir = os.listdir
    monkeypatch.setattr(os, "listdir", probe.wrap("listdir", lambda p: sorted(real_listdir(p))))
    with time_machine.travel(FROZEN, tick=False):
        got, texts = _run_ip_spread(case, tmp_path, probe)
    monkeypatch.undo()
    shutil.rmtree(tmp_path / case.name, ignore_errors=True)
    got = json.loads(json.dumps(got, sort_keys=True))

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        golden = _load_ip_spread_golden()
        golden[case.name] = got
        golden["_sql"] = {**golden.get("_sql", {}), **texts}
        IP_SPREAD_GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        IP_SPREAD_GOLDEN_PATH.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return

    want = _load_ip_spread_golden().get(case.name)
    assert want is not None, f"no golden for {case.name}; run with UPDATE_ROLLUP_GOLDEN=1"
    g_probes, w_probes = got.pop("probes"), want.pop("probes")
    if got != want:
        sqls = _load_ip_spread_golden().get("_sql", {})
        drift = "\n".join(f"got: {texts.get(h)}\nwant: {sqls.get(h)}" for h in got["sql"] if h not in want["sql"])
        raise AssertionError(f"{case.name}: behavior drifted from golden\n{got}\n{want}\n{drift}")
    for k, v in g_probes.items():
        assert v <= w_probes.get(k, 0), f"{case.name}: {k} probes {v} > golden {w_probes.get(k, 0)}"


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


# ── Merged from tests/repositories/test_time_series_rollup.py ──

"""Regression tests for QueryRunner.try_time_series_from_rollup.

The function had zero direct test coverage when it shipped, and the cursor
iterating in the request's input timezone (instead of UTC) silently dropped
hours from the response when the FE sent timezone-offset strings — see the
2026-06-11 missing-tail bar-chart incident. These tests pin the contract
explicitly so a regression would fail at CI time.
"""


from datetime import UTC, datetime, timezone
from pathlib import Path

import pytest

from backend.repositories.dashboard import get_aggregates


def _write_bundle(bundled_root: Path, hour_str: str, total_requests: int = 600) -> None:
    """Create a minimal time_series.parquet under bundled_root with rows for
    every minute of ``hour_str`` (UTC). One row per minute; the per-hour sum is
    deterministic at total_requests.
    """
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / "time_series.parquet"

    base = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    per_min = total_requests // 60
    rows_sql = ", ".join(
        f"(TIMESTAMPTZ '{(base + timedelta(minutes=m)).isoformat()}', {per_min}, 0, 0, 0, 0, 0, 0.0, 0, '{hour_str}')"
        for m in range(60)
    )
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) "
            f"AS t(bucket, requests, status_4xx, status_5xx, hits, cache_total, "
            f"resp_bytes_sum, ttfb_sum, ttfb_count, hour)) "
            f"TO '{out}' (FORMAT PARQUET)"
        )
    finally:
        con.close()


def _write_per_field_marker(per_field_root: Path, field: str, hour_str: str) -> None:
    """Create the per-field rollup dir so _hour_had_any_data sees the hour.

    The bundled-root reader iterates closed hours and, on a missing bundle,
    checks the per-field tree to decide between "skip (no data this hour)"
    and "fall back to raw (data exists but bundle is mid-build)". Tests
    create both halves so the reader behaves like in production.
    """
    (per_field_root / f"field={field}" / f"hour={hour_str}").mkdir(parents=True, exist_ok=True)


def _write_empty_all_fields_sentinel(bundled_root: Path, hour_str: str) -> None:
    """Create the verified-empty bundle written for a zero-traffic hour."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())])
    pq.write_table(pa.table({"field": [], "value": [], "count": []}, schema=schema), hour_dir / "all_fields.parquet")


def _write_nonempty_all_fields_bundle(bundled_root: Path, hour_str: str) -> None:
    """Create a normal non-empty all-fields bundle without time-series data."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {"field": ["country"], "value": ["US"], "count": [1]},
            schema=pa.schema([("field", pa.string()), ("value", pa.string()), ("count", pa.int64())]),
        ),
        hour_dir / "all_fields.parquet",
    )


@pytest.fixture
def rollup_layout(tmp_path):
    """Build a fake rollup layout under tmp_path and return the bundled root."""
    cache_dir = tmp_path / "cache" / "test-bucket"
    bundled = cache_dir / "rollups" / "hour_bundled"
    per_field = cache_dir / "rollups" / "hour"
    bundled.mkdir(parents=True)
    per_field.mkdir(parents=True)
    return bundled, per_field, str(cache_dir)


def _populate_past_window(
    bundled, per_field, *, hours_back_end: int, span_hours: int
) -> tuple[datetime, datetime, list[str]]:
    """Write bundles + per-field markers for ``span_hours`` UTC hours ending
    ``hours_back_end`` hours before "now". Returns (start_utc, end_utc, hours).

    Using a window in the past keeps the active-hour boundary irrelevant —
    every cursor string is < active_hour_str, so crosses_active stays False
    and the rollup reader doesn't need to invoke the live branch. This makes
    the cursor-iteration assertion deterministic without freezing time.
    """
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = now - timedelta(hours=hours_back_end)
    start = end - timedelta(hours=span_hours)
    hours = []
    cursor = start
    while cursor < end:
        hs = cursor.strftime("%Y-%m-%d-%H")
        hours.append(hs)
        _write_bundle(bundled, hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", hs)
        cursor += timedelta(hours=1)
    return start, end, hours


@pytest.mark.parametrize(
    ("start_seconds", "end_seconds", "expected_count", "expected_source"),
    [
        (0, 3600, 5, "rollup"),
        (30, 3600, 5, "raw"),
        (0.000001, 3600, 5, "raw"),
        (0, 3570, 2, "raw"),
    ],
)
def test_dashboard_count_preserves_exact_inclusive_window(
    rollup_layout, monkeypatch, start_seconds, end_seconds, expected_count, expected_source
):
    bundled, per_field, cache_dir = rollup_layout
    hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
    hour_key = hour.strftime("%Y-%m-%d-%H")
    hour_dir = bundled / f"hour={hour_key}"
    hour_dir.mkdir()
    _write_per_field_marker(per_field, "requests", hour_key)
    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)

    with duckdb.connect() as con:
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, status VARCHAR)")
        con.executemany(
            "INSERT INTO logs_test_service VALUES (?, '200')",
            [(hour + timedelta(seconds=offset),) for offset in (45, 60, 3590, 3600, 3600)],
        )
        con.sql(
            "SELECT date_trunc('minute', timestamp) AS bucket, count(*) AS requests "
            "FROM logs_test_service WHERE timestamp < ? GROUP BY 1",
            params=[hour + timedelta(hours=1)],
        ).write_parquet(str(hour_dir / "time_series.parquet"))
        start, end = hour + timedelta(seconds=start_seconds), hour + timedelta(seconds=end_seconds)
        raw_count = con.execute(
            "SELECT count(*) FROM logs_test_service WHERE timestamp >= ? AND timestamp <= ?",
            [start, end],
        ).fetchone()[0]
        result = get_aggregates(
            con=con,
            src=make_source(cache_dir),
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            filters={},
            chart_interval="1 minute",
            chart_metric="requests",
            include_time_series=True,
            include_conn_requests=False,
            include_map_data=False,
            include_top_n=False,
        )

    assert raw_count == expected_count
    assert result["total_rows"] == raw_count
    assert result["history"]["window_count_source"] == expected_source
    count_observation = next(row for row in result["reader_coverage"] if row["reader"] == "dashboard.count")
    assert count_observation["eligibility_reason"] == (
        "eligible" if expected_source == "rollup" else "unsupported_interval"
    )
    if expected_source == "raw":
        assert sum(row["value"] for row in result["time_series"]) == raw_count
        chart_observation = next(row for row in result["reader_coverage"] if row["reader"] == "dashboard.time_series")
        assert chart_observation["eligibility_reason"] == "unsupported_interval"


class TestTryTimeSeriesFromRollup:
    """Pin try_time_series_from_rollup's cursor + window semantics."""

    def test_cdt_offset_input_serves_full_utc_window(self, rollup_layout):
        """REGRESSION: the cursor must iterate in UTC, not in the input's TZ.

        Bug history: when start_time was a CDT-offset string like
        '...-05:00', the reader's cursor inherited tz=CDT from
        datetime.fromisoformat. cursor.strftime('%Y-%m-%d-%H') then
        produced CDT-named hour strings, but the bundles on disk are
        keyed by UTC hours. The names don't match — so this test (whose
        bundles exist only under UTC names) would have returned no rows
        if the bug were present.

        Pre-fix observed in prod: 5 hours dropped from the 24h chart.
        Pre-fix in this test: 0 hours returned (bundles never matched).
        """
        bundled, per_field, cache_dir = rollup_layout
        start_utc, end_utc, hours = _populate_past_window(bundled, per_field, hours_back_end=1, span_hours=24)
        assert len(hours) == 24

        # Re-express the same wall-clock instants with a CDT offset
        # (UTC-5) — that's the actual bug trigger.
        cdt = timezone(timedelta(hours=-5))
        start_cdt_iso = start_utc.astimezone(cdt).isoformat()
        end_cdt_iso = end_utc.astimezone(cdt).isoformat()

        src = make_source(cache_dir)
        con = duckdb.connect()
        try:
            runner = QueryRunner(con, src)
            rows = runner.try_time_series_from_rollup(
                chart_metric="requests",
                interval="1 hour",
                start_time=start_cdt_iso,
                end_time=end_cdt_iso,
                table_name="not_used_when_only_rollup_hours",
                where_clause="1=1",
                params=[],
            )
        finally:
            con.close()

        assert rows is not None, (
            "rollup reader returned None — the eligibility check failed or "
            "bundles weren't found (cursor-tz bug would cause this)."
        )
        # All 24 closed-hour UTC bundles should be served. Pre-fix: 0
        # because cursor iterated CDT hours and the per-field markers
        # (also UTC-named) wouldn't match either, so every hour was
        # silently 'skipped' as "no data".
        assert len(rows) == 24, (
            f"expected 24 hourly buckets covering the full UTC window, got "
            f"{len(rows)}. Sample: first={rows[0] if rows else None}, "
            f"last={rows[-1] if rows else None}. "
            f"A length of 0 strongly suggests the cursor-tz regression."
        )
        # Spot-check the actual content: the first and last UTC hours
        # match what we wrote. DuckDB serializes TIMESTAMPTZ in the
        # session tz, so the response strings may carry a non-UTC offset
        # on dev machines — compare as parsed datetimes to be tz-agnostic.
        row_instants = {datetime.fromisoformat(r["time"]).astimezone(UTC) for r in rows}
        assert start_utc in row_instants, (
            f"first UTC hour {start_utc.isoformat()} missing from response — "
            f"likely cursor-tz regression (sample times: {[r['time'] for r in rows[:3]]})"
        )
        last_closed_utc = end_utc - timedelta(hours=1)
        assert last_closed_utc in row_instants, (
            f"last UTC hour {last_closed_utc.isoformat()} missing — "
            f"likely cursor never reached the end of the UTC window "
            f"(sample tail: {[r['time'] for r in rows[-3:]]})"
        )

    def test_utc_offset_input_also_serves_full_window(self, rollup_layout):
        """Sibling: UTC-offset input (+00:00) also yields the full window.

        The pre-fix code path happened to work for UTC-offset input because
        the cursor's tz was already UTC. Pin that the UTC path still works
        after the fix so a future 'force input tz' regression would also
        be caught.
        """
        bundled, per_field, cache_dir = rollup_layout
        start_utc, end_utc, hours = _populate_past_window(bundled, per_field, hours_back_end=1, span_hours=24)

        src = make_source(cache_dir)
        con = duckdb.connect()
        try:
            runner = QueryRunner(con, src)
            rows = runner.try_time_series_from_rollup(
                chart_metric="requests",
                interval="1 hour",
                start_time=start_utc.isoformat(),  # +00:00
                end_time=end_utc.isoformat(),
                table_name="not_used",
                where_clause="1=1",
                params=[],
            )
        finally:
            con.close()

        assert rows is not None
        assert len(rows) == 24, f"UTC-offset path also expected 24 buckets, got {len(rows)}"


class TestActiveHourDirectLiveSlice:
    """The live active-hour slice must come from the direct buffer/hourly
    read when the caller declares the window unfiltered — never the bound
    view (which pays manifest/union overhead per load)."""

    def test_crosses_active_serves_live_slice_from_buffer_not_view(self, rollup_layout):
        """table_name deliberately does NOT exist: if the reader ever routes
        the live slice through the view branch, the final query raises and
        the reader returns None — failing this test loudly."""
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        # Two fully-bundled closed hours before the active hour.
        cursor = active_start - timedelta(hours=2)
        while cursor < active_start:
            hs = cursor.strftime("%Y-%m-%d-%H")
            _write_bundle(bundled, hs, total_requests=600)
            _write_per_field_marker(per_field, "requests", hs)
            cursor += timedelta(hours=1)

        # Buffer parquet carrying 3 active-hour rows.
        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        ts = (active_start + timedelta(minutes=5)).isoformat()
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES (TIMESTAMPTZ '{ts}'), (TIMESTAMPTZ '{ts}'), "
                f"(TIMESTAMPTZ '{ts}')) AS t(timestamp)) "
                f"TO '{buffer_dir / 'live.parquet'}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        st = (active_start - timedelta(hours=2)).isoformat()
        et = (active_start + timedelta(minutes=30)).isoformat()
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=st,
            end_time=et,
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert rows is not None, "reader fell back to the (nonexistent) view for the live slice"
        # Key by parsed datetime — safe_iso's exact string form (offset vs Z)
        # is not part of this test's contract.
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        assert sum(by_time.values()) == 600 + 600 + 3
        assert by_time[active_start] == 3

    def test_filtered_window_never_uses_direct_live_read(self, rollup_layout):
        """Without unfiltered_window=True the live slice MUST go through the
        table/where_clause branch (row filters would be silently dropped by
        the direct read). Pinned the same way: a nonexistent table means the
        reader must return None instead of serving a filter-ignoring result."""
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", hs)

        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        ts = (active_start + timedelta(minutes=5)).isoformat()
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM (VALUES (TIMESTAMPTZ '{ts}')) AS t(timestamp)) "
                f"TO '{buffer_dir / 'live.parquet'}' (FORMAT PARQUET)"
            )
        finally:
            con.close()

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="url = ?",
            params=["/x"],
        )

        assert rows is None, "filtered window must not serve the live slice via the direct read"


class TestPartialHourMergeIntoRollupReaders:
    """The partial-hour speed layer (Task 7's ``_partial_hour_adjusted_live_start``)
    must be wired into BOTH the time_series and count readers' live branches:
    narrowing ``live_start`` to the partial rollup's watermark (so the direct
    live read never re-scans rows the partial rollup already folded in) AND
    summing the partial rollup's rows into the response.

    Each test is built so it fails if EITHER half of the wiring is missing:
    dropping the narrowing double-counts the watermark-covered buffer row,
    dropping the partial-rows merge undercounts by that same row — neither
    produces the expected total.
    """

    def _merge_partial_hour_via_writer(
        self,
        cache_dir: str,
        active_start: datetime,
        *,
        watermark_instant: datetime,
        service_id: str = "svc-a",
        fields: tuple[str, ...] = ("country", "method"),
    ) -> None:
        """Feed ONE real request (with TWO populated fields) through the
        actual merge_partial_hour writer so the partial rollup these tests
        exercise is built the same way the cron job builds it in prod (real
        per-field rows + a genuine ``__total__`` row) — not a
        hand-fabricated ``('requests', '', count)`` row a real writer never
        produces.

        Deliberately populates MORE THAN ONE field for this single request:
        the C1 bug (final whole-branch review) is summing partial_rows
        across every field, which is invisible with only one field merged
        (1 row summed == 1 row read from __total__ either way) and only
        shows up once more than one field is populated (pre-fix, this
        single request would be counted twice — once per field).

        Writes the fixture row into the hourly-partition directory (like
        ``merge_partial_hour``'s own real inputs) with an event timestamp of
        exactly ``active_start`` (always safely before ``watermark_instant``
        for any watermark after the hour begins) and pins the file's mtime
        to ``watermark_instant`` via ``os.utime`` so the writer's own
        watermark computation (``max(mtime of new_files)``) is deterministic
        for the test — mirroring how a real file's mtime is just a
        filesystem timestamp, independent of the row content the C1 fix
        cares about getting right.
        """
        import os

        import backend.core.rollups.day_bundles as ph

        hour = active_start.strftime("%Y-%m-%d-%H")
        hourly_dir = Path(cache_dir) / "data" / f"timestamp_hour={hour}"
        hourly_dir.mkdir(parents=True, exist_ok=True)
        out_path = hourly_dir / "batch1.parquet"
        cols_sql = ", ".join(f"{f} VARCHAR" for f in fields)
        placeholders = ", ".join("?" for _ in fields)
        con = duckdb.connect(":memory:")
        try:
            con.execute("SET TimeZone='UTC';")
            con.execute(f"CREATE TABLE t (timestamp TIMESTAMP, {cols_sql})")
            con.execute(
                f"INSERT INTO t VALUES (?, {placeholders})",
                [active_start, *[f"{f}-value" for f in fields]],
            )
            con.execute(f"COPY t TO '{out_path}' (FORMAT PARQUET)")
        finally:
            con.close()
        ts = watermark_instant.timestamp()
        os.utime(out_path, (ts, ts))

        stats = ph.merge_partial_hour(service_id, {"_cache_dir_override": cache_dir}, list(fields))
        assert stats["new_files"] == 1

    def _write_buffer_rows(self, cache_dir: str, *timestamps: datetime) -> None:
        """Write buffer rows and pin the file's mtime to the LATEST of the
        given timestamps (never the real wall-clock "now").

        _create_active_hour_temp_direct prunes buffer files by mtime (a
        file finalized before `live_start - skew margin` cannot hold rows
        >= live_start — see backend/repositories/_base.py). Using the real
        wall-clock mtime made this test's pass/fail depend on what minute
        of the real hour it happened to run in: if the suite ran within the
        first few minutes of an hour, the file's real mtime could fall
        BELOW the margin floor and get pruned entirely, silently dropping
        the "not yet merged" row and undercounting — a genuine, observed
        flake (601 instead of 602), not a hypothetical one. Pinning the
        mtime to the row's own timestamp makes the fixture deterministic
        regardless of wall-clock timing.
        """
        import os

        buffer_dir = Path(cache_dir) / "buffer"
        buffer_dir.mkdir(parents=True, exist_ok=True)
        values_sql = ", ".join(f"(TIMESTAMPTZ '{ts.isoformat()}')" for ts in timestamps)
        out_path = buffer_dir / "live.parquet"
        con = duckdb.connect()
        try:
            con.execute(f"COPY (SELECT * FROM (VALUES {values_sql}) AS t(timestamp)) TO '{out_path}' (FORMAT PARQUET)")
        finally:
            con.close()
        mtime = max(ts.timestamp() for ts in timestamps)
        os.utime(out_path, (mtime, mtime))

    def test_time_series_merges_partial_hour_without_double_counting(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        # One closed hour before the active hour.
        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        # The real writer folds in one row at active_start (always before
        # the watermark below) — this is the "already merged" row. A
        # second, separate buffer row lands AFTER the watermark and must be
        # picked up by the narrowed live scan instead.
        watermark_instant = active_start + timedelta(minutes=10)
        not_yet_merged_row = active_start + timedelta(minutes=20)
        self._merge_partial_hour_via_writer(cache_dir, active_start, watermark_instant=watermark_instant)
        self._write_buffer_rows(cache_dir, not_yet_merged_row)

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert rows is not None, "reader fell back to raw — partial-hour wiring likely broke the live SQL"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        # Expected: 600 (closed hour) + 1 (partial rollup's already-merged
        # row) + 1 (live direct read's not-yet-merged row) = 602.
        # If narrowing were dropped: the live direct read would ALSO count
        # the already-merged row (its business timestamp is still >=
        # active_start), giving 603 (double count).
        # If the partial-rows merge were dropped: the already-merged row
        # would never surface at all (the narrowed live scan explicitly
        # excludes it), giving 601 (undercount).
        assert by_time[active_start] == 2, f"active-hour bucket should sum to 2 (1 partial + 1 live), got {by_time}"
        assert sum(by_time.values()) == 602, f"expected 602 total, got {sum(by_time.values())} ({by_time})"

    def test_count_merges_partial_hour_without_double_counting(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        watermark_instant = active_start + timedelta(minutes=10)
        not_yet_merged_row = active_start + timedelta(minutes=20)
        self._merge_partial_hour_via_writer(cache_dir, active_start, watermark_instant=watermark_instant)
        self._write_buffer_rows(cache_dir, not_yet_merged_row)

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert total is not None, "reader fell back to raw — partial-hour wiring likely broke the live SQL"
        assert total == 602, f"expected 602 (600 closed + 1 partial + 1 live), got {total}"

    def test_non_requests_metric_keeps_full_live_scan_when_partial_hour_exists(self, rollup_layout):
        """REGRESSION (Task 8 review round 1): only "requests" has a
        compensating partial-rollup merge. Before this fix, live_start was
        narrowed for EVERY chart_metric whenever a partial-hour rollup
        watermark existed — for "5xx"/"4xx"/"hit_rate" (no compensating
        merge), that silently dropped the
        [original_live_start, watermark) range from BOTH the rollup branch
        (still-open hour, not covered) and the narrowed live branch
        (skipped) — real data loss, not just "no active-hour boost".

        This pins that a non-"requests" metric's live branch keeps scanning
        the FULL original live range regardless of whether a partial-hour
        rollup happens to exist for the active hour.
        """
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

        closed_hs = (active_start - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
        _write_bundle(bundled, closed_hs, total_requests=600)
        _write_per_field_marker(per_field, "requests", closed_hs)

        # A partial-hour rollup watermark exists for the active hour (the
        # steady-state case) — this alone must NOT narrow live_start for a
        # metric with no compensating merge.
        watermark_instant = active_start + timedelta(minutes=10)
        self._merge_partial_hour_via_writer(cache_dir, active_start, watermark_instant=watermark_instant)

        before_watermark_row = active_start + timedelta(minutes=5)  # status 500 -> counts as 5xx
        after_watermark_row = active_start + timedelta(minutes=20)  # status 200 -> does not

        con = duckdb.connect()
        table_name = "live_5xx_regression_table"
        con.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM (VALUES "
            f"(TIMESTAMPTZ '{before_watermark_row.isoformat()}', 500), "
            f"(TIMESTAMPTZ '{after_watermark_row.isoformat()}', 200)) "
            f"AS t(timestamp, status)"
        )

        runner = QueryRunner(con, make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="5xx",
            interval="1 hour",
            start_time=(active_start - timedelta(hours=1)).isoformat(),
            end_time=(active_start + timedelta(minutes=30)).isoformat(),
            table_name=table_name,
            where_clause="1=1",
            params=[],
        )

        assert rows is not None, "reader fell back to raw unexpectedly"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        # 1 of 2 active-hour rows is 5xx -> 50.0%. Pre-fix (unconditional
        # narrowing), the before_watermark_row would have been silently
        # excluded from the live scan with nothing compensating for it,
        # yielding 0.0 instead of 50.0.
        assert by_time[active_start] == 50.0, (
            f"expected 50.0 (1 of 2 active-hour rows is 5xx) — a value of 0.0 means the "
            f"before-watermark 5xx row was dropped by an unconditional narrowing, got {by_time}"
        )


class TestMissingHourLiveHealInTimeSeriesAndCountReaders:
    """REGRESSION (2026-09-09, verified live): unlike ``execute_top_n_rollups``
    (which live-heals a CLOSED hour with no rollup bundle), the time_series
    and count readers silently SKIP such an hour via
    ``collect_hourly_bundle_paths`` — contributing zero for it instead of
    live-healing. On a real deployment with a 9/24-hour writer-coverage gap,
    top-N panels stayed correct (they heal) while the traffic chart showed
    ``[]`` and the total-requests count showed ``0``, despite real traffic
    existing in those gap hours.

    Each test writes one CLOSED hour with a real rollup bundle (H1) and one
    CLOSED hour with NO bundle and NO per-field marker at all (H2) — a pure
    writer-coverage gap, not the mid-build case that already triggers
    ``collect_hourly_bundle_paths``'s "return None" fallback. H2's rows exist
    only in the raw base table (``logs_<service>``), so a correct reader must
    live-heal H2 to include them.
    """

    def test_time_series_heals_missing_bundle_hour(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        h1_start = active_start - timedelta(hours=2)
        h2_start = active_start - timedelta(hours=1)
        h1_str = h1_start.strftime("%Y-%m-%d-%H")

        # H1: normal, fully-bundled closed hour.
        _write_bundle(bundled, h1_str, total_requests=600)
        _write_per_field_marker(per_field, "requests", h1_str)

        # H2: writer-coverage gap — no bundle, no per-field marker either.
        # Its rows exist only in the raw base table.
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO logs_test_service VALUES (?), (?), (?), (?), (?)",
            [h2_start + timedelta(minutes=m) for m in (1, 2, 3, 4, 5)],
        )

        runner = QueryRunner(con, make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=h1_start.isoformat(),
            end_time=(h2_start + timedelta(hours=1)).isoformat(),
            table_name="not_used",
            where_clause="1=1",
            params=[],
        )
        con.close()

        assert rows is not None, "reader fell back to raw unexpectedly"
        by_time = {datetime.fromisoformat(r["time"]).astimezone(UTC): r["value"] for r in rows}
        assert by_time.get(h2_start) == 5, (
            f"H2 (writer-coverage gap hour) contributed {by_time.get(h2_start)!r} instead of 5 — "
            f"the missing-hour heal did not run. Full response: {by_time}"
        )
        assert sum(by_time.values()) == 605, f"expected 600 (H1) + 5 (healed H2) = 605, got {by_time}"

    def test_count_heals_missing_bundle_hour(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        active_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        h1_start = active_start - timedelta(hours=2)
        h2_start = active_start - timedelta(hours=1)
        h1_str = h1_start.strftime("%Y-%m-%d-%H")

        _write_bundle(bundled, h1_str, total_requests=600)
        _write_per_field_marker(per_field, "requests", h1_str)

        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO logs_test_service VALUES (?), (?), (?), (?), (?)",
            [h2_start + timedelta(minutes=m) for m in (1, 2, 3, 4, 5)],
        )

        runner = QueryRunner(con, make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=h1_start.isoformat(),
            end_time=(h2_start + timedelta(hours=1)).isoformat(),
            table_name="not_used",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )
        con.close()

        assert total is not None, "reader fell back to raw unexpectedly"
        assert total == 605, (
            f"expected 600 (H1 bundle) + 5 (healed H2) = 605, got {total} — "
            f"the missing-hour heal did not run, undercounting the writer-coverage gap hour."
        )

    def test_time_series_accepts_verified_empty_hour_beyond_heal_cap(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        end = now - timedelta(hours=10)
        start = end - timedelta(hours=50)
        empty_hour = start + timedelta(hours=17)

        cursor = start
        while cursor < end:
            hour_str = cursor.strftime("%Y-%m-%d-%H")
            if cursor == empty_hour:
                _write_empty_all_fields_sentinel(bundled, hour_str)
            else:
                _write_bundle(bundled, hour_str, total_requests=600)
            _write_per_field_marker(per_field, "requests", hour_str)
            cursor += timedelta(hours=1)

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        rows = runner.try_time_series_from_rollup(
            chart_metric="requests",
            interval="1 hour",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
        )

        assert rows is not None, "a verified-empty hour must not force a raw-scan fallback"
        assert len(rows) == 49, "the empty sentinel should contribute a zero-valued hour"

    def test_count_accepts_verified_empty_hour_beyond_heal_cap(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        end = now - timedelta(hours=10)
        start = end - timedelta(hours=50)
        empty_hour = start + timedelta(hours=17)

        cursor = start
        while cursor < end:
            hour_str = cursor.strftime("%Y-%m-%d-%H")
            if cursor == empty_hour:
                _write_empty_all_fields_sentinel(bundled, hour_str)
            else:
                _write_bundle(bundled, hour_str, total_requests=600)
            _write_per_field_marker(per_field, "requests", hour_str)
            cursor += timedelta(hours=1)

        runner = QueryRunner(duckdb.connect(), make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            table_name="this_view_does_not_exist",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )

        assert total == 49 * 600, "the verified-empty hour must not force a raw-scan fallback"

    def test_nonempty_all_fields_bundle_is_live_healed(self, rollup_layout):
        bundled, per_field, cache_dir = rollup_layout
        now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        end = now - timedelta(hours=10)
        start = end - timedelta(hours=50)
        missing_time_series_hour = start + timedelta(hours=17)

        cursor = start
        while cursor < end:
            hour_str = cursor.strftime("%Y-%m-%d-%H")
            if cursor == missing_time_series_hour:
                _write_nonempty_all_fields_bundle(bundled, hour_str)
            else:
                _write_bundle(bundled, hour_str, total_requests=600)
            _write_per_field_marker(per_field, "requests", hour_str)
            cursor += timedelta(hours=1)

        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ)")
        con.execute(
            "INSERT INTO logs_test_service VALUES (?), (?), (?)",
            [missing_time_series_hour + timedelta(minutes=m) for m in (1, 2, 3)],
        )

        runner = QueryRunner(con, make_source(cache_dir))
        total = runner.try_count_from_rollup(
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            table_name="logs_test_service",
            where_clause="1=1",
            params=[],
            unfiltered_window=True,
        )
        con.close()

        assert total == 49 * 600 + 3


# ── Merged from tests/repositories/_sql/test_sql_templates.py ──

import copy
import re

import pytest

import backend.repositories.assets as alerts_sql
import backend.repositories.assets as usage_sql
import backend.repositories.insights.definitions as insights_sql
import backend.repositories.network as network_sql
import backend.repositories.origin as origin_sql
import backend.repositories.security as security_sql
from backend.repositories import dashboard as dashboard_sql
from backend.repositories import query as query_sql
from backend.repositories import sessions as sessions_sql
from backend.repositories.insights import registry


def _placeholders(template: str) -> list[str]:
    return sorted({p.split("}")[0] for p in template.split("{")[1:] if "}" in p and p.split("}")[0]})


def _placeholders_all(template: str) -> list[str]:
    return sorted(p.split("}")[0] for p in template.split("{")[1:] if "}" in p)


def _as_list(val: str | list[str], sep: str = " | ") -> list[str]:
    return val.split(sep) if isinstance(val, str) else val


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (alerts_sql.MAX_TIMESTAMP, "table"),
        (dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N, "backing_col table_name where_clause requests_metric"),
        (dashboard_sql.CONN_REQUESTS_BUCKET, "requests_metric table_name where_clause"),
        (dashboard_sql.TIME_SERIES, "time_bucket_select value_expr table_name extra_where where_clause"),
        (dashboard_sql.MAP_DATA_BY_COUNTRY, "requests_metric table_name where_clause"),
        (dashboard_sql.FIELD_VALUES_BOT_UA, "requests_metric table_name where_clause ua_filter"),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            "requests_metric backing_col table_name where_clause search_cond limit",
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            "clean_field requests_metric table_name where_clause search_cond limit",
        ),
        (
            origin_sql.SUMMARY_ROLLUP,
            "lat_val ottlb_p50 ottlb_p95 cdn_ovh ost_5xx obytes_p50 total_misses total_passes table where",
        ),
        (origin_sql.TIMESERIES_BUCKETED, "interval agg_expr unit_conv edge_col table where lat_expr edge_group"),
        (origin_sql.SLOW_URLS, "lat_val table where"),
        (origin_sql.STATUS_CODES, "table where"),
        (origin_sql.PATH_BREAKDOWN, "lat_val table where"),
        (origin_sql.POP_LATENCY, "lat_val table where"),
        (origin_sql.IP_HEALTH, "lat_val table where"),
        (origin_sql.SHIELDING_ANALYSIS, "table where time_where"),
        (origin_sql.AGGREGATES_CREATE_TEMP, "temp_table select_cols lat_us_expr table where_clause"),
        (query_sql.EXPLAIN_WRAPPER, "sql"),
        (query_sql.AUTO_LIMIT_WRAPPER, "inner limit"),
        (query_sql.PRESET_SAMPLE_ROWS, "table"),
        (query_sql.PRESET_ROW_COUNT, "table"),
        (query_sql.PRESET_COLUMN_STATS, "table"),
        (security_sql.TOP_UAS_BY_COUNT, "limit temp_table"),
        (security_sql.NGWAF_TOP_BOTS_JOIN, "n temp_table"),
        (security_sql.VERIFIED_BOTS_TS, "bucket_seconds temp_table"),
        (security_sql.NGWAF_VERIFIED_BOTS, "temp_table"),
        (security_sql.NGWAF_VERIFIED_BOTS_TS, "bucket_seconds temp_table"),
        (security_sql.FINGERPRINT_TOP_N, "col temp_table"),
        (security_sql.REQ_HEADER_SIZE_DIST, "temp_table"),
        (security_sql.TOP_IPS_BY_MAX_HEADER, "temp_table"),
        (security_sql.IPV6_ADOPTION_TS, "temp_table time_bucket_select"),
        (security_sql.PROXY_TYPE_DIST, "temp_table"),
        (security_sql.CONN_REUSE_DIST, "temp_table"),
        (security_sql.WELLKNOWN_BOTS_UA_IP, "prefilter temp_table"),
        (usage_sql.EDGE_RATIO_PCT, "table"),
    ],
)
def test_unique_placeholders_pinned(template: str, expected: str):
    assert _placeholders(template) == sorted(expected.split())


_MAP_MULTI_COLS = (
    "city_col lat_col lon_col metro_col join_city_col join_lat_col join_lon_col join_metro_col "
    "bucket_ms bucket_ms ploss_expr table table table table table table table where where"
)
_METRO_MULTI_COLS = "city_col region_col metro_col join_city_col join_region_col join_metro_col ploss_expr table table table where where"


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        (
            network_sql.HEATMAP_BY_ASN_BUCKET,
            "bucket_ms bucket_ms rtt_min_expr congestion_expr ploss_expr rtt_var_expr table table table table where where row_limit",
        ),
        (network_sql.MAP_BY_COUNTRY_BUCKET, _MAP_MULTI_COLS),
        (network_sql.METRO_LEADERBOARD, _METRO_MULTI_COLS),
        (network_sql.SPEED_DISTRIBUTION_BY_ASN, "table where placeholders"),
        (network_sql.RTT_PERCENTILES_BY_ASN, "table where placeholders"),
        (network_sql.QUALITY_BAR_BY_GROUP, "group_col group_col group_col group_col table rtt_filter extra_where"),
        (network_sql.QUALITY_COUNTRIES_DISTINCT, "table where_clause"),
        (network_sql.QUALITY_SCATTER, "table rtt_filter"),
        (sessions_sql.SESSIONS_PAGE_SELECT, "cte_prefix flag_expr flagged_filter sort_by sort_dir limit offset"),
        (sessions_sql.SESSIONS_COUNT_WRAPPER, "cte_prefix flag_expr flagged_filter"),
        (usage_sql.EDGE_RATIO_PCT, "table"),
    ],
)
def test_multiplicity_placeholders_pinned(template: str, expected: str):
    assert _placeholders_all(template) == sorted(expected.split())


def test_max_timestamp_renders_with_table_name():
    assert alerts_sql.MAX_TIMESTAMP.format(table='"logs_xyz"') == 'SELECT max(timestamp) FROM "logs_xyz"'


_DASH_BASE = {"requests_metric": "COUNT(*)", "table_name": '"logs_xyz"', "where_clause": "1=1"}
_TB_1M = "time_bucket(INTERVAL '1 minute', timestamp) AS bucket"


@pytest.mark.parametrize(
    ("template", "extra_kwargs", "expected_substrings"),
    [
        (
            dashboard_sql.VIRTUAL_FIELD_EXPLODED_TOP_N,
            {"backing_col": "waf_sig"},
            'unnest(string_split("waf_sig", \',\')) | FROM "logs_xyz" | WITH split_data AS | CROSS JOIN total_count | LIMIT 10',
        ),
        (
            dashboard_sql.CONN_REQUESTS_BUCKET,
            {},
            "'2–5' | '6–20' | '21+' | ORDER BY MIN(\"conn_requests\") | FROM \"logs_xyz\"",
        ),
        (
            dashboard_sql.TIME_SERIES,
            {"time_bucket_select": _TB_1M, "value_expr": "COUNT(*)", "extra_where": "", "where_clause": "status = 200"},
            "time_bucket(INTERVAL '1 minute', timestamp) | COUNT(*) AS value | FROM \"logs_xyz\" | WHERE timestamp IS NOT NULL AND status = 200 | GROUP BY 1 ORDER BY 1",
        ),
        (
            dashboard_sql.TIME_SERIES,
            {
                "time_bucket_select": _TB_1M,
                "value_expr": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) / 1000.0",
                "extra_where": ' AND "elapsed" IS NOT NULL',
            },
            'WHERE timestamp IS NOT NULL AND "elapsed" IS NOT NULL AND 1=1',
        ),
        (
            dashboard_sql.MAP_DATA_BY_COUNTRY,
            {},
            'SELECT "country" AS country | COUNT(*) AS count | WHERE "country" IS NOT NULL AND 1=1 | GROUP BY 1',
        ),
        (
            dashboard_sql.FIELD_VALUES_BOT_UA,
            {"ua_filter": "AND regexp_matches(ua, 'bot|crawl')"},
            "SELECT ua, COUNT(*) AS cnt | WHERE 1=1 AND ua IS NOT NULL AND regexp_matches(ua, 'bot|crawl') | GROUP BY ua | ORDER BY cnt DESC | LIMIT 5000",
        ),
        (dashboard_sql.FIELD_VALUES_BOT_UA, {"ua_filter": ""}, "WHERE 1=1 AND ua IS NOT NULL "),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            {"backing_col": "waf_sig", "search_cond": "AND trim(signal) ILIKE ?", "limit": 20},
            "unnest(string_split(\"waf_sig\", ',')) | FROM \"logs_xyz\" | WHERE trim(signal) != '' AND trim(signal) ILIKE ? | LIMIT 20",
        ),
        (
            dashboard_sql.FIELD_VALUES_VIRTUAL_SIGNALS,
            {"backing_col": "edge_score_reason", "search_cond": "", "limit": 10},
            "unnest(string_split(\"edge_score_reason\", ',')) | WHERE trim(signal) != ''  | LIMIT 10",
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            {"clean_field": "country", "search_cond": 'AND CAST("country" AS VARCHAR) ILIKE ?', "limit": 50},
            'SELECT "country" AS value | FROM "logs_xyz" | WHERE 1=1 AND CAST("country" AS VARCHAR) ILIKE ? | GROUP BY 1 ORDER BY 2 DESC LIMIT 50',
        ),
        (
            dashboard_sql.FIELD_VALUES_NATIVE_COLUMN,
            {"clean_field": "asn", "search_cond": "", "limit": 10},
            'SELECT "asn" AS value | WHERE 1=1  | LIMIT 10',
        ),
    ],
)
def test_dashboard_templates_render(template: str, extra_kwargs: dict, expected_substrings: str):
    rendered = template.format(**{**_DASH_BASE, **extra_kwargs})
    for substr in _as_list(expected_substrings):
        assert substr in rendered


def test_all_templates_exported():
    names = (
        "VIRTUAL_FIELD_EXPLODED_TOP_N CONN_REQUESTS_BUCKET TIME_SERIES MAP_DATA_BY_COUNTRY "
        "FIELD_VALUES_BOT_UA FIELD_VALUES_VIRTUAL_SIGNALS FIELD_VALUES_NATIVE_COLUMN"
    ).split()
    for name in names:
        assert isinstance(getattr(dashboard_sql, name, None), str)


def test_registry_sql_templates_match_module_constants():
    ids = (
        "error_spikes botnet_grouping new_country_traffic city_surges city_error_spikes "
        "city_latency_regressions new_city_traffic ua_monoculture new_probe_urls waf_signal_spikes "
        "proxy_surge asn_concentration asn_metro_performance cache_collapse latency_regression "
        "impossible_distance tail_latency cipher_spread request_size_anomaly connection_abuse "
        "region_latency cache_ttl_mismatch image_optimization_opportunities origin_latency_spike "
        "origin_error_rate origin_retries origin_ip_failure shield_path_degradation "
        "crawler_disparity stale_browser_version orphaned_deep_crawl residential_fingerprint_dispersion"
    ).split()
    for insight_id in ids:
        d = registry.get(insight_id)
        assert d is not None and d.sql_template == getattr(insights_sql, insight_id.upper())


_T = {"table_name": "t_logs"}
_TW = {"baseline_hours": 24, "window_hours": 1}
_CITY_LOC_KWARGS = {
    "label_expr": "'l'",
    "region_sel": '"region"',
    "country_sel": '"country"',
    "loc_cols": '"country", "region"',
}
_CITY_LOC_KEYS = "table_name label_expr region_sel country_sel loc_cols"


@pytest.mark.parametrize(
    ("template", "extra_kwargs", "expected_substrings", "q_count", "expected_placeholders"),
    [
        (
            insights_sql.ERROR_SPIKES,
            {},
            "FROM t_logs | CAST(? AS TIMESTAMPTZ) | HAVING w_total >= 3 | ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15",
            2,
            "table_name",
        ),
        (
            insights_sql.BOTNET_GROUPING,
            {"baseline_hours": 24, "window_hours": 1, "fp_col": "ja4"},
            'FROM t_logs WHERE "ja4" IS NOT NULL | GREATEST(24, 1.0) * 1',
            2,
            "table_name fp_col baseline_hours window_hours",
        ),
        (
            insights_sql.NEW_COUNTRY_TRAFFIC,
            {},
            'WHERE "country" IS NOT NULL | HAVING w_cnt >= 3 AND b_cnt = 0',
            2,
            "table_name",
        ),
        (
            insights_sql.CITY_SURGES,
            {**_CITY_LOC_KWARGS, **_TW},
            'FROM t_logs | WHERE "city" IS NOT NULL | ORDER BY spike_ratio DESC LIMIT 15',
            2,
            f"{_CITY_LOC_KEYS} baseline_hours window_hours",
        ),
        (
            insights_sql.CITY_ERROR_SPIKES,
            _CITY_LOC_KWARGS,
            'WITH base AS | WHERE "city" IS NOT NULL | HAVING w_total >= 10 AND w_rate >= 0.10',
            2,
            _CITY_LOC_KEYS,
        ),
        (
            insights_sql.CITY_LATENCY_REGRESSIONS,
            _CITY_LOC_KWARGS,
            "PERCENTILE_CONT(0.95) | w_p95 >= b_p95 * 3.0",
            2,
            _CITY_LOC_KEYS,
        ),
        (insights_sql.NEW_CITY_TRAFFIC, _CITY_LOC_KWARGS, "HAVING w_cnt >= 5 AND b_cnt = 0", 2, _CITY_LOC_KEYS),
        (insights_sql.UA_MONOCULTURE, {}, 'FROM t_logs GROUP BY "ua"', 2, "table_name"),
        (
            insights_sql.NEW_PROBE_URLS,
            {},
            "FROM t_logs | regexp_matches( | \\.env | admin | 'i'",
            3,
            "table_name",
        ),
        (
            insights_sql.WAF_SIGNAL_SPIKES,
            {"waf_table": "insights_waf_1", "baseline_hours": 24, "window_hours": 1},
            "FROM insights_waf_1 | GROUP BY signal",
            2,
            "waf_table baseline_hours window_hours",
        ),
        (insights_sql.PROXY_SURGE, {}, 'FROM t_logs WHERE "p_type" IS NOT NULL | totals AS', 2, "table_name"),
        (insights_sql.ASN_CONCENTRATION, {}, 'GROUP BY "asn" | w_cnt * 1.0 / w_total >= 0.20', 2, "table_name"),
        (insights_sql.ASN_METRO_PERFORMANCE, {}, "\"country\" = 'US' | w_med >= b_med * 1.5", 2, "table_name"),
        (insights_sql.CACHE_COLLAPSE, {}, "starts_with(cache, 'HIT') | b_rate >= 0.40", 2, "table_name"),
        (insights_sql.LATENCY_REGRESSION, {}, "PERCENTILE_CONT(0.95) | w_p95 >= b_p95 * 2.0", 2, "table_name"),
        (
            insights_sql.IMPOSSIBLE_DISTANCE,
            {
                "fp_col": "ja4",
                "pop_values": "('SJC', 37.0::DOUBLE, -121.0::DOUBLE)",
                "edge_filter": 'AND t."edge" = true',
            },
            "WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES ('SJC' | RADIANS | AND t.\"edge\" = true",
            1,
            "table_name fp_col pop_values edge_filter",
        ),
        (insights_sql.TAIL_LATENCY, {}, "PERCENTILE_CONT(0.99) | PERCENTILE_CONT(0.50) | ratio > 5", 1, "table_name"),
        (
            insights_sql.CIPHER_SPREAD,
            _TW,
            '"tls_ciphers_sha" IS NOT NULL | COUNT(DISTINCT "ip")',
            2,
            "table_name baseline_hours window_hours",
        ),
        (insights_sql.REQUEST_SIZE_ANOMALY, {}, "req_header_bytes > 0 | max_bytes > b_p95 * 3", 2, "table_name"),
        (
            insights_sql.CONNECTION_ABUSE,
            {},
            "conn_requests > 0 | max_reqs > b_p95 * 3 AND max_reqs >= 50",
            2,
            "table_name",
        ),
        (insights_sql.REGION_LATENCY, {}, "stats AS | ottfb_p95", 2, "table_name"),
        (
            insights_sql.CACHE_TTL_MISMATCH,
            {"q_col": '"url"'},
            'SELECT "url" AS label | AVG("hits") < 2 AND AVG("ttl") > 60',
            1,
            "table_name q_col",
        ),
        (
            insights_sql.IMAGE_OPTIMIZATION_OPPORTUNITIES,
            {"ua_mobile_sel": "0"},
            'WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ) AND t."status" = 200 | (0) AS mobile_ratio | %.jpg%',
            1,
            "table_name ua_mobile_sel",
        ),
        (
            insights_sql.ORIGIN_LATENCY_SPIKE,
            {"url_col": '"url"'},
            "overall_stats AS | url_stats AS | o.w_p95 > o.b_p95 * 2",
            2,
            "table_name url_col",
        ),
        (insights_sql.ORIGIN_ERROR_RATE, {}, '"ost" AS status | status >= 500', 2, "table_name"),
        (insights_sql.ORIGIN_RETRIES, {"url_col": '"url"'}, 'AVG("oretries") | requests >= 5', 1, "table_name url_col"),
        (insights_sql.ORIGIN_IP_FAILURE, {}, '"oip" IS NOT NULL | median_calc AS', 1, "table_name"),
        (
            insights_sql.SHIELD_PATH_DEGRADATION,
            {},
            "edge_logs AS | shield_logs AS | 'Direct to Origin'",
            3,
            "table_name",
        ),
        (
            insights_sql.COALESCED_CITY_AGGREGATES,
            {"label_expr": "'l'", "region_sel": '"region"', "country_sel": '"country"'},
            'FROM t_logs | WHERE "city" IS NOT NULL | GROUP BY "city", region, country | w_lat_total | b_lat_total',
            2,
            "table_name label_expr region_sel country_sel",
        ),
        (
            insights_sql.COALESCED_URL_AGGREGATES,
            {},
            'FROM t_logs | WHERE "url" IS NOT NULL | w_5xx | w_hits | w_p99 | w_p50 | GROUP BY "url"',
            2,
            "table_name",
        ),
        (
            insights_sql.COALESCED_ASN_AGGREGATES,
            {"ploss_sel": '"ploss"', "rtt_var_sel": '"rtt_var"', "retrans_sel": '"retrans"', "p_type_sel": '"p_type"'},
            "FROM t_logs | GROUP BY asn",
            2,
            "table_name ploss_sel rtt_var_sel retrans_sel p_type_sel",
        ),
        (
            insights_sql.CRAWLER_DISPARITY,
            {},
            "crawler_traffic | w_bytes | bytes_per_referral | bandwidth_share_pct",
            4,
            "table_name",
        ),
        (insights_sql.STALE_BROWSER_VERSION, _TW, "version_aggregates", 2, None),
        (insights_sql.ORPHANED_DEEP_CRAWL, _TW, "deep_requests", 2, None),
        (insights_sql.RESIDENTIAL_FINGERPRINT_DISPERSION, {}, "fingerprint_asn_maps", 2, None),
    ],
)
def test_insights_templates_render_and_pin_placeholders(
    template: str,
    extra_kwargs: dict,
    expected_substrings: str,
    q_count: int,
    expected_placeholders: str | None,
):
    rendered = template.format(**{**_T, **extra_kwargs})
    for substr in _as_list(expected_substrings):
        assert substr in rendered
    assert rendered.count("?") == q_count
    if expected_placeholders is not None:
        assert _placeholders(template) == sorted(expected_placeholders.split())


def test_new_probe_regex_contains_all_probes_escaped():
    for probe in insights_sql.NEW_PROBES:
        assert re.escape(probe) in insights_sql.NEW_PROBE_REGEX, f"probe {probe!r} missing from regex"


def test_no_hardcoded_bot_brands_in_disparity_or_efficiency_sql():
    forbidden = (
        "anthropic claudebot gptbot openai perplexity googlebot bingbot "
        "amazonbot bytespider semrush ahrefs ccbot petalbot"
    ).split()
    for name, template in [
        ("CRAWLER_DISPARITY", insights_sql.CRAWLER_DISPARITY),
        ("BOT_EFFICIENCY", security_sql.BOT_EFFICIENCY),
    ]:
        lower_sql = template.lower()
        for brand in forbidden:
            assert brand not in lower_sql, f"{name} must not hardcode bot/vendor name {brand!r}"


def test_heatmap_by_asn_bucket_renders_with_all_and_null_inputs():
    rendered = network_sql.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=300000,
        rtt_min_expr="MEDIAN(rtt_min)",
        congestion_expr="MEDIAN(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0))",
        ploss_expr="AVG(ploss)",
        rtt_var_expr="MEDIAN(rtt_var)",
        table='"_tmp_xyz"',
        where="1=1",
        row_limit=6000,
    )
    for expected in _as_list(
        'EPOCH_MS | APPROX_QUANTILE(tcp_rtt, 0.5) | MEDIAN(rtt_min) | AVG(ploss) | MEDIAN(rtt_var) | FROM "_tmp_xyz" | WHERE 1=1 | GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count | LIMIT 6000'
    ):
        assert expected in rendered
    assert rendered.count("300000") == 2

    rendered_null = network_sql.HEATMAP_BY_ASN_BUCKET.format(
        bucket_ms=60000,
        rtt_min_expr="NULL",
        congestion_expr="NULL",
        ploss_expr="NULL",
        rtt_var_expr="NULL",
        table='"_tmp_x"',
        where="1=1",
        row_limit=200,
    )
    assert "NULL           AS rtt_baseline_us" in rendered_null and "NULL             AS avg_ploss" in rendered_null


def test_map_by_country_bucket_and_metro_leaderboard_render():
    base_kwargs = {
        "city_col": "city",
        "lat_col": "lat",
        "lon_col": "lon",
        "metro_col": "metro",
        "join_city_col": '"_tmp_x".city',
        "join_lat_col": '"_tmp_x".lat',
        "join_lon_col": '"_tmp_x".lon',
        "join_metro_col": '"_tmp_x".metro',
        "bucket_ms": 300000,
        "ploss_expr": "AVG(ploss)",
        "table": '"_tmp_x"',
        "where": "1=1",
    }
    rendered = network_sql.MAP_BY_COUNTRY_BUCKET.format(**base_kwargs)
    for expected in _as_list(
        'APPROX_QUANTILE("_tmp_x".tcp_rtt, 0.5) | AVG(ploss) | FROM "_tmp_x" | GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count | LIMIT 5000 | ORDER BY bucket, reqs DESC'
    ):
        assert expected in rendered
    assert rendered.count("300000") == 2

    rendered_asn = network_sql.MAP_BY_COUNTRY_BUCKET.format(
        **{**base_kwargs, "bucket_ms": 60000, "where": "1=1 AND asn = ?"}
    )
    assert "WHERE 1=1 AND asn = ?" in rendered_asn

    metro = network_sql.METRO_LEADERBOARD.format(
        city_col="city",
        region_col="region",
        metro_col="metro",
        join_city_col='"_tmp_x".city',
        join_region_col='"_tmp_x".region',
        join_metro_col='"_tmp_x".metro',
        ploss_expr="AVG(ploss)",
        table='"_tmp_x"',
        where="1=1",
    )
    for expected in _as_list(
        'APPROX_QUANTILE(tcp_rtt, 0.5) | AVG(ploss) | FROM "_tmp_x" | GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count | LIMIT 100'
    ):
        assert expected in metro


def test_speed_rtt_and_quality_queries_render():
    speed = network_sql.SPEED_DISTRIBUTION_BY_ASN.format(table='"_tmp_x"', where="1=1", placeholders="?,?,?")
    for expected in ("SELECT asn, c_speed, COUNT(*)", "asn IN (?,?,?)", 'FROM "_tmp_x"', "GROUP BY asn, c_speed"):
        assert expected in speed

    rtt = network_sql.RTT_PERCENTILES_BY_ASN.format(table='"_tmp_x"', where="1=1", placeholders="?,?")
    for expected in _as_list(
        "APPROX_QUANTILE(tcp_rtt, 0.95) | APPROX_QUANTILE(tcp_rtt, 0.99) | asn IN (?,?) | GROUP BY asn"
    ):
        assert expected in rtt

    bar_no_extra = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="country",
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where="",
    )
    for expected in _as_list(
        '"country" AS label | APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms | GROUP BY "country" | LIMIT 25 | ORDER BY reqs DESC'
    ):
        assert expected in bar_no_extra

    bar_extra = network_sql.QUALITY_BAR_BY_GROUP.format(
        group_col="region",
        table='"logs_xyz"',
        rtt_filter="tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        extra_where=" AND country = ?",
    )
    assert "WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0 AND country = ?" in bar_extra and '"region" AS label' in bar_extra

    countries = network_sql.QUALITY_COUNTRIES_DISTINCT.format(
        table='"logs_xyz"', where_clause="ts BETWEEN '2026-01-01' AND '2026-01-02'"
    )
    for expected in _as_list(
        "SELECT DISTINCT country | FROM \"logs_xyz\" | country IS NOT NULL AND country != '' | ORDER BY country"
    ):
        assert expected in countries

    scatter = network_sql.QUALITY_SCATTER.format(
        table='"logs_xyz"',
        rtt_filter="ts BETWEEN '2026-01-01' AND '2026-01-02' AND tcp_rtt IS NOT NULL AND tcp_rtt > 0",
    )
    for expected in _as_list(
        "tcp_rtt / 1000.0 AS rtt_ms | ttfb * 1000.0 AS ttfb_ms | COALESCE(cache, 'UNKNOWN') AS cache_state | FROM \"logs_xyz\" | USING SAMPLE 2000 | ttfb IS NOT NULL AND ttfb > 0"
    ):
        assert expected in scatter


def test_summary_rollup_and_timeseries_bucketed_render():
    rendered = origin_sql.SUMMARY_ROLLUP.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)',
        ottlb_p50='MEDIAN("ottlb") / 1000.0',
        ottlb_p95='APPROX_QUANTILE("ottlb", 0.95) / 1000.0',
        cdn_ovh='MEDIAN("elapsed" - "ottlb") / 1000.0',
        ost_5xx='COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)',
        obytes_p50='MEDIAN("obytes")',
        total_misses="1",
        total_passes="1",
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
    )
    assert 'FROM "logs_xyz"' in rendered and 'MEDIAN(COALESCE("ottfb", "ttfb" * 1000000.0)) / 1000.0' in rendered
    assert "AS ottfb_p99_ms" in rendered and "GROUPING SETS" not in rendered and 'GROUPING("edge")' not in rendered

    ts = origin_sql.TIMESERIES_BUCKETED.format(
        interval="INTERVAL '5' minutes",
        agg_expr='APPROX_QUANTILE("ottfb", 0.95)',
        unit_conv="/ 1000.0",
        edge_col=', "edge"',
        table='"logs_xyz"',
        where="timestamp BETWEEN ? AND ?",
        lat_expr='"ottfb"',
        edge_group=', "edge"',
    )
    for expected in _as_list(
        'time_bucket(INTERVAL \'5\' minutes, "timestamp") | FROM "logs_xyz" | APPROX_QUANTILE("ottfb", 0.95) / 1000.0 | GROUP BY ts | ORDER BY ts'
    ):
        assert expected in ts


def test_origin_table_and_shielding_queries_render():
    slow = origin_sql.SLOW_URLS.format(
        lat_val='COALESCE("ottfb", "ttfb" * 1000000.0)', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?"
    )
    assert 'SELECT\n          "url"' in slow and 'FROM "logs_xyz"' in slow and "HAVING COUNT(*) >= ?" in slow
    assert slow.rstrip().endswith("LIMIT ?")

    status = origin_sql.STATUS_CODES.format(table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('"ost"', "OVER ()", 'FROM "logs_xyz"', "ORDER BY count DESC"):
        assert expected in status

    path_bd = origin_sql.PATH_BREAKDOWN.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "edge"', 'FROM "logs_xyz"', 'MEDIAN("ottfb")'):
        assert expected in path_bd

    pop = origin_sql.POP_LATENCY.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "pop"', 'FROM "logs_xyz"', "ORDER BY p95_ms DESC"):
        assert expected in pop
    assert pop.rstrip().endswith("LIMIT ?")

    ip_h = origin_sql.IP_HEALTH.format(lat_val='"ottfb"', table='"logs_xyz"', where="timestamp BETWEEN ? AND ?")
    for expected in ('GROUP BY "oip"', 'FROM "logs_xyz"', "HAVING COUNT(*) >= 10", "ORDER BY error_pct DESC"):
        assert expected in ip_h
    assert ip_h.rstrip().endswith("LIMIT ?")

    shield = origin_sql.SHIELDING_ANALYSIS.format(
        table='"logs_xyz"', where="timestamp BETWEEN ? AND ?", time_where="timestamp BETWEEN ? AND ?"
    )
    for expected in _as_list(
        "WITH edge_logs AS | shield_logs AS | INNER JOIN shield_logs s ON s.prid = e.rid | PERCENTILE_CONT(0.50) | COUNT(*) OVER ()  | ROW_NUMBER() OVER (ORDER BY requests DESC) | ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST) | WHERE rn_requests <= ? OR rn_overhead <= ? | AS total_routes"
    ):
        assert expected in shield
    assert shield.rstrip().endswith("ORDER BY requests DESC") and "LIMIT ?" not in shield

    temp = origin_sql.AGGREGATES_CREATE_TEMP.format(
        temp_table="t_origin_deadbeef",
        select_cols='"timestamp", "cache", "edge"',
        lat_us_expr='COALESCE("ottfb", "ttfb" * 1000000.0)',
        table='"logs_xyz"',
        where_clause="timestamp BETWEEN '2026-06-09T00:00:00Z' AND '2026-06-09T01:00:00Z'",
    )
    assert temp.startswith("CREATE TABLE t_origin_deadbeef AS ") and "CREATE TEMP TABLE" not in temp
    assert 'SELECT "timestamp", "cache", "edge",' in temp
    assert 'COALESCE("ottfb", "ttfb" * 1000000.0) AS lat_us' in temp and 'FROM "logs_xyz" WHERE' in temp


def test_query_templates_render():
    assert query_sql.EXPLAIN_WRAPPER.format(sql="SELECT 1") == "EXPLAIN SELECT 1"
    auto = query_sql.AUTO_LIMIT_WRAPPER.format(inner="SELECT * FROM logs_svc ORDER BY id", limit=1001)
    assert "SELECT * FROM (SELECT * FROM logs_svc ORDER BY id) AS _q" in auto and "LIMIT 1001" in auto
    assert "AS _q" in query_sql.AUTO_LIMIT_WRAPPER.format(inner="SELECT 1", limit=10)

    sample = query_sql.PRESET_SAMPLE_ROWS.format(table="logs_myservice")
    assert sample == "SELECT * FROM logs_myservice LIMIT 100" and "ORDER BY" not in sample.upper()
    assert (
        query_sql.PRESET_ROW_COUNT.format(table="logs_myservice") == "SELECT count(*) AS total_rows FROM logs_myservice"
    )
    assert query_sql.PRESET_COLUMN_STATS.format(table="logs_myservice") == "SUMMARIZE logs_myservice"


_SEC_BASE = {"temp_table": "t_filtered_xyz"}


@pytest.mark.parametrize(
    ("template", "extra_kwargs", "expected_substrings"),
    [
        (
            security_sql.TOP_UAS_BY_COUNT,
            {"limit": 2000},
            "SELECT ua, count(*) AS cnt | FROM t_filtered_xyz | WHERE ua IS NOT NULL | GROUP BY ua | ORDER BY cnt DESC | LIMIT 2000",
        ),
        (
            security_sql.NGWAF_TOP_BOTS_JOIN,
            {"n": 15},
            "SELECT nb.bot_name, nb.category, count(*) AS cnt | FROM t_filtered_xyz t | INNER JOIN temp.ngwaf_top_ngwaf_bots nb USING (waf_req_id) | WHERE nb.bot_name IS NOT NULL | LIMIT 15",
        ),
        (
            security_sql.VERIFIED_BOTS_TS,
            {"bucket_seconds": 300},
            "time_bucket(INTERVAL '300 seconds', timestamp) | replace(tag, 'VERIFIED-BOT.', '') | FROM t_filtered_xyz | WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%' | GROUP BY 1, 2 | ORDER BY 1, 2",
        ),
        (
            security_sql.NGWAF_VERIFIED_BOTS,
            {},
            "nb.bot_name | nb.wellknown_bot_name | nb.category | count(*) AS request_count | FROM t_filtered_xyz t | INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id) | GROUP BY 1, 2, 3 | ORDER BY 4 DESC",
        ),
        (
            security_sql.NGWAF_VERIFIED_BOTS_TS,
            {"bucket_seconds": 60},
            "time_bucket(INTERVAL '60 seconds', t.timestamp) | FROM t_filtered_xyz t | INNER JOIN temp.ngwaf_cache_ngwaf_bots nb USING (waf_req_id) | WHERE nb.bot_name IS NOT NULL | GROUP BY 1, 2",
        ),
        (
            security_sql.FINGERPRINT_TOP_N,
            {"col": "tls_ciphers_sha"},
            'SELECT "tls_ciphers_sha" | count(DISTINCT ip) as ip_count | count(*) as req_count | FROM t_filtered_xyz | WHERE "tls_ciphers_sha" IS NOT NULL | GROUP BY 1 ORDER BY 3 DESC LIMIT 20',
        ),
        (
            security_sql.REQ_HEADER_SIZE_DIST,
            {},
            "WHEN req_header_bytes <= 256 THEN '0-256B' | WHEN req_header_bytes <= 32768 THEN '24-32KB' | ELSE '>32KB' | MIN(req_header_bytes) as min_val | FROM t_filtered_xyz | WHERE req_header_bytes IS NOT NULL | GROUP BY 1 ORDER BY min_val",
        ),
        (
            security_sql.TOP_IPS_BY_MAX_HEADER,
            {},
            "SELECT ip, MAX(req_header_bytes) as max_header | FROM t_filtered_xyz | WHERE ip IS NOT NULL AND req_header_bytes IS NOT NULL | GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
        ),
        (
            security_sql.IPV6_ADOPTION_TS,
            {"time_bucket_select": "time_bucket(INTERVAL '1 hour', timestamp) AS bucket"},
            "time_bucket(INTERVAL '1 hour', timestamp) AS bucket | SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) * 100.0 / count(*) as ipv6_pct | FROM t_filtered_xyz | GROUP BY 1 ORDER BY 1",
        ),
        (
            security_sql.PROXY_TYPE_DIST,
            {},
            "SELECT p_type, count(*) as count | FROM t_filtered_xyz | WHERE p_type IS NOT NULL AND p_type != '' | GROUP BY 1 ORDER BY 2 DESC",
        ),
        (
            security_sql.CONN_REUSE_DIST,
            {},
            "WHEN conn_requests = 1 THEN '1 (None)' | WHEN conn_requests <= 5 THEN '2-5' | WHEN conn_requests <= 100 THEN '21-100' | ELSE '>100' | MIN(conn_requests) as min_val | FROM t_filtered_xyz | WHERE conn_requests IS NOT NULL AND conn_requests > 0 | GROUP BY 1 ORDER BY min_val",
        ),
        (
            security_sql.WELLKNOWN_BOTS_UA_IP,
            {"prefilter": "WHERE ua IS NOT NULL AND ip IS NOT NULL"},
            "SELECT ua, ip, count(*) AS cnt | FROM t_filtered_xyz | WHERE ua IS NOT NULL AND ip IS NOT NULL | GROUP BY ua, ip | ORDER BY cnt DESC | LIMIT 10000",
        ),
        (
            security_sql.WELLKNOWN_BOTS_UA_IP,
            {"prefilter": "WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '(googlebot|bingbot)')"},
            "regexp_matches(ua, '(googlebot|bingbot)') | FROM t_filtered_xyz",
        ),
    ],
)
def test_security_templates_render(template: str, extra_kwargs: dict, expected_substrings: str):
    rendered = template.format(**{**_SEC_BASE, **extra_kwargs})
    for substr in _as_list(expected_substrings):
        assert substr in rendered


def test_sessions_cte_pipeline_renders_and_pins_placeholders():
    proj_cols = "ua status resp_bytes rtt asn country url edge edge_sid cmcd_sid".split()
    proj_map = {f"{c}_proj": f', "{"tcp_rtt" if c == "rtt" else c}"' for c in proj_cols}
    rendered = sessions_sql.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip", "ja4"',
        table_name='"logs_xyz"',
        where_clause="timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)",
        part_key='"ip", "ja4"',
        extra_aggs=', SUM("resp_bytes") AS total_bytes, MAX("edge_sid") AS edge_sid',
        **proj_map,
    )
    for expected in _as_list(
        'WITH base AS | gaps AS | marks AS | sessions_raw AS | sessions_agg AS | LAG(ts) OVER (PARTITION BY | INTERVAL 30 MINUTES | ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW | CAST(? AS TIMESTAMPTZ) | "ja4" | "ua" | "edge_sid" | "cmcd_sid" | SUM("resp_bytes") AS total_bytes | MAX("edge_sid") AS edge_sid | FROM "logs_xyz"'
    ):
        assert expected in rendered

    empty_rendered = sessions_sql.SESSIONS_CTE_PIPELINE.format(
        group_key='"ip"',
        table_name='"logs_xyz"',
        where_clause="1=1",
        part_key='"ip"',
        extra_aggs="",
        **dict.fromkeys(proj_map, ""),
    )
    assert "WITH base AS" in empty_rendered and 'PARTITION BY "ip"' in empty_rendered
    for absent in ('"ja4"', '"ua"', '"status"', '"edge_sid"'):
        assert absent not in empty_rendered

    placeholders = _placeholders_all(sessions_sql.SESSIONS_CTE_PIPELINE)
    assert set(placeholders) == {*proj_map, "group_key", "table_name", "where_clause", "part_key", "extra_aggs"}
    assert placeholders.count("group_key") == 3 and placeholders.count("part_key") == 2


def test_sessions_page_select_and_count_wrapper_render():
    rendered = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count, NULL AS sid)",
        flag_expr="(req_count >= 1000)",
        flagged_filter="WHERE flagged = true",
        sort_by="session_start",
        sort_dir="DESC",
        limit=50,
        offset=100,
    )
    for expected in _as_list(
        "WITH sessions_agg AS | ((req_count >= 1000)) AS flagged | FROM sessions_agg | WHERE flagged = true | ORDER BY session_start DESC | LIMIT 50 OFFSET 100"
    ):
        assert expected in rendered

    unflagged = sessions_sql.SESSIONS_PAGE_SELECT.format(
        cte_prefix="",
        flag_expr="(req_count >= 1000)",
        flagged_filter="",
        sort_by="req_count",
        sort_dir="ASC",
        limit=20,
        offset=0,
    )
    assert (
        "ORDER BY req_count ASC" in unflagged and "LIMIT 20 OFFSET 0" in unflagged and "WHERE flagged" not in unflagged
    )

    count_sql = sessions_sql.SESSIONS_COUNT_WRAPPER.format(
        cte_prefix="WITH sessions_agg AS (SELECT 1 AS req_count)",
        flag_expr="(req_count >= 1000) OR ((reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0)",
        flagged_filter="WHERE flagged = true",
    )
    for expected in _as_list(
        "WITH sessions_agg AS | SELECT COUNT(*) FROM (SELECT | AS flagged FROM sessions_agg) sub | WHERE flagged = true | (reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= 20.0"
    ):
        assert expected in count_sql


def test_edge_ratio_pct_renders_with_table_name():
    rendered = usage_sql.EDGE_RATIO_PCT.format(table='"logs_xyz"')
    assert "count(*) FILTER (WHERE edge = true)" in rendered and 'FROM "logs_xyz"' in rendered


def _build_default_placeholders() -> dict[str, str]:
    out: dict[str, str] = {}
    groups = [
        (
            "",
            "extra_where search_clause search_cond ua_filter grouping_clause edge_col edge_group asn_proj country_proj "
            "cte_prefix edge_proj edge_sid_proj cmcd_sid_proj extra_aggs flagged_filter resp_bytes_proj "
            "rtt_proj status_proj ua_proj url_proj",
        ),
        (
            "NULL",
            "total_misses total_passes ottlb_p50 ottlb_p95 obytes_p50 ost_5xx congestion_expr metro_col ploss_expr join_metro_col",
        ),
        ("1=1", "where_clause where time_where prefilter"),
        ("COUNT(*)", "requests_metric chart_metric_expr value_expr"),
        ("elapsed", "lat_val lat_expr lat_us_expr"),
        ("status", "clean_field backing_col"),
        ("''", "edge_select"),
        ("0", "grouping_expr cdn_ovh offset"),
        ('"city"', "city_col join_city_col"),
        ("lat", "lat_col join_lat_col"),
        ("lon", "lon_col join_lon_col"),
        ('"region"', "region_col join_region_col"),
        ('"ip"', "group_key part_key"),
    ]
    for val, keys in groups:
        out.update(dict.fromkeys(keys.split(), val))
    out.update(
        table_name="t_logs",
        table='"t_logs"',
        temp_table="t_origin_tmp",
        waf_table="insights_waf_1",
        column='"status"',
        cache_col='"cache"',
        select_cols="*",
        time_bucket_select="time_bucket(INTERVAL '5 minutes', timestamp) AS bucket",
        agg_expr="AVG(elapsed)",
        unit_conv="/ 1000.0",
        fp_col="ja4",
        bucket_seconds="300",
        interval="INTERVAL '5 minutes'",
        baseline_hours="24",
        window_hours="1",
        limit="100",
        bucket_ms="300000",
        group_col="country",
        placeholders="?,?,?",
        row_limit="1000",
        rtt_filter="tcp_rtt IS NOT NULL AND tcp_rtt > 0",
        rtt_min_expr="MEDIAN(rtt_min)",
        rtt_var_expr="MEDIAN(rtt_var)",
        col='"ja4"',
        n="50",
        agg_cols="COUNT(*) AS hits",
        flag_expr="req_count >= 1000",
        sort_by="req_count",
        sort_dir="DESC",
    )
    return out


_DEFAULT_PLACEHOLDERS: dict[str, str] = _build_default_placeholders()


def _render_snapshot(template: str, **overrides: str) -> str:
    placeholders = set(re.findall(r"\{([a-z_][a-z_0-9]*)\}", template))
    values: dict[str, str] = {}
    for name in placeholders:
        if name in overrides:
            values[name] = overrides[name]
        elif name in _DEFAULT_PLACEHOLDERS:
            values[name] = _DEFAULT_PLACEHOLDERS[name]
        else:  # pragma: no cover
            raise KeyError(f"no default for SQL placeholder {{{name}}}; add to _DEFAULT_PLACEHOLDERS")
    return template.format(**values)


def test_insights_error_spikes_sql(snapshot):
    assert _render_snapshot(insights_sql.ERROR_SPIKES) == snapshot


def test_insights_botnet_grouping_sql(snapshot):
    assert _render_snapshot(insights_sql.BOTNET_GROUPING) == snapshot


def test_insights_waf_signal_spikes_sql(snapshot):
    assert _render_snapshot(insights_sql.WAF_SIGNAL_SPIKES) == snapshot


def test_insights_latency_regression_sql(snapshot):
    assert _render_snapshot(insights_sql.LATENCY_REGRESSION) == snapshot


def test_insights_origin_error_rate_sql(snapshot):
    assert _render_snapshot(insights_sql.ORIGIN_ERROR_RATE) == snapshot


def test_insights_proxy_surge_sql(snapshot):
    assert _render_snapshot(insights_sql.PROXY_SURGE) == snapshot


def test_insights_asn_concentration_sql(snapshot):
    assert _render_snapshot(insights_sql.ASN_CONCENTRATION) == snapshot


def test_dashboard_time_series_sql(snapshot):
    assert _render_snapshot(dashboard_sql.TIME_SERIES) == snapshot


def test_dashboard_map_data_by_country_sql(snapshot):
    assert _render_snapshot(dashboard_sql.MAP_DATA_BY_COUNTRY) == snapshot


def test_dashboard_field_values_bot_ua_sql(snapshot):
    assert _render_snapshot(dashboard_sql.FIELD_VALUES_BOT_UA) == snapshot


def test_dashboard_field_values_native_column_sql(snapshot):
    assert _render_snapshot(dashboard_sql.FIELD_VALUES_NATIVE_COLUMN) == snapshot


def test_origin_summary_rollup_sql(snapshot):
    assert _render_snapshot(origin_sql.SUMMARY_ROLLUP) == snapshot


def test_origin_timeseries_bucketed_sql(snapshot):
    assert _render_snapshot(origin_sql.TIMESERIES_BUCKETED, edge_group="''") == snapshot


def test_origin_slow_urls_sql(snapshot):
    assert _render_snapshot(origin_sql.SLOW_URLS) == snapshot


def test_origin_status_codes_sql(snapshot):
    assert _render_snapshot(origin_sql.STATUS_CODES) == snapshot


def test_origin_path_breakdown_sql(snapshot):
    assert _render_snapshot(origin_sql.PATH_BREAKDOWN) == snapshot


def test_origin_pop_latency_sql(snapshot):
    assert _render_snapshot(origin_sql.POP_LATENCY) == snapshot


def test_network_heatmap_by_asn_bucket_sql(snapshot):
    assert _render_snapshot(network_sql.HEATMAP_BY_ASN_BUCKET) == snapshot


def test_network_map_by_country_bucket_sql(snapshot):
    assert _render_snapshot(network_sql.MAP_BY_COUNTRY_BUCKET) == snapshot


def test_network_metro_leaderboard_sql(snapshot):
    assert _render_snapshot(network_sql.METRO_LEADERBOARD) == snapshot


def test_network_speed_distribution_by_asn_sql(snapshot):
    assert _render_snapshot(network_sql.SPEED_DISTRIBUTION_BY_ASN) == snapshot


def test_network_rtt_percentiles_by_asn_sql(snapshot):
    assert _render_snapshot(network_sql.RTT_PERCENTILES_BY_ASN) == snapshot


def test_network_quality_bar_by_group_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_BAR_BY_GROUP) == snapshot


def test_network_quality_countries_distinct_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_COUNTRIES_DISTINCT) == snapshot


def test_network_quality_scatter_sql(snapshot):
    assert _render_snapshot(network_sql.QUALITY_SCATTER) == snapshot


def test_security_top_uas_by_count_sql(snapshot):
    assert _render_snapshot(security_sql.TOP_UAS_BY_COUNT) == snapshot


def test_security_ngwaf_top_bots_join_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_TOP_BOTS_JOIN) == snapshot


def test_security_verified_bots_ts_sql(snapshot):
    assert _render_snapshot(security_sql.VERIFIED_BOTS_TS) == snapshot


def test_security_ngwaf_verified_bots_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_VERIFIED_BOTS) == snapshot


def test_security_ngwaf_verified_bots_ts_sql(snapshot):
    assert _render_snapshot(security_sql.NGWAF_VERIFIED_BOTS_TS) == snapshot


def test_security_fingerprint_top_n_sql(snapshot):
    assert _render_snapshot(security_sql.FINGERPRINT_TOP_N) == snapshot


def test_security_fingerprint_coverage_bulk_sql(snapshot):
    assert _render_snapshot(security_sql.FINGERPRINT_COVERAGE_BULK) == snapshot


def test_security_req_header_size_dist_sql(snapshot):
    assert _render_snapshot(security_sql.REQ_HEADER_SIZE_DIST) == snapshot


def test_security_top_ips_by_max_header_sql(snapshot):
    assert _render_snapshot(security_sql.TOP_IPS_BY_MAX_HEADER) == snapshot


def test_security_proxy_type_dist_sql(snapshot):
    assert _render_snapshot(security_sql.PROXY_TYPE_DIST) == snapshot


def test_security_conn_reuse_dist_sql(snapshot):
    assert _render_snapshot(security_sql.CONN_REUSE_DIST) == snapshot


def test_security_wellknown_bots_ua_ip_sql(snapshot):
    assert _render_snapshot(security_sql.WELLKNOWN_BOTS_UA_IP) == snapshot


def test_sessions_cte_pipeline_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_CTE_PIPELINE) == snapshot


def test_sessions_page_select_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_PAGE_SELECT) == snapshot


def test_sessions_count_wrapper_sql(snapshot):
    assert _render_snapshot(sessions_sql.SESSIONS_COUNT_WRAPPER) == snapshot


def _strip_volatile(payload: dict) -> dict:
    out = copy.deepcopy(payload)
    for key in ("_debug_queries", "_debug_calls", "_debug_sqlite", "_debug_postgres", "_section_timings", "_is_cached"):
        out.pop(key, None)
    if isinstance(out.get("version"), str):
        out["version"] = "<scrubbed>"
    return out


def test_api_health_response_shape(client, snapshot):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert _strip_volatile(r.json()) == snapshot


def test_api_bootstrap_empty_response_shape(client, snapshot, monkeypatch):
    r = client.get("/api/bootstrap")
    assert r.status_code == 200
    body = _strip_volatile(r.json())
    skeleton = {
        "active_service_id_is_none_or_str": body.get("active_service_id") is None
        or isinstance(body.get("active_service_id"), str),
        "services_is_list": isinstance(body.get("services"), list),
        "services_empty_in_sandbox": body.get("services") == [],
        "has_top_level_keys": sorted([k for k in body if not k.startswith("_") and k != "version"]),
    }
    assert skeleton == snapshot


# ── Merged from tests/repositories/test_dashboard.py ──

import gc
import hashlib
import ipaddress
import json as _json
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

import backend.utils.bot_sources as rdns_cache
from backend import config as svcconfig
from backend.core import duckdb as db_core
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.metadata import usage_log
from backend.core.request_telemetry import InstrumentedDuckDBConnection, query_registry
from backend.core.rollups._common import (
    OVERVIEW_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    _day_bundled_root,
    _hour_bundled_root,
)
from backend.core.rollups.day_bundles import compact_time_series_closed_days_to_daily
from backend.core.share_db import apply_pii_policy
from backend.high_scale import dashboard as high_scale_dashboard
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import HighScaleService
from backend.models.analytics import AggregatesRequest, FieldValuesRequest, RawRequest
from backend.models.common import FilterSpec
from backend.repositories import _base as repo_base
from backend.repositories import dashboard as dash
from backend.repositories import query as q
from backend.repositories._base import _safe_table
from backend.repositories.cmcd import get_cmcd_aggregates
from backend.repositories.dashboard import (
    DASHBOARD_CACHE_TTL,
    FIELDS,
    _add_bot_columns,
    _raw_extent_reaches_window_start,
    get_field_values,
    get_raw_df,
)
from backend.repositories.performance import get_performance_aggregates
from backend.repositories.query import _ANALYST_WINDOW_VIEW, MAX_QUERY_ROWS, execute_query, get_presets
from backend.utils import bot_sources
from backend.utils.telemetry import set_page_load_id
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID


def _src_without_table():
    return {"name": "nonexistent_service", "service_id": "nonexistent_service"}


def _empty_logs_table(con, src):
    con.execute("SET TimeZone='UTC';")
    table_name = _safe_table(src["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    con.execute(f"CREATE TABLE {table_name} ({schema_def})")
    return table_name


def test_filtered_dashboard_and_empty_and_populated_aggregates(in_memory_duckdb, test_service_source):
    empty_res = get_aggregates(in_memory_duckdb, _src_without_table(), None, None, {}, "1 minute", "requests")
    assert empty_res["total_rows"] == 0 and empty_res["time_series"] == [] and empty_res["map_data"] == []
    for field in FIELDS:
        assert empty_res["data"][field] == {"top": [], "total": 0}

    logs = generate_mock_logs(test_service_source, num_logs=60, hours_ago=1)
    for log in logs[:5]:
        log["status"] = 500
    for log in logs[5:10]:
        log["status"] = 404
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    assert result["total_rows"] > 0 and result["metric"] == "requests"
    obs = {row["reader"]: row for row in result["reader_coverage"]}
    for reader in ("dashboard.count", "dashboard.top_n", "dashboard.time_series"):
        assert obs[reader]["eligibility_reason"] == "filtered" and obs[reader]["eligible"] is False
    assert result["history"]["window_rows"] == result["total_rows"]
    assert result["history"]["window_count_source"] == "filtered_temp"
    for f in ("url", "ip", "status", "country"):
        assert result["data"][f]["total"] > 0
    assert len(result["debug_queries"]) > 0 and "sql" in result["debug_queries"][0]


@pytest.mark.parametrize("field_selection", ["omitted", "null", "explicit"])
def test_filtered_dashboard_virtual_expansions_preserve_scoped_counts(
    in_memory_duckdb, test_service_source, monkeypatch, field_selection
) -> None:
    """All-fields CSV panels preserve bounds, token multiplicity and masking."""
    monkeypatch.setattr(dash, "DASHBOARD_CACHE_TTL", 0)
    table = _empty_logs_table(in_memory_duckdb, test_service_source)
    in_memory_duckdb.execute(f"ALTER TABLE {table} ADD COLUMN edge_score_reason VARCHAR")
    in_memory_duckdb.executemany(
        f"""INSERT INTO {table}
            (timestamp, country, ip, ua, url, conn_requests, waf_sig, edge_score_reason)
            VALUES (?, ?, '192.0.2.1', 'fixture-agent', '/fixture', 1, ?, ?)""",
        [
            ("2026-10-27T20:45:59.999Z", "US", "BEFORE", "before"),
            ("2026-10-27T20:46:00Z", "US", " A, A, B, , ", " bot, slow,bot "),
            ("2026-10-27T20:47:00Z", "US", " A, A, B, , ", " bot, slow,bot "),
            ("2026-10-27T20:48:00Z", "US", None, ""),
            ("2026-10-27T21:46:00Z", "US", ", C, ", " slow, "),
            ("2026-10-27T21:46:00.001Z", "US", "AFTER", "after"),
            ("2026-10-27T20:49:00Z", "CA", "FILTERED", "filtered"),
        ],
    )
    queries: list[tuple[str, list]] = []
    execute = QueryRunner.execute

    def capture_execute(runner: QueryRunner, sql: str, params: list | None = None) -> Any:
        queries.append((sql, list(params or [])))
        return execute(runner, sql, params)

    monkeypatch.setattr(QueryRunner, "execute", capture_execute)
    options: dict[str, Any] = {}
    if field_selection == "null":
        options = dict(
            fields_filter=None,
            include_time_series=None,
            include_conn_requests=None,
            include_map_data=None,
            include_top_n=None,
        )
    elif field_selection == "explicit":
        options["fields_filter"] = ["country", "ip", "ua", "url", "waf_sig_ind", "edge_score_reason_ind"]
    result = get_aggregates(
        in_memory_duckdb,
        test_service_source,
        "2026-10-27T20:46:00Z",
        "2026-10-27T21:46:00Z",
        {"country": FilterSpec(mode="include", values=["US"])},
        "1 minute",
        "requests",
        **options,
    )
    assert result["total_rows"] == 4 and result["total_rows_total"] == 7
    assert sum(point["value"] for point in result["time_series"]) == 4
    assert result["map_data"] == [{"country": "US", "count": 4}]
    assert result["data"]["country"] == {"top": [{"value": "US", "count": 4}], "total": 4}
    assert result["data"]["conn_requests"] == {"top": [{"value": "1", "count": 4}], "total": 4}
    for field, counts in (
        ("waf_sig_ind", {"A": 4, "B": 2, "C": 1}),
        ("edge_score_reason_ind", {"bot": 4, "slow": 3}),
    ):
        assert {row["value"]: row["count"] for row in result["data"][field]["top"]} == counts
        assert result["data"][field]["total"] == 7
    if field_selection != "explicit":
        assert set(FIELDS) <= result["data"].keys()
    masked = apply_pii_policy(result, {"mask_ips": True})
    assert masked["data"]["ip"]["top"][0]["value"] != "192.0.2.1"
    for field in ("ua", "url", "country", "waf_sig_ind", "edge_score_reason_ind"):
        assert masked["data"][field] == result["data"][field]
    assert masked["time_series"] == result["time_series"] and masked["map_data"] == result["map_data"]

    creates = [sql for sql, _ in queries if sql.startswith("CREATE TEMP TABLE")]
    assert len(creates) == 1
    temp = creates[0].split()[3]
    assert f"FROM {table} WHERE" in creates[0]
    assert (
        in_memory_duckdb.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [temp]).fetchone()[0] == 0
    )
    expansions = [(sql, params) for sql, params in queries if "unnest(string_split(" in sql]
    assert len(expansions) == 2
    for sql, params in expansions:
        assert f"FROM {temp}" in sql and f"FROM {table}" not in sql
        assert "AND 1=1" in sql and params == []


@pytest.mark.parametrize(
    "metric", ["requests", "p95_latency", "hit_rate", "throughput", "req_size", "ttfb", "5xx", "4xx"]
)
def test_filtered_dashboard_all_fields_prunes_unrendered_columns_without_losing_dependencies(
    monkeypatch, metric
) -> None:
    """Use only real in-memory DuckDB; no service, metadata or root fixtures."""
    monkeypatch.setattr(dash, "DASHBOARD_CACHE_TTL", 0)
    monkeypatch.setattr(svcconfig, "get_status", lambda _name: {})
    monkeypatch.setattr(svcconfig, "is_durable_serving_mode", lambda _src: False)
    src = {
        "name": f"dashboard_width_{dash.uuid.uuid4().hex}",
        "log_fields": {
            "custom_fields": [
                {"name": "shown_custom"},
                {"name": "hidden_custom", "show_in_dashboard": False},
                {"name": "disabled_custom", "enabled": False},
            ]
        },
    }
    table = _safe_table(src["name"])
    create = QueryRunner.create_temp_table
    temp_columns: list[set[str]] = []
    count_queries: list[str] = []
    execute = QueryRunner.execute

    def capture_create(runner: QueryRunner, sql: str, params: list | None = None) -> bool:
        created = create(runner, sql, params)
        if created:
            temp_columns.append({row[0] for row in runner.con.execute(f"DESCRIBE {sql.split()[3]}").fetchall()})
        return created

    def capture_execute(runner: QueryRunner, sql: str, params: list | None = None) -> Any:
        if sql.upper().startswith("SELECT COUNT(*)"):
            count_queries.append(sql)
        return execute(runner, sql, params)

    monkeypatch.setattr(QueryRunner, "create_temp_table", capture_create)
    monkeypatch.setattr(QueryRunner, "execute", capture_execute)
    with duckdb.connect(":memory:") as con:
        con.execute(
            f"""CREATE TABLE {table} AS SELECT
                TIMESTAMPTZ '2026-01-01T00:01:00Z' AS timestamp, 'US' AS country,
                '192.0.2.1' AS ip, 'fixture-agent' AS ua, '/fixture' AS url, 200 AS status,
                1000.0 AS elapsed, 0.1 AS ttfb, 'HIT' AS cache, 100 AS resp_bytes,
                10 AS req_header_bytes, 20 AS req_bytes, 1 AS conn_requests,
                'A,A,B' AS waf_sig, 'bot,bot' AS edge_score_reason,
                'unused-request-id' AS rid, 10.0 AS lat, 'unused-source' AS _source_file,
                'shown' AS shown_custom,
                CASE WHEN range = 2 THEN 'drop' ELSE 'keep' END AS hidden_custom,
                'unused' AS disabled_custom FROM range(3)"""
        )
        result = get_aggregates(
            con,
            src,
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:02:00Z",
            {"hidden_custom": FilterSpec(mode="include", values=["keep"])},
            "1 minute",
            metric,
        )
        original_cols = {row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()}
    assert {"rid", "lat", "_source_file", "hidden_custom", "disabled_custom"} <= original_cols
    assert result["total_rows"] == 2 and result["total_rows_total"] == 3
    assert result["metric"] == metric and len(result["time_series"]) == 1
    expected_chart = {
        "requests": 2,
        "p95_latency": 1,
        "hit_rate": 100,
        "throughput": 100000,
        "req_size": 30,
        "ttfb": 100,
        "5xx": 0,
        "4xx": 0,
    }
    assert result["time_series"][0]["value"] == pytest.approx(expected_chart[metric])
    assert result["map_data"] == [{"country": "US", "count": 2}]
    assert result["data"]["shown_custom"] == {"top": [{"value": "shown", "count": 2}], "total": 2}
    assert result["data"]["waf_sig_ind"]["total"] == 6 and result["data"]["edge_score_reason_ind"]["total"] == 4
    assert result["data"]["rid"] == result["data"]["lat"] == {"top": [], "total": 0}
    assert not {"hidden_custom", "disabled_custom", "_source_file"} & result["data"].keys()
    assert len(temp_columns) == 1
    assert {
        "timestamp",
        "country",
        "ip",
        "ua",
        "url",
        "status",
        "elapsed",
        "ttfb",
        "cache",
        "resp_bytes",
        "req_header_bytes",
        "req_bytes",
        "conn_requests",
        "waf_sig",
        "edge_score_reason",
        "shown_custom",
    } <= temp_columns[0]
    assert not {"rid", "lat", "_source_file", "hidden_custom", "disabled_custom"} & temp_columns[0]
    assert count_queries and all("count(rid)" not in sql and "count(lat)" not in sql for sql in count_queries)


def test_dashboard_virtual_expansions_keep_raw_fallback_without_temp(
    in_memory_duckdb, test_service_source, monkeypatch
) -> None:
    """A rollup miss still uses the original inclusive bounds when no temp exists."""
    monkeypatch.setattr(dash, "DASHBOARD_CACHE_TTL", 0)
    monkeypatch.setattr(rr, "rollups_present", lambda _src: True)
    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", lambda *a, **k: ([], []))
    monkeypatch.setattr(QueryRunner, "try_count_from_rollup", lambda *a, **k: None)
    monkeypatch.setattr(QueryRunner, "try_virtual_field_top_n_from_rollup", lambda *a, **k: None)
    table = _empty_logs_table(in_memory_duckdb, test_service_source)
    in_memory_duckdb.execute(f"ALTER TABLE {table} ADD COLUMN edge_score_reason VARCHAR")
    in_memory_duckdb.executemany(
        f"""INSERT INTO {table} (timestamp, country, ip, url, waf_sig, edge_score_reason)
            VALUES (?, 'US', '192.0.2.1', '/fixture', ?, ?)""",
        [
            ("2026-10-27T20:45:59.999Z", "BEFORE", "before"),
            ("2026-10-27T20:46:00Z", "A,A", "bot"),
            ("2026-10-27T21:46:00Z", "B", "slow,slow"),
            ("2026-10-27T21:46:00.001Z", "AFTER", "after"),
        ],
    )
    result = get_aggregates(
        in_memory_duckdb,
        test_service_source,
        "2026-10-27T20:46:00Z",
        "2026-10-27T21:46:00Z",
        {},
        "1 minute",
        "requests",
        fields_filter=["waf_sig_ind", "edge_score_reason_ind"],
        include_time_series=False,
        include_conn_requests=False,
        include_map_data=False,
    )
    assert result["total_rows"] == 2
    for field, counts in (("waf_sig_ind", {"A": 2, "B": 1}), ("edge_score_reason_ind", {"slow": 2, "bot": 1})):
        assert {row["value"]: row["count"] for row in result["data"][field]["top"]} == counts
        assert result["data"][field]["total"] == 3
    assert not any("CREATE TEMP TABLE" in row["sql"] for row in result["debug_queries"])
    expansions = [row["sql"] for row in result["debug_queries"] if "unnest(string_split(" in row["sql"]]
    assert len(expansions) == 2 and all(f"FROM {table}" in sql for sql in expansions)


@pytest.mark.parametrize(
    "metric,expected_metric",
    [
        ("5xx", "5xx"),
        ("4xx", "4xx"),
        ("hit_rate", "hit_rate"),
        ("p95_latency", "p95_latency"),
        ("p50_latency", "p50_latency"),
        ("p99_latency", "p99_latency"),
        ("throughput", "throughput"),
        ("ttfb", "ttfb"),
        ("gibberish_not_a_metric", "requests"),
    ],
)
def test_get_aggregates_chart_metrics(in_memory_duckdb, test_service_source, metric, expected_metric):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    for log in logs[:5]:
        log["status"] = 500
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    flt = (
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])}
        if metric != "gibberish_not_a_metric"
        else {}
    )
    res = get_aggregates(in_memory_duckdb, test_service_source, None, None, flt, "1 minute", metric)
    assert res["metric"] == expected_metric
    if metric == "hit_rate" and res["time_series"]:
        assert 0.0 <= res["time_series"][0]["value"] <= 100.0


def test_self_heal_fires_once_when_stale_and_skips_when_outside_window_or_filtered(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    now = datetime.now(UTC)
    start_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    latest = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _name: {"local_rows": 30, "earliest_log_at": start_time, "latest_log_at": latest},
    )
    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", lambda *a, **k: ([("url", "some_url", 10)], ["url"]))

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        rebuild_calls["n"] += 1
        insert_mock_logs(con, table_name, logs)

    monkeypatch.setattr(dash, "force_rebuild_view", fake_rebuild)
    result = dash.get_aggregates(
        in_memory_duckdb, test_service_source, start_time, end_time, {}, "1 minute", "requests"
    )
    assert rebuild_calls["n"] == 1 and result["total_rows"] == 30 and result["data"]["url"]["total"] > 0

    rebuild_calls["n"] = 0
    old_start = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old_end = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    res_outside = dash.get_aggregates(
        in_memory_duckdb, test_service_source, old_start, old_end, {}, "1 minute", "requests"
    )
    assert rebuild_calls["n"] == 0 and res_outside["total_rows"] == 0

    res_filtered = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        start_time,
        end_time,
        {"country": FilterSpec(mode="include", values=["ZZ"])},
        "1 minute",
        "requests",
    )
    assert rebuild_calls["n"] == 0 and res_filtered["total_rows"] == 0


def test_get_aggregates_rollup_path_map_data_and_section_selectors(in_memory_duckdb, test_service_source, monkeypatch):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=40)
    )
    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    calls: list[tuple] = []

    def spy_top_n(self, fields, start_time, end_time, limit=10, per_field_limits=None, _phase_log=None, **_kwargs):
        calls.append((tuple(fields), limit, dict(per_field_limits or {})))
        country_entries = [("country", f"C{i:02d}", 100 - i) for i in range(12)]
        return country_entries + [("url", "/page1", 50), ("url", "/page2", 30)], list(fields)

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", spy_top_n)
    result = dash.get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    assert len(calls) == 1
    fields_called, limit_called, pfl_called = calls[0]
    assert "country" in fields_called and pfl_called.get("country") == 500 and limit_called == 10
    assert len(result["data"]["country"]["top"]) == 10 and len(result["map_data"]) == 12

    calls.clear()
    res_topten = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {},
        "1 minute",
        "requests",
        include_time_series=False,
        include_conn_requests=False,
        include_map_data=False,
        include_top_n=True,
    )
    assert len(calls) == 1 and len(calls[0][0]) > 5
    assert res_topten["data"]["url"]["top"] and res_topten["time_series"] == [] and res_topten["map_data"] == []

    for map_enabled, expected_calls in ((True, 1), (False, 0)):
        calls.clear()
        dash.get_aggregates(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {},
            "1 minute",
            "requests",
            include_time_series=True,
            include_conn_requests=True,
            include_map_data=map_enabled,
            include_top_n=False,
        )
        assert len(calls) == expected_calls


@pytest.mark.skipif(DASHBOARD_CACHE_TTL == 0, reason="Dashboard cache disabled")
def test_get_aggregates_result_is_cached(in_memory_duckdb, test_service_source):
    insert_mock_logs(
        in_memory_duckdb, _safe_table(test_service_source["name"]), generate_mock_logs(test_service_source, num_logs=10)
    )
    r1 = get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    r2 = get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, "1 minute", "requests")
    assert r2.get("is_cached") is True and r1["total_rows"] == r2["total_rows"]


@pytest.mark.parametrize(
    ("start", "end", "other_start", "other_end"),
    [
        ("2026-01-01T00:00:00Z", "2026-01-01T00:01:59Z", "2026-01-01T00:00:01Z", "2026-01-01T00:01:59Z"),
        ("2026-01-01T00:00:00Z", "2026-01-01T00:01:59Z", "2026-01-01T00:00:00Z", "2026-01-01T00:01:58Z"),
        (
            "2026-01-01T00:00:00.000001Z",
            "2026-01-01T00:01:59.999998Z",
            "2026-01-01T00:00:00.000002Z",
            "2026-01-01T00:01:59.999998Z",
        ),
        (
            "2026-01-01T00:00:00.000001Z",
            "2026-01-01T00:01:59.999998Z",
            "2026-01-01T00:00:00.000001Z",
            "2026-01-01T00:01:59.999999Z",
        ),
    ],
)
def test_dashboard_cache_preserves_exact_bounds_and_reuses_identical_window(
    monkeypatch, start, end, other_start, other_end
) -> None:
    """Cache get/peek must not alias second- or microsecond-distinct windows."""
    monkeypatch.setattr(dash, "DASHBOARD_CACHE_TTL", 30)
    monkeypatch.setattr(dash, "_dashboard_cache", dash.BoundedTTLCache(maxsize=10, ttl_seconds=30))
    src = {"name": "dashboard_exact_window", "service_id": "test-dashboard-exact-window"}
    calls: list[tuple[str, str]] = []

    def compute(**kwargs: Any) -> dict:
        calls.append((kwargs["start_time"], kwargs["end_time"]))
        payload = {"total_rows": len(calls), "time_series": [], "map_data": [], "data": {}}
        dash._dashboard_cache[kwargs["cache_key"]] = (kwargs["now"], payload)
        return payload

    def no_connection() -> Any:
        pytest.fail("Cache-only characterization must not acquire a connection")

    monkeypatch.setattr(dash, "_compute_dashboard_aggregates", compute)
    key_args = dict(src=src, filters={}, chart_interval="1 minute", chart_metric="requests")
    first_key = dash._dashboard_cache_key(start_time=start, end_time=end, **key_args)
    other_key = dash._dashboard_cache_key(start_time=other_start, end_time=other_end, **key_args)
    assert first_key != other_key
    assert first_key == dash._dashboard_cache_key(start_time=start, end_time=end, **key_args)
    first = get_aggregates(con=no_connection, start_time=start, end_time=end, **key_args)
    assert first["total_rows"] == 1
    assert dash.peek_aggregates(start_time=other_start, end_time=other_end, **key_args) is None
    second = get_aggregates(con=no_connection, start_time=other_start, end_time=other_end, **key_args)
    assert second["total_rows"] == 2
    repeated = get_aggregates(con=no_connection, start_time=start, end_time=end, **key_args)
    peeked = dash.peek_aggregates(start_time=start, end_time=end, **key_args)
    assert repeated["is_cached"] is True and repeated["total_rows"] == 1
    assert peeked is not None and peeked["is_cached"] is True and peeked["total_rows"] == 1
    assert calls == [(start, end), (other_start, other_end)]


def test_get_raw_df_and_add_bot_columns(in_memory_duckdb, test_service_source):
    assert get_raw_df(in_memory_duckdb, _src_without_table(), None, None, {}, limit=100, columns=[]).empty
    insert_mock_logs(
        in_memory_duckdb,
        _safe_table(test_service_source["name"]),
        generate_mock_logs(test_service_source, num_logs=5, hours_ago=1),
    )
    out = get_raw_df(in_memory_duckdb, test_service_source, None, None, {}, limit=5, columns=["timestamp", "status"])
    assert isinstance(out, pd.DataFrame) and len(out) <= 5 and set(out.columns) <= {"timestamp", "status"}

    select_cols = ['"status"']
    assert _add_bot_columns({"ua", "ip", "waf_req_id"}, ["status"], select_cols) == (False, False)
    select_cols = ['"timestamp"']
    assert _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols) == (True, False)
    assert '"ua"' in select_cols and '"ip"' in select_cols
    select_cols = ['"ua"']
    _add_bot_columns({"ua", "ip"}, ["_bot_name"], select_cols)
    assert select_cols.count('"ua"') == 1
    select_cols = ['"status"']
    assert _add_bot_columns(set(), ["_bot_name"], select_cols) == (True, False) and select_cols == ['"status"']
    select_cols = ['"timestamp"']
    assert _add_bot_columns({"waf_req_id"}, ["_ngwaf_bot_name"], select_cols) == (False, True)
    assert '"waf_req_id"' in select_cols and '"ua"' not in select_cols


@pytest.mark.parametrize(
    ("window_start", "earliest_event_at", "expected"),
    [
        ("2026-10-01T00:00:00Z", "2026-09-25T05:38:42Z", True),
        ("2026-10-01T00:00:00Z", "2026-10-01T00:00:01Z", False),
        ("2026-10-01T00:00:00Z", None, None),
    ],
)
def test_raw_extent_start_coverage_is_nullable_and_boundary_aware(window_start, earliest_event_at, expected):
    assert _raw_extent_reaches_window_start(window_start, earliest_event_at) is expected


def test_get_field_values_validation_and_country_and_cache(in_memory_duckdb, test_service_source, tmp_path):
    with pytest.raises(ValueError, match="Invalid field name"):
        get_field_values(in_memory_duckdb, test_service_source, "\";'''", "", 10, None, None, {})

    for fld in ("country", "_bot_name"):
        assert get_field_values(in_memory_duckdb, _src_without_table(), fld, "", 10, None, None, {})["values"] == []

    logs = generate_mock_logs(test_service_source, num_logs=30, hours_ago=1)
    for i, log in enumerate(logs):
        log["country"] = "US" if i < 20 else "GB"
        log["waf_sig"] = "SQLI,XSS" if i % 2 == 0 else "SQLI"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    for bad_field in ('"; DROP TABLE foo', "completely_unknown_field"):
        with pytest.raises(LookupError):
            get_field_values(in_memory_duckdb, test_service_source, bad_field, "", 10, None, None, {})

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = get_field_values(
            in_memory_duckdb,
            test_service_source,
            "country",
            "",
            10,
            None,
            None,
            {"country": FilterSpec(mode="include", values=["US"])},
        )
        vals = {v["value"]: v["count"] for v in out["values"]}
        assert vals["US"] == 20 and vals["GB"] == 10

        out_united = get_field_values(in_memory_duckdb, test_service_source, "country", "united", 10, None, None, {})
        assert {"US", "GB"} <= {v["value"] for v in out_united["values"]}

        out_waf = get_field_values(in_memory_duckdb, test_service_source, "waf_sig_ind", "", 10, None, None, {})
        assert {v["value"]: v["count"] for v in out_waf["values"]} == {"SQLI": 30, "XSS": 15}

        out_waf_search = get_field_values(
            in_memory_duckdb, test_service_source, "waf_sig_ind", "SQL", 10, None, None, {}
        )
        assert {v["value"] for v in out_waf_search["values"]} == {"SQLI"}

        (tmp_path / "top_values.json").write_text(_json.dumps({"country": [{"value": "ZZ", "count": 999}]}))
        out_cached = get_field_values(in_memory_duckdb, test_service_source, "country", "", 10, None, None, {})
        assert out_cached["values"][0]["value"] == "ZZ"


@pytest.mark.parametrize(
    ("start_time", "end_time", "filters", "expected"),
    [
        ("2026-01-02T00:00:00Z", "2026-01-02T23:59:59Z", {}, {"GET": 1, "POST": 1}),
        ("2026-01-02T00:00:00Z", None, {}, {"GET": 1, "POST": 1, "DELETE": 1}),
        (None, "2026-01-02T23:59:59Z", {}, {"GET": 2, "POST": 1}),
        (None, None, {"country": FilterSpec(mode="include", values=["US"])}, {"GET": 2}),
        (
            None,
            None,
            {
                "country": FilterSpec(mode="include", values=["GB"]),
                "method": FilterSpec(mode="include", values=["GET"]),
            },
            {"POST": 1, "DELETE": 1},
        ),
    ],
)
def test_get_field_values_scoped_requests_ignore_sample_cache(
    in_memory_duckdb, test_service_source, tmp_path, start_time, end_time, filters, expected
):
    table_name = _empty_logs_table(in_memory_duckdb, test_service_source)
    in_memory_duckdb.executemany(
        f"""INSERT INTO {table_name} (timestamp, "method", country, ip, url)
            VALUES (?, ?, ?, '192.0.2.1', '/fixture')""",
        [
            ("2026-01-01T12:00:00Z", "GET", "US"),
            ("2026-01-02T12:00:00Z", "GET", "US"),
            ("2026-01-02T12:00:00Z", "POST", "GB"),
            ("2026-01-03T12:00:00Z", "DELETE", "GB"),
        ],
    )
    (tmp_path / "top_values.json").write_text(_json.dumps({"method": [{"value": "CACHED_ONLY", "count": 999}]}))
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        result = get_field_values(
            in_memory_duckdb, test_service_source, "method", "", 10, start_time, end_time, filters
        )
    assert {row["value"]: row["count"] for row in result["values"]} == expected


@pytest.mark.parametrize("now", ["2026-10-27T03:30:00Z", "2026-10-27T05:30:00Z"])
@pytest.mark.parametrize("missing_hour", [False, True])
@pytest.mark.parametrize(
    ("start", "end"),
    [("02:03", "03:03"), ("02:00", "03:00"), ("02:03", "02:40"), ("02:00", "04:00"), ("03:01", "03:03")],
)
def test_dashboard_country_rollups_match_scoped_filter_at_hour_boundaries(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, now, start, end, missing_hour
):
    top_n_queries = []
    execute_top_n_rollups = QueryRunner.execute_top_n_rollups

    def capture_top_n_queries(runner, *args, **kwargs):
        before = len(runner.debug_queries)
        result = execute_top_n_rollups(runner, *args, **kwargs)
        top_n_queries.extend(runner.debug_queries[before:])
        return result

    monkeypatch.setattr(QueryRunner, "execute_top_n_rollups", capture_top_n_queries)
    table = _empty_logs_table(in_memory_duckdb, test_service_source)
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} (timestamp, country, ip, url, waf_sig) VALUES (?, ?, '192.0.2.1', '/fixture', 'A, B')",
        [
            (f"2026-10-27T{stamp}:00Z", country)
            for stamp, country in [
                ("02:00", "US"),
                ("02:02", "US"),
                ("02:03", "US"),
                ("02:30", "CA"),
                ("03:00", "US"),
                ("03:02", "US"),
                ("03:03", "US"),
                ("03:03", "US"),
                ("03:04", "US"),
                ("03:59", "CA"),
                ("04:00", "US"),
            ]
        ],
    )
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    monkeypatch.setattr(rr, "rollups_present", lambda _src: True)
    monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
    for hour in ("02", "03"):
        if missing_hour and hour == "03":
            continue
        directory = tmp_path / "rollups" / "hour_bundled" / f"hour=2026-10-27-{hour}"
        directory.mkdir(parents=True)
        in_memory_duckdb.execute(
            f"""COPY (
                WITH events AS (SELECT * FROM {table} WHERE strftime(timestamp, '%H') = ?)
                SELECT 'country' AS field, country AS value, count(*) AS count FROM events GROUP BY country
                UNION ALL
                SELECT 'waf_sig_ind', trim(signal), count(*)
                FROM (SELECT unnest(string_split(waf_sig, ',')) AS signal FROM events)
                GROUP BY 2
            ) TO ? (FORMAT PARQUET)""",
            [str(directory / "all_fields.parquet"), hour],
        )
    kwargs = dict(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=f"2026-10-27T{start}:00Z",
        end_time=f"2026-10-27T{end}:00Z",
        chart_interval="1 minute",
        chart_metric="requests",
        fields_filter=["country", "waf_sig_ind"],
        include_time_series=False,
        include_conn_requests=False,
    )
    with time_machine.travel(now, tick=False):
        initial = get_aggregates(filters={}, **kwargs)
        filtered = get_aggregates(filters={"country": FilterSpec(mode="include", values=["US"])}, **kwargs)
    initial_us = next(row["count"] for row in initial["data"]["country"]["top"] if row["value"] == "US")
    assert initial_us == filtered["total_rows"]
    assert {row["country"]: row["count"] for row in initial["map_data"]}["US"] == initial_us
    assert {row["value"]: row["count"] for row in initial["data"]["waf_sig_ind"]["top"]} == {
        "A": initial["total_rows"],
        "B": initial["total_rows"],
    }
    extent_idx = next(
        i for i, row in enumerate(initial["debug_queries"]) if "min(timestamp), max(timestamp)" in row["sql"]
    )
    top_n_queries = initial["debug_queries"][extent_idx + 1 :]
    if start == "02:00" and end in ("03:00", "04:00"):
        assert any("read_parquet" in row["sql"] for row in initial["debug_queries"])
    assert top_n_queries
    assert not any("strftime(timestamp, '%Y-%m-%d-%H') IN" in row["sql"] for row in top_n_queries)
    raw_table_queries = [
        row["sql"] for row in top_n_queries if f"FROM {table}" in row["sql"] or f"FROM ({table})" in row["sql"]
    ]
    assert len(raw_table_queries) <= 2


@pytest.mark.parametrize("now", ["2026-10-27T03:30:00Z", "2026-10-27T05:30:00Z"])
@pytest.mark.parametrize(
    ("layout", "race"),
    [
        ("per_field_hour", "cached_listing"),
        ("per_field_hour", "after_selection"),
        ("bundled_hour", "after_selection"),
        ("bundled_day", "after_selection"),
    ],
)
def test_dashboard_missing_stored_rollup_matches_full_scoped_raw(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, now, layout, race
):
    table = _empty_logs_table(in_memory_duckdb, test_service_source)
    start, end = "2026-10-24T02:03:00.125Z", "2026-10-27T03:03:00.250Z"
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} (timestamp, country, ip, url, waf_sig) VALUES (?, ?, '192.0.2.1', '/fixture', 'A, B')",
        [
            ("2026-10-24T02:03:00.124Z", "US"),
            (start, "US"),
            ("2026-10-24T02:30:00Z", "CA"),
            ("2026-10-25T02:00:00Z", "US"),
            ("2026-10-25T02:30:00Z", "CA"),
            ("2026-10-26T02:30:00Z", "CA"),
            ("2026-10-27T02:30:00Z", "US"),
            ("2026-10-27T03:00:00Z", "CA"),
            (end, "US"),
            (end, "US"),
            ("2026-10-27T03:03:00.251Z", "US"),
        ],
    )
    if layout == "per_field_hour":
        vanished = tmp_path / "rollups/hour/field=country/hour=2026-10-25-02/part.parquet"
    elif layout == "bundled_hour":
        vanished = tmp_path / "rollups/hour_bundled/hour=2026-10-25-02/all_fields.parquet"
    else:
        vanished = tmp_path / "rollups/day_bundled/day=2026-10-25/all_fields.parquet"
    survivor = tmp_path / "rollups/hour_bundled/hour=2026-10-26-02/all_fields.parquet"
    for path, day in [(vanished, "2026-10-25"), (survivor, "2026-10-26")]:
        path.parent.mkdir(parents=True)
        in_memory_duckdb.execute(
            f"""COPY (
                WITH events AS (SELECT * FROM {table} WHERE CAST(timestamp AS DATE) = DATE '{day}')
                SELECT 'country' AS field, country AS value, count(*) AS count FROM events GROUP BY country
            ) TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    monkeypatch.setattr(rr, "rollups_present", lambda _src: True)
    monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
    # Keep the existing unchanged-SQL retry local: no storage view or catalog setup.
    refreshes = []
    monkeypatch.setattr(_base, "_refresh_stale_view", lambda *args: refreshes.append(True))
    build_sql = _base._build_rolled_sql
    selected = []

    def remove_selected_file(paths, *args, **kwargs):
        assert str(vanished) in paths.hour_paths + paths.bundled_hour_paths + paths.bundled_day_paths
        selected.append(True)
        sql = build_sql(paths, *args, **kwargs)
        if race == "after_selection":
            assert vanished.is_file()
            vanished.unlink()
        return sql

    monkeypatch.setattr(_base, "_build_rolled_sql", remove_selected_file)
    if race == "cached_listing":
        assert vanished.name in rollup_coverage.cached_listdir(str(vanished.parent))
        vanished.unlink()
    expected = dict(
        in_memory_duckdb.execute(
            f"SELECT country, COUNT(*) FROM {table} WHERE timestamp >= ? AND timestamp <= ? GROUP BY country",
            [start, end],
        ).fetchall()
    )
    assert expected == {"US": 5, "CA": 4}
    with time_machine.travel(now, tick=False):
        result = get_aggregates(
            in_memory_duckdb,
            test_service_source,
            start,
            end,
            {},
            "1 minute",
            "requests",
            fields_filter=["country", "waf_sig_ind"],
            include_time_series=False,
            include_conn_requests=False,
        )
    assert selected and refreshes
    assert survivor.is_file()
    assert {row["value"]: row["count"] for row in result["data"]["country"]["top"]} == expected
    assert {row["country"]: row["count"] for row in result["map_data"]} == expected
    assert result["total_rows"] == sum(expected.values())
    assert {row["value"]: row["count"] for row in result["data"]["waf_sig_ind"]["top"]} == {"A": 9, "B": 9}


@pytest.mark.parametrize(
    "failed_read", ["rollup", "boundary", "active", "unrelated_missing", "permission", "raw_fallback", "interrupt"]
)
def test_dashboard_exact_top_n_propagates_read_failures(
    in_memory_duckdb, test_service_source, tmp_path, monkeypatch, failed_read
):
    table = _empty_logs_table(in_memory_duckdb, test_service_source)
    directory = tmp_path / "rollups" / "hour_bundled" / "hour=2026-10-27-02"
    directory.mkdir(parents=True)
    in_memory_duckdb.execute(
        "COPY (SELECT 'country' AS field, 'US' AS value, 1::BIGINT AS count) TO ? (FORMAT PARQUET)",
        [str(directory / "all_fields.parquet")],
    )
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    monkeypatch.setattr(_base, "_refresh_stale_view", lambda *args: None)
    runner = QueryRunner(in_memory_duckdb, test_service_source)
    execute = runner.execute

    def fail_execute(sql, *args, **kwargs):
        if "read_parquet" in sql:
            if failed_read == "unrelated_missing":
                raise duckdb.IOException('No files found that match "/unrelated.parquet": fixture read failure')
            if failed_read == "permission":
                raise duckdb.IOException(
                    f'Cannot open file "{directory / "all_fields.parquet"}": Permission denied: fixture read failure'
                )
            if failed_read == "interrupt":
                raise duckdb.InterruptException("fixture read failure")
            if failed_read == "raw_fallback":
                (directory / "all_fields.parquet").unlink()
        if (failed_read == "rollup" and "read_parquet" in sql) or (
            failed_read in ("boundary", "raw_fallback") and "SELECT * FROM" in sql
        ):
            raise duckdb.IOException("fixture read failure")
        return execute(sql, *args, **kwargs)

    def fail_active(*args, **kwargs):
        raise duckdb.IOException("fixture read failure")

    monkeypatch.setattr(runner, "execute", fail_execute)
    monkeypatch.setattr(
        runner, "_create_active_hour_temp_direct", fail_active if failed_read == "active" else lambda *a, **k: None
    )
    with (
        time_machine.travel("2026-10-27T03:30:00Z", tick=False),
        pytest.raises(
            duckdb.InterruptException if failed_read == "interrupt" else duckdb.IOException,
            match="fixture read failure",
        ),
    ):
        runner.execute_top_n_rollups(["country"], "2026-10-27T02:00:00Z", "2026-10-27T03:03:00Z", exact_window=True)


def test_get_field_values_bot_name_and_asn_search(in_memory_duckdb, test_service_source, tmp_path):
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for i, log in enumerate(logs):
        log["ua"] = "Bingbot/2.0"
        log["asn"] = 13335 if i < 6 else 32934
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=""),
        patch(
            "backend.utils.bot_sources.build_matcher",
            side_effect=lambda: lambda _ua: [{"id": "bingbot", "name": "Microsoft Bingbot"}],
        ),
    ):
        out = get_field_values(in_memory_duckdb, test_service_source, "_bot_name", "microsoft", 10, None, None, {})
        assert out["values"][0]["value"] == "bingbot" and out["values"][0]["count"] == 10
        out_miss = get_field_values(in_memory_duckdb, test_service_source, "_bot_name", "google", 10, None, None, {})
        assert out_miss["values"] == []

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", return_value=[13335]),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_asn = get_field_values(in_memory_duckdb, test_service_source, "asn", "cloudflare", 10, None, None, {})
        assert {v["value"] for v in out_asn["values"]} == {13335}

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)),
        patch("backend.core.metadata.asn_ints_for_search", side_effect=RuntimeError("db locked")),
        patch("backend.core.duckdb.enrich_asn_labels"),
    ):
        out_fallback = get_field_values(in_memory_duckdb, test_service_source, "asn", "133", 10, None, None, {})
        assert {v["value"] for v in out_fallback["values"]} == {13335}


def test_get_aggregates_rollup_temp_avoidance_virtual_fields_and_conn_requests_fallback(
    in_memory_duckdb, test_service_source, monkeypatch
):
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, generate_mock_logs(test_service_source, num_logs=4))
    in_memory_duckdb.execute(f"UPDATE {table_name} SET conn_requests = 1")
    in_memory_duckdb.execute(
        f"UPDATE {table_name} SET conn_requests = 7 WHERE rowid IN (SELECT rowid FROM {table_name} LIMIT 1)"
    )

    monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src: True)
    monkeypatch.setattr(
        QueryRunner, "execute_top_n_rollups", lambda self, fields, s, e, **kw: ([("url", "/p", 5)], fields)
    )

    sentinel = {"top": [{"value": "1", "count": 5}, {"value": "21+", "count": 2}], "total": 7}
    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda self, s, e, **kw: dict(sentinel))
    virtual_calls: list[str] = []
    monkeypatch.setattr(
        QueryRunner,
        "try_virtual_field_top_n_from_rollup",
        lambda self, vid, bcol, s, e, **kw: (virtual_calls.append(vid), {"top": [], "total": 0})[1],
    )
    temp_creates: list[str] = []
    orig_ctt = QueryRunner.create_temp_table
    monkeypatch.setattr(
        QueryRunner,
        "create_temp_table",
        lambda self, sql, params=None: (temp_creates.append(sql), orig_ctt(self, sql, params))[1],
    )

    result = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        {},
        "1 minute",
        "requests",
    )
    assert temp_creates == [] and result["data"]["conn_requests"] == sentinel
    assert "waf_sig_ind" in virtual_calls and result["data"]["waf_sig_ind"] == {"top": [], "total": 0}

    monkeypatch.setattr(QueryRunner, "try_conn_requests_hist_from_rollup", lambda self, s, e, **kw: None)
    st = (datetime.now(UTC) - timedelta(hours=26)).isoformat()
    et = (datetime.now(UTC) + timedelta(hours=26)).isoformat()
    res_fb = dash.get_aggregates(
        in_memory_duckdb,
        test_service_source,
        st,
        et,
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        "1 minute",
        "requests",
    )
    assert res_fb["data"]["conn_requests"] == {
        "top": [{"value": "1", "count": 3}, {"value": "6–20", "count": 1}],
        "total": 4,
    }


def test_get_aggregates_prunes_non_top_n_fields(in_memory_duckdb, test_service_source, monkeypatch):
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, status VARCHAR, host VARCHAR, req_bytes INTEGER, resp_bytes INTEGER)"
    )
    rollup_calls, batch_calls = [], []
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_rollups",
        lambda self, fields, s, e, **kw: (rollup_calls.append(list(fields)), ([], list(fields)))[1],
    )
    monkeypatch.setattr(
        QueryRunner,
        "execute_top_n_batch",
        lambda self, fields, *a, **kw: (batch_calls.append(list(fields)), ([], []))[1],
    )

    for use_rollups, target, expected in (
        (True, rollup_calls, {"status", "host", "country"}),
        (False, batch_calls, {"status", "host"}),
    ):
        monkeypatch.setattr("backend.core.rollups.rollups_present", lambda src, ur=use_rollups: ur)
        dash.get_aggregates(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {} if use_rollups else {"status": FilterSpec(mode="include", values=["200"])},
            "1 minute",
            "requests",
            fields_filter=["status", "req_bytes", "resp_bytes", "host"],
            include_time_series=False,
            include_conn_requests=False,
            include_map_data=False,
            include_top_n=True,
        )
        assert len(target) == 1 and set(target[0]) == expected


def _write_time_series_hour(con: duckdb.DuckDBPyConnection, hour_root: str, hour_str: str, reqs: int = 10) -> None:
    ts_dir = Path(hour_root) / f"hour={hour_str}"
    ts_dir.mkdir(parents=True, exist_ok=True)
    out = ts_dir / TIME_SERIES_BUNDLE_FILENAME
    bucket = f"{hour_str[:10]} {hour_str[11:13]}:00:00Z"
    con.execute(
        f"COPY (SELECT TIMESTAMPTZ '{bucket}' AS bucket, CAST({reqs} AS BIGINT) AS requests, "
        "CAST(0 AS BIGINT) AS status_4xx, CAST(0 AS BIGINT) AS status_5xx, CAST(5 AS BIGINT) AS hits, "
        "CAST(8 AS BIGINT) AS cache_total, CAST(1000 AS BIGINT) AS resp_bytes_sum, "
        f"CAST(2.0 AS DOUBLE) AS ttfb_sum, CAST(10 AS BIGINT) AS ttfb_count) TO '{out}' (FORMAT PARQUET)"
    )


def _write_day_overview(con: duckdb.DuckDBPyConnection, day_root: str, day_str: str, reqs_per_hour: int = 10) -> None:
    day_dir = Path(day_root) / f"day={day_str}"
    day_dir.mkdir(parents=True, exist_ok=True)
    out = day_dir / OVERVIEW_BUNDLE_FILENAME
    rows = ", ".join(f"('{day_str}T{h:02d}', CAST({reqs_per_hour} AS BIGINT))" for h in range(24))
    con.execute(f"COPY (SELECT * FROM (VALUES {rows}) AS t(hour_start, requests)) TO '{out}' (FORMAT PARQUET)")


def test_compact_time_series_closed_days_and_30d_and_rolling_window_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    con = duckdb.connect(":memory:")
    src = {"name": "svc_30d", "service_id": "svc_30d"}
    con.execute(
        "CREATE TABLE logs_svc_30d (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root, day_root = _hour_bundled_root(src), _day_bundled_root(src)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    st = (now - timedelta(days=29)).replace(hour=0)
    cur_day, yesterday = st.date(), (now - timedelta(days=1)).date()
    while cur_day < yesterday:
        _write_day_overview(con, day_root, cur_day.isoformat(), reqs_per_hour=10)
        cur_day += timedelta(days=1)
    for h in range(24):
        _write_time_series_hour(con, hour_root, f"{yesterday.isoformat()}-{h:02d}", reqs=20)

    assert compact_time_series_closed_days_to_daily("svc_30d", src, lookback_days=5) == 1
    assert (Path(day_root) / f"day={yesterday.isoformat()}" / TIME_SERIES_BUNDLE_FILENAME).is_file()
    for h in range(now.hour):
        _write_time_series_hour(con, hour_root, f"{now.date().isoformat()}-{h:02d}", reqs=30)

    rollup_coverage._listdir_cache.clear()
    runner = repo_base.QueryRunner(con, src)
    total = runner.try_count_from_rollup(
        st.isoformat(), now.isoformat(), "logs_svc_30d", "1=1", [], unfiltered_window=True
    )
    assert total is not None and total > 0
    ts_rows = runner.try_time_series_from_rollup(
        "requests", "1 hour", st.isoformat(), now.isoformat(), "logs_svc_30d", "1=1", [], unfiltered_window=True
    )
    assert ts_rows is not None and len(ts_rows) > 24


def test_rolling_window_uses_daily_time_series_bundle_when_day0_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_core, "_cache_dir", lambda _src: str(tmp_path))
    service = "svc_rolling_ts"
    src = {"name": service, "service_id": service}
    con = duckdb.connect(":memory:")
    con.execute(
        f"CREATE TABLE logs_{service} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    hour_root = _hour_bundled_root(src)
    now = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    days = [(now - timedelta(days=i)).date().isoformat() for i in range(7, 0, -1)]
    for d in days:
        for h in range(24):
            _write_time_series_hour(con, hour_root, f"{d}-{h:02d}", reqs=5)
    assert compact_time_series_closed_days_to_daily(service, src, lookback_days=10) == 7

    rollup_coverage._listdir_cache.clear()
    runner = repo_base.QueryRunner(con, src)
    ts = runner.try_time_series_from_rollup(
        "requests",
        "1 hour",
        f"{days[0]}T12:00:00Z",
        f"{now.date().isoformat()}T00:00:00Z",
        f"logs_{service}",
        "1=1",
        [],
        unfiltered_window=True,
    )
    assert ts is not None and sum(int(pt["value"]) for pt in ts) == (12 + 6 * 24) * 5
    assert any("read_parquet([7 files])" in q["sql"] for q in runner.telemetry()["debug_queries"])


def test_clickhouse_aggregates_field_values_and_raw_df(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_queries: list[str] = []

    class FakeClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            executed_queries.append(sql)
            p = params or {}
            dims = [v for k, v in sorted(p.items()) if k.startswith("dim_")]
            if "AS chart_bucket" in sql:
                return [{"chart_bucket": "2026-10-01 00:00:00", "value": 1000}]
            if "AS bucket_start" in sql or "SELECT bucket_start" in sql:
                return [{"bucket_start": "2026-10-01 00:00:00", "value": 1000}]
            if "GROUP BY dimension" in sql and "GROUP BY dimension, value" not in sql:
                return [{"dimension": d, "total_count": 2} for d in dims]
            if "LIMIT 10 BY dimension" in sql:
                return [{"dimension": d, "value": f"{d}_v1", "aggregate_count": 100} for d in dims]
            if "request_aggregates" in sql and "AS total_count" in sql:
                return [{"total_count": 2}]
            if "request_aggregates" in sql:
                return [{"value": "/api/v1", "aggregate_count": 50}, {"value": "/health", "aggregate_count": 10}]
            if "GROUP BY v" in sql:
                return [{"v": "/api/v1", "c": 50}]
            if "FROM request_facts" in sql:
                return [
                    {
                        "timestamp": datetime(2026, 10, 20, 12, 0, tzinfo=UTC),
                        "ip": "198.51.100.1",
                        "url": "/api/v1",
                        "country": "US",
                        "custom_fields": {"custom_header": "abc", "status": "200"},
                    }
                ]
            return [{"value": "US", "aggregate_count": 100, "total_count": 100}]

    service = HighScaleService(
        service_id="svc1",
        client=FakeClient(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="svc1",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 20, tzinfo=UTC),
            coverage_end=datetime(2026, 10, 20, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )

    res = high_scale_dashboard.aggregates(
        service,
        AggregatesRequest(fields=["url", "status", "country"], chart_interval="1 hour", chart_metric="requests"),
        "2026-09-20T00:00:00Z",
        "2026-10-20T00:00:00Z",
    )
    assert res.data["url"].total == 2 and res.data["url"].top[0].value == "url_v1" and len(executed_queries) <= 6

    monkeypatch.setattr(bot_sources, "enrich_bot_metadata", lambda df: None)
    unfiltered_fv = high_scale_dashboard.field_values(
        service, FieldValuesRequest(field="url", limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z"
    )
    assert len(unfiltered_fv.values) == 2 and unfiltered_fv.values[0].value == "/api/v1"
    search_fv = high_scale_dashboard.field_values(
        service, FieldValuesRequest(field="url", search="api", limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z"
    )
    assert len(search_fv.values) == 1 and search_fv.values[0].value == "/api/v1"
    df = high_scale_dashboard.raw_df(
        service, RawRequest(limit=10), "2026-10-19T00:00:00Z", "2026-10-20T00:00:00Z", limit=10
    )
    assert len(df) == 1 and df.loc[0, "custom_header"] == "abc" and "custom_fields" not in df.columns


def test_pii_policy_page_telemetry_invalidate_and_bot_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "schema": [
            {"column": "host", "type": "VARCHAR", "min": "example.com", "max": "z.example.com"},
            {"column": "client_ip", "type": "VARCHAR", "min": "100.1.232.227", "max": "199.232.65.91"},
        ],
        "services": [
            {
                "service_id": "svc1",
                "status": {
                    "schema": [
                        {"column": "client_ip", "type": "VARCHAR", "min": "99.95.121.49", "max": "199.232.65.91"}
                    ]
                },
            }
        ],
    }
    masked = apply_pii_policy(payload, {"mask_ips": True})
    assert (
        masked["schema"][1]["min"] == "100.1.232.xxx"
        and masked["services"][0]["status"]["schema"][0]["min"] == "99.95.121.xxx"
    )

    recorded: list[dict] = []
    monkeypatch.setattr(usage_log, "log_telemetry_query", lambda **kw: recorded.append(kw))
    con = duckdb.connect(":memory:")
    set_page_load_id("test-page-load-01")
    try:
        repo_base.QueryRunner(con, {"name": "svc", "service_id": "svc"}).execute("SELECT 1 AS x")
    finally:
        set_page_load_id(None)
    assert len(recorded) == 1 and recorded[0]["page_load_id"] == "test-page-load-01"

    dash._dashboard_cache["abc123:svc_test"] = (1.0, {"total_rows": 10})
    dash._dashboard_cache["top_bots:def456:svc_test"] = (1.0, {"bots": [{"name": "Bot"}]})
    dash._dashboard_cache["other:svc_keep"] = (1.0, {"total_rows": 5})
    dash.invalidate_service("svc_test")
    assert "abc123:svc_test" not in dash._dashboard_cache and "other:svc_keep" in dash._dashboard_cache
    dash.invalidate_service("svc_keep")

    fake_feeds = {
        "openai-gptbot": [ipaddress.ip_network(f"198.51.{i}.0/24") for i in range(100)],
        "anthropic-ai": [ipaddress.ip_network("203.0.113.0/24"), ipaddress.ip_network("2001:db8::/32")],
    }
    monkeypatch.setattr(bot_sources, "_get_compiled_ip_feeds", lambda: fake_feeds)
    monkeypatch.setattr(bot_sources, "_compiled_ip_feeds", {})
    monkeypatch.setattr(rdns_cache, "get_hostnames", lambda ips: {})
    df = pd.DataFrame(
        [
            {"ua": "Mozilla/5.0 (compatible; GPTBot/1.0)", "ip": "198.51.42.10", "waf_req_id": ""},
            {"ua": "Mozilla/5.0", "ip": "203.0.113.99", "waf_req_id": ""},
            {"ua": "Mozilla/5.0", "ip": "192.0.2.1", "waf_req_id": ""},
        ]
        * 2000
    )
    t0 = time.perf_counter()
    bot_sources.enrich_bot_metadata(df)
    assert time.perf_counter() - t0 < 5.0
    assert (
        "openai-gptbot" in str(df.loc[0, "_bot_name"])
        and "IP-Match" in str(df.loc[1, "_bot_name"])
        and df.loc[2, "_bot_name"] == "null"
    )


# ── Performance & CMCD aggregates tests (folded from test_performance.py) ──

_ALL_PERF_SECTIONS = {"waterfall", "top_urls", "top_asns", "ttl_dist", "scatter"}
_T0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)


def _perf_latency_hit(*_a, **kw):
    value = "/rolled" if kw.get("dimension") == "url" else 7922
    return {"rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]}


def test_performance_aggregates(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=50, hours_ago=1)
    for i, log in enumerate(logs[:5]):
        log["elapsed"] = 500000 + (i * 10000)
        log["ttfb"] = f"{(400000 + (i * 10000)) / 1000000:.6f}"
        log["url"] = f"/slow-path-{i}"
        log["asn"] = 7922
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {})
    assert isinstance(result["top_urls"], list) and isinstance(result["top_asns"], list)
    if result["top_urls"]:
        assert result["top_urls"][0]["p99"] > 0


def test_get_performance_aggregates_returns_empty_arrays_for_unknown_schema(in_memory_duckdb, test_service_source):
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {})
    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
        assert result[key] == []


@pytest.mark.parametrize(
    ("sort_by", "expected_first"),
    [(None, "/high-tail"), ("p50", "/slow-median")],
)
def test_get_performance_aggregates_top_urls_sorting(in_memory_duckdb, test_service_source, sort_by, expected_first):
    logs = generate_mock_logs(test_service_source, num_logs=80, hours_ago=1)
    for i, log in enumerate(logs):
        if sort_by == "p50":
            log["url"] = "/slow-median" if i < 40 else "/spiky"
            log["elapsed"] = 200_000 if i < 40 else (50_000 if i % 5 != 0 else 800_000)
        else:
            log["url"] = "/high-tail" if i < 40 else "/steady"
            log["elapsed"] = (100_000 if i % 10 != 0 else 1_000_000) if i < 40 else 100_000
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    kw = {"sort_by": sort_by} if sort_by else {}
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, {}, **kw)
    if len(result["top_urls"]) >= 2:
        assert result["top_urls"][0]["url"] == expected_first


def test_get_performance_aggregates_ttl_dist_and_scatter(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    ttl_values = [5, 25, 50, 200, 500, 3600, 86400 * 2]
    for i, log in enumerate(logs):
        log["ttl"] = ttl_values[i % len(ttl_values)]
        log["elapsed"] = 100_000
        log["ttfb"] = f"{0.05:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
    )
    buckets = {row["bucket"] for row in result["ttl_dist"]}
    assert any(b in buckets for b in ("<10s", "<30s", "<1m", "<5m", "<10m", "<1h", "<3d"))
    assert all(pt["edge"] >= 0 for pt in result["scatter"])


@pytest.mark.parametrize(
    ("sections", "filtered", "expected_present", "expected_timers", "blocked_timers"),
    [
        (None, False, _ALL_PERF_SECTIONS, set(), set()),
        (
            {"ttl_dist"},
            True,
            {"ttl_dist"},
            {"ttl_dist_query"},
            {"top_urls_query", "top_asns_query", "scatter_waterfall_query"},
        ),
        ({"top_urls", "top_asns"}, True, {"top_urls", "top_asns"}, {"top_urls_query", "top_asns_query"}, set()),
        (
            {"top_urls", "top_asns", "ttl_dist"},
            False,
            {"top_urls", "top_asns", "ttl_dist"},
            set(),
            {"scatter_waterfall_query"},
        ),
        (
            {"waterfall", "scatter"},
            True,
            {"waterfall", "scatter"},
            {"scatter_waterfall_query"},
            {"top_urls_query", "top_asns_query", "ttl_dist_query"},
        ),
    ],
)
def test_performance_aggregates_section_filtering(
    in_memory_duckdb, test_service_source, sections, filtered, expected_present, expected_timers, blocked_timers
):
    logs = generate_mock_logs(test_service_source, num_logs=25)
    for log in logs:
        log["ttl"] = 60
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    flt = {"status": FilterSpec(mode="include", values=["200", "404", "500"])} if filtered else {}
    result = get_performance_aggregates(in_memory_duckdb, test_service_source, None, None, flt, sections=sections)
    assert (_ALL_PERF_SECTIONS & result.keys()) == expected_present
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert expected_timers.issubset(timer_names)
    assert not (blocked_timers & timer_names)
    if sections == {"top_urls", "top_asns"}:
        assert sum(1 for t in result.get("section_timings", []) if t["section"] == "temp_table_create") == 1


def test_performance_aggregates_temp_skipped_and_narrowed_with_rollups(in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ttl"] = 60
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    with (
        patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", side_effect=_perf_latency_hit),
        patch(
            "backend.repositories._base.QueryRunner.try_perf_ttl_dist_from_rollup",
            return_value=[{"bucket": "<1m", "count": 5}],
        ),
    ):
        result = get_performance_aggregates(
            in_memory_duckdb,
            test_service_source,
            "2026-01-01T00:00:00Z",
            "2026-01-08T00:00:00Z",
            {},
            sections={"top_urls", "top_asns", "ttl_dist"},
        )
        names = {t["section"] for t in result["section_timings"]}
        assert "perf:temp_skipped" in names and "temp_table_create" not in names
        assert result["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
        assert result["top_urls"][0]["url"] == "/rolled" and result.get("approx") is True
        for row in result["section_timings"]:
            assert row["current_execution"] is True
            if row["section"] in {"top_urls_query_rollup", "top_asns_query_rollup", "ttl_dist_rollup"}:
                assert row["source"] == "rollup" and row["activity"] == "compute"
        labels = [row for row in result["section_timings"] if row["section"] == "performance.asn_labels"]
        assert len(labels) == 1 and labels[0]["activity"] == "overhead"

        res_scatter = get_performance_aggregates(
            in_memory_duckdb,
            test_service_source,
            "2026-01-01T00:00:00Z",
            "2026-01-08T00:00:00Z",
            {"status": FilterSpec(mode="include", values=["200", "404", "500"])},
            sections=None,
        )
        names2 = {t["section"] for t in res_scatter["section_timings"]}
        assert "perf:temp_skipped" in names2 and "temp_table_create" not in names2
        assert "ttl_dist_rollup" in names2 and "ttl_dist_query" not in names2
        assert res_scatter["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
        assert "scatter" in res_scatter and "waterfall" in res_scatter


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _make_table(con, name: str, rows: list[tuple]) -> dict:
    table = _safe_table(name)
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, cmcd_sid VARCHAR, cmcd_bs BOOLEAN, "
        "cmcd_mtp INTEGER, cmcd_ot VARCHAR, country VARCHAR, asn INTEGER)"
    )
    con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    return {"name": name, "service_id": f"svc-{name}"}


def test_rebuffer_by_country_and_cmcd_unavailable(con, monkeypatch):
    rows = [
        (_T0, "s1", False, 100, "v", "US", 1),
        (_T0, "s1", True, 300, "v", "US", 1),
        (_T0, "s1", False, 500, "v", "US", 1),
        (_T0, "s1", False, 9999, "a", "US", 1),
        (_T0, "s2", False, 1000, "v", "US", 2),
        (_T0, "s3", False, 7777, "a", "GB", 3),
    ]
    src = _make_table(con, "cmcd_country", rows)
    seen: list[str] = []
    original = _base.QueryRunner.execute
    monkeypatch.setattr(
        _base.QueryRunner, "execute", lambda self, sql, *a, **kw: (seen.append(sql), original(self, sql, *a, **kw))[1]
    )

    result = get_cmcd_aggregates(
        con, src, "2026-10-21T00:00:00Z", "2026-10-22T00:00:00Z", {}, sections={"rebuffer_by_country"}
    )
    by_country = {r["country"]: r for r in result["rebuffer_by_country"]}
    assert by_country["US"]["session_count"] == 2 and by_country["US"]["rebuffer_rate"] == 50.0
    assert by_country["US"]["median_throughput"] == 650
    assert by_country["GB"]["session_count"] == 1 and by_country["GB"]["median_throughput"] is None
    live = [row for row in result["section_timings"] if row.get("source") == "live"]
    assert {row["section"] for row in live} == {"temp_table_create", "rebuffer_by_country"}
    assert all(row["activity"] == "compute" and row["current_execution"] for row in live)

    per_session = [s for s in seen if "per_session" in s and "country" in s]
    assert per_session and all("APPROX_QUANTILE" not in s.upper() for s in per_session)

    table = _safe_table("cmcd_none")
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, country VARCHAR)")
    assert (
        get_cmcd_aggregates(con, {"name": "cmcd_none", "service_id": "svc-none"}, None, None, {})["available"] is False
    )


def test_cmcd_aggregates_router_endpoints(client, in_memory_duckdb, test_service_source, monkeypatch):
    mock_ret = {"available": True, "has_data": True, "overview": {"active_sessions": 10, "peak_viewers": 4}}
    with patch("backend.repositories.cmcd.get_cmcd_aggregates", return_value=mock_ret) as mock_get:
        resp = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "start_time": "2026-08-19T12:00:00Z",
                "end_time": "2026-08-19T13:00:00Z",
                "sections": ["overview"],
            },
        )
        assert resp.status_code == 200 and resp.json()["overview"] == {"active_sessions": 10, "peak_viewers": 4}
        mock_get.assert_called_once()

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})
    with patch(
        "backend.repositories.cmcd.get_cmcd_aggregates",
        return_value={
            "available": True,
            "has_data": False,
            "section_timings": [{"section": "temp_table_create", "time_ms": 1.5}],
        },
    ) as mock_get2:
        resp2 = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "range_token": "last_24h",
                "anchor": "2026-08-19T12:00:00Z",
                "sections": ["bitrate_ts"],
            },
        )
        assert resp2.status_code == 200 and resp2.json()["available"] is True and resp2.json()["has_data"] is False
        mock_get2.assert_called_once()
        names = [row["section"] for row in resp2.json()["_section_timings"]]
        assert names == ["request_setup", "connection_acquire", "temp_table_create"]


# ── Merged from tests/repositories/test_query.py ──


class TestLogsTableAlias:
    """execute_query rewrites 'logs' to the real table name when the source name differs."""

    def _con_with_table(self, table_name: str) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE {table_name} (id INTEGER, val VARCHAR)")
        con.execute(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
        return con

    @pytest.mark.parametrize(
        ("table_name", "src", "sql", "expected_rows"),
        [
            ("logs_myservice", {"name": "myservice"}, "SELECT * FROM logs ORDER BY id", 2),
            ("logs", {"name": "default"}, "SELECT * FROM logs ORDER BY id", 2),
            ("logs", None, "SELECT * FROM logs", 2),
            ("logs_svc", {"name": "svc"}, "select * from LOGS order by id", 2),
            ("logs_svc2", {"name": "svc2"}, "WITH logs AS (SELECT id FROM logs WHERE id = 1) SELECT * FROM logs", 1),
        ],
    )
    def test_rewrites_logs_variants(self, table_name, src, sql, expected_rows):
        con = self._con_with_table(table_name)
        result = execute_query(con, src, sql, max_rows=100, want_explain=False)
        assert result["row_count"] == expected_rows

    def test_logs_word_boundary_not_matched_in_longer_names(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc3 (id INTEGER); CREATE TABLE my_logs (id INTEGER)")
        con.execute("INSERT INTO logs_svc3 VALUES (1); INSERT INTO my_logs VALUES (99)")
        with pytest.raises(PermissionError, match="not in the allowed set"):
            execute_query(con, {"name": "svc3"}, "SELECT * FROM my_logs", max_rows=100, want_explain=False)


class TestGetPresets:
    def test_presets_use_actual_table_name_and_sample_rows_has_no_order_by(self, in_memory_duckdb, test_service_source):
        in_memory_duckdb.execute("CREATE TABLE logs_test_service (timestamp TIMESTAMPTZ, val INTEGER)")
        presets = get_presets(test_service_source, in_memory_duckdb)
        assert presets
        for p in presets:
            assert "logs_test_service" in p["sql"]
            assert "FROM logs " not in p["sql"] and not p["sql"].endswith("FROM logs")
        sample = next(p for p in presets if p["name"] == "Sample rows")
        assert "ORDER BY" not in sample["sql"].upper()


class TestAutoLimitPushdown:
    def _con(self, rows: int = 50) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER, val VARCHAR)")
        con.executemany("INSERT INTO logs VALUES (?, ?)", [(i, f"v{i}") for i in range(rows)])
        return con

    @pytest.mark.parametrize(
        ("rows", "sql", "max_rows", "expected_count", "expected_trunc", "expected_total"),
        [
            (50, "SELECT * FROM logs ORDER BY id", 10, 10, True, -1),
            (50, "SELECT * FROM logs ORDER BY id LIMIT 5", 100, 5, False, 5),
            (5, "SELECT * FROM logs ORDER BY id", 100, 5, False, 5),
            (42, "SELECT COUNT(*) AS n FROM logs", 100, 1, False, 1),
            (50, "/* This is a comment */ SELECT * FROM logs", 10, 10, True, -1),
        ],
    )
    def test_limit_pushdown_cases(self, rows, sql, max_rows, expected_count, expected_trunc, expected_total):
        con = self._con(rows=rows)
        result = execute_query(con, None, sql, max_rows=max_rows, want_explain=False)
        assert result["row_count"] == expected_count
        assert result["truncated"] is expected_trunc
        assert result["total_rows"] == expected_total

    def test_summarize_is_rejected_at_validator(self):
        con = self._con(rows=10)
        with pytest.raises(PermissionError, match="SHOW / DESCRIBE / SUMMARIZE"):
            execute_query(con, None, "SUMMARIZE logs", max_rows=100, want_explain=False)


class TestAnalystTimeWindow:
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, ?)",
            [
                ("2026-06-17T10:00:00+00:00", "1.2.3.4", "/a"),
                ("2026-06-17T10:30:00+00:00", "5.6.7.8", "/b"),
                ("2026-06-01T00:00:00+00:00", "9.9.9.9", "/old"),
            ],
        )
        return con

    def test_admin_and_analyst_window_filtering(self):
        con = self._con()
        assert (
            execute_query(con, self.SRC, "SELECT * FROM logs ORDER BY timestamp", max_rows=100, want_explain=False)[
                "row_count"
            ]
            == 3
        )

        res = execute_query(
            con,
            self.SRC,
            "SELECT * FROM logs ORDER BY timestamp",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert res["row_count"] == 2 and all("9.9.9.9" not in str(row) for row in res["data"])

        for sql in (
            "SELECT count(*) AS n FROM logs",
            "WITH logs AS (SELECT * FROM logs WHERE url LIKE '/%') SELECT count(*) AS n FROM logs",
        ):
            r = execute_query(con, self.SRC, sql, max_rows=100, want_explain=False, time_filter=self.WINDOW)
            assert r["data"][0]["n"] == 2

        widen = "SELECT * FROM logs WHERE timestamp >= TIMESTAMPTZ '1970-01-01T00:00:00+00:00'"
        assert (
            execute_query(con, self.SRC, widen, max_rows=100, want_explain=False, time_filter=self.WINDOW)["row_count"]
            == 2
        )

    def test_default_service_table_and_view_cleanup(self):
        con_def = duckdb.connect(":memory:")
        con_def.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR)")
        con_def.executemany(
            "INSERT INTO logs VALUES (?, ?)",
            [("2026-06-17T10:00:00+00:00", "1.1.1.1"), ("2026-06-01T00:00:00+00:00", "2.2.2.2")],
        )
        res = execute_query(
            con_def,
            {"name": "default"},
            "SELECT count(*) AS n FROM logs",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
        )
        assert res["data"][0]["n"] == 1

        con = self._con()
        execute_query(con, self.SRC, "SELECT * FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW)
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()

        with pytest.raises(Exception):  # noqa: B017
            execute_query(
                con, self.SRC, "SELECT no_such_col FROM logs", max_rows=100, want_explain=False, time_filter=self.WINDOW
            )
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM {_ANALYST_WINDOW_VIEW}").fetchall()


class TestValueShapeMasking:
    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.execute("INSERT INTO logs VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', '/path')")
        return con

    def test_mask_ips_survives_column_aliasing_and_false_returns_raw(self):
        con = self._con()
        res_masked = execute_query(
            con, None, "SELECT ip AS addr, url FROM logs", max_rows=100, want_explain=False, mask_ips=True
        )
        assert res_masked["data"][0]["addr"] == "1.2.3.xxx" and res_masked["data"][0]["url"] == "/path"
        res_raw = execute_query(
            con, None, "SELECT ip AS addr FROM logs", max_rows=100, want_explain=False, mask_ips=False
        )
        assert res_raw["data"][0]["addr"] == "1.2.3.4"


class TestSessionIdRedaction:
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")
    HASH = "a1b2c3d4deadbeefcafef00dfeedface"

    def _con(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, cookie_session VARCHAR, url VARCHAR)")
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, ?, ?)",
            [
                ("2026-06-17T10:00:00+00:00", "1.2.3.4", self.HASH, "/a"),
                ("2026-06-17T10:30:00+00:00", "5.6.7.8", "", "/b"),
            ],
        )
        return con

    @pytest.mark.security_regression
    def test_mask_ips_redacts_aliased_cookie_session(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT cookie_session AS s, ip AS addr, url FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["s"] for r in result["data"]] == ["[redacted]", ""]
        assert [r["addr"] for r in result["data"]] == ["[redacted]", "[redacted]"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_redacts_cookie_session_via_string_concat(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT 'x' || cookie_session AS c FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["c"] for r in result["data"]] == ["x[redacted]", "x"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_redacts_ip_via_string_concat_and_group_by(self):
        con = self._con()
        concat = execute_query(
            con,
            self.SRC,
            "SELECT 'x' || ip AS a FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["a"] for r in concat["data"]] == ["x[redacted]", "x[redacted]"]
        grouped = execute_query(
            con,
            self.SRC,
            "SELECT 'x' || ip AS a, count(*) AS c FROM logs GROUP BY ip",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert {r["a"] for r in grouped["data"]} == {"x[redacted]"}
        for raw in ("1.2.3.4", "5.6.7.8"):
            assert raw not in str(concat["data"]) and raw not in str(grouped["data"])

    @pytest.mark.security_regression
    def test_mask_ips_false_returns_raw_ip_on_query(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT ip AS a FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=False,
        )
        assert result["data"][0]["a"] == "1.2.3.4"

    @pytest.mark.security_regression
    def test_mask_ips_redacts_cookie_session_via_select_star(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT * FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert [r["cookie_session"] for r in result["data"]] == ["[redacted]", ""]
        assert [r["url"] for r in result["data"]] == ["/a", "/b"]
        assert self.HASH not in str(result["data"])

    @pytest.mark.security_regression
    def test_mask_ips_false_returns_raw_cookie_session(self):
        result = execute_query(
            self._con(),
            self.SRC,
            "SELECT cookie_session AS s FROM logs ORDER BY url",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=False,
        )
        assert result["data"][0]["s"] == self.HASH

    def test_missing_cookie_session_column_does_not_break_query(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, url VARCHAR)")
        con.execute("INSERT INTO logs_svc VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', '/a')")
        result = execute_query(
            con,
            self.SRC,
            "SELECT * FROM logs",
            max_rows=100,
            want_explain=False,
            time_filter=self.WINDOW,
            mask_ips=True,
        )
        assert result["data"][0]["ip"] == "[redacted]" and result["data"][0]["url"] == "/a"


class TestMaxRowsClamp:
    def test_max_rows_clamped_to_ceiling(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE logs (id INTEGER)")
        con.executemany("INSERT INTO logs VALUES (?)", [(i,) for i in range(MAX_QUERY_ROWS + 50)])
        result = execute_query(con, None, "SELECT * FROM logs ORDER BY id", max_rows=10**9, want_explain=False)
        assert result["row_count"] == MAX_QUERY_ROWS and result["truncated"] is True


@pytest.mark.security_regression
class TestCidSaltNeverRecorded:
    SALT = "ZZSALTMARKER" + "ab" * 10
    SRC = {"name": "svc"}
    WINDOW = ("2026-06-17T09:45:00+00:00", "2026-06-17T11:00:00+00:00")

    def _con(self):
        raw = duckdb.connect(":memory:")
        raw.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, ip VARCHAR, cid VARCHAR, url VARCHAR)")
        raw.execute("INSERT INTO logs_svc VALUES (TIMESTAMPTZ '2026-06-17T10:00:00+00:00', '1.2.3.4', 'abc', '/p')")
        return InstrumentedDuckDBConnection(raw, service_id="svc")

    def _run(self, con, sql, explain=False):
        with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=self.SALT):
            return execute_query(
                con,
                self.SRC,
                sql,
                max_rows=10,
                want_explain=explain,
                service_id="svc",
                time_filter=self.WINDOW,
                mask_ips=True,
            )

    def test_hash_is_unchanged(self):
        result = self._run(self._con(), "SELECT cid FROM logs")
        assert result["data"][0]["cid"] == hashlib.sha256((self.SALT + "abc").encode()).hexdigest()

    @pytest.mark.parametrize(
        "sql,explain",
        [
            ("SELECT cid FROM logs", True),
            ("SELECT cid::INT FROM logs", False),
            ("SELECT nope FROM logs", False),
            ("SELECT sql FROM duckdb_views()", False),
            ("SELECT * FROM information_schema.views", False),
            ("SELECT * FROM duckdb_temporary_files()", False),
        ],
    )
    def test_no_analyst_visible_channel_carries_the_salt(self, sql, explain):
        try:
            blob = repr(self._run(self._con(), sql, explain))
        except Exception as exc:
            blob = f"{type(exc).__name__}: {exc}"
        assert self.SALT not in blob

    def test_query_registry_does_not_record_the_salt(self):
        con = self._con()
        self._run(con, "SELECT cid FROM logs")
        gc.collect()
        snap = query_registry.snapshot(include_completed=True, full_sql=True, all_active=True)
        blob = json.dumps(snap, default=str)
        assert "_analyst_window_logs" in blob and self.SALT not in blob


# ── Merged from tests/repositories/test_query_cid_salt_not_recorded.py ──
SALT = "SECRETSALT-9f3a1c7e5b"
SVC = "svc"
WINDOW = ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z")
SRC = {"name": SVC, "service_id": SVC}


@pytest.fixture
def raw_con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    con.execute(
        "INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200), (now(), 'client-b', '5.6.7.8', 500),"
        " (now(), '', '9.9.9.9', 200), (now(), NULL, '8.8.8.8', 200)"
    )
    yield con
    con.close()


def _analyst_query(con, sql: str, *, want_explain: bool = False) -> dict:
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        return q.execute_query(con, SRC, sql, 100, want_explain, service_id=SVC, mask_ips=True, time_filter=WINDOW)


def _expected_hash(cid: str) -> str:
    return hashlib.sha256((SALT + cid).encode()).hexdigest()


def test_cid_salt_hash_parity_and_redaction_channels(raw_con, tmp_path):
    res = _analyst_query(raw_con, "SELECT cid FROM logs")
    assert sorted((r["cid"] or "") for r in res["data"]) == sorted(
        ["", "", _expected_hash("client-a"), _expected_hash("client-b")]
    )
    assert sum(r["cid"] is None for r in res["data"]) == 1

    legacy = raw_con.execute(f"SELECT sha256(CONCAT('{SALT}', cid)) FROM logs_svc WHERE status = 500").fetchone()[0]
    assert _analyst_query(raw_con, "SELECT cid FROM logs WHERE status = 500")["data"] == [{"cid": legacy}]
    assert {r["ip"] for r in _analyst_query(raw_con, "SELECT ip FROM logs ORDER BY ip")["data"]} == {"[redacted]"}

    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    _analyst_query(con, "SELECT cid, count(*) AS n FROM logs GROUP BY cid")
    texts = json.dumps(
        query_registry.snapshot(since_seq=0, full_sql=True, include_completed=True, all_active=True), default=str
    )
    assert "_analyst_window_logs" in texts and SALT not in texts

    persisted: list[tuple] = []
    with (
        patch("backend.core.request_telemetry._SLOW_QUERY_PERSIST_THRESHOLD_MS", 0.0),
        patch("backend.core.request_telemetry._SLOW_QUERY_PERSIST_DISABLED", False),
        patch("backend.core.metadata.insert_slow_query", side_effect=lambda sid, row: persisted.append((sid, row))),
    ):
        _analyst_query(con, "SELECT cid FROM logs")
    assert persisted and SALT not in json.dumps(persisted, default=str)
    assert SALT not in json.dumps(_analyst_query(con, "SELECT cid FROM logs", want_explain=True), default=str)

    raw_con.execute("CREATE TABLE logs_nots (cid VARCHAR, ip VARCHAR)")
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(duckdb.Error) as exc:
            q.execute_query(
                con,
                {"name": "nots", "service_id": SVC},
                "SELECT cid FROM logs",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
    assert SALT not in str(exc.value)

    leftovers = raw_con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary AND table_name != 'logs_nots' UNION ALL "
        "SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal"
    ).fetchall()
    assert leftovers == []

    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(PermissionError):
            q.execute_query(
                raw_con,
                SRC,
                f"SELECT * FROM {q._ANALYST_CID_SALT_TABLE}",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
        with pytest.raises(ValueError, match="invalid analyst window bounds"):
            q.execute_query(
                raw_con,
                SRC,
                "SELECT cid FROM logs",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=("not-a-date", "also-not"),
            )
    assert raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE temporary").fetchone()[0] == 0

    path = str(tmp_path / "ro.duckdb")
    seed = duckdb.connect(path)
    seed.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    seed.execute("INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200)")
    seed.close()
    ro = duckdb.connect(path, read_only=True)
    try:
        assert _analyst_query(ro, "SELECT cid FROM logs")["data"] == [{"cid": _expected_hash("client-a")}]
    finally:
        ro.close()

    class _FailViewDrop:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql.startswith("DROP VIEW"):
                raise RuntimeError("interrupted")
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    _analyst_query(_FailViewDrop(raw_con), "SELECT cid FROM logs")
    assert (
        raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = '_analyst_cid_salt'").fetchone()[0]
        == 0
    )
