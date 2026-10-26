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

from backend.core import rollups as rr
from backend.repositories import _base, rollup_coverage
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

from backend.utils.hll import HyperLogLog  # noqa: E402

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME  # noqa: E402
from backend.repositories.top_n_rollups import (  # noqa: E402
    IpSpreadPlan,
    TopNWindow,
    build_ip_spread_sql,
    fold_sketches,
    plan_ip_spread_paths,
    spread_meta,
)

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

        from backend.core.rollups import partial_hour as ph

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
