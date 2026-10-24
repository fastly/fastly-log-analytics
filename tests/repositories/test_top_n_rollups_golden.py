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

from backend.core import rollup_readiness as rr
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

from backend.core.rollups._common import IP_SPREAD_BUNDLE_FILENAME  # noqa: E402
from backend.repositories.ip_spread_rollups import (  # noqa: E402
    IpSpreadPlan,
    build_ip_spread_sql,
    fold_sketches,
    plan_ip_spread_paths,
    spread_meta,
)
from backend.repositories.top_n_rollups import TopNWindow  # noqa: E402
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
