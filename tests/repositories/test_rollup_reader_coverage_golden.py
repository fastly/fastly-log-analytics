"""Golden characterization of every rollup reader's coverage decisions.

For each ``try_*_from_rollup`` reader (plus the shared path-collection
primitives they sit on) this runs a matrix of on-disk layouts and window
shapes and records, against a recording ``execute`` stub:

* whether the reader fell back (returned ``None``) or went on to read,
* the exact ordered parquet paths handed to ``read_parquet``,
* the shape of the SQL it would run (paths/timestamps normalised),
* the value it returned for an empty result set,
* an upper bound on the filesystem probes it spent deciding.

The snapshot in ``golden/rollup_reader_coverage.json`` was captured from the
pre-refactor readers. Any refactor of the coverage walk must reproduce it
exactly (probe counts may only go down). Regenerate deliberately with
``UPDATE_ROLLUP_GOLDEN=1`` and review the diff.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pytest

from backend.core import rollups as rr
from backend.repositories import rollup_coverage
from backend.repositories._base import QueryRunner
from backend.utils.date_utils import parse_iso_utc
from tests._helpers.rollups import ALL_FIELDS, build_rollup_golden_layout
from tests._helpers.rollups import RollupGoldenLayout as Layout
from tests._helpers.sources import make_source

GOLDEN_PATH = Path(__file__).parent / "golden" / "rollup_reader_coverage.json"
_STAT_FNS = ("isfile", "isdir", "exists")
_COLS = ["timestamp", "country", "city", "lat", "lon", "metro", "conn_requests", "waf_req_id", "ottfb", "ttfb", "ottlb"]

TS = "time_series.parquet"


# ── scenarios ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Scenario:
    name: str
    st_off_h: float | None
    et_off_h: float | None
    layout: Callable[[Layout], None]
    anchor: str = "A"  # "A" = today 00:00 UTC, "H" = current hour start
    has_filters: bool = False
    durable: str | None = None  # None | "not_ready" | "ready"
    st_raw: str | None = None
    et_raw: str | None = None
    needs_filters_param: bool = False


def _full(lo: int, hi: int) -> Callable[[Layout], None]:
    def build(lay: Layout) -> None:
        lay.mkroot()
        lay.hour_files(range(lo, hi))

    return build


def _missing(lo: int, hi: int, gap: int, *, marker: bool = False, sentinel: bool = False, nonempty: bool = False):
    def build(lay: Layout) -> None:
        lay.mkroot()
        lay.hour_files([k for k in range(lo, hi) if k != gap])
        if marker:
            lay.marker(gap)
        if sentinel:
            lay.sentinel(gap)
        if nonempty:
            lay.nonempty_allfields(gap)

    return build


def _coverage(lo: int, hi: int, missing_mods: set[int]) -> Callable[[Layout], None]:
    def build(lay: Layout) -> None:
        lay.mkroot()
        lay.hour_files([k for k in range(lo, hi) if (k - lo) % 5 not in missing_mods])

    return build


def _days(lay: Layout) -> None:
    lay.mkroot()
    lay.day_files([-3, -2, -1])


def _days_and_hours(lay: Layout) -> None:
    lay.mkroot()
    lay.day_files([-3, -2, -1])
    lay.hour_files(range(-72, 0))


def _days_residual(lay: Layout) -> None:
    lay.mkroot()
    lay.day_files([-3, -2])
    lay.hour_files(range(-24, 0))


def _partial_start_day(lay: Layout) -> None:
    lay.mkroot()
    lay.day_files([-3, -2, -1])
    lay.hour_files(range(-66, -48))


def _days_alt(variant: str) -> Callable[[Layout], None]:
    def build(lay: Layout) -> None:
        lay.mkroot()
        lay.day_files([-3, -2, -1], variant)

    return build


def _no_files(lay: Layout) -> None:
    lay.mkroot()


def _nothing(lay: Layout) -> None:
    lay.root.mkdir(parents=True, exist_ok=True)


def _hour_files_only_allfields(lay: Layout) -> None:
    lay.mkroot()
    lay.hour_files(range(-72, 0), (ALL_FIELDS,))


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("hours_full_72h", -72, 0, _full(-72, 0)),
    Scenario("days_preferred", -72, 0, _days),
    Scenario("days_and_hours", -72, 0, _days_and_hours),
    Scenario("days_residual_hours", -72, 0, _days_residual),
    Scenario("partial_start_day", -66, 0, _partial_start_day),
    Scenario("days_alt_nested", -72, 0, _days_alt("nested_alt")),
    Scenario("days_alt_flat", -72, 0, _days_alt("flat_alt")),
    Scenario("partial_start_mid_hour", -71.5, 0, _full(-72, 0)),
    Scenario("missing_hour_marker", -72, 0, _missing(-72, 0, -10, marker=True)),
    Scenario("missing_hour_no_marker", -72, 0, _missing(-72, 0, -10)),
    Scenario("missing_hour_empty_sentinel", -72, 0, _missing(-72, 0, -10, marker=True, sentinel=True)),
    Scenario("missing_hour_nonempty_allfields", -72, 0, _missing(-72, 0, -10, marker=True, nonempty=True)),
    Scenario("missing_hour_beyond_heal_cap", -120, 0, _missing(-120, 0, -100, marker=True)),
    Scenario("missing_hour_recent_marker", -72, 0, _missing(-72, 0, -3, marker=True)),
    Scenario("missing_hour_recent_no_marker", -72, 0, _missing(-72, 0, -3)),
    Scenario("missing_hour_recent_sentinel", -72, 0, _missing(-72, 0, -3, sentinel=True)),
    Scenario("coverage_60pct", -72, 0, _coverage(-72, 0, {0, 1})),
    Scenario("coverage_40pct", -72, 0, _coverage(-72, 0, {0, 1, 2})),
    Scenario("only_allfields_hours", -72, 0, _hour_files_only_allfields),
    Scenario("no_files", -72, 0, _no_files),
    Scenario("no_root", -72, 0, _nothing),
    Scenario("win_1h", -1, 0, _full(-100, 0)),
    Scenario("win_3h", -3, 0, _full(-100, 0)),
    Scenario("win_30h", -30, 0, _full(-100, 0)),
    Scenario("win_50h", -50, 0, _full(-100, 0)),
    Scenario("win_400d", -400 * 24, 0, _full(-100, 0)),
    Scenario("win_366d", -366 * 24, 0, _full(-100, 0)),
    Scenario("reversed_window", 0, -72, _full(-100, 0)),
    Scenario("missing_start", None, 0, _full(-100, 0)),
    Scenario("bad_iso_start", None, 0, _full(-100, 0), st_raw="not-a-date"),
    Scenario("filters_set", -72, 0, _full(-72, 0), has_filters=True, needs_filters_param=True),
    Scenario("durable_not_ready", -72, 0, _full(-72, 0), durable="not_ready"),
    Scenario("durable_ready", -72, 0, _full(-72, 0), durable="ready"),
    Scenario("active_cross_full", -72, 0.5, _full(-72, 0), anchor="H"),
    Scenario("active_only_3h", -3, 0.5, _full(-3, 0), anchor="H"),
    Scenario("active_missing_hour", -72, 0.5, _missing(-72, 0, -10, marker=True), anchor="H"),
    Scenario("active_missing_no_marker", -72, 0.5, _missing(-72, 0, -10), anchor="H"),
)


# ── readers ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class W:
    st: str | None
    et: str | None
    has_filters: bool
    st_dt: datetime | None
    et_dt: datetime | None


@dataclass(frozen=True)
class Reader:
    name: str
    fns: tuple[str, ...]
    call: Callable[[QueryRunner, W, Layout], Any]
    takes_filters: bool = True
    recording: bool = True


def _r(name: str, fns: tuple[str, ...] | str, call: Callable[..., Any], **kw: Any) -> Reader:
    return Reader(name, (fns,) if isinstance(fns, str) else fns, call, **kw)


def _collect_hourly(fn: str, allow_all: bool) -> Callable[[QueryRunner, W, Layout], Any]:
    def call(runner: QueryRunner, w: W, lay: Layout) -> Any:
        if w.st_dt is None or w.et_dt is None:
            return "invalid_window"
        res = rollup_coverage.collect_hourly_bundle_paths(
            runner.src, w.st_dt, w.et_dt, str(lay.hour_root), fn, allow_all_fields_coverage=allow_all
        )
        return None if res is None else [list(res[0]), res[1]]

    return call


def _day_prefer(fn: str, skip_partial_start: bool) -> Callable[[QueryRunner, W, Layout], Any]:
    def call(runner: QueryRunner, w: W, lay: Layout) -> Any:
        if w.st_dt is None or w.et_dt is None:
            return "invalid_window"
        return runner._collect_rollup_paths(w.st_dt, w.et_dt, fn, skip_partial_start=skip_partial_start)

    return call


def _sessions_paths(runner: QueryRunner, w: W, lay: Layout) -> Any:
    from backend.repositories.sessions import _collect_sessions_rollup_paths

    if w.st_dt is None or w.et_dt is None:
        return "invalid_window"
    res = _collect_sessions_rollup_paths(runner.src, w.st_dt, w.et_dt)
    return None if res is None else [list(res[0]), res[1]]


READERS: tuple[Reader, ...] = (
    _r(
        "time_series[requests,direct]",
        TS,
        lambda r, w, lay: r.try_time_series_from_rollup(
            "requests", "1 hour", w.st, w.et, "tbl", "1=1", [], unfiltered_window=True
        ),
        takes_filters=False,
    ),
    _r(
        "time_series[5xx,direct]",
        TS,
        lambda r, w, lay: r.try_time_series_from_rollup(
            "5xx", "1 day", w.st, w.et, "tbl", "1=1", [], unfiltered_window=True
        ),
        takes_filters=False,
    ),
    _r(
        "time_series[requests,view]",
        TS,
        lambda r, w, lay: r.try_time_series_from_rollup("requests", "1 hour", w.st, w.et, "tbl", "1=1", ["p"]),
        takes_filters=False,
    ),
    _r(
        "time_series[bad_metric]",
        TS,
        lambda r, w, lay: r.try_time_series_from_rollup("p95_latency", "1 hour", w.st, w.et, "tbl", "1=1", []),
        takes_filters=False,
    ),
    _r(
        "count[unfiltered]",
        TS,
        lambda r, w, lay: r.try_count_from_rollup(
            w.st, w.et, "tbl", "1=1", [], unfiltered_window=True, projected_fields=[], actual_cols=_COLS
        ),
        takes_filters=False,
    ),
    _r(
        "count[filtered]",
        TS,
        lambda r, w, lay: r.try_count_from_rollup(w.st, w.et, "tbl", "1=1", []),
        takes_filters=False,
    ),
    _r(
        "slow_urls",
        "slow_urls.parquet",
        lambda r, w, lay: r.try_slow_urls_from_rollup(w.st, w.et, has_filters=w.has_filters, min_requests=5, limit=10),
    ),
    _r(
        "origin_summary",
        "origin_summary.parquet",
        lambda r, w, lay: r.try_origin_summary_from_rollup(
            w.st, w.et, has_filters=w.has_filters, actual_cols=set(_COLS)
        ),
    ),
    _r(
        "origin_pop_latency",
        "origin_pop.parquet",
        lambda r, w, lay: r.try_origin_pop_latency_from_rollup(w.st, w.et, has_filters=w.has_filters, limit=10),
    ),
    _r(
        "origin_ip_health",
        "origin_ip.parquet",
        lambda r, w, lay: r.try_origin_ip_health_from_rollup(w.st, w.et, has_filters=w.has_filters, limit=10),
    ),
    _r(
        "origin_path_breakdown",
        "origin_path.parquet",
        lambda r, w, lay: r.try_origin_path_breakdown_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "origin_status",
        ALL_FIELDS,
        lambda r, w, lay: r.try_origin_status_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "origin_latency_ts[ttfb,p95]",
        "origin_latency_ts.parquet",
        lambda r, w, lay: r.try_origin_latency_ts_from_rollup(
            w.st,
            w.et,
            has_filters=w.has_filters,
            bucket_minutes=5,
            metric="ttfb",
            percentile="p95",
            split_by_leg=False,
            table_name="tbl",
            where_clause="1=1",
            params=[],
        ),
    ),
    _r(
        "origin_latency_ts[split_by_leg]",
        "origin_latency_ts.parquet",
        lambda r, w, lay: r.try_origin_latency_ts_from_rollup(
            w.st,
            w.et,
            has_filters=w.has_filters,
            bucket_minutes=5,
            metric="ttfb",
            percentile="p95",
            split_by_leg=True,
            table_name="tbl",
            where_clause="1=1",
            params=[],
        ),
    ),
    _r(
        "origin_latency_ts[sub_minute]",
        "origin_latency_ts.parquet",
        lambda r, w, lay: r.try_origin_latency_ts_from_rollup(
            w.st,
            w.et,
            has_filters=w.has_filters,
            bucket_minutes=0.5,
            metric="ttlb",
            percentile="p50",
            split_by_leg=False,
            table_name="tbl",
            where_clause="1=1",
            params=[],
        ),
    ),
    _r(
        "network_quality",
        (
            "network_quality_country.parquet",
            "network_quality_asn.parquet",
            "network_quality_pop.parquet",
            "network_quality_region.parquet",
        ),
        lambda r, w, lay: r.try_network_quality_from_rollup(w.st, w.et, has_filters=w.has_filters, region_country="US"),
    ),
    _r(
        "network_rtt",
        "network_rtt.parquet",
        lambda r, w, lay: r.try_network_rtt_from_rollup(w.st, w.et, top_asns=[1, 2], has_filters=w.has_filters),
    ),
    _r(
        "network_rtt[no_asns]",
        "network_rtt.parquet",
        lambda r, w, lay: r.try_network_rtt_from_rollup(w.st, w.et, top_asns=[], has_filters=w.has_filters),
    ),
    _r(
        "network_speed",
        "network_speed.parquet",
        lambda r, w, lay: r.try_network_speed_from_rollup(w.st, w.et, top_asns=[1, 2], has_filters=w.has_filters),
    ),
    _r(
        "network_heatmap",
        "network_heatmap.parquet",
        lambda r, w, lay: r.try_network_heatmap_from_rollup(w.st, w.et, has_filters=w.has_filters, bucket_seconds=3600),
    ),
    _r(
        "network_heatmap[coarse_bucket]",
        "network_heatmap.parquet",
        lambda r, w, lay: r.try_network_heatmap_from_rollup(w.st, w.et, has_filters=w.has_filters, bucket_seconds=7200),
    ),
    _r(
        "network_geo",
        "network_geo.parquet",
        lambda r, w, lay: r.try_network_geo_from_rollup(w.st, w.et, has_filters=w.has_filters, bucket_seconds=3600),
    ),
    _r(
        "network_geo[map_asn]",
        "network_geo.parquet",
        lambda r, w, lay: r.try_network_geo_from_rollup(
            w.st, w.et, has_filters=w.has_filters, map_asn="123", bucket_seconds=3600
        ),
    ),
    _r(
        "verified_bots_ts",
        "verified_bots_ts.parquet",
        lambda r, w, lay: r.try_verified_bots_ts_from_rollup(
            w.st, w.et, temp_table="tmp", bucket_seconds=300, has_filters=w.has_filters
        ),
    ),
    _r(
        "verified_bots_ts[odd_bucket]",
        "verified_bots_ts.parquet",
        lambda r, w, lay: r.try_verified_bots_ts_from_rollup(
            w.st, w.et, temp_table="tmp", bucket_seconds=90, has_filters=w.has_filters
        ),
    ),
    _r(
        "perf_latency[url]",
        "perf_top_urls.parquet",
        lambda r, w, lay: r.try_perf_latency_from_rollup(
            w.st, w.et, dimension="url", sort_by="p95", has_filters=w.has_filters, min_requests=5, limit=10
        ),
    ),
    _r(
        "perf_latency[asn]",
        "perf_top_asns.parquet",
        lambda r, w, lay: r.try_perf_latency_from_rollup(
            w.st, w.et, dimension="asn", sort_by="avg", has_filters=w.has_filters, min_requests=5, limit=10
        ),
    ),
    _r(
        "security_req_size",
        "security_req_size.parquet",
        lambda r, w, lay: r.try_security_req_size_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "security_conn_reuse",
        "security_conn_reuse.parquet",
        lambda r, w, lay: r.try_security_conn_reuse_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "conn_requests_hist[reuse_bundle]",
        "security_conn_reuse.parquet",
        lambda r, w, lay: r.try_conn_requests_hist_from_rollup(
            w.st, w.et, has_filters=w.has_filters, actual_cols=_COLS
        ),
    ),
    _r(
        "conn_requests_hist[allfields_fallback]",
        ALL_FIELDS,
        lambda r, w, lay: r.try_conn_requests_hist_from_rollup(
            w.st, w.et, has_filters=w.has_filters, actual_cols=_COLS
        ),
    ),
    _r(
        "virtual_field_top_n",
        ALL_FIELDS,
        lambda r, w, lay: r.try_virtual_field_top_n_from_rollup(
            "waf_sig_ind", "waf_sig", w.st, w.et, has_filters=w.has_filters, actual_cols=_COLS
        ),
    ),
    _r(
        "ngwaf_top_bots",
        "ngwaf_bots.parquet",
        lambda r, w, lay: r.try_ngwaf_top_bots_from_rollup(w.st, w.et, has_filters=w.has_filters, n=10),
    ),
    _r(
        "security_top_ips",
        "security_topips.parquet",
        lambda r, w, lay: r.try_security_top_ips_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "security_coverage",
        "security_cov.parquet",
        lambda r, w, lay: r.try_security_coverage_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "perf_ttl_dist",
        "perf_ttl_dist.parquet",
        lambda r, w, lay: r.try_perf_ttl_dist_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r(
        "pop_health",
        "pop_health.parquet",
        lambda r, w, lay: r.try_pop_health_from_rollup(w.st, w.et, has_filters=w.has_filters),
    ),
    _r("prim:collect_hourly[time_series]", TS, _collect_hourly(TS, False), takes_filters=False, recording=False),
    _r(
        "prim:collect_hourly[time_series,allfields]",
        TS,
        _collect_hourly(TS, True),
        takes_filters=False,
        recording=False,
    ),
    _r(
        "prim:collect_hourly[sessions]",
        "sessions.parquet",
        _collect_hourly("sessions.parquet", False),
        takes_filters=False,
        recording=False,
    ),
    _r("prim:sessions_paths", "sessions.parquet", _sessions_paths, takes_filters=False, recording=False),
    _r(
        "prim:day_prefer[hour_file]",
        "origin_pop.parquet",
        _day_prefer("origin_pop.parquet", True),
        takes_filters=False,
        recording=False,
    ),
    _r("prim:day_prefer[all_fields]", ALL_FIELDS, _day_prefer(ALL_FIELDS, True), takes_filters=False, recording=False),
    _r(
        "prim:day_prefer[no_skip_partial]",
        "verified_bots_ts.parquet",
        _day_prefer("verified_bots_ts.parquet", False),
        takes_filters=False,
        recording=False,
    ),
)


# ── harness ─────────────────────────────────────────────────────────────────


class _Cur:
    def fetchall(self) -> list:
        return []

    def fetchone(self) -> tuple:
        return (0, 0)


class _Probe:
    def __init__(self, prefix: str = "") -> None:
        import threading

        self._threading = threading
        self.thread_id = threading.get_ident()
        self.on = False
        self.prefix = prefix
        self.counts: dict[str, int] = {}

    def wrap(self, name: str, fn: Callable[..., Any]) -> Callable[..., Any]:
        def inner(*a: Any, **k: Any) -> Any:
            if self.on and a and self._threading.get_ident() == self.thread_id:
                try:
                    p = os.fspath(a[0])
                    if isinstance(p, bytes):
                        p = p.decode("utf-8", errors="replace")
                    if not self.prefix or p.startswith(self.prefix):
                        self.counts[name] = self.counts.get(name, 0) + 1
                except TypeError:
                    pass
            return fn(*a, **k)

        return inner


_HOUR_RE = re.compile(r"\d{4}-\d{2}-\d{2}-\d{2}")
_DAY_RE = re.compile(r"(?<![\d-])\d{4}-\d{2}-\d{2}(?![\d-])")
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)?")
_PATHS_RE = re.compile(r"read_parquet\(\[([^\]]*)\]")


class _Norm:
    def __init__(self, anchor: datetime, root: Path, active: datetime) -> None:
        self.anchor = anchor
        self.active = active
        self.root = str(root)

    def hour(self, m: re.Match) -> str:
        dt = datetime.strptime(m.group(0), "%Y-%m-%d-%H").replace(tzinfo=UTC)
        if dt == self.active:
            return "hACTIVE"
        return f"h{int((dt - self.anchor).total_seconds() // 3600):+d}"

    def day(self, m: re.Match) -> str:
        dt = datetime.strptime(m.group(0), "%Y-%m-%d").replace(tzinfo=UTC)
        return f"d{(dt.date() - self.anchor.date()).days:+d}"

    def ts(self, m: re.Match) -> str:
        dt = parse_iso_utc(m.group(0).replace(" ", "T"))
        assert dt is not None
        if dt == self.active:
            return "tACTIVE"
        return f"t{int((dt - self.anchor).total_seconds() // 60):+d}m"

    def text(self, s: str) -> str:
        s = s.replace(self.root, "<R>")
        s = _TS_RE.sub(self.ts, s)
        s = _HOUR_RE.sub(self.hour, s)
        return _DAY_RE.sub(self.day, s)

    def sql(self, s: str) -> str:
        s = _PATHS_RE.sub(lambda m: f"read_parquet([<{m.group(1).count('.parquet')} paths>]", s)
        return self.text(s)

    def paths(self, s: str) -> list[str]:
        out: list[str] = []
        for m in _PATHS_RE.finditer(s):
            out.extend(self.text(p) for p in re.findall(r"'([^']+)'", m.group(1)))
        return out

    def value(self, v: Any) -> Any:
        if isinstance(v, str):
            return self.text(v)
        if isinstance(v, list | tuple):
            return [self.value(x) for x in v]
        return v


def _run_reader(reader: Reader, sc: Scenario, tmp: Path, probe: _Probe, anchors: dict[str, datetime]) -> dict:
    anchor = anchors[sc.anchor]
    root = tmp / f"{reader.name.replace(':', '_').replace('[', '_').replace(']', '')}__{sc.name}"
    lay = build_rollup_golden_layout(root, sc.layout, anchor=anchor, fns=reader.fns)

    def iso(off: float | None, raw: str | None) -> str | None:
        if raw is not None:
            return raw
        if off is None:
            return None
        return (anchor + timedelta(hours=off)).isoformat()

    st = iso(sc.st_off_h, sc.st_raw)
    et = iso(sc.et_off_h, sc.et_raw)
    w = W(st, et, sc.has_filters, parse_iso_utc(st), parse_iso_utc(et))

    src = make_source(str(root), name="svc", service_id="svc")
    con = duckdb.connect()
    norm = _Norm(anchor, root, anchors["H"])
    executed: list[tuple[str, Any]] = []
    try:
        runner = QueryRunner(con, src)

        def fake_execute(q: str, p: list | None = None) -> _Cur:
            executed.append((q, p))
            return _Cur()

        runner.execute = fake_execute  # type: ignore[method-assign]
        runner.get_schema_cols = lambda: list(_COLS)  # type: ignore[method-assign]
        runner._create_active_hour_temp_direct = lambda *a, **k: "__empty__"  # type: ignore[method-assign]
        runner._partial_hour_adjusted_live_start = lambda live_start, token, end: (live_start, [], 0)  # type: ignore[method-assign]

        rollup_coverage._listdir_cache.clear()
        rr.reset_rollup_coverage_ready()
        probe.counts = {}
        patches = []
        if sc.durable:
            patches.append(patch("backend.config.is_durable_serving_mode", return_value=True))
            if sc.durable == "ready":
                rr.mark_rollup_coverage_ready("svc")
        for p in patches:
            p.start()
        probe.on = True
        try:
            outcome = reader.call(runner, w, lay)
            raised = None
        except Exception as e:
            outcome, raised = None, type(e).__name__
        finally:
            probe.on = False
            for p in patches:
                p.stop()
            rr.reset_rollup_coverage_ready()
    finally:
        con.close()

    sql_texts = [norm.sql(q) for q, _ in executed]
    for text in sql_texts:
        SQL_TEXTS[_digest(text)] = text
    return {
        "outcome": _compact(norm.value(outcome) if not isinstance(outcome, str) else norm.text(outcome)),
        "raised": raised,
        "executes": len(executed),
        "sql": [_digest(t) for t in sql_texts],
        "paths": [_compact(norm.paths(q)) for q, _ in executed],
        "params": [norm.value(p) if p is not None else None for _, p in executed],
        "probes": dict(sorted(probe.counts.items())),
    }


SQL_TEXTS: dict[str, str] = {}


def _digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def _compact(v: Any) -> Any:
    """Keep long path lists exact but small: count, digest of the ordered list, and both ends."""
    if isinstance(v, list) and len(v) > 4 and all(isinstance(x, str) for x in v):
        return {"n": len(v), "sha": _digest("\n".join(v)), "head": v[:2], "tail": v[-2:]}
    if isinstance(v, list):
        return [_compact(x) for x in v]
    return v


_GOLDEN_NOW = datetime(2026, 6, 17, 22, 30, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _frozen_golden_clock():
    """The scenarios anchor on "now" (hour start / day start); without a pinned clock the 15-vs-16 hour
    window arithmetic changes with the time of day and the snapshot flips (it failed at 23:17 UTC); the snapshot was recorded at 22:xx UTC, so pin to that hour."""
    import time_machine

    with time_machine.travel(_GOLDEN_NOW, tick=False):
        yield


def _anchors() -> dict[str, datetime]:
    now = datetime.now(UTC)
    return {
        "H": now.replace(minute=0, second=0, microsecond=0),
        "A": now.replace(hour=0, minute=0, second=0, microsecond=0),
    }


def _capture(reader: Reader, tmp: Path, mp: pytest.MonkeyPatch) -> dict[str, dict]:
    probe = _Probe(str(tmp))
    for name in _STAT_FNS:
        mp.setattr(os.path, name, probe.wrap(name, getattr(os.path, name)))
    mp.setattr(os, "listdir", probe.wrap("listdir", os.listdir))
    out: dict[str, dict] = {}
    for _ in range(3):
        before = _anchors()
        out = {}
        for sc in SCENARIOS:
            if sc.needs_filters_param and not reader.takes_filters:
                continue
            out[sc.name] = _run_reader(reader, sc, tmp, probe, before)
            shutil.rmtree(
                tmp / f"{reader.name.replace(':', '_').replace('[', '_').replace(']', '')}__{sc.name}",
                ignore_errors=True,
            )
        if _anchors() == before:
            return out
    raise AssertionError("clock crossed an hour boundary on every attempt")


def _load_golden() -> dict:
    return json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else {}


def _jsonable(d: dict) -> dict:
    return json.loads(json.dumps(d, sort_keys=True))


@pytest.mark.parametrize("reader", READERS, ids=[r.name for r in READERS])
def test_reader_matches_golden(reader: Reader, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    got = _jsonable(_capture(reader, tmp_path, monkeypatch))
    monkeypatch.undo()

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        golden = _load_golden()
        golden[reader.name] = got
        golden["_sql"] = {**golden.get("_sql", {}), **SQL_TEXTS}
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return

    want = _load_golden().get(reader.name)
    assert want is not None, f"no golden recorded for {reader.name}; run with UPDATE_ROLLUP_GOLDEN=1"
    assert got.keys() == want.keys()
    for scenario in want:
        g, w = got[scenario], want[scenario]
        g_probes, w_probes = g.pop("probes"), w.pop("probes")
        if g != w:
            texts = _load_golden().get("_sql", {})
            drift = "\n".join(f"got: {SQL_TEXTS.get(h)}\nwant: {texts.get(h)}" for h in g["sql"] if h not in w["sql"])
            raise AssertionError(f"{reader.name} / {scenario}: behavior drifted from golden\n{g}\n{w}\n{drift}")
        for k, v in g_probes.items():
            assert v <= w_probes.get(k, 0), f"{reader.name} / {scenario}: {k} probes {v} > golden {w_probes.get(k, 0)}"
