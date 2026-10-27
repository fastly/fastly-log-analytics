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
from backend.routers.network import _fetch_pop_health_data
from backend.utils.date_utils import parse_iso_utc

from backend.core import rollups as rr
from backend.core.rollups import _common as rollup_common
from backend.repositories import rollup_coverage
from backend.repositories._base import QueryRunner
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


@pytest.fixture
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


@pytest.mark.usefixtures("_frozen_golden_clock")
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


# ── Merged from tests/repositories/test_reader_coverage_observations.py ──────
from typing import NoReturn
from unittest.mock import MagicMock

import time_machine
from backend.models.dashboard import (
    HourlyRollupCoverage,
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
    TopNRollupCoverage,
)
from backend.repositories.rollup_coverage import (
    collect_hourly_bundle_paths,
    heal_where_clause,
    plan_time_series_rollup_window,
)
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

from backend import config as svcconfig
from backend.repositories import rollup_coverage as coverage

START = datetime(2026, 1, 1, tzinfo=UTC)


def _hour(root: Path, hour: int) -> None:
    path = root / "rollups" / "hour_bundled" / f"hour=2026-01-01-{hour:02d}" / "security_conn_reuse.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


@pytest.mark.parametrize(
    "start,end,filtered,reason",
    [
        (None, None, True, "filtered"),
        (None, None, False, "bounds_missing"),
        ("invalid", "invalid", False, "invalid_window"),
        (START.isoformat(), START.isoformat(), False, "invalid_window"),
        (START.isoformat(), (START + timedelta(hours=1)).isoformat(), False, "window_too_short"),
        (START.isoformat(), (START + timedelta(days=367)).isoformat(), False, "window_too_long"),
        (START.isoformat(), (START + timedelta(hours=2)).isoformat(), False, "eligible"),
    ],
)
def test_eligibility_records_actual_threshold_and_short_circuit(start, end, filtered, reason) -> None:
    observed = ReaderCoverage(reader="dashboard.conn_requests_histogram", minimum_window_hours=2)
    result = coverage.eligible_window(start, end, has_filters=filtered, min_hours=2, observation=observed)
    assert observed.eligibility_reason == reason
    assert observed.eligible is (reason == "eligible")
    assert (result is not None) is (reason == "eligible")
    if reason in {"filtered", "bounds_missing", "invalid_window"}:
        assert observed.window_start is None and observed.window_end is None
    else:
        assert observed.window_start == START.isoformat() and observed.window_end == end
    assert observed.maximum_window_days == 366


@pytest.mark.parametrize("n,reason", [(0, "no_paths"), (1, "below_coverage_floor"), (2, "planned"), (4, "planned")])
def test_day_prefer_counts_missing_hours_in_denominator(tmp_path, n, reason) -> None:
    for hour in range(n):
        _hour(tmp_path, hour)
    (tmp_path / "rollups" / "hour_bundled").mkdir(parents=True, exist_ok=True)
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    with time_machine.travel(START + timedelta(days=1), tick=False):
        result = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=4),
            "security_conn_reuse.parquet",
            observation=observed,
        )
    assert (
        observed.reason == reason
        and observed.closed_hours == 4
        and observed.covered_hours == n
        and observed.coverage_fraction == n / 4
        and observed.hour_files == n
        and observed.day_files == 0
        and observed.minimum_coverage_fraction == 0.5
        and (result is not None) is (reason == "planned")
    )


def test_missing_tree_does_not_claim_zero_coverage(tmp_path) -> None:
    observed = RollupPathCoverage(bundle="ngwaf_bots")
    result = coverage.collect_day_prefer_paths(
        {"_cache_dir_override": str(tmp_path)},
        START,
        START + timedelta(days=30),
        "ngwaf_bots.parquet",
        observation=observed,
    )
    assert (
        result is None
        and observed.reason == "no_bundled_root"
        and observed.closed_hours is None
        and observed.covered_hours is None
        and observed.coverage_fraction is None
        and observed.active_hour_start is None
    )


def test_day_prefer_records_partial_start_and_day_tier_without_probing_more(tmp_path, monkeypatch) -> None:
    day = tmp_path / "rollups" / "day_bundled" / "day=2026-01-02" / "security_conn_reuse.parquet"
    day.parent.mkdir(parents=True)
    day.touch()
    _hour(tmp_path, 23)
    start, end = START + timedelta(hours=22, minutes=30), START + timedelta(days=2)
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    original = coverage.os.path.isfile
    probes: list[str] = []

    def probe(path: str) -> bool:
        probes.append(str(path))
        return original(path)

    monkeypatch.setattr(coverage.os.path, "isfile", probe)
    with time_machine.travel(end + timedelta(hours=1), tick=False):
        expected = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)}, start, end, "security_conn_reuse.parquet"
        )
        baseline_probes = probes.copy()
        probes.clear()
        actual = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)}, start, end, "security_conn_reuse.parquet", observation=observed
        )
    assert (
        actual
        == expected
        == [str(tmp_path / "rollups/hour_bundled/hour=2026-01-01-23/security_conn_reuse.parquet"), str(day)]
    )
    assert probes == baseline_probes
    assert observed.closed_hours == observed.covered_hours == 25
    assert observed.day_files == observed.hour_files == 1
    assert observed.partial_start_skipped is True
    assert observed.active_hour_start == (end + timedelta(hours=1)).isoformat()


def test_done_markers_are_distinct_from_parquet_presence(tmp_path) -> None:
    _hour(tmp_path, 0)
    marker = tmp_path / "rollups/hour_bundled/hour=2026-01-01-01/security_conn_reuse.parquet.done"
    marker.parent.mkdir(parents=True)
    marker.touch()
    observed = RollupPathCoverage(bundle="security_conn_reuse")
    with time_machine.travel(START + timedelta(days=1), tick=False):
        result = coverage.collect_day_prefer_paths(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=2),
            "security_conn_reuse.parquet",
            allow_empty_with_done=True,
            observation=observed,
        )
    assert len(result) == 1 and observed.covered_hours == 2
    assert observed.hour_files == observed.done_markers == 1 and observed.coverage_fraction == 1


def _parquet(root: Path, filename: str, data: dict) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = root / "rollups/hour_bundled/hour=2026-01-01-00" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), path)


@pytest.mark.parametrize("start_minute", [0, 56])
def test_pop_health_24h_window_preserves_leading_slice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, start_minute: int
) -> None:
    start = datetime(2026, 1, 1, 3, start_minute, tzinfo=UTC)
    end = start + timedelta(hours=24)
    source = make_source(str(tmp_path), name="svc", service_id="svc")
    with duckdb.connect() as con, time_machine.travel(end + timedelta(minutes=1), tick=False):
        con.execute(
            "CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, pop VARCHAR, status INTEGER, cache VARCHAR, "
            "resp_bytes BIGINT, tcp_rtt DOUBLE, ttfb DOUBLE)"
        )
        con.executemany(
            "INSERT INTO logs_svc VALUES (?, ?, 200, 'HIT', 100, 1000, 0.01)",
            [
                (start - timedelta(microseconds=1), "AAA"),
                (start, "AAA"),
                (start + timedelta(minutes=1), "AAA"),
                (start + timedelta(hours=1), "BBB"),
                (end - timedelta(minutes=1), "BBB"),
                (end + timedelta(microseconds=1), "BBB"),
            ],
        )
        monkeypatch.setattr(
            rollup_common,
            "acquire_rollup_connection_and_cols",
            lambda *a, **kw: (con, {"pop"}, False),
        )
        layout = build_rollup_golden_layout(tmp_path, anchor=start.replace(minute=0))
        assert rr.build_pop_health_bundles("svc", source, [layout.hkey(k) for k in range(24)]) == 24
        rollup_coverage._listdir_cache.clear()
        expected = dict(
            con.execute(
                "SELECT pop, COUNT(*) FROM logs_svc WHERE timestamp >= ? AND timestamp <= ? GROUP BY pop",
                [start, end],
            ).fetchall()
        )
        data, approximate, observation = _fetch_pop_health_data(con, source, start, end)
        assert {row.pop: row.requests for row in data} == expected == {"AAA": 2, "BBB": 2}
        assert approximate is (start_minute == 0)
        assert observation["section_timings"][-1]["source"] == ("rollup" if approximate else "live")


def test_histogram_records_both_attempts_and_served_outcome(in_memory_duckdb, tmp_path) -> None:
    _parquet(tmp_path, "all_fields.parquet", {"field": ["conn_requests"], "value": ["7"], "count": [5]})
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        result = runner.try_conn_requests_hist_from_rollup(
            START.isoformat(),
            (START + timedelta(hours=2)).isoformat(),
            has_filters=False,
            actual_cols=["conn_requests"],
        )
    assert result == {"top": [{"value": "6–20", "count": 5}], "total": 5}
    observed = runner.reader_coverage[0]
    assert observed.reader == "dashboard.conn_requests_histogram"
    assert observed.outcome == "served" and observed.live_top_up == "outside_window"
    assert [(a.bundle, a.reason) for a in observed.attempts] == [
        ("security_conn_reuse", "no_paths"),
        ("all_fields", "planned"),
    ]
    assert (
        observed.attempts[1].closed_hours == 2
        and observed.attempts[1].covered_hours == 1
        and observed.attempts[1].coverage_fraction == 0.5
        and runner.telemetry()["reader_coverage"][0]["outcome"] == "served"
    )


@pytest.mark.parametrize("kind", ["histogram", "ngwaf"])
def test_reader_records_live_unavailability_without_claiming_complete_results(
    in_memory_duckdb, tmp_path, monkeypatch, kind
) -> None:
    filename = "security_conn_reuse.parquet" if kind == "histogram" else "ngwaf_bots.parquet"
    data = (
        {"bucket": ["1"], "count": [2]}
        if kind == "histogram"
        else {"bot_name": ["Testbot"], "category": ["test"], "count": [2]}
    )
    _parquet(tmp_path, filename, data)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    monkeypatch.setattr(runner, "get_schema_cols", lambda: ["waf_req_id", "conn_requests"])
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: None)
    with time_machine.travel(START + timedelta(hours=2), tick=False):
        if kind == "histogram":
            result = runner.try_conn_requests_hist_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False
            )
            assert result["total"] == 2
        else:
            result = runner.try_ngwaf_top_bots_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False, n=10
            )
            assert result == [{"name": "Testbot", "category": "test", "request_count": 2}]
    observed = runner.reader_coverage[0]
    assert observed.outcome == "served" and observed.live_top_up == "unavailable"
    assert observed.live_start == (START + timedelta(hours=2)).isoformat()
    assert observed.live_end == (START + timedelta(hours=3)).isoformat()
    assert observed.attempts[0].covered_hours == 1 and observed.attempts[0].closed_hours == 2


def test_reader_failure_and_isolation(in_memory_duckdb, tmp_path) -> None:
    _hour(tmp_path, 0)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    other = QueryRunner(in_memory_duckdb, runner.src)
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        result = runner.try_conn_requests_hist_from_rollup(
            START.isoformat(), (START + timedelta(hours=2)).isoformat(), has_filters=False
        )
    assert result is None
    assert runner.reader_coverage[0].outcome == "read_failed"
    assert runner.reader_coverage[0].attempts[0].reason == "planned"
    assert other.reader_coverage == [] and "reader_coverage" not in other.telemetry()
    assert str(tmp_path) not in str(runner.telemetry()["reader_coverage"])


@pytest.mark.parametrize("kind", ["histogram", "ngwaf"])
@pytest.mark.parametrize("live_state", ["missing_column", "empty", "used", "failed"])
def test_live_top_up_outcomes_are_not_confused_with_coverage(
    in_memory_duckdb, tmp_path, monkeypatch, kind, live_state
) -> None:
    histogram = kind == "histogram"
    filename = "security_conn_reuse.parquet" if histogram else "ngwaf_bots.parquet"
    data = (
        {"bucket": ["1"], "count": [2]} if histogram else {"bot_name": ["Testbot"], "category": ["test"], "count": [2]}
    )
    _parquet(tmp_path, filename, data)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    column = "conn_requests" if histogram else "waf_req_id"
    monkeypatch.setattr(runner, "get_schema_cols", lambda: [] if live_state == "missing_column" else [column])
    if live_state == "used":
        if histogram:
            in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 1 AS conn_requests")
        else:
            in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 'req-test' AS waf_req_id")
            in_memory_duckdb.execute(
                "CREATE TEMP TABLE ngwaf_top_ngwaf_bots AS SELECT 'req-test' AS waf_req_id, 'Testbot' AS bot_name, 'test' AS category"
            )
    elif live_state == "failed":
        in_memory_duckdb.execute("CREATE TEMP TABLE live_observation AS SELECT 1 AS wrong_column")
    monkeypatch.setattr(
        runner,
        "_create_active_hour_temp_direct",
        lambda *a, **kw: "__empty__" if live_state == "empty" else "live_observation",
    )
    with time_machine.travel(START + timedelta(hours=2), tick=False):
        if histogram:
            result = runner.try_conn_requests_hist_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False
            )
            count = result["total"]
        else:
            result = runner.try_ngwaf_top_bots_from_rollup(
                START.isoformat(), (START + timedelta(hours=3)).isoformat(), has_filters=False, n=10
            )
            count = result[0]["request_count"]
    assert count == (3 if live_state == "used" else 2)
    assert runner.reader_coverage[0].live_top_up == live_state
    assert runner.reader_coverage[0].outcome == "served"
    assert runner.reader_coverage[0].attempts[0].covered_hours == 1


def test_durable_gate_and_empty_asn_and_filtered_reader(in_memory_duckdb, tmp_path, monkeypatch) -> None:
    observed = RollupPathCoverage(bundle="ngwaf_bots")
    monkeypatch.setattr(coverage, "durable_serving_blocked", lambda src: True)

    def forbidden(*args, **kwargs):
        raise AssertionError("must not inspect coverage/filesystem")

    monkeypatch.setattr(coverage.os.path, "isdir", forbidden)
    assert (
        coverage.collect_day_prefer_paths(
            {}, START, START + timedelta(hours=2), "ngwaf_bots.parquet", observation=observed
        )
        is None
    )
    assert observed.reason == "durable_unready" and observed.closed_hours is None and observed.covered_hours is None

    obs_asn = ReaderCoverage(reader="dashboard.ngwaf_bots", minimum_window_hours=48)
    assert (
        coverage.eligible_window(
            "invalid", "invalid", has_filters=False, require_top_asns=[], min_hours=48, observation=obs_asn
        )
        is None
    )
    assert obs_asn.eligibility_reason == "top_asns_empty" and obs_asn.window_start is None

    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    monkeypatch.setattr(coverage, "durable_serving_blocked", forbidden)
    assert runner.try_ngwaf_top_bots_from_rollup(None, None, has_filters=True, n=10) is None
    obs_filt = runner.reader_coverage[0]
    assert (
        obs_filt.eligible is False
        and obs_filt.eligibility_reason == "filtered"
        and obs_filt.outcome == "ineligible"
        and obs_filt.attempts == []
    )


def _bundle(root, day: int, filename: str = "time_series.parquet") -> None:
    path = root / "rollups/day_bundled" / f"day=2026-01-{day:02d}" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def _hour_scalar_coverage_observations(root, day: int, hour: int) -> None:
    path = root / "rollups/hour_bundled" / f"hour=2026-01-{day:02d}-{hour:02d}" / "time_series.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def test_daily_prefix_and_missing_trailing_tree(tmp_path) -> None:
    for day in range(1, 7):
        _bundle(tmp_path, day)

    obs_missing = ScalarRollupCoverage()
    with time_machine.travel(START + timedelta(days=8), tick=False):
        plan_missing = plan_time_series_rollup_window(
            {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(days=7), observation=obs_missing
        )
    assert (
        plan_missing is None
        and obs_missing.closed_hours == 168
        and obs_missing.prefix_hours == 144
        and obs_missing.trailing.reason == "no_bundled_root"
        and obs_missing.covered_hours is None
        and obs_missing.coverage_fraction is None
        and obs_missing.reason == "trailing_unavailable"
    )

    for hour in range(24):
        _hour_scalar_coverage_observations(tmp_path, 7, hour)
    observed = ScalarRollupCoverage()
    with time_machine.travel(START + timedelta(days=8), tick=False):
        plan = plan_time_series_rollup_window(
            {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(days=7), observation=observed
        )
    assert (
        plan is not None
        and observed.closed_hours == 168
        and observed.prefix_hours == 144
        and observed.day_time_series_files == 6
        and observed.day_overview_files == 0
        and observed.trailing_start == (START + timedelta(days=6)).isoformat()
        and observed.trailing.closed_hours == 24
        and observed.trailing.bundle_hours == 24
        and observed.trailing.missing_hours == 0
        and observed.covered_hours == 168
        and observed.coverage_fraction == 1
        and observed.reason == "planned"
    )


def test_hour_rollover_retains_both_boundaries_without_invalid_combined_fraction(tmp_path, monkeypatch) -> None:
    _hour_scalar_coverage_observations(tmp_path, 1, 0)
    _hour_scalar_coverage_observations(tmp_path, 1, 1)
    build = rollup_coverage.CoverageIndex.build
    clocks = iter([START + timedelta(hours=1), START + timedelta(hours=2)])

    monkeypatch.setattr(
        rollup_coverage.CoverageIndex, "build", lambda st, et, **kwargs: build(st, et, now=next(clocks), **kwargs)
    )
    observed = ScalarRollupCoverage()
    plan = plan_time_series_rollup_window(
        {"_cache_dir_override": str(tmp_path)}, START, START + timedelta(hours=2), observation=observed
    )
    assert (
        plan is not None
        and observed.closed_hours == 1
        and observed.trailing.closed_hours == 2
        and observed.reason == "boundary_changed"
        and observed.coverage_fraction is None
        and observed.covered_hours is None
        and ScalarRollupCoverage.model_validate(observed.model_dump()) == observed
    )


@pytest.mark.parametrize("age,reason", [(1, "planned"), (3, "old_gaps")])
def test_hourly_heal_reports_actual_gaps_without_assuming_fifty_percent_threshold(tmp_path, age, reason) -> None:
    _hour_scalar_coverage_observations(tmp_path, 1, 0)
    observed = HourlyRollupCoverage()
    with time_machine.travel(START + timedelta(days=age), tick=False):
        plan = rollup_coverage.plan_hourly_read(
            {"_cache_dir_override": str(tmp_path)},
            START,
            START + timedelta(hours=3),
            "time_series.parquet",
            allow_all_fields_coverage=True,
            heal=True,
            observation=observed,
        )
    assert (plan is not None) is (reason == "planned")
    assert (
        observed.reason == reason
        and observed.closed_hours == 3
        and observed.bundle_hours == 1
        and observed.missing_hours == 2
        and observed.verified_empty_hours == 0
        and observed.heal_horizon_hours == 48
        and observed.covered_hours == 1
        and observed.coverage_fraction == pytest.approx(1 / 3)
    )


def test_scalar_readers_record_gate_and_execution_and_real_coverage(in_memory_duckdb, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    runner_gate = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    assert runner_gate.try_count_from_rollup(None, None, "logs", "1=1", [], unfiltered_window=False) is None
    assert runner_gate.try_time_series_from_rollup("p99_latency", "1 hour", None, None, "logs", "1=1", []) is None
    assert [(o.reader, o.eligibility_reason, o.outcome) for o in runner_gate.reader_coverage] == [
        ("dashboard.count", "filtered", "ineligible"),
        ("dashboard.time_series", "unsupported_metric", "ineligible"),
    ]
    assert all(o.minimum_window_hours is None for o in runner_gate.reader_coverage)

    for hour, count in enumerate((2, 3)):
        _hour_scalar_coverage_observations(tmp_path, 1, hour)
        path = tmp_path / f"rollups/hour_bundled/hour=2026-01-01-{hour:02d}/time_series.parquet"
        pq.write_table(pa.table({"bucket": [START + timedelta(hours=hour)], "requests": [count]}), path)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    with time_machine.travel(START + timedelta(hours=3), tick=False):
        count = runner.try_count_from_rollup(
            START.isoformat(), (START + timedelta(hours=2)).isoformat(), "unused", "1=1", [], unfiltered_window=True
        )
        series = runner.try_time_series_from_rollup(
            "requests",
            "1 hour",
            START.isoformat(),
            (START + timedelta(hours=2)).isoformat(),
            "unused",
            "1=1",
            [],
            unfiltered_window=True,
        )
    assert count == 5 and [row["value"] for row in series] == [2, 3]
    assert [o.reader for o in runner.reader_coverage] == ["dashboard.count", "dashboard.time_series"]
    for observation in runner.reader_coverage:
        assert (
            observation.minimum_window_hours is None
            and observation.outcome == "served"
            and observation.scalar_plan.closed_hours == 2
            and observation.scalar_plan.covered_hours == 2
            and observation.scalar_plan.coverage_fraction == 1
            and observation.live_top_up == "outside_window"
        )


@pytest.mark.parametrize("reader", ["count", "time_series"])
def test_failed_composite_query_does_not_claim_successful_live_top_up(
    in_memory_duckdb, tmp_path, monkeypatch, reader
) -> None:
    _hour_scalar_coverage_observations(tmp_path, 1, 0)
    runner = QueryRunner(in_memory_duckdb, {"name": "svc", "_cache_dir_override": str(tmp_path)})
    in_memory_duckdb.execute("CREATE TEMP TABLE bad_live AS SELECT 1 AS wrong_column")
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: "bad_live")
    monkeypatch.setattr(runner, "_partial_hour_adjusted_live_start", lambda start, *a: (start, [], 0))
    with time_machine.travel(START + timedelta(hours=1), tick=False):
        if reader == "count":
            result = runner.try_count_from_rollup(
                START.isoformat(), (START + timedelta(hours=2)).isoformat(), "unused", "1=1", [], unfiltered_window=True
            )
        else:
            result = runner.try_time_series_from_rollup(
                "requests",
                "1 hour",
                START.isoformat(),
                (START + timedelta(hours=2)).isoformat(),
                "unused",
                "1=1",
                [],
                unfiltered_window=True,
            )
    assert (
        result is None
        and runner.reader_coverage[0].outcome == "read_failed"
        and runner.reader_coverage[0].live_top_up == "unverified"
    )


def test_virtual_only_top_n_and_any_field_presence(in_memory_duckdb, tmp_path, monkeypatch) -> None:
    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    in_memory_duckdb.execute(
        "CREATE TEMP TABLE virtual_live AS SELECT TIMESTAMPTZ '2026-01-01 00:00:00+00' AS timestamp"
    )
    monkeypatch.setattr(runner, "_create_active_hour_temp_direct", lambda *a, **kw: "virtual_live")
    monkeypatch.setattr(runner, "_partial_hour_adjusted_live_start", lambda start, *a: (start, [], 0))
    observed = ReaderCoverage(reader="dashboard.top_n")
    result = runner._top_n_live_active_hour(
        TopNWindow.build(START, START + timedelta(hours=1), START),
        ["waf_sig_ind"],
        "unused",
        10,
        None,
        SchemaSeeds(["timestamp"], {"timestamp": "TIMESTAMPTZ"}),
        lambda *a: None,
        observation=observed,
    )
    assert result == ([], []) and observed.live_top_up == "missing_column"

    path = tmp_path / "rollups/hour/field=url/hour=2026-01-01-00/part.parquet"
    path.parent.mkdir(parents=True)
    path.touch()
    win = TopNWindow.build(START, START + timedelta(hours=3), START + timedelta(days=3))
    obs_topn = TopNRollupCoverage()
    plan = plan_top_n_paths(str(tmp_path), ["url", "country"], win, observation=obs_topn)
    gaps = find_unrolled_hours(plan, win, ["url", "country"], 48, observation=obs_topn)
    assert (
        gaps == []
        and obs_topn.coverage_scope == "any_field_file_presence"
        and obs_topn.closed_hours == 3
        and obs_topn.any_field_covered_hours == 1
        and obs_topn.field_complete_hours is None
        and obs_topn.per_field_hour_files == 1
        and obs_topn.heal_candidates == 0
        and obs_topn.outside_heal_horizon_hours == 3
        and obs_topn.heal_missing_hours == 0
    )


def test_history_extents_observed_and_failed_extent(in_memory_duckdb, monkeypatch) -> None:
    from backend import config
    from backend.repositories._base import get_source_extent

    monkeypatch.setattr(config, "get_status", lambda name: None)
    in_memory_duckdb.execute(
        "CREATE TABLE logs_history AS SELECT timestamp FROM (VALUES (TIMESTAMPTZ '2026-01-01 00:00:00+00'), (TIMESTAMPTZ '2026-01-01 02:00:00+00')) AS t(timestamp)"
    )
    runner = QueryRunner(in_memory_duckdb, {"name": "svc"})
    total, first, last = get_source_extent(runner, runner.src, "logs_history")
    assert (
        total == 2
        and runner.history.extent_source == "live_query"
        and runner.history.extent_rows == 2
        and runner.history.earliest_event_at == first
        and runner.history.latest_event_at == last
        and runner.history.window_rows is None
        and runner.telemetry()["history"]["extent_source"] == "live_query"
    )

    runner_absent = QueryRunner(in_memory_duckdb, {"name": "svc"})
    assert get_source_extent(runner_absent, runner_absent.src, "logs_absent") == (0, None, None)
    assert (
        runner_absent.history.extent_source == "unavailable"
        and runner_absent.history.extent_rows is None
        and runner_absent.history.earliest_event_at is None
    )


def test_writer_behind_short_circuits_without_extra_probes(tmp_path, monkeypatch) -> None:
    _hour_scalar_coverage_observations(tmp_path, 1, 0)
    per_field = tmp_path / "rollups/hour/field=url/hour=2026-01-01-01"
    per_field.mkdir(parents=True)
    observed = HourlyRollupCoverage()
    with time_machine.travel(START + timedelta(days=1), tick=False):
        original = rollup_coverage.os.path.isfile
        probes = []

        def probe(path):
            probes.append(path)
            return original(path)

        monkeypatch.setattr(rollup_coverage.os.path, "isfile", probe)
        source = {"_cache_dir_override": str(tmp_path)}
        rollup_coverage._listdir_cache.clear()
        expected = rollup_coverage.plan_hourly_read(
            source, START, START + timedelta(hours=3), "time_series.parquet", heal=True
        )
        baseline = probes.copy()
        probes.clear()
        rollup_coverage._listdir_cache.clear()
        actual = rollup_coverage.plan_hourly_read(
            source, START, START + timedelta(hours=3), "time_series.parquet", heal=True, observation=observed
        )
    assert (
        actual is expected is None
        and probes == baseline
        and observed.reason == "writer_behind"
        and observed.closed_hours == 3
        and observed.covered_hours is None
        and observed.missing_hours is None
    )


# ── Merged from tests/repositories/test_top_n_rollup_plan.py ─────────────────
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
        assert win.active_dt == _dt(15, 10) and win.active_dt_end == _dt(15, 11)
        assert (win.st_floor, win.et_floor) == ("2026-03-12-00", "2026-03-14-23")

    def test_unbounded_and_mid_hour_and_range_checks(self) -> None:
        win_unbounded = TopNWindow.build(None, None, NOW)
        assert (win_unbounded.st_floor, win_unbounded.et_floor) == (None, None)
        assert win_unbounded.hour_in_range("2000-01-01-00")
        assert TopNWindow.build(None, _dt(14, 5, 30), NOW).et_floor == "2026-03-14-05"

        win_excl = TopNWindow.build(_dt(14), _dt(16), NOW)
        assert win_excl.hour_in_range("2026-03-14-00")
        for h in ("2026-03-13-23", "2026-03-15-10", "2026-03-15-11"):
            assert not win_excl.hour_in_range(h)

        win_days = TopNWindow.build(_dt(12, 6), _dt(14, 12), NOW)
        assert [win_days.day_fully_in_window(d) for d in ("2026-03-12", "2026-03-13", "2026-03-14", "garbage")] == [
            False,
            True,
            False,
            False,
        ]
        assert [win_days.day_in_range(d) for d in ("2026-03-11", "2026-03-12", "2026-03-14", "2026-03-15")] == [
            False,
            True,
            True,
            False,
        ]


@pytest.mark.usefixtures("_fresh_listdir")
class TestPlanTopNPaths:
    def test_nothing_on_disk_and_unrequested_fields(self, tmp_path: Path) -> None:
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(12), _dt(14), NOW))
        assert not plan.paths and plan.bundled_hours == plan.bundled_days == plan.per_field_hours_seen == set()

        _touch(tmp_path / "rollups/hour/field=other/hour=2026-03-13-05/p.parquet")
        assert not plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13), _dt(14), NOW)).paths

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

    @pytest.mark.parametrize("rel", ["day=2026-03-13/day_bundle_2026-03-13.parquet", "day_bundle_2026-03-13.parquet"])
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
        assert _rel(tmp_path, plan.paths.hour_paths) == ["rollups/hour/field=b/hour=2026-03-13-05/p.parquet"]

    def test_boundary_day_file_and_tmp_files_ignored(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-13-20/p.parquet")
        plan = plan_top_n_paths(str(tmp_path), ["a"], TopNWindow.build(_dt(13, 6), _dt(14), NOW))
        assert not plan.paths.day_paths and len(plan.paths.hour_paths) == 1
        assert plan.days_with_day_files_any == {"2026-03-13"}

        tmp2 = tmp_path / "sub"
        _touch(tmp2 / "rollups/day/field=a/day=2026-03-13/.tmp_p.parquet")
        (tmp2 / "rollups/day/field=b/day=2026-03-13").mkdir(parents=True)
        _touch(tmp2 / "rollups/hour/field=a/hour=2026-03-13-05/p.parquet")
        _touch(tmp2 / "rollups/hour/field=b/hour=2026-03-13-05/p.parquet")
        plan2 = plan_top_n_paths(str(tmp2), ["a", "b"], TopNWindow.build(_dt(13), _dt(14), NOW))
        assert (
            not plan2.paths.day_paths
            and len(plan2.paths.hour_paths) == 2
            and plan2.days_with_day_files_any == {"2026-03-13"}
        )


def _unrolled(root: Path, win: TopNWindow, cap: int = 48) -> list[str]:
    return find_unrolled_hours(plan_top_n_paths(str(root), ["a"], win), win, ["a"], cap)


@pytest.mark.usefixtures("_fresh_listdir")
class TestFindUnrolledHoursAndHealWhere:
    def test_missing_and_cap_and_open_ended_and_mid_hour(self, tmp_path: Path) -> None:
        assert _unrolled(tmp_path, TopNWindow.build(_dt(14, 6), _dt(14, 9), NOW)) == [
            "2026-03-14-06",
            "2026-03-14-07",
            "2026-03-14-08",
        ]
        assert _unrolled(tmp_path, TopNWindow.build(_dt(1), _dt(15, 10), NOW), cap=3) == [
            "2026-03-15-07",
            "2026-03-15-08",
            "2026-03-15-09",
        ]
        assert _unrolled(tmp_path, TopNWindow.build(_dt(15, 7), None, NOW)) == [
            "2026-03-15-07",
            "2026-03-15-08",
            "2026-03-15-09",
        ]
        assert _unrolled(tmp_path, TopNWindow.build(_dt(14, 6, 30), _dt(14, 8), NOW)) == [
            "2026-03-14-06",
            "2026-03-14-07",
        ]

    def test_coverage_and_compacted_days(self, tmp_path: Path) -> None:
        _touch(tmp_path / "rollups/hour_bundled/hour=2026-03-14-06/all_fields.parquet")
        _touch(tmp_path / "rollups/hour/field=a/hour=2026-03-14-07/p.parquet")
        assert _unrolled(tmp_path, TopNWindow.build(_dt(14, 6), _dt(14, 9), NOW)) == ["2026-03-14-08"]

        _touch(tmp_path / "rollups/day/field=a/day=2026-03-13/p.parquet")
        (tmp_path / "rollups/day/field=a/day=2026-03-14").mkdir(parents=True)
        assert _unrolled(tmp_path, TopNWindow.build(_dt(13, 22), _dt(14, 2), NOW)) == [
            "2026-03-14-00",
            "2026-03-14-01",
        ]

        _touch(tmp_path / "rollups/day_bundled/day=2026-03-12/all_fields.parquet")
        assert _unrolled(tmp_path, TopNWindow.build(_dt(12, 22), _dt(13), NOW)) == []

    def test_heal_where_clause(self) -> None:
        assert heal_where_clause(["2026-03-14-06", "2026-03-14-08"], _dt(14, 6, 30), _dt(14, 8, 15)) == (
            "timestamp >= '2026-03-14T06:30:00+00:00' AND timestamp < '2026-03-14T08:15:00+00:00' "
            "AND strftime(timestamp, '%Y-%m-%d-%H') IN ('2026-03-14-06', '2026-03-14-08')"
        )
        assert heal_where_clause(["2026-03-14-06"], None, None).startswith(
            "timestamp >= '2026-03-14T06:00:00+00:00' AND timestamp < '2026-03-14T07:00:00+00:00'"
        )


@pytest.mark.usefixtures("_fresh_listdir")
def test_partial_hour_adjusted_live_start_and_durable_hourly_bundle(tmp_path):
    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    active_hour = active_hour_dt.strftime("%Y-%m-%d-%H")
    watermark_dt = active_hour_dt + timedelta(minutes=20)
    window_end = datetime(2027, 1, 1, tzinfo=UTC)

    with (
        patch("backend.core.rollups.partial_hour.read_partial_hour_watermark", return_value=watermark_dt.timestamp()),
        patch(
            "backend.core.rollups.partial_hour.read_partial_hour_all_fields",
            return_value=[("country", "US", 42)],
        ),
        patch("backend.core.rollups.partial_hour.read_partial_hour_total", return_value=42),
    ):
        runner = QueryRunner.__new__(QueryRunner)
        runner.src = {"service_id": "svc-a", "name": "svc-a"}
        runner.con = None
        live_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            active_hour_dt, active_hour, window_end
        )
        assert live_start == watermark_dt and partial_rows == [("country", "US", 42)] and partial_total == 42

        mid_hour_start = active_hour_dt + timedelta(minutes=15)
        live_start2, partial_rows2, partial_total2 = runner._partial_hour_adjusted_live_start(
            mid_hour_start, active_hour, window_end
        )
        assert live_start2 == mid_hour_start and partial_rows2 == [] and partial_total2 == 0

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


SKIP = frozenset({"rid", "ttfb"})


class TestTopNUnits:
    BY_FIELD = {"a": {"x": 5, "y": 9, "z": 1}, "b": {"p": 2}, "c": {}}

    def test_live_topn_fields_and_fold_rows(self) -> None:
        assert live_topn_fields(["country", "rid", "city", "ttfb"], SKIP, None) == ["country", "city"]
        assert live_topn_fields(["cmcd_x", "edge_a", "rum_b", "fastly_req_id", "country"], SKIP, None) == ["country"]
        cfg = {
            "custom_fields": [
                {"name": "edge_a"},
                {"name": "rum_b", "enabled": False},
                {"name": "cmcd_x", "show_in_dashboard": False},
            ]
        }
        assert live_topn_fields(["cmcd_x", "edge_a", "rum_b"], SKIP, cfg) == ["edge_a"]
        assert live_topn_fields(["rid"], SKIP, {"custom_fields": [{"name": "rid"}]}) == []

        assert fold_rows([("a", "x", 1), ("a", "y", 2)], [("a", "x", 4), ("b", "z", 1)], []) == {
            "a": {"x": 5, "y": 2},
            "b": {"z": 1},
        }
        assert fold_rows() == {} and fold_rows([], []) == {}

    def test_truncate_top_n(self) -> None:
        rows, empty = truncate_top_n(self.BY_FIELD, ["b", "a"], 2, None)
        assert rows == [("b", "p", 2), ("a", "y", 9), ("a", "x", 5)] and empty == []

        rows_widen, _ = truncate_top_n(self.BY_FIELD, ["a", "b"], 1, {"a": 3})
        assert [r for r in rows_widen if r[0] == "a"] == [("a", "y", 9), ("a", "x", 5), ("a", "z", 1)]
        assert [r for r in rows_widen if r[0] == "b"] == [("b", "p", 2)]

        rows_miss, empty_miss = truncate_top_n(self.BY_FIELD, ["ghost", "c", "b"], 5, None)
        assert rows_miss == [("b", "p", 2)] and empty_miss == ["ghost", "c"]

        rows_ties, _ = truncate_top_n({"a": {"x": 1, "y": 1}}, ["a"], 5, None)
        assert [r[1] for r in rows_ties] == ["x", "y"]

    def test_schema_seeds(self) -> None:
        def _boom() -> NoReturn:
            raise AssertionError("must not be called")

        seeds = SchemaSeeds(["a"], {"a": "VARCHAR"})
        seeds.ensure(_boom, _boom)
        assert (seeds.cols, seeds.types) == (["a"], {"a": "VARCHAR"})

        calls: list[str] = []
        seeds2 = SchemaSeeds()
        seeds2.ensure(lambda: (calls.append("cols"), ["a"])[1], lambda: (calls.append("types"), {"a": "INT"})[1])
        seeds2.ensure(lambda: (calls.append("cols"), ["a"])[1], lambda: (calls.append("types"), {"a": "INT"})[1])
        assert calls == ["cols", "types"] and (seeds2.cols, seeds2.types) == (["a"], {"a": "INT"})

        calls_bad: list[str] = []
        seeds3 = SchemaSeeds()

        def bad_cols() -> list[str]:
            calls_bad.append("cols")
            raise RuntimeError("nope")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                seeds3.ensure(bad_cols, lambda: (calls_bad.append("types"), {})[1])
        assert calls_bad == ["cols", "cols"]

        seeds4 = SchemaSeeds()
        with pytest.raises(RuntimeError):
            seeds4.ensure(lambda: ["a"], lambda: (_ for _ in ()).throw(RuntimeError("nope")))
        assert seeds4.cols == ["a"]

    def test_qualify_limit_and_build_rolled_sql(self) -> None:
        assert qualify_limit_expr(["a", "b"], 10, None) == "10"
        assert qualify_limit_expr(["a", "b"], 10, {"a": 500, "ghost": 9}) == "CASE field WHEN 'a' THEN 500 ELSE 10 END"
        assert qualify_limit_expr(["a"], 3, {"ghost": 9}) == "3"
        assert "WHEN 'o''k' THEN 5" in qualify_limit_expr(["o'k"], 1, {"o'k": 5})

        assert build_rolled_sql(TopNPaths(), ["a"], 10, None) is None and not TopNPaths()

        paths = TopNPaths(["/d.parquet"], ["/h.parquet"], ["/bh.parquet"], ["/bd.parquet"])
        sql = build_rolled_sql(paths, ["a", "b"], 10, None)
        assert sql is not None and sql.count(" UNION ALL ") == 3
        order = [sql.index(p) for p in ("/d.parquet", "/h.parquet", "/bh.parquet", "/bd.parquet")]
        assert (
            order == sorted(order) and sql.count("hive_partitioning=1") == 2 and sql.count("hive_partitioning=0") == 2
        )

        sql2 = build_rolled_sql(TopNPaths(["/d.parquet"], [], ["/bh.parquet"], []), ["a", "o'k"], 10, None)
        assert (
            sql2 is not None
            and sql2.count("WHERE field IN ('a', 'o''k')") == 1
            and sql2.index("WHERE field IN") > sql2.index("/bh.parquet")
        )

        sql3 = build_rolled_sql(TopNPaths(["/d.parquet"]), ["a", "b"], 10, {"a": 50})
        assert sql3 is not None and sql3.endswith("<= CASE field WHEN 'a' THEN 50 ELSE 10 END")


def _durable_source() -> dict:
    return {"name": "durable_rollup_svc", "service_id": "durable_rollup_svc", "deployment_mode": "high_scale"}


def _enable_durable(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://catalog/ducklake")


def test_durable_rollup_unavailable_gates(monkeypatch, tmp_path):
    _enable_durable(monkeypatch)
    src = _durable_source()
    root = tmp_path / "rollups" / "hour_bundled"
    (root / "hour=2026-09-06-12").mkdir(parents=True)
    (root / "hour=2026-09-06-12" / "time_series.parquet").touch()

    assert (
        collect_hourly_bundle_paths(
            src,
            datetime(2026, 9, 6, 12, tzinfo=UTC),
            datetime(2026, 9, 6, 13, tzinfo=UTC),
            str(root),
            "time_series.parquet",
        )
        is None
    )

    con = MagicMock()
    runner = QueryRunner(con, src)
    assert runner.execute_top_n_rollups(["country"], None, None) == ([], ["country"])
    con.execute.assert_not_called()
    assert runner.execute_ip_spread_rollups(["tls_ciphers_sha"], None, None) == ({}, {})
    assert (
        runner.try_slow_urls_from_rollup(
            "2026-09-01T00:00:00Z", "2026-09-07T00:00:00Z", has_filters=False, min_requests=1, limit=10
        )
        is None
    )


def test_durable_network_health_and_log_activity_ducklake_fallback(monkeypatch, in_memory_duckdb, test_service_source):
    _enable_durable(monkeypatch)
    src = {**test_service_source, "name": "durable_network_svc", "deployment_mode": "high_scale"}
    table = src["name"]
    cols = [
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
    ]
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
    monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda _self: cols)
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
    assert result["available"] is not False and result["heatmap"]

    con_mock = MagicMock()
    describe_result = MagicMock()
    describe_result.fetchall.return_value = [("timestamp",), ("resp_bytes",)]
    query_result = MagicMock()
    query_result.fetchall.return_value = [(datetime(2026, 9, 6, 12, tzinfo=UTC), 3, 1200)]
    con_mock.execute.side_effect = [describe_result, query_result]

    with (
        patch("backend.core.duckdb.get_connection", return_value=con_mock),
        patch("backend.repositories.usage._log_activity_fallback") as local_fallback,
    ):
        from backend.repositories.usage import get_log_activity

        act_res = get_log_activity(_durable_source(), "2026-09-06T00:00:00Z", "2026-09-07T00:00:00Z", "hour")

    local_fallback.assert_not_called()
    con_mock.close.assert_called_once()
    assert act_res["data"] == [{"time": "2026-09-06T12:00", "row_count": 3, "bytes": 1200}]
    assert act_res["total_rows"] == 3 and act_res["total_bytes"] == 1200


# ── Merged from tests/correctness/test_live_rollup_agreement.py ──

from datetime import UTC, datetime, timedelta, timezone

import pytest

from tests._helpers.rollups import noop_lock as _noop_lock

# ── Harness ────────────────────────────────────────────────────────────────


class _NonClosing:
    """Delegating proxy whose ``close()`` is a no-op.

    The rollup writers call ``con.close()`` in their ``finally`` (e.g.
    ``time_series.py:179``). Our agreement harness shares ONE in-memory
    connection across writer, reader, and the live control — letting the
    writer close it would orphan the base table. This proxy forwards every
    attribute to the real connection but swallows ``close()``.
    """

    def __init__(self, con):
        self._con = con

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._con, name)


_BASE_COLS_SQL = (
    "timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, "
    "resp_bytes BIGINT, ttfb DOUBLE, url VARCHAR, ip VARCHAR, ja4 VARCHAR"
)


def _seed_base_table(con: duckdb.DuckDBPyConnection, rows: list[dict], table: str = "logs") -> None:
    """Create the base log table the writer + live control both read."""
    con.execute("SET threads = 1")
    con.execute(f"CREATE TABLE {table} ({_BASE_COLS_SQL})")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("status", 200),
                r.get("cache"),
                r.get("resp_bytes", 0),
                r.get("ttfb"),
                r.get("url", "/"),
                r.get("ip", "10.0.0.1"),
                r.get("ja4", "ja4-x"),
            ],
        )


def _make_src(cache_dir: str) -> dict:
    return {
        "name": "agree_svc",
        "service_id": "agree-svc-id",
        "_cache_dir_override": cache_dir,
    }


def _build_time_series_bundles(con, src, hours: list[str], table: str = "logs") -> int:
    """Run the REAL ``build_time_series_bundles`` writer against ``table``."""
    from unittest.mock import patch

    from backend.core.rollups import time_series

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.storage.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        return time_series.build_time_series_bundles(src["service_id"], src, hours)


def _hours_between(start: datetime, end: datetime) -> list[str]:
    out, cur = [], start.replace(minute=0, second=0, microsecond=0)
    while cur < end:
        out.append(cur.strftime("%Y-%m-%d-%H"))
        cur += timedelta(hours=1)
    return out


def _active_hour_start() -> datetime:
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)


def _live_requests(con, start: datetime, end: datetime, table: str = "logs") -> int:
    return con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE timestamp >= ? AND timestamp < ?",
        [start, end],
    ).fetchone()[0]


# ── time_series: requests, closed-window agreement ──────────────────────────


@pytest.mark.parametrize("interval", ["1 minute", "1 hour", "1 day"])
def test_ts_requests_closed_window_rollup_equals_live(tmp_path, interval):
    """Window entirely in the past (no active-hour merge): the rollup read
    must equal the live aggregation, bucket-for-bucket, with no duplicate
    buckets, for every supported interval."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    # Two full UTC days, ending at the start of *yesterday's* last hour so
    # the whole window is closed (well before the active hour).
    end = (_active_hour_start() - timedelta(hours=2)).replace(hour=0)
    start = end - timedelta(days=2)

    rows = []
    cur = start
    n = 0
    while cur < end:
        # Deterministic, varying per-hour volume so a dropped/duplicated
        # bucket changes the totals.
        for k in range((cur.hour % 5) + 1):
            rows.append({"timestamp": cur + timedelta(minutes=3 * k + 1), "status": 200})
            n += 1
        cur += timedelta(hours=1)
    assert n > 0
    _seed_base_table(con, rows)

    built = _build_time_series_bundles(con, src, _hours_between(start, end))
    assert built > 0, "writer produced no bundles for a populated window"

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval=interval,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, "rollup reader unexpectedly declined an eligible window"

    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate buckets in rollup output: {times}"

    rollup_total = sum(m["value"] for m in merged)
    live_total = _live_requests(con, start, end)
    assert rollup_total == live_total, f"interval={interval}: rollup total {rollup_total} != live total {live_total}"


@pytest.mark.parametrize(
    "interval,delta",
    [("1 minute", timedelta(minutes=1)), ("1 hour", timedelta(hours=1))],
)
def test_ts_requests_closed_window_per_bucket_agrees(tmp_path, interval, delta):
    """SEAM-05: the closed-window test above asserts only the window TOTAL and
    no-duplicate-buckets — but its docstring claims "bucket-for-bucket". This
    pins the actual per-bucket contract: EACH emitted bucket's value equals the
    live count over exactly that bucket's [start, start+interval) range. Seed
    deliberately varies the count per minute AND per hour, so a wrong bucket
    boundary or a shifted/merged bucket changes a value and trips here even when
    the total still balances. '1 minute' is the resolution the prior test never
    exercised at the value level."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    # 3 fully-closed hours; per-minute count varies as ((hour_idx + minute) % 4)
    # so adjacent minutes and adjacent hours hold different totals.
    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    hour_idx = 0
    while cur < end:
        for minute in range(60):
            for _ in range((hour_idx + minute) % 4):
                rows.append({"timestamp": cur + timedelta(minutes=minute), "status": 200})
        cur += timedelta(hours=1)
        hour_idx += 1
    assert rows
    _seed_base_table(con, rows)

    _build_time_series_bundles(con, src, _hours_between(start, end))
    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval=interval,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, "rollup reader unexpectedly declined an eligible window"

    for m in merged:
        bstart = datetime.fromisoformat(m["time"]).astimezone(UTC)
        live_in_bucket = con.execute(
            "SELECT COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
            [bstart, bstart + delta],
        ).fetchone()[0]
        assert m["value"] == live_in_bucket, (
            f"interval={interval}: bucket {bstart.isoformat()} value {m['value']} != live {live_in_bucket}"
        )
    # Total still balances — together with the per-bucket check this rules out a
    # dropped non-empty bucket (its rows would vanish from the sum).
    assert sum(m["value"] for m in merged) == _live_requests(con, start, end)


# ── time_series: the active-hour MERGE SEAM ─────────────────────────────────


def _seed_seam_scenario(con):
    """Seed rows in: a prior closed day, an earlier *closed hour of today*,
    and the *active hour*. Returns (today_start, active_start, window_end).

    Skips at UTC hour 00 where today has no closed hour to collide with.
    """
    active_start = _active_hour_start()
    if active_start.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")

    today_start = active_start.replace(hour=0)
    prior_closed_hour = active_start - timedelta(hours=1)  # still today, closed
    window_end = active_start + timedelta(minutes=20)  # inside the active hour

    rows = []
    # Prior day (closed) — 4 rows, all 200.
    pday = today_start - timedelta(hours=20)
    for k in range(4):
        rows.append({"timestamp": pday + timedelta(minutes=2 * k), "status": 200})
    # Earlier closed hour of TODAY — 2 rows (one 5xx).
    rows.append({"timestamp": prior_closed_hour + timedelta(minutes=5), "status": 200})
    rows.append({"timestamp": prior_closed_hour + timedelta(minutes=6), "status": 500})
    # Active hour (live only) — 3 rows (one 5xx).
    rows.append({"timestamp": active_start + timedelta(minutes=2), "status": 200})
    rows.append({"timestamp": active_start + timedelta(minutes=4), "status": 500})
    rows.append({"timestamp": active_start + timedelta(minutes=6), "status": 200})
    _seed_base_table(con, rows)
    return today_start, active_start, window_end


def test_ts_merge_seam_hour_interval_no_double_count(tmp_path, frozen_clock):
    """interval='1 hour' crossing the active hour: each hour is its own
    bucket, so live (active hour) and rollup (closed hours) never collide.
    Pin the good behavior + value agreement."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate hour buckets across the seam: {times}"
    assert sum(m["value"] for m in merged) == _live_requests(con, start, window_end)


def test_ts_merge_seam_day_interval_no_double_count(tmp_path, frozen_clock):
    """interval='1 day' crossing the active hour (regression for DP-1).

    The reader UNIONs the rollup branch (today's *closed* hours → day bucket D)
    with the live branch (the *active* hour → the SAME day bucket D). Before the
    fix it did NOT re-aggregate, so "today" was emitted TWICE, each partial. The
    default 30-day dashboard chart (``useReportConfig.ts`` defaults to '1 day'
    for ≥720h windows) hits exactly this path. The fix wraps the UNION in an
    outer ``GROUP BY out_bucket`` that rebuilds the metric from summed raw
    num/den, so the straddling day collapses to one correct value.
    """
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 day",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None

    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), (
        "duplicate day bucket at the active-hour seam — 'today' is emitted "
        f"once from the rollup (closed hours) and once from live (active hour) "
        f"without being summed: {merged}"
    )
    # And the per-day value must equal the live full-day count.
    today_iso_day = today_start
    today_value = sum(m["value"] for m in merged if datetime.fromisoformat(m["time"]).astimezone(UTC) == today_iso_day)
    assert today_value == _live_requests(con, today_start, window_end), (
        "today's daily bucket value disagrees with the live full-day count"
    )


def test_ts_merge_seam_tz_offset_still_agrees(tmp_path, frozen_clock):
    """Same seam, but the FE sends offset (CDT, UTC-5) ISO strings — the
    reader must iterate UTC hours (2026-06-11 regression) AND still produce
    a clean, agreeing series across the live/rollup seam."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    today_start, active_start, window_end = _seed_seam_scenario(con)
    start = today_start - timedelta(days=1)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    cdt = timezone(timedelta(hours=-5))
    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="requests",
        interval="1 hour",
        start_time=start.astimezone(cdt).isoformat(),
        end_time=window_end.astimezone(cdt).isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate buckets under tz-offset input: {times}"
    assert sum(m["value"] for m in merged) == _live_requests(con, start, window_end)


# ── time_series: rate-metric agreement (5xx) ────────────────────────────────


def test_ts_5xx_rate_closed_window_agrees(tmp_path):
    """The rollup expression ``SUM(status_5xx)*100/SUM(requests)`` must equal
    the raw ``COUNT(*) FILTER(status>=500)*100/COUNT(*)`` per bucket."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    while cur < end:
        # 10 rows/hour, 3 of them 5xx → exactly 30.00 %.
        for k in range(10):
            rows.append({"timestamp": cur + timedelta(minutes=k), "status": 500 if k < 3 else 200})
        cur += timedelta(hours=1)
    _seed_base_table(con, rows)
    _build_time_series_bundles(con, src, _hours_between(start, end))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric="5xx",
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None
    assert merged, "expected hourly 5xx buckets"
    for m in merged:
        assert abs(m["value"] - 30.0) < 1e-9, f"5xx rate {m['value']} != 30.0 at {m['time']}"


# Rate metrics keyed by chart_metric → the live numerator expression. The
# denominator for every rate is the raw row count COUNT(*) (mirrors the rollup
# `SUM(requests)`), so a NULL-cache row still counts in the hit_rate denominator.
_RATE_METRIC_LIVE_NUM = {
    "5xx": "COUNT(*) FILTER (WHERE status >= 500)",
    "4xx": "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)",
    "hit_rate": "COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE'))",
}

# The reader rounds rate percentages to 2 decimals, so a full-precision live
# re-derivation (e.g. 27.2727…) differs from the served value (27.27) by up to
# half a unit-in-the-last-place. This tolerance absorbs that rounding while
# staying far below any real divergence: a value error is ≥ one row's worth
# (≥1–10pp here) and the averaging bug this guards against is tens of pp off.
_RATE_TOL = 0.01


@pytest.mark.parametrize("metric", ["5xx", "4xx", "hit_rate"])
def test_ts_rate_metrics_closed_window_per_bucket_agree(tmp_path, metric):
    """SEAM-04: only ``5xx`` had a value-agreement test, and only at a flat
    30%. ``4xx`` and ``hit_rate`` had NONE. Pin all three per hour bucket
    against the live re-derivation ``num*100/COUNT(*)``. The seed mixes
    statuses AND cache states — including NULL-cache rows — so:
      * hit_rate's denominator must be the full row count (rollup
        ``SUM(requests)``); if it were ``SUM(cache_total)`` (the unused
        non-null-cache column the writer also emits) the NULL-cache rows make
        it diverge and this fails, and
      * each bucket carries a distinct rate so a shifted bucket is caught."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=3)
    rows = []
    cur = start
    hour_idx = 0
    while cur < end:
        # 10 rows/hour; status + cache mix shifts per hour so each bucket's
        # 4xx / 5xx / hit_rate differs from its neighbours. Some rows carry a
        # NULL cache (k == 9) that must still count in the denominator.
        for k in range(10):
            if k < 2 + hour_idx:
                status = 500
            elif k < 4 + hour_idx:
                status = 404
            else:
                status = 200
            if k == 9:
                cache = None
            elif k < 3:
                cache = "HIT" if k == 0 else "HIT-STALE"
            else:
                cache = "MISS"
            rows.append({"timestamp": cur + timedelta(minutes=k), "status": status, "cache": cache})
        cur += timedelta(hours=1)
        hour_idx += 1
    _seed_base_table(con, rows)
    _build_time_series_bundles(con, src, _hours_between(start, end))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric=metric,
        interval="1 hour",
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, f"rollup reader declined an eligible window for {metric}"
    assert merged, f"expected hourly {metric} buckets"
    num_expr = _RATE_METRIC_LIVE_NUM[metric]
    for m in merged:
        bstart = datetime.fromisoformat(m["time"]).astimezone(UTC)
        num, den = con.execute(
            f"SELECT {num_expr}, COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
            [bstart, bstart + timedelta(hours=1)],
        ).fetchone()
        expected = (num * 100.0 / den) if den else 0.0
        assert abs(m["value"] - expected) < _RATE_TOL, (
            f"{metric} at {bstart.isoformat()}: rollup {m['value']} != live {expected}"
        )


@pytest.mark.parametrize("metric", ["5xx", "4xx", "hit_rate"])
def test_ts_rate_metrics_day_seam_reaggregate_not_average(tmp_path, frozen_clock, metric):
    """SEAM-03: a rate metric whose day bucket STRADDLES the live/rollup seam
    must re-derive ``SUM(num)/SUM(den)`` over the whole day — never average the
    closed-hour rate with the active-hour rate. The seed gives today's closed
    hour and the active hour deliberately DIFFERENT rates AND different row
    counts, so the weighted answer (correct) and the averaged answer (bug)
    diverge. Asserts today's day bucket equals the live re-derivation over the
    full day [today_start, window_end)."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")

    active_start = _active_hour_start()
    if active_start.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")
    today_start = active_start.replace(hour=0)
    prior_closed_hour = active_start - timedelta(hours=1)  # still today, closed
    window_end = active_start + timedelta(minutes=20)
    start = today_start - timedelta(days=1)

    rows = []
    # Prior day (closed, separate day bucket) — neutral filler.
    pday = today_start - timedelta(hours=20)
    for k in range(4):
        rows.append({"timestamp": pday + timedelta(minutes=2 * k), "status": 200, "cache": "MISS"})
    # Today CLOSED hour: 8 rows — 2x 5xx, 2x 4xx, 3x HIT/HIT-STALE. (25% 5xx)
    for k in range(8):
        status = 500 if k < 2 else (404 if k < 4 else 200)
        cache = ("HIT" if k == 4 else "HIT-STALE") if 4 <= k < 6 else ("HIT" if k == 6 else "MISS")
        rows.append({"timestamp": prior_closed_hour + timedelta(minutes=k), "status": status, "cache": cache})
    # ACTIVE hour (live only): 3 rows — all 5xx, all MISS. (100% 5xx, 0% hit)
    for k in range(3):
        rows.append({"timestamp": active_start + timedelta(minutes=k + 1), "status": 500, "cache": "MISS"})
    _seed_base_table(con, rows)

    _build_time_series_bundles(con, src, _hours_between(start, active_start))

    runner = QueryRunner(con, src)
    merged = runner.try_time_series_from_rollup(
        chart_metric=metric,
        interval="1 day",
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        table_name="logs",
        where_clause="1=1",
        params=[],
    )
    assert merged is not None, f"rollup reader declined the day-seam window for {metric}"
    times = [m["time"] for m in merged]
    assert len(times) == len(set(times)), f"duplicate day buckets at the seam for {metric}: {merged}"

    today_bucket = [m for m in merged if datetime.fromisoformat(m["time"]).astimezone(UTC) == today_start]
    assert len(today_bucket) == 1, f"expected exactly one 'today' day bucket, got {today_bucket}"

    num_expr = _RATE_METRIC_LIVE_NUM[metric]
    num, den = con.execute(
        f"SELECT {num_expr}, COUNT(*) FROM logs WHERE timestamp >= ? AND timestamp < ?",
        [today_start, window_end],
    ).fetchone()
    expected = (num * 100.0 / den) if den else 0.0
    assert abs(today_bucket[0]["value"] - expected) < _RATE_TOL, (
        f"{metric}: day-seam bucket {today_bucket[0]['value']} != weighted live {expected} "
        "(averaging the closed and active rates instead of SUM(num)/SUM(den)?)"
    )


# ── Filtered fallback: filters must NEVER serve unfiltered rollup numbers ────


# ── verified_bots_ts: the OTHER time-series reader (correct seam handling) ───
#
# This is the executable counterpart to DP-1: try_verified_bots_ts_from_rollup
# merges rollup⊕live with an OUTER GROUP BY SUM, so a bucket straddling the
# closed/active boundary is summed (not duplicated). Proving it agrees with
# live across the seam confirms the correct pattern works — and that the
# time_series reader's omission of that step (DP-1) is the anomaly.


def _seed_vbts_table(con, rows, table="logs_vbts"):
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, waf_sig VARCHAR)")
    for ts, sig in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [ts, sig])


def _build_vbts_bundles(con, src, hours, table="logs_vbts"):
    from unittest.mock import patch

    from backend.core.rollups import verified_bots_ts

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return verified_bots_ts.build_verified_bots_ts_bundles(src["service_id"], src, hours)


def _live_vbts(con, st, et, bucket_seconds, table="logs_vbts"):
    """Canonical live verified_bots_ts series over [st, et) — the answer the
    rollup⊕live merge must reproduce exactly (integer SUM, no approximation)."""
    rows = con.execute(
        f"""
        SELECT time_bucket(INTERVAL '{bucket_seconds} seconds', timestamp) AS bucket,
               replace(tag, 'VERIFIED-BOT.', '') AS bot_type, COUNT(*) AS count
        FROM (
            SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag
            FROM {table}
            WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%'
              AND timestamp >= ? AND timestamp < ?
        ) sub
        WHERE tag LIKE 'VERIFIED-BOT.%'
        GROUP BY 1, 2
        """,
        [st, et],
    ).fetchall()
    return {(b.astimezone(UTC), bt): int(c) for b, bt, c in rows}


def test_verified_bots_ts_seam_agrees_with_live(tmp_path, frozen_clock):
    """verified_bots_ts rollup⊕live (1-hour buckets) crossing the active hour
    must equal the canonical live series exactly — including the day-straddle
    that DP-1 gets wrong for time_series."""
    src = _make_src(str(tmp_path))
    src["name"] = "vbts_svc"  # distinct cache scoping from other tests
    con = duckdb.connect(":memory:")

    active = _active_hour_start()
    if active.hour == 0:
        pytest.skip("degenerate at UTC hour 00 — today has no closed hour to merge")
    start = active - timedelta(hours=50)  # ≥ 48h window (reader gate)
    window_end = active + timedelta(minutes=20)  # into the active hour

    rows = []
    # Closed hours: several bot types, incl. a multi-tag waf_sig (unnest) and
    # a tag that must be filtered out (RULE-X).
    rows.append((start + timedelta(hours=1, minutes=5), "VERIFIED-BOT.GOOGLEBOT"))
    rows.append((start + timedelta(hours=1, minutes=6), "VERIFIED-BOT.GOOGLEBOT,RULE-X"))
    rows.append((start + timedelta(hours=1, minutes=7), "VERIFIED-BOT.BINGBOT"))
    rows.append((active - timedelta(hours=1, minutes=10), "VERIFIED-BOT.GOOGLEBOT"))  # last closed hour today
    rows.append((active - timedelta(hours=1, minutes=11), "RULE-ONLY.NOTABOT"))  # excluded entirely
    # Active hour (live only): same + a new type.
    rows.append((active + timedelta(minutes=2), "VERIFIED-BOT.GOOGLEBOT"))
    rows.append((active + timedelta(minutes=3), "VERIFIED-BOT.DUCKDUCKBOT"))
    _seed_vbts_table(con, rows)

    built = _build_vbts_bundles(con, src, _hours_between(start, active))
    assert built > 0

    runner = QueryRunner(con, src)
    got = runner.try_verified_bots_ts_from_rollup(
        start_time=start.isoformat(),
        end_time=window_end.isoformat(),
        temp_table="logs_vbts",
        bucket_seconds=3600,
        has_filters=False,
    )
    assert got is not None, "verified_bots_ts reader declined an eligible window"

    merged = {}
    for bucket_ts, bot_type, count in got:
        key = (bucket_ts.astimezone(UTC), bot_type)
        merged[key] = merged.get(key, 0) + int(count)
        # No duplicate (bucket, bot_type) rows — the outer GROUP BY SUM must
        # already have collapsed any straddle.
    assert len(got) == len(merged), f"duplicate (bucket, bot_type) rows from the seam merge: {got}"
    assert merged == _live_vbts(con, start, window_end, 3600), "verified_bots_ts merge disagrees with live"


# ── origin_summary: exact-SUM agreement over a closed window ─────────────────


def _seed_origin_table(con, rows, table="logs_origin"):
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, cache VARCHAR, ttfb DOUBLE, ost INTEGER)")
    for ts, cache, ttfb, ost in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", [ts, cache, ttfb, ost])


def _build_origin_bundles(con, src, hours, table="logs_origin"):
    from unittest.mock import patch

    from backend.core.rollups import origin_summary

    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return origin_summary.build_origin_summary_bundles(src["service_id"], src, hours)


def test_origin_summary_closed_window_exact_sums_agree(tmp_path):
    """origin_summary's exact (non-percentile) fields — total_misses,
    total_passes, origin_error_rate — must equal the live aggregation over the
    same closed window. (Percentiles are request-weighted/approx by design and
    are NOT asserted here.)"""
    src = _make_src(str(tmp_path))
    src["name"] = "origin_svc"
    con = duckdb.connect(":memory:")

    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0)
    start = end - timedelta(hours=50)  # ≥ 48h, fully closed

    rows = []
    cur = start
    while cur < end:
        # 10 rows/hour: 4 MISS, 2 PASS, 4 HIT; 3 origin-5xx out of 8 with ost.
        for k in range(10):
            cache = "MISS" if k < 4 else ("PASS" if k < 6 else "HIT")
            ost = (500 if k < 3 else 200) if k < 8 else None
            rows.append((cur + timedelta(minutes=k), cache, 0.05, ost))
        cur += timedelta(hours=1)
    _seed_origin_table(con, rows)

    built = _build_origin_bundles(con, src, _hours_between(start, end))
    assert built > 0

    runner = QueryRunner(con, src)
    got = runner.try_origin_summary_from_rollup(
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        has_filters=False,
        actual_cols={"timestamp", "cache", "ttfb", "ost"},
    )
    assert got is not None and got["has_data"], "origin_summary reader declined an eligible window"

    live = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE cache ILIKE 'MISS%') AS misses,
            COUNT(*) FILTER (WHERE cache ILIKE 'PASS%') AS passes,
            COUNT(*) FILTER (WHERE ost BETWEEN 500 AND 599) * 1.0
                / NULLIF(COUNT(*) FILTER (WHERE ost IS NOT NULL), 0) AS err_rate
        FROM logs_origin WHERE timestamp >= ? AND timestamp < ?
        """,
        [start, end],
    ).fetchone()

    assert got["total_misses"] == live[0], f"total_misses {got['total_misses']} != live {live[0]}"
    assert got["total_passes"] == live[1], f"total_passes {got['total_passes']} != live {live[1]}"
    assert abs(got["origin_error_rate"] - float(live[2])) < 1e-9, (
        f"origin_error_rate {got['origin_error_rate']} != live {live[2]}"
    )


def test_slow_urls_filtered_query_falls_back_to_live(tmp_path):
    """The slow_urls rollup is built unfiltered. A filtered request must
    return ``None`` (caller falls back to live) rather than silently serving
    the unfiltered rollup numbers under the user's filter chips."""
    src = _make_src(str(tmp_path))
    con = duckdb.connect(":memory:")
    _seed_base_table(con, [])  # no rows needed; the gate fires first
    runner = QueryRunner(con, src)
    out = runner.try_slow_urls_from_rollup(
        start_time=(datetime.now(UTC) - timedelta(days=7)).isoformat(),
        end_time=datetime.now(UTC).isoformat(),
        has_filters=True,
        min_requests=1,
        limit=10,
    )
    assert out is None, "filtered slow_urls request must fall back to live, not serve unfiltered rollup"


def test_time_series_caller_gates_rollup_on_no_filters():
    """Pin the caller-side contract (dashboard.py:274): the time_series
    rollup fast-path is gated on ``not filters``. This is the only thing
    stopping a filtered request from being served unfiltered rollup numbers
    (try_time_series_from_rollup itself takes no filter argument). If a
    refactor drops the ``not filters`` clause, filtered charts would silently
    show unfiltered totals.
    """
    import inspect

    from backend.repositories import dashboard

    src_text = inspect.getsource(dashboard.get_aggregates) + inspect.getsource(dashboard._compute_dashboard_aggregates)
    assert "not filters" in src_text, (
        "dashboard.get_aggregates no longer gates the time_series rollup on "
        "`not filters` — filtered charts could serve unfiltered rollup numbers"
    )


# ── Remaining metrics: exact-column agreement (writer→reader→live) ───────────
#
# The day-prefer readers (slow_urls / origin_summary / network_rtt /
# network_speed / perf_latency) carry request-weighted percentile columns
# (`_approx`) that can't be asserted exactly across hours. Their EXACT columns
# — counts and SUM-reconstructed means — CAN, as long as the synthetic data
# avoids the writers' per-hour top-K + min-requests pruning (each dimension
# value clears the floor every hour and the value count stays under TOP_K).
# These tests construct exactly that no-pruning regime so the exact columns are
# a clean live==rollup comparison; the approx percentile columns are left to
# their documented tolerance.


def _build_rollup(module_path: str, fn_name: str, con, src, hours: list[str], table: str) -> int:
    """Run any rollup writer ``module.fn(service_id, src, hours)`` against the
    shared in-memory connection (writer's ``con.close()`` is a no-op via the
    proxy; ``_safe_table_for`` and the storage view lock are patched)."""
    import importlib
    from unittest.mock import patch

    mod = importlib.import_module(module_path)
    with (
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        return getattr(mod, fn_name)(src["service_id"], src, hours)


def _closed_48h_window():
    """A ≥48h window entirely in the closed past (the day-prefer readers'
    minimum)."""
    active = _active_hour_start()
    end = active - timedelta(hours=1)
    start = active - timedelta(hours=50)
    return start, end


def _insert(con, table: str, rows: list[tuple]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)


# --- network_speed: exact cnt per (asn, c_speed) ---------------------------


def test_network_speed_closed_window_exact_counts_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "nspeed_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_ns (timestamp TIMESTAMPTZ, asn BIGINT, c_speed VARCHAR)")

    start, end = _closed_48h_window()
    asns = [100, 200]
    speeds = ["broadband", "cellular"]
    rows = []
    cur = start
    while cur < end:
        for asn in asns:
            for sp in speeds:
                for k in range(3):  # 6/asn/hour clears MIN_REQUESTS_PER_HOUR=5
                    rows.append((cur + timedelta(minutes=2 * k + (0 if sp == "broadband" else 1)), asn, sp))
        cur += timedelta(hours=1)
    _insert(con, "logs_ns", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.network",
            "build_network_speed_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_ns",
        )
        > 0
    )

    got = QueryRunner(con, src).try_network_speed_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), top_asns=asns, has_filters=False
    )
    assert got is not None, "network_speed reader declined an eligible window"
    rollup = {(asn, sp): cnt for asn, sp, cnt in got}

    live = {
        (r[0], r[1]): int(r[2])
        for r in con.execute(
            "SELECT asn, c_speed, COUNT(*) FROM logs_ns "
            "WHERE timestamp >= ? AND timestamp < ? AND asn IN (100, 200) "
            "AND c_speed IS NOT NULL AND c_speed != '' GROUP BY asn, c_speed",
            [start, end],
        ).fetchall()
    }
    assert rollup == live, f"network_speed counts disagree: rollup={rollup} live={live}"


# --- slow_urls: exact requests per url (no per-hour pruning) -----------------


def test_slow_urls_closed_window_exact_requests_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "surls_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_su (timestamp TIMESTAMPTZ, url VARCHAR, ttfb DOUBLE)")

    start, end = _closed_48h_window()
    urls = ["/a", "/b", "/c"]
    rows = []
    cur = start
    while cur < end:
        for ui, url in enumerate(urls):
            for k in range(6):  # clears MIN_REQUESTS_PER_HOUR=5; 3 urls << TOP_K
                rows.append((cur + timedelta(minutes=k + ui), url, 0.01 * (k + 1)))
        cur += timedelta(hours=1)
    _insert(con, "logs_su", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.slow_urls", "build_slow_urls_bundles", con, src, _hours_between(start, end), "logs_su"
        )
        > 0
    )

    got = QueryRunner(con, src).try_slow_urls_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), has_filters=False, min_requests=1, limit=100
    )
    assert got is not None and got["has_data"], "slow_urls reader declined an eligible window"
    rollup_req = {r["url"]: r["requests"] for r in got["rows"]}

    live_req = {
        r[0]: int(r[1])
        for r in con.execute(
            "SELECT url, COUNT(*) FROM logs_su WHERE timestamp >= ? AND timestamp < ? AND url IS NOT NULL GROUP BY url",
            [start, end],
        ).fetchall()
    }
    assert rollup_req == live_req, f"slow_urls requests disagree: rollup={rollup_req} live={live_req}"


# --- perf_latency: exact requests + avg per value ---------------------------


def test_perf_latency_closed_window_exact_requests_and_avg_agree(tmp_path):
    src = _make_src(str(tmp_path))
    src["name"] = "perf_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_pl (timestamp TIMESTAMPTZ, url VARCHAR, elapsed DOUBLE)")

    start, end = _closed_48h_window()
    urls = ["/x", "/y"]
    rows = []
    cur = start
    while cur < end:
        for ui, url in enumerate(urls):
            for k in range(6):  # clears PERF_URLS_MIN_REQUESTS_PER_HOUR=5
                rows.append((cur + timedelta(minutes=k + ui), url, float(1000 * (k + 1) + ui * 50)))
        cur += timedelta(hours=1)
    _insert(con, "logs_pl", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.perf_latency",
            "build_perf_latency_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_pl",
        )
        > 0
    )

    got = QueryRunner(con, src).try_perf_latency_from_rollup(
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        dimension="url",
        sort_by="p99",
        has_filters=False,
        min_requests=0,
        limit=100,
    )
    assert got is not None, "perf_latency reader declined an eligible window"
    rollup = {r["value"]: (r["requests"], r["avg_ms"]) for r in got["rows"]}

    live = {
        r[0]: (int(r[1]), float(r[2]) / 1000.0)
        for r in con.execute(
            "SELECT url, COUNT(*), AVG(elapsed) FROM logs_pl "
            "WHERE timestamp >= ? AND timestamp < ? AND url IS NOT NULL GROUP BY url",
            [start, end],
        ).fetchall()
    }
    assert set(rollup) == set(live)
    for value in live:
        assert rollup[value][0] == live[value][0], f"perf_latency requests disagree for {value}"
        assert abs(rollup[value][1] - live[value][1]) < 1e-6, f"perf_latency avg_ms disagrees for {value}"


# --- network_rtt: no exact column → path-selection + ballpark p95 ------------


def test_network_rtt_closed_window_path_and_ballpark(tmp_path):
    """network_rtt exposes only request-weighted percentile approximations —
    nothing exact to assert. Pin that the rollup IS selected for an eligible
    window, returns the requested ASNs, and the weighted p95 lands in a sane
    ballpark of the live APPROX_QUANTILE (within 2×)."""
    src = _make_src(str(tmp_path))
    src["name"] = "nrtt_svc"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_nr (timestamp TIMESTAMPTZ, asn BIGINT, tcp_rtt DOUBLE)")

    start, end = _closed_48h_window()
    asns = [100, 200]
    rtt_dist = [1000, 1500, 2000, 3000, 4000, 5000, 6000, 8000, 10000, 20000]
    rows = []
    cur = start
    while cur < end:
        for asn in asns:
            for k, v in enumerate(rtt_dist):  # 10/asn/hour, fixed distribution
                rows.append((cur + timedelta(minutes=k), asn, float(v + asn)))
        cur += timedelta(hours=1)
    _insert(con, "logs_nr", rows)

    assert (
        _build_rollup(
            "backend.core.rollups.network",
            "build_network_rtt_bundles",
            con,
            src,
            _hours_between(start, end),
            "logs_nr",
        )
        > 0
    )

    got = QueryRunner(con, src).try_network_rtt_from_rollup(
        start_time=start.isoformat(), end_time=end.isoformat(), top_asns=asns, has_filters=False
    )
    assert got is not None, "network_rtt reader declined an eligible window"
    assert set(got) == set(asns), f"network_rtt returned ASNs {set(got)} != {set(asns)}"

    for asn in asns:
        live_p95 = con.execute(
            "SELECT APPROX_QUANTILE(tcp_rtt, 0.95) FROM logs_nr "
            "WHERE timestamp >= ? AND timestamp < ? AND asn = ? AND tcp_rtt > 0",
            [start, end, asn],
        ).fetchone()[0]
        got_p95 = got[asn]["p95_rtt_us"]
        assert got_p95 is not None and got_p95 > 0
        assert 0.5 * live_p95 <= got_p95 <= 2.0 * live_p95, (
            f"network_rtt weighted p95 {got_p95} not in ballpark of live {live_p95} for asn {asn}"
        )


# --- sessions: exact session aggregates across the active-hour seam ----------


def _build_session_bundles(con, src, hours, table="logs_sess"):
    return _build_rollup("backend.core.rollups.sessions", "build_session_bundles", con, src, hours, table)


def test_sessions_seam_exact_aggregates_agree(tmp_path, frozen_clock):
    """sessions reader merges closed-hour rollup ⊕ active-hour live and stitches
    per (ip, ja4). For sessions that don't break the 30-min gap, the exact
    aggregates (req_count / reqs_4xx / reqs_5xx / total_bytes) must equal the
    live totals over the whole window — across the seam.

    live SEAM-01: the clock is frozen (``frozen_clock``) so the seam is
    deterministic. The previous guard (``if active.minute > 35: skip``) was
    DEAD — ``_active_hour_start()`` floors the minute to 0 — so it never fired
    and left the test exposed to an hour-rollover flake: if "now" crossed the
    hour between seeding and the read, the seeded active hour became a closed
    hour with no bundle and the reader declined."""
    src = _make_src(str(tmp_path))
    src["name"] = "sess_svc"
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs_sess (timestamp TIMESTAMPTZ, ip VARCHAR, ja4 VARCHAR, "
        "status INTEGER, resp_bytes BIGINT, country VARCHAR, asn INTEGER)"
    )

    active = _active_hour_start()
    start = active - timedelta(hours=3)
    window_end = active + timedelta(minutes=20)
    closed_hours = [active - timedelta(hours=h) for h in (3, 2, 1)]

    # Two entities; each a single continuous session (hourly rollup rows are
    # <30min apart across the seam). Status mix gives nonzero 4xx/5xx.
    entities = [("1.1.1.1", "ja4-a", "US", 100, 110, 200), ("2.2.2.2", "ja4-b", "GB", 300, 90, 222)]
    rows = []
    for ip, ja4, country, b_ok, b_bad, asn in entities:
        for h in closed_hours:
            rows.append((h + timedelta(minutes=5), ip, ja4, 200, b_ok, country, asn))
            rows.append((h + timedelta(minutes=55), ip, ja4, 404, b_bad, country, asn))
        rows.append((active + timedelta(minutes=2), ip, ja4, 200, b_ok, country, asn))
        rows.append((active + timedelta(minutes=5), ip, ja4, 500, b_bad, country, asn))
    _insert(con, "logs_sess", rows)

    assert _build_session_bundles(con, src, [h.strftime("%Y-%m-%d-%H") for h in closed_hours]) == 3

    from backend.repositories.sessions import _get_sessions_from_rollup

    runner = QueryRunner(con, src)
    result = _get_sessions_from_rollup(
        runner=runner,
        con=con,
        src=src,
        table_name="logs_sess",
        actual_cols={"timestamp", "ip", "ja4", "status", "resp_bytes", "country", "asn"},
        start_dt=start,
        end_dt=window_end,
        page=1,
        limit=100,
        sort_by="req_count",
        sort_dir="DESC",
        flagged_only=False,
        min_reqs_flag=1000,
        min_4xx_pct_flag=20.0,
        has_ja4=True,
        has_rtt=False,
        has_edge=False,
        has_edge_sid=False,
        has_cmcd=False,
        section_timings=[],
        rollup_filters=None,
    )
    assert result is not None, "sessions reader declined an eligible window"
    by_ip = {s["ip"]: s for s in result["sessions"]}
    assert set(by_ip) == {"1.1.1.1", "2.2.2.2"}, f"expected one stitched session per entity, got {by_ip}"

    live = {
        r[0]: (int(r[1]), int(r[2]), int(r[3]), int(r[4]))
        for r in con.execute(
            "SELECT ip, COUNT(*), "
            "  SUM(CASE WHEN status BETWEEN 400 AND 499 THEN 1 ELSE 0 END), "
            "  SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END), "
            "  SUM(resp_bytes) "
            "FROM logs_sess WHERE timestamp >= ? AND timestamp < ? GROUP BY ip",
            [start, window_end],
        ).fetchall()
    }
    for ip, (req, r4, r5, tb) in live.items():
        s = by_ip[ip]
        assert s["req_count"] == req, f"{ip}: req_count {s['req_count']} != live {req}"
        assert s["reqs_4xx"] == r4, f"{ip}: reqs_4xx {s['reqs_4xx']} != live {r4}"
        assert s["reqs_5xx"] == r5, f"{ip}: reqs_5xx {s['reqs_5xx']} != live {r5}"
        assert s["total_bytes"] == tb, f"{ip}: total_bytes {s['total_bytes']} != live {tb}"


# --- ip_spread: HLL distinct-IP merge agreement (writer→from_bytes/merge/count→live) ---
#
# ip_spread is the only rollup reader whose merge primitive is *approximate*:
# QueryRunner.execute_ip_spread_rollups folds the per-hour HyperLogLog sketches
# via ``from_bytes(...).merge(...)`` (_base.py:2532-2559) and returns the
# cardinality estimate. The HLL unit tests (tests/utils/test_hll.py) cover the
# primitive and the hour-bundling tests cover the writer, but nothing drove the
# REAL writer → REAL reader merge → live ``COUNT(DISTINCT ip)`` end to end. This
# closes that gap — the last uncovered reader in the agreement harness.


def _build_ip_spread(con, src, table: str = "logs", fields=("ja4",)) -> None:
    """Run the REAL ``_run_ip_spread_per_field`` writer against ``table``.

    Unlike the other builders this writer takes ``table_ident`` / ``where_sql``
    directly (no ``_safe_table_for`` hop) and groups by hour itself, so there is
    no ``hours`` argument — it materializes a sketch for every hour present in
    the table. ``describe_columns`` runs through ``execute_with_stale_view_retry``
    so that passthrough patch is required too; the table must carry an ``ip``
    column or the writer returns early (recompute.py:618)."""
    from unittest.mock import patch

    from backend.core.rollups import recompute

    with (
        patch("backend.core.duckdb.get_connection", return_value=_NonClosing(con)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
    ):
        recompute._run_ip_spread_per_field(src["service_id"], src, table, "1=1", list(fields))


def test_ip_spread_closed_window_distinct_count_merge_agrees(tmp_path):
    """The HLL merge reader path (``from_bytes``→``merge``→``count``) must agree
    with the live ``COUNT(DISTINCT ip)`` — within HLL's error bound — after
    merging sketches across multiple closed hours.

    The SAME distinct-IP set is seeded into every hour for each ``ja4`` value,
    so the cross-hour merge is a set UNION: a correct HLL merge returns ~N,
    while a merge that mistakenly SUMMED per-hour counts would return ~N×hours
    and blow past tolerance. That asymmetry is what makes this a real check of
    the merge primitive and not just the single-sketch count. IP strings are
    fixed, so the HLL estimate is deterministic (no per-run flake)."""
    src = _make_src(str(tmp_path))
    src["name"] = "ipspread_svc"
    con = duckdb.connect(":memory:")
    # Match prod: get_connection pins the session to UTC (duckdb.py:861). The
    # writer buckets hours via DuckDB strftime (session TZ) while the reader's
    # _in_window compares Python-UTC hour strings — they only agree on a UTC
    # connection, which is exactly what every prod connection is.
    con.execute("SET TimeZone='UTC'")

    # Fully-closed 3-hour window (the reader skips the active hour).
    end = (_active_hour_start() - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=3)
    closed_hours = [start + timedelta(hours=h) for h in range(3)]

    # Disjoint distinct-IP sets per value, REPEATED every hour so the merge is a
    # union (not a sum). Far under IP_SAMPLE_CAP=5000, so no value is capped.
    value_ips = {
        "ja4-A": [f"10.10.{i // 250}.{(i % 250) + 1}" for i in range(240)],
        "ja4-B": [f"10.20.{i // 250}.{(i % 250) + 1}" for i in range(60)],
    }
    rows = []
    for h in closed_hours:
        for ja4, ips in value_ips.items():
            for i, ip in enumerate(ips):
                rows.append({"timestamp": h + timedelta(seconds=i), "ja4": ja4, "ip": ip})
    _seed_base_table(con, rows)

    _build_ip_spread(con, src, table="logs", fields=("ja4",))

    spread, meta = QueryRunner(con, src).execute_ip_spread_rollups(["ja4"], start.isoformat(), end.isoformat())
    assert spread, "ip_spread reader returned no merged sketches for an eligible closed window"
    # Exactly the two seeded (field, value) pairs come back — no value silently
    # dropped from the merge, none spuriously invented.
    assert set(spread) == {("ja4", "ja4-A"), ("ja4", "ja4-B")}, set(spread)

    for ja4, ips in value_ips.items():
        live = con.execute(
            "SELECT COUNT(DISTINCT ip) FROM logs WHERE timestamp >= ? AND timestamp < ? AND ja4 = ?",
            [start, end, ja4],
        ).fetchone()[0]
        assert live == len(ips), "sanity: the repeated-hour union is the per-value distinct set"
        est = spread[("ja4", ja4)]
        assert abs(est - live) / live <= 0.15, (
            f"ip_spread HLL merge for {ja4}: estimate {est} not within 15% of live "
            f"distinct {live} (a sum-instead-of-union merge bug would land near {live * len(closed_hours)})"
        )

    # Per-field meta: every seeded closed hour contributed a sketch; nothing hit
    # the IP_SAMPLE_CAP, so no (field, value) is flagged capped.
    assert meta["ja4"]["coverage_hours"] == len(closed_hours)
    assert meta["ja4"]["capped_values"] == 0
