"""Tests for origin_dims, origin_latency_ts, and origin_summary rollup writers,
backfill drivers, daily compactors, and /origin readers."""

from __future__ import annotations

import contextlib
import logging
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests._helpers.rollups import (
    build_writer_patches as _build_patches,
)
from tests._helpers.rollups import (
    make_stub_runner,
    write_hour_bundle,
    writer_patches,
)
from tests._helpers.rollups import (
    make_stub_runner as _stub_runner,
)
from tests._helpers.rollups import (
    noop_lock as _noop_lock,
)
from tests._helpers.rollups import (
    past_hour as _past_hour,
)
from tests._helpers.rollups import (
    three_days_ago_iso as _three_days_ago_iso,
)
from tests._helpers.rollups import (
    two_days_ago_iso as _two_days_ago_iso,
)
from tests._helpers.rollups import (
    write_hour_all_fields as _write_hour_all_fields,
)
from tests._helpers.rollups import (
    write_hour_bundle as _write_hour_bundle,
)
from tests._helpers.rollups import (
    yesterday_iso as _yesterday_iso,
)

from backend.core import rollups
from backend.core.rollups import day_bundles, origin_dims, origin_latency_ts, origin_summary
from backend.repositories import origin as origin_repo
from backend.repositories._base import QueryRunner

_ORIGIN_DIMS_DDL = (
    "timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN, ottfb DOUBLE, ttfb DOUBLE"
)
_OLTS_DDL = "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE"
_SUMMARY_DDL = (
    "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE, "
    "elapsed DOUBLE, ost INTEGER, obytes BIGINT, cache VARCHAR"
)


def _cache_root(tmp_path):
    p = tmp_path / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _seed_table(con: duckdb.DuckDBPyConnection, table: str, cols_ddl: str, rows: list[dict]) -> None:
    con.execute(f"CREATE TABLE {table} ({cols_ddl})")
    colnames = [c.strip().split()[0].strip('"') for c in cols_ddl.split(",")]
    placeholders = ", ".join("?" for _ in colnames)
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES ({placeholders})", [r.get(c) for c in colnames])


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    _seed_table(con, table, _ORIGIN_DIMS_DDL, rows)


def _seed_logs_rollups_origin_latency_ts(
    con: duckdb.DuckDBPyConnection, table: str, rows: list[dict], *, cols: str | None = None
) -> None:
    _seed_table(con, table, cols or _OLTS_DDL, rows)


def _seed_logs_rollups_origin_summary(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    _seed_table(con, table, _SUMMARY_DDL, rows)


def _seed_logs_partial(con: duckdb.DuckDBPyConnection, table: str, cols: list[str], rows: list[dict]) -> None:
    col_defs = ", ".join(['"timestamp" TIMESTAMPTZ', *(f'"{c}" DOUBLE' for c in cols)])
    _seed_table(con, table, col_defs, rows)


@contextlib.contextmanager
def _run_with_build_patches(cache_root, table: str, fresh_con):
    with contextlib.ExitStack() as stack:
        for cm in _build_patches(cache_root, table, fresh_con):
            stack.enter_context(cm)
        yield


@contextlib.contextmanager
def _compact_patches(cache_root):
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
    ):
        yield


def _seed_two_bundled_hours(cache_root, existing_bundle: str) -> tuple[str, str]:
    h1_token, _ = _past_hour(3)
    h2_token, _ = _past_hour(4)
    bundled_root = cache_root / "rollups" / "hour_bundled"
    for tok in (h1_token, h2_token):
        (bundled_root / f"hour={tok}").mkdir(parents=True, exist_ok=True)
        (bundled_root / f"hour={tok}" / "all_fields.parquet").write_bytes(b"x")
    (bundled_root / f"hour={h2_token}" / existing_bundle).write_bytes(b"x")
    return h1_token, h2_token


def _write_all_fields(hour_dir: str, field_value_counts: list[tuple[str, str | None, int]]) -> None:
    os.makedirs(hour_dir, exist_ok=True)
    with contextlib.closing(duckdb.connect()) as wcon:
        tuples = ", ".join(
            f"('{f}', 'h', {'NULL' if v is None else repr(v)}, CAST({c} AS BIGINT))" for f, v, c in field_value_counts
        )
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {tuples}) AS t(field, hour, value, count)) "
            f"TO '{hour_dir}/all_fields.parquet' (FORMAT PARQUET)"
        )


def _build_temp_table(con: duckdb.DuckDBPyConnection, temp_table: str, rows: list[dict]) -> None:
    con.execute(
        f"CREATE TABLE {temp_table} (timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN, lat_us DOUBLE)"
    )
    for r in rows:
        ottfb, ttfb = r.get("ottfb"), r.get("ttfb")
        lat_us = ottfb if ottfb is not None else (ttfb * 1_000_000.0 if ttfb is not None else None)
        con.execute(
            f"INSERT INTO {temp_table} VALUES (?, ?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("pop"), r.get("oip"), r.get("ost"), r.get("edge"), lat_us],
        )


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginDims:
    def test_build_origin_dims_writes_three_bundles(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-od-1"}
        hour_token, hour_dt = _past_hour(2)
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "pop": "DEN" if i % 2 == 0 else "IAD",
                "oip": "10.0.0.1",
                "ost": 200 if i < 6 else 500,
                "edge": i % 2 == 0,
                "ottfb": 100_000.0 + i * 1000,
                "ttfb": 0.1,
            }
            for i in range(8)
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_od", rows)
            return c

        with _run_with_build_patches(cache_root, "logs_od", _fresh_con):
            assert origin_dims.build_origin_dims_bundles("svc-od-1", src, [hour_token]) == 3

        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        pop_file, ip_file, path_file = (
            hour_dir / f for f in ("origin_pop.parquet", "origin_ip.parquet", "origin_path.parquet")
        )
        assert pop_file.exists() and ip_file.exists() and path_file.exists()

        assert {"pop", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us"}.issubset(
            pq.read_table(str(pop_file)).column_names
        )
        assert set("oip requests lat_us_count lat_us_sum p50_us p95_us ost_5xx_count ost_total_count".split()).issubset(
            pq.read_table(str(ip_file)).column_names
        )
        assert {"edge", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us"}.issubset(
            pq.read_table(str(path_file)).column_names
        )

        ip_rows = {r["oip"]: r for r in pq.read_table(str(ip_file)).to_pylist()}
        assert ip_rows["10.0.0.1"]["ost_5xx_count"] == 2 and ip_rows["10.0.0.1"]["ost_total_count"] == 8

    def test_build_origin_dims_skips_active_hour(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_od", [])
            return c

        with _run_with_build_patches(cache_root, "logs_od", _fresh_con):
            assert origin_dims.build_origin_dims_bundles("svc-od-2", {"name": "svc-od-2"}, [active_token]) == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}" / "origin_pop.parquet").exists()

    def test_build_origin_dims_missing_oip_skips_ip_only(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_noip (timestamp TIMESTAMPTZ, pop VARCHAR, edge BOOLEAN, ottfb DOUBLE, ttfb DOUBLE)"
            )
            for i in range(4):
                c.execute(
                    "INSERT INTO logs_noip VALUES (?, ?, ?, ?, ?)",
                    [hour_dt + timedelta(minutes=i), "DEN", i % 2 == 0, 100_000.0, 0.1],
                )
            return c

        with _run_with_build_patches(cache_root, "logs_noip", _fresh_con):
            assert origin_dims.build_origin_dims_bundles("svc-od-3", {"name": "svc-od-3"}, [hour_token]) == 2
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "origin_pop.parquet").exists() and (hour_dir / "origin_path.parquet").exists()
        assert not (hour_dir / "origin_ip.parquet").exists()

    def test_build_origin_dims_no_latency_writes_nothing(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, _ = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_nolat (timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN)"
            )
            return c

        with _run_with_build_patches(cache_root, "logs_nolat", _fresh_con):
            assert origin_dims.build_origin_dims_bundles("svc-od-4", {"name": "svc-od-4"}, [hour_token]) == 0

    def test_backfill_origin_dims_walks_existing_bundle_hours(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        h1_token, h2_token = _seed_two_bundled_hours(cache_root, "origin_pop.parquet")
        written_by_bundle: dict[str, list[str]] = {}

        def _spy_per_hour(_sid, _src, hours, *, bundle_filename, **_kw):
            written_by_bundle[bundle_filename] = list(hours)
            return len(hours)

        with (
            patch("backend.core.rollups._common.build_per_hour_bundles", side_effect=_spy_per_hour),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        ):
            n = origin_dims.backfill_origin_dims_bundles("svc-od-5", {"name": "svc-od-5"})

        assert written_by_bundle == {
            "origin_pop.parquet": [h1_token],
            "origin_ip.parquet": sorted([h1_token, h2_token]),
            "origin_path.parquet": sorted([h1_token, h2_token]),
        }
        assert n == 5

    def test_readers_eligibility_gates(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, {"name": "svc-od-6"})
            readers = [
                lambda s, e, hf: runner.try_origin_pop_latency_from_rollup(s, e, has_filters=hf, limit=30),
                lambda s, e, hf: runner.try_origin_ip_health_from_rollup(s, e, has_filters=hf, limit=30),
                lambda s, e, hf: runner.try_origin_path_breakdown_from_rollup(s, e, has_filters=hf),
                lambda s, e, hf: runner.try_origin_status_from_rollup(s, e, has_filters=hf),
            ]
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                for call in readers:
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None
                    assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None
                    assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None

    def test_try_origin_status_from_rollup_reads_all_fields(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        bundled_root = str(cache_root / "rollups" / "hour_bundled")
        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        for dt in hours:
            _write_all_fields(
                f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}",
                [("ost", "200", 10), ("ost", "404", 3), ("ost", "829", 1), ("ost", None, 2), ("url", "/x", 99)],
            )

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, {"name": "svc-od-7"})
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                result = runner.try_origin_status_from_rollup(st_iso, et_iso, has_filters=False)

        assert result is not None and result["has_data"] is True and "_approx" not in result
        by_status = {r["status"]: r for r in result["rows"]}
        assert by_status[200]["count"] == 500 and by_status[404]["count"] == 150 and by_status[-1]["count"] == 50
        assert abs(by_status[200]["pct"] - (500 * 100.0 / 700)) < 1e-6
        assert abs(sum(r["pct"] for r in result["rows"]) - 100.0) < 1e-6

    def test_rollup_readers_match_live_temp_path(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-od-8"}
        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]

        def _canonical_rows(hour_dt):
            return [
                *(
                    {
                        "timestamp": hour_dt + timedelta(seconds=i),
                        "pop": "DEN",
                        "oip": "10.0.0.1",
                        "ost": 200 if i < 8 else 500,
                        "edge": True,
                        "ottfb": 100_000.0 + i * 1000,
                        "ttfb": 0.1,
                    }
                    for i in range(10)
                ),
                *(
                    {
                        "timestamp": hour_dt + timedelta(seconds=30 + i),
                        "pop": "IAD",
                        "oip": "10.0.0.2",
                        "ost": 200,
                        "edge": False,
                        "ottfb": 500_000.0 + i * 1000,
                        "ttfb": 0.5,
                    }
                    for i in range(10)
                ),
            ]

        for dt in hours:
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs(c, "logs_par", rows)
                return c

            with _run_with_build_patches(cache_root, "logs_par", _fresh_con):
                origin_dims.build_origin_dims_bundles("svc-od-8", src, [dt.strftime("%Y-%m-%d-%H")])

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            _build_temp_table(con, "t_par", [r for dt in hours for r in _canonical_rows(dt)])
            runner = QueryRunner(con, src)
            actual_cols = {"pop", "oip", "ost", "edge"}
            live_pop = origin_repo._origin_pop_latency_from_temp(runner, "t_par", actual_cols, 30)
            live_ip = origin_repo._origin_ip_health_from_temp(runner, "t_par", actual_cols, 30)
            live_path = origin_repo._origin_path_breakdown_from_temp(runner, "t_par", actual_cols)

            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                roll_pop = runner.try_origin_pop_latency_from_rollup(st_iso, et_iso, has_filters=False, limit=30)
                roll_ip = runner.try_origin_ip_health_from_rollup(st_iso, et_iso, has_filters=False, limit=30)
                roll_path = runner.try_origin_path_breakdown_from_rollup(st_iso, et_iso, has_filters=False)

        assert roll_pop is not None and roll_ip is not None and roll_path is not None
        assert roll_pop["_approx"] is True and roll_ip["_approx"] is True and roll_path["_approx"] is True

        live_pop_by, roll_pop_by = {r["pop"]: r for r in live_pop["rows"]}, {r["pop"]: r for r in roll_pop["rows"]}
        assert set(live_pop_by) == set(roll_pop_by)
        for pop, lr in live_pop_by.items():
            rr = roll_pop_by[pop]
            assert (
                rr["requests"] == lr["requests"]
                and abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
                and abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5
            )

        live_ip_by, roll_ip_by = {r["oip"]: r for r in live_ip["rows"]}, {r["oip"]: r for r in roll_ip["rows"]}
        assert set(live_ip_by) == set(roll_ip_by)
        for oip, lr in live_ip_by.items():
            rr = roll_ip_by[oip]
            assert (
                rr["requests"] == lr["requests"]
                and abs(rr["error_pct"] - lr["error_pct"]) < 1e-6
                and abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5
            )

        assert roll_path["shielding_detected"] == live_path["shielding_detected"] is True
        live_path_by, roll_path_by = (
            {r["edge"]: r for r in live_path["rows"]},
            {r["edge"]: r for r in roll_path["rows"]},
        )
        assert set(live_path_by) == set(roll_path_by)
        for edge, lr in live_path_by.items():
            rr = roll_path_by[edge]
            assert (
                rr["requests"] == lr["requests"]
                and abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
                and abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5
            )


def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


def _write_hour_olts(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    float_cols = "ttfb_p50_us ttfb_p95_us ttfb_p99_us ttlb_p50_us ttlb_p95_us ttlb_p99_us".split()
    data = {
        "bucket_ts": pa.array([r["bucket_ts"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "ttfb_count": pa.array([r["ttfb_count"] for r in rows], type=pa.int64()),
        "ttlb_count": pa.array([r.get("ttlb_count", 0) for r in rows], type=pa.int64()),
        **{c: pa.array([r.get(c) for r in rows], type=pa.float64()) for c in float_cols},
    }
    p = os.path.join(d, "origin_latency_ts.parquet")
    pq.write_table(pa.table(data), p)
    return p


def _seed_two_days_olts(cache_root, *, count: int, p95_us: float) -> tuple[str, str, str, str]:
    day_a, day_b = _three_days_ago_iso(), _two_days_ago_iso()
    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_olts(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [
                    {
                        "bucket_ts": _hour_dt(d_iso, h),
                        "ttfb_count": count,
                        "ttfb_p50_us": 1000.0,
                        "ttfb_p95_us": p95_us,
                        "ttfb_p99_us": p95_us + 4000.0,
                        "ttlb_count": count,
                        "ttlb_p50_us": 1100.0,
                        "ttlb_p95_us": p95_us + 100.0,
                        "ttlb_p99_us": p95_us + 4100.0,
                    }
                ],
            )
    st_iso = f"{day_a}T00:00:00+00:00"
    et_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    return day_a, day_b, st_iso, et_iso


def _origin_lat_us(r: dict) -> float | None:
    if r.get("ottfb") is not None:
        return float(r["ottfb"])
    if r.get("ttfb") is not None:
        return float(r["ttfb"]) * 1_000_000.0
    return None


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginLatencyTs:
    def test_build_writes_per_minute_both_bases(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=minute, seconds=i),
                "ottfb": 100_000.0 + i * 1000,
                "ttfb": 0.1,
                "ottlb": 200_000.0 + i * 1000,
            }
            for minute in range(3)
            for i in range(4)
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_olts", rows)
            return c

        with _run_with_build_patches(cache_root, "logs_olts", _fresh_con):
            assert (
                origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-1", {"name": "svc-olts-1"}, [hour_token])
                == 1
            )

        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        assert out.exists()
        cols = set(pq.read_table(str(out)).column_names)
        expected = set(
            "bucket_ts ttfb_count ttfb_p50_us ttfb_p95_us ttfb_p99_us ttlb_count ttlb_p50_us ttlb_p95_us ttlb_p99_us".split()
        )
        assert expected.issubset(cols)
        pylist = pq.read_table(str(out)).to_pylist()
        assert len(pylist) == 3
        for row in pylist:
            assert row["ttfb_count"] == 4 and row["ttlb_count"] == 4
            assert row["ttfb_p50_us"] is not None and row["ttlb_p95_us"] is not None

    def test_build_skips_active_hour(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_olts", [])
            return c

        with _run_with_build_patches(cache_root, "logs_olts", _fresh_con):
            n = origin_latency_ts.build_origin_latency_ts_bundles(
                "svc-olts-active", {"name": "svc-olts-active"}, [active_token]
            )
        assert n == 0 and not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}").exists()

    def test_build_missing_ottlb_writes_ttlb_zero_null(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_noottlb (timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE)")
            for i in range(5):
                c.execute(
                    "INSERT INTO logs_noottlb VALUES (?, ?, ?)",
                    [hour_dt + timedelta(seconds=i), 100_000.0 + i * 500, 0.1],
                )
            return c

        with _run_with_build_patches(cache_root, "logs_noottlb", _fresh_con):
            n = origin_latency_ts.build_origin_latency_ts_bundles(
                "svc-olts-noottlb", {"name": "svc-olts-noottlb"}, [hour_token]
            )
        assert n == 1
        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        assert {"ttlb_count", "ttlb_p50_us", "ttlb_p95_us", "ttlb_p99_us"}.issubset(
            pq.read_table(str(out)).column_names
        )
        pylist = pq.read_table(str(out)).to_pylist()
        assert len(pylist) == 1
        row = pylist[0]
        assert row["ttfb_count"] == 5 and row["ttlb_count"] == 0
        assert row["ttlb_p50_us"] is None and row["ttlb_p95_us"] is None and row["ttlb_p99_us"] is None

    def test_build_no_latency_writes_nothing(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_nolat (timestamp TIMESTAMPTZ, ottlb DOUBLE)")
            c.execute("INSERT INTO logs_nolat VALUES (?, ?)", [hour_dt, 200_000.0])
            return c

        with _run_with_build_patches(cache_root, "logs_nolat", _fresh_con):
            n = origin_latency_ts.build_origin_latency_ts_bundles(
                "svc-olts-nolat", {"name": "svc-olts-nolat"}, [hour_token]
            )
        assert (
            n == 0
            and not (
                cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
            ).exists()
        )

    def test_backfill_skips_built_hours(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        h1_token, _ = _seed_two_bundled_hours(cache_root, "origin_latency_ts.parquet")
        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with (
            patch.object(origin_latency_ts, "build_origin_latency_ts_bundles", side_effect=_stub_build),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        ):
            n = origin_latency_ts.backfill_origin_latency_ts_bundles(
                "svc-olts-bf", {"name": "svc-olts-bf", "service_id": "svc-olts-bf"}
            )
        assert n == 1 and captured == [[h1_token]]

    def test_reader_eligibility_gates(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-olts-elig"})

        def _call(**over):
            kwargs = {
                "has_filters": False,
                "bucket_minutes": 60,
                "metric": "ttfb",
                "percentile": "p95",
                "split_by_leg": False,
                "table_name": "logs_x",
                "where_clause": "1=1",
                "params": [],
                **over,
            }
            return runner.try_origin_latency_ts_from_rollup(
                kwargs.pop("start_time", "2026-06-01T00:00:00Z"),
                kwargs.pop("end_time", "2026-06-15T00:00:00Z"),
                **kwargs,
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            for over in [
                {"has_filters": True},
                {"split_by_leg": True},
                {"bucket_minutes": 1 / 60},
                {"bucket_minutes": 1.5},
                {"end_time": "2026-06-02T12:00:00Z"},
                {"start_time": "not-a-date"},
                {},
                {"metric": "bogus"},
                {"percentile": "p42"},
            ]:
                assert _call(**over) is None

    def test_reader_fail_closed_on_missing_closed_hour(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        day_a, _, st_iso, et_iso = _seed_two_days_olts(cache_root, count=5, p95_us=2000.0)
        missing_hour = f"{day_a}-05"
        os.remove(
            os.path.join(
                str(cache_root), "rollups", "hour_bundled", f"hour={missing_hour}", "origin_latency_ts.parquet"
            )
        )
        field_dir = os.path.join(str(cache_root), "rollups", "hour", "field=ottfb", f"hour={missing_hour}")
        os.makedirs(field_dir, exist_ok=True)
        with open(os.path.join(field_dir, "data.parquet"), "wb") as f:
            f.write(b"x")

        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-olts-failclosed"})
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_latency_ts_from_rollup(
                st_iso,
                et_iso,
                has_filters=False,
                bucket_minutes=60,
                metric="ttfb",
                percentile="p95",
                split_by_leg=False,
                table_name="logs_x",
                where_clause="1=1",
                params=[],
            )
        assert result is None

    def test_reader_serves_closed_hours(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        _, _, st_iso, et_iso = _seed_two_days_olts(cache_root, count=10, p95_us=5000.0)
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-olts-serve"})
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_latency_ts_from_rollup(
                st_iso,
                et_iso,
                has_filters=False,
                bucket_minutes=60,
                metric="ttfb",
                percentile="p95",
                split_by_leg=False,
                table_name="logs_x",
                where_clause="1=1",
                params=[],
            )
        assert result is not None and result["_approx"] is True and result["has_data"] is True
        assert len(result["series"]) == 48
        first = result["series"][0]
        assert first["miss_count"] == 10 and abs(first["value"] - 5.0) < 1e-6

    def test_compact_preserves_minutes(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-olts-cd", "service_id": "svc-olts-cd"}
        day = _yesterday_iso()
        for h in range(24):
            rows = [
                {
                    "bucket_ts": _hour_dt(day, h) + timedelta(minutes=minute),
                    "ttfb_count": 10,
                    "ttfb_p50_us": 1000.0,
                    "ttfb_p95_us": 5000.0,
                    "ttfb_p99_us": 9000.0,
                    "ttlb_count": 8,
                    "ttlb_p50_us": 1100.0,
                    "ttlb_p95_us": 5100.0,
                    "ttlb_p99_us": 9100.0,
                }
                for minute in range(2)
            ]
            _write_hour_olts(str(cache_root), f"{day}-{h:02d}", rows)

        with _compact_patches(cache_root):
            assert rollups.compact_origin_latency_ts_closed_days_to_daily("svc-olts-cd", src) == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_latency_ts.parquet"
        assert day_file.exists()

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            n_rows, n_buckets, ttfb_total, ttlb_total = con.execute(
                f"SELECT count(*), count(DISTINCT bucket_ts), SUM(ttfb_count), SUM(ttlb_count) FROM read_parquet('{day_file}')"
            ).fetchone()
            p95 = con.execute(f"SELECT DISTINCT ttfb_p95_us FROM read_parquet('{day_file}')").fetchall()

        assert n_rows == 48 and n_buckets == 48 and ttfb_total == 480 and ttlb_total == 384 and p95 == [(5000.0,)]

    def test_compact_skips_active_day(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-olts-cd-active", "service_id": "svc-olts-cd-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        for h in range(24):
            _write_hour_olts(
                str(cache_root),
                f"{today}-{h:02d}",
                [
                    {
                        "bucket_ts": _hour_dt(today, h),
                        "ttfb_count": 5,
                        "ttfb_p50_us": 1.0,
                        "ttfb_p95_us": 2.0,
                        "ttfb_p99_us": 3.0,
                        "ttlb_count": 5,
                        "ttlb_p50_us": 1.0,
                        "ttlb_p95_us": 2.0,
                        "ttlb_p99_us": 3.0,
                    }
                ],
            )

        with _compact_patches(cache_root):
            assert rollups.compact_origin_latency_ts_closed_days_to_daily("svc-olts-cd-active", src) == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_accuracy_rollup_reader_matches_live_temp_path(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-olts-acc"}
        base = (datetime.now(UTC) - timedelta(hours=53)).replace(minute=0, second=0, microsecond=0)
        hours = [base + timedelta(hours=i) for i in range(50)]
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=minute, seconds=j),
                "ottfb": 50_000.0 + hi * 500 + minute * 200 + j * 100,
                "ttfb": None,
                "ottlb": 80_000.0 + hi * 500 + minute * 200 + j * 100,
            }
            for hi, hour_dt in enumerate(hours)
            for minute in range(10)
            for j in range(20)
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_acc", rows)
            return c

        hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
        with _run_with_build_patches(cache_root, "logs_acc", _fresh_con):
            assert origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-acc", src, hour_tokens) == 50

        st_iso, et_iso = hours[0].isoformat(), (hours[-1] + timedelta(hours=1)).isoformat()
        runner = QueryRunner(duckdb.connect(":memory:"), src)
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rollup = runner.try_origin_latency_ts_from_rollup(
                st_iso,
                et_iso,
                has_filters=False,
                bucket_minutes=60,
                metric="ttfb",
                percentile="p95",
                split_by_leg=False,
                table_name="logs_acc",
                where_clause="1=1",
                params=[],
            )
        assert rollup is not None and rollup["_approx"] is True
        rollup_by_time = {s["time"]: s for s in rollup["series"]}

        live_con = duckdb.connect(":memory:")
        live_con.execute("CREATE TABLE t_live (timestamp TIMESTAMPTZ, lat_us DOUBLE, ottfb DOUBLE, ottlb DOUBLE)")
        for r in rows:
            live_con.execute(
                "INSERT INTO t_live VALUES (?, ?, ?, ?)",
                [r["timestamp"], _origin_lat_us(r), r.get("ottfb"), r.get("ottlb")],
            )
        live = origin_repo._origin_timeseries_from_temp(
            QueryRunner(live_con, src), "t_live", {"timestamp", "ottfb", "ottlb", "lat_us"}, 60, False, "ttfb", "p95"
        )
        live_by_time = {s["time"]: s for s in live["series"]}

        assert set(rollup_by_time) == set(live_by_time)
        for t, lr in live_by_time.items():
            rr = rollup_by_time[t]
            assert rr["miss_count"] == lr["miss_count"]
            live_v, roll_v = lr["value"], rr["value"]
            assert live_v is not None and roll_v is not None and abs(roll_v - live_v) / live_v < 0.10


class _FailingFirstCopyCon:
    def __init__(self, real):
        self._real = real
        self._copies_seen = 0

    def execute(self, sql, *args, **kwargs):
        if sql.startswith("COPY ("):
            self._copies_seen += 1
            if self._copies_seen == 1:
                raise duckdb.Error("simulated COPY failure")
        return self._real.execute(sql, *args, **kwargs)

    def close(self):
        return self._real.close()

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginSummary:
    def test_build_origin_summary_writes_expected_columns(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)
        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "ottfb": 100_000.0 + i * 100,
                "ttfb": 0.1,
                "ottlb": 200_000.0 + i * 100,
                "elapsed": 300_000.0 + i * 100,
                "ost": 200 if i < 8 else 500,
                "obytes": 1000,
                "cache": "MISS" if i < 5 else "HIT",
            }
            for i in range(10)
        ]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [hour_token]) == 1

        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
        assert bundle.exists()
        t = pq.read_table(str(bundle))
        expected = set(
            "requests total_misses total_passes lat_us_count ottfb_p50_us ottfb_p75_us ottfb_p95_us ottfb_p99_us "
            "ottlb_count ottlb_p50_us ottlb_p95_us cdn_ovh_count cdn_ovh_p50_us ost_5xx_count ost_total_count obytes_count obytes_p50".split()
        )
        assert expected.issubset(t.column_names)
        row = t.to_pylist()[0]
        assert row["requests"] == 10 and row["total_misses"] == 5 and row["ost_5xx_count"] == 2
        assert row["ost_total_count"] == 10 and row["lat_us_count"] == 10

    def test_build_origin_summary_handles_unsigned_latency_columns(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        hour_token, hour_dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE logs_os (timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb UBIGINT, "
            "elapsed UBIGINT, ost INTEGER, obytes BIGINT, cache VARCHAR)"
        )
        con.execute(
            "INSERT INTO logs_os VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [hour_dt, 100.0, 0.1, 294429, 119641, 200, 1000, "MISS"],
        )

        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [hour_token]) == 1

        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
        assert pq.read_table(str(bundle)).to_pylist()[0]["cdn_ovh_p50_us"] == 119641.0 - 294429.0

    def test_build_origin_summary_skips_active_hour(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])
        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [active_token]) == 0

    def test_backfill_origin_summary_walks_existing_bundle_hours(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        h1_token, _ = _seed_two_bundled_hours(cache_root, "origin_summary.parquet")
        written_hours: list[list[str]] = []

        def _spy_build(_sid, _src, hours):
            written_hours.append(list(hours))
            return len(hours)

        with (
            patch("backend.core.rollups.origin_dims.build_origin_summary_bundles", side_effect=_spy_build),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        ):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", {"name": "svc-os"})
        assert n == 1 and written_hours == [[h1_token]]

    def test_try_origin_summary_from_rollup_eligibility_gates(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-os"})
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            for st, et, hf in [
                ("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True),
                ("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False),
                ("not-a-date", "2026-06-15T00:00:00Z", False),
            ]:
                assert (
                    runner.try_origin_summary_from_rollup(st, et, has_filters=hf, actual_cols={"ottfb", "ttfb"}) is None
                )

    def test_try_origin_summary_from_rollup_aggregates_across_hours(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        bundled_root = str(cache_root / "rollups" / "hour_bundled")
        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        for dt in hours:
            hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
            os.makedirs(hour_dir, exist_ok=True)
            write_hour_bundle(
                hour_dir,
                "origin_summary.parquet",
                "(CAST(1000 AS BIGINT), CAST(500 AS BIGINT), CAST(200 AS BIGINT), CAST(1000 AS BIGINT), "
                "50000.0, 100000.0, 200000.0, 500000.0, CAST(900 AS BIGINT), 80000.0, 250000.0, "
                "CAST(850 AS BIGINT), 12000.0, CAST(10 AS BIGINT), CAST(1000 AS BIGINT), CAST(1000 AS BIGINT), 2048.0)",
                "requests, total_misses, total_passes, lat_us_count, ottfb_p50_us, ottfb_p75_us, ottfb_p95_us, "
                "ottfb_p99_us, ottlb_count, ottlb_p50_us, ottlb_p95_us, cdn_ovh_count, cdn_ovh_p50_us, "
                "ost_5xx_count, ost_total_count, obytes_count, obytes_p50",
            )

        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-os"})
        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        actual_cols = {"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"}
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(st_iso, et_iso, has_filters=False, actual_cols=actual_cols)

        assert result is not None and result["_approx"] is True and result["has_data"] is True
        assert result["total_misses"] == 25000 and result["total_passes"] == 10000
        assert abs(result["ottfb_p50_ms"] - 50.0) < 1e-6 and abs(result["ottfb_p95_ms"] - 200.0) < 1e-6
        assert abs(result["ottlb_p50_ms"] - 80.0) < 1e-6 and abs(result["cdn_overhead_p50_ms"] - 12.0) < 1e-6
        assert abs(result["obytes_p50"] - 2048.0) < 1e-6 and abs(result["origin_error_rate"] - 0.01) < 1e-9

    def test_build_origin_summary_empty_hours_returns_zero(self):
        with patch("backend.core.duckdb.get_connection") as mock_conn:
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, []) == 0
        mock_conn.assert_not_called()

    def test_build_origin_summary_skips_malformed_hour_tokens(self, tmp_path, caplog):
        cache_root = _cache_root(tmp_path)
        valid_token, valid_dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(
            con, "logs_os", [{"timestamp": valid_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        )

        with (
            writer_patches(cache_root, "logs_os", con),
            caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"),
        ):
            n = origin_summary.build_origin_summary_bundles(
                "svc-os", {"name": "svc-os"}, ["totally-not-an-hour", valid_token]
            )
        assert n == 1 and any("malformed hour token" in r.message for r in caplog.records)

    def test_build_origin_summary_returns_zero_when_safe_table_missing(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        token, _ = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token]) == 0

    def test_build_origin_summary_returns_zero_when_describe_columns_fails(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        token, _ = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.rollups._common.describe_columns", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token]) == 0

    def test_build_origin_summary_ottfb_only_schema(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ottfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ottfb": 100_000.0 + i * 1000} for i in range(5)],
        )
        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["requests"] == 5 and row["lat_us_count"] == 5
        assert row["ottlb_count"] == 0 and row["cdn_ovh_count"] == 0
        assert row["ost_5xx_count"] == 0 and row["obytes_count"] == 0 and row["total_misses"] == 0

    def test_build_origin_summary_ttfb_only_schema(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ttfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ttfb": 0.05 + i * 0.001} for i in range(5)],
        )
        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["lat_us_count"] == 5 and row["ottfb_p50_us"] >= 49_999.0

    def test_build_origin_summary_no_latency_columns_returns_zero(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(con, "logs_os", ["elapsed"], [{"timestamp": dt, "elapsed": 100_000.0}])
        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token]) == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet").exists()

    def test_build_origin_summary_logs_and_skips_on_copy_failure(self, tmp_path, caplog):
        cache_root = _cache_root(tmp_path)
        h1_token, _ = _past_hour(3)
        h2_token, h2_dt = _past_hour(2)
        real = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(
            real, "logs_os", [{"timestamp": h2_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        )
        con = _FailingFirstCopyCon(real)

        with (
            writer_patches(cache_root, "logs_os", con),
            caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [h1_token, h2_token])

        assert n == 1 and any("COPY failed for hour" in r.message for r in caplog.records)
        h1_dir = cache_root / "rollups" / "hour_bundled" / f"hour={h1_token}"
        if h1_dir.exists():
            assert not any(p.name.startswith(".tmp_os_") for p in h1_dir.iterdir())

    def test_build_origin_summary_logs_on_publish_failure(self, tmp_path, caplog):
        cache_root = _cache_root(tmp_path)
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(
            con, "logs_os", [{"timestamp": dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        )

        with (
            writer_patches(cache_root, "logs_os", con),
            patch("os.replace", side_effect=OSError("simulated rename failure")),
            caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 0 and any("could not publish origin_summary" in r.message for r in caplog.records)

    def test_backfill_returns_zero_when_bundled_root_missing(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            assert origin_summary.backfill_origin_summary_bundles("svc-empty", {"name": "svc-empty"}) == 0

    def test_backfill_skips_non_hour_entries_and_malformed_tokens(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        bundled_root.mkdir(parents=True)
        (bundled_root / "junk_file.txt").write_text("ignore me")
        (bundled_root / "metadata").mkdir()
        (bundled_root / "hour=not-an-hour").mkdir()
        (bundled_root / "hour=not-an-hour" / "all_fields.parquet").write_bytes(b"x")
        valid_token, _ = _past_hour(3)
        (bundled_root / f"hour={valid_token}").mkdir()

        captured: list[list[str]] = []
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch(
                "backend.core.rollups.origin_dims.build_origin_summary_bundles",
                side_effect=lambda _s, _c, h: captured.append(list(h)) or len(h),
            ),
        ):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", {"name": "svc-os"})
        assert n == 0 and captured == []

    def test_backfill_returns_zero_on_listdir_oserror(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        (cache_root / "rollups" / "hour_bundled").mkdir(parents=True)
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("os.listdir", side_effect=OSError("permission denied")),
        ):
            assert origin_summary.backfill_origin_summary_bundles("svc-os", {"name": "svc-os"}) == 0


def _write_hour_summary(cache_root: str, hour: str, row: dict) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    int_cols = "requests total_misses total_passes lat_us_count ottlb_count cdn_ovh_count ost_5xx_count ost_total_count obytes_count".split()
    float_cols = "ottfb_p50_us ottfb_p75_us ottfb_p95_us ottfb_p99_us ottlb_p50_us ottlb_p95_us cdn_ovh_p50_us obytes_p50".split()
    cols_dict = {c: pa.array([row[c]], type=pa.int64()) for c in int_cols}
    cols_dict.update({c: pa.array([row.get(c)], type=pa.float64()) for c in float_cols})
    p = os.path.join(d, "origin_summary.parquet")
    pq.write_table(pa.table(cols_dict), p)
    return p


def _uniform_row(req: int, lat_us: float) -> dict:
    return {
        **dict.fromkeys(
            "requests total_misses lat_us_count ottlb_count cdn_ovh_count ost_total_count obytes_count".split(), req
        ),
        **dict.fromkeys(
            "ottfb_p50_us ottfb_p75_us ottfb_p95_us ottfb_p99_us ottlb_p50_us ottlb_p95_us cdn_ovh_p50_us".split(),
            lat_us,
        ),
        "total_passes": 0,
        "ost_5xx_count": 0,
        "obytes_p50": 1024.0,
    }


def _make_stub_summary_runner(src: dict, table_name: str, row_tuple: tuple):
    captured_paths: list[str] = []

    class _StubResult:
        def fetchone(self):
            return row_tuple

    class _StubConn:
        def execute(self, sql):
            captured_paths.append(sql)
            return _StubResult()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner._table = table_name
    runner.execute = _StubConn().execute  # type: ignore[method-assign]
    return runner, captured_paths


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginSummaryDaily:
    def test_compact_writes_per_day_file_with_correct_aggregates(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-os-day", "service_id": "svc-os-day"}
        day = _yesterday_iso()
        for h in range(24):
            _write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(100, 10000.0))

        with _compact_patches(cache_root):
            assert rollups.compact_origin_summary_closed_days_to_daily("svc-os-day", src) == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_summary.parquet"
        assert day_file.exists()

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            row = con.execute(f"SELECT * FROM read_parquet('{day_file}')").fetchone()
            cols = [d[0] for d in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{day_file}')").fetchall()]
        rec = dict(zip(cols, row, strict=True))
        for k in "requests total_misses lat_us_count ottlb_count cdn_ovh_count ost_total_count obytes_count".split():
            assert rec[k] == 2400
        for k in ("ottfb_p50_us", "ottfb_p75_us", "ottfb_p95_us", "ottfb_p99_us"):
            assert rec[k] == 10000.0

    def test_compact_skips_active_day(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-os-active", "service_id": "svc-os-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        for h in range(24):
            _write_hour_summary(str(cache_root), f"{today}-{h:02d}", _uniform_row(50, 5000.0))

        with _compact_patches(cache_root):
            assert rollups.compact_origin_summary_closed_days_to_daily("svc-os-active", src) == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_compact_is_idempotent_until_inputs_change(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-os-idem", "service_id": "svc-os-idem"}
        day = _yesterday_iso()
        hour_paths = [
            _write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(10, 1000.0)) for h in range(24)
        ]

        with _compact_patches(cache_root):
            assert rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src) == 1
            assert rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src) == 0
            future = (
                os.path.getmtime(cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_summary.parquet") + 5
            )
            os.utime(hour_paths[0], (future, future))
            assert rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src) == 1

    def test_reader_uses_day_file_when_whole_day_in_window(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-os-reader", "service_id": "svc-os-reader"}
        day_a, day_b = _three_days_ago_iso(), _two_days_ago_iso()
        for d in (day_a, day_b):
            for h in range(24):
                _write_hour_summary(str(cache_root), f"{d}-{h:02d}", _uniform_row(100, 20000.0))

        with _compact_patches(cache_root):
            assert day_bundles.compact_origin_summary_closed_days_to_daily("svc-os-reader", src) == 2

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        runner, captured_paths = _make_stub_summary_runner(
            src,
            "logs_svc_os_reader",
            (4800, 4800, 0, 4800, *(20000.0,) * 7, 0.0, 1024.0),
        )
        actual_cols = set("ottfb ttfb ottlb elapsed ost obytes cache".split())
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(st_iso, end_iso, has_filters=False, actual_cols=actual_cols)

        assert result is not None and result.get("_approx") is True and len(captured_paths) == 1
        sql = captured_paths[0]
        assert "day_bundled" in sql and f"day={day_a}" in sql and f"day={day_b}" in sql
        assert sql.count(".parquet") == 2 and "hour_bundled" not in sql

    def test_reader_falls_back_to_hour_when_day_file_missing(self, tmp_path):
        cache_root = _cache_root(tmp_path)
        src = {"name": "svc-os-hour", "service_id": "svc-os-hour"}
        day_a, day_b = _three_days_ago_iso(), _two_days_ago_iso()
        for d in (day_a, day_b):
            for h in range(24):
                _write_hour_summary(str(cache_root), f"{d}-{h:02d}", _uniform_row(100, 30000.0))

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        runner, captured_paths = _make_stub_summary_runner(
            src,
            "logs_svc_os_hour",
            (2400, 2400, 0, 2400, *(30000.0,) * 7, 0.0, 1024.0),
        )
        actual_cols = set("ottfb ttfb ottlb elapsed ost obytes cache".split())
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(st_iso, end_iso, has_filters=False, actual_cols=actual_cols)

        assert result is not None and len(captured_paths) == 1
        sql = captured_paths[0]
        assert "day_bundled" not in sql and sql.count("hour_bundled") >= 1 and sql.count(".parquet") == 48


# ── Merged from tests/core/test_rollups_network_and_perf.py ──


# ── Merged from tests/core/test_rollups_network_rtt.py ──
def _write_hour_rtt(cache_root: str, hour: str, rows: list[dict]) -> str:
    """Write a network_rtt.parquet for one hour with the given rows."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "asn": pa.array([r["asn"] for r in rows], type=pa.int64()),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "rtt_count": pa.array([r["rtt_count"] for r in rows], type=pa.int64()),
            "p95_us": pa.array([r["p95_us"] for r in rows], type=pa.float64()),
            "p99_us": pa.array([r["p99_us"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, "network_rtt.parquet")
    pq.write_table(table, p)
    return p


def _stub_runner_rtt(cache_root_str: str, src: dict, captured_sql: list[str], stub_rows: list[tuple]):
    return make_stub_runner(src, captured_sql, stub_rows, table="logs_svc_nr_reader")


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNetworkRtt:
    def test_backfill_skips_hours_already_built(self, tmp_path):
        """Backfill walks the bundle tree; only hours WITH all_fields.parquet
        AND WITHOUT network_rtt.parquet get queued. Already-built hours
        skip cleanly. Verified without a real duckdb path by stubbing the
        inner writer."""
        from backend.core.rollups import network_rtt

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-bf", "service_id": "svc-nr-bf"}
        yday = _yesterday_iso()

        # Hour A: has all_fields but no network_rtt → SHOULD be queued
        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        # Hour B: has all_fields AND network_rtt → SHOULD be skipped
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_rtt(
            str(cache_root), f"{yday}-11", [{"asn": 1, "requests": 5, "rtt_count": 5, "p95_us": 100.0, "p99_us": 200.0}]
        )
        # Hour C: no all_fields → SHOULD be skipped
        d = cache_root / "rollups" / "hour_bundled" / f"hour={yday}-12"
        d.mkdir(parents=True)

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(network_rtt, "build_network_rtt_bundles", side_effect=_stub_build):
                n = network_rtt.backfill_network_rtt_bundles("svc-nr-bf", src)

        assert n == 1, "exactly one hour eligible"
        assert captured == [[f"{yday}-10"]], f"got {captured}"

    def test_reader_returns_none_when_filtered(self, tmp_path):
        """has_filters=True → None (rollup is unfiltered-only)."""
        src = {"name": "svc-nr-r", "service_id": "svc-nr-r"}
        runner = _stub_runner_rtt(str(tmp_path), src, [], [])
        result = runner.try_network_rtt_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            top_asns=[1],
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        """Window < 48h → None (caller's live path handles short windows fine)."""
        src = {"name": "svc-nr-r2", "service_id": "svc-nr-r2"}
        runner = _stub_runner_rtt(str(tmp_path), src, [], [])
        result = runner.try_network_rtt_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            top_asns=[1],
            has_filters=False,
        )
        assert result is None

    def test_reader_serves_top_asn_dict_from_hour_files(self, tmp_path):
        """Eligible window with hour files → reader returns the dict shape
        the live path produces. Verify WHERE-prunes to caller's top_asns."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-r3", "service_id": "svc-nr-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_rtt(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"asn": 7922, "requests": 100, "rtt_count": 100, "p95_us": 50000.0, "p99_us": 80000.0},
                        {"asn": 15169, "requests": 200, "rtt_count": 200, "p95_us": 30000.0, "p99_us": 50000.0},
                    ],
                )

        captured: list[str] = []
        # Stub returns two rows shaped (asn, p95, p99) per the reader's SELECT.
        stub_rows = [(7922, 50000.0, 80000.0), (15169, 30000.0, 50000.0)]
        runner = _stub_runner_rtt(str(cache_root), src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_network_rtt_from_rollup(
                st_iso,
                end_iso,
                top_asns=[7922, 15169],
                has_filters=False,
            )

        assert result is not None
        assert len(captured) == 1
        sql = captured[0]
        assert "network_rtt.parquet" in sql
        assert "asn IN (?, ?)" in sql, f"WHERE should bind 2 placeholders, got: {sql}"
        assert result[7922]["p95_rtt_us"] == 50000.0
        assert result[7922]["p99_rtt_us"] == 80000.0
        assert result[15169]["p95_rtt_us"] == 30000.0

    def test_compact_writes_per_day_file_with_correct_aggregates(self, tmp_path):
        """24 hour files for a closed day → 1 day file. Per-day row weighted-
        averages the percentiles using rtt_count and SUMs the counts. Schema
        matches the hour file so the reader treats both interchangeably."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-cd", "service_id": "svc-nr-cd"}
        day = _yesterday_iso()

        # Uniform: every hour has the same 2 ASNs with same counts + percentiles.
        for h in range(24):
            _write_hour_rtt(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {"asn": 7922, "requests": 100, "rtt_count": 100, "p95_us": 50000.0, "p99_us": 80000.0},
                    {"asn": 15169, "requests": 50, "rtt_count": 50, "p95_us": 30000.0, "p99_us": 50000.0},
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_rtt_closed_days_to_daily("svc-nr-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "network_rtt.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT asn, requests, rtt_count, p95_us, p99_us FROM read_parquet('{day_file}') ORDER BY asn"
            ).fetchall()
        finally:
            con.close()
        # ASN 7922: 24 * 100 = 2400 reqs, p95 stays 50000 (uniform avg), p99 80000
        # ASN 15169: 24 * 50 = 1200 reqs, p95 30000, p99 50000
        assert rows == [
            (7922, 2400, 2400, 50000.0, 80000.0),
            (15169, 1200, 1200, 30000.0, 50000.0),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-nr-active", "service_id": "svc-nr-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_rtt(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"asn": 1, "requests": 5, "rtt_count": 5, "p95_us": 100.0, "p99_us": 200.0}],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_rtt_closed_days_to_daily("svc-nr-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_network_speed.py ──
def _write_hour_speed(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "asn": pa.array([r["asn"] for r in rows], type=pa.int64()),
            "c_speed": pa.array([r["c_speed"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "network_speed.parquet")
    pq.write_table(table, p)
    return p


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsNetworkSpeed:
    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT network_speed.parquet."""
        from backend.core.rollups import network_speed

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-bf", "service_id": "svc-ns-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_speed(str(cache_root), f"{yday}-11", [{"asn": 1, "c_speed": "fast", "count": 10}])

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(network_speed, "build_network_speed_bundles", side_effect=_stub_build):
                n = network_speed.backfill_network_speed_bundles("svc-ns-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-ns-r", "service_id": "svc-ns-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_network_speed_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            top_asns=[1],
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-ns-r2", "service_id": "svc-ns-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_network_speed_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            top_asns=[1],
            has_filters=False,
        )
        assert result is None

    def test_reader_serves_rows_with_prune_to_top_asns(self, tmp_path):
        """Eligible window → reader builds a read_parquet SQL with the ASN
        list bound, returns (asn, c_speed, count) tuples."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-r3", "service_id": "svc-ns-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_speed(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"asn": 7922, "c_speed": "fast", "count": 50},
                        {"asn": 15169, "c_speed": "slow", "count": 30},
                    ],
                )

        captured: list[str] = []
        stub_rows = [(7922, "fast", 2400), (15169, "slow", 1440)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_network_speed_from_rollup(
                st_iso,
                end_iso,
                top_asns=[7922, 15169],
                has_filters=False,
            )

        assert result == [(7922, "fast", 2400), (15169, "slow", 1440)]
        assert len(captured) == 1
        sql = captured[0]
        assert "network_speed.parquet" in sql
        assert "asn IN (?, ?)" in sql
        assert "GROUP BY asn, c_speed" in sql

    def test_compact_writes_per_day_file_with_correct_sums(self, tmp_path):
        """24 hour files for a closed day → 1 day file. Exact integer SUMs
        per (asn, c_speed); no weighted-average since counts are exact."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-cd", "service_id": "svc-ns-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_speed(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {"asn": 7922, "c_speed": "fast", "count": 100},
                    {"asn": 7922, "c_speed": "slow", "count": 25},
                    {"asn": 15169, "c_speed": "fast", "count": 50},
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "network_speed.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT asn, c_speed, count FROM read_parquet('{day_file}') ORDER BY asn, c_speed"
            ).fetchall()
        finally:
            con.close()
        # 24 hours × the per-hour counts: 7922/fast=2400, 7922/slow=600, 15169/fast=1200
        assert rows == [
            (7922, "fast", 2400),
            (7922, "slow", 600),
            (15169, "fast", 1200),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-active", "service_id": "svc-ns-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_speed(
                str(cache_root),
                f"{today}-{h:02d}",
                [{"asn": 1, "c_speed": "fast", "count": 5}],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_perf_dims.py ──
def _seed_logs_perf_dims(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the perf_dims writer reads and
    INSERT rows. Columns: timestamp, ttl."""
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, ttl BIGINT)")
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [r["timestamp"], r.get("ttl")])


def _write_hour_ttl(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "bucket": pa.array([r["bucket"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
            "min_ttl": pa.array([r["min_ttl"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "perf_ttl_dist.parquet")
    pq.write_table(table, p)
    return p


_LIVE_TTL_DIST_SQL = """
    SELECT
        CASE
            WHEN ttl <= 0 THEN '0s'
            WHEN ttl <= 10 THEN '<10s'
            WHEN ttl <= 30 THEN '<30s'
            WHEN ttl <= 60 THEN '<1m'
            WHEN ttl <= 300 THEN '<5m'
            WHEN ttl <= 600 THEN '<10m'
            WHEN ttl <= 1800 THEN '<30m'
            WHEN ttl <= 3600 THEN '<1h'
            WHEN ttl <= 10800 THEN '<3h'
            WHEN ttl <= 21600 THEN '<6h'
            WHEN ttl <= 43200 THEN '<12h'
            WHEN ttl <= 86400 THEN '<1d'
            WHEN ttl <= 259200 THEN '<3d'
            WHEN ttl <= 604800 THEN '<1w'
            WHEN ttl <= 1209600 THEN '<2w'
            WHEN ttl <= 2592000 THEN '<30d'
            WHEN ttl <= 7776000 THEN '<90d'
            WHEN ttl <= 31536000 THEN '<1y'
            ELSE '>1y'
        END as bucket,
        count(*) as count,
        min(ttl) as min_ttl
    FROM {temp_table}
    WHERE ttl IS NOT NULL
    GROUP BY 1 ORDER BY min_ttl
"""


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPerfDims:
    def test_build_perf_dims_writes_bundle(self, tmp_path):
        """A closed hour with the ttl column produces perf_ttl_dist.parquet with the
        documented schema + exact bucket counts + MIN(ttl)."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-1"}
        hour_token, hour_dt = _past_hour(2)

        # ttl values spanning three buckets: 0s (<=0), <10s (1..10), <30s (11..30).
        rows = [
            {"timestamp": hour_dt + timedelta(minutes=0), "ttl": 0},
            {"timestamp": hour_dt + timedelta(minutes=1), "ttl": -5},
            {"timestamp": hour_dt + timedelta(minutes=2), "ttl": 5},
            {"timestamp": hour_dt + timedelta(minutes=3), "ttl": 8},
            {"timestamp": hour_dt + timedelta(minutes=4), "ttl": 25},
            {"timestamp": hour_dt + timedelta(minutes=5), "ttl": None},  # NULL → excluded
        ]

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_perf_dims(c, "logs_pd", rows)
            return c

        p = _build_patches(cache_root, "logs_pd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-1", src, [hour_token])

        assert n == 1
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        ttl_file = hour_dir / "perf_ttl_dist.parquet"
        assert ttl_file.exists()

        assert {"bucket", "count", "min_ttl"}.issubset(set(pq.read_table(str(ttl_file)).column_names))

        by_bucket = {r["bucket"]: r for r in pq.read_table(str(ttl_file)).to_pylist()}
        # 0s: ttl 0 and -5 (min -5, count 2); <10s: 5,8 (min 5, count 2); <30s: 25.
        assert by_bucket["0s"]["count"] == 2 and by_bucket["0s"]["min_ttl"] == -5
        assert by_bucket["<10s"]["count"] == 2 and by_bucket["<10s"]["min_ttl"] == 5
        assert by_bucket["<30s"]["count"] == 1 and by_bucket["<30s"]["min_ttl"] == 25

    def test_build_perf_dims_skips_active_hour(self, tmp_path):
        """The active UTC hour is still being written; the writer must skip it."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-2"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_perf_dims(c, "logs_pd", [])
            return c

        p = _build_patches(cache_root, "logs_pd", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-2", src, [active_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
        assert not (hour_dir / "perf_ttl_dist.parquet").exists()

    def test_build_perf_dims_missing_ttl_skips_bundle(self, tmp_path):
        """A service whose schema lacks the ttl column produces no bundle."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-3"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute("CREATE TABLE logs_nottl (timestamp TIMESTAMPTZ, other BIGINT)")
            for i in range(4):
                c.execute("INSERT INTO logs_nottl VALUES (?, ?)", [hour_dt + timedelta(minutes=i), i])
            return c

        p = _build_patches(cache_root, "logs_nottl", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = perf_dims.build_perf_dims_bundles("svc-pd-3", src, [hour_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert not (hour_dir / "perf_ttl_dist.parquet").exists()

    def test_backfill_perf_dims_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT perf_ttl_dist.parquet."""
        from backend.core.rollups import perf_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-bf", "service_id": "svc-pd-bf"}
        h1, _ = _past_hour(3)
        h2, _ = _past_hour(4)

        _write_hour_all_fields(str(cache_root), h1)
        _write_hour_all_fields(str(cache_root), h2)
        # h1 already has the ttl_dist bundle — only h2 should be queued.
        _write_hour_ttl(str(cache_root), h1, [{"bucket": "0s", "count": 1, "min_ttl": 0}])

        calls: list[set[str]] = []

        def _spy_bphb(sid, src, hours, *, bundle_filename, **kw):
            hours = list(hours)
            calls.append(set(hours))
            return len(hours)

        with patch("backend.core.rollups.perf_latency.build_per_hour_bundles", side_effect=_spy_bphb):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = perf_dims.backfill_perf_dims_bundles("svc-pd-bf", src)

        assert n == 1
        assert calls == [{h2}]

    def test_compact_perf_dims_day_merge(self, tmp_path):
        """Day-compaction over a full closed UTC day: SUM counts + MIN-of-MIN per
        bucket into one day file."""
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-4"}

        day_dt = (datetime.now(UTC) - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day_dt.strftime("%Y-%m-%d")
        bundled_root = cache_root / "rollups" / "hour_bundled"

        for hh in (3, 4):
            hour_token = (day_dt + timedelta(hours=hh)).strftime("%Y-%m-%d-%H")
            hour_dir = str(bundled_root / f"hour={hour_token}")
            # '0s' appears in both hours (counts 4 + 6, mins -5 + 0); '<10s' only h4.
            _write_hour_bundle(
                hour_dir,
                "perf_ttl_dist.parquet",
                "('0s', CAST(4 AS BIGINT), CAST(-5 AS BIGINT))"
                if hh == 3
                else "('0s', CAST(6 AS BIGINT), CAST(0 AS BIGINT)), ('<10s', CAST(2 AS BIGINT), CAST(5 AS BIGINT))",
                "bucket, count, min_ttl",
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-4", src)

        assert n == 1  # one bundle for the single closed day
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day_str}" / "perf_ttl_dist.parquet"
        assert day_file.exists()

        by_bucket = {r["bucket"]: r for r in pq.read_table(str(day_file)).to_pylist()}
        assert by_bucket["0s"]["count"] == 10 and by_bucket["0s"]["min_ttl"] == -5  # 4+6, MIN(-5, 0)
        assert by_bucket["<10s"]["count"] == 2 and by_bucket["<10s"]["min_ttl"] == 5

    def test_compact_perf_dims_skips_active_day(self, tmp_path):
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_ttl(str(cache_root), f"{today}-{h:02d}", [{"bucket": "0s", "count": 5, "min_ttl": 0}])

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-active", src)

        assert n == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-pd-r", "service_id": "svc-pd-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_ttl_dist_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            has_filters=True,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-pd-r2", "service_id": "svc-pd-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_ttl_dist_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            has_filters=False,
        )
        assert result is None

    def test_reader_returns_none_malformed_timestamps(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r2b", "service_id": "svc-pd-r2b"}
        runner = _stub_runner(src, [], [])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            assert runner.try_perf_ttl_dist_from_rollup("not-a-date", "2026-06-15T00:00:00Z", has_filters=False) is None

    def test_reader_returns_none_no_coverage(self, tmp_path):
        """Eligible window but no rollup files at all → None (fall back to live)."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r2c", "service_id": "svc-pd-r2c"}
        runner = _stub_runner(src, [], [])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_ttl_dist_from_rollup(
                "2026-05-01T00:00:00+00:00",
                "2026-05-30T00:00:00+00:00",
                has_filters=False,
            )
        assert result is None

    def test_reader_serves_rows_sum_per_bucket_min_ttl_order(self, tmp_path):
        """Eligible window → reader builds a read_parquet SQL with SUM(count) per
        bucket ORDER BY MIN(min_ttl), returns [{"bucket","count"}] ordered."""
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-r3", "service_id": "svc-pd-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_ttl(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {"bucket": "0s", "count": 3, "min_ttl": 0},
                        {"bucket": "<10s", "count": 7, "min_ttl": 5},
                    ],
                )

        captured: list[str] = []
        # The stub returns rows already in min_ttl order (0s then <10s).
        stub_rows = [("0s", 144), ("<10s", 336)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_ttl_dist_from_rollup(st_iso, end_iso, has_filters=False)

        assert result == [{"bucket": "0s", "count": 144}, {"bucket": "<10s", "count": 336}]
        assert len(captured) == 1
        sql = captured[0]
        assert "perf_ttl_dist.parquet" in sql
        assert "SUM(count)" in sql
        assert "ORDER BY MIN(min_ttl)" in sql

    def test_rollup_reader_matches_live_sql(self, tmp_path):
        """Seed known rows across a 50-hour closed window; build the perf_dims
        rollup; assert the reader's [{"bucket","count"}] output is byte-equal to the
        live ttl_dist SQL over the SAME rows (one temp table holding all 50 hours)."""
        from backend.core.rollups import perf_dims
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pd-8"}

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]

        def _canonical_rows(hour_dt):
            # ttl values spanning many buckets, including boundary + NULL + huge.
            ttls = [-1, 0, 7, 10, 30, 59, 200, 3600, 86400, 700000, 40000000, None]
            return [{"timestamp": hour_dt + timedelta(seconds=j), "ttl": t} for j, t in enumerate(ttls)]

        for dt in hours:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs_perf_dims(c, "logs_par", rows)
                return c

            p = _build_patches(cache_root, "logs_par", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                perf_dims.build_perf_dims_bundles("svc-pd-8", src, [hour_token])

        # Live SQL over one temp table holding ALL 50 hours of the same rows.
        con = duckdb.connect(":memory:")
        all_rows: list[dict] = []
        for dt in hours:
            all_rows.extend(_canonical_rows(dt))
        _seed_logs_perf_dims(con, "t_par", all_rows)
        runner = QueryRunner(con, src)

        live = con.execute(_LIVE_TTL_DIST_SQL.format(temp_table="t_par")).fetchall()
        live_pairs = [(r[0], int(r[1])) for r in live]  # (bucket, count) in min_ttl order

        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            rolled = runner.try_perf_ttl_dist_from_rollup(st_iso, et_iso, has_filters=False)

        assert rolled is not None
        assert [(r["bucket"], r["count"]) for r in rolled] == live_pairs
        assert "_approx" not in {k for d in rolled for k in d}  # EXACT — no _approx key


# ── Merged from tests/core/test_rollups_perf_latency.py ──
_COLS = ("value", "requests", "elapsed_count", "elapsed_sum", "p50_us", "p95_us", "p99_us")


def _write_hour_perf(cache_root: str, hour: str, rows: list[dict], filename: str = "perf_top_urls.parquet") -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "elapsed_count": pa.array([r["elapsed_count"] for r in rows], type=pa.int64()),
            "elapsed_sum": pa.array([r["elapsed_sum"] for r in rows], type=pa.float64()),
            "p50_us": pa.array([r["p50_us"] for r in rows], type=pa.float64()),
            "p95_us": pa.array([r["p95_us"] for r in rows], type=pa.float64()),
            "p99_us": pa.array([r["p99_us"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, filename)
    pq.write_table(table, p)
    return p


def _row(value: str, requests: int, p50: float, p95: float, p99: float, avg_us: float = 1000.0) -> dict:
    return {
        "value": value,
        "requests": requests,
        "elapsed_count": requests,
        "elapsed_sum": avg_us * requests,
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
    }


def _serve_window(cache_root, filename: str):
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()
    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_perf(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [_row("/a", 50, 1000.0, 2000.0, 3000.0), _row("/b", 30, 500.0, 900.0, 1500.0)],
                filename=filename,
            )
    st_iso = f"{day_a}T00:00:00+00:00"
    end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    return st_iso, end_iso


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPerfLatency:
    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks hour_bundled and queues hours WITH all_fields.parquet
        but WITHOUT perf_top_urls.parquet (the sentinel)."""
        from backend.core.rollups import perf_latency

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-bf", "service_id": "svc-pl-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_perf(str(cache_root), f"{yday}-11", [_row("/a", 10, 1.0, 2.0, 3.0)])

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(perf_latency, "build_perf_latency_bundles", side_effect=_stub_build):
                n = perf_latency.backfill_perf_latency_bundles("svc-pl-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-pl-r", "service_id": "svc-pl-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_latency_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            dimension="url",
            sort_by="p99",
            has_filters=True,
            min_requests=5,
            limit=20,
        )
        assert result is None

    def test_reader_returns_none_for_short_window(self, tmp_path):
        src = {"name": "svc-pl-r2", "service_id": "svc-pl-r2"}
        runner = _stub_runner(src, [], [])
        result = runner.try_perf_latency_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T12:00:00+00:00",
            dimension="url",
            sort_by="p99",
            has_filters=False,
            min_requests=5,
            limit=20,
        )
        assert result is None

    def test_reader_serves_url_rows_weighted_avg(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-r3", "service_id": "svc-pl-r3"}
        st_iso, end_iso = _serve_window(cache_root, "perf_top_urls.parquet")

        captured: list[str] = []
        # (value, requests, avg_us, p50_us_w, p95_us_w, p99_us_w)
        stub_rows = [("/a", 2400, 1000.0, 1000.0, 2000.0, 3000.0)]
        runner = _stub_runner(src, captured, stub_rows)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_latency_from_rollup(
                st_iso,
                end_iso,
                dimension="url",
                sort_by="p99",
                has_filters=False,
                min_requests=5,
                limit=20,
            )

        assert result == {
            "rows": [{"value": "/a", "requests": 2400, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}],
            "_approx": True,
        }
        assert len(captured) == 1
        sql = captured[0]
        assert "perf_top_urls.parquet" in sql
        assert "read_parquet" in sql
        assert "SUM(p99_us * requests)" in sql
        assert "GROUP BY value" in sql
        assert "ORDER BY p99_us_w DESC" in sql  # default sort_by=p99

    def test_reader_asn_dim_and_sort_by_p95(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-r4", "service_id": "svc-pl-r4"}
        st_iso, end_iso = _serve_window(cache_root, "perf_top_asns.parquet")

        captured: list[str] = []
        runner = _stub_runner(src, captured, [("7922", 100, 1000.0, 1.0, 2.0, 3.0)])
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_perf_latency_from_rollup(
                st_iso,
                end_iso,
                dimension="asn",
                sort_by="p95",
                has_filters=False,
                min_requests=10,
                limit=20,
            )
        assert result is not None
        sql = captured[0]
        assert "perf_top_asns.parquet" in sql
        assert "ORDER BY p95_us_w DESC" in sql  # sort_by=p95 honored

    def test_compact_weighted_avg_per_day(self, tmp_path):
        """24 hour files → 1 day file: requests summed, percentiles request-
        weight-averaged. Same value+p95 each hour → day p95 unchanged, requests
        summed."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-cd", "service_id": "svc-pl-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_perf(
                str(cache_root),
                f"{day}-{h:02d}",
                [_row("/a", 10, 100.0, 200.0, 300.0), _row("/b", 5, 50.0, 90.0, 150.0)],
                filename="perf_top_urls.parquet",
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-cd", src)

        assert rebuilt >= 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "perf_top_urls.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT value, requests, p95_us FROM read_parquet('{day_file}') ORDER BY value"
            ).fetchall()
        finally:
            con.close()
        # /a: 24×10 reqs = 240, p95 weighted-avg of identical 200 = 200
        # /b: 24×5 reqs = 120, p95 = 90
        assert rows == [("/a", 240, 200.0), ("/b", 120, 90.0)]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-pl-active", "service_id": "svc-pl-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_perf(str(cache_root), f"{today}-{h:02d}", [_row("/a", 10, 100.0, 200.0, 300.0)])

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Merged from tests/core/test_rollups_pop_health.py ──
def _write_hour_pop_health(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "pop": pa.array([r["pop"] for r in rows]),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "errors": pa.array([r["errors"] for r in rows], type=pa.int64()),
            "cache_hits": pa.array([r["cache_hits"] for r in rows], type=pa.int64()),
            "bandwidth_bytes": pa.array([r["bandwidth_bytes"] for r in rows], type=pa.int64()),
            "p50_rtt_us": pa.array([r["p50_rtt_us"] for r in rows], type=pa.float64()),
            "p95_ttfb_ms": pa.array([r["p95_ttfb_ms"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, "pop_health.parquet")
    pq.write_table(table, p)
    return p


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsPopHealth:
    def test_backfill_skips_built_hours(self, tmp_path):
        from backend.core.rollups import pop_health

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-bf", "service_id": "svc-ns-bf"}
        yday = _yesterday_iso()

        _write_hour_all_fields(str(cache_root), f"{yday}-10")
        _write_hour_all_fields(str(cache_root), f"{yday}-11")
        _write_hour_pop_health(
            str(cache_root),
            f"{yday}-11",
            [
                {
                    "pop": "IAD",
                    "requests": 100,
                    "errors": 10,
                    "cache_hits": 90,
                    "bandwidth_bytes": 1000,
                    "p50_rtt_us": 10.0,
                    "p95_ttfb_ms": 100.0,
                }
            ],
        )

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch.object(pop_health, "build_pop_health_bundles", side_effect=_stub_build):
                n = pop_health.backfill_pop_health_bundles("svc-ns-bf", src)

        assert n == 1
        assert captured == [[f"{yday}-10"]]

    def test_reader_returns_none_when_filtered(self, tmp_path):
        src = {"name": "svc-ns-r", "service_id": "svc-ns-r"}
        runner = _stub_runner(src, [], [])
        result = runner.try_pop_health_from_rollup(
            "2026-05-01T00:00:00+00:00",
            "2026-05-30T00:00:00+00:00",
            has_filters=True,
        )
        assert result is None

    def test_reader_serves_rows_with_prune_to_top_asns(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-r3", "service_id": "svc-ns-r3"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_pop_health(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {
                            "pop": "IAD",
                            "requests": 50,
                            "errors": 0,
                            "cache_hits": 50,
                            "bandwidth_bytes": 500,
                            "p50_rtt_us": 10.0,
                            "p95_ttfb_ms": 100.0,
                        },
                    ],
                )

        captured: list[str] = []
        stub_rows = [("IAD", 2400, 0, 2400, 24000, 10.0, 100.0)]
        runner = _stub_runner(src, captured, stub_rows)

        st_iso = f"{day_a}T00:00:00+00:00"
        end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_pop_health_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
            )

        assert result == stub_rows
        assert len(captured) == 1
        sql = captured[0]
        assert "pop_health.parquet" in sql

    def test_compact_writes_per_day_file_with_correct_sums(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-cd", "service_id": "svc-ns-cd"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_pop_health(
                str(cache_root),
                f"{day}-{h:02d}",
                [
                    {
                        "pop": "IAD",
                        "requests": 100,
                        "errors": 5,
                        "cache_hits": 80,
                        "bandwidth_bytes": 1000,
                        "p50_rtt_us": 10.0,
                        "p95_ttfb_ms": 50.0,
                    },
                    {
                        "pop": "LHR",
                        "requests": 50,
                        "errors": 0,
                        "cache_hits": 45,
                        "bandwidth_bytes": 500,
                        "p50_rtt_us": 20.0,
                        "p95_ttfb_ms": 100.0,
                    },
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_pop_health_closed_days_to_daily("svc-ns-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "pop_health.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                f"SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms FROM read_parquet('{day_file}') ORDER BY pop"
            ).fetchall()
        finally:
            con.close()

        assert rows == [
            ("IAD", 2400, 120, 1920, 24000, 10.0, 50.0),
            ("LHR", 1200, 0, 1080, 12000, 20.0, 100.0),
        ]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-ns-active", "service_id": "svc-ns-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_pop_health(
                str(cache_root),
                f"{today}-{h:02d}",
                [
                    {
                        "pop": "IAD",
                        "requests": 5,
                        "errors": 0,
                        "cache_hits": 5,
                        "bandwidth_bytes": 50,
                        "p50_rtt_us": 10.0,
                        "p95_ttfb_ms": 100.0,
                    }
                ],
            )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.storage.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_pop_health_closed_days_to_daily("svc-ns-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()
