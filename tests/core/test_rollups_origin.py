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
    write_hour_bundle,
    writer_patches,
)
from tests._helpers.rollups import (
    yesterday_iso as _yesterday_iso,
)

_ORIGIN_DIMS_DDL = (
    "timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN, ottfb DOUBLE, ttfb DOUBLE"
)
_OLTS_DDL = "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE"
_SUMMARY_DDL = (
    "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE, "
    "elapsed DOUBLE, ost INTEGER, obytes BIGINT, cache VARCHAR"
)


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
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
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
    wcon = duckdb.connect()
    try:
        tuples = ", ".join(
            f"('{f}', 'h', {'NULL' if v is None else repr(v)}, CAST({c} AS BIGINT))" for f, v, c in field_value_counts
        )
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {tuples}) AS t(field, hour, value, count)) "
            f"TO '{hour_dir}/all_fields.parquet' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def _build_temp_table(con: duckdb.DuckDBPyConnection, temp_table: str, rows: list[dict]) -> None:
    con.execute(
        f"CREATE TABLE {temp_table} ("
        f"  timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN, lat_us DOUBLE"
        f")"
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
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            n = origin_dims.build_origin_dims_bundles("svc-od-1", src, [hour_token])

        assert n == 3
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        pop_file = hour_dir / "origin_pop.parquet"
        ip_file = hour_dir / "origin_ip.parquet"
        path_file = hour_dir / "origin_path.parquet"
        assert pop_file.exists() and ip_file.exists() and path_file.exists()

        pop_cols = set(pq.read_table(str(pop_file)).column_names)
        assert {"pop", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us"}.issubset(pop_cols)

        ip_cols = set(pq.read_table(str(ip_file)).column_names)
        assert {
            "oip",
            "requests",
            "lat_us_count",
            "lat_us_sum",
            "p50_us",
            "p95_us",
            "ost_5xx_count",
            "ost_total_count",
        }.issubset(ip_cols)

        path_cols = set(pq.read_table(str(path_file)).column_names)
        assert {"edge", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us"}.issubset(path_cols)

        ip_rows = {r["oip"]: r for r in pq.read_table(str(ip_file)).to_pylist()}
        assert ip_rows["10.0.0.1"]["ost_5xx_count"] == 2
        assert ip_rows["10.0.0.1"]["ost_total_count"] == 8

    def test_build_origin_dims_skips_active_hour(self, tmp_path):
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_od", [])
            return c

        with _run_with_build_patches(cache_root, "logs_od", _fresh_con):
            n = origin_dims.build_origin_dims_bundles("svc-od-2", {"name": "svc-od-2"}, [active_token])

        assert n == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}" / "origin_pop.parquet").exists()

    def test_build_origin_dims_missing_oip_skips_ip_only(self, tmp_path):
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            n = origin_dims.build_origin_dims_bundles("svc-od-3", {"name": "svc-od-3"}, [hour_token])

        assert n == 2
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "origin_pop.parquet").exists()
        assert (hour_dir / "origin_path.parquet").exists()
        assert not (hour_dir / "origin_ip.parquet").exists()

    def test_build_origin_dims_no_latency_writes_nothing(self, tmp_path):
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        hour_token, _ = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_nolat (timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN)"
            )
            return c

        with _run_with_build_patches(cache_root, "logs_nolat", _fresh_con):
            n = origin_dims.build_origin_dims_bundles("svc-od-4", {"name": "svc-od-4"}, [hour_token])

        assert n == 0

    def test_backfill_origin_dims_walks_existing_bundle_hours(self, tmp_path):
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert result is not None
        assert result["has_data"] is True
        assert "_approx" not in result
        by_status = {r["status"]: r for r in result["rows"]}
        assert by_status[200]["count"] == 500
        assert by_status[404]["count"] == 150
        assert by_status[-1]["count"] == 50
        total = 500 + 150 + 50
        assert abs(by_status[200]["pct"] - (500 * 100.0 / total)) < 1e-6
        assert abs(sum(r["pct"] for r in result["rows"]) - 100.0) < 1e-6

    def test_rollup_readers_match_live_temp_path(self, tmp_path):
        from backend.core.rollups import origin_dims
        from backend.repositories import origin as origin_repo
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            all_rows = [r for dt in hours for r in _canonical_rows(dt)]
            _build_temp_table(con, "t_par", all_rows)
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

        assert roll_pop["_approx"] is True
        live_pop_by = {r["pop"]: r for r in live_pop["rows"]}
        roll_pop_by = {r["pop"]: r for r in roll_pop["rows"]}
        assert set(live_pop_by) == set(roll_pop_by)
        for pop, lr in live_pop_by.items():
            rr = roll_pop_by[pop]
            assert rr["requests"] == lr["requests"]
            assert abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5

        assert roll_ip["_approx"] is True
        live_ip_by = {r["oip"]: r for r in live_ip["rows"]}
        roll_ip_by = {r["oip"]: r for r in roll_ip["rows"]}
        assert set(live_ip_by) == set(roll_ip_by)
        for oip, lr in live_ip_by.items():
            rr = roll_ip_by[oip]
            assert rr["requests"] == lr["requests"]
            assert abs(rr["error_pct"] - lr["error_pct"]) < 1e-6
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5

        assert roll_path["_approx"] is True
        assert roll_path["shielding_detected"] == live_path["shielding_detected"] is True
        live_path_by = {r["edge"]: r for r in live_path["rows"]}
        roll_path_by = {r["edge"]: r for r in roll_path["rows"]}
        assert set(live_path_by) == set(roll_path_by)
        for edge, lr in live_path_by.items():
            rr = roll_path_by[edge]
            assert rr["requests"] == lr["requests"]
            assert abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5


# ── Merged from tests/core/test_rollups_origin_latency_ts.py ──
def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


def _write_hour_olts(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "bucket_ts": pa.array([r["bucket_ts"] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "ttfb_count": pa.array([r["ttfb_count"] for r in rows], type=pa.int64()),
            "ttfb_p50_us": pa.array([r.get("ttfb_p50_us") for r in rows], type=pa.float64()),
            "ttfb_p95_us": pa.array([r.get("ttfb_p95_us") for r in rows], type=pa.float64()),
            "ttfb_p99_us": pa.array([r.get("ttfb_p99_us") for r in rows], type=pa.float64()),
            "ttlb_count": pa.array([r.get("ttlb_count", 0) for r in rows], type=pa.int64()),
            "ttlb_p50_us": pa.array([r.get("ttlb_p50_us") for r in rows], type=pa.float64()),
            "ttlb_p95_us": pa.array([r.get("ttlb_p95_us") for r in rows], type=pa.float64()),
            "ttlb_p99_us": pa.array([r.get("ttlb_p99_us") for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, "origin_latency_ts.parquet")
    pq.write_table(table, p)
    return p


def _seed_two_days_olts(cache_root, *, count: int, p95_us: float) -> tuple[str, str, str, str]:
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()
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
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-1", {"name": "svc-olts-1"}, [hour_token])

        assert n == 1
        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        assert out.exists()
        cols = set(pq.read_table(str(out)).column_names)
        assert {
            "bucket_ts",
            "ttfb_count",
            "ttfb_p50_us",
            "ttfb_p95_us",
            "ttfb_p99_us",
            "ttlb_count",
            "ttlb_p50_us",
            "ttlb_p95_us",
            "ttlb_p99_us",
        }.issubset(cols)

        pylist = pq.read_table(str(out)).to_pylist()
        assert len(pylist) == 3
        for row in pylist:
            assert row["ttfb_count"] == 4
            assert row["ttlb_count"] == 4
            assert row["ttfb_p50_us"] is not None
            assert row["ttlb_p95_us"] is not None

    def test_build_skips_active_hour(self, tmp_path):
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_olts", [])
            return c

        with _run_with_build_patches(cache_root, "logs_olts", _fresh_con):
            n = origin_latency_ts.build_origin_latency_ts_bundles(
                "svc-olts-active", {"name": "svc-olts-active"}, [active_token]
            )

        assert n == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}").exists()

    def test_build_missing_ottlb_writes_ttlb_zero_null(self, tmp_path):
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        cols = set(pq.read_table(str(out)).column_names)
        assert {"ttlb_count", "ttlb_p50_us", "ttlb_p95_us", "ttlb_p99_us"}.issubset(cols)
        pylist = pq.read_table(str(out)).to_pylist()
        assert len(pylist) == 1
        row = pylist[0]
        assert row["ttfb_count"] == 5
        assert row["ttlb_count"] == 0
        assert row["ttlb_p50_us"] is None and row["ttlb_p95_us"] is None and row["ttlb_p99_us"] is None

    def test_build_no_latency_writes_nothing(self, tmp_path):
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert n == 0
        assert not (
            cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        ).exists()

    def test_backfill_skips_built_hours(self, tmp_path):
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert n == 1
        assert captured == [[h1_token]]

    def test_reader_eligibility_gates(self, tmp_path):
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        runner = QueryRunner(duckdb.connect(":memory:"), {"name": "svc-olts-elig"})

        def _call(**over):
            kwargs = dict(
                has_filters=False,
                bucket_minutes=60,
                metric="ttfb",
                percentile="p95",
                split_by_leg=False,
                table_name="logs_x",
                where_clause="1=1",
                params=[],
            )
            kwargs.update(over)
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
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert result is not None
        assert result["_approx"] is True and result["has_data"] is True
        assert len(result["series"]) == 48
        first = result["series"][0]
        assert first["miss_count"] == 10
        assert abs(first["value"] - 5.0) < 1e-6

    def test_compact_preserves_minutes(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            rebuilt = rollups.compact_origin_latency_ts_closed_days_to_daily("svc-olts-cd", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_latency_ts.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            n_rows, n_buckets, ttfb_total, ttlb_total = con.execute(
                f"SELECT count(*), count(DISTINCT bucket_ts), "
                f"       SUM(ttfb_count), SUM(ttlb_count) "
                f"FROM read_parquet('{day_file}')"
            ).fetchone()
            p95 = con.execute(f"SELECT DISTINCT ttfb_p95_us FROM read_parquet('{day_file}')").fetchall()
        finally:
            con.close()

        assert n_rows == 48 and n_buckets == 48
        assert ttfb_total == 48 * 10 and ttlb_total == 48 * 8
        assert p95 == [(5000.0,)]

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            rebuilt = rollups.compact_origin_latency_ts_closed_days_to_daily("svc-olts-cd-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_accuracy_rollup_reader_matches_live_temp_path(self, tmp_path):
        from backend.core.rollups import origin_latency_ts
        from backend.repositories._base import QueryRunner
        from backend.repositories.origin import _origin_timeseries_from_temp

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-acc"}

        base = (datetime.now(UTC) - timedelta(hours=53)).replace(minute=0, second=0, microsecond=0)
        hours = [base + timedelta(hours=i) for i in range(50)]

        rows: list[dict] = [
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
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-acc", src, hour_tokens)
        assert n == 50

        st_iso = hours[0].isoformat()
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat()

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
        live = _origin_timeseries_from_temp(
            QueryRunner(live_con, src), "t_live", {"timestamp", "ottfb", "ottlb", "lat_us"}, 60, False, "ttfb", "p95"
        )
        live_by_time = {s["time"]: s for s in live["series"]}

        assert set(rollup_by_time) == set(live_by_time)
        for t, lr in live_by_time.items():
            rr = rollup_by_time[t]
            assert rr["miss_count"] == lr["miss_count"]
            live_v, roll_v = lr["value"], rr["value"]
            assert live_v is not None and roll_v is not None
            rel = abs(roll_v - live_v) / live_v
            assert rel < 0.10, f"bucket {t}: rollup p95 {roll_v} vs live {live_v} ({rel:.2%})"


# ── Merged from tests/core/test_rollups_origin_summary.py ──
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
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [hour_token])

        assert n == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
        assert bundle.exists()

        t = pq.read_table(str(bundle))
        cols = set(t.column_names)
        expected = {
            "requests",
            "total_misses",
            "total_passes",
            "lat_us_count",
            "ottfb_p50_us",
            "ottfb_p75_us",
            "ottfb_p95_us",
            "ottfb_p99_us",
            "ottlb_count",
            "ottlb_p50_us",
            "ottlb_p95_us",
            "cdn_ovh_count",
            "cdn_ovh_p50_us",
            "ost_5xx_count",
            "ost_total_count",
            "obytes_count",
            "obytes_p50",
        }
        assert expected.issubset(cols), f"missing required columns; got: {cols}"

        row = t.to_pylist()[0]
        assert row["requests"] == 10
        assert row["total_misses"] == 5
        assert row["ost_5xx_count"] == 2
        assert row["ost_total_count"] == 10
        assert row["lat_us_count"] == 10

    def test_build_origin_summary_handles_unsigned_latency_columns(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        hour_token, hour_dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE logs_os ("
            "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb UBIGINT,"
            "elapsed UBIGINT, ost INTEGER, obytes BIGINT, cache VARCHAR)"
        )
        con.execute(
            "INSERT INTO logs_os VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [hour_dt, 100.0, 0.1, 294429, 119641, 200, 1000, "MISS"],
        )

        with writer_patches(cache_root, "logs_os", con):
            assert origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [hour_token]) == 1

        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["cdn_ovh_p50_us"] == 119641.0 - 294429.0

    def test_build_origin_summary_skips_active_hour(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [active_token])
        assert n == 0

    def test_backfill_origin_summary_walks_existing_bundle_hours(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert n == 1
        assert written_hours == [[h1_token]]

    def test_try_origin_summary_from_rollup_eligibility_gates(self, tmp_path):
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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

        assert result is not None
        assert result["_approx"] is True and result["has_data"] is True
        assert result["total_misses"] == 500 * 50
        assert result["total_passes"] == 200 * 50
        assert abs(result["ottfb_p50_ms"] - 50.0) < 1e-6
        assert abs(result["ottfb_p95_ms"] - 200.0) < 1e-6
        assert abs(result["ottlb_p50_ms"] - 80.0) < 1e-6
        assert abs(result["cdn_overhead_p50_ms"] - 12.0) < 1e-6
        assert abs(result["obytes_p50"] - 2048.0) < 1e-6
        assert abs(result["origin_error_rate"] - 0.01) < 1e-9

    def test_build_origin_summary_empty_hours_returns_zero(self):
        from backend.core.rollups import origin_summary

        with patch("backend.core.duckdb.get_connection") as mock_conn:
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [])
        assert n == 0
        mock_conn.assert_not_called()

    def test_build_origin_summary_skips_malformed_hour_tokens(self, tmp_path, caplog):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        valid_token, valid_dt = _past_hour(2)

        rows = [{"timestamp": valid_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with writer_patches(cache_root, "logs_os", con):
            with caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"):
                n = origin_summary.build_origin_summary_bundles(
                    "svc-os", {"name": "svc-os"}, ["totally-not-an-hour", valid_token]
                )

        assert n == 1
        assert any("malformed hour token" in r.message for r in caplog.records)

    def test_build_origin_summary_returns_zero_when_safe_table_missing(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, _ = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 0

    def test_build_origin_summary_returns_zero_when_describe_columns_fails(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, _ = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.rollups._common.describe_columns", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 0

    def test_build_origin_summary_ottfb_only_schema(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ottfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ottfb": 100_000.0 + i * 1000} for i in range(5)],
        )

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["requests"] == 5 and row["lat_us_count"] == 5
        assert row["ottlb_count"] == 0 and row["cdn_ovh_count"] == 0
        assert row["ost_5xx_count"] == 0 and row["obytes_count"] == 0 and row["total_misses"] == 0

    def test_build_origin_summary_ttfb_only_schema(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ttfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ttfb": 0.05 + i * 0.001} for i in range(5)],
        )

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["lat_us_count"] == 5
        assert row["ottfb_p50_us"] >= 49_999.0

    def test_build_origin_summary_no_latency_columns_returns_zero(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, dt = _past_hour(2)
        con = duckdb.connect(":memory:")
        _seed_logs_partial(con, "logs_os", ["elapsed"], [{"timestamp": dt, "elapsed": 100_000.0}])

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])
        assert n == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet").exists()

    def test_build_origin_summary_logs_and_skips_on_copy_failure(self, tmp_path, caplog):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        h1_token, _ = _past_hour(3)
        h2_token, h2_dt = _past_hour(2)

        rows = [{"timestamp": h2_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        real = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(real, "logs_os", rows)
        con = _FailingFirstCopyCon(real)

        with writer_patches(cache_root, "logs_os", con):
            with caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"):
                n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [h1_token, h2_token])

        assert n == 1
        assert any("COPY failed for hour" in r.message for r in caplog.records)
        h1_dir = cache_root / "rollups" / "hour_bundled" / f"hour={h1_token}"
        if h1_dir.exists():
            assert not any(p.name.startswith(".tmp_os_") for p in h1_dir.iterdir())

    def test_build_origin_summary_logs_on_publish_failure(self, tmp_path, caplog):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        token, dt = _past_hour(2)

        rows = [{"timestamp": dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with (
            writer_patches(cache_root, "logs_os", con),
            patch("os.replace", side_effect=OSError("simulated rename failure")),
            caplog.at_level(logging.WARNING, logger="backend.core.rollups.origin_dims"),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [token])

        assert n == 0
        assert any("could not publish origin_summary" in r.message for r in caplog.records)

    def test_backfill_returns_zero_when_bundled_root_missing(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            assert origin_summary.backfill_origin_summary_bundles("svc-empty", {"name": "svc-empty"}) == 0

    def test_backfill_skips_non_hour_entries_and_malformed_tokens(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        bundled_root = cache_root / "rollups" / "hour_bundled"
        bundled_root.mkdir(parents=True)

        (bundled_root / "junk_file.txt").write_text("ignore me")
        (bundled_root / "metadata").mkdir()
        (bundled_root / "hour=not-an-hour").mkdir()
        (bundled_root / "hour=not-an-hour" / "all_fields.parquet").write_bytes(b"x")
        valid_token, _ = _past_hour(3)
        (bundled_root / f"hour={valid_token}").mkdir()

        captured: list[list[str]] = []

        def _spy_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups.origin_dims.build_origin_summary_bundles", side_effect=_spy_build),
        ):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", {"name": "svc-os"})
        assert n == 0 and captured == []

    def test_backfill_returns_zero_on_listdir_oserror(self, tmp_path):
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        (cache_root / "rollups" / "hour_bundled").mkdir(parents=True)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("os.listdir", side_effect=OSError("permission denied")),
        ):
            assert origin_summary.backfill_origin_summary_bundles("svc-os", {"name": "svc-os"}) == 0


# ── Merged from tests/core/test_rollups_origin_summary_daily.py ──
def _write_hour_summary(cache_root: str, hour: str, row: dict) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    int_cols = [
        "requests",
        "total_misses",
        "total_passes",
        "lat_us_count",
        "ottlb_count",
        "cdn_ovh_count",
        "ost_5xx_count",
        "ost_total_count",
        "obytes_count",
    ]
    float_cols = [
        "ottfb_p50_us",
        "ottfb_p75_us",
        "ottfb_p95_us",
        "ottfb_p99_us",
        "ottlb_p50_us",
        "ottlb_p95_us",
        "cdn_ovh_p50_us",
        "obytes_p50",
    ]
    cols_dict = {c: pa.array([row[c]], type=pa.int64()) for c in int_cols}
    cols_dict.update({c: pa.array([row.get(c)], type=pa.float64()) for c in float_cols})
    p = os.path.join(d, "origin_summary.parquet")
    pq.write_table(pa.table(cols_dict), p)
    return p


def _uniform_row(req: int, lat_us: float) -> dict:
    return {
        "requests": req,
        "total_misses": req,
        "total_passes": 0,
        "lat_us_count": req,
        "ottfb_p50_us": lat_us,
        "ottfb_p75_us": lat_us,
        "ottfb_p95_us": lat_us,
        "ottfb_p99_us": lat_us,
        "ottlb_count": req,
        "ottlb_p50_us": lat_us,
        "ottlb_p95_us": lat_us,
        "cdn_ovh_count": req,
        "cdn_ovh_p50_us": lat_us,
        "ost_5xx_count": 0,
        "ost_total_count": req,
        "obytes_count": req,
        "obytes_p50": 1024.0,
    }


def _make_stub_summary_runner(src: dict, table_name: str, row_tuple: tuple):
    from backend.repositories._base import QueryRunner

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
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-day", "service_id": "svc-os-day"}
        day = _yesterday_iso()

        for h in range(24):
            _write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(100, 10000.0))

        with _compact_patches(cache_root):
            rebuilt = rollups.compact_origin_summary_closed_days_to_daily("svc-os-day", src)

        assert rebuilt == 1
        day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_summary.parquet"
        assert day_file.exists()

        con = duckdb.connect(":memory:")
        try:
            row = con.execute(f"SELECT * FROM read_parquet('{day_file}')").fetchone()
            cols = [d[0] for d in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{day_file}')").fetchall()]
        finally:
            con.close()
        rec = dict(zip(cols, row, strict=True))
        for k in (
            "requests",
            "total_misses",
            "lat_us_count",
            "ottlb_count",
            "cdn_ovh_count",
            "ost_total_count",
            "obytes_count",
        ):
            assert rec[k] == 2400
        for k in ("ottfb_p50_us", "ottfb_p75_us", "ottfb_p95_us", "ottfb_p99_us"):
            assert rec[k] == 10000.0

    def test_compact_skips_active_day(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-active", "service_id": "svc-os-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_summary(str(cache_root), f"{today}-{h:02d}", _uniform_row(50, 5000.0))

        with _compact_patches(cache_root):
            rebuilt = rollups.compact_origin_summary_closed_days_to_daily("svc-os-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_compact_is_idempotent_until_inputs_change(self, tmp_path):
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
        from backend.core.rollups import day_bundles

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            (4800, 4800, 0, 4800, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 0.0, 1024.0),
        )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
                actual_cols={"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"},
            )

        assert result is not None and result.get("_approx") is True
        assert len(captured_paths) == 1
        sql = captured_paths[0]
        assert "day_bundled" in sql and f"day={day_a}" in sql and f"day={day_b}" in sql
        assert sql.count(".parquet") == 2 and "hour_bundled" not in sql

    def test_reader_falls_back_to_hour_when_day_file_missing(self, tmp_path):
        cache_root = tmp_path / "cache"
        cache_root.mkdir()
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
            (2400, 2400, 0, 2400, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 0.0, 1024.0),
        )

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
                actual_cols={"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"},
            )

        assert result is not None and len(captured_paths) == 1
        sql = captured_paths[0]
        assert "day_bundled" not in sql and sql.count("hour_bundled") >= 1 and sql.count(".parquet") == 48
