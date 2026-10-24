"""Tests for the origin_dims (pop / oip / edge) per-hour bundle writers, their
backfill driver, and the four /origin readers
(try_origin_pop_latency / ip_health / path_breakdown / status_from_rollup).

Mirrors test_rollups_slow_urls.py in structure — same patch pattern, same
_noop_lock + _past_hour helpers — so the writers stay legible side-by-side.

origin_dims emits THREE files per closed hour from one module:
  origin_pop.parquet   (key=pop,  top-K by requests)
  origin_ip.parquet    (key=oip,  per-hour floor + carried 5xx/total counts)
  origin_path.parquet  (key=edge, 2 rows/hr, no top-K)

The status_codes reader is special: it has NO dedicated writer — it reads
field='ost' out of the existing all_fields.parquet bundle and is exact.
"""

from __future__ import annotations

import contextlib
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests._helpers.dates import past_hour as _past_hour
from tests._helpers.dates import three_days_ago_iso as _three_days_ago_iso
from tests._helpers.dates import two_days_ago_iso as _two_days_ago_iso
from tests._helpers.dates import yesterday_iso as _yesterday_iso
from tests._helpers.locks import noop_lock as _noop_lock
from tests._helpers.rollups import build_writer_patches as _build_patches
from tests._helpers.rollups import write_hour_bundle, writer_patches


# ── Merged from tests/core/test_rollups_origin_dims.py ──
def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the origin_dims writers read and
    INSERT rows. Columns: timestamp, pop, oip, ost, edge, ottfb, ttfb."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, "
        f"  edge BOOLEAN, ottfb DOUBLE, ttfb DOUBLE"
        f")"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("pop"),
                r.get("oip"),
                r.get("ost"),
                r.get("edge"),
                r.get("ottfb"),
                r.get("ttfb"),
            ],
        )


def _write_all_fields(hour_dir: str, field_value_counts: list[tuple[str, str | None, int]]) -> None:
    """Write a minimal all_fields.parquet (schema: field, hour, value, count)."""
    os.makedirs(hour_dir, exist_ok=True)
    wcon = duckdb.connect()
    try:
        # VALUES tuples — value may be NULL.
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
    """Materialize the per-request temp table the live origin path reads from
    (timestamp + the dimension cols + a precomputed lat_us column)."""
    con.execute(
        f"CREATE TABLE {temp_table} ("
        f"  timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN, lat_us DOUBLE"
        f")"
    )
    for r in rows:
        ottfb = r.get("ottfb")
        ttfb = r.get("ttfb")
        lat_us = ottfb if ottfb is not None else (ttfb * 1_000_000.0 if ttfb is not None else None)
        con.execute(
            f"INSERT INTO {temp_table} VALUES (?, ?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("pop"), r.get("oip"), r.get("ost"), r.get("edge"), lat_us],
        )


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginDims:
    def test_build_origin_dims_writes_three_bundles(self, tmp_path):
        """A closed hour with pop/oip/edge rows produces all three parquet files
        with their documented schemas."""
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-1"}
        hour_token, hour_dt = _past_hour(2)

        rows = []
        # Two POPs; one IP that clears the per-hour floor (>= 5), edge true+false.
        for i in range(8):
            rows.append(
                {
                    "timestamp": hour_dt + timedelta(minutes=i),
                    "pop": "DEN" if i % 2 == 0 else "IAD",
                    "oip": "10.0.0.1",
                    "ost": 200 if i < 6 else 500,
                    "edge": i % 2 == 0,
                    "ottfb": 100_000.0 + i * 1000,
                    "ttfb": 0.1,
                }
            )

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_od", rows)
            return c

        p = _build_patches(cache_root, "logs_od", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
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

        # ip bundle's carried 5xx/total counts are exact: 2 of 8 are 5xx.
        ip_rows = {r["oip"]: r for r in pq.read_table(str(ip_file)).to_pylist()}
        assert ip_rows["10.0.0.1"]["ost_5xx_count"] == 2
        assert ip_rows["10.0.0.1"]["ost_total_count"] == 8

    def test_build_origin_dims_skips_active_hour(self, tmp_path):
        """The active UTC hour is still being written; the writer must skip it."""
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-2"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs(c, "logs_od", [])
            return c

        p = _build_patches(cache_root, "logs_od", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_dims.build_origin_dims_bundles("svc-od-2", src, [active_token])

        assert n == 0
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
        assert not (hour_dir / "origin_pop.parquet").exists()

    def test_build_origin_dims_missing_oip_skips_ip_only(self, tmp_path):
        """A service with no `oip` column still gets pop + path bundles; only the
        ip bundle is skipped."""
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-3"}
        hour_token, hour_dt = _past_hour(2)

        # Table WITHOUT oip (and without ost — the ip bundle needs both).
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

        p = _build_patches(cache_root, "logs_noip", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_dims.build_origin_dims_bundles("svc-od-3", src, [hour_token])

        # pop + path written, ip skipped → 2
        assert n == 2
        hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}"
        assert (hour_dir / "origin_pop.parquet").exists()
        assert (hour_dir / "origin_path.parquet").exists()
        assert not (hour_dir / "origin_ip.parquet").exists()

    def test_build_origin_dims_no_latency_writes_nothing(self, tmp_path):
        """A service with no latency column (no ottfb/ttfb) has no percentile to
        compute — none of the three bundles are written."""
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-4"}
        hour_token, _ = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            c.execute(
                "CREATE TABLE logs_nolat (timestamp TIMESTAMPTZ, pop VARCHAR, oip VARCHAR, ost INTEGER, edge BOOLEAN)"
            )
            return c

        p = _build_patches(cache_root, "logs_nolat", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_dims.build_origin_dims_bundles("svc-od-4", src, [hour_token])

        assert n == 0

    def test_backfill_origin_dims_walks_existing_bundle_hours(self, tmp_path):
        """The self-heal driver checks each of the 3 bundles (origin_pop,
        origin_ip, origin_path) independently and builds only the missing ones."""
        from backend.core.rollups import origin_dims

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-5"}

        h1_token, _ = _past_hour(3)
        h2_token, _ = _past_hour(4)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        (bundled_root / f"hour={h1_token}").mkdir(parents=True)
        (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")
        (bundled_root / f"hour={h2_token}").mkdir(parents=True)
        (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
        # h2 already has origin_pop.parquet — backfill should skip pop for h2
        # while still healing ip and path for both h1 and h2.
        (bundled_root / f"hour={h2_token}" / "origin_pop.parquet").write_bytes(b"x")

        written_by_bundle: dict[str, list[str]] = {}

        def _spy_per_hour(_sid, _src, hours, *, bundle_filename, **_kw):
            written_by_bundle[bundle_filename] = list(hours)
            return len(hours)

        with patch("backend.core.rollups._common.build_per_hour_bundles", side_effect=_spy_per_hour):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = origin_dims.backfill_origin_dims_bundles("svc-od-5", src)

        assert written_by_bundle == {
            "origin_pop.parquet": [h1_token],
            "origin_ip.parquet": sorted([h1_token, h2_token]),
            "origin_path.parquet": sorted([h1_token, h2_token]),
        }
        assert n == 5

    def test_readers_eligibility_gates(self, tmp_path):
        """All four readers return None for: filters present, window too short,
        missing closed-hour rollup file, malformed timestamps."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-6"}
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, src)

            readers = [
                lambda s, e, hf: runner.try_origin_pop_latency_from_rollup(s, e, has_filters=hf, limit=30),
                lambda s, e, hf: runner.try_origin_ip_health_from_rollup(s, e, has_filters=hf, limit=30),
                lambda s, e, hf: runner.try_origin_path_breakdown_from_rollup(s, e, has_filters=hf),
                lambda s, e, hf: runner.try_origin_status_from_rollup(s, e, has_filters=hf),
            ]

            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                for call in readers:
                    # has_filters → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", True) is None
                    # window < 48h → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-02T12:00:00Z", False) is None
                    # empty bundle dir → None
                    assert call("2026-06-01T00:00:00Z", "2026-06-15T00:00:00Z", False) is None
                    # malformed timestamps → None
                    assert call("not-a-date", "2026-06-15T00:00:00Z", False) is None

    def test_try_origin_status_from_rollup_reads_all_fields(self, tmp_path):
        """End-to-end: build a 50-hour window of all_fields.parquet carrying
        field='ost' rows, then verify the status reader returns exact counts + pct
        and normalizes out-of-range codes to -1, dropping the NULL value row."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-7"}
        bundled_root = str(cache_root / "rollups" / "hour_bundled")

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        for dt in hours:
            hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
            # Per-hour: 200×10, 404×3, 829×1 (→ -1 bucket), NULL×2 (dropped),
            # plus a non-ost field that must be ignored.
            _write_all_fields(
                hour_dir,
                [
                    ("ost", "200", 10),
                    ("ost", "404", 3),
                    ("ost", "829", 1),
                    ("ost", None, 2),
                    ("url", "/x", 99),
                ],
            )

        with contextlib.closing(duckdb.connect(":memory:")) as con:
            runner = QueryRunner(con, src)
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                result = runner.try_origin_status_from_rollup(st_iso, et_iso, has_filters=False)

        assert result is not None, "status reader returned None — eligibility gate fired unexpectedly"
        assert result["has_data"] is True
        assert "_approx" not in result  # status is EXACT
        by_status = {r["status"]: r for r in result["rows"]}
        # 200: 10×50 = 500; 404: 3×50 = 150; -1 (829): 1×50 = 50. NULL dropped.
        assert by_status[200]["count"] == 500
        assert by_status[404]["count"] == 150
        assert by_status[-1]["count"] == 50
        total = 500 + 150 + 50
        assert abs(by_status[200]["pct"] - (500 * 100.0 / total)) < 1e-6
        assert abs(sum(r["pct"] for r in result["rows"]) - 100.0) < 1e-6

    def test_rollup_readers_match_live_temp_path(self, tmp_path):
        """Seed known rows; build the origin_dims rollups across a 50-hour window;
        assert each rollup reader's output matches the live temp-path helper.

        Each hour gets IDENTICAL rows, so the request-weighted cross-hour average
        of the per-hour percentiles equals the per-hour percentile, which (for a
        single hour) is exactly what the live temp path computes over that hour's
        rows. error_pct + status counts are exact.
        """
        from backend.core.rollups import origin_dims
        from backend.repositories import origin as origin_repo
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-od-8"}

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]

        # One canonical set of per-hour rows (replicated each hour). Designed so:
        #  - pop has 2 POPs with distinct latency profiles
        #  - oip has 2 IPs, both clearing the per-hour floor (>=5) and the
        #    window-level HAVING (>=10 over 50 hours); one with 5xx errors
        #  - edge has true + false rows (shielding_detected → true)
        def _canonical_rows(hour_dt):
            rows = []
            for i in range(10):
                rows.append(
                    {
                        "timestamp": hour_dt + timedelta(seconds=i),
                        "pop": "DEN",
                        "oip": "10.0.0.1",
                        "ost": 200 if i < 8 else 500,
                        "edge": True,
                        "ottfb": 100_000.0 + i * 1000,
                        "ttfb": 0.1,
                    }
                )
            for i in range(10):
                rows.append(
                    {
                        "timestamp": hour_dt + timedelta(seconds=30 + i),
                        "pop": "IAD",
                        "oip": "10.0.0.2",
                        "ost": 200,
                        "edge": False,
                        "ottfb": 500_000.0 + i * 1000,
                        "ttfb": 0.5,
                    }
                )
            return rows

        # Build the rollups: seed each closed hour with the canonical rows and run
        # the writer once per hour (the writer's window is [hour, hour+1)). The
        # factory hands each of the 3 per-bundle driver passes its own fresh
        # connection (the driver closes the conn it's given).
        for dt in hours:
            hour_token = dt.strftime("%Y-%m-%d-%H")
            canonical = _canonical_rows(dt)

            def _fresh_con(rows=canonical):
                c = duckdb.connect(":memory:")
                _seed_logs(c, "logs_par", rows)
                return c

            p = _build_patches(cache_root, "logs_par", _fresh_con)
            with p[0], p[1], p[2], p[3], p[4]:
                origin_dims.build_origin_dims_bundles("svc-od-8", src, [hour_token])

        # Live temp path: one temp table over ALL the same rows.
        with contextlib.closing(duckdb.connect(":memory:")) as con:
            all_rows: list[dict] = []
            for dt in hours:
                all_rows.extend(_canonical_rows(dt))
            _build_temp_table(con, "t_par", all_rows)
            runner = QueryRunner(con, src)
            actual_cols = {"pop", "oip", "ost", "edge"}

            live_pop = origin_repo._origin_pop_latency_from_temp(runner, "t_par", actual_cols, 30)
            live_ip = origin_repo._origin_ip_health_from_temp(runner, "t_par", actual_cols, 30)
            live_path = origin_repo._origin_path_breakdown_from_temp(runner, "t_par", actual_cols)

            # Rollup readers over the 50-hour window.
            st_iso = hours[0].isoformat().replace("+00:00", "Z")
            et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                roll_pop = runner.try_origin_pop_latency_from_rollup(st_iso, et_iso, has_filters=False, limit=30)
                roll_ip = runner.try_origin_ip_health_from_rollup(st_iso, et_iso, has_filters=False, limit=30)
                roll_path = runner.try_origin_path_breakdown_from_rollup(st_iso, et_iso, has_filters=False)

        assert roll_pop is not None and roll_ip is not None and roll_path is not None

        # Both the live temp path AND the rollup read the SAME 50 hours of data
        # (the temp table holds all 50 hours of rows; the rollup sums 50 per-hour
        # files each carrying one hour's rows), so the aggregates are directly
        # equal — requests/error_pct exact, percentiles within float tolerance for
        # the request-weighted reconstruction.

        # ── pop_latency parity ──
        assert roll_pop["_approx"] is True
        live_pop_by = {r["pop"]: r for r in live_pop["rows"]}
        roll_pop_by = {r["pop"]: r for r in roll_pop["rows"]}
        assert set(live_pop_by) == set(roll_pop_by)
        for pop, lr in live_pop_by.items():
            rr = roll_pop_by[pop]
            assert rr["requests"] == lr["requests"]  # exact SUM
            assert abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5

        # ── ip_health parity (error_pct EXACT) ──
        assert roll_ip["_approx"] is True
        live_ip_by = {r["oip"]: r for r in live_ip["rows"]}
        roll_ip_by = {r["oip"]: r for r in roll_ip["rows"]}
        assert set(live_ip_by) == set(roll_ip_by)
        for oip, lr in live_ip_by.items():
            rr = roll_ip_by[oip]
            assert rr["requests"] == lr["requests"]  # exact SUM
            # error_pct is exact across hours (SUM 5xx / SUM total).
            assert abs(rr["error_pct"] - lr["error_pct"]) < 1e-6
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5

        # ── path_breakdown parity (shielding_detected preserved) ──
        assert roll_path["_approx"] is True
        assert roll_path["shielding_detected"] == live_path["shielding_detected"] is True
        live_path_by = {r["edge"]: r for r in live_path["rows"]}
        roll_path_by = {r["edge"]: r for r in roll_path["rows"]}
        assert set(live_path_by) == set(roll_path_by)
        for edge, lr in live_path_by.items():
            rr = roll_path_by[edge]
            assert rr["requests"] == lr["requests"]  # exact SUM
            assert abs(rr["p50_ms"] - lr["p50_ms"]) < 0.5
            assert abs(rr["p95_ms"] - lr["p95_ms"]) < 0.5


# ── Merged from tests/core/test_rollups_origin_latency_ts.py ──
def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


def _seed_logs_rollups_origin_latency_ts(
    con: duckdb.DuckDBPyConnection, table: str, rows: list[dict], *, cols: str | None = None
) -> None:
    """Create ``table`` and INSERT rows. Default schema carries the columns the
    writer reads: timestamp, ottfb, ttfb, ottlb."""
    if cols is None:
        cols = "timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE"
    con.execute(f"CREATE TABLE {table} ({cols})")
    colnames = [c.strip().split()[0] for c in cols.split(",")]
    placeholders = ", ".join("?" for _ in colnames)
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES ({placeholders})",
            [r.get(c) for c in colnames],
        )


def _write_hour_olts(cache_root: str, hour: str, rows: list[dict]) -> str:
    """Write a hand-built origin_latency_ts.parquet for a closed hour."""
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


def _origin_lat_us(r: dict) -> float | None:
    ottfb = r.get("ottfb")
    ttfb = r.get("ttfb")
    if ottfb is not None:
        return float(ottfb)
    if ttfb is not None:
        return float(ttfb) * 1_000_000.0
    return None


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginLatencyTs:
    def test_build_writes_per_minute_both_bases(self, tmp_path):
        """A closed hour with ottfb + ttfb + ottlb produces a per-minute parquet
        with the documented schema and exact ttfb/ttlb counts."""
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-1"}
        hour_token, hour_dt = _past_hour(2)

        # 3 minutes, 4 rows each. ottfb present (us), ottlb present.
        rows = []
        for minute in range(3):
            for i in range(4):
                rows.append(
                    {
                        "timestamp": hour_dt + timedelta(minutes=minute, seconds=i),
                        "ottfb": 100_000.0 + i * 1000,
                        "ttfb": 0.1,
                        "ottlb": 200_000.0 + i * 1000,
                    }
                )

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_olts", rows)
            return c

        p = _build_patches(cache_root, "logs_olts", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-1", src, [hour_token])

        assert n == 1
        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        assert out.exists()
        cols = set(pq.read_table(str(out)).column_names)
        # The shared build_per_hour_bundles driver appends a partition `hour`
        # column, so assert via subset rather than exact equality.
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
        assert len(pylist) == 3  # 3 minutes
        for row in pylist:
            assert row["ttfb_count"] == 4
            assert row["ttlb_count"] == 4
            assert row["ttfb_p50_us"] is not None
            assert row["ttlb_p95_us"] is not None

    def test_build_skips_active_hour(self, tmp_path):
        """The active UTC hour is still being written; the writer must skip it."""
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-active"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_olts", [])
            return c

        p = _build_patches(cache_root, "logs_olts", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-active", src, [active_token])

        assert n == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}").exists()

    def test_build_missing_ottlb_writes_ttlb_zero_null(self, tmp_path):
        """A service without ``ottlb`` still produces the bundle with a uniform
        ttlb block: count=0, p*=NULL. ttfb columns are populated."""
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-noottlb"}
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

        p = _build_patches(cache_root, "logs_noottlb", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-noottlb", src, [hour_token])

        assert n == 1
        out = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        cols = set(pq.read_table(str(out)).column_names)
        # Schema is still uniform — ttlb columns are present.
        assert {"ttlb_count", "ttlb_p50_us", "ttlb_p95_us", "ttlb_p99_us"}.issubset(cols)
        pylist = pq.read_table(str(out)).to_pylist()
        assert len(pylist) == 1
        row = pylist[0]
        assert row["ttfb_count"] == 5
        assert row["ttlb_count"] == 0
        assert row["ttlb_p50_us"] is None
        assert row["ttlb_p95_us"] is None
        assert row["ttlb_p99_us"] is None

    def test_build_no_latency_writes_nothing(self, tmp_path):
        """A service with no ttfb-latency column (no ottfb/ttfb) has nothing to roll
        up — the bundle is not written even if ottlb exists."""
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-nolat"}
        hour_token, hour_dt = _past_hour(2)

        def _fresh_con():
            c = duckdb.connect(":memory:")
            # ottlb present but NO ottfb/ttfb → skip entirely.
            c.execute("CREATE TABLE logs_nolat (timestamp TIMESTAMPTZ, ottlb DOUBLE)")
            c.execute("INSERT INTO logs_nolat VALUES (?, ?)", [hour_dt, 200_000.0])
            return c

        p = _build_patches(cache_root, "logs_nolat", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-nolat", src, [hour_token])

        assert n == 0
        assert not (
            cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet"
        ).exists()

    def test_backfill_skips_built_hours(self, tmp_path):
        """Backfill walks rollups/hour_bundled and only queues hours WITH
        all_fields.parquet AND WITHOUT origin_latency_ts.parquet."""
        from backend.core.rollups import origin_latency_ts

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-bf", "service_id": "svc-olts-bf"}

        h1_token, _ = _past_hour(3)
        h2_token, _ = _past_hour(4)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        (bundled_root / f"hour={h1_token}").mkdir(parents=True)
        (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")
        (bundled_root / f"hour={h2_token}").mkdir(parents=True)
        (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
        # h2 already has the sentinel — backfill should target h1 only.
        (bundled_root / f"hour={h2_token}" / "origin_latency_ts.parquet").write_bytes(b"x")

        captured: list[list[str]] = []

        def _stub_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with patch.object(origin_latency_ts, "build_origin_latency_ts_bundles", side_effect=_stub_build):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = origin_latency_ts.backfill_origin_latency_ts_bundles("svc-olts-bf", src)

        assert n == 1
        assert captured == [[h1_token]]

    def test_reader_eligibility_gates(self, tmp_path):
        """Reader returns None for: has_filters, split_by_leg, sub-minute bucket,
        non-integer-minute bucket, window < 48h, and an empty bundle dir
        (missing-closed-hour fail-closed)."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-elig"}
        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)

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
            # has_filters → None
            assert _call(has_filters=True) is None
            # split_by_leg=True → None
            assert _call(split_by_leg=True) is None
            # sub-minute bucket (< 1) → None
            assert _call(bucket_minutes=1 / 60) is None
            # non-integer-minute bucket → None
            assert _call(bucket_minutes=1.5) is None
            # window < 48h → None
            assert _call(end_time="2026-06-02T12:00:00Z") is None
            # malformed timestamp → None
            assert _call(start_time="not-a-date") is None
            # empty bundle dir (no closed-hour rollup files) → None (fail-closed)
            assert _call() is None
            # bad metric / percentile → None
            assert _call(metric="bogus") is None
            assert _call(percentile="p42") is None

    def test_reader_fail_closed_on_missing_closed_hour(self, tmp_path):
        """If a closed hour in the window had per-field data but no rollup bundle on
        disk, the reader fails closed (returns None) rather than undercount."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-failclosed"}

        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()
        # Write rollup files for ALL hours of day_a + day_b...
        for d_iso in (day_a, day_b):
            for h in range(24):
                _write_hour_olts(
                    str(cache_root),
                    f"{d_iso}-{h:02d}",
                    [
                        {
                            "bucket_ts": _hour_dt(d_iso, h),
                            "ttfb_count": 5,
                            "ttfb_p50_us": 1000.0,
                            "ttfb_p95_us": 2000.0,
                            "ttfb_p99_us": 3000.0,
                            "ttlb_count": 5,
                            "ttlb_p50_us": 1100.0,
                            "ttlb_p95_us": 2100.0,
                            "ttlb_p99_us": 3100.0,
                        }
                    ],
                )
        # ...but drop one hour's rollup AND give it a per-field marker so
        # collect_hourly_bundle_paths treats it as "had data, missing bundle".
        missing_hour = f"{day_a}-05"
        os.remove(
            os.path.join(
                str(cache_root), "rollups", "hour_bundled", f"hour={missing_hour}", "origin_latency_ts.parquet"
            )
        )
        # per-field rollup marker dir for the missing hour. The per-field root is
        # rollups/hour/field=<f>/hour=<h> (see _rollups_root); collect_hourly_
        # bundle_paths scans those to decide "had data but no bundle → fail closed".
        field_dir = os.path.join(str(cache_root), "rollups", "hour", "field=ottfb", f"hour={missing_hour}")
        os.makedirs(field_dir, exist_ok=True)
        with open(os.path.join(field_dir, "data.parquet"), "wb") as f:
            f.write(b"x")

        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)
        st_iso = f"{day_a}T00:00:00+00:00"
        et_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()

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
        """Eligible window fully inside closed hours → reader re-buckets the rollup
        and returns the timeseries shape with _approx + miss_count."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-serve"}

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
                            "ttfb_count": 10,
                            "ttfb_p50_us": 1000.0,
                            "ttfb_p95_us": 5000.0,
                            "ttfb_p99_us": 9000.0,
                            "ttlb_count": 10,
                            "ttlb_p50_us": 1100.0,
                            "ttlb_p95_us": 5100.0,
                            "ttlb_p99_us": 9100.0,
                        }
                    ],
                )

        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)
        st_iso = f"{day_a}T00:00:00+00:00"
        et_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()

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
        assert result["_approx"] is True
        assert result["has_data"] is True
        # 48 closed hours, bucket=60min → 48 buckets.
        assert len(result["series"]) == 48
        first = result["series"][0]
        assert first["miss_count"] == 10
        # ttfb_p95 = 5000 us → 5.0 ms.
        assert abs(first["value"] - 5.0) < 1e-6

    def test_compact_preserves_minutes(self, tmp_path):
        """24 hour files for a closed day → 1 day file that PRESERVES per-minute
        granularity (the union of the 24 hours), NOT collapsed to one row."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-cd", "service_id": "svc-olts-cd"}
        day = _yesterday_iso()

        for h in range(24):
            # two minutes per hour
            rows = []
            for minute in range(2):
                rows.append(
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
                )
            _write_hour_olts(str(cache_root), f"{day}-{h:02d}", rows)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
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
            # request-weighted p95 of a single value is the value itself.
            p95 = con.execute(f"SELECT DISTINCT ttfb_p95_us FROM read_parquet('{day_file}')").fetchall()
        finally:
            con.close()

        # 24 hours × 2 minutes = 48 rows; 48 distinct minute buckets (preserved!).
        assert n_rows == 48
        assert n_buckets == 48
        assert ttfb_total == 48 * 10
        assert ttlb_total == 48 * 8
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

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_origin_latency_ts_closed_days_to_daily("svc-olts-cd-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_accuracy_rollup_reader_matches_live_temp_path(self, tmp_path):
        """Seed known latencies across a >=48h window; build the origin_latency_ts
        rollups; compare the rollup reader's re-bucketed p95 series (bucket=60min,
        metric=ttfb) against the live _origin_timeseries_from_temp p95 series.
        Assert per-bucket within a few % (request-weighted approximation)."""
        from backend.core.rollups import origin_latency_ts
        from backend.repositories._base import QueryRunner
        from backend.repositories.origin import _origin_timeseries_from_temp

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-olts-acc"}

        # 50 closed hours ending ~2h ago (avoid the active hour so the rollup
        # reader is purely closed-hour rollup — no live merge needed).
        base = (datetime.now(UTC) - timedelta(hours=53)).replace(minute=0, second=0, microsecond=0)
        hours = [base + timedelta(hours=i) for i in range(50)]

        # Each hour: a deterministic spread of ottfb (us) latencies over 10 minutes
        # so the per-hour p95 is well-defined and varies by hour.
        rows: list[dict] = []
        for hi, hour_dt in enumerate(hours):
            for minute in range(10):
                for j in range(20):
                    # latency grows with hour index + within-hour position.
                    lat = 50_000.0 + hi * 500 + minute * 200 + j * 100
                    rows.append(
                        {
                            "timestamp": hour_dt + timedelta(minutes=minute, seconds=j),
                            "ottfb": lat,
                            "ttfb": None,
                            "ottlb": lat + 30_000.0,
                        }
                    )

        def _fresh_con():
            c = duckdb.connect(":memory:")
            _seed_logs_rollups_origin_latency_ts(c, "logs_acc", rows)
            return c

        hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
        p = _build_patches(cache_root, "logs_acc", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-acc", src, hour_tokens)
        assert n == 50

        st_iso = hours[0].isoformat()
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat()

        # --- Rollup reader (closed hours; window ends before the active hour) ---
        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)
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
        assert rollup is not None
        assert rollup["_approx"] is True
        rollup_by_time = {s["time"]: s for s in rollup["series"]}

        # --- Live temp path: materialize the same rows into a temp table the
        # live helper reads (timestamp + precomputed lat_us), run the live
        # _origin_timeseries_from_temp WITHOUT a rollup (table_name=None disables
        # the rollup-first branch so we get the true live series). ---
        live_con = duckdb.connect(":memory:")
        live_con.execute("CREATE TABLE t_live (timestamp TIMESTAMPTZ, lat_us DOUBLE, ottfb DOUBLE, ottlb DOUBLE)")
        for r in rows:
            lat = _origin_lat_us(r)
            live_con.execute(
                "INSERT INTO t_live VALUES (?, ?, ?, ?)",
                [r["timestamp"], lat, r.get("ottfb"), r.get("ottlb")],
            )
        live_runner = QueryRunner(live_con, src)
        live = _origin_timeseries_from_temp(
            live_runner,
            "t_live",
            {"timestamp", "ottfb", "ottlb", "lat_us"},
            60,  # bucket_minutes
            False,  # split_by_leg
            "ttfb",
            "p95",
            # table_name omitted → rollup-first branch is skipped, pure live path.
        )
        live_by_time = {s["time"]: s for s in live["series"]}

        # Same set of buckets, exact counts, and p95 within a few %.
        assert set(rollup_by_time) == set(live_by_time)
        for t, lr in live_by_time.items():
            rr = rollup_by_time[t]
            assert rr["miss_count"] == lr["miss_count"]
            live_v = lr["value"]
            roll_v = rr["value"]
            assert live_v is not None and roll_v is not None
            # request-weighted approximation: per-minute p95 averaged vs the true
            # per-hour p95. With this smooth spread it stays within a few %.
            rel = abs(roll_v - live_v) / live_v
            assert rel < 0.10, f"bucket {t}: rollup p95 {roll_v} vs live {live_v} ({rel:.2%})"


# ── Merged from tests/core/test_rollups_origin_summary.py ──
def _seed_logs_rollups_origin_summary(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the origin_summary writer reads
    and INSERT rows."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, ottfb DOUBLE, ttfb DOUBLE, ottlb DOUBLE,"
        f"  elapsed DOUBLE, ost INTEGER, obytes BIGINT, cache VARCHAR"
        f")"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ottfb"),
                r.get("ttfb"),
                r.get("ottlb"),
                r.get("elapsed"),
                r.get("ost"),
                r.get("obytes"),
                r.get("cache"),
            ],
        )


def _seed_logs_partial(con: duckdb.DuckDBPyConnection, table: str, cols: list[str], rows: list[dict]) -> None:
    """Create ``table`` with only the listed columns + a TIMESTAMPTZ ts."""
    col_defs = ", ".join(f'"{c}" DOUBLE' for c in cols)
    con.execute(f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, {col_defs})')
    placeholder = ", ".join(["?"] * (1 + len(cols)))
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES ({placeholder})",
            [r["timestamp"], *(r.get(c) for c in cols)],
        )


class _FailingFirstCopyCon:
    """Thin wrapper that delegates everything to a real DuckDB conn
    but fails the FIRST ``COPY (...)`` execute with duckdb.Error.
    Lets us hit the copy-failure exception branch without monkey-
    patching read-only DuckDB attributes."""

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
        """Happy path: one closed hour with rows produces one parquet row with
        the documented column set + correct counts."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        hour_token, hour_dt = _past_hour(2)

        rows = [
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "ottfb": 100_000.0 + i * 100,
                "ttfb": 0.1,
                "ottlb": 200_000.0 + i * 100,
                "elapsed": 300_000.0 + i * 100,
                "ost": 200 if i < 8 else 500,  # 2/10 are 5xx
                "obytes": 1000,
                "cache": "MISS" if i < 5 else "HIT",
            }
            for i in range(10)
        ]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [hour_token])

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
        # ost: 8 × 200, 2 × 500-599 ⇒ 2 5xx / 10 total
        assert row["ost_5xx_count"] == 2
        assert row["ost_total_count"] == 10
        # 10 rows all have lat_us non-null (ottfb is non-null)
        assert row["lat_us_count"] == 10

    def test_build_origin_summary_handles_unsigned_latency_columns(self, tmp_path):
        """Unsigned elapsed/ottlb columns must not underflow during subtraction."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
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
            assert origin_summary.build_origin_summary_bundles("svc-os", src, [hour_token]) == 1

        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["cdn_ovh_p50_us"] == 119641.0 - 294429.0

    def test_build_origin_summary_skips_active_hour(self, tmp_path):
        """Active UTC hour must be skipped (live SQL serves it). Convention
        shared with time_series + slow_urls writers."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [active_token])

        assert n == 0

    def test_backfill_origin_summary_walks_existing_bundle_hours(self, tmp_path):
        """Self-heal driver picks up closed hours with all_fields.parquet but
        no origin_summary.parquet."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}

        h1_token, _ = _past_hour(3)
        h2_token, _ = _past_hour(4)
        bundled_root = cache_root / "rollups" / "hour_bundled"
        (bundled_root / f"hour={h1_token}").mkdir(parents=True)
        (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")
        (bundled_root / f"hour={h2_token}").mkdir(parents=True)
        (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
        (bundled_root / f"hour={h2_token}" / "origin_summary.parquet").write_bytes(b"x")  # already-built

        written_hours: list[list[str]] = []

        def _spy_build(_sid, _src, hours):
            written_hours.append(list(hours))
            return len(hours)

        with patch("backend.core.rollups.origin_dims.build_origin_summary_bundles", side_effect=_spy_build):
            with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
                n = origin_summary.backfill_origin_summary_bundles("svc-os", src)

        assert n == 1
        assert written_hours == [[h1_token]]

    def test_try_origin_summary_from_rollup_eligibility_gates(self, tmp_path):
        """Reader returns None for has_filters=True, window<48h, missing
        coverage, malformed timestamps."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}

        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            # has_filters → None
            assert (
                runner.try_origin_summary_from_rollup(
                    "2026-06-01T00:00:00Z",
                    "2026-06-15T00:00:00Z",
                    has_filters=True,
                    actual_cols={"ottfb", "ttfb"},
                )
                is None
            )
            # Window < 48h
            assert (
                runner.try_origin_summary_from_rollup(
                    "2026-06-01T00:00:00Z",
                    "2026-06-02T12:00:00Z",
                    has_filters=False,
                    actual_cols={"ottfb", "ttfb"},
                )
                is None
            )
            # Malformed timestamp
            assert (
                runner.try_origin_summary_from_rollup(
                    "not-a-date",
                    "2026-06-15T00:00:00Z",
                    has_filters=False,
                    actual_cols={"ottfb", "ttfb"},
                )
                is None
            )

    def test_try_origin_summary_from_rollup_aggregates_across_hours(self, tmp_path):
        """End-to-end: build a 50-hour window of origin_summary files, verify
        counts SUM exactly + percentiles request-weighted-average."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}

        bundled_root = str(cache_root / "rollups" / "hour_bundled")

        base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
        hours = [base_dt + timedelta(hours=i) for i in range(50)]
        for dt in hours:
            hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
            os.makedirs(hour_dir, exist_ok=True)
            # Each hour: requests=1000, misses=500, passes=200, 5xx=10/1000,
            # lat_us_count=1000, ottfb_p50_us=50000 (50 ms), ottfb_p95_us=200000,
            # ottlb_count=900, ottlb_p50_us=80000, cdn_ovh_count=850,
            # cdn_ovh_p50_us=12000, obytes_count=1000, obytes_p50=2048
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

        con = duckdb.connect(":memory:")
        runner = QueryRunner(con, src)

        st_iso = hours[0].isoformat().replace("+00:00", "Z")
        et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        actual_cols = {"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"}
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(
                st_iso,
                et_iso,
                has_filters=False,
                actual_cols=actual_cols,
            )

        assert result is not None, "reader returned None — eligibility gate fired"
        assert result["_approx"] is True
        assert result["has_data"] is True

        # Counts SUM exactly across 50 hours.
        assert result["total_misses"] == 500 * 50
        assert result["total_passes"] == 200 * 50

        # Percentile fields are request-weighted average — same per-hour
        # values everywhere → cross-hour value == per-hour value.
        assert abs(result["ottfb_p50_ms"] - 50.0) < 1e-6  # 50000us = 50ms
        assert abs(result["ottfb_p95_ms"] - 200.0) < 1e-6
        assert abs(result["ottlb_p50_ms"] - 80.0) < 1e-6
        assert abs(result["cdn_overhead_p50_ms"] - 12.0) < 1e-6
        assert abs(result["obytes_p50"] - 2048.0) < 1e-6

        # Error rate: SUM(5xx) / SUM(total) = 10*50 / 1000*50 = 0.01 — exact across hours.
        assert abs(result["origin_error_rate"] - 0.01) < 1e-9

    def test_build_origin_summary_empty_hours_returns_zero(self):
        """Caller passes an empty list (no closed hours to rebuild). Must
        return 0 immediately — never opens a DuckDB connection."""
        from backend.core.rollups import origin_summary

        with patch("backend.core.duckdb.get_connection") as mock_conn:
            n = origin_summary.build_origin_summary_bundles("svc-os", {"name": "svc-os"}, [])
        assert n == 0
        mock_conn.assert_not_called()

    def test_build_origin_summary_skips_malformed_hour_tokens(self, tmp_path, caplog):
        """Malformed hour tokens (e.g. legacy artifacts, hand-edits) are
        skipped with a warning. The valid token in the same call still
        runs."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        valid_token, valid_dt = _past_hour(2)

        rows = [{"timestamp": valid_dt + timedelta(minutes=1), "ottfb": 50_000.0, "ttfb": 0.05}]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with writer_patches(cache_root, "logs_os", con):
            import logging as _logging

            with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_dims"):
                n = origin_summary.build_origin_summary_bundles("svc-os", src, ["totally-not-an-hour", valid_token])

        assert n == 1
        assert any("malformed hour token" in r.message for r in caplog.records)

    def test_build_origin_summary_returns_zero_when_safe_table_missing(self, tmp_path):
        """A service whose schema can't resolve to a SQL-safe table (no
        name match) short-circuits at 0 — no parquet ever written."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, _ = _past_hour(2)

        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
        assert n == 0

    def test_build_origin_summary_returns_zero_when_describe_columns_fails(self, tmp_path):
        """If describe_columns returns None (schema lookup failed), the
        writer returns 0 and closes the connection in finally."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, _ = _past_hour(2)

        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", [])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.rollups._common.describe_columns", return_value=None),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
        assert n == 0

    def test_build_origin_summary_ottfb_only_schema(self, tmp_path):
        """Schema with ottfb but no ttfb — latency expression uses ottfb
        directly (not the COALESCE form)."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, dt = _past_hour(2)

        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ottfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ottfb": 100_000.0 + i * 1000} for i in range(5)],
        )

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
        assert n == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        assert row["requests"] == 5
        assert row["lat_us_count"] == 5
        # All optional columns absent → 0 / NULL.
        assert row["ottlb_count"] == 0
        assert row["cdn_ovh_count"] == 0
        assert row["ost_5xx_count"] == 0
        assert row["obytes_count"] == 0
        assert row["total_misses"] == 0  # no cache column

    def test_build_origin_summary_ttfb_only_schema(self, tmp_path):
        """Schema with ttfb but no ottfb — latency expression scales ttfb
        (seconds) to microseconds."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, dt = _past_hour(2)

        con = duckdb.connect(":memory:")
        _seed_logs_partial(
            con,
            "logs_os",
            ["ttfb"],
            [{"timestamp": dt + timedelta(minutes=i), "ttfb": 0.05 + i * 0.001} for i in range(5)],
        )

        with writer_patches(cache_root, "logs_os", con):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
        assert n == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet"
        row = pq.read_table(str(bundle)).to_pylist()[0]
        # 0.05 s = 50_000 us — verify the *1e6 scaling kicked in.
        assert row["lat_us_count"] == 5
        assert row["ottfb_p50_us"] >= 49_999.0

    def test_build_origin_summary_no_latency_columns_returns_zero(self, tmp_path):
        """A service with no ottfb AND no ttfb has nothing to summarise —
        return 0 without writing any bundle."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, dt = _past_hour(2)

        con = duckdb.connect(":memory:")
        # Schema with only an unrelated column.
        _seed_logs_partial(con, "logs_os", ["elapsed"], [{"timestamp": dt, "elapsed": 100_000.0}])

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.duckdb.get_connection", return_value=con),
        ):
            n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])
        assert n == 0
        assert not (cache_root / "rollups" / "hour_bundled" / f"hour={token}" / "origin_summary.parquet").exists()

    def test_build_origin_summary_logs_and_skips_on_copy_failure(self, tmp_path, caplog):
        """A duckdb.Error on the COPY for one hour must not abort the
        whole call — log a warning, clean up the tmp file, continue to
        the next hour."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        h1_token, _ = _past_hour(3)
        h2_token, h2_dt = _past_hour(2)

        rows = [
            {
                "timestamp": h2_dt + timedelta(minutes=1),
                "ottfb": 50_000.0,
                "ttfb": 0.05,
                "ottlb": None,
                "elapsed": None,
                "ost": None,
                "obytes": None,
                "cache": None,
            }
        ]
        real = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(real, "logs_os", rows)
        con = _FailingFirstCopyCon(real)

        with writer_patches(cache_root, "logs_os", con):
            import logging as _logging

            with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_dims"):
                n = origin_summary.build_origin_summary_bundles("svc-os", src, [h1_token, h2_token])

        # h1 failed, h2 succeeded.
        assert n == 1
        assert any("COPY failed for hour" in r.message for r in caplog.records)
        # Tmp file from h1 must NOT be left behind.
        h1_dir = cache_root / "rollups" / "hour_bundled" / f"hour={h1_token}"
        if h1_dir.exists():
            assert not any(p.name.startswith(".tmp_os_") for p in h1_dir.iterdir())

    def test_build_origin_summary_logs_on_publish_failure(self, tmp_path, caplog):
        """A successful COPY followed by an OSError during the atomic
        os.replace publish must be caught, logged, and the tmp file cleaned
        up so the directory stays consistent."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}
        token, dt = _past_hour(2)

        rows = [
            {
                "timestamp": dt + timedelta(minutes=1),
                "ottfb": 50_000.0,
                "ttfb": 0.05,
                "ottlb": None,
                "elapsed": None,
                "ost": None,
                "obytes": None,
                "cache": None,
            }
        ]
        con = duckdb.connect(":memory:")
        _seed_logs_rollups_origin_summary(con, "logs_os", rows)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._common._safe_table_for", return_value="logs_os"),
            patch("backend.core.duckdb.get_connection", return_value=con),
            patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
            patch("os.replace", side_effect=OSError("simulated rename failure")),
        ):
            import logging as _logging

            with caplog.at_level(_logging.WARNING, logger="backend.core.rollups.origin_dims"):
                n = origin_summary.build_origin_summary_bundles("svc-os", src, [token])

        assert n == 0
        assert any("could not publish origin_summary" in r.message for r in caplog.records)

    def test_backfill_returns_zero_when_bundled_root_missing(self, tmp_path):
        """Brand-new service: bundled_root doesn't exist yet. backfill
        must return 0 immediately rather than calling listdir on a
        nonexistent path."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-empty"}

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = origin_summary.backfill_origin_summary_bundles("svc-empty", src)
        assert n == 0

    def test_backfill_skips_non_hour_entries_and_malformed_tokens(self, tmp_path):
        """Bundled root may contain non-hour entries (.tmp files, junk
        from prior crashes) and malformed hour tokens. Both must be
        filtered out at the directory-walk stage."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}

        bundled_root = cache_root / "rollups" / "hour_bundled"
        bundled_root.mkdir(parents=True)

        # Junk entry that doesn't start with hour=.
        (bundled_root / "junk_file.txt").write_text("ignore me")
        (bundled_root / "metadata").mkdir()
        # Malformed hour token.
        (bundled_root / "hour=not-an-hour").mkdir()
        (bundled_root / "hour=not-an-hour" / "all_fields.parquet").write_bytes(b"x")
        # Valid hour but missing all_fields.parquet (not yet bundled).
        valid_token, _ = _past_hour(3)
        (bundled_root / f"hour={valid_token}").mkdir()
        # Note: no all_fields.parquet, so this should also be skipped.

        captured: list[list[str]] = []

        def _spy_build(_sid, _src, hours):
            captured.append(list(hours))
            return len(hours)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch(
                "backend.core.rollups.origin_dims.build_origin_summary_bundles",
                side_effect=_spy_build,
            ),
        ):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", src)
        # Nothing to build → returns 0 and never calls build.
        assert n == 0
        assert captured == []

    def test_backfill_returns_zero_on_listdir_oserror(self, tmp_path):
        """If os.listdir raises (e.g. permission denied), backfill must
        return 0 rather than propagate the OSError up the cron scheduler."""
        from backend.core.rollups import origin_summary

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os"}

        bundled_root = cache_root / "rollups" / "hour_bundled"
        bundled_root.mkdir(parents=True)

        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("os.listdir", side_effect=OSError("permission denied")),
        ):
            n = origin_summary.backfill_origin_summary_bundles("svc-os", src)
        assert n == 0


# ── Merged from tests/core/test_rollups_origin_summary_daily.py ──
def _write_hour_summary(cache_root: str, hour: str, row: dict) -> str:
    """Write one origin_summary.parquet at
    ``<cache_root>/rollups/hour_bundled/hour=<hour>/origin_summary.parquet``
    and return the path."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "requests": pa.array([row["requests"]], type=pa.int64()),
            "total_misses": pa.array([row["total_misses"]], type=pa.int64()),
            "total_passes": pa.array([row["total_passes"]], type=pa.int64()),
            "lat_us_count": pa.array([row["lat_us_count"]], type=pa.int64()),
            "ottfb_p50_us": pa.array([row["ottfb_p50_us"]], type=pa.float64()),
            "ottfb_p75_us": pa.array([row["ottfb_p75_us"]], type=pa.float64()),
            "ottfb_p95_us": pa.array([row["ottfb_p95_us"]], type=pa.float64()),
            "ottfb_p99_us": pa.array([row["ottfb_p99_us"]], type=pa.float64()),
            "ottlb_count": pa.array([row["ottlb_count"]], type=pa.int64()),
            "ottlb_p50_us": pa.array([row.get("ottlb_p50_us")], type=pa.float64()),
            "ottlb_p95_us": pa.array([row.get("ottlb_p95_us")], type=pa.float64()),
            "cdn_ovh_count": pa.array([row["cdn_ovh_count"]], type=pa.int64()),
            "cdn_ovh_p50_us": pa.array([row.get("cdn_ovh_p50_us")], type=pa.float64()),
            "ost_5xx_count": pa.array([row["ost_5xx_count"]], type=pa.int64()),
            "ost_total_count": pa.array([row["ost_total_count"]], type=pa.int64()),
            "obytes_count": pa.array([row["obytes_count"]], type=pa.int64()),
            "obytes_p50": pa.array([row.get("obytes_p50")], type=pa.float64()),
        }
    )
    p = os.path.join(d, "origin_summary.parquet")
    pq.write_table(table, p)
    return p


def _uniform_row(req: int, lat_us: float) -> dict:
    """A canonical hour row where every percentile equals ``lat_us`` and
    every count is derived from ``req``. Used so the expected day
    aggregate is trivial to predict."""
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


@pytest.mark.usefixtures("frozen_clock")
class TestRollupsOriginSummaryDaily:
    def test_compact_writes_per_day_file_with_correct_aggregates(self, tmp_path):
        """24 per-hour parquets for a closed day → 1 day file with SUM for
        counts and request-weighted-average for percentiles. Schema matches
        the hour file so the reader treats both interchangeably."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-day", "service_id": "svc-os-day"}
        day = _yesterday_iso()

        # Each hour has 100 requests at 10000 us p95 — uniform so the
        # weighted average is trivially 10000 us and the SUMs are
        # 100 * 24 = 2400 per count column.
        for h in range(24):
            _write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(100, 10000.0))

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
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
        assert rec["requests"] == 2400
        assert rec["total_misses"] == 2400
        assert rec["lat_us_count"] == 2400
        assert rec["ottfb_p50_us"] == 10000.0
        assert rec["ottfb_p75_us"] == 10000.0
        assert rec["ottfb_p95_us"] == 10000.0
        assert rec["ottfb_p99_us"] == 10000.0
        assert rec["ottlb_count"] == 2400
        assert rec["cdn_ovh_count"] == 2400
        assert rec["ost_total_count"] == 2400
        assert rec["obytes_count"] == 2400

    def test_compact_skips_active_day(self, tmp_path):
        """The current UTC day is still being written; the compactor must
        leave it alone or the day file would change as new hours land."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-active", "service_id": "svc-os-active"}
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        for h in range(24):
            _write_hour_summary(str(cache_root), f"{today}-{h:02d}", _uniform_row(50, 5000.0))

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                rebuilt = rollups.compact_origin_summary_closed_days_to_daily("svc-os-active", src)

        assert rebuilt == 0
        assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()

    def test_compact_is_idempotent_until_inputs_change(self, tmp_path):
        """Re-running with the day file newer than all hour inputs returns 0
        rebuilt. Touching an hour file → next run rebuilds."""
        from backend.core import rollups

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-idem", "service_id": "svc-os-idem"}
        day = _yesterday_iso()

        hour_paths = [
            _write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(10, 1000.0)) for h in range(24)
        ]

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                first = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
                assert first == 1
                second = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
                assert second == 0, "mtime gate should suppress rebuilds when nothing changed"

                # Touch one hour file so its mtime jumps past the day file.
                future = (
                    os.path.getmtime(cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_summary.parquet")
                    + 5
                )
                os.utime(hour_paths[0], (future, future))
                third = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
                assert third == 1, "stale day file should rebuild when an hour input is newer"

    def test_reader_uses_day_file_when_whole_day_in_window(self, tmp_path):
        """Whole-UTC-day window with day file present → day files opened
        (one per closed day), NOT 24 hour files per day. Window spans 2
        whole closed days to satisfy the reader's 48h min-window gate."""
        from backend.core.rollups import day_bundles
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-reader", "service_id": "svc-os-reader"}
        day_a = _three_days_ago_iso()  # earlier day
        day_b = _two_days_ago_iso()  # later day; together a 48h window behind active

        for d in (day_a, day_b):
            for h in range(24):
                _write_hour_summary(str(cache_root), f"{d}-{h:02d}", _uniform_row(100, 20000.0))

        # Build both day files via the real compactor so their schema matches.
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
                assert day_bundles.compact_origin_summary_closed_days_to_daily("svc-os-reader", src) == 2

        # Window: 00:00 of day_a to 00:00 of day_b+1 day (= 48h).
        st_iso = f"{day_a}T00:00:00+00:00"
        end_dt = datetime.fromisoformat(st_iso) + timedelta(days=2)
        end_iso = end_dt.isoformat()

        captured_paths: list[str] = []

        class _StubResult:
            def fetchone(self):
                # Mirror the column tuple shape try_origin_summary_from_rollup unpacks.
                return (4800, 4800, 0, 4800, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 20000.0, 0.0, 1024.0)

        class _StubConn:
            def execute(self, sql):
                # Capture the path list passed to read_parquet so we can
                # assert day files were opened, not hour files.
                captured_paths.append(sql)
                return _StubResult()

        runner = QueryRunner.__new__(QueryRunner)
        runner.src = src
        runner._table = "logs_svc_os_reader"
        runner.execute = _StubConn().execute  # type: ignore[method-assign]

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
                actual_cols={"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"},
            )

        assert result is not None
        assert result.get("_approx") is True
        # The SQL the reader builds should reference exactly the 2 day files —
        # NOT the 48 hour files.
        assert len(captured_paths) == 1
        sql = captured_paths[0]
        assert "day_bundled" in sql, f"reader should pick day files, got SQL: {sql[:300]}"
        assert f"day={day_a}" in sql
        assert f"day={day_b}" in sql
        assert sql.count(".parquet") == 2, f"expected 2 day-file paths, got {sql.count('.parquet')}"
        assert "hour_bundled" not in sql, "reader should NOT touch hour_bundled when day files cover the window"

    def test_reader_falls_back_to_hour_when_day_file_missing(self, tmp_path):
        """No day file present → reader walks hour files (same behaviour as
        before Task C landed). Confirms Task C is purely additive."""
        from backend.repositories._base import QueryRunner

        cache_root = tmp_path / "cache"
        cache_root.mkdir()
        src = {"name": "svc-os-hour", "service_id": "svc-os-hour"}
        day_a = _three_days_ago_iso()
        day_b = _two_days_ago_iso()

        # Write hour files but DO NOT compact them — no day files should exist.
        for d in (day_a, day_b):
            for h in range(24):
                _write_hour_summary(str(cache_root), f"{d}-{h:02d}", _uniform_row(100, 30000.0))

        st_iso = f"{day_a}T00:00:00+00:00"
        end_dt = datetime.fromisoformat(st_iso) + timedelta(days=2)
        end_iso = end_dt.isoformat()

        captured_paths: list[str] = []

        class _StubResult:
            def fetchone(self):
                return (2400, 2400, 0, 2400, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 30000.0, 0.0, 1024.0)

        class _StubConn:
            def execute(self, sql):
                captured_paths.append(sql)
                return _StubResult()

        runner = QueryRunner.__new__(QueryRunner)
        runner.src = src
        runner._table = "logs_svc_os_hour"
        runner.execute = _StubConn().execute  # type: ignore[method-assign]

        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            result = runner.try_origin_summary_from_rollup(
                st_iso,
                end_iso,
                has_filters=False,
                actual_cols={"ottfb", "ttfb", "ottlb", "elapsed", "ost", "obytes", "cache"},
            )

        assert result is not None
        assert len(captured_paths) == 1
        sql = captured_paths[0]
        # Hour files, no day_bundled, 48 paths (24 per day × 2 closed days).
        assert "day_bundled" not in sql
        assert sql.count("hour_bundled") >= 1
        assert sql.count(".parquet") == 48, f"expected 48 hour files, got {sql.count('.parquet')}"
