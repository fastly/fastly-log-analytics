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

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


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


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _build_patches(cache_root, table: str, con_factory):
    """The common writer-side patch stack — _cache_dir, the resolved view
    table, the read-only connection, the iceberg lock, and the stale-view
    retry shim.

    ``con_factory`` is a zero-arg callable returning a FRESH connection each
    time. In production ``get_connection`` opens a new connection per call and
    the shared driver closes it in its ``finally``; origin_dims drives the
    shared driver once PER bundle (3×), so a single reused connection would be
    closed after the first bundle. The factory hands each driver pass its own
    connection — matching the prod semantics.
    """
    return (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=table),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: con_factory()),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    )


# ── Writer: happy path / top-K ──────────────────────────────────────────────


def test_build_origin_dims_writes_three_bundles(tmp_path):
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


def test_build_origin_dims_skips_active_hour(tmp_path):
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


def test_build_origin_dims_missing_oip_skips_ip_only(tmp_path):
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


def test_build_origin_dims_no_latency_writes_nothing(tmp_path):
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


# ── Backfill driver ─────────────────────────────────────────────────────────


def test_backfill_origin_dims_walks_existing_bundle_hours(tmp_path):
    """The self-heal driver picks up closed hours that have all_fields.parquet
    but no origin_pop.parquet (the sentinel) and builds the missing bundles."""
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
    # h2 already has the pop sentinel — backfill should target h1 only.
    (bundled_root / f"hour={h2_token}" / "origin_pop.parquet").write_bytes(b"x")

    written_hours: list[list[str]] = []

    def _spy_build(_sid, _src, hours):
        written_hours.append(list(hours))
        return len(hours)

    with patch("backend.core.rollups.origin_dims.build_origin_dims_bundles", side_effect=_spy_build):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = origin_dims.backfill_origin_dims_bundles("svc-od-5", src)

    assert n == 1
    assert written_hours == [[h1_token]]


# ── Reader eligibility gates ────────────────────────────────────────────────


def test_readers_eligibility_gates(tmp_path):
    """All four readers return None for: filters present, window too short,
    missing closed-hour rollup file, malformed timestamps."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-od-6"}
    con = duckdb.connect(":memory:")
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


# ── Reader: status_codes from existing all_fields.parquet ───────────────────


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


def test_try_origin_status_from_rollup_reads_all_fields(tmp_path):
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

    con = duckdb.connect(":memory:")
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


# ── Parity: rollup reader output == live temp-path output ────────────────────


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


def test_rollup_readers_match_live_temp_path(tmp_path):
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
    con = duckdb.connect(":memory:")
    all_rows: list[dict] = []
    for dt in hours:
        all_rows.extend(_canonical_rows(dt))
    _build_temp_table(con, "t_par", all_rows)
    runner = QueryRunner(con, src)
    actual_cols = {"pop", "oip", "ost", "edge"}

    live_pop = origin_repo._origin_pop_latency_from_temp(runner, "t_par", actual_cols, 30, has_filters=True)
    live_ip = origin_repo._origin_ip_health_from_temp(runner, "t_par", actual_cols, 30, has_filters=True)
    live_path = origin_repo._origin_path_breakdown_from_temp(runner, "t_par", actual_cols, has_filters=True)

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
