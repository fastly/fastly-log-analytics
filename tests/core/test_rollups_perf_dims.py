"""Tests for the per-hour perf_dims (ttl_dist) rollup writer + backfill driver,
the day-compaction merge, and the /api/performance/aggregates reader
(try_perf_ttl_dist_from_rollup).

perf_dims emits ONE file per closed hour from one module:
  perf_ttl_dist.parquet   (ttl histogram bucket + count + MIN(ttl))

The math is EXACT (count SUM + MIN-of-MIN) — no _approx flag on the reader.
The parity test asserts byte-equal aggregates against the live ttl_dist
histogram SQL (replicated verbatim from backend/repositories/performance.py)
over the SAME closed-hour data.

Mirrors test_rollups_security_dims.py (writer-side _build_patches stack, real
DuckDB COPY) and test_rollups_network_speed.py (seeded-parquet reader + day
compactor) in structure.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ── helpers ──────────────────────────────────────────────────────────────────


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the perf_dims writer reads and
    INSERT rows. Columns: timestamp, ttl."""
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, ttl BIGINT)")
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES (?, ?)", [r["timestamp"], r.get("ttl")])


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _build_patches(cache_root, table: str, con_factory):
    """The common writer-side patch stack — _cache_dir, the resolved view
    table, the read-only connection, the iceberg lock, and the stale-view
    retry shim. ``con_factory`` returns a FRESH connection each call (the
    shared driver runs once per bundle and closes the conn it's given)."""
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


def _write_hour_all_fields(cache_root: str, hour: str) -> None:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table({"field": pa.array(["x"]), "value": pa.array(["y"]), "count": pa.array([1], type=pa.int64())}),
        os.path.join(d, "all_fields.parquet"),
    )


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Writer: happy path + schema + exact aggregates ───────────────────────────


def test_build_perf_dims_writes_bundle(tmp_path):
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
        _seed_logs(c, "logs_pd", rows)
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


def test_build_perf_dims_skips_active_hour(tmp_path):
    """The active UTC hour is still being written; the writer must skip it."""
    from backend.core.rollups import perf_dims

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pd-2"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_pd", [])
        return c

    p = _build_patches(cache_root, "logs_pd", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = perf_dims.build_perf_dims_bundles("svc-pd-2", src, [active_token])

    assert n == 0
    hour_dir = cache_root / "rollups" / "hour_bundled" / f"hour={active_token}"
    assert not (hour_dir / "perf_ttl_dist.parquet").exists()


def test_build_perf_dims_missing_ttl_skips_bundle(tmp_path):
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


# ── Backfill driver ──────────────────────────────────────────────────────────


def test_backfill_perf_dims_skips_built_hours(tmp_path):
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

    with patch("backend.core.rollups.perf_dims.build_per_hour_bundles", side_effect=_spy_bphb):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = perf_dims.backfill_perf_dims_bundles("svc-pd-bf", src)

    assert n == 1
    assert calls == [{h2}]


# ── Day-compaction merge ─────────────────────────────────────────────────────


def _write_hour_bundle(hour_dir: str, filename: str, select_sql_values: str, schema_cols: str) -> None:
    os.makedirs(hour_dir, exist_ok=True)
    wcon = duckdb.connect()
    try:
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {select_sql_values}) AS t({schema_cols})) "
            f"TO '{hour_dir}/{filename}' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def test_compact_perf_dims_day_merge(tmp_path):
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
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-4", src)

    assert n == 1  # one bundle for the single closed day
    day_file = cache_root / "rollups" / "day_bundled" / f"day={day_str}" / "perf_ttl_dist.parquet"
    assert day_file.exists()

    by_bucket = {r["bucket"]: r for r in pq.read_table(str(day_file)).to_pylist()}
    assert by_bucket["0s"]["count"] == 10 and by_bucket["0s"]["min_ttl"] == -5  # 4+6, MIN(-5, 0)
    assert by_bucket["<10s"]["count"] == 2 and by_bucket["<10s"]["min_ttl"] == 5


def test_compact_perf_dims_skips_active_day(tmp_path):
    from backend.core.rollups import day_bundles

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pd-active"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    for h in range(24):
        _write_hour_ttl(str(cache_root), f"{today}-{h:02d}", [{"bucket": "0s", "count": 5, "min_ttl": 0}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            n = day_bundles.compact_perf_dims_closed_days_to_daily("svc-pd-active", src)

    assert n == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()


# ── Reader ───────────────────────────────────────────────────────────────────


def _stub_runner(src: dict, captured_sql: list[str], stub_rows: list[tuple]):
    from backend.repositories._base import QueryRunner

    class _Result:
        def fetchall(self):
            return stub_rows

    class _Conn:
        def execute(self, sql, params=None):
            captured_sql.append(sql)
            return _Result()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


def test_reader_returns_none_when_filtered(tmp_path):
    src = {"name": "svc-pd-r", "service_id": "svc-pd-r"}
    runner = _stub_runner(src, [], [])
    result = runner.try_perf_ttl_dist_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        has_filters=True,
    )
    assert result is None


def test_reader_returns_none_for_short_window(tmp_path):
    src = {"name": "svc-pd-r2", "service_id": "svc-pd-r2"}
    runner = _stub_runner(src, [], [])
    result = runner.try_perf_ttl_dist_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-01T12:00:00+00:00",
        has_filters=False,
    )
    assert result is None


def test_reader_returns_none_malformed_timestamps(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pd-r2b", "service_id": "svc-pd-r2b"}
    runner = _stub_runner(src, [], [])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert runner.try_perf_ttl_dist_from_rollup("not-a-date", "2026-06-15T00:00:00Z", has_filters=False) is None


def test_reader_returns_none_no_coverage(tmp_path):
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


def test_reader_serves_rows_sum_per_bucket_min_ttl_order(tmp_path):
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


# ── Parity: rollup reader output == live ttl_dist SQL over the SAME data ──────

# The live ttl_dist histogram SQL, replicated verbatim from
# backend/repositories/performance.py (the section is inline, not a named
# template). The {temp_table} placeholder mirrors the live call site.
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


def test_rollup_reader_matches_live_sql(tmp_path):
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
            _seed_logs(c, "logs_par", rows)
            return c

        p = _build_patches(cache_root, "logs_par", _fresh_con)
        with p[0], p[1], p[2], p[3], p[4]:
            perf_dims.build_perf_dims_bundles("svc-pd-8", src, [hour_token])

    # Live SQL over one temp table holding ALL 50 hours of the same rows.
    con = duckdb.connect(":memory:")
    all_rows: list[dict] = []
    for dt in hours:
        all_rows.extend(_canonical_rows(dt))
    _seed_logs(con, "t_par", all_rows)
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
