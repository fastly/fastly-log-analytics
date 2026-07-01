"""Tests for the per-hour MINUTE-granular origin-latency-percentile time-series
rollup writer + hybrid reader + day compactor (perf plan Part A.2).

origin_latency_ts is a NEW hybrid shape combining two templates:
  - the minute-granular time-series WRITER shape of verified_bots_ts
    (date_trunc('minute', timestamp) AS bucket_ts; reader re-buckets), and
  - the request-weighted percentile merge math of slow_urls
    (SUM(p_us*cnt)/SUM(cnt) across minutes → biased → _approx).

Each closed hour stores BOTH latency bases (ttfb + ttlb) so the reader serves
either metric. The day compactor PRESERVES the minute (bucket_ts) dimension
because the panel is a time series (minutes are disjoint across the 24 hours →
the day file is their union). Percentiles are request-weighted approximations
(_approx: True); counts are exact SUMs.

Each test uses a UNIQUE per-test cache root (tmp_path) + service name so the
parallel suite (-n auto) doesn't share an iceberg/cache key.
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


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict], *, cols: str | None = None) -> None:
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


def _build_patches(cache_root, table: str, con_factory):
    """The common writer-side patch stack — _cache_dir, the resolved view
    table, the read-only connection, the iceberg lock, and the stale-view
    retry shim. ``con_factory`` hands each driver pass its OWN connection
    (build_per_hour_bundles closes it in its finally)."""
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


# ── Writer: happy path ───────────────────────────────────────────────────────


def test_build_writes_per_minute_both_bases(tmp_path):
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
        _seed_logs(c, "logs_olts", rows)
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


def test_build_skips_active_hour(tmp_path):
    """The active UTC hour is still being written; the writer must skip it."""
    from backend.core.rollups import origin_latency_ts

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-olts-active"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_logs(c, "logs_olts", [])
        return c

    p = _build_patches(cache_root, "logs_olts", _fresh_con)
    with p[0], p[1], p[2], p[3], p[4]:
        n = origin_latency_ts.build_origin_latency_ts_bundles("svc-olts-active", src, [active_token])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}").exists()


def test_build_missing_ottlb_writes_ttlb_zero_null(tmp_path):
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


def test_build_no_latency_writes_nothing(tmp_path):
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
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "origin_latency_ts.parquet").exists()


# ── Backfill driver ──────────────────────────────────────────────────────────


def test_backfill_skips_built_hours(tmp_path):
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


# ── Reader eligibility gates ─────────────────────────────────────────────────


def test_reader_eligibility_gates(tmp_path):
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


def test_reader_fail_closed_on_missing_closed_hour(tmp_path):
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
        os.path.join(str(cache_root), "rollups", "hour_bundled", f"hour={missing_hour}", "origin_latency_ts.parquet")
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


def test_reader_serves_closed_hours(tmp_path):
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


# ── Day compactor: preserves minute dimension ────────────────────────────────


def test_compact_preserves_minutes(tmp_path):
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


def test_compact_skips_active_day(tmp_path):
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


# ── Accuracy spot-check: rollup reader vs live temp path ─────────────────────


def _origin_lat_us(r: dict) -> float | None:
    ottfb = r.get("ottfb")
    ttfb = r.get("ttfb")
    if ottfb is not None:
        return float(ottfb)
    if ttfb is not None:
        return float(ttfb) * 1_000_000.0
    return None


def test_accuracy_rollup_reader_matches_live_temp_path(tmp_path):
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
        _seed_logs(c, "logs_acc", rows)
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
