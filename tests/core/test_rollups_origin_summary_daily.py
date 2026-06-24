"""Tests for the origin_summary closed-day compactor + reader's
day-prefer path (Task C).

Pinned here:

* ``compact_origin_summary_closed_days_to_daily``: 24 per-hour
  ``origin_summary.parquet`` files for a closed UTC day get rolled
  into one ``day_bundled/day=YYYY-MM-DD/origin_summary.parquet``
  with the same schema, SUM-for-counts + request-weighted-average-
  for-percentiles. Active day skipped. mtime-gated. In-memory DuckDB
  so it doesn't contend with uvicorn's RW connection on the per-
  service .duckdb file (2026-06-06 incident lesson).

* ``QueryRunner.try_origin_summary_from_rollup``: prefers the per-day
  compacted file when the whole UTC day is in window AND the file
  exists; falls back to per-hour for partial-day edges or missing
  day files. Math invariant: reading via day file produces the same
  weighted aggregate as reading the 24 hour files directly.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ── Fixture helpers ─────────────────────────────────────────────────────


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


@contextmanager
def _noop_lock(_key):
    yield


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


# ── compact_origin_summary_closed_days_to_daily ─────────────────────────


def test_compact_writes_per_day_file_with_correct_aggregates(tmp_path):
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


def test_compact_skips_active_day(tmp_path):
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


def test_compact_is_idempotent_until_inputs_change(tmp_path):
    """Re-running with the day file newer than all hour inputs returns 0
    rebuilt. Touching an hour file → next run rebuilds."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-os-idem", "service_id": "svc-os-idem"}
    day = _yesterday_iso()

    hour_paths = [_write_hour_summary(str(cache_root), f"{day}-{h:02d}", _uniform_row(10, 1000.0)) for h in range(24)]

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            first = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
            assert first == 1
            second = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
            assert second == 0, "mtime gate should suppress rebuilds when nothing changed"

            # Touch one hour file so its mtime jumps past the day file.
            future = (
                os.path.getmtime(cache_root / "rollups" / "day_bundled" / f"day={day}" / "origin_summary.parquet") + 5
            )
            os.utime(hour_paths[0], (future, future))
            third = rollups.compact_origin_summary_closed_days_to_daily("svc-os-idem", src)
            assert third == 1, "stale day file should rebuild when an hour input is newer"


# ── try_origin_summary_from_rollup prefers day file ─────────────────────


def test_reader_uses_day_file_when_whole_day_in_window(tmp_path):
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


def test_reader_falls_back_to_hour_when_day_file_missing(tmp_path):
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
