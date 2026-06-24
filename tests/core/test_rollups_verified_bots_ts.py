"""Tests for the per-hour minute-granular verified_bots_ts rollup writer +
hybrid reader + day compactor (perf Priority 1).

verified_bots_ts differs from the leaderboard rollups (network_speed /
network_rtt) in two ways:
  - there is NO top-K cut (bot_type is a small fixed vocabulary), and
  - the day compactor PRESERVES the minute (bucket_ts) dimension because
    the panel is a time series, not a leaderboard.

The reader is hybrid: it re-buckets the closed-hour rollup AND fills the
in-progress active hour live from the temp table, merged in one SQL
statement. Math is EXACT (integer SUM); no ``_approx`` flag.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def _write_hour_vbts(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "bucket_ts": pa.array([r["bucket_ts"] for r in rows], type=pa.timestamp("us", tz="UTC")),
            "bot_type": pa.array([r["bot_type"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "verified_bots_ts.parquet")
    pq.write_table(table, p)
    return p


def _write_hour_all_fields(cache_root: str, hour: str) -> None:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table({"field": pa.array(["x"]), "value": pa.array(["y"]), "count": pa.array([1], type=pa.int64())}),
        os.path.join(d, "all_fields.parquet"),
    )


@contextmanager
def _noop_lock(_key):
    yield


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


def _hour_dt(day: str, h: int) -> datetime:
    return datetime.strptime(f"{day}-{h:02d}", "%Y-%m-%d-%H").replace(tzinfo=UTC)


def test_backfill_skips_built_hours(tmp_path):
    """Backfill walks rollups/hour_bundled and only queues hours WITH
    all_fields.parquet AND WITHOUT verified_bots_ts.parquet."""
    from backend.core.rollups import verified_bots_ts

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-vbts-bf", "service_id": "svc-vbts-bf"}
    yday = _yesterday_iso()

    _write_hour_all_fields(str(cache_root), f"{yday}-10")
    _write_hour_all_fields(str(cache_root), f"{yday}-11")
    _write_hour_vbts(
        str(cache_root), f"{yday}-11", [{"bucket_ts": _hour_dt(yday, 11), "bot_type": "google", "count": 3}]
    )

    captured: list[list[str]] = []

    def _stub_build(_sid, _src, hours):
        captured.append(list(hours))
        return len(hours)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch.object(verified_bots_ts, "build_verified_bots_ts_bundles", side_effect=_stub_build):
            n = verified_bots_ts.backfill_verified_bots_ts_bundles("svc-vbts-bf", src)

    assert n == 1
    assert captured == [[f"{yday}-10"]]


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
    runner._table = "logs_svc_vbts_reader"
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


def test_reader_returns_none_when_filtered(tmp_path):
    src = {"name": "svc-vbts-r", "service_id": "svc-vbts-r"}
    runner = _stub_runner(src, [], [])
    result = runner.try_verified_bots_ts_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        temp_table="logs_x_tmp",
        bucket_seconds=3600,
        has_filters=True,
    )
    assert result is None


def test_reader_returns_none_for_short_window(tmp_path):
    src = {"name": "svc-vbts-r2", "service_id": "svc-vbts-r2"}
    runner = _stub_runner(src, [], [])
    result = runner.try_verified_bots_ts_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-01T12:00:00+00:00",
        temp_table="logs_x_tmp",
        bucket_seconds=3600,
        has_filters=False,
    )
    assert result is None


def test_reader_returns_none_for_non_minute_bucket(tmp_path):
    """bucket_seconds not a multiple of 60 → minute-granular re-bucketing
    would be inexact, so the reader bails to the live path."""
    src = {"name": "svc-vbts-r2b", "service_id": "svc-vbts-r2b"}
    runner = _stub_runner(src, [], [])
    result = runner.try_verified_bots_ts_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        temp_table="logs_x_tmp",
        bucket_seconds=90,  # not a multiple of 60
        has_filters=False,
    )
    assert result is None


def test_reader_serves_hybrid_rows(tmp_path):
    """Eligible window → reader builds a UNION ALL of read_parquet (closed
    hours) + a scoped live temp-table query (active hour), and returns the
    (bucket_ts, bot_type, count) rows."""
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-vbts-r3", "service_id": "svc-vbts-r3"}
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()

    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_vbts(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [{"bucket_ts": _hour_dt(d_iso, h), "bot_type": "google", "count": 5}],
            )

    captured: list[str] = []
    stub_rows = [(_hour_dt(day_a, 0), "google", 120), (_hour_dt(day_a, 1), "bing", 40)]
    runner = _stub_runner(src, captured, stub_rows)

    st_iso = f"{day_a}T00:00:00+00:00"
    end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_verified_bots_ts_from_rollup(
            st_iso,
            end_iso,
            temp_table="logs_x_tmp",
            bucket_seconds=3600,
            has_filters=False,
        )

    assert result == [(_hour_dt(day_a, 0), "google", 120), (_hour_dt(day_a, 1), "bing", 40)]
    assert len(captured) == 1
    sql = captured[0]
    assert "verified_bots_ts.parquet" in sql
    assert "read_parquet" in sql
    assert "UNION ALL" in sql
    assert "logs_x_tmp" in sql
    assert "unnest" in sql
    assert "VERIFIED-BOT." in sql
    assert "time_bucket(INTERVAL '3600 seconds'" in sql


def test_compact_writes_per_day_file_preserving_minutes(tmp_path):
    """24 hour files for a closed day → 1 day file that PRESERVES minute
    granularity (one bucket_ts per source minute), unlike the leaderboard
    compactors which collapse the time dimension."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-vbts-cd", "service_id": "svc-vbts-cd"}
    day = _yesterday_iso()

    for h in range(24):
        _write_hour_vbts(
            str(cache_root),
            f"{day}-{h:02d}",
            [
                {"bucket_ts": _hour_dt(day, h), "bot_type": "google", "count": 10},
                {"bucket_ts": _hour_dt(day, h), "bot_type": "bing", "count": 4},
            ],
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_verified_bots_ts_closed_days_to_daily("svc-vbts-cd", src)

    assert rebuilt == 1
    day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "verified_bots_ts.parquet"
    assert day_file.exists()

    con = duckdb.connect(":memory:")
    try:
        n_rows, n_buckets, n_google, n_bing = con.execute(
            f"SELECT count(*), count(DISTINCT bucket_ts), "
            f"       SUM(count) FILTER (WHERE bot_type='google'), "
            f"       SUM(count) FILTER (WHERE bot_type='bing') "
            f"FROM read_parquet('{day_file}')"
        ).fetchone()
    finally:
        con.close()
    # 24 hours × 2 bot_types = 48 rows; 24 distinct minute buckets;
    # google total 24×10=240, bing 24×4=96.
    assert n_rows == 48
    assert n_buckets == 24
    assert n_google == 240
    assert n_bing == 96


def test_compact_skips_active_day(tmp_path):
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-vbts-active", "service_id": "svc-vbts-active"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    for h in range(24):
        _write_hour_vbts(
            str(cache_root),
            f"{today}-{h:02d}",
            [{"bucket_ts": _hour_dt(today, h), "bot_type": "google", "count": 5}],
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_verified_bots_ts_closed_days_to_daily("svc-vbts-active", src)

    assert rebuilt == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()
