"""Tests for ``backend.core.local_compaction``.

The contract:

  * Only partitions with strictly more than ``min_files_per_partition``
    files get merged.
  * After a successful pass, each compacted partition has exactly ONE
    parquet file (the merged one); the originals are deleted.
  * Row counts are preserved across the merge (no data loss).
  * Errors in one partition don't abort the whole pass.
  * dry_run reports counts without writing.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core import local_compaction as lc
from backend.core.local_compaction import _compact_single_partition
from tests._helpers.sources import make_source


# ── Merged from tests/core/test_local_compaction.py ──
def _write_parquet(path: str, rows: int, ts_start: int = 0, rid_start: int | None = None) -> None:
    """Write a tiny parquet file with `rows` records. When ``rid_start``
    is provided, every row gets a unique ``rid`` (used to exercise the
    dedup-by-rid pass)."""
    cols = {
        "timestamp": pa.array(range(ts_start, ts_start + rows), type=pa.int64()),
        "ip": pa.array([f"10.0.0.{i % 255}" for i in range(rows)]),
        "status": pa.array([200 + (i % 5) for i in range(rows)], type=pa.int32()),
    }
    if rid_start is not None:
        cols["rid"] = pa.array([f"r{rid_start + i}" for i in range(rows)])
    table = pa.table(cols)
    pq.write_table(table, path, compression="zstd")


_NAME = "test-svc-local-compact"


def _local_compact_source(tmp_path) -> dict:
    """Source whose ``_cache_dir`` resolves under tmp_path (``data/`` pre-created)."""
    return make_source(tmp_path / "cache" / _NAME, _NAME, cache_key="_test_cache_root", mkdirs=("data",))


@pytest.fixture
def patched_cache_dir(tmp_path, monkeypatch):
    """Patch ``_cache_dir`` to point at a tmp directory for the test."""
    src = _local_compact_source(tmp_path)

    def fake_cache_dir(source: dict) -> str:
        return source["_test_cache_root"]

    monkeypatch.setattr("backend.core.duckdb._cache_dir", fake_cache_dir)
    # Insulate the hourly tests from temporal drift. Tests use fixed date
    # strings like "2026-05-15"; without these pins, once those dates drift
    # past the default _DAILY_TIER_AGE_DAYS=30 / _WEEKLY_TIER_AGE_DAYS=30
    # boundaries the daily/weekly tiers silently roll up the hour partitions
    # and the hour-tier tests fail. Pin both to 365 so neither tier
    # activates for any test that doesn't explicitly want it; tests that DO
    # want daily/weekly behavior already override the relevant constant
    # locally.
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 365)
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 365)
    return src


def test_skips_partitions_below_threshold(patched_cache_dir):
    """A single-file partition is left alone — no compaction to do."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-00")
    os.makedirs(part)
    # Only 1 file; default min_files_per_partition=1 means we need >1.
    _write_parquet(os.path.join(part, "f0.parquet"), rows=10, ts_start=0)

    result = lc.compact_local_partitions(src)

    assert result["partitions_scanned"] == 0
    assert result["partitions_compacted"] == 0
    assert len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 1


def test_dedup_removes_cross_file_duplicate_rids(patched_cache_dir):
    """Two parquet files in the same partition containing OVERLAPPING rids
    (the orphan-pattern produced by the buffer-commit ↔ tombstone race)
    must merge into ONE file with each rid appearing exactly once. Without
    this guarantee the dashboard double-counts every request for hours
    affected by the race (the 2026-06-12 audit found ~12 days affected)."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-02")
    os.makedirs(part)
    # File A: rids 1..10. File B: rids 6..15 (5 overlap with A). Merged
    # file should contain rids 1..15 (15 unique), not 20 rows.
    _write_parquet(os.path.join(part, "a.parquet"), rows=10, ts_start=0, rid_start=1)
    _write_parquet(os.path.join(part, "b.parquet"), rows=10, ts_start=10, rid_start=6)

    result = lc.compact_local_partitions(src)
    assert result["partitions_compacted"] == 1

    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged_path = os.path.join(part, remaining[0])
    import duckdb as _ddb

    con = _ddb.connect(":memory:")
    try:
        n_rows, n_uniq = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT rid) FROM read_parquet('{merged_path}')"
        ).fetchone()
    finally:
        con.close()
    assert n_rows == 15, f"merged file must dedupe by rid, got {n_rows} rows"
    assert n_uniq == 15


def test_merges_partitions_above_threshold(patched_cache_dir):
    """A partition with > min_files files is merged into one."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-01")
    os.makedirs(part)
    for i in range(5):  # 5 > 3 → eligible
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)

    result = lc.compact_local_partitions(src)

    assert result["partitions_compacted"] == 1
    assert result["files_merged"] == 5
    assert result["files_removed"] == 5
    # Exactly one parquet left, named "compacted_*".
    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    assert remaining[0].startswith("compacted_")


def test_row_count_preserved_across_merge(patched_cache_dir):
    """The merged file contains every row from every input file."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-02")
    os.makedirs(part)
    total_rows = 0
    for i in range(4):
        rows = (i + 1) * 10
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=rows, ts_start=i * 100)
        total_rows += rows

    lc.compact_local_partitions(src)

    # Read the merged file and verify total row count.
    merged = [f for f in os.listdir(part) if f.endswith(".parquet")][0]
    merged_path = os.path.join(part, merged)
    con = duckdb.connect(":memory:")
    try:
        actual = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged_path}')").fetchone()[0]
    finally:
        con.close()
    assert actual == total_rows


def test_dry_run_reports_without_writing(patched_cache_dir):
    """dry_run=True reports what WOULD happen but doesn't touch disk."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-03")
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)

    result = lc.compact_local_partitions(src, dry_run=True)

    assert result["dry_run"] is True
    assert result["partitions_compacted"] == 1
    assert result["files_merged"] == 5
    # No writes — originals still there, no merged file.
    files = sorted(os.listdir(part))
    assert files == [f"f{i}.parquet" for i in range(5)]


def test_empty_cache_dir_is_a_noop(tmp_path, monkeypatch):
    """Missing data/ directory is fine; the function returns zero counts."""
    src = {"name": "empty-svc", "_test_cache_root": str(tmp_path / "cache" / "empty-svc")}
    os.makedirs(src["_test_cache_root"])

    def fake_cache_dir(source: dict) -> str:
        return source["_test_cache_root"]

    monkeypatch.setattr("backend.core.duckdb._cache_dir", fake_cache_dir)

    result = lc.compact_local_partitions(src)

    assert result["partitions_scanned"] == 0
    assert result["errors"] == []


def test_active_hour_is_never_compacted(patched_cache_dir, monkeypatch):
    """The current UTC hour partition is left alone — sync may be appending."""
    from datetime import UTC, datetime

    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    active = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    part = os.path.join(cache_root, "data", active)
    os.makedirs(part)
    for i in range(5):  # would normally be eligible
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10, ts_start=i * 10)

    result = lc.compact_local_partitions(src)

    assert result["active_hour_skipped"] is True
    assert result["partitions_compacted"] == 0
    # Originals untouched.
    assert len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 5


def test_stale_tmp_files_get_cleaned(patched_cache_dir):
    """`.tmp_*.parquet` leftovers from crashed runs are removed."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-15-00")
    os.makedirs(part)
    # One real file (not enough to trigger compaction), one stale tmp.
    _write_parquet(os.path.join(part, "real.parquet"), rows=10)
    stale = os.path.join(part, ".tmp_compacted_dead.parquet")
    with open(stale, "wb") as f:
        f.write(b"garbage")

    result = lc.compact_local_partitions(src)

    assert result["stale_tmp_removed"] == 1
    assert not os.path.exists(stale)
    assert os.path.exists(os.path.join(part, "real.parquet"))


def test_partition_above_size_ceiling_is_skipped(patched_cache_dir, monkeypatch):
    """Don't merge a partition whose total size > _MAX_PARTITION_BYTES."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-04")
    os.makedirs(part)
    for i in range(5):
        _write_parquet(os.path.join(part, f"f{i}.parquet"), rows=10)
    # Lower the ceiling well below the test partition's total size.
    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", 100)

    result = lc.compact_local_partitions(src)

    assert result["partitions_compacted"] == 0
    # All originals still present.
    assert len([f for f in os.listdir(part) if f.endswith(".parquet")]) == 5


def test_daily_tier_rolls_up_old_partitions(patched_cache_dir, monkeypatch):
    """Hour partitions older than _DAILY_TIER_AGE_DAYS merge into a single
    daily file under data/daily/."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")
    # Force the daily-tier threshold to 0 so EVERY non-current partition is old.
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)

    # Create 3 hour partitions for the same day with 1 file each.
    day = "2026-05-15"
    for hh in ("00", "01", "02"):
        part = os.path.join(data_dir, f"timestamp_hour={day}-{hh}")
        os.makedirs(part)
        _write_parquet(os.path.join(part, "f0.parquet"), rows=10)

    result = lc.compact_local_partitions(src)

    assert result["daily_rollups"] == 1
    # The daily/ dir now has one merged file.
    daily_dir = os.path.join(data_dir, "daily")
    daily_files = [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]
    assert len(daily_files) == 1
    assert daily_files[0].startswith(f"daily_{day}_")
    # The hour partitions are gone.
    for hh in ("00", "01", "02"):
        assert not os.path.isdir(os.path.join(data_dir, f"timestamp_hour={day}-{hh}"))


def test_weekly_tier_rolls_up_old_daily_files(patched_cache_dir, monkeypatch):
    """Daily files older than _WEEKLY_TIER_AGE_DAYS that fall in the same
    ISO week merge into a single weekly file under data/weekly/."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")
    daily_dir = os.path.join(data_dir, "daily")
    os.makedirs(daily_dir)
    # Force the threshold to 0 so every daily file is eligible.
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)
    # 3 daily files in the same ISO week (2026-W19: Mon May 4 – Sun May 10).
    for day in ("2026-05-04", "2026-05-05", "2026-05-06"):
        _write_parquet(os.path.join(daily_dir, f"daily_{day}_abc12345.parquet"), rows=10)

    result = lc.compact_local_partitions(src)

    assert result["weekly_rollups"] == 1
    weekly_dir = os.path.join(data_dir, "weekly")
    weekly_files = [f for f in os.listdir(weekly_dir) if f.endswith(".parquet")]
    assert len(weekly_files) == 1
    assert weekly_files[0].startswith("weekly_2026-W19_")
    # Originals removed.
    assert not [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]


def test_weekly_tier_skips_single_day_weeks(patched_cache_dir, monkeypatch):
    """A week with only one daily file is left alone — nothing to merge."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    daily_dir = os.path.join(cache_root, "data", "daily")
    os.makedirs(daily_dir)
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)
    _write_parquet(os.path.join(daily_dir, "daily_2026-05-04_deadbeef.parquet"), rows=10)

    result = lc.compact_local_partitions(src)

    assert result["weekly_rollups"] == 0
    weekly_dir = os.path.join(cache_root, "data", "weekly")
    assert not os.path.isdir(weekly_dir) or not os.listdir(weekly_dir)


def test_compaction_stats_snapshot(patched_cache_dir):
    """Stats helper returns counts useful for monitoring."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")
    # 2 partitions: one with 5 files (above threshold), one with 1 file.
    p1 = os.path.join(data_dir, "timestamp_hour=2026-05-20-00")
    os.makedirs(p1)
    for i in range(5):
        _write_parquet(os.path.join(p1, f"f{i}.parquet"), rows=10)
    p2 = os.path.join(data_dir, "timestamp_hour=2026-05-20-01")
    os.makedirs(p2)
    _write_parquet(os.path.join(p2, "f0.parquet"), rows=10)

    s = lc.compaction_stats(src)

    assert s["total_files"] == 6
    assert s["partitions"] == 2
    assert s["partitions_above_3"] == 1
    assert s["partitions_above_10"] == 0
    assert s["avg_files_per_partition"] == 3.0


def test_daily_tier_bin_packing_splits_files(patched_cache_dir, monkeypatch):
    """If a day's files exceed _MAX_PARTITION_BYTES, they are split into multiple daily files."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")

    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)

    day = "2026-05-15"
    paths = []
    for hh in ("00", "01", "02"):
        part = os.path.join(data_dir, f"timestamp_hour={day}-{hh}")
        os.makedirs(part)
        p = os.path.join(part, "f0.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    sizes = [os.path.getsize(p) for p in paths]
    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", sizes[0] + 50)

    result = lc.compact_local_partitions(src)

    daily_dir = os.path.join(data_dir, "daily")
    daily_files = sorted([f for f in os.listdir(daily_dir) if f.endswith(".parquet")])
    assert len(daily_files) == 3
    for f in daily_files:
        assert f.startswith(f"daily_{day}_")

    con = duckdb.connect(":memory:")
    try:
        total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{daily_dir}/*.parquet')").fetchone()[0]
    finally:
        con.close()
    assert total_rows == 30

    for hh in ("00", "01", "02"):
        assert not os.path.isdir(os.path.join(data_dir, f"timestamp_hour={day}-{hh}"))


def test_weekly_tier_bin_packing_splits_files(patched_cache_dir, monkeypatch):
    """If a week's daily files exceed _MAX_PARTITION_BYTES, they are split into multiple weekly files."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")
    daily_dir = os.path.join(data_dir, "daily")
    os.makedirs(daily_dir)

    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 0)

    paths = []
    for day in ("2026-05-04", "2026-05-05", "2026-05-06"):
        p = os.path.join(daily_dir, f"daily_{day}_abc12345.parquet")
        _write_parquet(p, rows=10)
        paths.append(p)

    sizes = [os.path.getsize(p) for p in paths]
    monkeypatch.setattr("backend.core.local_compaction._MAX_PARTITION_BYTES", sizes[0] + 50)

    result = lc.compact_local_partitions(src)

    weekly_dir = os.path.join(data_dir, "weekly")
    weekly_files = sorted([f for f in os.listdir(weekly_dir) if f.endswith(".parquet")])
    assert len(weekly_files) == 3
    for f in weekly_files:
        assert f.startswith("weekly_2026-W19_")

    con = duckdb.connect(":memory:")
    try:
        total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{weekly_dir}/*.parquet')").fetchone()[0]
    finally:
        con.close()
    assert total_rows == 30

    assert not [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]


def test_daily_tier_migrates_single_file_bins_and_removes_dir(patched_cache_dir, monkeypatch):
    """Daily compaction correctly migrates a single-file hourly partition to the daily folder
    and removes the hourly partition dir."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")

    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)

    day = "2026-05-15"
    part = os.path.join(data_dir, f"timestamp_hour={day}-00")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "single_file.parquet"), rows=10)

    lc.compact_local_partitions(src)

    daily_dir = os.path.join(data_dir, "daily")
    daily_files = [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]
    assert len(daily_files) == 1
    assert daily_files[0].startswith(f"daily_{day}_")

    assert not os.path.isdir(part)


def _write_parquet_with_explicit_rids(path: str, rids: list[str | None], ts_offsets: list[int]) -> None:
    """Like ``_write_parquet`` but takes explicit per-row rid + timestamp.

    Lets the caller mix NULL and non-NULL rids in one file to exercise
    the dedup branch on a row-by-row basis.
    """
    assert len(rids) == len(ts_offsets)
    cols = {
        "timestamp": pa.array(ts_offsets, type=pa.int64()),
        "ip": pa.array([f"10.0.0.{i}" for i in range(len(rids))]),
        "status": pa.array([200] * len(rids), type=pa.int32()),
        "rid": pa.array(rids, type=pa.string()),
    }
    pq.write_table(pa.table(cols), path, compression="zstd")


def test_null_rid_rows_pass_through_unchanged(patched_cache_dir):
    """NULL-rid rows must be preserved verbatim (no dedup, no drop).

    The dedup SQL filters ``WHERE rid IS NOT NULL`` for the dedup branch
    and ``WHERE rid IS NULL`` for the passthrough branch — without the
    second branch every NULL-rid row would silently disappear.
    """
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-03")
    os.makedirs(part)
    # File A: 2 NULL-rid rows + 1 non-NULL. File B: 1 NULL-rid + 1 dup of A's non-NULL.
    _write_parquet_with_explicit_rids(
        os.path.join(part, "a.parquet"),
        rids=[None, None, "shared-1"],
        ts_offsets=[0, 1, 2],
    )
    _write_parquet_with_explicit_rids(
        os.path.join(part, "b.parquet"),
        rids=[None, "shared-1"],
        ts_offsets=[3, 4],
    )

    result = lc.compact_local_partitions(src)
    assert result["partitions_compacted"] == 1

    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged = os.path.join(part, remaining[0])

    con = duckdb.connect(":memory:")
    try:
        n_total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}')").fetchone()[0]
        n_null = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}') WHERE rid IS NULL").fetchone()[0]
        n_non_null = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}') WHERE rid IS NOT NULL").fetchone()[0]
    finally:
        con.close()

    # All 3 NULL-rid rows from both files preserved + 1 deduped non-NULL = 4.
    assert n_null == 3, f"NULL-rid rows must pass through unchanged, got {n_null}"
    assert n_non_null == 1, f"non-NULL rid must dedupe to 1, got {n_non_null}"
    assert n_total == 4


def test_dedup_keeps_earliest_timestamp_for_same_rid(patched_cache_dir):
    """``ROW_NUMBER() OVER (PARTITION BY rid ORDER BY timestamp)`` keeps
    the row with the lowest timestamp — pinned because a refactor that
    flipped to ``DESC`` would silently invert the dedup behaviour and
    surface as last-write-wins instead of first-write-wins.
    """
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-04")
    os.makedirs(part)
    _write_parquet_with_explicit_rids(os.path.join(part, "a.parquet"), rids=["shared"], ts_offsets=[100])
    _write_parquet_with_explicit_rids(os.path.join(part, "b.parquet"), rids=["shared"], ts_offsets=[50])
    _write_parquet_with_explicit_rids(os.path.join(part, "c.parquet"), rids=["shared"], ts_offsets=[200])

    lc.compact_local_partitions(src)

    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged = os.path.join(part, remaining[0])

    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(f"SELECT timestamp FROM read_parquet('{merged}') WHERE rid = 'shared'").fetchall()
    finally:
        con.close()
    # Exactly one row, with the EARLIEST timestamp (50, not 100 or 200).
    assert rows == [(50,)], f"earliest-timestamp must win, got {rows}"


def test_schema_without_rid_column_falls_back_to_plain_select(patched_cache_dir):
    """Older sources (and test fixtures without rid_start) produce
    parquet files with no ``rid`` column. The dedup branch is gated on
    ``has_rid`` — when False, the helper must emit a plain SELECT
    rather than reference the missing column (which would raise).
    """
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-05")
    os.makedirs(part)
    # rid_start=None → no rid column in either file.
    _write_parquet(os.path.join(part, "a.parquet"), rows=5, ts_start=0)
    _write_parquet(os.path.join(part, "b.parquet"), rows=5, ts_start=10)

    # Compaction must succeed (no SQL error from missing column).
    result = lc.compact_local_partitions(src)
    assert result["partitions_compacted"] == 1

    remaining = [f for f in os.listdir(part) if f.endswith(".parquet")]
    assert len(remaining) == 1
    merged = os.path.join(part, remaining[0])

    con = duckdb.connect(":memory:")
    try:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{merged}')").fetchone()[0]
        # rid column absent in output schema.
        cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{merged}')").fetchall()}
    finally:
        con.close()
    # No dedup applied → all 10 rows preserved.
    assert n == 10
    assert "rid" not in cols


def test_compact_without_ip_column(tmp_path):
    # Create fake parquet files with timestamp and some data but no ip column
    con = duckdb.connect(":memory:")

    file1_name = "file1.parquet"
    file1 = os.path.join(tmp_path, file1_name)
    con.execute("CREATE TABLE t1 (timestamp TIMESTAMP, value INTEGER)")
    con.execute("INSERT INTO t1 VALUES ('2026-08-19 12:00:00', 42)")
    con.execute(f"COPY t1 TO '{file1}' (FORMAT PARQUET)")
    con.execute("DROP TABLE t1")

    file2_name = "file2.parquet"
    file2 = os.path.join(tmp_path, file2_name)
    con.execute("CREATE TABLE t2 (timestamp TIMESTAMP, value INTEGER)")
    con.execute("INSERT INTO t2 VALUES ('2026-08-19 12:05:00', 100)")
    con.execute(f"COPY t2 TO '{file2}' (FORMAT PARQUET)")
    con.close()

    # We will test _compact_single_partition directly on these files.
    # We must ensure that it successfully merges them into a single file and deletes the originals
    # without raising a Binder Error: Referenced column "ip" not found.
    parquets = [file1_name, file2_name]
    out_dir = str(tmp_path)

    # Run compaction!
    res = _compact_single_partition(out_dir, parquets)

    assert res is not None
    assert res["files_removed"] == 2
    assert len(res["removed_basenames"]) == 2

    # Verify the merged file exists and contains the sorted data
    merged_files = [f for f in os.listdir(out_dir) if f.endswith(".parquet") and f not in parquets]
    assert len(merged_files) == 1

    merged_path = os.path.join(out_dir, merged_files[0])
    con = duckdb.connect(":memory:")
    rows = con.execute(f"SELECT * FROM read_parquet('{merged_path}') ORDER BY timestamp").fetchall()
    assert len(rows) == 2
    assert rows[0][1] == 42
    assert rows[1][1] == 100
    con.close()


# ── Merged from tests/core/test_local_compaction_branches.py ──
def test_build_merge_select_sql_no_rid_plain_select():
    """No ``rid`` column → plain SELECT * with EXCLUDE clause."""
    sql = lc._build_merge_select_sql("'a.parquet', 'b.parquet'", [], has_rid=False)
    assert "ROW_NUMBER" not in sql
    assert "UNION ALL" not in sql
    assert "read_parquet" in sql


def test_build_merge_select_sql_no_rid_with_exclude_cols():
    """EXCLUDE list flows into the SELECT when present."""
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour", "dt"], has_rid=False)
    assert "EXCLUDE (timestamp_hour, dt)" in sql


def test_build_merge_select_sql_with_rid_emits_dedup_window():
    """``rid`` present → dedup-by-rid via ROW_NUMBER, NULL-rid pass-through
    UNION ALL'd at the end."""
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour"], has_rid=True)
    assert "ROW_NUMBER() OVER (PARTITION BY rid" in sql
    assert "UNION ALL BY NAME" in sql
    assert "WHERE rid IS NULL" in sql
    # The _dup_rn helper column is excluded from the output.
    assert "_dup_rn" in sql  # appears in EXCLUDE
    assert "WHERE _dup_rn = 1" in sql


def test_build_merge_select_sql_with_natural_keys_vitals():
    """RUM client_vitals natural keys → dedup via ROW_NUMBER over composite key."""
    keys = ("timestamp", "metric_name", "pathname", "cid", "req_id")
    sql = lc._build_merge_select_sql("'a.parquet'", ["timestamp_hour"], has_rid=False, natural_keys=keys)
    assert (
        'ROW_NUMBER() OVER (PARTITION BY "timestamp", "metric_name", "pathname", "cid", "req_id" ORDER BY timestamp) AS _dup_rn'
        in sql
    )
    assert "WHERE _dup_rn = 1" in sql
    assert "_dup_rn" in sql
    assert "UNION ALL" not in sql


def test_build_merge_select_sql_with_natural_keys_errors():
    """RUM client_errors natural keys → dedup via ROW_NUMBER over composite key."""
    keys = ("timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id")
    sql = lc._build_merge_select_sql("'a.parquet'", [], has_rid=False, natural_keys=keys)
    assert 'PARTITION BY "timestamp", "error_message", "error_file", "error_line", "pathname", "cid", "req_id"' in sql
    assert "WHERE _dup_rn = 1" in sql
    assert "EXCLUDE (_dup_rn)" in sql


def test_detect_natural_keys():
    """_detect_natural_keys accurately identifies vitals and errors schemas."""
    vitals_cols = {"timestamp", "metric_name", "pathname", "cid", "req_id", "metric_value", "browser"}
    assert lc._detect_natural_keys(vitals_cols) == ("timestamp", "metric_name", "pathname", "cid", "req_id")

    errors_cols = {
        "timestamp",
        "error_message",
        "error_file",
        "error_line",
        "pathname",
        "cid",
        "req_id",
        "browser",
    }
    assert lc._detect_natural_keys(errors_cols) == (
        "timestamp",
        "error_message",
        "error_file",
        "error_line",
        "pathname",
        "cid",
        "req_id",
    )

    request_cols = {"timestamp", "rid", "service_id", "status"}
    assert lc._detect_natural_keys(request_cols) is None


def test_bin_pack_files_empty_returns_empty(tmp_path):
    assert lc._bin_pack_files([], 1024) == []


def test_bin_pack_files_single_small_file(tmp_path):
    p = tmp_path / "a.parquet"
    p.write_bytes(b"x" * 100)
    bins = lc._bin_pack_files([str(p)], 1024)
    assert bins == [[str(p)]]


def test_bin_pack_files_packs_under_cap(tmp_path):
    """Multiple files whose sum is under the cap go in one bin."""
    files = []
    for name, size in [("a.parquet", 200), ("b.parquet", 200), ("c.parquet", 200)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    bins = lc._bin_pack_files(files, 1024)
    assert bins == [files]


def test_bin_pack_files_starts_new_bin_when_cap_exceeded(tmp_path):
    """When adding a file would push the bin over the cap, a new bin
    starts. Preserves original order across bins."""
    files = []
    for name, size in [("a.parquet", 400), ("b.parquet", 400), ("c.parquet", 400)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    bins = lc._bin_pack_files(files, 1000)
    # 400+400=800 fits in bin 1; +400=1200>1000 → c goes alone.
    assert len(bins) == 2
    assert bins[0] == files[:2]
    assert bins[1] == [files[2]]


def test_bin_pack_files_huge_single_file_gets_own_bin(tmp_path):
    """A single file bigger than the cap goes in its own bin (can't be
    split). Pinned because the alternative (skip) would silently leave
    data behind."""
    files = []
    for name, size in [("a.parquet", 100), ("huge.parquet", 5000), ("b.parquet", 100)]:
        p = tmp_path / name
        p.write_bytes(b"x" * size)
        files.append(str(p))
    bins = lc._bin_pack_files(files, 1000)
    # Order: [a alone in bin1 because adding huge=5000 would exceed 1000],
    # then huge alone, then b in its own bin.
    assert [str(tmp_path / "huge.parquet")] in bins
    # All 3 files accounted for across the bins.
    flat = [p for b in bins for p in b]
    assert sorted(flat) == sorted(files)


def test_bin_pack_files_skips_files_that_oserror_on_getsize(tmp_path):
    """Line 132-133: an OSError on getsize (file vanished mid-scan) is
    swallowed — the file is dropped from binning rather than crashing
    the compaction job."""
    real = tmp_path / "real.parquet"
    real.write_bytes(b"x" * 100)
    ghost = str(tmp_path / "ghost.parquet")  # never created
    bins = lc._bin_pack_files([ghost, str(real)], 1024)
    # ghost is silently dropped; real is in one bin.
    assert bins == [[str(real)]]


def test_cleanup_stale_tmp_removes_both_naming_conventions(tmp_path):
    """Both legacy (.tmp_<name>.parquet) and current (<name>.parquet.tmp)
    naming conventions are removed."""
    # New naming
    (tmp_path / "a.parquet.tmp").write_bytes(b"")
    # Legacy naming (.tmp_ prefix + .parquet suffix)
    (tmp_path / ".tmp_b.parquet").write_bytes(b"")
    # Not a tmp file — must be left alone
    (tmp_path / "keep.parquet").write_bytes(b"keep")
    # Nested
    nested = tmp_path / "sub"
    nested.mkdir()
    (nested / "c.parquet.tmp").write_bytes(b"")

    n = lc._cleanup_stale_tmp(str(tmp_path))
    assert n == 3
    assert (tmp_path / "keep.parquet").exists()
    assert not (tmp_path / "a.parquet.tmp").exists()
    assert not (tmp_path / ".tmp_b.parquet").exists()
    assert not (nested / "c.parquet.tmp").exists()


def test_cleanup_stale_tmp_swallows_oserror(tmp_path):
    """Line 366-367: OSError on os.remove (permission, file vanished)
    is caught — the loop continues and the count reflects only
    successful removals."""
    (tmp_path / "a.parquet.tmp").write_bytes(b"")
    (tmp_path / "b.parquet.tmp").write_bytes(b"")

    real_remove = os.remove
    call_count = {"n": 0}

    def _maybe_fail(p):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise OSError("simulated")
        return real_remove(p)

    with patch("os.remove", side_effect=_maybe_fail):
        n = lc._cleanup_stale_tmp(str(tmp_path))

    # 2 attempts, 1 succeeded.
    assert n == 1


def test_compact_local_partitions_returns_skeleton_when_data_dir_missing(tmp_path):
    """No data/ dir → return the empty result skeleton without doing
    any work."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = lc.compact_local_partitions(src)
    assert out["partitions_scanned"] == 0
    assert out["files_merged"] == 0
    assert out["errors"] == []
    assert out["duration_ms"] >= 0
    assert out["dry_run"] is False


def test_compact_local_partitions_active_hour_flag_set(tmp_path):
    """When the active-UTC-hour partition exists on disk, the
    ``active_hour_skipped`` flag is True (downstream cron-status
    surface)."""
    from datetime import UTC, datetime

    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    active = datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
    (data_dir / active).mkdir()

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = lc.compact_local_partitions(src)
    assert out["active_hour_skipped"] is True


def test_compact_local_partitions_skips_non_directory_entries(tmp_path):
    """Line 240: random files at data/ root are skipped (the loop only
    descends into directories that look like partitions)."""
    src = {"name": "svc", "_cache_dir_override": str(tmp_path)}
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    # A file (not a directory) at the root — must be skipped, not crash.
    (data_dir / "stray.txt").write_bytes(b"x")
    # A directory that's not a partition (no parquet files inside) — handled.
    (data_dir / "empty_part").mkdir()

    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path)):
        out = lc.compact_local_partitions(src)
    # No errors, no work done.
    assert out["errors"] == []
    assert out["partitions_scanned"] == 0
