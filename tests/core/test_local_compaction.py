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

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core import local_compaction as lc
from tests._helpers.sources import make_source


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


def test_compaction_registers_deleted_basenames(patched_cache_dir, monkeypatch):
    """After merging + deleting originals, the deleted basenames must land
    in the local_compacted_files registry so sync_data won't re-download
    them. This is the fix for the compaction-vs-sync feedback loop."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    part = os.path.join(cache_root, "data", "timestamp_hour=2026-05-30-05")
    os.makedirs(part)
    for i in range(5):  # >3 → eligible
        _write_parquet(os.path.join(part, f"original-{i}.parquet"), rows=10)

    captured: list[tuple[str, list[str]]] = []

    def fake_register(service_id: str, names: list[str]) -> None:
        captured.append((service_id, list(names)))

    monkeypatch.setattr("backend.core.metadata.register_locally_compacted", fake_register)

    result = lc.compact_local_partitions(src)

    assert result["partitions_compacted"] == 1
    assert len(captured) == 1
    service_id, names = captured[0]
    assert service_id == src["service_id"]
    assert sorted(names) == sorted(f"original-{i}.parquet" for i in range(5))


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
    """Daily compaction correctly migrates a single-file hourly partition to the daily folder,
    removes the hourly partition dir, and registers its basename in the deleted registry."""
    src = patched_cache_dir
    cache_root = src["_test_cache_root"]
    data_dir = os.path.join(cache_root, "data")

    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 0)

    day = "2026-05-15"
    part = os.path.join(data_dir, f"timestamp_hour={day}-00")
    os.makedirs(part)
    _write_parquet(os.path.join(part, "single_file.parquet"), rows=10)

    captured: list[tuple[str, list[str]]] = []

    def fake_register(service_id: str, names: list[str]) -> None:
        captured.append((service_id, list(names)))

    monkeypatch.setattr("backend.core.metadata.register_locally_compacted", fake_register)

    result = lc.compact_local_partitions(src)

    daily_dir = os.path.join(data_dir, "daily")
    daily_files = [f for f in os.listdir(daily_dir) if f.endswith(".parquet")]
    assert len(daily_files) == 1
    assert daily_files[0].startswith(f"daily_{day}_")

    assert len(captured) == 1
    assert captured[0][1] == ["single_file.parquet"]

    assert not os.path.isdir(part)


# ── rid-dedup edge cases (audit follow-up) ──────────────────────────────────


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
