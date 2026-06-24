"""Defensive-branch coverage for backend/core/local_compaction.py.

Targets pure functions, OSError handlers, and simple early-return guards
that the heavier integration tests in test_local_compaction.py don't
exercise (those need real parquet files + a real Iceberg view)."""

from __future__ import annotations

import os
from unittest.mock import patch

from backend.core import local_compaction as lc

# ── _build_merge_select_sql: rid + exclude branches ────────────────────────


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


# ── _bin_pack_files: pure-function packing ─────────────────────────────────


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


# ── _cleanup_stale_tmp: walk + OSError handling ───────────────────────────


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


# ── compact_local_partitions: early-return guards ─────────────────────────


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
