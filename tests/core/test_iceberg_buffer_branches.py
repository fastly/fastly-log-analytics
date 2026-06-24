"""Defensive-branch coverage for backend/core/iceberg/buffer.py.

The bulk of buffer/commit testing lives across test_buffer_commit_*.py
and test_iceberg_helpers.py. This file targets the simpler helpers and
defensive branches not exercised elsewhere."""

from __future__ import annotations

import json
import os
import time
from unittest.mock import patch

import pytest

from backend.core.iceberg.buffer import (
    _buffer_basename_marker,
    _is_tombstone_marker,
    _quarantine_buffer_file,
    _tombstone_marker_path,
    _tombstoned_parquet_paths,
    buffer_backlog_stats,
    buffer_files,
    sweep_tombstoned_buffer_files,
    tombstone_buffer_files,
)

# ── _buffer_basename_marker: deterministic 12-hex hash ───────────────────


def test_buffer_basename_marker_is_12_hex_chars():
    """Pinned because the marker lands in Iceberg snapshot summary
    metadata.json and the on-disk size is load-bearing for catalog
    perf at scale."""
    marker = _buffer_basename_marker("batch_abc.parquet")
    assert len(marker) == 12
    assert all(c in "0123456789abcdef" for c in marker)


def test_buffer_basename_marker_is_deterministic():
    """Same basename → same marker, every time. Recovery on restart
    matches snapshot markers against buffer basenames and relies on
    determinism."""
    assert _buffer_basename_marker("x.parquet") == _buffer_basename_marker("x.parquet")


def test_buffer_basename_marker_differs_for_distinct_basenames():
    """Different basenames → different markers (the whole point)."""
    assert _buffer_basename_marker("a.parquet") != _buffer_basename_marker("b.parquet")


# ── _is_tombstone_marker: substring matcher precision ───────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("batch_abc.parquet.consumed-1700000000", True),
        ("batch_abc.parquet.consumed-0", True),
        # Non-tombstones
        ("batch_abc.parquet", False),
        ("batch.parquet.consumed-", False),  # empty timestamp
        ("batch.parquet.consumed-notanint", False),
        ("random-file.txt", False),
        # Substring fake-out: a directory named with .consumed-... in the
        # middle but no .parquet suffix on head must NOT match.
        ("bucket.consumed-12345.parquet", False),
    ],
)
def test_is_tombstone_marker(name, expected):
    assert _is_tombstone_marker(name) is expected


def test_tombstone_marker_path_appends_suffix():
    out = _tombstone_marker_path("/buf/batch_x.parquet", 1700000000)
    assert out == "/buf/batch_x.parquet.consumed-1700000000"


# ── _tombstoned_parquet_paths: walks the buf dir ────────────────────────


def test_tombstoned_parquet_paths_empty_when_buf_dir_missing(tmp_path):
    assert _tombstoned_parquet_paths(str(tmp_path / "no_such")) == set()


def test_tombstoned_parquet_paths_returns_recovered_parquet_paths(tmp_path):
    """For every ``<x>.parquet.consumed-<ts>`` found, the corresponding
    ``<x>.parquet`` path is in the returned set."""
    buf = tmp_path / "buf"
    buf.mkdir()
    (buf / "batch_a.parquet").write_bytes(b"x")
    (buf / "batch_a.parquet.consumed-1700000000").write_bytes(b"")
    (buf / "batch_b.parquet").write_bytes(b"x")  # NOT tombstoned

    out = _tombstoned_parquet_paths(str(buf))
    assert str(buf / "batch_a.parquet") in out
    assert str(buf / "batch_b.parquet") not in out


# ── buffer_files: tombstone-aware listing ───────────────────────────────


def test_buffer_files_excludes_tombstoned_parquets(tmp_path):
    """A parquet with an active tombstone sidecar must NOT appear in
    buffer_files() — view rebuilds use this list and binding a
    soon-to-be-swept path re-opens the race tombstoning fixes."""
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    (buf / "batch_keep.parquet").write_bytes(b"x")
    (buf / "batch_drop.parquet").write_bytes(b"x")
    (buf / "batch_drop.parquet.consumed-1700000000").write_bytes(b"")

    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)):
        out = buffer_files(src)

    assert str(buf / "batch_keep.parquet") in out
    assert str(buf / "batch_drop.parquet") not in out
    # Tombstone marker itself is also excluded.
    assert not any(p.endswith(".consumed-1700000000") for p in out)


def test_buffer_files_empty_when_buf_dir_missing(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(tmp_path / "no_buf")):
        assert buffer_files(src) == []


# ── tombstone_buffer_files: create + collision + error ──────────────────


def test_tombstone_buffer_files_creates_markers(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p1 = buf / "batch_a.parquet"
    p2 = buf / "batch_b.parquet"
    p1.write_bytes(b"x")
    p2.write_bytes(b"x")

    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)):
        out = tombstone_buffer_files(src, [str(p1), str(p2)], ts=1700000000)
    assert sorted(out) == sorted([str(p1), str(p2)])
    assert (buf / "batch_a.parquet.consumed-1700000000").exists()
    assert (buf / "batch_b.parquet.consumed-1700000000").exists()


def test_tombstone_buffer_files_tolerates_existing_marker(tmp_path):
    """Two commits in the same second already tombstoned this file —
    FileExistsError must be swallowed (already-consumed is fine)."""
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p = buf / "batch_a.parquet"
    p.write_bytes(b"x")
    (buf / "batch_a.parquet.consumed-1700000000").write_bytes(b"")  # pre-existing

    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)):
        out = tombstone_buffer_files(src, [str(p)], ts=1700000000)
    # The path is reported as tombstoned (no duplicate-failure semantics).
    assert out == [str(p)]


def test_tombstone_buffer_files_logs_and_skips_on_oserror(tmp_path, caplog):
    """If open(marker, 'x') raises a non-FileExists OSError (permissions,
    full disk), log a warning and skip — must not propagate to commit."""
    import logging as _logging

    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p = buf / "batch_a.parquet"
    p.write_bytes(b"x")

    real_open = open

    def _open(path, *a, **kw):
        if str(path).endswith(".consumed-1700000000"):
            raise PermissionError("denied")
        return real_open(path, *a, **kw)

    with patch("builtins.open", side_effect=_open):
        with caplog.at_level(_logging.WARNING, logger="backend.core.iceberg.buffer"):
            out = tombstone_buffer_files(src, [str(p)], ts=1700000000)

    assert out == []  # nothing tombstoned
    assert any("Failed to tombstone" in r.message for r in caplog.records)


# ── sweep_tombstoned_buffer_files: grace window honored ─────────────────


def test_sweep_returns_zero_when_buf_dir_missing(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(tmp_path / "no_buf")):
        assert sweep_tombstoned_buffer_files(src) == 0


def test_sweep_skips_files_within_grace_window(tmp_path):
    """A tombstone younger than ``grace_seconds`` is left alone — its
    parquet may still be referenced by an in-flight query."""
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p = buf / "batch.parquet"
    p.write_bytes(b"x")
    ts_now = int(time.time())
    marker = buf / f"batch.parquet.consumed-{ts_now}"
    marker.write_bytes(b"")

    with (
        patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)),
        patch("backend.core.iceberg.buffer._meta_mod.purge_committed_buffer_rows"),
    ):
        # now == ts → age 0, well inside the default grace window.
        swept = sweep_tombstoned_buffer_files(src, grace_seconds=60, now=ts_now)

    assert swept == 0
    assert p.exists()
    assert marker.exists()


def test_sweep_unlinks_parquet_and_marker_past_grace_window(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p = buf / "batch.parquet"
    p.write_bytes(b"x")
    old_ts = 1700000000
    marker = buf / f"batch.parquet.consumed-{old_ts}"
    marker.write_bytes(b"")
    now = old_ts + 9999  # well past the grace window

    with (
        patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)),
        patch("backend.core.iceberg.buffer._meta_mod.purge_committed_buffer_rows") as mock_purge,
    ):
        swept = sweep_tombstoned_buffer_files(src, grace_seconds=60, now=now)

    assert swept == 1
    assert not p.exists()
    assert not marker.exists()
    # Purges the metadata bookkeeping rows for the swept basename.
    mock_purge.assert_called_once()
    assert mock_purge.call_args[0][1] == ["batch.parquet"]


def test_sweep_skips_marker_with_unparseable_ts(tmp_path):
    """If somehow a marker has a non-int suffix, skip rather than
    raising ValueError out of the sweep."""
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    (buf / "batch.parquet").write_bytes(b"x")
    (buf / "batch.parquet.consumed-notanint").write_bytes(b"")

    with (
        patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)),
        patch("backend.core.iceberg.buffer._meta_mod.purge_committed_buffer_rows"),
    ):
        # Doesn't raise; nothing swept (the broken marker isn't a valid tombstone).
        swept = sweep_tombstoned_buffer_files(src, grace_seconds=0)
    assert swept == 0


# ── buffer_backlog_stats: empty + populated ─────────────────────────────


def test_buffer_backlog_stats_empty_buf_dir(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(tmp_path / "no_buf")):
        out = buffer_backlog_stats(src)
    assert out == {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None}


def test_buffer_backlog_stats_reports_count_and_oldest(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    p1 = buf / "a.parquet"
    p2 = buf / "b.parquet"
    p1.write_bytes(b"x" * 100)
    p2.write_bytes(b"y" * 250)
    # Backdate p1 so it's the oldest.
    old = time.time() - 600
    os.utime(p1, (old, old))

    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)):
        out = buffer_backlog_stats(src)

    assert out["file_count"] == 2
    assert out["total_bytes"] == 350
    assert out["oldest_age_seconds"] >= 599
    assert out["oldest_path"] == str(p1)


# ── _quarantine_buffer_file: rename + sidecar JSON ──────────────────────


def test_quarantine_buffer_file_moves_and_writes_sidecar(tmp_path):
    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    bad = buf / "corrupt.parquet"
    bad.write_bytes(b"not a parquet")

    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)):
        new_path = _quarantine_buffer_file(src, str(bad), RuntimeError("decode failed"))

    assert new_path is not None
    assert not bad.exists()  # original moved
    assert os.path.exists(new_path)
    assert os.path.exists(new_path + ".json")

    sidecar = json.loads(open(new_path + ".json").read())
    assert sidecar["original_path"] == str(bad)
    assert sidecar["error_type"] == "RuntimeError"
    assert "decode failed" in sidecar["error_message"]


def test_quarantine_buffer_file_returns_none_on_failure(tmp_path, caplog):
    """If the rename itself fails (target dir unwritable, file vanished
    between stat + rename), return None so the caller knows quarantine
    didn't happen but the buffer file might still be there."""
    import logging as _logging

    src = {"_cache_dir_override": str(tmp_path), "name": "svc"}
    buf = tmp_path / "buffer"
    buf.mkdir()
    bad = buf / "corrupt.parquet"
    bad.write_bytes(b"x")

    with (
        patch("backend.core.iceberg._core._buffer_dir", return_value=str(buf)),
        patch("os.rename", side_effect=OSError("denied")),
    ):
        with caplog.at_level(_logging.ERROR, logger="backend.core.iceberg.buffer"):
            out = _quarantine_buffer_file(src, str(bad), RuntimeError("decode failed"))

    assert out is None
    assert any("Failed to quarantine" in r.message for r in caplog.records)
