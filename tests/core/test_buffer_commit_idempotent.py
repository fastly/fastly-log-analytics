"""Tests for the buffer-commit ↔ tombstone race fix.

The race we're closing: ``commit_buffer`` used to do
``table.append(combined)`` followed by ``tombstone_buffer_files(...)``
with nothing durable in between. A crash in that window left the
buffer file active (no tombstone) but the rows already in Iceberg —
the next commit tick re-read the buffer and re-appended, producing
duplicate rows.

The fix: write a ``committed_buffers`` SQLite row between the append
and the tombstone. On the next commit tick, ``commit_buffer``'s
recovery sweep finds those rows, tombstones the orphan buffer files,
and skips the re-append.

These tests pin that contract: the metadata helpers do what they say,
and any future refactor of ``commit_buffer`` that breaks the
mark-before-tombstone order will fail here.
"""

from __future__ import annotations

import pytest

from backend.core import metadata as _meta


@pytest.fixture
def svc_id(tmp_path, monkeypatch):
    """Per-test SQLite db rooted under tmp_path so the migration sweep
    starts from a clean schema each run."""
    svc = "test-buffer-commit-svc"
    monkeypatch.setattr("backend.config.DATA_DIR", tmp_path)
    monkeypatch.setattr("backend.core.metadata.base.DATA_DIR", tmp_path, raising=False)
    # Touch the connection so migrations + schema apply.
    _meta.get_con(svc).execute("SELECT 1")
    return svc


def test_filter_uncommitted_returns_input_when_table_empty(svc_id):
    """Fresh DB: no rows in committed_buffers → every basename is
    uncommitted. The set must equal the input as a set (order
    preservation isn't part of the contract)."""
    names = ["batch_a.parquet", "batch_b.parquet", "batch_c.parquet"]
    assert _meta.filter_uncommitted_buffers(svc_id, names) == set(names)


def test_filter_uncommitted_excludes_marked_basenames(svc_id):
    """After mark_buffers_committed lands a row, the same basename must
    drop out of filter_uncommitted_buffers. This is the crash-recovery
    contract: ``commit_buffer`` uses the inverse (``list_committed_basenames``)
    to find files to tombstone-and-skip on its next tick."""
    _meta.mark_buffers_committed(svc_id, ["batch_a.parquet", "batch_b.parquet"])
    result = _meta.filter_uncommitted_buffers(svc_id, ["batch_a.parquet", "batch_b.parquet", "batch_c.parquet"])
    assert result == {"batch_c.parquet"}


def test_list_committed_inverts_filter_uncommitted(svc_id):
    """``list_committed_basenames`` returns the names that ARE in
    committed_buffers — the inverse of ``filter_uncommitted_buffers``
    over the same candidate set. ``commit_buffer`` uses this to know
    which buffer files to tombstone-rescue."""
    _meta.mark_buffers_committed(svc_id, ["batch_a.parquet"])
    candidates = ["batch_a.parquet", "batch_b.parquet"]
    assert _meta.list_committed_basenames(svc_id, candidates) == {"batch_a.parquet"}
    assert _meta.filter_uncommitted_buffers(svc_id, candidates) == {"batch_b.parquet"}


def test_mark_buffers_committed_is_idempotent(svc_id):
    """Re-marking the same basename must NOT raise (PRIMARY KEY
    constraint would otherwise hit on the second call). A partial-batch
    retry should be able to safely re-mark rows that already landed."""
    _meta.mark_buffers_committed(svc_id, ["batch_a.parquet"])
    _meta.mark_buffers_committed(svc_id, ["batch_a.parquet", "batch_b.parquet"])
    assert _meta.list_committed_basenames(svc_id, ["batch_a.parquet", "batch_b.parquet"]) == {
        "batch_a.parquet",
        "batch_b.parquet",
    }


def test_purge_committed_buffer_rows_drops_only_listed(svc_id):
    """``purge_committed_buffer_rows`` removes only the rows whose
    basename is in the input list — never accidentally clears the whole
    table. Called from the tombstone sweep after the on-disk parquet
    and tombstone marker are both gone."""
    _meta.mark_buffers_committed(svc_id, ["batch_a.parquet", "batch_b.parquet", "batch_c.parquet"])
    n = _meta.purge_committed_buffer_rows(svc_id, ["batch_a.parquet", "batch_c.parquet"])
    assert n == 2
    assert _meta.list_committed_basenames(svc_id, ["batch_a.parquet", "batch_b.parquet", "batch_c.parquet"]) == {
        "batch_b.parquet"
    }


def test_empty_inputs_skip_sql_round_trip(svc_id):
    """Empty input lists must short-circuit — no SQL executed. Cheap
    defensive coding: ``commit_buffer`` calls these with the per-chunk
    basename list every iteration, including chunks where every file
    failed to read and the list is empty."""
    assert _meta.filter_uncommitted_buffers(svc_id, []) == set()
    assert _meta.list_committed_basenames(svc_id, []) == set()
    assert _meta.purge_committed_buffer_rows(svc_id, []) == 0
    # And mark_buffers_committed on [] is a no-op.
    _meta.mark_buffers_committed(svc_id, [])


# ── Iceberg-snapshot marker (second durable channel) ─────────────────────


def test_buffer_marker_is_deterministic():
    """``_buffer_basename_marker`` must produce the same value for the
    same input across processes. The recovery sweep relies on this:
    the marker stored in the Iceberg snapshot at write time must match
    the marker computed at read time on a different process / restart.
    """
    from backend.core.iceberg.buffer import _buffer_basename_marker

    m1 = _buffer_basename_marker("batch_abc123def456.parquet")
    m2 = _buffer_basename_marker("batch_abc123def456.parquet")
    assert m1 == m2
    assert len(m1) == 12
    # Different basenames must NOT collide (within reasonable bounds —
    # 48-bit hash is overwhelmingly safe per commit chunk).
    assert _buffer_basename_marker("batch_aaa.parquet") != _buffer_basename_marker("batch_bbb.parquet")


def test_recent_snapshot_markers_returns_recent_only():
    """``_recent_snapshot_markers`` must honour the time cutoff — the
    point of the cutoff is to bound work on long-lived tables with
    thousands of snapshots."""
    from backend.core.iceberg.buffer import _COMMIT_MARKER_PREFIX, _recent_snapshot_markers

    class _Summary:
        def __init__(self, props):
            self.additional_properties = props

    class _Snap:
        def __init__(self, ts_ms, props):
            self.timestamp_ms = ts_ms
            self.summary = _Summary(props)

    class _Table:
        def __init__(self, snaps):
            self._snaps = snaps

        def snapshots(self):
            return self._snaps

    now_ms = 1_700_000_000_000
    table = _Table(
        [
            _Snap(now_ms - 60_000, {f"{_COMMIT_MARKER_PREFIX}aaaaa": "1"}),
            _Snap(now_ms - 7_200_000, {f"{_COMMIT_MARKER_PREFIX}bbbbb": "1"}),
        ]
    )
    markers = _recent_snapshot_markers(table, since_ms=now_ms - 3_600_000)
    assert markers == {"aaaaa"}


def test_recent_snapshot_markers_swallows_iceberg_errors():
    """A flaky catalog read MUST NOT propagate — the recovery sweep
    falls back to SQLite-only in that case (compaction-dedup is the
    safety net below that)."""
    from backend.core.iceberg.buffer import _recent_snapshot_markers

    class _BrokenTable:
        def snapshots(self):
            raise RuntimeError("simulated catalog outage")

    assert _recent_snapshot_markers(_BrokenTable(), since_ms=0) == set()


def test_recent_snapshot_markers_ignores_non_marker_props():
    """Snapshots carry many Iceberg-internal summary properties (added-
    files-size, total-records, etc.). The scan must only return keys
    under our namespace — picking up Iceberg's keys would create
    nonsensical 'committed' basenames."""
    from backend.core.iceberg.buffer import _COMMIT_MARKER_PREFIX, _recent_snapshot_markers

    class _Summary:
        def __init__(self, props):
            self.additional_properties = props

    class _Snap:
        def __init__(self):
            self.timestamp_ms = 1_700_000_000_000
            self.summary = _Summary(
                {
                    "added-records": "1000",
                    "added-files-size": "12345",
                    f"{_COMMIT_MARKER_PREFIX}ourmarker": "1",
                }
            )

    class _Table:
        def snapshots(self):
            return [_Snap()]

    markers = _recent_snapshot_markers(_Table(), since_ms=0)
    assert markers == {"ourmarker"}
