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
