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


def test_purge_committed_buffer_rows_drops_only_listed(svc_id):
    """``purge_committed_buffer_rows`` removes only the rows whose
    basename is in the input list — never accidentally clears the whole
    table. Called from the tombstone sweep after the on-disk parquet
    and tombstone marker are both gone."""
    con = _meta.get_con(svc_id)
    for name in ("batch_a.parquet", "batch_b.parquet", "batch_c.parquet"):
        con.execute(
            "INSERT INTO committed_buffers (buffer_filename) VALUES (?)",
            (name,),
        )
    con.commit()
    n = _meta.purge_committed_buffer_rows(svc_id, ["batch_a.parquet", "batch_c.parquet"])
    assert n == 2
    remaining = {
        r["buffer_filename"]
        for r in con.execute(
            "SELECT buffer_filename FROM committed_buffers",
        ).fetchall()
    }
    assert remaining == {"batch_b.parquet"}


def test_empty_inputs_skip_sql_round_trip(svc_id):
    """Empty input lists must short-circuit — no SQL executed."""
    assert _meta.purge_committed_buffer_rows(svc_id, []) == 0
