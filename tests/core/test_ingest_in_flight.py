"""Tests for the in_flight crash-recovery layer (atomic ingest).

The window between ``iceberg.write_to_buffer`` and
``metadata_db.insert_ingested_files`` is the only place where an ingest
crash can produce silent duplicates: the buffer Parquet lands on disk,
the next tick re-LISTs the same source files, generates a NEW buffer
file, and the commit cron pushes both to Iceberg.

The fix is mark-before-write:
  1. ``record_in_flight`` persists the (file, row_count, size) tuples
     for the chunk to a new ``ingest_in_flight`` table.
  2. ``iceberg.write_to_buffer`` writes a Parquet file with a
     deterministic name (``batch_{sha256(sorted_chunk)[:16]}.parquet``).
  3. ``insert_ingested_files`` commits the tuples.
  4. ``clear_in_flight`` drops the in_flight row.

On startup, ``_recover_in_flight`` reconciles:
  - buffer present  → promote tuples into ``ingested_files`` and clear.
  - buffer missing  → drop the row without touching ``ingested_files``
                       (the source files will re-ingest on next tick).
"""

from __future__ import annotations

import os
from unittest.mock import patch

from backend.core import ingest
from backend.core import metadata as metadata_db


def test_deterministic_buffer_name_is_stable_across_runs():
    name1 = ingest._deterministic_buffer_name(["s3://b/a.gz", "s3://b/b.gz"])
    name2 = ingest._deterministic_buffer_name(["s3://b/b.gz", "s3://b/a.gz"])
    assert name1 == name2, "buffer name must be order-independent"
    assert name1.startswith("batch_") and name1.endswith(".parquet")
    assert len(name1) == len("batch_") + 16 + len(".parquet")


def test_deterministic_buffer_name_differs_for_distinct_chunks():
    n1 = ingest._deterministic_buffer_name(["s3://b/a.gz"])
    n2 = ingest._deterministic_buffer_name(["s3://b/b.gz"])
    assert n1 != n2


def test_record_and_list_in_flight_roundtrip():
    sid = "svc-inflight-roundtrip"
    rows = [("s3://b/a.gz", 100, 4096), ("s3://b/b.gz", 200, 8192)]
    metadata_db.record_in_flight(sid, "batch_abc.parquet", rows)

    pending = metadata_db.list_in_flight(sid)
    assert len(pending) == 1
    name, tuples = pending[0]
    assert name == "batch_abc.parquet"
    assert tuples == rows


def test_record_in_flight_overwrites_on_same_buffer_name():
    """A re-ingest of the same chunk (same deterministic name) must
    overwrite the prior manifest cleanly — never raise on UNIQUE."""
    sid = "svc-inflight-upsert"
    metadata_db.record_in_flight(sid, "batch_xyz.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid, "batch_xyz.parquet", [("a", 1, 10), ("b", 2, 20)])

    pending = metadata_db.list_in_flight(sid)
    assert len(pending) == 1
    assert pending[0][1] == [("a", 1, 10), ("b", 2, 20)]


def test_clear_in_flight_is_idempotent():
    sid = "svc-inflight-clear"
    metadata_db.record_in_flight(sid, "batch_clr.parquet", [("a", 1, 10)])
    metadata_db.clear_in_flight(sid, "batch_clr.parquet")
    metadata_db.clear_in_flight(sid, "batch_clr.parquet")  # must not raise
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_promotes_when_buffer_exists(tmp_path):
    """Buffer present on disk → the in_flight tuples are committed to
    ``ingested_files`` and the in_flight row is cleared."""
    sid = "svc-recover-promote"
    src = {"name": sid}

    rows = [("s3://b/x.gz", 50, 1000), ("s3://b/y.gz", 75, 2000)]
    metadata_db.record_in_flight(sid, "batch_present.parquet", rows)

    # Buffer file exists on disk.
    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_present.parquet").write_bytes(b"PAR1stub")

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 1
    assert result["dropped"] == 0
    assert result["rows_recovered"] == 125

    # Tuples promoted into ingested_files
    committed = metadata_db.get_ingested_filenames(sid)
    assert committed == {"s3://b/x.gz", "s3://b/y.gz"}

    # in_flight row cleared
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_drops_when_buffer_missing(tmp_path):
    """Buffer missing on disk → drop in_flight row WITHOUT committing.
    The source files will re-LIST and re-ingest cleanly next tick."""
    sid = "svc-recover-drop"
    src = {"name": sid}

    rows = [("s3://b/q.gz", 10, 100)]
    metadata_db.record_in_flight(sid, "batch_missing.parquet", rows)

    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()  # empty dir, no parquet

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 0
    assert result["dropped"] == 1

    # ingested_files was NOT touched
    assert metadata_db.get_ingested_filenames(sid) == set()

    # in_flight row cleared so the next sweep doesn't replay it
    assert metadata_db.list_in_flight(sid) == []


def test_recover_in_flight_is_noop_when_no_pending(tmp_path):
    sid = "svc-recover-empty"
    src = {"name": sid}

    with patch("backend.core.iceberg._buffer_dir", return_value=str(tmp_path)):
        result = ingest._recover_in_flight(src)
    assert result == {"promoted": 0, "dropped": 0, "rows_recovered": 0}


def test_recover_in_flight_handles_mixed_state(tmp_path):
    """One buffer present, one missing — recovery handles each independently."""
    sid = "svc-recover-mixed"
    src = {"name": sid}

    metadata_db.record_in_flight(sid, "batch_here.parquet", [("a.gz", 5, 50)])
    metadata_db.record_in_flight(sid, "batch_gone.parquet", [("b.gz", 7, 70)])

    buf_dir = tmp_path / "buffer"
    buf_dir.mkdir()
    (buf_dir / "batch_here.parquet").write_bytes(b"PAR1stub")

    with patch("backend.core.iceberg._buffer_dir", return_value=str(buf_dir)):
        result = ingest._recover_in_flight(src)

    assert result["promoted"] == 1
    assert result["dropped"] == 1
    assert result["rows_recovered"] == 5

    committed = metadata_db.get_ingested_filenames(sid)
    assert committed == {"a.gz"}  # b.gz NOT committed
    assert metadata_db.list_in_flight(sid) == []


def test_list_in_flight_is_per_service():
    """Two services' in_flight rows must not bleed across each other."""
    sid_a = "svc-inflight-a"
    sid_b = "svc-inflight-b"
    metadata_db.record_in_flight(sid_a, "batch_a.parquet", [("a", 1, 10)])
    metadata_db.record_in_flight(sid_b, "batch_b.parquet", [("b", 2, 20)])

    a_pending = metadata_db.list_in_flight(sid_a)
    b_pending = metadata_db.list_in_flight(sid_b)
    assert len(a_pending) == 1 and a_pending[0][0] == "batch_a.parquet"
    assert len(b_pending) == 1 and b_pending[0][0] == "batch_b.parquet"


def test_recover_in_flight_survives_buffer_dir_missing(tmp_path):
    """The buffer directory itself may not exist yet (fresh service);
    the recovery sweep should treat that as 'no buffer present' for
    every pending row and drop them rather than crash."""
    sid = "svc-recover-no-dir"
    src = {"name": sid}

    metadata_db.record_in_flight(sid, "batch_nodir.parquet", [("a.gz", 1, 1)])
    missing_dir = str(tmp_path / "does_not_exist" / "buffer")
    assert not os.path.exists(missing_dir)

    with patch("backend.core.iceberg._buffer_dir", return_value=missing_dir):
        result = ingest._recover_in_flight(src)
    assert result["dropped"] == 1
    assert result["promoted"] == 0
