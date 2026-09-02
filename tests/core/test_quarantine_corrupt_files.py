"""``_quarantine_corrupt_files`` — the sync-ingest path's corrupt-line
sidecar writer.

This is the last stop for log lines that DuckDB could not parse. The sync
path deletes the raw ``.gz`` after a successful ingest, so anything this
function fails to write down is gone: the operator gets a row-count
shortfall in the Usage Log with no way to see which lines caused it.

The invariants that matter, in risk order:

* only the BAD lines are uploaded — never the whole raw file (the valid rows
  are already ingested, and re-uploading the file would duplicate log data
  into a prefix nobody expects to be ingestable);
* every quarantined file lands in ``quarantined_files`` with per-file counts
  and a reason histogram, since that table is what the admin Quarantine view
  and the retention sweeper read;
* it is best-effort per file: one file's FOS failure must never abort the
  ingest run or skip the remaining files.

Asserts against the REAL metadata store (``list_quarantined_files``), not a
mock call count — the row is the observable outcome. Only the FOS/S3 client
is mocked.
"""

import json
from unittest.mock import MagicMock

from backend.core.ingest import _quarantine_corrupt_files
from backend.core.metadata.quarantine import get_con, list_quarantined_files

BUCKET = "test-bucket"


def _clear(service_id: str):
    # The metadata DB is per-service, so quarantined_files has no
    # service_id column to filter on.
    con = get_con(service_id)
    con.execute("DELETE FROM quarantined_files")
    con.commit()
    return con


def _src(service_id: str, prefix: str = "") -> dict:
    return {"service_id": service_id, "name": service_id, "bucket": BUCKET, "prefix": prefix}


def _s3(key: str) -> str:
    return f"s3://{BUCKET}/{key}"


def test_quarantine_corrupt_files_uploads_only_bad_lines_and_records_the_file():
    """One file, two corrupt lines out of five: the .bad.jsonl body must
    contain exactly those two lines, and the sidecar plus DB row must carry
    the valid/corrupt/total split the admin view displays."""
    service_id = "test-quarantine-sync-basic"
    _clear(service_id)
    key = "raw/2026/08/27/13/00/batch.json.gz"
    fos = MagicMock()

    quarantined = _quarantine_corrupt_files(
        fos,
        BUCKET,
        _src(service_id),
        corrupt_s3_paths=[_s3(key)],
        truly_corrupt=[
            (_s3(key), '  {"truncated": \n', "invalid_json"),
            (_s3(key), '{"url": "/a"}', "missing_timestamp"),
        ],
        count_map={_s3(key): 5},
        valid_counts={_s3(key): 3},
        file_sizes={_s3(key): 4096},
        source_name=service_id,
    )

    assert quarantined == 1

    keys = [c.kwargs["Key"] for c in fos.put_object.call_args_list]
    assert keys == [
        "errors/2026/08/27/13/00/batch.json.bad.jsonl",
        "errors/2026/08/27/13/00/batch.json.bad.jsonl.meta.json",
    ]
    # Only the bad lines, stripped — not the raw file.
    assert fos.put_object.call_args_list[0].kwargs["Body"] == b'{"truncated":\n{"url": "/a"}'
    assert fos.put_object.call_args_list[0].kwargs["ContentType"] == "application/x-ndjson"

    meta = json.loads(fos.put_object.call_args_list[1].kwargs["Body"])
    assert meta["original_key"] == key
    assert meta["valid_rows"] == 3
    assert meta["corrupt_rows"] == 2
    assert meta["total_rows"] == 5
    assert meta["file_size_bytes"] == 4096
    assert meta["reason_counts"] == {"invalid_json": 1, "missing_timestamp": 1}
    assert meta["source_name"] == service_id

    rows = list_quarantined_files(service_id)
    assert len(rows) == 1
    row = rows[0]
    assert row["file_name"] == "batch.json.gz"
    assert row["fos_key"] == key
    assert row["error_key"] == keys[0]
    assert row["meta_key"] == keys[1]
    assert row["valid_rows"] == 3
    assert row["corrupt_rows"] == 2
    assert row["file_size_bytes"] == 4096
    assert row["reason_counts"] == {"invalid_json": 1, "missing_timestamp": 1}
    assert row["corrupt_samples"] == ['{"truncated":', '{"url": "/a"}']


def test_quarantine_corrupt_files_attributes_lines_to_the_right_file():
    """Corrupt lines arrive as one flat list across the whole chunk. Each
    file's sidecar must contain only its OWN lines — cross-attribution would
    point the operator at the wrong raw file."""
    service_id = "test-quarantine-sync-multi"
    _clear(service_id)
    key_a = "raw/2026/08/27/13/01/a.json.gz"
    key_b = "raw/2026/08/27/13/02/b.json.gz"
    fos = MagicMock()

    quarantined = _quarantine_corrupt_files(
        fos,
        BUCKET,
        _src(service_id),
        corrupt_s3_paths=[_s3(key_a), _s3(key_b)],
        truly_corrupt=[
            (_s3(key_a), "a-bad-1", "invalid_json"),
            (_s3(key_b), "b-bad-1", "missing_timestamp"),
            (_s3(key_a), "a-bad-2", "invalid_json"),
        ],
        count_map={_s3(key_a): 10, _s3(key_b): 4},
        valid_counts={_s3(key_a): 8, _s3(key_b): 3},
        file_sizes={_s3(key_a): 100, _s3(key_b): 200},
        source_name=service_id,
    )

    assert quarantined == 2
    bodies = {c.kwargs["Key"]: c.kwargs["Body"] for c in fos.put_object.call_args_list}
    assert bodies["errors/2026/08/27/13/01/a.json.bad.jsonl"] == b"a-bad-1\na-bad-2"
    assert bodies["errors/2026/08/27/13/02/b.json.bad.jsonl"] == b"b-bad-1"

    by_file = {r["file_name"]: r for r in list_quarantined_files(service_id)}
    assert by_file["a.json.gz"]["corrupt_rows"] == 2
    assert by_file["a.json.gz"]["reason_counts"] == {"invalid_json": 2}
    assert by_file["b.json.gz"]["corrupt_rows"] == 1
    assert by_file["b.json.gz"]["reason_counts"] == {"missing_timestamp": 1}


def test_quarantine_corrupt_files_honors_the_source_prefix():
    """With a source prefix, both the raw and errors trees are nested under
    it. A key built off the unprefixed root would land outside the source
    and be invisible to the quarantine reader."""
    service_id = "test-quarantine-sync-prefix"
    _clear(service_id)
    key = "tenant-b/raw/2026/08/27/13/03/p.json.gz"
    fos = MagicMock()

    assert (
        _quarantine_corrupt_files(
            fos,
            BUCKET,
            _src(service_id, prefix="/tenant-b/"),
            corrupt_s3_paths=[_s3(key)],
            truly_corrupt=[(_s3(key), "bad", "invalid_json")],
            count_map={_s3(key): 2},
            valid_counts={_s3(key): 1},
            file_sizes={_s3(key): 10},
            source_name=service_id,
        )
        == 1
    )

    keys = [c.kwargs["Key"] for c in fos.put_object.call_args_list]
    assert keys[0] == "tenant-b/errors/2026/08/27/13/03/p.json.bad.jsonl"
    assert list_quarantined_files(service_id)[0]["error_key"] == keys[0]


def test_quarantine_corrupt_files_skips_a_file_with_no_corrupt_lines():
    """A file can be flagged by the row-count shortfall check yet have no
    concrete bad lines attributed to it. Writing an empty sidecar for it
    would show the operator a phantom quarantined file."""
    service_id = "test-quarantine-sync-empty"
    _clear(service_id)
    key = "raw/2026/08/27/13/04/clean.json.gz"
    fos = MagicMock()

    assert (
        _quarantine_corrupt_files(
            fos,
            BUCKET,
            _src(service_id),
            corrupt_s3_paths=[_s3(key)],
            truly_corrupt=[],
            count_map={_s3(key): 3},
            valid_counts={_s3(key): 3},
            file_sizes={_s3(key): 10},
            source_name=service_id,
        )
        == 0
    )

    fos.put_object.assert_not_called()
    assert list_quarantined_files(service_id) == []


def test_quarantine_corrupt_files_skips_a_key_outside_the_raw_prefix():
    """The errors key is derived by slicing the raw prefix off the original
    key. A key that does not start with it (e.g. a RUM key, or another
    source's layout) must be skipped rather than mangled into a wrong
    destination."""
    service_id = "test-quarantine-sync-offprefix"
    _clear(service_id)
    key = "rum/raw/2026/08/27/13/05/beacon.json.gz"
    fos = MagicMock()

    assert (
        _quarantine_corrupt_files(
            fos,
            BUCKET,
            _src(service_id),
            corrupt_s3_paths=[_s3(key)],
            truly_corrupt=[(_s3(key), "bad", "invalid_json")],
            count_map={_s3(key): 2},
            valid_counts={_s3(key): 1},
            file_sizes={_s3(key): 10},
            source_name=service_id,
        )
        == 0
    )

    fos.put_object.assert_not_called()
    assert list_quarantined_files(service_id) == []


def test_quarantine_corrupt_files_is_best_effort_per_file():
    """One file's FOS failure must not abort the run: quarantine is a
    diagnostic, and the valid rows of every file in the chunk are already
    ingested. The surviving files must still be recorded, and the returned
    count must reflect only what actually landed."""
    service_id = "test-quarantine-sync-partial"
    _clear(service_id)
    key_bad = "raw/2026/08/27/13/06/explodes.json.gz"
    key_ok = "raw/2026/08/27/13/07/fine.json.gz"
    fos = MagicMock()

    def _put(**kwargs):
        if "explodes" in kwargs["Key"]:
            raise Exception("AccessDenied writing errors/ prefix")
        return {}

    fos.put_object.side_effect = _put

    quarantined = _quarantine_corrupt_files(
        fos,
        BUCKET,
        _src(service_id),
        corrupt_s3_paths=[_s3(key_bad), _s3(key_ok)],
        truly_corrupt=[(_s3(key_bad), "x", "invalid_json"), (_s3(key_ok), "y", "invalid_json")],
        count_map={_s3(key_bad): 2, _s3(key_ok): 2},
        valid_counts={_s3(key_bad): 1, _s3(key_ok): 1},
        file_sizes={_s3(key_bad): 10, _s3(key_ok): 10},
        source_name=service_id,
    )

    assert quarantined == 1
    rows = list_quarantined_files(service_id)
    assert [r["file_name"] for r in rows] == ["fine.json.gz"]


def test_quarantine_corrupt_files_caps_the_stored_samples():
    """Samples are inlined into the sidecar and the DB row — an all-corrupt
    file must not blow either up. At most 5 samples, each truncated."""
    service_id = "test-quarantine-sync-samples"
    _clear(service_id)
    key = "raw/2026/08/27/13/08/many.json.gz"
    fos = MagicMock()
    long_line = "x" * 3000
    corrupt = [(_s3(key), f"{long_line}{i}", "invalid_json") for i in range(9)]

    _quarantine_corrupt_files(
        fos,
        BUCKET,
        _src(service_id),
        corrupt_s3_paths=[_s3(key)],
        truly_corrupt=corrupt,
        count_map={_s3(key): 9},
        valid_counts={_s3(key): 0},
        file_sizes={_s3(key): 999},
        source_name=service_id,
    )

    meta = json.loads(fos.put_object.call_args_list[1].kwargs["Body"])
    assert len(meta["corrupt_samples"]) == 5
    assert all(len(s) == 2000 for s in meta["corrupt_samples"])
    assert meta["corrupt_rows"] == 9

    row = list_quarantined_files(service_id)[0]
    assert len(row["corrupt_samples"]) == 5
    # ...but every bad line is still uploaded in full to the .bad.jsonl.
    assert fos.put_object.call_args_list[0].kwargs["Body"].count(b"\n") == 8
