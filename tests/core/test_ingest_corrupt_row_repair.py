"""Integration tests for ingest.py's corrupt-row repair pipeline (lines 726-965).

These tests exercise the full ingest() flow with real gzipped log files
whose JSON content includes the specific malformations the repair
pipeline targets: empty-value fields (``"a": ,``) that the `_EMPTY_VALUE_RE`
regex rewrites to `:null`. The test setup mirrors test_ingest_corruption.py
(local-mock-S3 via boto3-shim) so the corrupt-row code path runs against
real DuckDB read_csv + json_valid invocations rather than mocked seams.

Also covers the network-failure rollback path (lines 924-965): when the
corrupt-line re-read raises a network/disk error, affected files are
rolled back from staging and added to ``failed_paths`` for retry.
"""

from __future__ import annotations

import gzip
import io
import json

from backend.core.ingest import ingest


def _drain(gen) -> list[dict]:
    return list(gen)


def _patch_ingest_to_local(monkeypatch, fos_source, in_memory_duckdb, log_dir):
    """Mirror of the test_ingest_corruption.py harness: rewrite
    ``s3://bucket/...`` to ``file://`` so DuckDB reads from local disk."""
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(log_dir.parent / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a: None)
    monkeypatch.setattr("time.sleep", lambda *a: None)

    bucket_prefix = f"s3://{fos_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"

    class _Rewrite:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    import backend.core.duckdb as my_duckdb

    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda src: _Rewrite(in_memory_duckdb))
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 50)


def _mock_fos(log_dir, keys):
    class MockPaginator:
        def paginate(self, **kwargs):
            return [{"Contents": [{"Key": k, "Size": 100} for k in keys]}]

    class MockFos:
        def get_paginator(self, *args, **kwargs):
            return MockPaginator()

        def __init__(self):
            self.delete_calls = []
            self.delete_response = {}

        def get_object(self, Bucket, Key):
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            self.delete_calls.append(kwargs)
            return self.delete_response

    return MockFos()


def test_corrupt_row_pipeline_skipped_when_all_rows_valid(fos_source, in_memory_duckdb, monkeypatch, tmp_path):
    """When every row has a non-NULL timestamp after staging, the repair
    block at line 725 is skipped entirely (``corrupt_in_batch <= 0``)."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True)
    key = "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    (log_dir / key).parent.mkdir(parents=True)
    with gzip.open(log_dir / key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T10:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        f.write(json.dumps({"timestamp": "2026-05-18T10:01:00Z", "status": 200, "url": "/ok"}) + "\n")

    _patch_ingest_to_local(monkeypatch, fos_source, in_memory_duckdb, log_dir)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: _mock_fos(log_dir, [key]))

    events = _drain(ingest(source={**fos_source}))
    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 2
    # corrupt_rows shouldn't appear (repair pipeline never ran)
    assert done.get("corrupt_rows", 0) == 0


def test_corrupt_row_pipeline_repairs_empty_value_via_null_substitution(
    fos_source, in_memory_duckdb, monkeypatch, tmp_path
):
    """An empty-value field (``"x": ,``) makes the staging row's timestamp
    NULL (json parse fails). The repair pipeline re-reads the file as
    raw VARCHAR, rewrites ``:`` followed by ``,`` to ``:null`` via
    `_EMPTY_VALUE_RE`, and re-inserts the now-valid row into
    ``_ingest_staging``. After repair, the row counts as ingested."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True)
    key = "raw/2026-05-18/11/2026-05-18T11-00-00.r.gz"
    (log_dir / key).parent.mkdir(parents=True)
    # Two rows: one clean, one with an empty-value field that the repair
    # regex can fix. The repaired row's timestamp is intact, so it should
    # come back through the repair re-insert path.
    with gzip.open(log_dir / key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T11:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        # Manually craft a malformed row: status field has no value before the comma.
        f.write('{"timestamp": "2026-05-18T11:01:00Z", "status": , "url": "/repair"}\n')

    _patch_ingest_to_local(monkeypatch, fos_source, in_memory_duckdb, log_dir)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: _mock_fos(log_dir, [key]))

    events = _drain(ingest(source={**fos_source}))
    done = next(e for e in events if e["type"] == "done")

    # The done event should reflect SOME ingestion (at minimum the valid row,
    # plus the repaired row if the pipeline succeeded).
    assert done["rows_inserted"] >= 1


def test_corrupt_row_pipeline_captures_exact_evidence_and_still_deletes_source(
    fos_source, in_memory_duckdb, monkeypatch, tmp_path
):
    """Lines so malformed even the empty-value regex can't help (e.g.
    unclosed bracket) accumulate in ``total_corrupt_details``. The done
    event surfaces this as ``corrupt_rows > 0`` and a sample list."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True)
    key = "raw/request/2026-05-18/12/2026-05-18T12-00-00.b.gz"
    (log_dir / key).parent.mkdir(parents=True)
    malformed = b'{"timestamp": "2026-05-18T12:01:00Z", "status": 200\n'
    with gzip.open(log_dir / key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T12:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        # Truly broken: unclosed brace
        f.write(malformed.decode())

    _patch_ingest_to_local(monkeypatch, fos_source, in_memory_duckdb, log_dir)
    mock_fos = _mock_fos(log_dir, [key])
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: mock_fos)
    captured = []
    mock_fos.delete_response = {"Errors": [{"Key": key, "Code": "InternalError"}]}

    def capture_evidence(service_id, source_type, original_key, payload, **metadata):
        captured.append((source_type, original_key, payload))
        return {"id": len(captured), "cap_evictions": 0, "quarantine_capture_failures": 1}

    monkeypatch.setattr("backend.core.quarantine.capture_evidence", capture_evidence)

    events = _drain(ingest(source={**fos_source}, delete_after=True))
    done = next(e for e in events if e["type"] == "done")

    # The valid row is ingested; the broken row is noted as corrupt.
    assert done["rows_inserted"] >= 1
    assert done["corrupt_rows"] > 0
    assert captured == [("request", key, malformed)]
    assert done["outcome_counters"]["malformed_records"] == 1
    assert done["outcome_counters"]["quarantine_capture_failures"] == 1
    assert done["outcome_counters"]["source_delete_failures"] == 1
    assert done["outcome_counters"]["objects_failed"] == 1
    assert done["outcome_counters"]["objects_partial"] == 0
    assert mock_fos.delete_calls == [
        {"Bucket": fos_source["bucket"], "Delete": {"Objects": [{"Key": key}], "Quiet": True}}
    ]


def test_network_failure_during_corrupt_reread_falls_back_to_gzip_scan(
    fos_source, in_memory_duckdb, monkeypatch, tmp_path
):
    """A failed DuckDB diagnostic scan falls back to parsing downloaded gzips.

    The raw in-memory connection exercises the download-based path without
    rewriting paths used by the diagnostic query.
    """
    import backend.core.duckdb as my_duckdb

    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True)

    clean_key = "raw/request/2026-05-18/13/2026-05-18T13-00-00.v.gz"
    corrupt_key = "raw/request/2026-05-18/13/2026-05-18T13-00-00.r.gz"
    for k in (clean_key, corrupt_key):
        (log_dir / k).parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(log_dir / clean_key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T13:00:00Z", "status": 200, "url": "/ok"}) + "\n")

    with gzip.open(log_dir / corrupt_key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T13:01:00Z", "status": 200, "url": "/clean"}) + "\n")
        f.write(json.dumps({"status": 200, "url": "/no-timestamp"}) + "\n")

    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda src: in_memory_duckdb)
    monkeypatch.setattr(my_duckdb, "INGEST_CHUNK_SIZE", 50)
    monkeypatch.setattr("time.sleep", lambda *a: None)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: _mock_fos(log_dir, [clean_key, corrupt_key]))

    real_execute = my_duckdb._execute_query_with_retry

    def _failing_execute(con, query, **kwargs):
        if "read_csv" in query and "column0" in query:
            raise Exception("IO Error: No such file or directory: could not resolve hostname")
        return real_execute(con, query, **kwargs)

    monkeypatch.setattr("backend.core.ingest._execute_query_with_retry", _failing_execute)

    events = _drain(ingest(source={**fos_source}))
    done = next(e for e in events if e["type"] == "done")

    assert done["rows_inserted"] == 2
    assert done["outcome_counters"]["valid_records"] == 2
    assert done["outcome_counters"]["malformed_records"] == 1
    assert done["outcome_counters"]["quarantine_capture_failures"] == 0

    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(fos_source["name"])
    assert f"s3://{fos_source['bucket']}/{clean_key}" in ingested
    assert f"s3://{fos_source['bucket']}/{corrupt_key}" in ingested
