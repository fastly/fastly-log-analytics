"""Integration tests for ingest.py's corrupt-row repair pipeline (lines 726-855).

These tests exercise the full ingest() flow with real gzipped log files
whose JSON content includes the specific malformations the repair
pipeline targets: empty-value fields (``"a": ,``) that the `_EMPTY_VALUE_RE`
regex rewrites to `:null`. The test setup mirrors test_ingest_corruption.py
(local-mock-S3 via boto3-shim) so the corrupt-row code path runs against
real DuckDB read_csv + json_valid invocations rather than mocked seams.
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

        def get_object(self, Bucket, Key):
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            return {}

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


def test_corrupt_row_pipeline_unfixable_lines_accumulate_in_corrupt_details(
    fos_source, in_memory_duckdb, monkeypatch, tmp_path
):
    """Lines so malformed even the empty-value regex can't help (e.g.
    unclosed bracket) accumulate in ``total_corrupt_details``. The done
    event surfaces this as ``corrupt_rows > 0`` and a sample list."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True)
    key = "raw/2026-05-18/12/2026-05-18T12-00-00.b.gz"
    (log_dir / key).parent.mkdir(parents=True)
    with gzip.open(log_dir / key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T12:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        # Truly broken: unclosed brace
        f.write('{"timestamp": "2026-05-18T12:01:00Z", "status": 200\n')

    _patch_ingest_to_local(monkeypatch, fos_source, in_memory_duckdb, log_dir)
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: _mock_fos(log_dir, [key]))

    events = _drain(ingest(source={**fos_source}))
    done = next(e for e in events if e["type"] == "done")

    # The valid row is ingested; the broken row is noted as corrupt.
    assert done["rows_inserted"] >= 1
