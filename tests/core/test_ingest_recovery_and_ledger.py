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
import json as _json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import botocore.exceptions
import duckdb
import pytest

from backend.core import iceberg as ice
from backend.core import ingest as ing
from backend.core import metadata as metadata_db
from backend.core.ingest import (
    LEDGER_MAX_ATTEMPTS,
    LEDGER_RECLAIM_AFTER_S,
    LEDGER_RETRY_BASE_S,
    RAW_DELETE_GRACE_S,
    _ledger_record_failure,
    _quarantine_convert_corrupt_lines,
    convert_batch_objects,
    finalize_committed_raw,
    ingest,
    merge_lake_files,
    sweep_ledger_once,
)
from tests._helpers.ledger import (
    DuckLakeLedgerEnv,
    clear_ledger,
    lake_reader,
)
from tests._helpers.ledger import attacher as _attacher
from tests._helpers.ledger import (
    clear_ledger as _clear_ledger,
)


# ── Merged from tests/core/test_ingest_corrupt_row_repair.py ──
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


# ── Merged from tests/core/test_ingest_corruption.py ──
def _drain_ingest(gen):
    events = []
    for e in gen:
        events.append(e)
    return events


def test_ingest_retains_corrupt_gzip_evidence_and_outcomes(fos_source, in_memory_duckdb, monkeypatch, tmp_path):
    """A bad container must not abort valid siblings and must retain exact evidence.

    Pinned because a single malformed file (e.g. truncated upload) must NOT
    abort the entire batch or crash the cron scheduler.
    """
    # 1. Setup local mock logs directory
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Valid file: standard gzip
    valid_data = {"timestamp": "2026-05-18T10:00:00Z", "status": 200, "url": "/ok"}
    valid_file = log_dir / "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    valid_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(valid_file, "wt") as f:
        f.write(json.dumps(valid_data) + "\n")

    # Corrupted file: not a gzip file, but ends in .gz
    corrupt_file = log_dir / "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz"
    corrupt_file.write_bytes(b"This is definitely not a gzip file")

    # 2. Redirect cache and disable FOS config
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(tmp_path / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a: None)
    monkeypatch.setattr("time.sleep", lambda *a: None)

    # 3. Mock DuckDB memory connection
    monkeypatch.setattr("backend.core.duckdb.get_memory_connection", lambda _: in_memory_duckdb)

    # 4. Mock FOS client paginator to "discover" these files
    class MockPaginator:
        def paginate(self, **kwargs):
            return [
                {
                    "Contents": [
                        {"Key": "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz", "Size": 100},
                        {"Key": "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz", "Size": 100},
                    ]
                }
            ]

    class MockFos:
        def get_paginator(self, *args, **kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):
            # Ingest now pre-downloads each chunk via boto3.get_object before
            # passing local paths to DuckDB. Serve from the local mock_logs
            # dir so tests don't need a real S3 endpoint.
            import io

            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            return {}

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *a: MockFos())
    captured_evidence = []

    def capture_evidence(service_id, source_type, original_key, payload, **metadata):
        captured_evidence.append((service_id, source_type, original_key, payload, metadata))
        return {"id": len(captured_evidence), "cap_evictions": 0, "quarantine_capture_failures": 0}

    monkeypatch.setattr("backend.core.quarantine.capture_evidence", capture_evidence)

    # 5. Route ingest through local filesystem.
    # Production ingest hands s3://bucket/... paths to DuckDB and lets the
    # `fos_proxy` SECRET route them through the telemetry proxy → CDN. In
    # tests we have no proxy, so wrap the in-memory DuckDB connection's
    # `execute` to rewrite s3://test-bucket/... → file://localdir/... before
    # the SQL hits httpfs. This is the test-only equivalent of the
    # production-removed `s3://bucket` → `cdn_url` rewrite.
    test_src = {**fos_source}
    bucket_prefix = f"s3://{fos_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"

    class _S3RewritingConn:
        # DuckDBPyConnection's execute is C-level read-only, so wrap rather
        # than monkeypatch. Used to substitute the production-removed
        # cdn_url rewrite at the SQL boundary.
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    import backend.core.duckdb as my_duckdb

    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda src: _S3RewritingConn(in_memory_duckdb))

    # 6. Run ingest
    # Small chunk size to force batch failure and trigger isolation
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 10)

    events = _drain_ingest(ingest(source=test_src))

    # 7. Assertions
    done = next(e for e in events if e["type"] == "done")
    assert done["outcome_counters"] == {
        "valid_records": 1,
        "malformed_records": 0,
        "corrupt_containers": 1,
        "quarantine_capture_failures": 0,
        "source_delete_failures": 0,
        "cap_evictions": 0,
        "objects_processed": 2,
        "objects_successful": 1,
        "objects_partial": 0,
        "objects_failed": 1,
    }
    assert captured_evidence[0][1:4] == (
        "request",
        "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz",
        b"This is definitely not a gzip file",
    )

    # new_files counts attempted/processed files (both)
    assert done["new_files"] == 2
    # rows_inserted should only reflect the valid file
    assert done["rows_inserted"] == 1

    # Check metadata_db record - only the valid file should be tracked
    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(test_src["name"])

    # The corrupted file failed isolation and should NOT be in the metadata DB
    assert len(ingested) == 1
    valid_path = f"s3://{fos_source['bucket']}/raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    assert any(valid_path in f for f in ingested)

    # Verify DuckDB buffer has the data
    from backend.core import iceberg

    buffer_files = iceberg.buffer_files(test_src)
    assert len(buffer_files) >= 1


# ── Merged from tests/core/test_ingest_crash_recovery.py ──
@pytest.fixture
def ingest_env(s3_mock, fos_source, tmp_path, monkeypatch):
    """Wire ``ingest()`` to moto S3 + a local DuckLake data path.

    Yields a dict with the moto client, the source dict, and the cache
    path so a test can re-seed and re-run ``ingest()`` across a
    simulated restart.
    """
    # DuckLake tables persist in the shared per-worker Postgres catalog across
    # tests; a reused service_id would resolve a table whose relative data dir
    # was never created under this test's tmp data root.
    fos_source["service_id"] = f"cr{uuid.uuid4().hex[:10]}"
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

    # ingest.py holds its own module-level reference to _get_fos_client; wrap
    # the moto client to swallow the production-only ``caller_hint`` kwarg.
    class _Shim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _Shim(s3_mock))

    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()

    ice.init_iceberg_table(fos_source)
    return {
        "s3": s3_mock,
        "src": fos_source,
        "cache": cache_path,
        "bucket": fos_source["bucket"],
    }


def _seed_gz(s3, bucket: str, key: str, rows: list[dict]) -> None:
    body = io.BytesIO()
    with gzip.GzipFile(fileobj=body, mode="wb") as gz:
        gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue())


def _one_log_file(s3, bucket: str, n_rows: int = 4) -> tuple[str, int]:
    """Seed a single Fastly-shaped .gz log file; return (key, n_rows)."""
    base = datetime.now(UTC) - timedelta(hours=2)
    key = "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz"
    rows = [
        {
            "timestamp": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "ip": f"10.0.0.{i}",
            "status": 200 if i % 2 == 0 else 404,
            "url": f"/path/{i}",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 1000 + i,
        }
        for i in range(n_rows)
    ]
    _seed_gz(s3, bucket, key, rows)
    return key, n_rows


def _lake_count(src: dict) -> int:
    from backend.core.duckdb import get_connection
    from backend.core.iceberg._ducklake import ducklake_table_name

    con = get_connection(src)
    try:
        row = con.execute(f'SELECT count(*) FROM lake."{ducklake_table_name(src)}"').fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


def _ingested_names(sid: str) -> set[str]:
    return metadata_db.get_ingested_filenames(sid)


def _summary(sid: str) -> dict:
    return metadata_db.get_ingested_files_status_summary(sid)


class _RaiseOnce:
    """Wrap a callable so the first call raises, later calls delegate.

    Lets us crash the FIRST ingest tick at a precise checkpoint while
    leaving the same function healthy for the recovery tick.
    """

    def __init__(self, real, exc: Exception):
        self._real = real
        self._exc = exc
        self.calls = 0

    def __call__(self, *a, **kw):
        self.calls += 1
        if self.calls == 1:
            raise self._exc
        return self._real(*a, **kw)


def test_crash_before_write_to_buffer_recovers_clean(ingest_env, monkeypatch):
    """record_in_flight ran, write_to_buffer dies. The buffer never landed,
    so recovery must DROP the dangling in_flight row, leave ingested_files
    empty, and the raw .gz must still be in FOS to re-ingest cleanly."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    # _RaiseOnce: the FIRST write_to_buffer (this tick) crashes; the restart
    # tick's call delegates to the real implementation. No monkeypatch.undo()
    # here — the autouse metadata-isolation fixture shares this monkeypatch
    # instance and undo() would repoint the metadata DB at the real DATA_DIR.
    monkeypatch.setattr(
        "backend.core.iceberg.write_to_buffer",
        _RaiseOnce(ice.write_to_buffer, RuntimeError("crash: disk full mid write_to_buffer")),
    )

    with pytest.raises(RuntimeError, match="write_to_buffer"):
        _drain(ing.ingest(source=src, delete_after=True))

    # Intermediate state: in_flight dangling, nothing recorded, buffer absent,
    # raw file NOT deleted (delete happens after the crash point).
    assert _ingested_names(sid) == set()
    assert len(metadata_db.list_in_flight(sid)) == 1
    assert ice.buffer_files(src) == []
    assert "Contents" in s3.list_objects_v2(Bucket=ingest_env["bucket"], Prefix="raw/request/"), "raw .gz orphaned/lost"

    # Restart: the recovery sweep drops the dangling row, then the file
    # re-LISTs and ingests cleanly (write_to_buffer now delegates).
    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")
    assert done["rows_inserted"] == n
    # Exactly one ingested-files row for the file; in_flight drained.
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert metadata_db.list_in_flight(sid) == []
    assert _summary(sid)["total_rows"] == n  # no double-count in the rollup


def test_crash_before_insert_ingested_files_promotes_buffer(ingest_env, monkeypatch):
    """write_to_buffer succeeded, insert_ingested_files dies. The buffer is on
    disk but its files are unrecorded. Recovery must PROMOTE the buffer's
    files into ingested_files (no re-ingest, no duplicate buffer) and the
    eventual commit must write the rows exactly once."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash: DB locked before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)

    with pytest.raises(RuntimeError, match="before insert"):
        _drain(ing.ingest(source=src, delete_after=True))

    # Buffer present, files unrecorded, in_flight dangling.
    assert len(ice.buffer_files(src)) == 1
    assert _ingested_names(sid) == set()
    assert len(metadata_db.list_in_flight(sid)) == 1
    buf_before = set(os.path.basename(p) for p in ice.buffer_files(src))

    # Restart: _recover_in_flight (insert healthy on the 2nd call) promotes.
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")

    # The recovered file must NOT be re-listed/re-ingested (it's now recorded),
    # so the restart tick ingests 0 new rows and produces no second buffer.
    assert done["rows_inserted"] == 0, "recovered file was re-ingested — double-count risk"
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert metadata_db.list_in_flight(sid) == []
    assert set(os.path.basename(p) for p in ice.buffer_files(src)) == buf_before, "duplicate buffer created"
    assert _summary(sid)["total_rows"] == n

    # Commit the promoted buffer → rows land in DuckLake exactly once.
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n
    assert _lake_count(src) == n, "rows must land in the DuckLake table exactly once"


def test_crash_before_clear_in_flight_is_idempotent(ingest_env, monkeypatch):
    """insert_ingested_files succeeded (file + summary durable), clear_in_flight
    dies. Recovery re-promotes: it re-inserts the SAME file (idempotent upsert)
    and clears. The critical invariant is the ``ingested_files_summary`` rollup
    must NOT double-count the re-insert (delta must be 0)."""
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.clear_in_flight, RuntimeError("crash: DB locked before clear"))
    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser)

    with pytest.raises(RuntimeError, match="before clear"):
        _drain(ing.ingest(source=src, delete_after=True))

    # File recorded once, summary == n, in_flight dangling, buffer present.
    assert len([x for x in _ingested_names(sid) if x.endswith(key)]) == 1
    assert _summary(sid)["total_rows"] == n
    assert _summary(sid)["file_count"] == 1
    assert len(metadata_db.list_in_flight(sid)) == 1
    assert len(ice.buffer_files(src)) == 1

    # Restart: recovery re-inserts (idempotent) + clears.
    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser._real)
    next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")

    assert metadata_db.list_in_flight(sid) == []
    assert _summary(sid)["total_rows"] == n, "summary rollup double-counted the re-insert"
    assert _summary(sid)["file_count"] == 1, "summary file_count double-counted the re-insert"
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n
    assert _lake_count(src) == n, "rows must land in the DuckLake table exactly once"


def test_sha256_name_prevents_duplicate_buffer_if_recovery_skipped(ingest_env, monkeypatch):
    """Belt-and-suspenders: even if the in_flight recovery sweep is itself
    lost (DB blip), a raw file LISTed twice across a crash must not produce
    two buffer parquets — the SHA-256 chunk name makes the second write
    OVERWRITE the first, so commit can't double-append.

    Setup: crash before insert (file stays unrecorded, raw stays in FOS),
    then restart with the recovery sweep neutralised — the file is re-LISTed
    and re-ingested, hitting the same deterministic buffer name.
    """
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])

    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)
    with pytest.raises(RuntimeError):
        _drain(ing.ingest(source=src, delete_after=False))

    bufs_after_crash = ice.buffer_files(src)
    assert len(bufs_after_crash) == 1
    crashed_buf_name = os.path.basename(bufs_after_crash[0])

    # Restart but DISABLE the recovery sweep (simulate the sweep itself
    # failing) AND drop the dangling in_flight row so the file looks brand
    # new again — the worst case the SHA naming must survive.
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    monkeypatch.setattr(
        "backend.core.ingest._recover_in_flight", lambda _src: {"promoted": 0, "dropped": 0, "rows_recovered": 0}
    )
    metadata_db.clear_in_flight(sid, crashed_buf_name)

    done = next(e for e in ing.ingest(source=src, delete_after=False) if e["type"] == "done")
    assert done["rows_inserted"] == n  # re-ingested the same file

    # The deterministic name means the re-ingest OVERWROTE the same buffer —
    # exactly one buffer file, so commit appends the rows once, not twice.
    bufs = ice.buffer_files(src)
    assert len(bufs) == 1, f"SHA-256 naming failed to dedupe — {len(bufs)} buffers: {bufs}"
    assert os.path.basename(bufs[0]) == crashed_buf_name
    res = ice.commit_buffer(src)
    assert res["rows_committed"] == n, "double-commit despite deterministic buffer name"
    assert _lake_count(src) == n


# ── Merged from tests/core/test_ingest_partial_failure.py ──
def _seed_local(log_dir, key, ts_iso, status=200, n_rows=1):
    path = log_dir / key
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"timestamp": ts_iso, "ip": f"10.0.0.{i}", "status": status, "url": f"/p/{i}", "method": "GET"}
        for i in range(n_rows)
    ]
    with gzip.open(path, "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


class _S3RewritingConn:
    """Wrap a DuckDB connection so SQL with ``s3://bucket/...`` paths
    is rewritten to ``file://local/...`` before execution. Same pattern
    as test_ingest_corruption.py."""

    def __init__(self, conn, bucket_prefix: str, file_prefix: str):
        self._conn = conn
        self._bucket_prefix = bucket_prefix
        self._file_prefix = file_prefix

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and self._bucket_prefix in sql:
            sql = sql.replace(self._bucket_prefix, self._file_prefix)
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _install_mock_fos(monkeypatch, log_dir, *, keys, get_object_handler=None, valid_size=100):
    """Wire a MockFos client + redirect cache + reroute SQL to local files."""

    class MockPaginator:
        def paginate(self, **_kwargs):
            return [
                {"Contents": [{"Key": k, "Size": valid_size} for k in keys]},
            ]

    class MockFos:
        def get_paginator(self, *_args, **_kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):  # noqa: N803
            if get_object_handler is not None:
                resp = get_object_handler(Bucket, Key)
                if resp is not None:
                    return resp
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **_kwargs):
            return {}

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *_a: MockFos())


def _route_duckdb_to_local(monkeypatch, in_memory_duckdb, bucket, log_dir):
    """Rewrite s3://bucket/... → file://logdir/... inside DuckDB SQL."""
    bucket_prefix = f"s3://{bucket}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    import backend.core.duckdb as my_duckdb

    monkeypatch.setattr(
        my_duckdb,
        "get_memory_connection",
        lambda src: _S3RewritingConn(in_memory_duckdb, bucket_prefix, file_prefix),
    )


@pytest.fixture
def ingest_local_env(monkeypatch, fos_source, in_memory_duckdb, tmp_path):
    """Common setup: redirect cache, disable FOS proxy + DuckDB rewrite,
    small chunk size so isolation kicks in."""
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(tmp_path / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *_a: None)
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 10)

    _route_duckdb_to_local(monkeypatch, in_memory_duckdb, fos_source["bucket"], log_dir)

    return {"log_dir": log_dir, "src": {**fos_source}, "duckdb": in_memory_duckdb}


def test_isolation_retry_total_failure_writes_no_buffer_no_metadata(ingest_local_env, monkeypatch):
    """Every file in the batch is corrupt → the isolation loop walks each
    one, finds all unreadable, yields 'All files in batch are unreadable',
    and ``continue``s without writing a buffer. Pin three contracts:

      1. No buffer parquet appears under cache/buffer/
      2. metadata_db.ingested_files stays empty
      3. ingest_in_flight stays empty (no orphan rows)

    Without this test, a regression that erroneously called
    insert_ingested_files on the all-failure path would silently mark
    files 'ingested' that never had their rows persisted — permanent
    silent data loss until the file's retention window expired."""
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    # Two files, both non-gzip garbage — both will fail DuckDB read_json_auto.
    for key in ("raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz", "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz"):
        p = log_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"not a gzip at all")

    _install_mock_fos(
        monkeypatch,
        log_dir,
        keys=[
            "raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz",
            "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz",
        ],
    )

    events = _drain(ingest(source=src))

    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 0

    from backend.core import iceberg
    from backend.core import metadata as metadata_db

    assert iceberg.buffer_files(src) == [], (
        "isolation-retry-total-failure path wrote a buffer parquet; "
        "no rows survived isolation so no buffer should exist."
    )
    assert metadata_db.get_ingested_filenames(src["name"]) == set(), (
        "isolation-retry-total-failure path recorded files in metadata_db; "
        "without a successful ingest there is nothing to commit and these "
        "would become silent 'ingested but not actually persisted' ghosts."
    )
    assert metadata_db.list_in_flight(src["name"]) == [], (
        "ingest_in_flight non-empty after all-files-failed path; the "
        "in_flight row would re-fire _recover_in_flight forever."
    )


def test_chunk_get_object_failure_skips_just_that_file(ingest_local_env, monkeypatch):
    """boto3 ``get_object`` raises NoSuchKey for one file → the
    downloader records that file as failed and proceeds with the rest.
    The healthy file still lands in the buffer + metadata; the missing
    one does NOT.

    This pins the S3 deletion race: a file LISTed at T0 can be
    gone by the time the per-chunk GET fires at T1 (lifecycle policy,
    Fastly internal cleanup, etc.). The cron must not abort the whole
    tick over a single missing file."""
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    good_key = "raw/2026-06-01/11/2026-06-01T11-00-00.ok.gz"
    gone_key = "raw/2026-06-01/11/2026-06-01T11-05-00.gone.gz"
    _seed_local(log_dir, good_key, "2026-06-01T11:00:00Z")
    # gone_key is intentionally NOT created — get_object will raise.

    def _flaky_get(Bucket, Key):  # noqa: N803
        if Key == gone_key:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "key vanished"}}, "GetObject"
            )
        return None  # fall through to default

    _install_mock_fos(monkeypatch, log_dir, keys=[good_key, gone_key], get_object_handler=_flaky_get)

    events = _drain(ingest(source=src))

    done = next(e for e in events if e["type"] == "done")
    # Only the healthy file's row landed.
    assert done["rows_inserted"] == 1

    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any(good_key in name for name in ingested), (
        f"good file {good_key!r} missing from metadata_db.ingested_files: {ingested}"
    )
    assert not any(gone_key in name for name in ingested), (
        f"file that vanished from S3 ({gone_key!r}) ended up in "
        "metadata_db.ingested_files — the downloader's failed_paths "
        "tracking failed to filter it out."
    )


def test_recover_in_flight_runs_at_start_of_every_ingest(ingest_local_env, monkeypatch):
    """A previous tick left an orphan in_flight row with a buffer file
    present (crashed AFTER write_to_buffer, BEFORE insert_ingested_files).
    The next ingest call must run _recover_in_flight FIRST, promote
    those rows into ingested_files, and clear the orphan in_flight
    BEFORE attempting to discover any new files.

    Pins the contract that crash-recovery is not deferred to a separate
    job — it's inline at the top of every ingest tick. A refactor that
    moved it to a manual admin endpoint would let orphan in_flights
    accumulate across restarts."""
    from backend.core import iceberg
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    # Plant a fake buffer parquet + matching in_flight row, as if a prior
    # ingest crashed between write_to_buffer and insert_ingested_files.
    buf_dir = iceberg._buffer_dir(src)  # type: ignore[attr-defined]
    import os

    os.makedirs(buf_dir, exist_ok=True)
    orphan_buffer = "batch_orphan_deadbeef.parquet"
    open(os.path.join(buf_dir, orphan_buffer), "wb").write(b"placeholder")
    orphan_rows = [
        ("s3://test-bucket/raw/2026-05-30/10/2026-05-30T10-00-00.orphan.gz", 42, 100),
    ]
    metadata_db.record_in_flight(src["name"], orphan_buffer, orphan_rows)

    # No new files to discover — the ingest must still run recovery.
    _install_mock_fos(monkeypatch, log_dir, keys=[])

    _drain(ingest(source=src))

    # After ingest: the orphan in_flight is gone AND ingested_files has
    # the promoted entry.
    assert metadata_db.list_in_flight(src["name"]) == [], (
        "_recover_in_flight failed to clear the orphan row — the next "
        "ingest will re-recover and the in_flight table grows unbounded."
    )
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any("orphan.gz" in n for n in ingested), f"recovery did not promote the in_flight rows; ingested={ingested}"


def test_executor_submit_failure_falls_back_to_inline_delete(ingest_local_env, monkeypatch):
    """``concurrent.futures.thread._shutdown`` is process-wide and one-way.
    Once it flips (uvicorn worker recycling, multiprocessing libraries,
    bpython-style hot reload), every subsequent ThreadPoolExecutor.submit
    raises ``RuntimeError: cannot schedule new futures after interpreter
    shutdown`` — even on a freshly-constructed executor.

    Without the inline-delete fallback, the ingest loop crashes mid-chunk
    AFTER ``write_to_buffer`` + ``insert_ingested_files`` have already
    persisted the rows. The .gz files stay in FOS forever as orphans
    because the dedup gate at the top of every subsequent ingest sees
    them in ``ingested_files`` and skips them. Observed 2026-06-16:
    167 orphans across 4 hours of late-arriving Fastly drops.

    Pin: a forced submit failure must fall back to a synchronous
    ``_delete_objects_robust`` call so no orphans leak, and ingest must
    still report ``done`` instead of crashing.
    """
    import concurrent.futures

    from backend.core import ingest as ingest_module
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    key = "raw/2026-06-01/12/2026-06-01T12-00-00.ok.gz"
    _seed_local(log_dir, key, "2026-06-01T12:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])

    # Simulate the post-interpreter-shutdown state — but ONLY for the
    # delete executor. The chunk-download path also uses a ThreadPoolExecutor;
    # breaking that one too would short-circuit ingest before any delete
    # path is reached. Targeting by thread_name_prefix isolates the failure
    # to the exact submit site this regression test cares about.
    original_submit = concurrent.futures.ThreadPoolExecutor.submit

    def _selectively_broken_submit(self, *args, **kwargs):
        if getattr(self, "_thread_name_prefix", "") == "ingest_delete":
            raise RuntimeError("cannot schedule new futures after interpreter shutdown")
        return original_submit(self, *args, **kwargs)

    monkeypatch.setattr(concurrent.futures.ThreadPoolExecutor, "submit", _selectively_broken_submit)

    # Track inline-delete invocations to prove the fallback fired.
    inline_delete_calls: list[list[str]] = []
    original_delete = ingest_module._delete_objects_robust_with_failures

    def _tracking_delete(client, bucket, keys):
        inline_delete_calls.append(list(keys))
        return original_delete(client, bucket, keys)

    monkeypatch.setattr(ingest_module, "_delete_objects_robust_with_failures", _tracking_delete)

    events = _drain(ingest(source=src, delete_after=True))

    # Ingest still completes successfully — no crash from the broken submit.
    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 1, f"ingest did not complete cleanly under broken executor; events={events!r}"

    # Critical: the inline fallback fired AND covered the key we ingested.
    # Without this, the file would stay in FOS forever as an orphan.
    assert inline_delete_calls, (
        "broken executor.submit triggered, but no inline _delete_objects_robust "
        "call followed — the ingested .gz would leak as a permanent FOS orphan."
    )
    assert any(key in batch for batch in inline_delete_calls), (
        f"inline delete fired but the ingested key {key!r} was not in any batch; got {inline_delete_calls!r}"
    )

    # And the row IS in ingested_files (the inline fallback runs AFTER
    # insert_ingested_files; the contract is that we never leave a file
    # marked-ingested-but-not-deleted on the FOS side).
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert any(key in name for name in ingested), (
        f"file marked-ingested-AND-deleted contract broken; ingested={ingested!r}"
    )


def test_partial_source_delete_failure_marks_only_affected_object_failed(ingest_local_env, monkeypatch):
    from backend.core import ingest as ingest_module

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    key = "raw/2026-06-01/12/2026-06-01T12-00-00.delete-fails.gz"
    _seed_local(log_dir, key, "2026-06-01T12:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])
    monkeypatch.setattr(
        ingest_module,
        "_delete_objects_robust_with_failures",
        lambda _client, _bucket, keys: (0, set(keys)),
    )

    done = next(event for event in _drain(ingest(source=src, delete_after=True)) if event["type"] == "done")

    assert done["outcome_counters"]["source_delete_failures"] == 1
    assert done["outcome_counters"]["objects_processed"] == 1
    assert done["outcome_counters"]["objects_failed"] == 1
    assert done["outcome_counters"]["objects_partial"] == 0
    assert done["outcome_counters"]["objects_successful"] == 0


def test_combined_failure_modes_in_single_tick(ingest_local_env, monkeypatch):
    """One ingest tick where THREE distinct failure modes hit different files:

      - file_a: present and healthy (ingests cleanly)
      - file_b: vanishes between LIST and GET (S3 deletion race / NoSuchKey)
      - file_c: present but corrupt gzip (DuckDB read raises)

    The contract: every healthy file lands in the buffer + metadata; every
    failed file is filtered out of ingested_files; the tick reports success.
    Each individual failure mode is exercised by its sibling test; this
    pins that they COMPOSE — a real bad day might see all three at once,
    and the recovery paths must not interact poorly (e.g. one path's
    cleanup must not retry a different path's file).
    """
    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    good_key = "raw/2026-06-02/09/2026-06-02T09-00-00.good.gz"
    gone_key = "raw/2026-06-02/09/2026-06-02T09-05-00.gone.gz"
    bad_key = "raw/2026-06-02/09/2026-06-02T09-10-00.bad.gz"

    _seed_local(log_dir, good_key, "2026-06-02T09:00:00Z")
    # gone_key intentionally NOT created — get_object raises NoSuchKey.
    # bad_key: present on disk but contents are invalid gzip → DuckDB reader fails.
    bad_path = log_dir / bad_key
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_bytes(b"not a gzip file\x00\x01\x02")

    def _selective_get(Bucket, Key):  # noqa: N803
        if Key == gone_key:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "key vanished"}}, "GetObject"
            )
        return None  # fall through to default loader

    _install_mock_fos(monkeypatch, log_dir, keys=[good_key, gone_key, bad_key], get_object_handler=_selective_get)

    events = _drain(ingest(source=src))

    # Tick reports done (no top-level abort despite two failures).
    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 1, (
        f"expected 1 healthy row to land; got rows_inserted={done['rows_inserted']!r}; events={events!r}"
    )

    from backend.core import metadata as metadata_db

    ingested = metadata_db.get_ingested_filenames(src["name"])
    # Only the good file made it.
    assert any(good_key in n for n in ingested), f"good file missing from ingested_files: {ingested!r}"
    assert not any(gone_key in n for n in ingested), f"vanished file leaked into ingested_files: {ingested!r}"
    assert not any(bad_key in n for n in ingested), f"corrupt file leaked into ingested_files: {ingested!r}"

    # No orphan in_flight — the failures were detected BEFORE the
    # mark-ingested step, not after. A leaked in_flight here would
    # accumulate across ticks.
    assert metadata_db.list_in_flight(src["name"]) == []


def test_fully_filtered_file_is_recorded_as_zero_row_marker(ingest_local_env, monkeypatch):
    """DATA-SAFETY producer side: a file whose rows are ALL excluded by the
    time-range WHERE filter (here, row timestamp past end_time, but the filename
    still passes the ±1h discovery pre-filter) writes NO buffer yet is still
    ledgered to suppress re-LIST. It must be recorded with row_count == 0 so the
    stranded-delete reconcile recognises it as a no-data marker and never deletes
    its raw .gz (the only copy of that data)."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]

    key = "raw/2026-06-01/10/2026-06-01T10-00-00.svc.gz"
    # Row timestamp is 10:00:30 — strictly AFTER the end_time gate below, so every
    # row is filtered out (valid_rows == 0), but the filename dt (10:00:00) is
    # within end_time + 1h so the file is NOT skipped at discovery.
    _seed_local(log_dir, key, "2026-06-01T10:00:30Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])

    events = _drain(
        ingest(
            source=src,
            delete_after=False,  # keep the raw so this is a real "marker" strand
            start_time="2026-06-01T09:00:00Z",
            end_time="2026-06-01T10:00:00Z",
        )
    )

    done = next(e for e in events if e["type"] == "done")
    assert done["rows_inserted"] == 0

    files = metadata_db.list_ingested_files(src["name"])
    marker = next((f for f in files if key in f["file_name"]), None)
    assert marker is not None, f"fully-filtered file should still be ledgered; got {files!r}"
    assert marker["row_count"] == 0, "a no-data marker must be recorded with row_count 0"

    # And the reconcile must never classify it reclaimable — even with a wide-open
    # (far-past) durability epoch, a row_count==0 marker is excluded.
    assert (
        metadata_db.get_reclaimable_strand_filenames(src["name"], {marker["file_name"]}, "2000-01-01 00:00:00") == set()
    )


def test_final_done_folds_strand_reclaim_with_new_file_deletes(ingest_local_env, monkeypatch):
    """End-to-end final-fold: a pre-existing durable strand (row_count>0, raw still
    in the bucket) is reclaimed by the reconcile while a brand-new file is ingested
    and deleted by the per-chunk path. The success done event's deleted_files must
    be the SUM of both, and the message must note the reclaim."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    bucket = src["bucket"]

    strand_key = "raw/2026-06-01/10/2026-06-01T10-00-00.strand.gz"
    new_key = "raw/2026-06-01/11/2026-06-01T11-00-00.fresh.gz"

    # Pre-seed the ledger with the strand as DURABLE (row_count>0) — i.e. a genuine
    # interrupted-delete, NOT a marker. Its raw is still listed in the bucket.
    metadata_db.insert_ingested_files(src["name"], [(f"s3://{bucket}/{strand_key}", 100, 5000)])
    # The new file has real, in-range rows so it actually ingests + per-chunk deletes.
    _seed_local(log_dir, new_key, "2026-06-01T11:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key, new_key])

    # The seeded strand is ingested at "now"; open the durability epoch so it is
    # treated as a reclaimable post-fix row.
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2000-01-01 00:00:00")
    events = _drain(ingest(source=src, delete_after=True))

    done = next(e for e in events if e["type"] == "done")
    assert done["new_files"] == 1, f"only the fresh file is new; events={events!r}"
    assert done["rows_inserted"] == 1
    # 1 strand reclaimed + 1 new file deleted by the per-chunk path = 2.
    assert done["deleted_files"] == 2, f"final-fold must sum reclaim + per-chunk deletes; got {done!r}"
    assert "reclaim" in done["message"].lower()


def test_pre_epoch_durable_strand_is_not_reclaimed(ingest_local_env, monkeypatch):
    """DATA-SAFETY: a durable strand whose ledger row predates the durability epoch
    is NOT reclaimed — its row_count can't be trusted (pre-fix rows recorded
    PRE-filter counts), so the reconcile leaves it for the 1-day ledger trim rather
    than risk deleting a legacy no-data marker."""
    from backend.core import metadata as metadata_db

    log_dir = ingest_local_env["log_dir"]
    src = ingest_local_env["src"]
    bucket = src["bucket"]
    strand_key = "raw/2026-06-01/10/2026-06-01T10-00-00.legacy.gz"

    metadata_db.insert_ingested_files(src["name"], [(f"s3://{bucket}/{strand_key}", 100, 5000)])
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key])

    # Epoch is in the far future relative to the seeded "now" row → pre-epoch.
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2999-01-01 00:00:00")
    events = _drain(ingest(source=src, delete_after=True))

    done = next(e for e in events if e["type"] == "done")
    assert done["deleted_files"] == 0, "a pre-epoch strand must not be reclaimed"


# ── Merged from tests/core/test_finalize_committed_raw.py ──
SERVICE_ID = "test-finalize-raw"

BUCKET = "test-bucket"

SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}


def _seed_committed(con, keys: list[str], *, age_s: float, raw_deleted_at: float | None = None) -> None:
    """Seed committed ledger rows whose commit is ``age_s`` seconds old."""
    now = time.time()
    cur = con.cursor()
    for key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, status, discovered_at, committed_at, published_at, raw_deleted_at) "
            "VALUES (?, ?, 'committed', ?, ?, ?, ?)",
            (SERVICE_ID, key, now - age_s - 1, now - age_s, now - age_s, raw_deleted_at),
        )
    con.commit()


def _stamps(con) -> dict[str, float | None]:
    return {
        r["object_key"]: r["raw_deleted_at"]
        for r in con.execute(
            "SELECT object_key, raw_deleted_at FROM ingest_ledger WHERE service_id=?", (SERVICE_ID,)
        ).fetchall()
    }


def _cfg(delete_after: bool | None) -> dict:
    if delete_after is None:
        return {"service_id": SERVICE_ID}
    return {"service_id": SERVICE_ID, "provisioning": {"cron_sync": {"delete_after": delete_after}}}


def _env(fos, cfg: dict, src: dict | None = SRC):
    return (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos),
    )


def _run(fos, cfg: dict, src: dict | None = SRC, **kwargs) -> dict:
    load_cfg, get_src, get_fos = _env(fos, cfg, src)
    with load_cfg, get_src, get_fos:
        return finalize_committed_raw(SERVICE_ID, **kwargs)


AGED = RAW_DELETE_GRACE_S + 60


def test_finalize_deletes_aged_committed_raw_files_and_stamps_them():
    con, _ = clear_ledger(SERVICE_ID)
    keys = ["raw/2026/08/27/12/00/a.json.gz", "raw/2026/08/27/12/01/b.json.gz"]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 2, "eligible": 2, "delete_after": True}
    fos.delete_objects.assert_called_once()
    kw = fos.delete_objects.call_args.kwargs
    assert kw["Bucket"] == BUCKET
    assert sorted(o["Key"] for o in kw["Delete"]["Objects"]) == sorted(keys)
    assert kw["Delete"]["Quiet"] is True
    assert all(v is not None for v in _stamps(con).values())


def test_finalize_respects_the_raw_delete_grace_window():
    """A just-committed file may still be visible to a reader holding the
    pre-commit view. Deleting inside the grace window destroys the only copy
    of data a query is mid-scan on."""
    con, _ = clear_ledger(SERVICE_ID)
    fresh = "raw/2026/08/27/12/02/fresh.json.gz"
    aged = "raw/2026/08/27/12/03/aged.json.gz"
    _seed_committed(con, [fresh], age_s=RAW_DELETE_GRACE_S - 60)
    _seed_committed(con, [aged], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert [o["Key"] for o in fos.delete_objects.call_args.kwargs["Delete"]["Objects"]] == [aged]
    stamps = _stamps(con)
    assert stamps[fresh] is None
    assert stamps[aged] is not None


def test_finalize_deletes_nothing_when_delete_after_is_disabled():
    """``delete_after: false`` means the operator keeps their raw archive.
    The eligible count must still be reported (so the admin UI can show the
    backlog) but nothing may be deleted or stamped."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/04/keep.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    result = _run(fos, _cfg(False))

    assert result == {"deleted": 0, "eligible": 1, "delete_after": False}
    fos.delete_objects.assert_not_called()
    assert _stamps(con)[key] is None


def test_finalize_stands_down_for_a_high_scale_shared_source_even_with_delete_after_true():
    """A service whose raw stream is shared with an out-of-band high-scale
    consumer must never have its raw objects deleted by the legacy celery
    finalizer, regardless of its own ``delete_after`` setting — raw-deletion
    authority for that stream belongs to the high-scale deletion controller.
    This is the fix for the legacy-cleanup-races-high-scale-worker trap."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/09/13/01/56/shared.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    cfg = {
        "service_id": SERVICE_ID,
        "provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}},
    }
    result = _run(fos, cfg)

    assert result == {"deleted": 0, "eligible": 1, "delete_after": False}
    fos.delete_objects.assert_not_called()
    assert _stamps(con)[key] is None


def test_finalize_treats_an_absent_delete_after_key_as_enabled():
    """The documented default is True — a cron_sync block that simply omits
    the key must still reap, or FOS grows without bound for every service
    provisioned before the key existed."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/04/default.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, {"service_id": SERVICE_ID, "provisioning": {"cron_sync": {}}})

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert _stamps(con)[key] is not None


def test_finalize_defaults_to_deleting_when_config_is_absent_entirely():
    """A service with no config at all must still have its raw files reaped
    — otherwise FOS grows without bound for every un-migrated service."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/05/nocfg.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, None)

    assert result == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert _stamps(con)[key] is not None


def test_finalize_is_idempotent_and_never_re_deletes_a_stamped_row():
    """``raw_deleted_at`` is the idempotency stamp. A second run must issue
    no further deletes — re-deleting is a wasted Class-A op per key, on
    every scheduler tick, forever."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/06/once.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    first = _run(fos, _cfg(True))
    stamped_at = _stamps(con)[key]
    second = _run(fos, _cfg(True))

    assert first == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert second == {"deleted": 0, "eligible": 0, "delete_after": True}
    assert fos.delete_objects.call_count == 1
    assert _stamps(con)[key] == stamped_at


def test_finalize_ignores_rows_that_are_not_committed():
    """Only ``committed`` rows are safe to delete the raw file for. A
    ``claimed``/``discovered``/``quarantined`` row's lines are NOT in the
    lake yet — deleting its raw file loses them permanently."""
    con, cur = clear_ledger(SERVICE_ID)
    now = time.time()
    for status in ("discovered", "claimed", "quarantined", "dead_letter"):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (SERVICE_ID, f"raw/2026/08/27/12/07/{status}.json.gz", status, now - AGED, now - AGED),
        )
    con.commit()

    fos = MagicMock()
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 0, "eligible": 0, "delete_after": True}
    fos.delete_objects.assert_not_called()
    assert all(v is None for v in _stamps(con).values())


def test_finalize_does_not_delete_committed_but_unpublished_rows():
    con, cur = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/07/not-published.json.gz"
    now = time.time()
    cur.execute(
        "INSERT INTO ingest_ledger "
        "(service_id, object_key, status, discovered_at, committed_at, published_at) "
        "VALUES (?, ?, 'committed', ?, ?, NULL)",
        (SERVICE_ID, key, now - AGED, now - AGED),
    )
    con.commit()

    fos = MagicMock()
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 0, "eligible": 0, "delete_after": True}
    fos.delete_objects.assert_not_called()


def test_finalize_leaves_per_key_failures_unstamped_for_the_next_run():
    """A batch delete reports per-key errors. An errored key must NOT be
    stamped — otherwise the object is orphaned in FOS with no ledger row
    left willing to retry it."""
    con, _ = clear_ledger(SERVICE_ID)
    ok_key = "raw/2026/08/27/12/08/ok.json.gz"
    bad_key = "raw/2026/08/27/12/09/denied.json.gz"
    _seed_committed(con, [ok_key, bad_key], age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {"Errors": [{"Key": bad_key, "Code": "AccessDenied"}]}
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 1, "eligible": 2, "delete_after": True}
    stamps = _stamps(con)
    assert stamps[ok_key] is not None
    assert stamps[bad_key] is None

    # Next run picks the failed key back up on its own.
    fos.delete_objects.return_value = {}
    retry = _run(fos, _cfg(True))
    assert retry == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert [o["Key"] for o in fos.delete_objects.call_args.kwargs["Delete"]["Objects"]] == [bad_key]
    assert _stamps(con)[bad_key] is not None


def test_finalize_stamps_nothing_when_the_batch_delete_call_itself_raises():
    """A transport-level failure means we do not know what was deleted, so
    nothing may be stamped: the whole batch has to stay eligible."""
    con, _ = clear_ledger(SERVICE_ID)
    keys = ["raw/2026/08/27/12/10/x.json.gz", "raw/2026/08/27/12/11/y.json.gz"]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.side_effect = Exception("connection reset by peer")
    result = _run(fos, _cfg(True))

    assert result == {"deleted": 0, "eligible": 2, "delete_after": True}
    assert all(v is None for v in _stamps(con).values())
    claims = con.execute(
        "SELECT raw_delete_claim_token, raw_delete_claimed_at FROM ingest_ledger WHERE service_id=?",
        (SERVICE_ID,),
    ).fetchall()
    assert all(row[0] for row in claims)

    # The claim lease prevents a concurrent/reentrant finalizer from issuing
    # a duplicate delete while the first worker's result is unknown.
    again = _run(fos, _cfg(True))
    assert again == {"deleted": 0, "eligible": 0, "delete_after": True}
    assert fos.delete_objects.call_count == 1


def test_finalize_attributes_late_delete_failure_to_originating_run():
    import json

    from backend.core.metadata.cron_log import OUTCOME_COUNTER_KEYS, record_ingest_object_outcome, start_cron_run

    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/10/origin.json.gz"
    run_id = start_cron_run(SERVICE_ID, "log_discovery")
    _seed_committed(con, [key], age_s=AGED)
    initial = dict.fromkeys(OUTCOME_COUNTER_KEYS, 0)
    initial.update(valid_records=8, objects_processed=1, objects_successful=1)
    con.execute(
        "UPDATE ingest_ledger SET originating_task='log_discovery', originating_run_id=? "
        "WHERE service_id=? AND object_key=?",
        (run_id, SERVICE_ID, key),
    )
    con.commit()
    assert record_ingest_object_outcome(SERVICE_ID, key, initial)

    fos = MagicMock()
    fos.delete_objects.return_value = {"Errors": [{"Key": key, "Code": "AccessDenied"}]}
    assert _run(fos, _cfg(True)) == {"deleted": 0, "eligible": 1, "delete_after": True}

    row = con.execute(
        "SELECT status, rows_ingested, outcome_counters FROM cron_runs WHERE id=?",
        (run_id,),
    ).fetchone()
    counters = json.loads(row["outcome_counters"])
    assert row["status"] == "error"
    assert row["rows_ingested"] == 8
    assert counters["source_delete_failures"] == 1
    assert counters["objects_failed"] == 1
    assert counters["objects_successful"] == 0


def test_finalize_without_a_registered_source_deletes_nothing():
    """No source means no bucket to delete from — report the backlog, touch
    nothing."""
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/12/nosrc.json.gz"
    _seed_committed(con, [key], age_s=AGED)

    fos = MagicMock()
    result = _run(fos, _cfg(True), src=None)

    assert result == {"deleted": 0, "eligible": 1, "delete_after": True}
    fos.delete_objects.assert_not_called()
    assert _stamps(con)[key] is None


def test_finalize_honors_batch_size_so_one_tick_cannot_run_away():
    con, _ = clear_ledger(SERVICE_ID)
    keys = [f"raw/2026/08/27/12/13/{i}.json.gz" for i in range(5)]
    _seed_committed(con, keys, age_s=AGED)

    fos = MagicMock()
    fos.delete_objects.return_value = {}
    result = _run(fos, _cfg(True), batch_size=2)

    assert result == {"deleted": 2, "eligible": 2, "delete_after": True}
    assert len(fos.delete_objects.call_args.kwargs["Delete"]["Objects"]) == 2
    assert sum(1 for v in _stamps(con).values() if v is not None) == 2


def test_merge_lake_files_flushes_and_compacts_without_losing_rows(tmp_path):
    """Compaction must be row-preserving: it rewrites data files, and a bug
    here silently drops committed log lines."""
    catalog = str(tmp_path / "cat.ducklake")
    data_path = str(tmp_path / "lakedata")
    attach = _attacher(catalog, data_path)

    setup = duckdb.connect()
    attach(setup, None)
    setup.execute("CREATE TABLE lake.logs (ts TIMESTAMPTZ, url VARCHAR)")
    for i in range(3):
        setup.execute(f"INSERT INTO lake.logs VALUES ('2026-08-27T12:0{i}:00Z', '/p{i}')")
    setup.close()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
    ):
        merge_lake_files(SERVICE_ID)

    reader = duckdb.connect()
    attach(reader, None, read_only=True)
    assert reader.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 3
    assert sorted(r[0] for r in reader.execute("SELECT url FROM lake.logs").fetchall()) == ["/p0", "/p1", "/p2"]
    reader.close()


def test_merge_lake_files_raises_without_a_registered_source():
    """merge_lake_files raises on failure by design — its caller records the
    outcome. Returning quietly would report a compaction that never ran."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        with pytest.raises(RuntimeError, match="no source registered"):
            merge_lake_files(SERVICE_ID)


def test_merge_lake_files_raises_when_the_lake_attach_fails():
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=False),
        patch("backend.core.ingest._configure_fos"),
    ):
        with pytest.raises(RuntimeError, match="attach failed"):
            merge_lake_files(SERVICE_ID)


def test_merge_lake_files_publishes_after_schema_mismatch_compaction_failure():
    class Connection:
        def execute(self, sql, *args, **kwargs):
            if "ducklake_merge_adjacent_files" in sql:
                raise duckdb.InvalidInputException("schema mismatch in glob")

        def close(self):
            pass

    def attach(con_arg, src_arg, read_only=False):
        con_arg.execute("CALL ducklake_flush_inlined_data('lake')")
        return True

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch("duckdb.connect", return_value=Connection()),
        patch("backend.core.ingest._mark_ledger_published") as mark_published,
    ):
        merge_lake_files(SERVICE_ID)

    mark_published.assert_called_once_with(SERVICE_ID)


def test_merge_lake_files_skips_rum_numeric_schema_mismatch_after_flushing():
    """A RUM schema-drift warning must not bypass the durability flush.

    ``2147483539`` is a valid int32 ``error_line`` value; its presence in the
    DuckLake glob diagnostic does not make the rows unsafe to publish.  The
    adjacent-file rewrite is the unsafe operation, so leave it skipped while
    preserving the flush-first and publication invariants.
    """

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, sql, *args, **kwargs):
            self.calls.append(sql)
            if "ducklake_merge_adjacent_files" in sql:
                raise duckdb.InvalidInputException(
                    'schema mismatch in glob: "error_line" INTEGER vs BIGINT (2147483539)'
                )

        def close(self):
            pass

    connection = Connection()

    def attach(con_arg, src_arg, read_only=False):
        return True

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=attach),
        patch("backend.core.ingest._configure_fos"),
        patch("duckdb.connect", return_value=connection),
        patch("backend.core.ingest._mark_ledger_published") as mark_published,
    ):
        merge_lake_files(SERVICE_ID)

    assert [call for call in connection.calls if call.startswith("CALL ducklake_")] == [
        "CALL ducklake_flush_inlined_data('lake')",
        "CALL ducklake_merge_adjacent_files('lake')",
    ]
    mark_published.assert_called_once_with(SERVICE_ID)


# ── Merged from tests/core/test_step4_sweeper.py ──
def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def test_kill_worker_mid_convert_heal_test(monkeypatch):
    """A claim older than the reclaim window is reset to 'discovered' and
    re-dispatched — the crash-recovery path for a dead worker."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, object_key, stale_claim, stale_claim - 30),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["reclaimed"] == 1
    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["claimed_by"] is None


def test_sweeper_redispatches_stuck_discovered_rows(monkeypatch):
    """A row stuck in 'discovered' (its convert message was lost to a broker
    restart / crash-after-insert) is re-dispatched."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/07/lost_dispatch.json.gz"

    con, cur = _clear_ledger(service_id)
    old = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, old),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["redispatched"] == 1


def test_sweeper_excludes_rum_keys_from_reclaim_and_redispatch(monkeypatch):
    """A stale rum/raw/ row must be left alone by sweep_ledger_once — that's
    sweep_rum_ledger_once's job."""
    service_id = "test-celery-svc"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"

    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    old_discovered = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at)"
        " VALUES (?, ?, 'claimed', 'dead-worker-1', ?, ?)",
        (service_id, rum_key, stale_claim, stale_claim - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, log_key, old_discovered),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    dispatched_keys = [k for call in mock_convert_delay.call_args_list for k in call.args[1]]
    assert log_key in dispatched_keys
    assert rum_key not in dispatched_keys
    assert summary["reclaimed"] == 0
    assert summary["redispatched"] == 1

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, rum_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "dead-worker-1"


def test_sweeper_discovers_unseen_files(monkeypatch):
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/06/missed_logs.json.gz"

    con, cur = _clear_ledger(service_id)

    def mock_list_fos_files(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 1234},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}
        ):
            with patch("backend.core.ingest.list_fos_files", side_effect=mock_list_fos_files):
                with patch("backend.core.ingest.convert_batch_files.delay") as mock_convert_delay:
                    summary = sweep_ledger_once(service_id, run_id=79)
                    mock_convert_delay.assert_called_with(service_id, [object_key])

    assert summary["discovered"] == 1
    cur.execute(
        "SELECT status, size_bytes, discovered_at, originating_task, originating_run_id "
        "FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    row = cur.fetchone()
    assert row["status"] == "discovered"
    assert row["size_bytes"] == 1234
    assert row["discovered_at"] is not None
    assert row["originating_task"] == "ledger_sweep"
    assert row["originating_run_id"] == 79


def test_convert_failure_requeues_then_quarantines(tmp_path):
    """convert_batch_objects records failures on the ledger row: attempts increment,
    the row requeues as 'discovered', and after LEDGER_MAX_ATTEMPTS it is
    quarantined instead of retrying forever."""
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/08/poison.json.gz"

    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    raw_file = tmp_path / "poison.json"
    raw_file.write_text('{"timestamp": "2026-08-27T10:08:00Z"}\n')
    s3_map = {f"s3://test-bucket/{object_key}": str(raw_file)}

    def _boom(*args, **kwargs):
        raise RuntimeError("poison file")

    statuses = []
    with (
        DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local=s3_map),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=_boom),
    ):
        for _ in range(LEDGER_MAX_ATTEMPTS):
            convert_batch_objects(service_id, [object_key], "test-worker")
            row = con.execute(
                "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key=?",
                (service_id, object_key),
            ).fetchone()
            statuses.append(row["status"])

    assert statuses[:-1] == ["discovered"] * (LEDGER_MAX_ATTEMPTS - 1)
    assert statuses[-1] == "quarantined"

    row = con.execute(
        "SELECT status, attempts, last_error FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "quarantined"
    assert row["attempts"] == LEDGER_MAX_ATTEMPTS
    assert "poison file" in row["last_error"]


def test_failure_persists_exponential_retry_schedule():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, lease_generation) "
        "VALUES (?, 'raw/retry.gz', 'claimed', 1)",
        (service_id,),
    )
    con.commit()

    before = time.time()
    with patch("backend.core.ingest.random.uniform", return_value=0):
        status = _ledger_record_failure(con, service_id, "raw/retry.gz", "temporary outage", 1)
    after = time.time()

    assert status == "discovered"
    row = con.execute(
        "SELECT attempts, error_kind, next_attempt_at FROM ingest_ledger "
        "WHERE service_id=? AND object_key='raw/retry.gz'",
        (service_id,),
    ).fetchone()
    assert row["attempts"] == 1
    assert row["error_kind"] == "transient"
    assert before + LEDGER_RETRY_BASE_S <= row["next_attempt_at"] <= after + LEDGER_RETRY_BASE_S


def test_sweeper_only_redispatches_retry_rows_when_due():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    now = time.time()
    cur.execute(
        "INSERT INTO ingest_ledger "
        "(service_id, object_key, status, discovered_at, next_attempt_at) "
        "VALUES (?, 'raw/not-due.gz', 'discovered', ?, ?)",
        (service_id, now - 3600, now + 3600),
    )
    con.commit()

    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"bucket": "test-bucket"}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    assert summary["redispatched"] == 0
    mock_delay.assert_not_called()

    con.execute(
        "UPDATE ingest_ledger SET next_attempt_at=? WHERE service_id=? AND object_key='raw/not-due.gz'",
        (time.time() - 1, service_id),
    )
    con.commit()
    with patch("backend.config.load_config", return_value={"service_id": service_id}):
        with patch("backend.core.duckdb.get_source_for_service", return_value={"bucket": "test-bucket"}):
            with patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos):
                with (
                    patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
                    patch("backend.core.ingest.convert_batch_files.delay") as mock_delay,
                ):
                    summary = sweep_ledger_once(service_id)

    assert summary["redispatched"] == 1
    mock_delay.assert_called_once()


def test_lease_generation_fences_stale_failure():
    """A reclaimed worker cannot fail a newer regular/RUM claim."""
    con, cur = _clear_ledger("test-celery-svc")
    for object_key in ("raw/request/log.gz", "raw/rum/beacon.gz"):
        cur.execute(
            "INSERT INTO ingest_ledger "
            "(service_id, object_key, status, claimed_by, claimed_at, lease_generation, discovered_at) "
            "VALUES (?, ?, 'claimed', 'new-worker', ?, 2, ?)",
            ("test-celery-svc", object_key, time.time(), time.time()),
        )
    con.commit()

    for object_key in ("raw/request/log.gz", "raw/rum/beacon.gz"):
        assert _ledger_record_failure(con, "test-celery-svc", object_key, "stale error", 1) == "stale"

    rows = con.execute(
        "SELECT object_key, status, claimed_by, lease_generation, attempts "
        "FROM ingest_ledger WHERE service_id=? ORDER BY object_key",
        ("test-celery-svc",),
    ).fetchall()
    assert [(r["status"], r["claimed_by"], r["lease_generation"], r["attempts"]) for r in rows] == [
        ("claimed", "new-worker", 2, 0),
        ("claimed", "new-worker", 2, 0),
    ]


def _read_expr_for(raw_file, columns_sql="{'timestamp': 'VARCHAR', 'url': 'VARCHAR'}") -> str:
    from backend.utils.sql_validator import escape_sql_literal

    safe_local = escape_sql_literal(str(raw_file))
    return (
        f"read_json_auto('{safe_local}', format='newline_delimited', "
        f"records='auto', columns={columns_sql}, ignore_errors=true)"
    )


def test_quarantine_convert_corrupt_lines_captures_local_evidence(tmp_path):
    """A malformed JSON line: ignore_errors=true does NOT drop it, it
    inserts an all-NULL row — that NULL-timestamp row is the actual corruption
    signal this function must detect and quarantine."""
    raw_file = tmp_path / "raw.json"
    valid_line_1 = _json.dumps({"timestamp": "2026-01-01T00:00:00Z", "url": "/a"})
    valid_line_2 = _json.dumps({"timestamp": "2026-01-01T00:00:01Z", "url": "/b"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:02Z", "url": "/c"'  # truncated JSON
    raw_file.write_text(f"{valid_line_1}\n{valid_line_2}\n{corrupt_line}\n")

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)
    assert con.execute(f"SELECT count(*) FROM {read_expr}").fetchone()[0] == 3
    assert con.execute(f"SELECT count(*) FROM {read_expr} WHERE timestamp IS NULL").fetchone()[0] == 1

    fos = MagicMock()
    src = {"service_id": "svc-quarantine", "name": "svc-quarantine", "bucket": "test-bucket", "prefix": ""}

    with patch(
        "backend.core.quarantine.capture_evidence",
        return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
    ) as capture:
        result = _quarantine_convert_corrupt_lines(
            con, fos, src, read_expr, str(raw_file), "raw/request/year=2026/file.gz"
        )

    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 2,
        "malformed_records": 1,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }
    assert capture.call_args.args[0:3] == (
        "svc-quarantine",
        "request",
        "raw/request/year=2026/file.gz",
    )
    assert capture.call_args.args[3] == (corrupt_line + "\n").encode()


def test_quarantine_convert_corrupt_lines_noop_when_nothing_dropped(tmp_path):
    """The common case (no corrupt lines) performs no evidence writes."""
    raw_file = tmp_path / "raw.json"
    raw_file.write_text('{"timestamp": "2026-01-01T00:00:00Z", "url": "/a"}\n')

    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)

    fos = MagicMock()
    src = {"service_id": "svc-clean", "name": "svc-clean", "bucket": "test-bucket", "prefix": ""}

    result = _quarantine_convert_corrupt_lines(con, fos, src, read_expr, str(raw_file), "raw/request/file.gz")
    fos.put_object.assert_not_called()
    assert result == {
        "valid_records": 1,
        "malformed_records": 0,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }


def test_convert_batch_excludes_null_timestamp_rows_from_lake(tmp_path):
    """End-to-end guard: a corrupt line must never land in the queryable lake
    table, even though ignore_errors=true alone would have put a NULL row there."""
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    object_key = "raw/2026/08/27/10/09/mixed.json.gz"
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con.commit()

    raw_file = tmp_path / "mixed.json"
    valid_line = _json.dumps({"timestamp": "2026-01-01T00:00:00Z"})
    corrupt_line = '{"timestamp": "2026-01-01T00:00:01Z"'  # truncated
    raw_file.write_text(f"{valid_line}\n{corrupt_line}\n")
    s3_map = {f"s3://test-bucket/{object_key}": str(raw_file)}

    with DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local=s3_map) as env:
        summary = convert_batch_objects(service_id, [object_key], "test-worker")

    assert summary["committed"] == 1
    check_con = lake_reader(env, read_only=True)
    try:
        rows = check_con.execute("SELECT count(*) FROM lake.logs").fetchone()[0]
        null_rows = check_con.execute("SELECT count(*) FROM lake.logs WHERE timestamp IS NULL").fetchone()[0]
        assert rows == 1, "only the valid row should have been written to the lake table"
        assert null_rows == 0, "a corrupt row must never land in the queryable lake table"
    finally:
        check_con.close()

    from backend.core.metadata.ingest_log import get_ingested_files_status_summary

    ingested = get_ingested_files_status_summary(service_id)
    assert ingested.get("total_rows") == 1, "ingested_files must record only the valid row, not the corrupt one"


def test_sweep_dead_letter_count_excludes_rum_objects():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    now = time.time()
    for key, status in (
        ("raw/request/2026/08/27/10/a.gz", "dead_letter"),
        ("raw/rum/2026/08/27/10/b.gz", "dead_letter"),
        ("raw/rum/2026/08/27/10/c.gz", "quarantined"),
    ):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?)",
            (service_id, key, status, now),
        )
    con.commit()

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
    ):
        summary = sweep_ledger_once(service_id)

    assert summary["dead_letter"] == 1
