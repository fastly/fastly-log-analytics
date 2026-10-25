"""Integration tests for ingest.py corrupt-row repair, crash recovery, finalizer, and ledger sweeper."""

from __future__ import annotations

import concurrent.futures
import gzip
import io
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import botocore.exceptions
import duckdb
import pytest
from tests._helpers.ledger import DuckLakeLedgerEnv, clear_ledger, lake_reader
from tests._helpers.ledger import attacher as _attacher
from tests._helpers.ledger import clear_ledger as _clear_ledger

import backend.core.duckdb as my_duckdb
from backend.core import iceberg as ice
from backend.core import ingest as ing
from backend.core import metadata as metadata_db
from backend.core.iceberg._ducklake import ducklake_table_name
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
from backend.core.metadata.cron_log import OUTCOME_COUNTER_KEYS, record_ingest_object_outcome, start_cron_run
from backend.core.metadata.ingest_log import get_ingested_files_status_summary
from backend.utils.sql_validator import escape_sql_literal


def _drain(gen) -> list[dict]:
    return list(gen)


class _S3RewritingConn:
    def __init__(self, conn, bucket_prefix: str, file_prefix: str):
        self._conn = conn
        self._bucket_prefix = bucket_prefix
        self._file_prefix = file_prefix

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and self._bucket_prefix in sql:
            sql = sql.replace(self._bucket_prefix, self._file_prefix)
        return self._conn.execute(sql, *args, **kwargs)

    def close(self) -> None:
        pass

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _install_mock_fos(monkeypatch, log_dir, *, keys, get_object_handler=None, valid_size=100):
    class MockPaginator:
        def paginate(self, **_kwargs):
            return [{"Contents": [{"Key": k, "Size": valid_size} for k in keys]}]

    class MockFos:
        def __init__(self):
            self.delete_calls = []
            self.delete_response = {}

        def get_paginator(self, *_args, **_kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):  # noqa: N803
            if get_object_handler is not None:
                resp = get_object_handler(Bucket, Key)
                if resp is not None:
                    return resp
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            self.delete_calls.append(kwargs)
            return self.delete_response

    fos = MockFos()
    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda *_a: fos)
    return fos


@pytest.fixture
def ingest_local_env(monkeypatch, fos_source, in_memory_duckdb, tmp_path):
    log_dir = tmp_path / "mock_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: str(tmp_path / "cache"))
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *_a: None)
    monkeypatch.setattr("time.sleep", lambda *_a: None)
    monkeypatch.setattr("tenacity.nap.sleep", lambda *_a: None)
    monkeypatch.setattr("backend.core.duckdb.INGEST_CHUNK_SIZE", 10)
    bucket_prefix = f"s3://{fos_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    monkeypatch.setattr(
        my_duckdb,
        "get_memory_connection",
        lambda src: _S3RewritingConn(in_memory_duckdb, bucket_prefix, file_prefix),
    )
    return {"log_dir": log_dir, "src": {**fos_source}, "duckdb": in_memory_duckdb}


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


def test_corrupt_row_pipeline_valid_and_repair_and_evidence(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    key_ok = "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    _seed_local(log_dir, key_ok, "2026-05-18T10:00:00Z", n_rows=2)
    _install_mock_fos(monkeypatch, log_dir, keys=[key_ok])
    done = next(e for e in _drain(ingest(source=src)) if e["type"] == "done")
    assert done["rows_inserted"] == 2 and done.get("corrupt_rows", 0) == 0

    # Empty value repair
    key_rep = "raw/2026-05-18/11/2026-05-18T11-00-00.r.gz"
    (log_dir / key_rep).parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(log_dir / key_rep, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T11:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        f.write('{"timestamp": "2026-05-18T11:01:00Z", "status": , "url": "/repair"}\n')
    _install_mock_fos(monkeypatch, log_dir, keys=[key_rep])
    done_rep = next(e for e in _drain(ingest(source=src)) if e["type"] == "done")
    assert done_rep["rows_inserted"] >= 1

    # Unclosed brace evidence capture + source delete failure
    key_bad = "raw/request/2026-05-18/12/2026-05-18T12-00-00.b.gz"
    (log_dir / key_bad).parent.mkdir(parents=True, exist_ok=True)
    malformed = b'{"timestamp": "2026-05-18T12:01:00Z", "status": 200\n'
    with gzip.open(log_dir / key_bad, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T12:00:00Z", "status": 200, "url": "/ok"}) + "\n")
        f.write(malformed.decode())
    mock_fos = _install_mock_fos(monkeypatch, log_dir, keys=[key_bad])
    mock_fos.delete_response = {"Errors": [{"Key": key_bad, "Code": "InternalError"}]}
    captured = []
    monkeypatch.setattr(
        "backend.core.quarantine.capture_evidence",
        lambda sid, stype, okey, payload, **meta: (
            captured.append((stype, okey, payload)),
            {"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 1},
        )[1],
    )
    done_bad = next(e for e in _drain(ingest(source=src, delete_after=True)) if e["type"] == "done")
    assert done_bad["rows_inserted"] >= 1 and done_bad["corrupt_rows"] > 0
    assert captured == [("request", key_bad, malformed)]
    assert done_bad["outcome_counters"]["malformed_records"] == 1
    assert done_bad["outcome_counters"]["quarantine_capture_failures"] == 1
    assert done_bad["outcome_counters"]["source_delete_failures"] == 1


def test_network_failure_during_corrupt_reread_falls_back_to_gzip_scan(
    fos_source, in_memory_duckdb, monkeypatch, tmp_path
):
    log_dir = tmp_path / "mock_logs"
    clean_key = "raw/request/2026-05-18/13/2026-05-18T13-00-00.v.gz"
    corrupt_key = "raw/request/2026-05-18/13/2026-05-18T13-00-00.r.gz"
    _seed_local(log_dir, clean_key, "2026-05-18T13:00:00Z")
    (log_dir / corrupt_key).parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(log_dir / corrupt_key, "wt") as f:
        f.write(json.dumps({"timestamp": "2026-05-18T13:01:00Z", "status": 200, "url": "/clean"}) + "\n")
        f.write(json.dumps({"status": 200, "url": "/no-timestamp"}) + "\n")

    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda src: in_memory_duckdb)
    monkeypatch.setattr(my_duckdb, "INGEST_CHUNK_SIZE", 50)
    monkeypatch.setattr("time.sleep", lambda *a: None)
    _install_mock_fos(monkeypatch, log_dir, keys=[clean_key, corrupt_key])

    real_execute = my_duckdb._execute_query_with_retry
    monkeypatch.setattr(
        "backend.core.ingest._execute_query_with_retry",
        lambda con, q, **kw: (
            (_ for _ in ()).throw(Exception("IO Error"))
            if "read_csv" in q and "column0" in q
            else real_execute(con, q, **kw)
        ),
    )
    done = next(e for e in _drain(ingest(source={**fos_source})) if e["type"] == "done")
    assert done["rows_inserted"] == 2 and done["outcome_counters"]["malformed_records"] == 1
    ingested = metadata_db.get_ingested_filenames(fos_source["name"])
    assert (
        f"s3://{fos_source['bucket']}/{clean_key}" in ingested
        and f"s3://{fos_source['bucket']}/{corrupt_key}" in ingested
    )


def test_ingest_retains_corrupt_gzip_evidence_and_outcomes(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    valid_key = "raw/2026-05-18/10/2026-05-18T10-00-00.v.gz"
    corrupt_key = "raw/2026-05-18/10/2026-05-18T10-05-00.c.gz"
    _seed_local(log_dir, valid_key, "2026-05-18T10:00:00Z")
    (log_dir / corrupt_key).write_bytes(b"This is definitely not a gzip file")
    _install_mock_fos(monkeypatch, log_dir, keys=[valid_key, corrupt_key])
    captured = []
    monkeypatch.setattr(
        "backend.core.quarantine.capture_evidence",
        lambda sid, stype, okey, payload, **meta: (
            captured.append((sid, stype, okey, payload)),
            {"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
        )[1],
    )
    done = next(e for e in _drain(ingest(source=src)) if e["type"] == "done")
    assert done["outcome_counters"]["corrupt_containers"] == 1 and done["rows_inserted"] == 1
    assert captured[0][1:4] == ("request", corrupt_key, b"This is definitely not a gzip file")
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert len(ingested) == 1 and any(valid_key in f for f in ingested)


@pytest.fixture
def ingest_env(s3_mock, fos_source, tmp_path, monkeypatch):
    fos_source["service_id"] = f"cr{uuid.uuid4().hex[:10]}"
    fos_source["duckdb_path"] = str(tmp_path / "e2e.duckdb")
    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})

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
    return {"s3": s3_mock, "src": fos_source, "cache": cache_path, "bucket": fos_source["bucket"]}


def _one_log_file(s3, bucket: str, n_rows: int = 4) -> tuple[str, int]:
    base = datetime.now(UTC) - timedelta(hours=2)
    key = "raw/request/year=2026/month=05/day=20/hour=10/minute=00/2026-05-20T10-00-00.svc.gz"
    rows = [
        {
            "timestamp": (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "ip": f"10.0.0.{i}",
            "status": 200,
            "url": f"/path/{i}",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 1000 + i,
        }
        for i in range(n_rows)
    ]
    body = io.BytesIO()
    with gzip.GzipFile(fileobj=body, mode="wb") as gz:
        gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue())
    return key, n_rows


def _lake_count(src: dict) -> int:
    con = my_duckdb.get_connection(src)
    try:
        return int(con.execute(f'SELECT count(*) FROM lake."{ducklake_table_name(src)}"').fetchone()[0])
    finally:
        con.close()


class _RaiseOnce:
    def __init__(self, real, exc: Exception):
        self._real, self._exc, self.calls = real, exc, 0

    def __call__(self, *a, **kw):
        self.calls += 1
        if self.calls == 1:
            raise self._exc
        return self._real(*a, **kw)


def test_crash_before_write_to_buffer_recovers_clean(ingest_env, monkeypatch):
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])
    monkeypatch.setattr(
        "backend.core.iceberg.write_to_buffer", _RaiseOnce(ice.write_to_buffer, RuntimeError("crash: write_to_buffer"))
    )
    with pytest.raises(RuntimeError, match="write_to_buffer"):
        _drain(ing.ingest(source=src, delete_after=True))
    assert (
        metadata_db.get_ingested_filenames(sid) == set()
        and len(metadata_db.list_in_flight(sid)) == 1
        and ice.buffer_files(src) == []
    )

    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")
    assert done["rows_inserted"] == n and metadata_db.list_in_flight(sid) == []
    assert metadata_db.get_ingested_files_status_summary(sid)["total_rows"] == n


def test_crash_before_insert_ingested_files_promotes_buffer(ingest_env, monkeypatch):
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])
    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash: before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)
    with pytest.raises(RuntimeError, match="before insert"):
        _drain(ing.ingest(source=src, delete_after=True))
    assert len(ice.buffer_files(src)) == 1 and metadata_db.get_ingested_filenames(sid) == set()

    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    done = next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")
    assert done["rows_inserted"] == 0 and metadata_db.list_in_flight(sid) == []
    assert ice.commit_buffer(src)["rows_committed"] == n and _lake_count(src) == n


def test_crash_before_clear_in_flight_and_sha256_dedupe(ingest_env, monkeypatch):
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    key, n = _one_log_file(s3, ingest_env["bucket"])
    raiser = _RaiseOnce(metadata_db.clear_in_flight, RuntimeError("crash: before clear"))
    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser)
    with pytest.raises(RuntimeError, match="before clear"):
        _drain(ing.ingest(source=src, delete_after=True))
    assert len(metadata_db.list_in_flight(sid)) == 1

    monkeypatch.setattr("backend.core.metadata.clear_in_flight", raiser._real)
    next(e for e in ing.ingest(source=src, delete_after=True) if e["type"] == "done")
    assert (
        metadata_db.list_in_flight(sid) == [] and metadata_db.get_ingested_files_status_summary(sid)["total_rows"] == n
    )
    assert ice.commit_buffer(src)["rows_committed"] == n and _lake_count(src) == n


def test_sha256_name_prevents_duplicate_buffer_if_recovery_skipped(ingest_env, monkeypatch):
    s3, src, sid = ingest_env["s3"], ingest_env["src"], ingest_env["src"]["name"]
    _, n = _one_log_file(s3, ingest_env["bucket"])
    raiser = _RaiseOnce(metadata_db.insert_ingested_files, RuntimeError("crash before insert"))
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser)
    with pytest.raises(RuntimeError):
        _drain(ing.ingest(source=src, delete_after=False))
    crashed_buf_name = os.path.basename(ice.buffer_files(src)[0])

    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", raiser._real)
    monkeypatch.setattr(
        "backend.core.ingest._recover_in_flight", lambda _src: {"promoted": 0, "dropped": 0, "rows_recovered": 0}
    )
    metadata_db.clear_in_flight(sid, crashed_buf_name)

    done = next(e for e in ing.ingest(source=src, delete_after=False) if e["type"] == "done")
    assert done["rows_inserted"] == n and len(ice.buffer_files(src)) == 1
    assert ice.commit_buffer(src)["rows_committed"] == n and _lake_count(src) == n


def test_isolation_total_failure(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    for key in ("raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz", "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz"):
        p = log_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"not a gzip at all")
    _install_mock_fos(
        monkeypatch,
        log_dir,
        keys=["raw/2026-06-01/10/2026-06-01T10-00-00.bad1.gz", "raw/2026-06-01/10/2026-06-01T10-05-00.bad2.gz"],
    )
    done = next(e for e in _drain(ingest(source=src)) if e["type"] == "done")
    assert (
        done["rows_inserted"] == 0
        and ice.buffer_files(src) == []
        and metadata_db.get_ingested_filenames(src["name"]) == set()
    )


def test_isolation_partial_failures(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    good_key = "raw/2026-06-02/09/2026-06-02T09-00-00.good.gz"
    gone_key = "raw/2026-06-02/09/2026-06-02T09-05-00.gone.gz"
    bad_key = "raw/2026-06-02/09/2026-06-02T09-10-00.bad.gz"
    _seed_local(log_dir, good_key, "2026-06-02T09:00:00Z")
    (log_dir / bad_key).parent.mkdir(parents=True, exist_ok=True)
    (log_dir / bad_key).write_bytes(b"not a gzip file\x00\x01\x02")

    def _selective_get(Bucket, Key):
        if Key == gone_key:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "key vanished"}}, "GetObject"
            )
        return None

    _install_mock_fos(monkeypatch, log_dir, keys=[good_key, gone_key, bad_key], get_object_handler=_selective_get)
    done2 = next(e for e in _drain(ingest(source=src)) if e["type"] == "done")
    assert done2["rows_inserted"] == 1
    ingested = metadata_db.get_ingested_filenames(src["name"])
    assert (
        any(good_key in n for n in ingested)
        and not any(gone_key in n for n in ingested)
        and not any(bad_key in n for n in ingested)
    )


def test_recover_in_flight_inline_delete_fallback_and_delete_failure(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    buf_dir = ice._buffer_dir(src)  # type: ignore[attr-defined]
    os.makedirs(buf_dir, exist_ok=True)
    orphan_buffer = "batch_orphan_deadbeef.parquet"
    open(os.path.join(buf_dir, orphan_buffer), "wb").write(b"placeholder")
    metadata_db.record_in_flight(
        src["name"], orphan_buffer, [("s3://test-bucket/raw/2026-05-30/10/2026-05-30T10-00-00.orphan.gz", 42, 100)]
    )
    _install_mock_fos(monkeypatch, log_dir, keys=[])
    _drain(ingest(source=src))
    assert metadata_db.list_in_flight(src["name"]) == [] and any(
        "orphan.gz" in n for n in metadata_db.get_ingested_filenames(src["name"])
    )

    # Executor submit failure falls back to inline delete
    key = "raw/2026-06-01/12/2026-06-01T12-00-00.ok.gz"
    _seed_local(log_dir, key, "2026-06-01T12:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])
    original_submit = concurrent.futures.ThreadPoolExecutor.submit
    monkeypatch.setattr(
        concurrent.futures.ThreadPoolExecutor,
        "submit",
        lambda self, *a, **kw: (
            (_ for _ in ()).throw(RuntimeError("shutdown"))
            if getattr(self, "_thread_name_prefix", "") == "ingest_delete"
            else original_submit(self, *a, **kw)
        ),
    )
    inline_delete_calls: list[list[str]] = []
    original_delete = ing._delete_objects_robust_with_failures
    monkeypatch.setattr(
        ing,
        "_delete_objects_robust_with_failures",
        lambda c, b, k: (inline_delete_calls.append(list(k)), original_delete(c, b, k))[1],
    )
    done = next(e for e in _drain(ingest(source=src, delete_after=True)) if e["type"] == "done")
    assert done["rows_inserted"] == 1 and any(key in batch for batch in inline_delete_calls)

    # Partial source delete failure
    key_fail = "raw/2026-06-01/12/2026-06-01T12-00-00.delete-fails.gz"
    _seed_local(log_dir, key_fail, "2026-06-01T12:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key_fail])
    monkeypatch.setattr(ing, "_delete_objects_robust_with_failures", lambda _c, _b, keys: (0, set(keys)))
    done_fail = next(e for e in _drain(ingest(source=src, delete_after=True)) if e["type"] == "done")
    assert (
        done_fail["outcome_counters"]["source_delete_failures"] == 1
        and done_fail["outcome_counters"]["objects_failed"] == 1
    )


def test_zero_row_marker_and_strand_reclaim_and_pre_epoch_guard(ingest_local_env, monkeypatch):
    log_dir, src = ingest_local_env["log_dir"], ingest_local_env["src"]
    bucket = src["bucket"]
    key = "raw/2026-06-01/10/2026-06-01T10-00-00.svc.gz"
    _seed_local(log_dir, key, "2026-06-01T10:00:30Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[key])
    done = next(
        e
        for e in _drain(
            ingest(source=src, delete_after=False, start_time="2026-06-01T09:00:00Z", end_time="2026-06-01T10:00:00Z")
        )
        if e["type"] == "done"
    )
    assert done["rows_inserted"] == 0
    marker = next(f for f in metadata_db.list_ingested_files(src["name"]) if key in f["file_name"])
    assert marker["row_count"] == 0
    assert (
        metadata_db.get_reclaimable_strand_filenames(src["name"], {marker["file_name"]}, "2000-01-01 00:00:00") == set()
    )

    # Pre-epoch strand not reclaimed
    strand_key = "raw/2026-06-01/10/2026-06-01T10-00-00.strand.gz"
    metadata_db.insert_ingested_files(src["name"], [(f"s3://{bucket}/{strand_key}", 100, 5000)])
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key])
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2999-01-01 00:00:00")
    done_pre = next(e for e in _drain(ingest(source=src, delete_after=True)) if e["type"] == "done")
    assert done_pre["deleted_files"] == 0

    # Post-epoch strand reclaimed + folded with fresh file delete
    new_key = "raw/2026-06-01/11/2026-06-01T11-00-00.fresh.gz"
    _seed_local(log_dir, new_key, "2026-06-01T11:00:00Z")
    _install_mock_fos(monkeypatch, log_dir, keys=[strand_key, new_key])
    monkeypatch.setattr("backend.core.ingest._RECONCILE_LEDGER_EPOCH", "2000-01-01 00:00:00")
    done_post = next(e for e in _drain(ingest(source=src, delete_after=True)) if e["type"] == "done")
    assert done_post["new_files"] == 1 and done_post["deleted_files"] == 2 and "reclaim" in done_post["message"].lower()


SERVICE_ID = "test-finalize-raw"
BUCKET = "test-bucket"
SRC = {"service_id": SERVICE_ID, "name": SERVICE_ID, "bucket": BUCKET, "prefix": ""}
AGED = RAW_DELETE_GRACE_S + 60


def _seed_committed(con, keys: list[str], *, age_s: float, raw_deleted_at: float | None = None) -> None:
    now = time.time()
    cur = con.cursor()
    for key in keys:
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at, published_at, raw_deleted_at) "
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
    return (
        {"service_id": SERVICE_ID}
        if delete_after is None
        else {"service_id": SERVICE_ID, "provisioning": {"cron_sync": {"delete_after": delete_after}}}
    )


def _run(fos, cfg: dict | None, src: dict | None = SRC, **kwargs) -> dict:
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.ingest._get_fos_client", return_value=fos),
    ):
        return finalize_committed_raw(SERVICE_ID, **kwargs)


def test_finalize_deletes_aged_and_respects_grace_and_idempotency():
    con, _ = clear_ledger(SERVICE_ID)
    fresh, aged = "raw/2026/08/27/12/02/fresh.json.gz", "raw/2026/08/27/12/03/aged.json.gz"
    _seed_committed(con, [fresh], age_s=RAW_DELETE_GRACE_S - 60)
    _seed_committed(con, [aged], age_s=AGED)
    fos = MagicMock()
    fos.delete_objects.return_value = {}
    assert _run(fos, _cfg(True)) == {"deleted": 1, "eligible": 1, "delete_after": True}
    assert _stamps(con)[fresh] is None and _stamps(con)[aged] is not None
    # Idempotent second run
    assert _run(fos, _cfg(True)) == {"deleted": 0, "eligible": 0, "delete_after": True}
    assert fos.delete_objects.call_count == 1


@pytest.mark.parametrize(
    "cfg,src,expected_del,expected_da",
    [
        (_cfg(False), SRC, 0, False),
        (
            {
                "service_id": SERVICE_ID,
                "provisioning": {"cron_sync": {"delete_after": True, "high_scale_shared_source": True}},
            },
            SRC,
            0,
            False,
        ),
        ({"service_id": SERVICE_ID, "provisioning": {"cron_sync": {}}}, SRC, 1, True),
        (None, SRC, 1, True),
        (_cfg(True), None, 0, True),
    ],
)
def test_finalize_config_and_source_modes(cfg, src, expected_del, expected_da):
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/2026/08/27/12/04/mode.json.gz"
    _seed_committed(con, [key], age_s=AGED)
    fos = MagicMock()
    fos.delete_objects.return_value = {}
    assert _run(fos, cfg, src=src) == {"deleted": expected_del, "eligible": 1, "delete_after": expected_da}


def test_finalize_ignores_uncommitted_or_unpublished_and_honors_batch_size():
    con, cur = clear_ledger(SERVICE_ID)
    now = time.time()
    for status in ("discovered", "claimed", "quarantined", "dead_letter"):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at) VALUES (?, ?, ?, ?, ?)",
            (SERVICE_ID, f"raw/{status}.gz", status, now - AGED, now - AGED),
        )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at, committed_at, published_at) VALUES (?, 'raw/unpub.gz', 'committed', ?, ?, NULL)",
        (SERVICE_ID, now - AGED, now - AGED),
    )
    con.commit()
    fos = MagicMock()
    assert _run(fos, _cfg(True)) == {"deleted": 0, "eligible": 0, "delete_after": True}

    con, _ = clear_ledger(SERVICE_ID)
    _seed_committed(con, [f"raw/{i}.gz" for i in range(5)], age_s=AGED)
    fos.delete_objects.return_value = {}
    assert _run(fos, _cfg(True), batch_size=2) == {"deleted": 2, "eligible": 2, "delete_after": True}


def test_finalize_per_key_failures_transport_error_and_late_attribution():
    con, _ = clear_ledger(SERVICE_ID)
    ok_key, bad_key = "raw/ok.json.gz", "raw/denied.json.gz"
    _seed_committed(con, [ok_key, bad_key], age_s=AGED)
    fos = MagicMock()
    fos.delete_objects.return_value = {"Errors": [{"Key": bad_key, "Code": "AccessDenied"}]}
    assert _run(fos, _cfg(True)) == {"deleted": 1, "eligible": 2, "delete_after": True}
    assert _stamps(con)[ok_key] is not None and _stamps(con)[bad_key] is None
    fos.delete_objects.return_value = {}
    assert _run(fos, _cfg(True)) == {"deleted": 1, "eligible": 1, "delete_after": True}

    # Transport error holds claim lease
    con, _ = clear_ledger(SERVICE_ID)
    _seed_committed(con, ["raw/x.gz", "raw/y.gz"], age_s=AGED)
    fos2 = MagicMock()
    fos2.delete_objects.side_effect = Exception("connection reset")
    assert _run(fos2, _cfg(True)) == {"deleted": 0, "eligible": 2, "delete_after": True}
    assert _run(fos2, _cfg(True)) == {"deleted": 0, "eligible": 0, "delete_after": True}

    # Late delete failure attribution
    con, _ = clear_ledger(SERVICE_ID)
    key = "raw/origin.json.gz"
    run_id = start_cron_run(SERVICE_ID, "log_discovery")
    _seed_committed(con, [key], age_s=AGED)
    initial = dict.fromkeys(OUTCOME_COUNTER_KEYS, 0)
    initial.update(valid_records=8, objects_processed=1, objects_successful=1)
    con.execute(
        "UPDATE ingest_ledger SET originating_task='log_discovery', originating_run_id=? WHERE service_id=? AND object_key=?",
        (run_id, SERVICE_ID, key),
    )
    con.commit()
    assert record_ingest_object_outcome(SERVICE_ID, key, initial)
    fos3 = MagicMock()
    fos3.delete_objects.return_value = {"Errors": [{"Key": key, "Code": "AccessDenied"}]}
    assert _run(fos3, _cfg(True)) == {"deleted": 0, "eligible": 1, "delete_after": True}
    row = con.execute("SELECT status, rows_ingested, outcome_counters FROM cron_runs WHERE id=?", (run_id,)).fetchone()
    counters = json.loads(row["outcome_counters"])
    assert row["status"] == "error" and counters["source_delete_failures"] == 1 and counters["objects_failed"] == 1


def test_merge_lake_files_compaction_and_error_handling(tmp_path):
    attach = _attacher(str(tmp_path / "cat.ducklake"), str(tmp_path / "lakedata"))
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
    reader.close()

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        with pytest.raises(RuntimeError, match="no source registered"):
            merge_lake_files(SERVICE_ID)

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=False),
        patch("backend.core.ingest._configure_fos"),
    ):
        with pytest.raises(RuntimeError, match="attach failed"):
            merge_lake_files(SERVICE_ID)

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
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=SRC),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=True),
        patch("backend.core.ingest._configure_fos"),
        patch("duckdb.connect", return_value=connection),
        patch("backend.core.ingest._mark_ledger_published") as mark_published,
    ):
        merge_lake_files(SERVICE_ID)
    assert [c for c in connection.calls if c.startswith("CALL ducklake_")] == [
        "CALL ducklake_flush_inlined_data('lake')",
        "CALL ducklake_merge_adjacent_files('lake')",
    ]
    mark_published.assert_called_once_with(SERVICE_ID)


def _empty_list_fos(*args, **kwargs):
    yield {"type": "status", "message": "Discovering"}
    return {"new_files": [], "file_sizes": {}, "skipped_already": 0, "stranded_already": []}


def test_sweeper_reclaim_redispatch_rum_exclusion_and_discovery():
    service_id = "test-celery-svc"
    con, cur = _clear_ledger(service_id)
    stale_claim = time.time() - LEDGER_RECLAIM_AFTER_S - 60
    old_disc = time.time() - 3600
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, 'raw/reclaim.gz', 'claimed', 'dead-1', ?, ?)",
        (service_id, stale_claim, stale_claim - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, discovered_at) VALUES (?, 'raw/rum/beacons.gz', 'claimed', 'dead-1', ?, ?)",
        (service_id, stale_claim, stale_claim - 30),
    )
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, 'raw/lost.gz', 'discovered', ?)",
        (service_id, old_disc),
    )
    con.commit()

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
        patch("backend.core.ingest.convert_batch_files.delay") as mock_delay,
    ):
        summary = sweep_ledger_once(service_id)

    dispatched = [k for call in mock_delay.call_args_list for k in call.args[1]]
    assert "raw/reclaim.gz" in dispatched and "raw/lost.gz" in dispatched and "raw/rum/beacons.gz" not in dispatched
    assert summary["reclaimed"] == 1 and summary["redispatched"] == 2

    # Unseen file discovery
    con, cur = _clear_ledger(service_id)
    object_key = "raw/2026/08/27/10/06/missed_logs.json.gz"

    def mock_list(*args, **kwargs):
        yield {"type": "status", "message": "Discovering"}
        return {
            "new_files": [f"s3://test-bucket/{object_key}"],
            "file_sizes": {f"s3://test-bucket/{object_key}": 1234},
            "skipped_already": 0,
            "stranded_already": [],
        }

    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}),
        patch("backend.core.ingest.list_fos_files", side_effect=mock_list),
        patch("backend.core.ingest.convert_batch_files.delay") as mock_delay2,
    ):
        summary2 = sweep_ledger_once(service_id, run_id=79)
        mock_delay2.assert_called_with(service_id, [object_key])
    assert summary2["discovered"] == 1
    row = cur.execute(
        "SELECT status, size_bytes, originating_task, originating_run_id FROM ingest_ledger WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    ).fetchone()
    assert row["status"] == "discovered" and row["size_bytes"] == 1234 and row["originating_run_id"] == 79


def test_convert_failure_retry_schedule_fencing_and_dead_letter(tmp_path):
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
    statuses = []
    with (
        DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local={f"s3://test-bucket/{object_key}": str(raw_file)}),
        patch("backend.core.ingest.get_ingest_columns_sql", side_effect=RuntimeError("poison file")),
    ):
        for _ in range(LEDGER_MAX_ATTEMPTS):
            convert_batch_objects(service_id, [object_key], "test-worker")
            statuses.append(
                con.execute(
                    "SELECT status FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
                ).fetchone()["status"]
            )
    assert statuses[:-1] == ["discovered"] * (LEDGER_MAX_ATTEMPTS - 1) and statuses[-1] == "quarantined"

    # Retry schedule & due check
    con, cur = _clear_ledger(service_id)
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, lease_generation) VALUES (?, 'raw/retry.gz', 'claimed', 1)",
        (service_id,),
    )
    con.commit()
    before = time.time()
    with patch("backend.core.ingest.random.uniform", return_value=0):
        assert _ledger_record_failure(con, service_id, "raw/retry.gz", "temporary outage", 1) == "discovered"
    row = con.execute(
        "SELECT attempts, error_kind, next_attempt_at FROM ingest_ledger WHERE service_id=? AND object_key='raw/retry.gz'",
        (service_id,),
    ).fetchone()
    assert (
        row["attempts"] == 1
        and row["error_kind"] == "transient"
        and row["next_attempt_at"] >= before + LEDGER_RETRY_BASE_S
    )

    # Lease generation fencing & dead_letter count excluding RUM
    con, cur = _clear_ledger(service_id)
    now = time.time()
    for okey in ("raw/request/log.gz", "raw/rum/beacon.gz"):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, claimed_by, claimed_at, lease_generation, discovered_at) VALUES (?, ?, 'claimed', 'new-worker', ?, 2, ?)",
            (service_id, okey, now, now),
        )
        assert _ledger_record_failure(con, service_id, okey, "stale error", 1) == "stale"
    for k, st in (
        ("raw/request/a.gz", "dead_letter"),
        ("raw/rum/b.gz", "dead_letter"),
        ("raw/rum/c.gz", "quarantined"),
    ):
        cur.execute(
            "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, ?, ?)",
            (service_id, k, st, now),
        )
    con.commit()
    with (
        patch("backend.config.load_config", return_value={"service_id": service_id}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test", "bucket": "test-bucket"}),
        patch("backend.core.ingest.list_fos_files", side_effect=_empty_list_fos),
        patch("backend.celery_status.celery_queue_depths", return_value=({"q.ingest": 0}, True)),
    ):
        assert sweep_ledger_once(service_id)["dead_letter"] == 1


def _read_expr_for(raw_file, columns_sql="{'timestamp': 'VARCHAR', 'url': 'VARCHAR'}") -> str:
    safe_local = escape_sql_literal(str(raw_file))
    return f"read_json_auto('{safe_local}', format='newline_delimited', records='auto', columns={columns_sql}, ignore_errors=true)"


def test_quarantine_convert_corrupt_lines_and_lake_exclusion(tmp_path):
    raw_file = tmp_path / "raw.json"
    corrupt_line = '{"timestamp": "2026-01-01T00:00:02Z", "url": "/c"'
    raw_file.write_text(f"{json.dumps({'timestamp': '2026-01-01T00:00:00Z', 'url': '/a'})}\n{corrupt_line}\n")
    con = duckdb.connect()
    read_expr = _read_expr_for(raw_file)
    fos = MagicMock()
    src = {"service_id": "svc-quarantine", "name": "svc-quarantine", "bucket": "test-bucket", "prefix": ""}
    with patch(
        "backend.core.quarantine.capture_evidence",
        return_value={"id": 1, "cap_evictions": 0, "quarantine_capture_failures": 0},
    ) as capture:
        res = _quarantine_convert_corrupt_lines(
            con, fos, src, read_expr, str(raw_file), "raw/request/year=2026/file.gz"
        )
    assert res == {"valid_records": 1, "malformed_records": 1, "cap_evictions": 0, "quarantine_capture_failures": 0}
    assert capture.call_args.args[3] == (corrupt_line + "\n").encode()

    # Clean file noop
    clean_file = tmp_path / "clean.json"
    clean_file.write_text('{"timestamp": "2026-01-01T00:00:00Z", "url": "/a"}\n')
    assert _quarantine_convert_corrupt_lines(
        con, fos, src, _read_expr_for(clean_file), str(clean_file), "raw/request/file.gz"
    ) == {
        "valid_records": 1,
        "malformed_records": 0,
        "cap_evictions": 0,
        "quarantine_capture_failures": 0,
    }

    # End-to-end convert_batch_objects excludes NULL timestamp rows from lake
    service_id = "test-celery-svc"
    con_l, cur_l = _clear_ledger(service_id)
    object_key = "raw/2026/08/27/10/09/mixed.json.gz"
    cur_l.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status, discovered_at) VALUES (?, ?, 'discovered', ?)",
        (service_id, object_key, time.time()),
    )
    con_l.commit()
    with DuckLakeLedgerEnv(service_id, tmp_path, s3_to_local={f"s3://test-bucket/{object_key}": str(raw_file)}) as env:
        assert convert_batch_objects(service_id, [object_key], "test-worker")["committed"] == 1
    check_con = lake_reader(env, read_only=True)
    try:
        assert check_con.execute("SELECT count(*) FROM lake.logs").fetchone()[0] == 1
        assert check_con.execute("SELECT count(*) FROM lake.logs WHERE timestamp IS NULL").fetchone()[0] == 0
    finally:
        check_con.close()
    assert get_ingested_files_status_summary(service_id).get("total_rows") == 1
