"""Consolidated DuckLake admission, attach, commit, manifest, and rollup reader tests."""

from __future__ import annotations

import os
import threading
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from unittest.mock import Mock, patch

import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests._helpers.sources import make_source

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core import duckdb as duckdb_core
from backend.core.duckdb import get_connection
from backend.core.duckdb_pool import _Pool
from backend.core.iceberg import _ducklake as ducklake
from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg._ducklake import (
    DuckLakeAdmissionTimeout,
    _ducklake_attach,
    _reset_for_tests,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
)
from backend.core.iceberg.buffer import _commit_buffer_impl, _ducklake_write_connection, buffer_files
from backend.core.iceberg.manifest import ducklake_table_exists, get_snapshot_calendar, get_table_info
from backend.core.rollups.time_series import build_time_series_bundles

_make_source = partial(make_source, kind="lake", buffer_dir=True)


@pytest.fixture
def reset_admission_state():
    _reset_for_tests()
    yield
    _reset_for_tests()


def _durable_source(tmp_path):
    return {
        "name": "durable-svc",
        "service_id": "durable-svc",
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
        "deployment_mode": "high_scale",
        "bucket": "bucket",
        "prefix": "logs",
    }


def _make_committed_source(tmp_path, name: str) -> dict:
    src = _make_source(tmp_path, name)
    buf = os.path.join(src["_cache_dir_override"], "buffer", "init.parquet")
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array([datetime(2026, 8, 30, 12, 0, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")),
                "ip": pa.array(["10.0.0.1"]),
                "_source_file": pa.array(["s3://b/raw/init.gz"]),
            }
        ),
        buf,
    )
    _commit_buffer_impl(src)
    return src


def _write_buffer(
    source: dict,
    filename: str,
    *,
    ts: datetime,
    source_file: str,
    n: int = 1,
    extra: dict | None = None,
) -> str:
    cols: dict = {
        "timestamp": pa.array([ts + timedelta(seconds=i) for i in range(n)], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array([f"10.0.0.{i}" for i in range(n)]),
        "_source_file": pa.array([source_file] * n),
    }
    for k, v in (extra or {}).items():
        cols[k] = pa.array([v] * n)
    path = os.path.join(source["_cache_dir_override"], "buffer", filename)
    pq.write_table(pa.table(cols), path)
    return path


def _lake_count(source: dict) -> int:
    con = get_connection(source)
    try:
        table = ducklake_table_name(source)
        return int(con.execute(f'SELECT count(*) FROM lake."{table}"').fetchone()[0])
    finally:
        con.close()


class _ExecuteProxy:
    def __init__(self, con, reject_insert):
        self._con = con
        self._reject_insert = reject_insert

    def execute(self, query, *args, **kwargs):
        self._reject_insert(query)
        return self._con.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


def _proxy_commit_connection(monkeypatch, reject_insert):
    original = buffer_mod._ducklake_write_connection

    @contextmanager
    def proxied(source):
        with original(source) as con:
            yield _ExecuteProxy(con, reject_insert)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", proxied)
    return original


# ── Admission & PG Advisory Locks ──
@pytest.mark.usefixtures("reset_admission_state")
def test_local_admission_serializes_same_service_and_isolates_different_services():
    entered, release = threading.Event(), threading.Event()
    order: list[str] = []

    def first() -> None:
        with ducklake_write_admission("admission-local"):
            order.append("first")
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=first)
    worker.start()
    assert entered.wait(timeout=2)

    with pytest.raises(DuckLakeAdmissionTimeout), ducklake_write_admission("admission-local", timeout_s=0.02):
        pass

    started = time.monotonic()
    with ducklake_write_admission("admission-other-service", timeout_s=0.2):
        assert (time.monotonic() - started) < 0.15

    release.set()
    worker.join(timeout=2)
    stats = get_admission_stats()["admission-local"]
    assert order == ["first"] and stats["acquisitions"] == 1 and stats["timeouts"] == 1


@pytest.mark.usefixtures("reset_admission_state")
def test_postgres_admission_fair_lock_and_timeout():
    calls: list[tuple[str, object]] = []

    class Cursor:
        def __init__(self, fail_lock=False):
            self.fail_lock = fail_lock

        def execute(self, sql, params=None):
            calls.append((sql, params))
            if self.fail_lock and "pg_advisory_lock(" in sql:
                raise psycopg.errors.LockNotAvailable("timeout")
            return self

        def fetchone(self):
            return (True,)

    with patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=Cursor()):
        with ducklake_write_admission("admission-postgres", timeout_s=0.2):
            pass

    sqls = [c[0] for c in calls]
    assert not any("pg_try_advisory_lock" in s for s in sqls)
    assert any(s.startswith("SET lock_timeout") for s in sqls)
    acq_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_lock(" in s)
    rel_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_unlock(" in s)
    assert acq_idx < rel_idx and calls[acq_idx][1][0] == calls[rel_idx][1][0]

    calls.clear()
    with patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=Cursor(fail_lock=True)):
        with pytest.raises(DuckLakeAdmissionTimeout), ducklake_write_admission("admission-pg-timeout", timeout_s=0.1):
            pass
    assert any(s == "SET lock_timeout = 0" for s in [c[0] for c in calls])
    assert get_admission_stats()["admission-pg-timeout"]["timeouts"] == 1


# ── Extension Loading, Attach & Durable Serving ──
@pytest.mark.usefixtures("reset_admission_state")
def test_duckdb_connect_extension_options(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/duck'lake/ducklake.duckdb_extension")
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)
    duckdb_core._duckdb_connect("service.duckdb", read_only=False, config={"threads": "2"})
    connect.assert_called_once_with(
        "service.duckdb", read_only=False, config={"threads": "2", "allow_unsigned_extensions": "true"}
    )

    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    connect.reset_mock()
    duckdb_core._duckdb_connect(":memory:")
    connect.assert_called_once_with(":memory:")


@pytest.mark.usefixtures("reset_admission_state")
def test_ducklake_attach_extension_options(monkeypatch, tmp_path):
    statements: list[str] = []

    class FakeConn:
        def execute(self, stmt):
            statements.append(stmt)
            if stmt.startswith("LOAD"):
                raise RuntimeError("load failed")
            return Mock(fetchone=Mock(return_value=None))

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", "postgres:dbname=test")
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/duck'lake/ducklake.duckdb_extension")
    monkeypatch.delenv("DUCKDB_EXTENSION_DIRECTORY", raising=False)
    assert ducklake._ducklake_attach(FakeConn(), {"service_id": "svc"}) is False
    assert statements == ["LOAD '/opt/duck''lake/ducklake.duckdb_extension';"]

    catalog_path = tmp_path / "lake.ducklake"
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", f"ducklake:{catalog_path}")
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(tmp_path / "data"))
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, {"service_id": "test_direct", "name": "test_direct"}, read_only=False)
        con.execute("CREATE TABLE lake.t (id INT)")
        con.execute("INSERT INTO lake.t VALUES (1), (2)")
        assert con.execute("SELECT file_count FROM ducklake_table_info('lake')").fetchone()[0] > 0
    finally:
        con.close()


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
    temp_dir = tmp_path / "duckdb-temp"
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(temp_dir))
    source = _durable_source(tmp_path)

    with (
        patch("backend.core.duckdb._configure_fos"),
        patch("backend.core.iceberg.configure_duckdb_s3"),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=True),
        patch("backend.core.iceberg.update_iceberg_view"),
    ):
        con = db.get_connection(source=source, read_only=True, skip_view_update=False)
    try:
        assert db.db_path_for_source(source) == ":memory:durable-svc"
        assert not (tmp_path / "must-not-open.duckdb").exists()
        assert temp_dir.is_dir()
        assert con.execute("SELECT 42").fetchone() == (42,)
    finally:
        con.close()

    with pytest.raises(RuntimeError, match="only permits read-only"):
        db.get_connection(source=source, read_only=False)

    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    source["deployment_mode"] = "standard"
    assert db.db_path_for_source(source) == str(tmp_path / "must-not-open.duckdb")


# ── Concurrency, Pool & View Reattach Contracts ──
def test_reader_reuses_live_read_write_attach_and_pool_keeps_sibling_attached(tmp_path):
    name = f"rwreuse{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    with _ducklake_write_connection(src) as writer:
        assert writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone() == (
            False,
        )
        reader = get_connection(source=src, read_only=True)
        try:
            assert writer.execute(
                "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
            ).fetchone() == (False,)
            assert _ducklake_attach(reader, src, read_only=True) is True
        finally:
            reader.close()

    pool = _Pool(name, max_size=2)
    first = pool.acquire(src, max_wait=10, skip_view_update=True)
    second = pool.acquire(src, max_wait=10, skip_view_update=True)
    try:
        pool.release(first)
        assert second.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()[0] >= 1
    finally:
        pool.release(second)
        for con in list(pool._idle.queue):
            con.close()

    with closing(get_connection(source=src, read_only=True)) as c2:
        assert c2.execute("SELECT 1").fetchone()[0] == 1


def test_ducklake_detach_lock_and_retry_backoff(monkeypatch):
    monkeypatch.setattr(ducklake, "_ATTACH_CONFLICT_RETRY_SLEEP_S", 0.05)
    monkeypatch.setattr(ducklake, "_ATTACH_CONFLICT_RETRY_ATTEMPTS", 5)

    class FakeConn:
        def __init__(self):
            self.commands: list[str] = []
            self.attempts = 0

        def execute(self, sql: str):
            self.commands.append(sql)
            if sql.startswith("ATTACH 'ducklake:") and " AS lake " in sql:
                self.attempts += 1
                if self.attempts <= 2:
                    raise RuntimeError("Unique file handle conflict: already attached")
            return self

        def fetchone(self):
            return None

    fc = FakeConn()
    assert ducklake._ducklake_attach(fc, {"service_id": "test"}, read_only=False) is True
    assert fc.attempts == 3


# ── Commit, Tombstones, Schema Evolution & Manifest ──
@pytest.mark.parametrize(
    ("source", "subtable", "expected"),
    [
        ({"service_id": "AbC-123"}, None, "logs_abc_123"),
        ({"service_id": "FastlyService123", "name": "Customer"}, None, "logs_fastlyservice123"),
        ({"name": "svc.two"}, "client_vitals", "logs_svc_two__client_vitals"),
        ({"service_id": 'x"; DROP TABLE lake.logs;--'}, None, "logs_x___drop_table_lake_logs"),
    ],
)
def test_ducklake_table_name_is_per_service_and_sanitized(source, subtable, expected):
    assert (ducklake_table_name(source, subtable) if subtable else ducklake_table_name(source)) == expected


def test_commit_tombstone_idempotency_and_schema_evolution(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    p1 = _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)

    res = _commit_buffer_impl(src)
    assert res["files_committed"] == 1 and res["rows_committed"] == 3 and _lake_count(src) == 3
    assert os.path.exists(p1) and buffer_files(src) == []

    # Simulate lost tombstone sidecar -> recommit must deduplicate via DELETE-by-_source_file
    buf_dir = os.path.dirname(p1)
    for f in os.listdir(buf_dir):
        if ".consumed-" in f:
            os.remove(os.path.join(buf_dir, f))
    assert _commit_buffer_impl(src)["files_committed"] == 1
    assert _lake_count(src) == 3

    # Schema evolution: new column in second batch
    _write_buffer(src, "batch_b.parquet", ts=ts, source_file="s3://b/raw/b.gz", n=2, extra={"custom_hdr": "v1"})
    assert _commit_buffer_impl(src)["rows_committed"] == 2
    assert _lake_count(src) == 5

    # Metadata & snapshot calendar
    assert ducklake_table_exists(src) is True
    info = get_table_info(src)
    assert "error" not in info and info["snapshots"] >= 2 and info["data_files"] >= 1
    cal = get_snapshot_calendar(src)
    assert len(cal) >= 1


def test_commit_failure_rolls_back_and_leaves_buffer_untombstoned(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dlfail{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    p = _write_buffer(src, "batch_fail.parquet", ts=ts, source_file="s3://b/raw/f.gz", n=2)

    def _boom(q: str):
        if "INSERT INTO" in q:
            raise RuntimeError("simulated insert crash")

    _proxy_commit_connection(monkeypatch, _boom)
    res = _commit_buffer_impl(src)
    assert res["files_committed"] == 0 and res["rows_committed"] == 0
    assert buffer_files(src) == [p]


# ── Rollup Reader Lake Integration ──
def test_rollup_time_series_reads_from_committed_ducklake(tmp_path):
    src = _make_source(tmp_path, f"dlrollup{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 5, tzinfo=UTC)
    _write_buffer(
        src,
        "rollup_batch.parquet",
        ts=ts,
        source_file="s3://b/raw/r.gz",
        n=4,
        extra={"status": 200, "bytes": 1000, "time_total_us": 5000, "cache_status": "HIT"},
    )
    _commit_buffer_impl(src)
    written = build_time_series_bundles(src["name"], src, ["2026-08-30-12"])
    assert written == 1
