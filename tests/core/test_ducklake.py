"""Per-service DuckLake writer admission tests."""

from __future__ import annotations

import glob
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from importlib import import_module
from unittest.mock import MagicMock, Mock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core import duckdb as duckdb_core
from backend.core.duckdb import _safe_table_name, get_connection
from backend.core.iceberg import _ducklake as ducklake
from backend.core.iceberg import buffer as buffer_mod
from backend.core.iceberg import manifest as manifest_mod
from backend.core.iceberg import view
from backend.core.iceberg import view as view_mod
from backend.core.iceberg._ducklake import (
    DuckLakeAdmissionTimeout,
    _ducklake_attach,
    _reset_for_tests,
    ducklake_table_name,
    ducklake_write_admission,
    get_admission_stats,
)
from backend.core.iceberg.buffer import _commit_buffer_impl, buffer_files
from backend.core.iceberg.manifest import ducklake_table_exists, get_snapshot_calendar, get_table_info
from backend.core.rollups import _common as rollup_common
from backend.core.rollups.time_series import build_time_series_bundles
from tests._helpers.sources import make_source


# ── Merged from tests/core/test_ducklake.py ──
@pytest.fixture
def reset_admission_state():
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.mark.usefixtures("reset_admission_state")
def test_local_admission_serializes_same_service():
    entered = threading.Event()
    release = threading.Event()
    order: list[str] = []

    def first() -> None:
        with ducklake_write_admission("admission-local"):
            order.append("first")
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=first)
    worker.start()
    assert entered.wait(timeout=2)

    with pytest.raises(DuckLakeAdmissionTimeout):
        with ducklake_write_admission("admission-local", timeout_s=0.02):
            pass

    release.set()
    worker.join(timeout=2)
    assert order == ["first"]
    stats = get_admission_stats()["admission-local"]
    assert stats["acquisitions"] == 1
    assert stats["timeouts"] == 1
    assert stats["hold_ms_total"] >= 0


@pytest.mark.usefixtures("reset_admission_state")
def test_different_services_do_not_share_local_admission():
    entered = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with ducklake_write_admission("admission-service-a"):
            entered.set()
            release.wait(timeout=2)

    worker = threading.Thread(target=holder)
    worker.start()
    assert entered.wait(timeout=2)

    started = time.monotonic()
    with ducklake_write_admission("admission-service-b", timeout_s=0.2):
        elapsed = time.monotonic() - started

    release.set()
    worker.join(timeout=2)
    assert elapsed < 0.15


@pytest.mark.usefixtures("reset_admission_state")
def test_postgres_admission_uses_fair_blocking_lock():
    # Fairness: block in PostgreSQL's FIFO lock-wait queue (pg_advisory_lock),
    # bounded by lock_timeout, instead of busy-polling pg_try_advisory_lock
    # (which has no queue and starves whichever writer keeps losing the race).
    calls: list[tuple[str, object]] = []

    class Cursor:
        def execute(self, sql, params=None):
            calls.append((sql, params))
            return self

        def fetchone(self):
            return (True,)

    fake_con = Cursor()
    with (
        patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=fake_con),
    ):
        with ducklake_write_admission("admission-postgres", timeout_s=0.2):
            pass

    sqls = [c[0] for c in calls]
    assert not any("pg_try_advisory_lock" in s for s in sqls)
    # lock_timeout is armed before the blocking acquire and cleared afterward.
    assert any(s.startswith("SET lock_timeout") for s in sqls)
    acquire_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_lock(" in s)
    unlock_idx = next(i for i, s in enumerate(sqls) if "pg_advisory_unlock(" in s)
    assert acquire_idx < unlock_idx
    acquire_key = calls[acquire_idx][1][0]
    unlock_key = calls[unlock_idx][1][0]
    assert acquire_key == unlock_key


@pytest.mark.usefixtures("reset_admission_state")
def test_postgres_admission_timeout_raises_on_lock_not_available():
    import psycopg

    calls: list[str] = []

    class Cursor:
        def execute(self, sql, params=None):
            calls.append(sql)
            if "pg_advisory_lock(" in sql:
                raise psycopg.errors.LockNotAvailable("canceling statement due to lock timeout")
            return self

        def fetchone(self):
            return (True,)

    fake_con = Cursor()
    with (
        patch("backend.core.metadata.pg_connection.get_pg_thread_connection", return_value=fake_con),
    ):
        with pytest.raises(DuckLakeAdmissionTimeout):
            with ducklake_write_admission("admission-pg-timeout", timeout_s=0.2):
                pass

    # lock_timeout is reset even when the acquire times out, and no unlock is
    # issued for a lock that was never held.
    assert any(s == "SET lock_timeout = 0" for s in calls)
    assert not any("pg_advisory_unlock(" in s for s in calls)
    assert get_admission_stats()["admission-pg-timeout"]["timeouts"] == 1


@pytest.mark.usefixtures("reset_admission_state")
def test_admission_stats_capture_wait_and_hold_time():
    with ducklake_write_admission("admission-stats"):
        time.sleep(0.005)

    stats = get_admission_stats()["admission-stats"]
    assert stats["acquisitions"] == 1
    assert stats["timeouts"] == 0
    assert stats["wait_ms_total"] >= 0
    assert stats["hold_ms_total"] >= 5


@pytest.mark.usefixtures("reset_admission_state")
def test_duckdb_connect_allows_bundled_unsigned_extension(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    result = duckdb_core._duckdb_connect(":memory:")

    assert result is connect.return_value
    connect.assert_called_once_with(":memory:", config={"allow_unsigned_extensions": "true"})


@pytest.mark.usefixtures("reset_admission_state")
def test_duckdb_connect_keeps_default_config_without_bundled_extension(monkeypatch):
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    duckdb_core._duckdb_connect(":memory:")

    connect.assert_called_once_with(":memory:")


@pytest.mark.usefixtures("reset_admission_state")
def test_duckdb_connect_preserves_other_connection_settings(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)
    settings = {"threads": "2"}

    duckdb_core._duckdb_connect("service.duckdb", read_only=False, config=settings)

    connect.assert_called_once_with(
        "service.duckdb", read_only=False, config={"threads": "2", "allow_unsigned_extensions": "true"}
    )
    assert settings == {"threads": "2"}


@pytest.mark.usefixtures("reset_admission_state")
def test_memory_connection_uses_bundled_extension_config(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    connect = Mock(return_value=Mock())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    duckdb_core.get_memory_connection()

    connect.assert_called_once_with(":memory:", config={"allow_unsigned_extensions": "true"})


@pytest.mark.usefixtures("reset_admission_state")
def test_bundled_ducklake_load_failure_does_not_install_official_extension(monkeypatch, tmp_path):
    extension = tmp_path / "ducklake.duckdb_extension"
    extension.touch()
    statements = []

    class Connection:
        def execute(self, statement):
            statements.append(statement)
            if statement.startswith(("LOAD", "INSTALL")):
                raise RuntimeError("extension load failed")

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", "postgres:dbname=test")
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", str(extension))
    monkeypatch.delenv("DUCKDB_EXTENSION_DIRECTORY", raising=False)

    attached = ducklake._ducklake_attach(Connection(), {"service_id": "svc"})

    assert attached is False
    assert statements == [f"LOAD '{extension}';"]


@pytest.mark.usefixtures("reset_admission_state")
def test_bundled_ducklake_path_is_sql_escaped(monkeypatch):
    statements = []

    class Connection:
        def execute(self, statement):
            statements.append(statement)
            raise RuntimeError("stop after extension load")

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", "postgres:dbname=test")
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/duck'lake/ducklake.duckdb_extension")
    monkeypatch.delenv("DUCKDB_EXTENSION_DIRECTORY", raising=False)

    assert ducklake._ducklake_attach(Connection(), {"service_id": "svc"}) is False
    assert statements == ["LOAD '/opt/duck''lake/ducklake.duckdb_extension';"]


@pytest.mark.usefixtures("reset_admission_state")
def test_ducklake_attach_configures_data_inlining_row_limit_zero(monkeypatch):
    statements = []

    class Connection:
        def execute(self, statement):
            statements.append(statement)
            if "duckdb_databases" in statement:
                return Mock(fetchone=Mock(return_value=None))
            return Mock(fetchone=Mock(return_value=None))

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", "postgres:dbname=test")
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    monkeypatch.delenv("DUCKDB_EXTENSION_DIRECTORY", raising=False)

    attached = ducklake._ducklake_attach(Connection(), {"service_id": "svc"})

    assert attached is True
    attach_stmts = [s for s in statements if s.startswith("ATTACH 'ducklake:")]
    assert len(attach_stmts) == 1
    assert "DATA_INLINING_ROW_LIMIT 0" in attach_stmts[0]


@pytest.mark.usefixtures("reset_admission_state")
def test_ducklake_attach_disables_inlined_tables_and_writes_parquet_directly(tmp_path, monkeypatch):
    catalog_path = tmp_path / "lake.ducklake"
    data_path = tmp_path / "data"
    source = {"service_id": "test_direct_parquet", "name": "test_direct_parquet"}

    monkeypatch.setattr(ducklake.config, "DUCKLAKE_CATALOG", f"ducklake:{catalog_path}")
    monkeypatch.setattr(ducklake.config, "DUCKLAKE_DATA_PATH", str(data_path))
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)

    import duckdb

    con = duckdb.connect()
    try:
        assert ducklake._ducklake_attach(con, source, read_only=False) is True
        con.execute("CREATE TABLE lake.t (id INT)")
        con.execute("INSERT INTO lake.t VALUES (1), (2)")
        file_count = con.execute("SELECT file_count FROM ducklake_table_info('lake')").fetchone()[0]
        assert file_count > 0, "DATA_INLINING_ROW_LIMIT 0 must write parquet immediately without inlining"
        snapshots = con.execute("SELECT * FROM lake.snapshots()").fetchall()
        for snap in snapshots:
            changes = snap[3]
            if isinstance(changes, dict):
                assert "inlined_insert" not in changes, "no inlined insert changes should occur"
    finally:
        con.close()


def _durable_source(tmp_path):
    return {
        "name": "durable-svc",
        "service_id": "durable-svc",
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
        "deployment_mode": "high_scale",
        "bucket": "bucket",
        "prefix": "logs",
    }


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_uses_ephemeral_connection_and_never_opens_native_file(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
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
        assert con.execute("SELECT 42").fetchone() == (42,)
    finally:
        con.close()


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_uses_writable_temp_directory(monkeypatch, tmp_path):
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
        assert temp_dir.is_dir()
        assert con.execute("SELECT current_setting('temp_directory')").fetchone() == (str(temp_dir),)
    finally:
        con.close()


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_rejects_writable_connection(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")

    with pytest.raises(RuntimeError, match="only permits read-only"):
        db.get_connection(source=_durable_source(tmp_path), read_only=False)


@pytest.mark.usefixtures("reset_admission_state")
def test_sync_file_mode_keeps_native_service_path(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    source = _durable_source(tmp_path)
    source["deployment_mode"] = "standard"

    assert db.db_path_for_source(source) == str(tmp_path / "must-not-open.duckdb")


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_serving_holder_skips_file_backed_pool(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
    source = _durable_source(tmp_path)
    fake_con = MagicMock()

    from backend import deps
    from backend.core import duckdb_pool

    with (
        patch.object(duckdb_pool, "checkout_connection") as checkout,
        patch("backend.deps.get_connection", return_value=fake_con) as get_con,
    ):
        holder = deps._ConnectionHolder(source, read_only=True)
        assert holder.__enter__() is fake_con
        holder.__exit__(None, None, None)

    checkout.assert_not_called()
    get_con.assert_called_once()


@pytest.mark.usefixtures("reset_admission_state")
def test_durable_view_does_not_consult_local_buffers(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")
    source = _durable_source(tmp_path)
    source["log_fields"] = {}

    with (
        patch.object(view._core_mod, "buffer_files", side_effect=AssertionError("local buffer accessed")),
        patch.object(view._core_mod, "get_arrow_schema", return_value=[]),
        patch("backend.core.iceberg._ducklake._ducklake_attach", return_value=True),
        patch.object(view, "_ducklake_view_token", return_value="ducklake:1"),
    ):
        view._view_cache.pop(source["name"], None)
        # The view rebuild uses the durable DuckLake table when present. This
        # fake connection only needs to answer catalog probes and record DDL.
        executed: list[str] = []

        class _Result:
            description = []

            def fetchone(self):
                return None

        class _Connection:
            def execute(self, sql, params=None):
                executed.append(sql)
                return _Result()

        view._update_iceberg_view_locked(_Connection(), source)

    assert any("CREATE OR REPLACE TEMP VIEW" in sql for sql in executed)


# ── Merged from tests/core/test_ducklake_attach_concurrency.py ──
def _make_committed_source(tmp_path, name: str) -> dict:
    """A service with one real committed row — matches the production
    scenario (an already-initialized DuckLake catalog with real
    snapshots), which is what actually raced in the live incident. A
    totally fresh, never-written-to local-file catalog has its own
    separate pre-create quirk unrelated to this bug."""
    cache = tmp_path / f"cache_{name}"
    (cache / "buffer").mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    cols = {
        "timestamp": pa.array([ts], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array(["1.2.3.4"]),
        "_source_file": pa.array(["s3://b/raw/a.gz"]),
    }
    path = os.path.join(str(cache), "buffer", "batch_a.parquet")
    pq.write_table(pa.table(cols), path)
    duckdb.connect(src["duckdb_path"]).close()
    assert _commit_buffer_impl(src)["rows_committed"] == 1
    return src


def test_concurrent_get_connection_all_attach_successfully(tmp_path):
    """N threads racing to build a fresh read-only pooled connection for
    the same already-initialized service must ALL end up with a working
    ``lake`` catalog — not just a non-throwing ``ATTACH`` statement.

    Before the ``_attach_lock`` fix, this reproduced the corruption on
    every racing connection, every run: ``_ducklake_attach`` returned
    True for all of them, but ``ducklake_snapshots('lake')`` then failed
    with ``Catalog "__ducklake_metadata_lake" does not exist`` on all of
    them too.
    """
    name = f"race{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    n_threads = 2
    results: list[tuple[int, str, object]] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        con = get_connection(source=src, read_only=True)
        try:
            row = con.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            with lock:
                results.append((i, "ok", row))
        except Exception as e:  # noqa: BLE001 - recording the failure IS the assertion
            with lock:
                results.append((i, "fail", str(e)))
        finally:
            con.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n_threads
    failures = [r for r in results if r[1] == "fail"]
    assert not failures, f"lake catalog corrupted on {len(failures)}/{n_threads} racing connections: {failures}"


def test_ducklake_attach_reuses_existing_matching_lake_alias(tmp_path):
    """A pooled checkout may call _ducklake_attach on a connection whose
    previous release failed to detach ``lake``. That path must verify and
    reuse the existing alias instead of issuing a second ATTACH, which can
    fatal the DuckDB connection."""
    from backend.core.iceberg._ducklake import _ducklake_attach

    name = f"reattach{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    con = get_connection(source=src, read_only=True)
    try:
        first = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert first is not None

        assert _ducklake_attach(con, src, read_only=True) is True

        second = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert second == first
    finally:
        con.close()


def test_readonly_attach_reuses_live_readwrite_attach_without_detaching(tmp_path):
    """A read-only caller must never evict a live read-write attach.

    ``duckdb.connect()`` calls against the SAME service .duckdb file share
    one underlying DuckDB instance/attach namespace — attaching ``lake``
    on one "connection" makes it visible (and, pre-fix, contestable) on
    every other connection to that same file. The pre-fix
    ``_ducklake_attach`` unconditionally detached on ANY mode mismatch, so
    a plain ``get_connection(read_only=True)`` racing a live writer
    (ingest/buffer/commit, or the startup Iceberg-view pre-warm, which
    both need ``read_only=False``) would rip the writer's in-flight attach
    out from under it — reproducing DuckDB core's own
    ``ResourceInUseException``: "Unique file handle conflict ... in the
    process of being detached". Observed in production at GCE cold start,
    surviving the process-wide ``_attach_lock`` and the retry-budget
    extension because it isn't a transient timing race — it is the reader
    itself destroying the writer's still-needed attach.

    Fix: a read-only caller that finds ``lake`` already attached
    read-write must reuse it as-is (a write-mode attach fully supports
    SELECT) instead of downgrading it. Only a genuine write caller that
    finds a read-only attach still needs to upgrade in place.
    """
    from backend.core.iceberg._ducklake import _ducklake_attach
    from backend.core.iceberg.buffer import _ducklake_write_connection

    name = f"rwreuse{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # Simulate the live writer via the SAME mechanism production uses
    # (`_commit_buffer_impl`): a dedicated connection to the service
    # .duckdb file with `lake` attached read-write, held open for the
    # duration of the "commit".
    with _ducklake_write_connection(src) as writer:
        before = writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
        assert before == (False,), "writer must hold a read-write lake attach"

        # A second connection to the SAME db_path (the reader) must be able
        # to attach read-only WITHOUT detaching the writer's live attach.
        reader = get_connection(source=src, read_only=True)
        try:
            after = writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
            assert after == (False,), "reader must not have downgraded/detached the writer's live read-write attach"

            # The writer must still be fully usable after the reader raced it.
            row = writer.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            assert row is not None

            # The reader itself must also see a working (shared, read-write
            # mode) catalog rather than erroring out.
            assert _ducklake_attach(reader, src, read_only=True) is True
            reader_row = reader.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            assert reader_row == row
        finally:
            reader.close()


def test_pool_release_keeps_lake_attached_for_sibling_in_flight_connection(tmp_path):
    """Releasing one pooled connection must not evict ``lake`` from a sibling.

    Connections to the same service file share one DuckDB instance, so ATTACH
    state is shared. A release-time ``DETACH lake`` removed the catalog from
    every concurrently checked-out connection; the dashboard bundle's parallel
    aggregates branch then failed with ``schema "lake" does not exist``
    (GCE 2026-10-07).
    """
    from backend.core.duckdb_pool import _Pool

    name = f"sibling{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)
    pool = _Pool(name, max_size=2)

    first = pool.acquire(src, max_wait=10, skip_view_update=True)
    second = pool.acquire(src, max_wait=10, skip_view_update=True)
    try:
        pool.release(first)
        row = second.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None

        reused = pool.acquire(src, max_wait=10, skip_view_update=True)
        assert reused.execute("SELECT count(*) FROM ducklake_snapshots('lake')").fetchone()[0] >= 1
        pool.release(reused)
    finally:
        pool.release(second)
        for con in list(pool._idle.queue):
            con.close()


def test_update_iceberg_view_locked_skips_reattach_when_lake_already_attached(tmp_path, monkeypatch):
    """A connection that already has ``lake`` attached must not be
    re-attached — a mode-mismatched re-attach (read_only=False on an
    already read_only=True-attached alias) is what corrupted the
    catalog in the live incident."""
    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"skip{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # get_connection() itself already attaches lake read-only for a pooled
    # connection (mirroring the real request-context path), so by the time
    # _update_iceberg_view_locked runs, lake is already attached.
    con = get_connection(source=src, read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        # lake is already attached — _update_iceberg_view_locked must not
        # re-attach it (that mode-mismatched re-attach is what corrupted
        # the catalog in the live incident).
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [], "lake already attached — _ducklake_attach must not be called again"

        # Calling it again is equally a no-op on the attach front.
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == []

        # The connection is genuinely still functional afterward.
        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()


def test_ducklake_detach_is_serialized_by_the_attach_lock():
    """``_ducklake_detach`` (used by pool release, commit_buffer's
    read-write reattach, and legacy adoption) must acquire the SAME
    process-wide ``_attach_lock`` that ``_ducklake_attach`` does.

    Before this fix, ``release()``/``_ducklake_write_connection()``/
    ``adopt_iceberg_to_ducklake()`` each called a raw, unlocked
    ``con.execute("DETACH lake")``. That raced with any OTHER connection
    concurrently inside a locked ``_ducklake_attach`` call and ripped
    ``lake`` out from under it mid-operation — reproduced in production as
    "Catalog Error: Schema with name lake does not exist!" commit
    failures on connections that had just successfully attached moments
    earlier. This test proves ``_ducklake_detach`` cannot run while
    another caller holds ``_attach_lock``.
    """
    from backend.core.iceberg import _ducklake as ducklake_mod

    class FakeConnection:
        def __init__(self):
            self.commands: list[str] = []

        def execute(self, sql: str):
            self.commands.append(sql)
            return self

    fake_con = FakeConnection()
    release_event = threading.Event()
    detach_started = threading.Event()
    detach_done = threading.Event()

    def holder():
        ducklake_mod._attach_lock.acquire()
        try:
            release_event.wait(timeout=5)
        finally:
            ducklake_mod._attach_lock.release()

    def detacher():
        detach_started.set()
        ducklake_mod._ducklake_detach(fake_con, aliases=("lake",), service_id="test")
        detach_done.set()

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    try:
        # Wait until the holder thread definitely has the lock.
        for _ in range(200):
            if ducklake_mod._attach_lock.locked():
                break
            import time as _time

            _time.sleep(0.005)
        assert ducklake_mod._attach_lock.locked()

        detach_thread = threading.Thread(target=detacher)
        detach_thread.start()
        detach_started.wait(timeout=2)
        # The detach must NOT have executed its DETACH statement yet —
        # it's blocked waiting for the lock the holder thread is holding.
        assert fake_con.commands == [], "DETACH ran without waiting for the process-wide attach lock"
        assert not detach_done.is_set()

        release_event.set()
        detach_thread.join(timeout=5)
        assert detach_done.is_set()
        assert fake_con.commands == ["DETACH lake"]
    finally:
        holder_thread.join(timeout=5)


def test_attach_retry_releases_lock_during_backoff_and_succeeds(monkeypatch):
    """After a 'unique file handle conflict' (a DIFFERENT, still-busy
    connection holding the real attach open), the retry loop must:

    1. Release ``_attach_lock`` during its backoff sleep — otherwise an
       unrelated THIRD connection's attach attempt is forced to queue
       behind this one's wait for a lock it isn't even contending for.
    2. Eventually succeed once the conflict clears, using the extended
       retry budget (previously 5 attempts/~7.5s total, which production
       showed was too short for realistic ~10-20s cron-overlap windows).
    """
    from backend.core.iceberg import _ducklake as ducklake_mod

    monkeypatch.setattr(ducklake_mod, "_ATTACH_CONFLICT_RETRY_SLEEP_S", 0.2)
    monkeypatch.setattr(ducklake_mod, "_ATTACH_CONFLICT_RETRY_ATTEMPTS", 10)

    class FakeConn:
        def __init__(self):
            self.commands: list[str] = []
            self.attach_attempts = 0

        def execute(self, sql: str):
            self.commands.append(sql)
            if sql.startswith("ATTACH 'ducklake:") and " AS lake " in sql:
                self.attach_attempts += 1
                if self.attach_attempts <= 3:
                    raise RuntimeError("Unique file handle conflict: already attached by database xyz")
            return self

        def fetchone(self):
            return None

    fake_con = FakeConn()
    lock_acquired_during_sleep = threading.Event()

    def foreign_acquirer() -> None:
        # Give the retry loop a moment to hit its first conflict + sleep.
        time.sleep(0.05)
        if ducklake_mod._attach_lock.acquire(timeout=1.0):
            lock_acquired_during_sleep.set()
            ducklake_mod._attach_lock.release()

    foreign_thread = threading.Thread(target=foreign_acquirer)
    foreign_thread.start()
    try:
        result = ducklake_mod._ducklake_attach(fake_con, {"service_id": "test"}, read_only=False)
    finally:
        foreign_thread.join(timeout=5)

    assert result is True
    assert fake_con.attach_attempts == 4, "expected 3 conflicts then a succeeding 4th ATTACH attempt"
    assert lock_acquired_during_sleep.is_set(), (
        "a foreign connection could not acquire _attach_lock while this call was sleeping in its "
        "retry backoff — the lock is being held across the whole wait instead of released"
    )
    # Sanity: not held after return either.
    assert not ducklake_mod._attach_lock.locked()


def test_update_iceberg_view_locked_attaches_matching_connection_mode(tmp_path, monkeypatch):
    """When ``lake`` is NOT yet attached on the connection,
    ``_update_iceberg_view_locked`` must attach it matching the
    connection's actual read-only mode — not hardcode ``read_only=False``
    (which raises on a genuinely read-only connection and, on a
    connection where some OTHER alias attach already exists, is the
    mode-mismatched re-attach that corrupts the catalog)."""
    import duckdb

    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"raw{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # A bare read-only connection to the same file, bypassing
    # get_connection() entirely, so lake is genuinely not attached yet.
    con = duckdb.connect(src["duckdb_path"], read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [True], (
            f"expected a single read_only=True attach to match the RO connection, got {attach_calls}"
        )

        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()


def test_get_connection_closing_context_manager_behavior(tmp_path):
    """Verify that using contextlib.closing with get_connection closes the connection and releases its file handle."""
    from contextlib import closing

    import pytest

    name = f"closing{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    with closing(get_connection(source=src, read_only=True)) as con:
        assert con.execute("SELECT 1").fetchone()[0] == 1

    import duckdb

    with pytest.raises((duckdb.ConnectionException, duckdb.InvalidInputException, Exception)):
        con.execute("SELECT 1")


# ── Merged from tests/core/test_ducklake_layer.py ──
_make_source = partial(make_source, kind="lake", buffer_dir=True)


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
        row = con.execute(f'SELECT count(*) FROM lake."{table}"').fetchone()
        assert row is not None
        return int(row[0])
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


def test_ducklake_table_name_is_per_service_and_sanitized():
    assert ducklake_table_name({"service_id": "AbC-123"}) == "logs_abc_123"
    assert ducklake_table_name({"service_id": "FastlyService123", "name": "Customer-facing name"}) == (
        "logs_fastlyservice123"
    )
    assert ducklake_table_name({"name": "svc.two"}) == "logs_svc_two"
    assert ducklake_table_name({"name": "svc.two"}, "client_vitals") == "logs_svc_two__client_vitals"
    # hostile input cannot break out of an identifier
    assert ducklake_table_name({"service_id": 'x"; DROP TABLE lake.logs;--'}) == "logs_x___drop_table_lake_logs"


class TestCommitTombstoneContract:
    """Trap #26: commit must tombstone (not unlink) and be idempotent per file."""

    def test_commit_tombstones_instead_of_unlinking(self, tmp_path):
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        p = _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)

        res = _commit_buffer_impl(src)
        assert res["files_committed"] == 1
        assert res["rows_committed"] == 3
        assert _lake_count(src) == 3

        # The parquet must still exist on disk (views bound pre-commit keep
        # reading it) but be tombstoned out of new view binds.
        assert os.path.exists(p), "commit must NOT hard-unlink buffer parquet"
        assert buffer_files(src) == []
        buf_dir = os.path.dirname(p)
        markers = [f for f in os.listdir(buf_dir) if ".consumed-" in f]
        assert markers, "commit must write a tombstone sidecar"

    def test_recommit_after_lost_tombstone_does_not_duplicate(self, tmp_path):
        """Crash between INSERT and tombstone → the file is visible again on
        the next cycle. The DELETE-by-_source_file + INSERT transaction must
        replace, not append."""
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        p = _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)

        assert _commit_buffer_impl(src)["rows_committed"] == 3
        # Simulate the crash: tombstone never landed.
        buf_dir = os.path.dirname(p)
        for f in os.listdir(buf_dir):
            if ".consumed-" in f:
                os.remove(os.path.join(buf_dir, f))
        assert buffer_files(src) == [p]

        res2 = _commit_buffer_impl(src)
        assert res2["files_committed"] == 1
        assert _lake_count(src) == 3, "re-commit of the same buffer file must not duplicate rows"

    def test_second_source_file_commit_appends(self, tmp_path):
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3
        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2
        assert _lake_count(src) == 5

    def test_commit_matches_narrower_buffer_by_column_name(self, tmp_path):
        """A later buffer can have fewer columns than the evolved lake table."""
        src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(
            src,
            "batch_wide.parquet",
            ts=ts,
            source_file="s3://b/raw/wide.gz",
            extra={f"custom_{i}": f"value-{i}" for i in range(5)},
        )
        assert _commit_buffer_impl(src)["rows_committed"] == 1

        _write_buffer(src, "batch_narrow.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/narrow.gz")
        result = _commit_buffer_impl(src)

        assert result["rows_committed"] == 1
        assert _lake_count(src) == 2


def test_rollup_count_preserves_rows_when_buffer_commits_mid_recompute(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)
    assert _commit_buffer_impl(src)["rows_committed"] == 3
    _write_buffer(src, "batch_b.parquet", ts=ts, source_file="s3://b/raw/b.gz", n=2)
    original = buffer_mod._ducklake_write_connection
    written = []

    @contextmanager
    def publish_during_commit(source):
        with original(source) as con:
            yield con
            written.append(build_time_series_bundles(source["name"], source, ["2026-08-30-12"]))

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", publish_during_commit)
    with rollup_common.shared_rollup_session(src) as session:
        con, _ = session.acquire(
            rollup_common._safe_table_for(src),
            logger=rollup_common.logger,
            log_label="rollup commit consistency",
        )
        assert con.execute(f"SELECT COUNT(*) FROM {_safe_table_name(src['name'])}").fetchone()[0] == 5
        assert _commit_buffer_impl(src)["rows_committed"] == 2

    assert written == [1]
    bundle = os.path.join(rollup_common._hour_bundled_root(src), "hour=2026-08-30-12", "time_series.parquet")
    assert sum(pq.read_table(bundle).column("requests").to_pylist()) == 5

    _write_buffer(src, "batch_c.parquet", ts=ts, source_file="s3://b/raw/c.gz", n=1)
    fresh = get_connection(src)
    try:
        assert fresh.execute(f"SELECT COUNT(*) FROM {_safe_table_name(src['name'])}").fetchone()[0] == 6
    finally:
        fresh.close()


def test_view_cache_refreshes_if_a_commit_follows_snapshot_selection(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=3)
    assert _commit_buffer_impl(src)["rows_committed"] == 3
    _write_buffer(src, "batch_b.parquet", ts=ts, source_file="s3://b/raw/b.gz", n=2)
    original = view_mod._finalize_view_sql

    def finalize_after_commit(*args, **kwargs):
        monkeypatch.setattr(view_mod, "_finalize_view_sql", original)
        assert _commit_buffer_impl(src)["rows_committed"] == 2
        return original(*args, **kwargs)

    monkeypatch.setattr(view_mod, "_finalize_view_sql", finalize_after_commit)
    con = get_connection(src)
    try:
        assert con.execute(f"SELECT COUNT(*) FROM {_safe_table_name(src['name'])}").fetchone()[0] == 5
    finally:
        con.close()

    fresh = get_connection(src)
    try:
        assert fresh.execute(f"SELECT COUNT(*) FROM {_safe_table_name(src['name'])}").fetchone()[0] == 5
    finally:
        fresh.close()


def test_committed_view_refuses_an_unverifiable_snapshot(tmp_path, monkeypatch):
    src = _make_source(tmp_path, f"dl{uuid.uuid4().hex[:8]}")
    _write_buffer(src, "batch_a.parquet", ts=datetime(2026, 8, 30, tzinfo=UTC), source_file="s3://b/raw/a.gz")
    assert _commit_buffer_impl(src)["rows_committed"] == 1
    con = get_connection(src)
    monkeypatch.setattr(
        import_module("backend.core.iceberg._ducklake"), "ducklake_current_snapshot_id", lambda con: None
    )
    try:
        with pytest.raises(RuntimeError, match="Cannot verify the DuckLake snapshot"):
            view_mod._update_iceberg_view_locked(con, src)
    finally:
        con.close()


class TestTenantIsolation:
    def test_shared_catalog_views_cannot_see_other_tenant_rows(self, tmp_path, monkeypatch):
        """Two sources attached to ONE shared DuckLake catalog must not see
        each other's rows through their per-service views."""
        a = _make_source(tmp_path, f"ta{uuid.uuid4().hex[:6]}")
        b = _make_source(tmp_path, f"tb{uuid.uuid4().hex[:6]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(a, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", extra={"host": "a.example"})
        _write_buffer(b, "batch_b.parquet", ts=ts, source_file="s3://b/raw/b.gz", extra={"host": "b.example"})

        assert _commit_buffer_impl(a)["rows_committed"] == 1
        assert _commit_buffer_impl(b)["rows_committed"] == 1

        for src, own_host, other_host in ((a, "a.example", "b.example"), (b, "b.example", "a.example")):
            view_mod.clear_source_caches(src["name"])
            con = get_connection(src, read_only=False)
            try:
                view_mod._update_iceberg_view_locked(con, src)
                view_name = _safe_table_name(src["name"])
                hosts = {r[0] for r in con.execute(f"SELECT host FROM {view_name}").fetchall()}
                assert own_host in hosts
                assert other_host not in hosts, f"{src['name']} view leaked another tenant's rows"
            finally:
                con.close()


class TestFastPathSteadyState:
    def test_two_consecutive_checkouts_do_not_rebuild(self, tmp_path, monkeypatch):
        """After one slow-path build, subsequent checkouts must bind via the
        lock-free fast path (the DuckLake-era token used to NEVER match,
        sending every read through the RLock + full rebuild)."""
        src = _make_source(tmp_path, f"fp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        # Open the connection FIRST — get_connection triggers its own view
        # build, which would consume the slow-path call this test counts.
        con = get_connection(src, read_only=False)
        view_mod.clear_source_caches(src["name"])

        slow_calls = {"n": 0}
        orig = view_mod._update_iceberg_view_locked

        def spy(c, source, target_table="logs", force=False):
            slow_calls["n"] += 1
            return orig(c, source, target_table=target_table, force=force)

        monkeypatch.setattr(view_mod._core_mod, "_update_iceberg_view_locked", spy)

        try:
            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 1
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is False

            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 1, "steady-state checkout must not rebuild"
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True

            # A second connection (fresh pool checkout) also fast-paths.
            con2 = get_connection(src, read_only=False)
            try:
                view_mod.update_iceberg_view(con2, src)
                assert slow_calls["n"] == 1
                assert view_mod.get_last_view_stats(src)["was_fast_path"] is True
            finally:
                con2.close()
        finally:
            con.close()

    def test_buffer_only_change_reconstructs_without_lock(self, tmp_path, monkeypatch):
        src = _make_source(tmp_path, f"fp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            view_mod.update_iceberg_view(con, src)

            slow_calls = {"n": 0}
            orig = view_mod._update_iceberg_view_locked

            def spy(c, source, target_table="logs", force=False):
                slow_calls["n"] += 1
                return orig(c, source, target_table=target_table, force=force)

            monkeypatch.setattr(view_mod._core_mod, "_update_iceberg_view_locked", spy)

            # New buffer file, no commit: the fast path must reconstruct the
            # view SQL (buffer-only change) instead of taking the lock.
            _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=3)
            view_mod.update_iceberg_view(con, src)
            assert slow_calls["n"] == 0, "buffer-only change must stay on the lock-free fast path"
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True

            view_name = _safe_table_name(src["name"])
            row = con.execute(f"SELECT count(*) FROM {view_name}").fetchone()
            assert row is not None and row[0] == 5, "reconstructed view must union lake + new buffer"
        finally:
            con.close()


class TestViewSchemaParity:
    def test_view_carries_computed_columns_and_decodes(self, tmp_path):
        from backend.core import field_registry as fc

        src = _make_source(tmp_path, f"vp{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 34, tzinfo=UTC)
        _write_buffer(
            src,
            "batch_a.parquet",
            ts=ts,
            source_file="s3://b/raw/a.gz",
            extra={"c_speed": fc.CONN_SPEED_ENCODE["broadband"], "ttl": 3599.7},
        )
        assert _commit_buffer_impl(src)["rows_committed"] == 1

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            view_mod._update_iceberg_view_locked(con, src)
            view_name = _safe_table_name(src["name"])
            row = con.execute(f"SELECT timestamp_hour, dt, c_speed, ttl FROM {view_name}").fetchone()
            assert row == ("2026-08-30-12", "2026-08-30", "broadband", 3600)
        finally:
            con.close()

    def test_analyst_time_range_clamp_applies_to_view(self, tmp_path):
        """Data-scoping regression guard: a read_only source's view must
        clamp to the granted time_range window on BOTH paths."""
        window_start = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        window_end = datetime(2026, 8, 30, 13, 0, tzinfo=UTC)
        src = _make_source(
            tmp_path,
            f"an{uuid.uuid4().hex[:8]}",
            access_level="read_only",
            time_range={"start": window_start.isoformat(), "end": window_end.isoformat()},
        )
        _write_buffer(src, "batch_in.parquet", ts=window_start + timedelta(minutes=5), source_file="s3://b/raw/in.gz")
        _write_buffer(src, "batch_out.parquet", ts=window_start - timedelta(hours=2), source_file="s3://b/raw/out.gz")
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        view_mod.clear_source_caches(src["name"])
        con = get_connection(src, read_only=False)
        try:
            # Slow path
            view_mod._update_iceberg_view_locked(con, src)
            view_name = _safe_table_name(src["name"])
            rows = con.execute(f"SELECT timestamp FROM {view_name}").fetchall()
            assert len(rows) == 1, f"analyst view must clamp to time_range; got {rows}"
            assert rows[0][0].replace(tzinfo=UTC) >= window_start

            # Fast path (buffer-only change → reconstruction) must clamp too.
            _write_buffer(
                src, "batch_out2.parquet", ts=window_start - timedelta(hours=3), source_file="s3://b/raw/o2.gz"
            )
            view_mod.update_iceberg_view(con, src)
            assert view_mod.get_last_view_stats(src)["was_fast_path"] is True
            rows = con.execute(f"SELECT timestamp FROM {view_name}").fetchall()
            assert len(rows) == 1, "fast-path reconstruction dropped the analyst time clamp"
        finally:
            con.close()


class TestActiveHourDirectRead:
    def test_direct_read_includes_committed_lake_rows(self, tmp_path):
        """Committed rows live in lake.<table> post-v3 — the active-hour
        direct read must union them with the (non-tombstoned) buffer, with
        no double count."""
        from backend.repositories._base import QueryRunner

        src = _make_source(tmp_path, f"ah{uuid.uuid4().hex[:8]}")
        live_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        live_end = live_start + timedelta(hours=1)

        # 3 committed rows (drained buffer → tombstoned) + 2 fresh buffer rows.
        _write_buffer(src, "batch_a.parquet", ts=live_start + timedelta(minutes=1), source_file="s3://b/a.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3
        _write_buffer(src, "batch_b.parquet", ts=live_start + timedelta(minutes=30), source_file="s3://b/b.gz", n=2)

        con = get_connection(src)
        try:
            runner = QueryRunner(con, src)
            tmp_name = runner._create_active_hour_temp_direct(["ip"], ["timestamp", "ip"], live_start, live_end)
            assert tmp_name is not None
            row = con.execute(f'SELECT count(*) FROM "{tmp_name}"').fetchone()
            assert row is not None
            assert row[0] == 5, f"direct active-hour read must union lake (3 committed) + live buffer (2), got {row[0]}"
        finally:
            con.close()


def test_target_file_size_cap_asserted_on_rw_attach(tmp_path):
    """Item: ducklake_merge_adjacent_files must respect the same size cap as
    local compaction (LOCAL_COMPACT_MAX_PARTITION_MB, 256MB default) — the
    option is pinned on every read-write attach."""
    src = _make_source(tmp_path, f"sz{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz")
    assert _commit_buffer_impl(src)["rows_committed"] == 1

    con = get_connection(src)
    try:
        row = con.execute("SELECT value FROM lake.options() WHERE option_name = 'target_file_size'").fetchone()
        assert row is not None, "target_file_size must be set on the DuckLake catalog"
        assert int(row[0]) == 256 * 1024 * 1024
    finally:
        con.close()


class TestDuckLakeTableInfo:
    """get_table_info / get_snapshot_calendar / ducklake_table_exists —
    the DuckLake-native rewrite (v3 write-path cutover). The pyiceberg
    catalog these used to read is permanently frozen post-cutover; these
    now read `lake.*` state via a real DuckLake attach, no mocks."""

    def test_table_missing_before_first_commit(self, tmp_path):
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        assert ducklake_table_exists(src) is False

        info = get_table_info(src)
        assert info["error"]
        assert info["snapshots"] == 0
        assert info["data_files"] == 0
        assert info["size_bytes"] == 0
        assert info["min_timestamp"] is None
        assert info["max_timestamp"] is None
        assert get_snapshot_calendar(src) == {}

    def test_small_commit_is_inlined_but_still_counted(self, tmp_path):
        """Small inserts land "inlined" in DuckLake's catalog metadata
        rather than as parquet files — ducklake_table_info reports
        file_count=0 for a table that genuinely has committed rows. The
        panel must not show a false "0 files"; data_files falls back to
        the scanned row count in that state (see AGENTS.md DuckLake trap)."""
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        assert ducklake_table_exists(src) is True

        con = get_connection(src)
        try:
            table = ducklake_table_name(src)
            file_count = con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = ?", [table]
            ).fetchone()[0]
            assert file_count in (0, 1), "file_count must be 0 (inlined) or 1 (single parquet)"
        finally:
            con.close()

        info = get_table_info(src)
        assert "error" not in info
        assert info["snapshots"] == 1
        assert info["data_files"] in (1, 2)
        assert info["min_timestamp"] == "2026-08-30T12:00:00+00:00"
        assert info["max_timestamp"] == "2026-08-30T12:00:01+00:00"
        assert info["table_location"]

        calendar = get_snapshot_calendar(src)
        assert calendar == {"2026-08-30": {"data_files": 2, "size_bytes": 0}}

    def test_calendar_spans_multiple_days_across_commits(self, tmp_path):
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        day1 = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        day2 = datetime(2026, 8, 31, 6, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=day1, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2
        _write_buffer(src, "batch_b.parquet", ts=day2, source_file="s3://b/raw/b.gz", n=3)
        assert _commit_buffer_impl(src)["rows_committed"] == 3

        calendar = get_snapshot_calendar(src)
        assert len(calendar) == 2
        assert "2026-08-30" in calendar
        assert "2026-08-31" in calendar
        info = get_table_info(src)
        assert info["data_files"] in (2, 5)
        assert info["min_timestamp"] == "2026-08-30T12:00:00+00:00"
        assert info["max_timestamp"] == "2026-08-31T06:00:02+00:00"
        assert info["snapshots"] == 2

    def test_scan_is_cached_across_calls_until_next_commit(self, tmp_path, monkeypatch):
        """The 60s status-poller (_duckdb_status.refresh_config_status)
        calls get_table_info on a steady cadence — a full-table scan on
        every tick would regress the exact scan-cost class this codebase
        is pinned against. Cache key is the DuckLake snapshot id: repeat
        reads with no new commit must not re-scan; a new commit must."""
        src = _make_source(tmp_path, f"ti{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        assert _commit_buffer_impl(src)["rows_committed"] == 2

        calls = {"n": 0}
        orig = manifest_mod._scan_ducklake_table_metadata

        def counting(con, lake_table):
            calls["n"] += 1
            return orig(con, lake_table)

        monkeypatch.setattr(manifest_mod, "_scan_ducklake_table_metadata", counting)

        get_table_info(src)
        get_snapshot_calendar(src)
        get_table_info(src)
        assert calls["n"] == 1, "repeat reads with no new commit must reuse the cached scan"

        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(minutes=1), source_file="s3://b/raw/b.gz", n=1)
        assert _commit_buffer_impl(src)["rows_committed"] == 1
        get_table_info(src)
        assert calls["n"] == 2, "a new commit must invalidate the cached scan"


class TestCommitBufferChunking:
    """A large backlog must not hold one exclusive DuckLake write-attach
    for the whole commit — see AGENTS.md Trap #35 (fifth instance). Each
    chunk of ``_BUFFER_COMMIT_CHUNK_SIZE`` files must open and detach its
    own write-attach so concurrent readers/writers get a fair shot at the
    process-wide attach lock between chunks, instead of starving for the
    entire backlog-catchup duration."""

    def test_large_backlog_commits_in_bounded_chunks(self, tmp_path, monkeypatch):
        monkeypatch.setattr(buffer_mod, "_BUFFER_COMMIT_CHUNK_SIZE", 2)

        attach_calls = {"n": 0}
        orig_write_conn = buffer_mod._ducklake_write_connection

        def counting_write_conn(source):
            attach_calls["n"] += 1
            return orig_write_conn(source)

        monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", counting_write_conn)

        src = _make_source(tmp_path, f"chunk{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        for i in range(5):
            _write_buffer(
                src,
                f"batch_{i}.parquet",
                ts=ts + timedelta(seconds=i),
                source_file=f"s3://b/raw/{i}.gz",
                n=1,
            )

        result = _commit_buffer_impl(src)

        assert result["rows_committed"] == 5
        assert result["files_committed"] == 5
        assert result["quarantined_files"] == 0
        # 5 files at chunk size 2 => 3 attach/detach cycles (2, 2, 1), never 1.
        assert attach_calls["n"] == 3
        assert _lake_count(src) == 5

    def test_chunked_commit_preserves_rows_when_a_later_chunk_fails_to_attach(self, tmp_path, monkeypatch):
        """If a later chunk's attach fails (transient contention), rows
        already committed by earlier chunks must stay committed — the
        function must not discard prior progress just because a
        subsequent chunk couldn't get the write-attach."""
        monkeypatch.setattr(buffer_mod, "_BUFFER_COMMIT_CHUNK_SIZE", 2)

        orig_write_conn = buffer_mod._ducklake_write_connection
        state = {"n": 0}

        def failing_after_first_chunk(source):
            state["n"] += 1
            if state["n"] == 2:
                raise RuntimeError("Failed to attach DuckLake in read-write mode")
            return orig_write_conn(source)

        monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", failing_after_first_chunk)

        src = _make_source(tmp_path, f"chunkfail{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        for i in range(4):
            _write_buffer(
                src,
                f"batch_{i}.parquet",
                ts=ts + timedelta(seconds=i),
                source_file=f"s3://b/raw/{i}.gz",
                n=1,
            )

        result = _commit_buffer_impl(src)

        # First chunk (2 files) commits fine; second chunk's attach raises
        # and processing stops there rather than aborting everything.
        assert result["rows_committed"] == 2
        assert result["files_committed"] == 2
        assert "Failed to attach DuckLake" in result.get("error", "")
        assert _lake_count(src) == 2

        monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", orig_write_conn)
        retry_result = _commit_buffer_impl(src)
        assert retry_result["files_committed"] == 2
        assert retry_result["rows_committed"] == 2
        assert retry_result.get("error") is None
        assert _lake_count(src) == 4

    def test_unreadable_parquet_is_quarantined_without_losing_valid_rows(self, tmp_path):
        src = _make_source(tmp_path, f"quarantine{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_good.parquet", ts=ts, source_file="s3://b/raw/good.gz", n=2)
        bad_path = os.path.join(src["_cache_dir_override"], "buffer", "batch_bad.parquet")
        with open(bad_path, "wb") as bad_file:
            bad_file.write(b"not a parquet file")

        result = _commit_buffer_impl(src)

        assert result["files_committed"] == 1
        assert result["rows_committed"] == 2
        assert result["quarantined_files"] == 1
        assert result.get("error") is None
        assert _lake_count(src) == 2
        assert not os.path.exists(bad_path)
        quarantine_dir = os.path.join(src["_cache_dir_override"], "buffer", ".quarantine")
        assert any(name.endswith("__batch_bad.parquet") for name in os.listdir(quarantine_dir))

    def test_per_file_fallback_reports_parquet_row_count(self, tmp_path, monkeypatch):
        def reject_multi_file_insert(query):
            if query.startswith("INSERT INTO") and query.count("batch_") > 1:
                raise RuntimeError("force per-file fallback")

        _proxy_commit_connection(monkeypatch, reject_multi_file_insert)
        src = _make_source(tmp_path, f"fallbackrows{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(seconds=1), source_file="s3://b/raw/b.gz", n=3)

        result = _commit_buffer_impl(src)

        assert result["files_committed"] == 2
        assert result["rows_committed"] == 5
        assert result.get("error") is None
        assert _lake_count(src) == 5

    def test_per_file_fallback_reports_retryable_file_failure(self, tmp_path, monkeypatch):
        failed = {"once": True}

        def reject_multi_file_insert_and_one_file(query):
            if query.startswith("INSERT INTO") and query.count("batch_") > 1:
                raise RuntimeError("force per-file fallback")
            if query.startswith("INSERT INTO") and "batch_b.parquet" in query and failed["once"]:
                failed["once"] = False
                raise RuntimeError("transient per-file write failure")

        original = _proxy_commit_connection(monkeypatch, reject_multi_file_insert_and_one_file)
        src = _make_source(tmp_path, f"fallbackfailure{uuid.uuid4().hex[:8]}")
        ts = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
        _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=2)
        _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(seconds=1), source_file="s3://b/raw/b.gz", n=3)

        result = _commit_buffer_impl(src)

        assert result["files_committed"] == 1
        assert result["rows_committed"] == 2
        assert "transient per-file write failure" in result.get("error", "")
        assert buffer_files(src) == [os.path.join(src["_cache_dir_override"], "buffer", "batch_b.parquet")]
        assert _lake_count(src) == 2

        monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", original)
        retry_result = _commit_buffer_impl(src)
        assert retry_result["files_committed"] == 1
        assert retry_result["rows_committed"] == 3
        assert _lake_count(src) == 5


def test_ducklake_failsafe_blocks_remote_s3_parquet_scan(tmp_path, monkeypatch, caplog):
    """Verify failsafe blocks S3 DATA_PATH and falls back to local disk unless opted in."""
    import logging
    from unittest.mock import MagicMock

    from backend import config
    from backend.core.iceberg import _ducklake as dl

    src = {"service_id": "svc-guard-test", "bucket": "fos-some-bucket"}
    local_expected = str(config.SERVICES_DATA_DIR / "svc-guard-test" / "ducklake")

    # 1. Standard mode defaults to local disk even with a bucket present.
    assert dl._default_data_path(src) == local_expected

    # High-scale workers may not share /app/data with the serving pod, so
    # their default must be durable object storage.
    hs_src = {**src, "deployment_mode": "high_scale", "prefix": "logs"}
    assert dl._default_data_path(hs_src) == "s3://fos-some-bucket/logs/ducklake/"
    monkeypatch.setenv("DUCKLAKE_LOCAL_STORAGE", "1")
    assert dl._default_data_path(hs_src) == local_expected
    monkeypatch.delenv("DUCKLAKE_LOCAL_STORAGE")

    # 2. An explicit remote path in standard mode is still blocked unless opted in.
    monkeypatch.setattr("backend.config.DUCKLAKE_DATA_PATH", "s3://fos-some-bucket/ducklake/")
    monkeypatch.delenv("ALLOW_REMOTE_S3_PARQUET_SCAN", raising=False)

    fake_con = MagicMock()

    # Simulate fresh connection where 'lake' is not yet attached
    def _execute_side_effect(sql, *args, **kwargs):
        if "ducklake_snapshots" in sql:
            raise RuntimeError("database 'lake' does not exist")
        return MagicMock(fetchone=lambda: None)

    fake_con.execute.side_effect = _execute_side_effect

    with caplog.at_level(logging.ERROR):
        # We test that _ducklake_attach intercepts the s3 path and substitutes local_expected
        dl._ducklake_attach(fake_con, src, read_only=True)

    assert "BLOCKED remote S3 DuckLake parquet scan" in caplog.text
    all_sql = [call[0][0] for call in fake_con.execute.call_args_list]
    assert any(local_expected in sql for sql in all_sql)
    assert not any("s3://" in sql for sql in all_sql)

    # 3. Explicit opt-in via ALLOW_REMOTE_S3_PARQUET_SCAN=1 allows it with warning
    monkeypatch.setenv("ALLOW_REMOTE_S3_PARQUET_SCAN", "1")
    fake_con.reset_mock()
    fake_con.execute.side_effect = _execute_side_effect
    caplog.clear()

    with caplog.at_level(logging.WARNING):
        dl._ducklake_attach(fake_con, src, read_only=True)

    assert "Remote S3 DuckLake parquet scan is actively allowed" in caplog.text
    all_sql_allowed = [call[0][0] for call in fake_con.execute.call_args_list]
    assert any("s3://fos-some-bucket/ducklake/" in sql for sql in all_sql_allowed)


# ── Merged from tests/core/test_lake_info.py ──
def test_iceberg_fallback_returns_table_info(tmp_path):
    """When the fast path fails (e.g. ``get_object`` raises NoSuchKey),
    the DuckLake fallback reads real committed lake state directly. Tiny
    commits may be inlined or materialized into a different number of files,
    so only require a non-empty file/row count here."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 2, 1, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=1)
    _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(seconds=1), source_file="s3://b/raw/b.gz", n=1)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 2

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is True
    assert out["table_exists"] is True
    assert out["info"]["min_timestamp"] == "2026-02-01T00:00:00+00:00"
    assert out["info"]["data_files"] >= 1
    assert out["calendar"] == {"2026-02-01": {"data_files": 2, "size_bytes": 0}}
    assert out["range"]["start"] == "2026-02-01T00:00:00+00:00"


def test_iceberg_fallback_returns_table_does_not_exist_when_none(tmp_path):
    """A DuckLake catalog with no committed table yet (pre-first-ingest)
    must surface as ``table_exists: False`` so the UI shows the empty
    state instead of an error."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_iceberg_fallback_surfaces_unexpected_errors(tmp_path):
    """A real error (bad DuckLake attach) — NOT a missing-table — surfaces
    as ``ok: False`` with the error string. The frontend distinguishes
    this from the empty-lake case to render a different UI affordance."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 AccessDenied")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
    ):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is False
    assert "403" in out["error"]


def test_temp_cache_path_clears_source_caches_on_exit(tmp_path):
    """``use_temp_cache=True`` is used during PROVISIONING (the service
    isn't registered yet). After deriving info, it MUST call
    ``clear_source_caches`` — otherwise the in-memory catalog cache
    grows by one entry per provisioning attempt that gets aborted."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    _write_buffer(src, "batch.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=1)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 1

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
    ):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["table_exists"] is True
    mock_clear.assert_called_once_with(src["name"])


def test_temp_cache_path_returns_empty_lake_when_table_missing(tmp_path):
    """Same empty-table coercion as the direct path, but routed through
    the temp_cache branch — pinned because both branches must agree
    on the shape they return to the provision wizard."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_temp_cache_path_surfaces_unexpected_errors(tmp_path):
    """An unexpected error inside the temp_cache branch must surface
    via the outer except as ``ok: False``."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 boom")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg._core import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["ok"] is False
    assert "403" in out["error"]


# ── Merged from tests/core/test_cloud_maintenance_ducklake.py ──
NOW = datetime.now(UTC)


def _set_knobs(monkeypatch, **cron_sync) -> None:
    monkeypatch.setattr(svcconfig, "load_config", lambda _sid: {"provisioning": {"cron_sync": cron_sync}})


def _lake_con(src: dict):
    con = get_connection(src)
    try:
        con.execute("DETACH lake")
    except Exception:
        pass
    assert _ducklake_attach(con, src, read_only=False)
    return con


def _seed(src: dict, table_name: str, columns: str, rows: list[tuple], *, inline: bool = True):
    """Create ``lake.<per-service table>`` and INSERT ``rows``.

    ``inline=False`` disables DuckLake's small-insert inlining so the rows
    land as real parquet — needed by the byte-reclamation assertions.
    """
    tbl = ducklake_table_name(src, table_name=table_name)
    con = _lake_con(src)
    try:
        if not inline:
            con.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        con.execute(f'CREATE TABLE lake."{tbl}" ({columns})')
        placeholders = ", ".join(["?"] * len(rows[0]))
        for row in rows:
            con.execute(f'INSERT INTO lake."{tbl}" VALUES ({placeholders})', list(row))
    finally:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        con.close()
    return tbl


def _read(src: dict, table_name: str = "logs", cols: str = "ip"):
    tbl = ducklake_table_name(src, table_name=table_name)
    con = get_connection(src, read_only=True)
    try:
        try:
            con.execute("DETACH lake")
        except Exception:
            pass
        _ducklake_attach(con, src, read_only=True)
        return [r[0] for r in con.execute(f'SELECT {cols} FROM lake."{tbl}" ORDER BY 1').fetchall()]
    finally:
        con.close()


def _age_catalog(src: dict, days: int, *, schedule_too: bool = False) -> None:
    """Backdate the catalog's snapshot log so ``older_than`` has work to do.

    Under Postgres, DuckLake catalog tables live in the isolated Postgres schema.
    Updating ``ducklake_snapshot.snapshot_time`` directly exercises the real
    ``ducklake_expire_snapshots`` cutoff without sleeping for a day.
    """
    from backend.core.metadata import pg_connection

    con = pg_connection.get_pg_thread_connection()
    con.execute(f"UPDATE ducklake_snapshot SET snapshot_time = snapshot_time - INTERVAL '{int(days)} days'")
    if schedule_too:
        con.execute(
            f"UPDATE ducklake_files_scheduled_for_deletion SET schedule_start = schedule_start - INTERVAL '{int(days)} days'"
        )
    con.commit()


def _parquet_files(src: dict) -> list[str]:
    root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "ducklake")
    files = glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)
    if not files:
        legacy_root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "parquet")
        files = glob.glob(os.path.join(legacy_root, "**", "*.parquet"), recursive=True)
    return files


def _logs_cols() -> str:
    return "timestamp TIMESTAMPTZ, ip VARCHAR"


def _logs_cols_with_rum() -> str:
    return "timestamp TIMESTAMPTZ, ip VARCHAR, rum_cid VARCHAR"


class TestCloudMaintenanceDucklake:
    @pytest.fixture(autouse=True)
    def _isolated_local_catalog(self, monkeypatch):
        """Use the isolated worker Postgres DuckLake catalog."""
        pass

    def test_retention_deletes_old_rows_and_keeps_newer_ones(self, tmp_path, monkeypatch):
        """The load-bearing assertion for the whole change: the DELETE must be
        bounded by the timestamp predicate. A too-broad delete here destroys
        customer data irreversibly."""
        src = _make_source(tmp_path, f"ret{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols(),
            [
                (NOW - timedelta(days=45), "old-45"),
                (NOW - timedelta(days=31), "old-31"),
                (NOW - timedelta(days=29), "keep-29"),
                (NOW - timedelta(hours=1), "keep-1h"),
            ],
        )
        _set_knobs(monkeypatch, data_retention_days=30, cache_retention_days=0, rollup_retention_months=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert result["data_deleted_before_days"] == 30
        assert result["data_rows_deleted"] == 2
        assert _read(src) == ["keep-1h", "keep-29"], (
            "rows NEWER than the retention cutoff must survive — a delete that took them "
            "too would be irreversible customer-data loss"
        )

    def test_data_retention_zero_deletes_nothing(self, tmp_path, monkeypatch):
        """``0`` means keep forever. Getting this backwards wipes the dataset."""
        src = _make_source(tmp_path, f"zero{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols(),
            [(NOW - timedelta(days=900), "ancient"), (NOW - timedelta(days=1), "fresh")],
        )
        _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deleted_before_days" not in result
        assert "data_deletion_error" not in result, result
        assert _read(src) == ["ancient", "fresh"], "retention_days=0 must delete NOTHING"

    def test_data_retention_zero_is_not_capped_by_rum_retention(self, tmp_path, monkeypatch):
        """``data_retention_days=0`` (forever) + ``rum_retention_days=30``.

        The pyiceberg original took the ``rum_retention_days > data_retention_days``
        branch here, resolving the cutoff to "now" and deleting every row with a
        NULL ``rum_cid`` — plus a ceiling pass at 30 days over everything. It never
        fired because the function was dead against DuckLake data. Now that it
        runs, "0 == forever" has to hold.
        """
        src = _make_source(tmp_path, f"fvr{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols_with_rum(),
            [
                (NOW - timedelta(days=400), "ancient-no-rum", None),
                (NOW - timedelta(days=400), "ancient-rum", "cid-a"),
                (NOW - timedelta(minutes=5), "fresh", None),
            ],
        )
        _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=30, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert _read(src) == ["ancient-no-rum", "ancient-rum", "fresh"], (
            "with log retention disabled the RUM knob must not delete log rows"
        )

    def test_rum_retention_longer_than_data_retention_keeps_rum_correlated_rows(self, tmp_path, monkeypatch):
        """The conditional-prune split: at the data cutoff only rows with no
        ``rum_cid`` go; the rum_cid rows survive until the RUM ceiling."""
        src = _make_source(tmp_path, f"split{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols_with_rum(),
            [
                (NOW - timedelta(days=100), "old-beyond-ceiling", "cid-x"),
                (NOW - timedelta(days=45), "mid-no-rum", None),
                (NOW - timedelta(days=45), "mid-rum", "cid-y"),
                (NOW - timedelta(days=5), "fresh-no-rum", None),
            ],
        )
        _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=90, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert result["data_deleted_before_days"] == 30
        assert result["rum_deleted_before_days"] == 90
        assert _read(src) == ["fresh-no-rum", "mid-rum"], (
            "the 45-day rum_cid row must outlive the 30-day cutoff; the 100-day one must not outlive the 90-day RUM ceiling"
        )

    def test_missing_rum_cid_column_falls_back_to_flat_delete(self, tmp_path, monkeypatch):
        """A service whose schema predates the RUM fields (rum_enabled=False —
        the live default) must still get retention. Silently skipping the step
        is the compliance failure this whole change fixes."""
        src = _make_source(tmp_path, f"norum{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols(),  # no rum_cid column at all
            [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=2), "new")],
        )
        _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=90, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert result["data_rows_deleted"] == 1, "retention must not silently no-op on a pre-RUM schema"
        assert _read(src) == ["new"]

    def test_rum_retention_prunes_the_beacon_tables(self, tmp_path, monkeypatch):
        """RUM telemetry lives in its OWN DuckLake tables keyed by ``cid`` — the
        pyiceberg version only ever touched ``logs``, so ``rum_retention_days``
        never reached the beacon data at all."""
        src = _make_source(tmp_path, f"beacon{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=2), "log-fresh")])
        _seed(
            src,
            "client_vitals",
            "timestamp TIMESTAMPTZ, cid VARCHAR",
            [(NOW - timedelta(days=200), "vital-old"), (NOW - timedelta(days=3), "vital-new")],
        )
        _seed(
            src,
            "client_errors",
            "timestamp TIMESTAMPTZ, cid VARCHAR",
            [(NOW - timedelta(days=200), "err-old"), (NOW - timedelta(days=3), "err-new")],
        )
        _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=90, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert result["rum_beacon_rows_deleted"] == 2
        assert _read(src, "client_vitals", cols="cid") == ["vital-new"]
        assert _read(src, "client_errors", cols="cid") == ["err-new"]

    def test_rum_retention_zero_leaves_beacon_tables_alone(self, tmp_path, monkeypatch):
        src = _make_source(tmp_path, f"bzero{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=2), "log-fresh")])
        _seed(
            src,
            "client_vitals",
            "timestamp TIMESTAMPTZ, cid VARCHAR",
            [(NOW - timedelta(days=900), "vital-ancient")],
        )
        _set_knobs(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "rum_beacon_rows_deleted" not in result
        assert _read(src, "client_vitals", cols="cid") == ["vital-ancient"]

    def test_absent_beacon_tables_are_not_an_error(self, tmp_path, monkeypatch):
        """RUM was never provisioned (no client_vitals/client_errors tables) —
        the step must skip them, not record a data_deletion_error."""
        src = _make_source(tmp_path, f"nobeacon{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=1), "new")])
        _set_knobs(monkeypatch, data_retention_days=30, rum_retention_days=30, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" not in result, result
        assert "rum_beacon_rows_deleted" not in result
        assert _read(src) == ["new"]

    def test_missing_logs_table_records_error_without_raising(self, tmp_path, monkeypatch):
        """Defensive posture: a failing step records its own ``*_error`` key (which
        the cron wrapper turns into a ``warning`` run) and lets the others run.
        Nothing may propagate out of the function."""
        src = _make_source(tmp_path, f"nolog{uuid.uuid4().hex[:8]}")
        _set_knobs(monkeypatch, data_retention_days=30, cache_retention_days=0)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_deletion_error" in result
        assert "snapshots_before" in result, "expiry must still run after a retention failure"

    def test_unsafe_table_name_never_reaches_sql(self):
        """Trap #4: the interpolated identifier is validated at the point of use."""
        with pytest.raises(ValueError, match="unsafe ducklake table name"):
            buffer_mod._lake_ident('logs"; DROP TABLE x; --')

    def test_snapshot_expiry_reports_honest_before_after_counts(self, tmp_path, monkeypatch):
        src = _make_source(tmp_path, f"exp{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=1), f"r{i}") for i in range(4)])
        _set_knobs(
            monkeypatch, data_retention_days=0, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0
        )
        _age_catalog(src, 30)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "snapshot_expiry_error" not in result, result
        before = result["snapshots_before"]
        after = result["snapshots_after"]
        assert before > 0
        assert result["snapshots_expired_before_days"] == 1
        assert after < before, f"aged snapshots must actually be expired ({before} -> {after})"
        assert result["snapshots_expired_count"] == before - after, (
            "the reported count must be the real before/after delta, not an assumption"
        )
        # Expiry is metadata-only; the rows themselves are untouched.
        assert len(_read(src)) == 4

    def test_snapshot_expiry_alone_reclaims_no_bytes(self, tmp_path, monkeypatch):
        """Verified against the real extension: ``ducklake_expire_snapshots``
        reclaims NO bytes on its own, and the retention DELETE leaves the rows
        physically in place behind delete files. The note must say exactly that
        rather than implying storage came back."""
        src = _make_source(tmp_path, f"note{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols(),
            [(NOW - timedelta(days=40 + i), f"old{i}") for i in range(3)] + [(NOW - timedelta(days=1), "fresh")],
            inline=False,
        )
        _set_knobs(
            monkeypatch, data_retention_days=30, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0
        )
        files_before = _parquet_files(src)
        assert files_before, "premise: the seeded rows must be real parquet, not inlined"
        _age_catalog(src, 30)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert "snapshot_expiry_error" not in result, result
        assert result["snapshots_expired_count"] > 0
        note = result["snapshot_expiry_note"]
        assert "not deleted by the expiry itself" in note, note
        assert "ducklake_cleanup_old_files" in note, note
        assert len(_parquet_files(src)) == len(files_before), (
            "expiry must not be reported as reclaiming storage — the parquet is still on disk"
        )
        assert _read(src) == ["fresh"], "retention still deleted the rows logically"

    def test_cleanup_pass_unlinks_files_a_later_run_can_reclaim(self, tmp_path, monkeypatch):
        """The byte-reclamation half of the story. A file is queued for deletion at
        expiry time, so with ``older_than=keep_snapshot_days`` it is always a LATER
        run that unlinks it — which is why the cleanup sweep must not be gated on
        "this run expired something"."""
        src = _make_source(tmp_path, f"reclaim{uuid.uuid4().hex[:8]}")
        _seed(
            src,
            "logs",
            _logs_cols(),
            [(NOW - timedelta(days=40 + i), f"old{i}") for i in range(3)] + [(NOW - timedelta(days=1), "fresh")],
            inline=False,
        )
        _set_knobs(
            monkeypatch, data_retention_days=30, rum_retention_days=0, keep_snapshot_days=1, cache_retention_days=0
        )

        # A snapshot that still anchors a live data file cannot be expired, so let
        # the optimize path supersede the seed files first (as the daily job does).
        assert "error" not in buffer_mod._optimize_table_impl(src)
        files_before = _parquet_files(src)
        _age_catalog(src, 30)

        first = buffer_mod._run_cloud_maintenance_impl(src)
        assert first["snapshots_expired_count"] > 0
        assert first["data_files_cleaned"] == 0, "nothing is old enough to unlink on the run that queued it"

        # A week passes: the queued files age past the cutoff.
        _age_catalog(src, 30, schedule_too=True)
        second = buffer_mod._run_cloud_maintenance_impl(src)

        assert "data_file_cleanup_error" not in second, second
        assert second["data_files_cleaned"] >= 1, (
            "the queued files must eventually be unlinked — otherwise retention never reclaims a byte"
        )
        assert len(_parquet_files(src)) < len(files_before)
        assert _read(src) == ["fresh"], "reclamation must not touch surviving rows"

    def test_expire_does_not_invalidate_view_caches(self, tmp_path):
        """Expiry drops OLD snapshot rows; the current snapshot's file membership
        is unchanged, so it must NOT bust the view / snapshot-file caches.

        Exercised through the step helper rather than the whole job because
        ``get_connection`` legitimately repopulates ``_view_cache`` on its way in,
        which would mask the thing being pinned.
        """
        src = _make_source(tmp_path, f"cache{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=1), "r0")])
        _age_catalog(src, 30)

        from backend.core import iceberg as _ice

        con = _lake_con(src)
        _ice._view_cache[src["name"]] = ("sentinel-too",)
        try:
            out = buffer_mod._ducklake_expire_snapshots(con, src, 1)
            assert out["snapshots_expired_count"] > 0, "premise: the expiry must have done real work"
            assert _ice._view_cache.get(src["name"]) == ("sentinel-too",)
        finally:
            _ice._view_cache.pop(src["name"], None)
            con.close()

    def test_retention_delete_does_invalidate_view_caches(self, tmp_path):
        """Contrast with the above: a retention delete DOES change file membership,
        so the caches for this service (and its sub-table keys) must be dropped."""
        src = _make_source(tmp_path, f"inval{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(days=45), "old"), (NOW - timedelta(days=1), "new")])

        from backend.core import iceberg as _ice

        con = _lake_con(src)
        _ice._view_cache[src["name"]] = ("stale",)
        _ice._view_cache[f"{src['name']}::client_vitals"] = ("stale-sub",)
        try:
            out = buffer_mod._ducklake_retention_delete(con, src, 30, 0)
            assert out["data_rows_deleted"] == 1
            assert src["name"] not in _ice._view_cache
            assert f"{src['name']}::client_vitals" not in _ice._view_cache
        finally:
            _ice._view_cache.pop(src["name"], None)
            _ice._view_cache.pop(f"{src['name']}::client_vitals", None)
            con.close()

    def test_config_load_failure_returns_error_without_raising(self, tmp_path, monkeypatch):
        """A total failure returns ``{"error": ...}`` (the cron wrapper's
        ``status='error'`` path) rather than propagating."""
        src = _make_source(tmp_path, f"cfg{uuid.uuid4().hex[:8]}")

        def _boom(_sid):
            raise RuntimeError("config unreadable")

        monkeypatch.setattr(svcconfig, "load_config", _boom)

        result = buffer_mod._run_cloud_maintenance_impl(src)

        assert result == {"error": "config unreadable"}

    def test_optimize_table_flushes_inlined_rows_to_parquet(self, tmp_path, monkeypatch):
        """DuckLake inlines small commits into the metadata catalog, and NEITHER
        ``ducklake_rewrite_data_files`` NOR ``ducklake_merge_adjacent_files``
        promotes them — both only touch already-materialized files. Without an
        explicit ``ducklake_flush_inlined_data`` the table stays at file_count=0
        forever and the only copy of the data lives inside the catalog DB, with
        the raw .gz already deleted.
        """
        src = _make_source(tmp_path, f"flush{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW - timedelta(hours=i), f"r{i}") for i in range(5)])

        data_root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "ducklake")
        legacy_root = os.path.join(str(svcconfig.SERVICES_DATA_DIR), src["service_id"], "parquet")

        result = buffer_mod._optimize_table_impl(src)

        assert "error" not in result
        files = glob.glob(os.path.join(data_root, "**", "*.parquet"), recursive=True) or glob.glob(
            os.path.join(legacy_root, "**", "*.parquet"), recursive=True
        )
        assert files, "parquet files must exist — otherwise the catalog DB holds the only copy of every ingested row"
        assert _read(src) == [f"r{i}" for i in range(5)], "the flush must be lossless"

    def test_ducklake_write_connection_file_mode(self, tmp_path):
        """Under file mode, _ducklake_write_connection attaches read-write,
        executes statements, and restores read-only mode upon exit."""
        src = _make_source(tmp_path, f"wrconn{uuid.uuid4().hex[:8]}")
        _seed(src, "logs", _logs_cols(), [(NOW, "seed_row")])

        table = f'lake."{ducklake_table_name(src)}"'
        with buffer_mod._ducklake_write_connection(src) as con:
            con.execute(f"INSERT INTO {table} (timestamp, ip) VALUES ('2026-09-21 00:00:00', '1.1.1.1')")

        # After exiting, a normal connection should be able to connect and read
        con_check = get_connection(src, read_only=True)
        rows = con_check.execute(f"SELECT ip FROM {table}").fetchall()
        con_check.close()
        assert {r[0] for r in rows} == {"seed_row", "1.1.1.1"}

    def test_ducklake_write_connection_postgres_mode(self, tmp_path, monkeypatch):
        """When DUCKLAKE_CATALOG points to Postgres, get_memory_connection is used."""
        import backend.core.iceberg._ducklake as _ducklake_mod

        src = _make_source(tmp_path, f"pgconn{uuid.uuid4().hex[:8]}")
        monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgres://user:pass@localhost:5432/db")

        called_mem = []

        def fake_get_memory_connection(s):
            called_mem.append(s)
            import duckdb

            return duckdb.connect(":memory:")

        monkeypatch.setattr("backend.core.duckdb.get_memory_connection", fake_get_memory_connection)
        monkeypatch.setattr(_ducklake_mod, "_ducklake_attach", lambda con, s, read_only=False: True)

        with buffer_mod._ducklake_write_connection(src) as con:
            assert con is not None
            assert len(called_mem) == 1
