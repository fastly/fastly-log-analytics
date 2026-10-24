"""Per-service DuckLake writer admission tests."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from backend.core.ducklake_admission import (
    DuckLakeAdmissionTimeout,
    _reset_for_tests,
    ducklake_write_admission,
    get_admission_stats,
)


@pytest.fixture(autouse=True)
def reset_admission_state():
    _reset_for_tests()
    yield
    _reset_for_tests()


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


def test_admission_stats_capture_wait_and_hold_time():
    with ducklake_write_admission("admission-stats"):
        time.sleep(0.005)

    stats = get_admission_stats()["admission-stats"]
    assert stats["acquisitions"] == 1
    assert stats["timeouts"] == 0
    assert stats["wait_ms_total"] >= 0
    assert stats["hold_ms_total"] >= 5


# ── Merged from test_ducklake_extension_load.py ──

from unittest.mock import Mock

from backend.core import duckdb as duckdb_core
from backend.core.iceberg import _ducklake as ducklake


def test_duckdb_connect_allows_bundled_unsigned_extension(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    result = duckdb_core._duckdb_connect(":memory:")

    assert result is connect.return_value
    connect.assert_called_once_with(":memory:", config={"allow_unsigned_extensions": "true"})


def test_duckdb_connect_keeps_default_config_without_bundled_extension(monkeypatch):
    monkeypatch.delenv("DUCKLAKE_EXTENSION_PATH", raising=False)
    connect = Mock(return_value=object())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    duckdb_core._duckdb_connect(":memory:")

    connect.assert_called_once_with(":memory:")


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


def test_memory_connection_uses_bundled_extension_config(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    monkeypatch.setenv("DUCKDB_TEMP_DIRECTORY", str(tmp_path))
    connect = Mock(return_value=Mock())
    monkeypatch.setattr(duckdb_core.duckdb, "connect", connect)

    duckdb_core.get_memory_connection()

    connect.assert_called_once_with(":memory:", config={"allow_unsigned_extensions": "true"})


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


# ── Merged from test_durable_serving.py ──

from unittest.mock import MagicMock

import pytest

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core.iceberg import view


def _durable_source(tmp_path):
    return {
        "name": "durable-svc",
        "service_id": "durable-svc",
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
        "deployment_mode": "high_scale",
        "bucket": "bucket",
        "prefix": "logs",
    }


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


def test_durable_serving_rejects_writable_connection(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")

    with pytest.raises(RuntimeError, match="only permits read-only"):
        db.get_connection(source=_durable_source(tmp_path), read_only=False)


def test_sync_file_mode_keeps_native_service_path(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    source = _durable_source(tmp_path)
    source["deployment_mode"] = "standard"

    assert db.db_path_for_source(source) == str(tmp_path / "must-not-open.duckdb")


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
