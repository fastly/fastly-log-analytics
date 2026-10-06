from __future__ import annotations

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


def test_safe_connection_uses_same_bundled_config_as_service_connections(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKLAKE_EXTENSION_PATH", "/opt/ducklake/ducklake.duckdb_extension")
    path = str(tmp_path / "service.duckdb")
    with duckdb_core._duckdb_connect(path), duckdb_core.get_safe_duckdb_connection(path) as con:
        assert con.execute("SELECT current_setting('allow_unsigned_extensions')").fetchone() == (True,)


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
