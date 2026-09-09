from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.core import duckdb as db
from backend.core.iceberg import view


def _durable_source(tmp_path):
    return {
        "name": "durable-svc",
        "service_id": "durable-svc",
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
        "deployment_mode": "high_throughput",
        "bucket": "bucket",
        "prefix": "logs",
    }


def test_durable_serving_uses_ephemeral_connection_and_never_opens_native_file(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
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


def test_durable_serving_rejects_writable_connection(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", "postgresql://pg/ducklake")

    with pytest.raises(RuntimeError, match="only permits read-only"):
        db.get_connection(source=_durable_source(tmp_path), read_only=False)


def test_sync_file_mode_keeps_native_service_path(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    source = _durable_source(tmp_path)
    source["deployment_mode"] = "standard"

    assert db.db_path_for_source(source) == str(tmp_path / "must-not-open.duckdb")


def test_durable_serving_holder_skips_file_backed_pool(monkeypatch, tmp_path):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
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
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
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
