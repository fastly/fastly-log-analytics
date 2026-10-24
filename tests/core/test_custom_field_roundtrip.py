"""Custom-field full-roundtrip integration tests.

Audit finding: no single test exercises the full custom-field roundtrip —
API add → /log-fields → ingest → DuckDB view → SELECT, plus the same
roundtrip across a disable/re-enable cycle. Field-id stability is pinned
in test_custom_field_lifecycle.py but never verified end-to-end through
ingest. Pattern mirrors test_integration_custom_fields.py (direct router
calls with a mocked admin Request).
"""

from __future__ import annotations

import gzip
import io
import json
import os
from types import SimpleNamespace


def _build_source(tmp_path, svc_id):
    return {
        "name": svc_id,
        "duckdb_path": str(tmp_path / f"{svc_id}.db"),
        "bucket": "mock_bucket",
        "prefix": "",
        "endpoint": "mock_endpoint",
        "region": "mock_region",
    }


def _admin_request():
    # No ``analyst_session`` on request.state ⇒ admin path through the gate.
    return SimpleNamespace(state=SimpleNamespace())


def _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir):
    """Wire DuckDB/iceberg/ingest module globals to the test sandbox.
    See test_integration_custom_fields._S3RewritingConn for the rewrite rationale —
    DuckDBPyConnection.execute is C-level read-only so we wrap, not patch."""
    import backend.core.duckdb as my_duckdb
    import backend.core.ingest as ingest
    from backend import config

    monkeypatch.setattr(my_duckdb, "DUCKDB_PATH", source["duckdb_path"])
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(my_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(ingest, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(my_duckdb, "get_source_for_service", lambda x: None)
    monkeypatch.setattr(os, "remove", lambda *a: None)

    bucket_prefix = f"s3://{source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig = my_duckdb.get_memory_connection

    class _Rewriter:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *a, **k):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    monkeypatch.setattr(my_duckdb, "get_memory_connection", lambda s: _Rewriter(_orig(s)))
    monkeypatch.setattr(ingest, "get_memory_connection", lambda s: _Rewriter(_orig(s)), raising=False)


def _ingest_once(monkeypatch, source, log_dir, key, payload, *, include_commit: bool = False):
    """Seed one gzipped JSON log line + wire mock FOS + run ingest + clear caches."""
    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg
    import backend.core.ingest as ingest

    log_file = log_dir / key
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(log_file, "wt") as f:
        f.write(json.dumps(payload) + "\n")

    class _Fos:
        def get_paginator(self, *a, **k):
            return type("P", (), {"paginate": lambda self, **kw: [{"Contents": [{"Key": key, "Size": 100}]}]})()

        def get_object(self, Bucket, Key):
            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **k):
            return {}

    monkeypatch.setattr(ingest, "_get_fos_client", lambda *a: _Fos())
    iceberg._view_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()
    list(ingest.ingest(source=source))
    if include_commit:
        iceberg.commit_buffer(source)
    iceberg._view_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()


def _open_view(source):
    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg

    con = my_duckdb.get_connection(source=source, skip_view_update=False)
    iceberg._view_cache.clear()
    iceberg.update_iceberg_view(con, source)
    return con


def test_custom_field_full_roundtrip_end_to_end(tmp_path, monkeypatch):
    """API add → /log-fields shows it → ingest → view exposes column → SELECT returns value."""
    import backend.core.duckdb as my_duckdb
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate
    from backend.routers import services
    from backend.routers.services import core as services_core

    svc_id = "roundtrip_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_rt"

    # Patch CONFIGS_DIR BEFORE save_config — save_config writes to the
    # current CONFIGS_DIR at call time, and the route handler reads from
    # the same; if we save first then patch, the handler 404s.
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {"service_id": svc_id, "status": {}, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}},
    )
    my_duckdb._ensure_source_registered(source)

    # 1. POST custom-fields
    cf = CustomFieldCreate(
        name="my_field",
        label="My Field",
        vcl_log_expression="%{req.http.X-My-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)

    # 2. GET /custom-fields ⇒ my_field present with type VARCHAR
    custom = services_core.api_list_custom_fields(_admin_request(), svc_id).fields
    assert len(custom) == 1 and custom[0].name == "my_field" and custom[0].duckdb_type == "VARCHAR"

    # 3-4. Seed + ingest
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.test.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "my_field": "hello"},
    )

    # 5-6. View has the column AND SELECT returns the value
    con = _open_view(source)
    table_name = my_duckdb._safe_table_name(svc_id)
    cols = {c["name"] for c in my_duckdb.get_schema(con, source)}
    assert "my_field" in cols, f"Expected 'my_field' in view columns, got {sorted(cols)}"
    assert con.execute(f"SELECT my_field FROM {table_name}").fetchall() == [("hello",)]


def test_custom_field_full_roundtrip_after_disable_enable_cycle(tmp_path, monkeypatch):
    """Add → ingest v1 → disable → re-enable → ingest v2 → both rows readable.
    Field-id stability across a disable cycle is pinned by
    test_reenabled_field_gets_same_iceberg_field_id in test_custom_field_lifecycle.py;
    this is the end-to-end ingest-path cross-check."""
    import backend.core.duckdb as my_duckdb
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate, CustomFieldUpdate
    from backend.routers import services
    from backend.routers.services import core as services_core

    svc_id = "roundtrip_cycle_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_cycle"

    # Patch CONFIGS_DIR BEFORE save_config — save_config writes to the
    # current CONFIGS_DIR at call time, and the route handler reads from
    # the same; if we save first then patch, the handler 404s.
    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {"service_id": svc_id, "status": {}, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}},
    )
    my_duckdb._ensure_source_registered(source)

    cf = CustomFieldCreate(
        name="cycle_field",
        label="Cycle Field",
        vcl_log_expression="%{req.http.X-Cycle-Field}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    services.api_create_custom_field(_admin_request(), svc_id, cf)
    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/12/2026-05-10T12-00-00.v1.gz",
        {"timestamp": "2026-05-10T12:00:00Z", "ip": "1.2.3.4", "status": 200, "cycle_field": "v1"},
    )

    # Disable
    services_core.api_update_custom_field(_admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=False))
    assert (
        next(cf for cf in config.load_config(svc_id)["log_fields"]["custom_fields"] if cf["name"] == "cycle_field")[
            "enabled"
        ]
        is False
    )

    # Re-enable
    services_core.api_update_custom_field(_admin_request(), svc_id, "cycle_field", CustomFieldUpdate(enabled=True))
    assert (
        next(cf for cf in config.load_config(svc_id)["log_fields"]["custom_fields"] if cf["name"] == "cycle_field")[
            "enabled"
        ]
        is True
    )

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-10/13/2026-05-10T13-00-00.v2.gz",
        {"timestamp": "2026-05-10T13:00:00Z", "ip": "1.2.3.5", "status": 200, "cycle_field": "v2"},
    )

    # Both v1 and v2 readable through the post-cycle view
    con = _open_view(source)
    table_name = my_duckdb._safe_table_name(svc_id)
    values = [r[0] for r in con.execute(f"SELECT cycle_field FROM {table_name} ORDER BY timestamp").fetchall()]
    assert values == ["v1", "v2"], f"Expected both v1 and v2 readable after disable/re-enable, got {values}"


def test_ingest_and_query_custom_fields(tmp_path, monkeypatch):
    """Test full lifecycle of a custom field: Config -> Ingest -> DuckLake Commit -> Query."""
    import backend.core.duckdb as my_duckdb
    from backend import config

    svc_id = "custom_test_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_int"

    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": [
                    {
                        "name": "x_custom_id",
                        "label": "Custom ID",
                        "vcl_log_expression": "%{req.http.X-Custom-ID}V",
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "bytes_estimate": 36,
                        "enabled": True,
                    }
                ],
            },
        },
    )
    my_duckdb._ensure_source_registered(source)

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-test.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "x_custom_id": "abc-123-xyz",
        },
        include_commit=True,
    )

    con = _open_view(source)
    schema = my_duckdb.get_schema(con, source)
    col_names = [c["name"] for c in schema]
    assert "x_custom_id" in col_names, f"Custom field missing from schema: {col_names}"

    table_name = my_duckdb._safe_table_name(svc_id)
    res = con.execute(f"SELECT x_custom_id FROM {table_name}").fetchall()
    assert res == [("abc-123-xyz",)]


def test_ingest_and_query_numeric_custom_fields(tmp_path, monkeypatch):
    """Test full lifecycle of numeric custom fields (BIGINT and DOUBLE)."""
    import backend.core.duckdb as my_duckdb
    from backend import config

    svc_id = "numeric_custom_svc"
    source = _build_source(tmp_path, svc_id)
    log_dir = tmp_path / "mock_logs_num"

    _install_pipeline_mocks(monkeypatch, tmp_path, source, log_dir)
    config.save_config(
        svc_id,
        {
            "service_id": svc_id,
            "status": {},
            "log_fields": {
                "schema_version": 2,
                "groups": ["A"],
                "custom_fields": [
                    {
                        "name": "custom_int",
                        "label": "Custom Int",
                        "vcl_log_expression": "%{req.http.X-Int}V",
                        "duckdb_type": "BIGINT",
                        "value_type": "integer",
                        "bytes_estimate": 8,
                        "enabled": True,
                    },
                    {
                        "name": "custom_float",
                        "label": "Custom Float",
                        "vcl_log_expression": "%{req.http.X-Float}V",
                        "duckdb_type": "DOUBLE",
                        "value_type": "float",
                        "bytes_estimate": 10,
                        "enabled": True,
                    },
                ],
            },
        },
    )
    my_duckdb._ensure_source_registered(source)

    _ingest_once(
        monkeypatch,
        source,
        log_dir,
        "raw/2026-05-01/00/2026-05-01T00-00-00.000-num.log.gz",
        {
            "timestamp": "2026-05-01T00:00:00Z",
            "ip": "1.2.3.4",
            "status": 200,
            "url": "/test",
            "method": "GET",
            "cache": "HIT",
            "resp_bytes": 100,
            "elapsed": 5000,
            "custom_int": 42,
            "custom_float": 3.14159,
        },
        include_commit=True,
    )

    con = _open_view(source)
    schema = my_duckdb.get_schema(con, source)
    schema_dict = {c["name"]: c["type"] for c in schema}
    assert schema_dict.get("custom_int") == "BIGINT"
    assert schema_dict.get("custom_float") == "DOUBLE"

    table_name = my_duckdb._safe_table_name(svc_id)
    res = con.execute(f"SELECT custom_int * 2, ROUND(custom_float, 2) FROM {table_name}").fetchall()
    assert res == [(84, 3.14)]
