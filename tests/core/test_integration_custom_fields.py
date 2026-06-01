def _build_source(tmp_path, svc_id):
    return {
        "name": svc_id,
        "duckdb_path": str(tmp_path / f"{svc_id}.db"),
        "bucket": "mock_bucket",
        "prefix": "",
        "endpoint": "mock_endpoint",
        "region": "mock_region",
    }


def test_ingest_and_query_custom_fields(tmp_path, monkeypatch):
    import gzip
    import json
    import os

    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg
    import backend.core.ingest as ingest
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate
    from backend.routers import services

    test_service_source = {"name": "test_service"}
    svc_id = test_service_source["name"]
    db_path = str(tmp_path / f"{svc_id}.db")
    test_service_source["duckdb_path"] = db_path
    test_service_source["bucket"] = "mock_bucket"
    test_service_source["prefix"] = ""
    test_service_source["endpoint"] = "mock_endpoint"
    test_service_source["region"] = "mock_region"

    monkeypatch.setattr(my_duckdb, "DUCKDB_PATH", db_path)
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(my_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(ingest, "_configure_fos", lambda *a: None)

    def mock_get_catalog(*args, **kwargs):
        raise RuntimeError("No catalog in integration tests")

    monkeypatch.setattr(iceberg, "_get_catalog", mock_get_catalog)

    cfg = {"service_id": svc_id, "status": {}}
    cfg["log_fields"] = {"schema_version": 2, "groups": ["A"], "custom_fields": []}
    config.save_config(svc_id, cfg)

    my_duckdb._ensure_source_registered(test_service_source)

    cf_data = CustomFieldCreate(
        name="my_custom_domain",
        label="My Custom Domain",
        vcl_log_expression="%{req.http.Host}V",
        duckdb_type="VARCHAR",
        value_type="string",
        bytes_estimate=20,
    )
    monkeypatch.setattr(my_duckdb, "get_source_for_service", lambda x: None)
    services.api_create_custom_field(svc_id, cf_data)

    updated_cfg = config.load_config(svc_id)
    assert len(updated_cfg["log_fields"]["custom_fields"]) == 1
    # Custom field is persisted to the config JSON, which ingest reads via svcconfig.load_config().
    # No DuckDB-side _sources mirror is needed any more.

    log_dir = tmp_path / "mock_logs"
    log_file = log_dir / "raw/2026-05-08/12/2026-05-08T12-00-00.test_log.gz"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    log_data = {
        "timestamp": "2026-05-08T12:00:00Z",
        "ip": "1.2.3.4",
        "status": 200,
        "my_custom_domain": "example.com",
    }
    with gzip.open(log_file, "wt") as f:
        f.write(json.dumps(log_data) + "\n")

    class MockPaginator:
        def paginate(self, **kwargs):
            return [{"Contents": [{"Key": str(log_file.relative_to(log_dir)), "Size": 100}]}]

    class MockFos:
        def get_paginator(self, *args, **kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):
            # Ingest now pre-downloads via boto3.get_object before passing
            # local paths to DuckDB. Serve from log_dir so tests don't need
            # a real S3 endpoint.
            import io

            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            return {}

    monkeypatch.setattr(ingest, "_get_fos_client", lambda *a: MockFos())
    monkeypatch.setattr(os, "remove", lambda *a: None)

    # Production ingest hands s3://bucket/... to DuckDB and the `fos_proxy`
    # SECRET routes reads through the telemetry proxy. In tests there's no
    # proxy, so wrap `get_memory_connection` so its returned conn rewrites
    # s3://mock_bucket/... → file://localdir/... at execute time. This is the
    # test-only equivalent of the (production-removed) cdn_url rewrite.
    bucket_prefix = f"s3://{test_service_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig_get_mem = my_duckdb.get_memory_connection

    class _S3RewritingConn:
        # DuckDBPyConnection's `execute` is C-level read-only, so we can't
        # monkeypatch it directly. Wrap the connection and proxy attribute
        # access — only `execute` gets the s3://→file:// rewrite.
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def _wrap_mem(src):
        return _S3RewritingConn(_orig_get_mem(src))

    monkeypatch.setattr(my_duckdb, "get_memory_connection", _wrap_mem)
    monkeypatch.setattr(ingest, "get_memory_connection", _wrap_mem, raising=False)

    iceberg._view_cache.clear()
    iceberg._snapshot_files_cache.clear()
    iceberg._table_object_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()

    list(ingest.ingest(source=test_service_source))

    iceberg._view_cache.clear()
    iceberg._snapshot_files_cache.clear()
    iceberg._table_object_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()

    con = my_duckdb.get_connection(source=test_service_source, skip_view_update=False)
    iceberg._view_cache.clear()
    iceberg.update_iceberg_view(con, my_duckdb.get_source_by_name(con, svc_id) or test_service_source)

    res = con.execute(f"SELECT my_custom_domain FROM {my_duckdb._safe_table_name(svc_id)}").fetchall()

    assert len(res) == 1
    assert res[0][0] == "example.com"


def test_ingest_and_query_numeric_custom_fields(tmp_path, monkeypatch):
    """BIGINT and DOUBLE custom fields are stored and retrieved with correct types, not as VARCHAR."""
    import gzip
    import json
    import os

    import pyarrow as pa

    import backend.core.duckdb as my_duckdb
    import backend.core.iceberg as iceberg
    import backend.core.ingest as ingest
    from backend import config
    from backend.models.custom_fields import CustomFieldCreate
    from backend.routers import services

    svc_id = "test_numeric_service"
    test_source = _build_source(tmp_path, svc_id)

    monkeypatch.setattr(my_duckdb, "DUCKDB_PATH", test_source["duckdb_path"])
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    monkeypatch.setattr(my_duckdb, "_cache_dir", lambda s: str(tmp_path / "cache" / s.get("name", "unknown")))
    monkeypatch.setattr(my_duckdb, "_configure_fos", lambda *a: None)
    monkeypatch.setattr(ingest, "_configure_fos", lambda *a: None)

    def mock_get_catalog(*args, **kwargs):
        raise RuntimeError("No catalog in integration tests")

    monkeypatch.setattr(iceberg, "_get_catalog", mock_get_catalog)

    cfg = {
        "service_id": svc_id,
        "status": {},
        "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []},
    }
    config.save_config(svc_id, cfg)
    my_duckdb._ensure_source_registered(test_source)

    monkeypatch.setattr(my_duckdb, "get_source_for_service", lambda x: None)
    for name, dtype, vtype in [
        ("req_size_bytes", "BIGINT", "numeric"),
        ("response_time_ms", "DOUBLE", "numeric"),
    ]:
        services.api_create_custom_field(
            svc_id,
            CustomFieldCreate(
                name=name,
                label=name,
                vcl_log_expression=f"%{{{name}}}V",
                duckdb_type=dtype,
                value_type=vtype,
                bytes_estimate=10,
            ),
        )

    updated_cfg = config.load_config(svc_id)
    assert len(updated_cfg["log_fields"]["custom_fields"]) == 2
    # Custom field is persisted to the config JSON, which ingest reads via svcconfig.load_config().
    # No DuckDB-side _sources mirror is needed any more.

    log_dir = tmp_path / "mock_logs_numeric"
    log_file = log_dir / "raw/2026-05-09/10/2026-05-09T10-00-00.test.gz"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_data = {
        "timestamp": "2026-05-09T10:00:00Z",
        "ip": "1.2.3.4",
        "status": 200,
        "req_size_bytes": 12345,
        "response_time_ms": 42.7,
    }
    with gzip.open(log_file, "wt") as f:
        f.write(json.dumps(log_data) + "\n")

    class MockPaginator:
        def paginate(self, **kwargs):
            return [{"Contents": [{"Key": str(log_file.relative_to(log_dir)), "Size": 100}]}]

    class MockFos:
        def get_paginator(self, *args, **kwargs):
            return MockPaginator()

        def get_object(self, Bucket, Key):
            # Ingest now pre-downloads via boto3.get_object before passing
            # local paths to DuckDB. Serve from log_dir so tests don't need
            # a real S3 endpoint.
            import io

            with open(log_dir / Key, "rb") as fh:
                return {"Body": io.BytesIO(fh.read())}

        def delete_objects(self, **kwargs):
            return {}

    monkeypatch.setattr(ingest, "_get_fos_client", lambda *a: MockFos())
    monkeypatch.setattr(os, "remove", lambda *a: None)

    # See [test_ingest_and_query_custom_fields] for why this s3://→file://
    # rewrite is wrapped onto the in-memory DuckDB connection at test time.
    bucket_prefix = f"s3://{test_source['bucket']}/"
    file_prefix = "file://" + str(log_dir.absolute()) + "/"
    _orig_get_mem = my_duckdb.get_memory_connection

    class _S3RewritingConn:
        # DuckDBPyConnection's `execute` is C-level read-only, so we can't
        # monkeypatch it directly. Wrap the connection and proxy attribute
        # access — only `execute` gets the s3://→file:// rewrite.
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            if isinstance(sql, str) and bucket_prefix in sql:
                sql = sql.replace(bucket_prefix, file_prefix)
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def _wrap_mem(src):
        return _S3RewritingConn(_orig_get_mem(src))

    monkeypatch.setattr(my_duckdb, "get_memory_connection", _wrap_mem)

    iceberg._view_cache.clear()
    iceberg._snapshot_files_cache.clear()
    iceberg._table_object_cache.clear()
    my_duckdb._clear_schema_cache()
    my_duckdb._initialized_paths.clear()

    list(ingest.ingest(source=test_source))

    # Verify buffer Parquet was written with correct Arrow types (not STRING)
    buf_files = iceberg.buffer_files(test_source)
    assert buf_files, "Expected at least one buffer file after ingest"
    import pyarrow.parquet as pq

    buf_table = pq.read_table(buf_files[0])
    assert pa.types.is_integer(buf_table.schema.field("req_size_bytes").type) or pa.types.is_large_integer(
        buf_table.schema.field("req_size_bytes").type
    ), f"req_size_bytes should be integer type, got {buf_table.schema.field('req_size_bytes').type}"
    assert pa.types.is_floating(buf_table.schema.field("response_time_ms").type), (
        f"response_time_ms should be float type, got {buf_table.schema.field('response_time_ms').type}"
    )

    # Verify values are preserved correctly
    row = buf_table.to_pydict()
    assert row["req_size_bytes"][0] == 12345
    assert abs(row["response_time_ms"][0] - 42.7) < 0.01
