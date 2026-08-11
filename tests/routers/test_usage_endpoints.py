from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app


@pytest.fixture
def test_client():
    return TestClient(app)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_success(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """The endpoint returns 200 when storage stats and FOS listing both
    succeed. Migrated from inline boto3-MagicMock paginator stubs to the
    shared moto-backed s3_mock fixture — the LIST fallback now flows
    through real S3 semantics rather than a hand-rolled paginator stub.

    ``get_connection`` is still mocked: the route opens a DuckDB handle
    only to pass to the already-mocked ``get_storage_stats``, so the real
    DB connection contributes nothing to what we're verifying — and under
    pytest-xdist the on-disk per-service DuckDB file can be held by a
    peer worker, surfacing as a DBBusyError 500 we'd otherwise chase.
    """
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    mock_get_storage_stats.return_value = {"total_files": 10, "total_bytes": 1024, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    # Seed one iceberg object so the FOS LIST fallback finds something.
    # get_table_info (tried first) has no real iceberg metadata to read
    # and will swallow its own exception, falling through to the LIST.
    source = {**fos_source, "prefix": "test-prefix"}
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/iceberg/data/x.parquet",
        Body=b"x" * 1024,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_splits_out_rum_bytes(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """RUM beacon objects live under ``raw_rum/`` inside the same bucket as
    regular logs. The endpoint must report their size separately
    (``rum_bytes``) and subtract it from the regular-log total
    (``regular_log_bytes``) rather than double-counting it into both."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 5000
    mock_get_storage_stats.return_value = {"total_files": 3, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "prefix": "test-prefix"}
    rum_object_size = 777
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="test-prefix/raw_rum/2026-08-05T00-00-00.log.gz",
        Body=b"r" * rum_object_size,
    )

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == rum_object_size
        assert body["regular_log_bytes"] == total_bytes - rum_object_size
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_rum_scan_failure_defaults_to_zero(
    mock_load_config, mock_get_storage_stats, mock_get_connection, s3_mock, fos_source, test_client
):
    """A broken FOS listing for the RUM prefix must not 500 the whole
    cost panel — it should fail open to rum_bytes=0 (regular_log_bytes
    falls back to the full total, same as before this field existed)."""
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    total_bytes = 2048
    mock_get_storage_stats.return_value = {"total_files": 1, "total_bytes": total_bytes, "_debug_queries": []}
    mock_get_connection.return_value.close = lambda: None

    source = {**fos_source, "bucket": "bucket-that-does-not-exist"}

    app.dependency_overrides[get_source] = lambda: source
    try:
        response = test_client.get(
            "/api/usage/current-storage",
            params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"},
        )
        assert response.status_code == 200, f"unexpected status; body: {response.text[:600]}"
        body = response.json()
        assert body["rum_bytes"] == 0
        assert body["regular_log_bytes"] == total_bytes
    finally:
        app.dependency_overrides.pop(get_source, None)
