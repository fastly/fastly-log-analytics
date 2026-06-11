from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app


@pytest.fixture
def test_client():
    return TestClient(app)


@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.config.load_config")
def test_usage_current_storage_success(mock_load_config, mock_get_storage_stats, s3_mock, fos_source, test_client):
    """The endpoint returns 200 when storage stats and FOS listing both
    succeed. Migrated from inline boto3-MagicMock paginator stubs to the
    shared moto-backed s3_mock fixture — the LIST fallback now flows
    through real S3 semantics rather than a hand-rolled paginator stub.
    """
    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}
    mock_get_storage_stats.return_value = {"total_files": 10, "total_bytes": 1024, "_debug_queries": []}

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
        assert response.status_code == 200
    finally:
        app.dependency_overrides.pop(get_source, None)
