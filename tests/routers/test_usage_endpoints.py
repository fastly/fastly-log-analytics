from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app


@pytest.fixture
def test_client():
    return TestClient(app)


@patch("backend.routers.usage.get_source")
@patch("backend.core.duckdb.get_connection")
@patch("backend.routers.usage.repo.get_storage_stats")
@patch("backend.core.duckdb._get_fos_client")
@patch("backend.config.load_config")
def test_usage_current_storage_success(
    mock_load_config, mock_get_fos_client, mock_get_storage_stats, mock_get_connection, mock_get_source, test_client
):
    mock_get_source.return_value = {
        "name": "test-svc",
        "service_id": "test-svc",
        "bucket": "test-bucket",
        "prefix": "test-prefix",
        "region": "us-east-1",
        "endpoint": "test-endpoint",
        "access_key_id": "test",
        "secret_access_key": "test",
    }

    mock_load_config.return_value = {"provisioning": {"cron_sync": {"delete_after": True, "log_retention_days": 30}}}

    mock_get_storage_stats.return_value = {"total_files": 10, "total_bytes": 1024, "_debug_queries": []}

    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Size": 1024}]}]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_get_fos_client.return_value = mock_s3

    app.dependency_overrides[get_source] = lambda: mock_get_source.return_value

    response = test_client.get(
        "/api/usage/current-storage", params={"start": "2026-05-14T17:12:58.000Z", "end": "2026-05-15T17:12:58.000Z"}
    )

    assert response.status_code == 200
