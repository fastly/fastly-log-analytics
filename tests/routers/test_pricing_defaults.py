"""Tests for centralized pricing defaults and global rate management."""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app

_MOCK_GLOBAL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.006,  # Non-default
    "class_b_rate_per_10k": 0.02,  # Non-default
    "cdn_egress_rate_per_gb": 0.15,  # Non-default
    "storage_rate_per_gb_month": 0.03,  # Non-default
}

_TEST_SOURCE = {"name": "test_service", "service_id": "svc1"}


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
def test_prefill_uses_global_rates(mock_load):
    """Verify that /api/usage/prefill returns the global rates, not hardcoded defaults."""
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get("/api/usage/prefill")
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    data = r.json()
    assert data["class_a_rate_per_1k"] == 0.006
    assert data["class_b_rate_per_10k"] == 0.02
    assert data["cdn_egress_rate_per_gb"] == 0.15
    assert data["storage_rate_per_gb_month"] == 0.03


@patch("backend.config.load_usage_logging_config", return_value=_MOCK_GLOBAL_CFG)
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_uses_global_rates_for_cost(mock_get_logs, mock_load):
    """Verify that aggregate cost calculations use the global rates."""
    mock_get_logs.return_value = (
        [],
        0,
        {
            "total_class_a": 1000,
            "total_class_b": 10000,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 1073741824,  # 1 GB
            "total_fos_bytes": 0,
            "class_a_breakdown": {},
            "class_b_breakdown": {},
        },
    )

    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get(
            "/api/admin/usage-log",
            params={"start": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"},
        )
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    agg = r.json()["aggregate"]

    # Class A: 1000 ops @ $0.006/1k = 0.006
    assert abs(agg["estimated_cost_class_a"] - 0.006) < 1e-10
    # Class B: 10000 ops @ $0.02/10k = 0.02
    assert abs(agg["estimated_cost_class_b"] - 0.02) < 1e-10
    # CDN: 1GB @ $0.15/GB = 0.15
    assert abs(agg["estimated_cost_cdn"] - 0.15) < 1e-10
    # Total: 0.006 + 0.02 + 0.15 = 0.176
    assert abs(agg["estimated_cost_total"] - 0.176) < 1e-10
