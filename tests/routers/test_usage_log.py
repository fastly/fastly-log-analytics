"""Tests for GET/PATCH /api/admin/usage-logging and GET /api/admin/usage-log."""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app

_DEFAULT_UL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.01,
    "cdn_egress_rate_per_gb": 0.12,
}

_TEST_SOURCE = {"name": "test_service", "service_id": "svc1"}


@patch("backend.config.load_usage_logging_config")
def test_get_usage_logging_settings(mock_load):
    mock_load.return_value = _DEFAULT_UL_CFG
    with TestClient(app) as c:
        r = c.get("/api/admin/usage-logging")
    assert r.status_code == 200
    assert r.json()["enabled"] is True


@patch("backend.config.load_usage_logging_config")
@patch("backend.config.save_usage_logging_config")
def test_update_usage_logging_settings(mock_save, mock_load):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as c:
        r = c.patch("/api/admin/usage-logging", json={"enabled": False, "retention_days": 60})
    assert r.status_code == 200
    assert r.json()["enabled"] is False
    assert r.json()["retention_days"] == 60
    assert mock_save.called


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_empty_table(mock_get_logs, mock_ul_cfg):
    mock_get_logs.return_value = (
        [],
        0,
        {
            "total_class_a": 0,
            "total_class_b": 0,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 0,
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
    body = r.json()
    assert body["total"] == 0
    assert body["aggregate"]["total_class_a"] == 0


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_aggregates_correctly(mock_get_logs, mock_ul_cfg):
    mock_get_logs.return_value = (
        [
            {
                "id": 1,
                "timestamp": "2024-06-01 12:00:00",
                "service_id": "svc1",
                "operation_class": "A",
                "operation_type": "PutObject",
                "url": "/file.gz",
                "bytes": 0,
                "duration_ms": 10,
                "function_name": "test",
                "process_context": "test",
                "status": "OK",
            }
        ]
        * 4,
        4,
        {
            "total_class_a": 2,
            "total_class_b": 1,
            "total_cdn_downloads": 1,
            "total_cdn_bytes": 2147483648,
            "total_fos_bytes": 1073741824,
            "class_a_breakdown": {"PutObject": 2},
            "class_b_breakdown": {"GetObject": 1},
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
    body = r.json()
    assert body["total"] == 4
    agg = body["aggregate"]
    assert agg["total_class_a"] == 2
    assert agg["total_class_b"] == 1

    # Class A: 2 ops @ $0.005/1k = 0.00001
    assert abs(agg["estimated_cost_class_a"] - 0.00001) < 1e-10
    # Class B: 1 op @ $0.01/10k = 0.000001
    assert abs(agg["estimated_cost_class_b"] - 0.000001) < 1e-10
    # CDN: 2GB @ $0.12/GB = 0.24
    assert abs(agg["estimated_cost_cdn"] - 0.24) < 1e-6


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_usage_type_filter_cdn_only(mock_get_logs, mock_ul_cfg):
    mock_get_logs.return_value = (
        [
            {
                "id": 1,
                "timestamp": "2024-06-01 12:01:00",
                "service_id": "svc1",
                "operation_class": "CDN",
                "operation_type": "download",
                "url": "/b.parquet",
                "bytes": 1024,
                "duration_ms": 10,
                "function_name": "test",
                "process_context": "test",
                "status": "OK",
            }
        ],
        1,
        {
            "total_class_a": 0,
            "total_class_b": 0,
            "total_cdn_downloads": 1,
            "total_cdn_bytes": 1024,
            "total_fos_bytes": 0,
            "class_a_breakdown": {},
            "class_b_breakdown": {},
        },
    )
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get(
            "/api/admin/usage-log",
            params={
                "start": "2024-01-01 00:00:00",
                "end": "2024-12-31 23:59:59",
                "usage_type": "CDN",
            },
        )
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["entries"][0]["operation_class"] == "CDN"


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_process_context_filter(mock_get_logs, mock_ul_cfg):
    mock_get_logs.return_value = (
        [
            {
                "id": 1,
                "timestamp": "2024-06-01 12:00:00",
                "service_id": "svc1",
                "operation_class": "A",
                "operation_type": "PutObject",
                "url": "/a.gz",
                "bytes": 0,
                "duration_ms": 10,
                "function_name": "test",
                "process_context": "cron:sync:svc1",
                "status": "OK",
            }
        ],
        1,
        {
            "total_class_a": 1,
            "total_class_b": 0,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 0,
            "total_fos_bytes": 0,
            "class_a_breakdown": {"PutObject": 1},
            "class_b_breakdown": {},
        },
    )
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get(
            "/api/admin/usage-log",
            params={
                "start": "2024-01-01 00:00:00",
                "end": "2024-12-31 23:59:59",
                "process_context": "cron:sync",
            },
        )
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert "cron:sync" in body["entries"][0]["process_context"]


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
@patch("backend.core.metadata_db.get_usage_logs")
def test_usage_log_operation_type_filter(mock_get_logs, mock_ul_cfg):
    mock_get_logs.return_value = (
        [
            {
                "id": 1,
                "timestamp": "2024-06-01 12:00:00",
                "service_id": "svc1",
                "operation_class": "A",
                "operation_type": "PutObject",
                "url": "/a.gz",
                "bytes": 0,
                "duration_ms": 10,
                "function_name": "test",
                "process_context": "test",
                "status": "OK",
            }
        ],
        1,
        {
            "total_class_a": 1,
            "total_class_b": 0,
            "total_cdn_downloads": 0,
            "total_cdn_bytes": 0,
            "total_fos_bytes": 0,
            "class_a_breakdown": {"PutObject": 1},
            "class_b_breakdown": {},
        },
    )
    mock_get_logs.return_value = mock_get_logs.return_value  # keep it
    app.dependency_overrides[get_source] = lambda: _TEST_SOURCE
    with TestClient(app) as c:
        r = c.get(
            "/api/admin/usage-log",
            params={
                "start": "2024-01-01 00:00:00",
                "end": "2024-12-31 23:59:59",
                "operation_type": "PutObject",
            },
        )
    app.dependency_overrides.pop(get_source, None)

    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["entries"][0]["operation_type"] == "PutObject"
