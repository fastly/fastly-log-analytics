"""Tests for /api/admin/usage-log*, /api/admin/usage-logging, and /api/admin/system-jobs."""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.core import metadata as metadata_db
from backend.deps import get_source
from backend.main import app
from backend.routers.admin import router as admin_router
from backend.utils.remote_access import _is_blocked_path

_DEFAULT_UL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.01,
    "cdn_egress_rate_per_gb": 0.12,
    "storage_rate_per_gb_month": 0.02,
    "min_billed_days": 30,
    "billing_zone": "MBZ200",
}

_TEST_SOURCE = {"name": "svc-usage-test", "service_id": "svc-usage-test"}


# ── GET / PATCH /api/admin/usage-logging ──────────────────────────────────────


@patch("backend.config.load_usage_logging_config")
def test_get_usage_logging_settings(mock_load):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.get("/api/admin/usage-logging")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 30
    assert body["class_a_rate_per_1k"] == pytest.approx(0.005)
    assert body["billing_zone"] == "MBZ200"


@patch("backend.config.load_usage_logging_config")
@patch("backend.config.save_usage_logging_config")
def test_patch_usage_logging_partial_update(mock_save, mock_load):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.patch(
            "/api/admin/usage-logging",
            json={"enabled": False, "retention_days": 60, "class_b_rate_per_10k": 0.025},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is False
    assert body["retention_days"] == 60
    assert body["class_b_rate_per_10k"] == pytest.approx(0.025)
    assert body["class_a_rate_per_1k"] == pytest.approx(0.005)
    mock_save.assert_called_once()


@pytest.mark.parametrize(
    "field",
    [
        "retention_days",
        "class_a_rate_per_1k",
        "class_b_rate_per_10k",
        "cdn_egress_rate_per_gb",
        "storage_rate_per_gb_month",
        "min_billed_days",
    ],
)
@pytest.mark.parametrize("bad_value", [0, -1, -0.05])
@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_patch_usage_logging_rejects_non_positive_numbers(mock_load, field: str, bad_value: float):
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={field: bad_value})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["field"] == field
    assert "positive number" in detail["error"]


@pytest.mark.parametrize(
    ("zone", "expected_rate"),
    [
        ("MBZ100", 0.08),
        ("MBZ200", 0.12),
        ("Global", 0.18),
    ],
)
@patch("backend.config.load_usage_logging_config")
@patch("backend.config.save_usage_logging_config")
def test_patch_usage_logging_billing_zone_auto_sets_cdn_rate(mock_save, mock_load, zone: str, expected_rate: float):
    mock_load.return_value = dict(_DEFAULT_UL_CFG)
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={"billing_zone": zone})
    assert r.status_code == 200
    body = r.json()
    assert body["billing_zone"] == zone
    assert body["cdn_egress_rate_per_gb"] == pytest.approx(expected_rate)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_patch_usage_logging_rejects_invalid_billing_zone(mock_load):
    with TestClient(app) as client:
        r = client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"})
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert detail["field"] == "billing_zone"
    assert "Invalid billing_zone" in detail["error"]


# ── GET /api/admin/usage-log (entries, hourly summary, filters) ──────────────


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_usage_log_postgres_roundtrip_and_filters(mock_ul_cfg):
    svc = "svc-pg-usage-log"
    metadata_db.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "PutObject",
                        "path": "s3://bucket/a.parquet",
                        "bytes": 2048,
                        "time_ms": 12.5,
                        "caller": "commit_files",
                        "status": "200",
                    }
                ],
                process_context="cron:commit:svc",
            )
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T11:30:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "GetObject",
                        "path": "s3://bucket/raw.gz",
                        "bytes": 4096,
                        "time_ms": 8.0,
                        "caller": "download_batch",
                        "status": "200",
                    }
                ],
                process_context="cron:sync:svc",
            )
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T12:05:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "CDN",
                        "method": "edge_egress",
                        "path": "https://cdn.example.com/item",
                        "bytes": 1073741824,
                        "time_ms": 3.2,
                        "caller": "fastly_stats",
                        "status": "200",
                    }
                ],
                process_context="fastly:stats",
            )

        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as client:
                # 1. Unfiltered window (uses usage_log_hourly_summary for interior hours + boundary scan)
                r = client.get(
                    "/api/admin/usage-log",
                    params={"start": "2026-06-01T09:00:00Z", "end": "2026-06-01T14:00:00Z"},
                )
                assert r.status_code == 200
                body = r.json()
                assert body["service_id"] == svc
                assert body["total"] == 3
                agg = body["aggregate"]
                assert agg["total_class_a"] == 1
                assert agg["total_class_b"] == 1
                assert agg["total_cdn_downloads"] == 1
                assert agg["total_cdn_bytes"] == 1073741824
                assert agg["total_fos_bytes"] == 2048 + 4096
                assert agg["class_a_breakdown"] == {"PutObject": 1}
                assert agg["class_b_breakdown"] == {"GetObject": 1}
                assert agg["estimated_cost_cdn"] == pytest.approx(0.12)

                # 2. Filter by usage_type="FOS-B"
                r_b = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "usage_type": "FOS-B",
                    },
                )
                assert r_b.status_code == 200
                body_b = r_b.json()
                assert body_b["total"] == 1
                assert body_b["entries"][0]["operation_class"] == "B"
                assert body_b["entries"][0]["count"] == 1
                # Per-row cost at $0.01/10k = 0.000001
                assert body_b["entries"][0]["estimated_cost"] == pytest.approx(0.000001)

                # 3. Filter by process_context="cron:commit"
                r_proc = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "process_context": "cron:commit",
                    },
                )
                assert r_proc.status_code == 200
                body_proc = r_proc.json()
                assert body_proc["total"] == 1
                assert body_proc["entries"][0]["operation_type"] == "PutObject"

                # 4. Filter by operation_type="edge_egress"
                r_op = client.get(
                    "/api/admin/usage-log",
                    params={
                        "start": "2026-06-01T09:00:00Z",
                        "end": "2026-06-01T14:00:00Z",
                        "operation_type": "edge_egress",
                    },
                )
                assert r_op.status_code == 200
                body_op = r_op.json()
                assert body_op["total"] == 1
                assert body_op["entries"][0]["operation_class"] == "CDN"
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


# ── GET /api/admin/usage-log/export (CSV streaming + formula sanitization) ───


def test_usage_log_export_csv_and_formula_escaping():
    svc = "svc-pg-usage-export"
    metadata_db.clear_usage_log(svc)
    try:
        with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
            metadata_db.log_usage_calls(
                svc,
                [
                    {
                        "service": "FOS",
                        "method": "PutObject",
                        "path": "=cmd|' /C calc'!A0",
                        "bytes": 512,
                        "time_ms": 5.0,
                        "caller": "+sum(1,2)",
                        "status": "-200",
                    }
                ],
                process_context="@at_context",
            )
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        try:
            with TestClient(app) as client:
                r = client.get(
                    "/api/admin/usage-log/export",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
            assert r.status_code == 200
            assert "text/csv" in r.headers["content-type"]
            assert "filename=usage_log.csv" in r.headers["content-disposition"]

            rows = list(csv.reader(io.StringIO(r.text)))
            assert len(rows) == 2
            assert rows[0] == [
                "timestamp",
                "service_id",
                "operation_class",
                "operation_type",
                "url",
                "bytes",
                "duration_ms",
                "function_name",
                "process_context",
                "status",
                "count",
            ]
            data_row = rows[1]
            assert data_row[1] == svc
            assert data_row[2] == "A"
            # Formula-injection prefixes (=, +, @, -) must be escaped with leading "'"
            assert data_row[4].startswith("'=")
            assert data_row[7].startswith("'+")
            assert data_row[8].startswith("'@")
            assert data_row[9].startswith("'-")
            assert data_row[10] == "1"
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


# ── DELETE /api/admin/usage-log ───────────────────────────────────────────────


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_delete_usage_log_clears_service_entries(mock_ul_cfg):
    svc = "svc-pg-usage-delete"
    metadata_db.clear_usage_log(svc)
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value="2026-06-01T10:15:00Z"):
        metadata_db.log_usage_calls(
            svc,
            [
                {
                    "service": "FOS",
                    "method": "PutObject",
                    "path": "s3://bucket/a.parquet",
                    "bytes": 100,
                    "time_ms": 1.0,
                    "caller": "fn",
                    "status": "200",
                }
            ],
            process_context="ctx",
        )
    app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
    try:
        with TestClient(app) as client:
            del_res = client.delete("/api/admin/usage-log")
            assert del_res.status_code == 200
            assert del_res.json()["ok"] is True

            get_res = client.get(
                "/api/admin/usage-log",
                params={"start": "2026-06-01T00:00:00Z", "end": "2026-06-02T00:00:00Z"},
            )
            assert get_res.status_code == 200
            assert get_res.json()["total"] == 0
    finally:
        app.dependency_overrides.pop(get_source, None)


# ── GET /api/admin/system-jobs ────────────────────────────────────────────────


@patch("backend.utils.system_jobs.get_system_job_status")
@patch("backend.cron.scheduler.get_scheduler")
def test_get_system_jobs_endpoint(mock_sched, mock_status):
    mock_status.return_value = {
        "bot_data_refresh": {
            "last_run_at": "2026-06-01T10:00:00Z",
            "status": "ok",
            "duration_s": 1.25,
            "detail": "3 sources updated",
        }
    }
    fake_job = SimpleNamespace(next_run_time=datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC))
    sched_obj = MagicMock()
    sched_obj.get_job.side_effect = lambda jid: fake_job if jid == "bot_data_refresh" else None
    mock_sched.return_value = sched_obj

    with TestClient(app) as client:
        r = client.get("/api/admin/system-jobs")

    assert r.status_code == 200
    jobs = {j["id"]: j for j in r.json()["jobs"]}
    assert set(jobs.keys()) == {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}
    assert jobs["bot_data_refresh"]["status"] == "ok"
    assert jobs["bot_data_refresh"]["next_run_at"] == "2026-06-02T03:00:00Z"
    assert jobs["rdns_enrichment"]["next_run_at"] is None


# ── Analyst Path B RBAC (403 admin_only) ─────────────────────────────────────


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/admin/usage-logging"),
        ("PATCH", "/api/admin/usage-logging"),
        ("GET", "/api/admin/usage-log"),
        ("GET", "/api/admin/usage-log/export"),
        ("DELETE", "/api/admin/usage-log"),
        ("GET", "/api/admin/system-jobs"),
    ],
)
def test_usage_log_endpoints_reject_analyst_path_b(method: str, path: str):
    # 1. Middleware path gate blocks all /api/admin/* paths
    assert _is_blocked_path(path) is True

    # 2. Route-level Depends(require_admin) also rejects Analyst Path B with 403 admin_only
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def _stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-usage",
            service_ids=["svc-usage-test"],
            email="analyst@example.com",
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)
    analyst_app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

    with TestClient(analyst_app) as client:
        if method == "PATCH":
            r = client.patch(path, json={"enabled": True})
        elif method == "DELETE":
            r = client.delete(path)
        else:
            r = client.get(path)

    assert r.status_code == 403
    detail = r.json().get("detail", r.json())
    assert detail["error"] == "admin_only"
