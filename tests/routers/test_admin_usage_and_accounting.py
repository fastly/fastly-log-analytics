"""Consolidated tests for Admin Log Accounting, Usage Logging, and Usage Router."""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend import config
from backend.core import metadata as metadata_db
from backend.core.metadata import get_con
from backend.deps import get_source
from backend.main import app
from backend.routers.usage import _extract_fos_ops, _fastly_api
from backend.utils.remote_access import _is_blocked_path
from tests.conftest import MOCK_SERVICE_ID

_DEFAULT_UL_CFG = {
    "enabled": True,
    "retention_days": 30,
    "class_a_rate_per_1k": 0.005,
    "class_b_rate_per_10k": 0.004,
    "cdn_egress_rate_per_gb": 0.12,
    "storage_rate_per_gb_month": 0.02,
    "min_billed_days": 30,
    "billing_zone": "MBZ100",
}

_SAMPLE_USAGE_ENTRY = {
    "timestamp": "2024-06-01 12:00:00",
    "service_id": MOCK_SERVICE_ID,
    "operation_class": "A",
    "operation_type": "PutObject",
    "url": "s3://bucket/a.parquet",
    "bytes": 1024,
    "duration_ms": 12.5,
    "function_name": "upload",
    "process_context": "cron:commit",
    "status": "200",
    "count": 1,
}


@pytest.fixture
def _clear_log_accounting_ttl_caches():
    from backend.routers.admin import log_accounting as _la

    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()
    yield
    _la._FASTLY_COUNTS_CACHE.clear()
    _la._DUCKDB_COUNTS_CACHE.clear()


@pytest.fixture
def log_accounting_source():
    return {
        "name": "test_service",
        "service_id": "test-service-id",
        "logging_service_id": "test-logging-svc-id",
    }


@pytest.fixture
def log_accounting_client(log_accounting_source, in_memory_duckdb):
    from backend.deps import get_con as dep_get_con

    app.dependency_overrides[dep_get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: log_accounting_source
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _fake_urlopen(payload: dict):
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(payload).encode()

    return lambda req, timeout=30: _Resp()


def _seed_ingested(svc_id: str, rows: list[tuple[str, int]]):
    con = metadata_db.get_con(svc_id)
    for i, (ts_iso, rc) in enumerate(rows):
        ts = ts_iso.replace("T", " ")
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, ingested_at, row_count, file_size_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"raw/file-{i}.gz", svc_id, ts, rc, 1000),
        )
    con.commit()


def _complete_src(**overrides) -> dict:
    base = {
        "name": "test_service",
        "service_id": MOCK_SERVICE_ID,
        "logging_service_id": MOCK_SERVICE_ID,
        "bucket": "test-bucket",
        "prefix": "",
        "region": "us-east-1",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "k",
        "secret_access_key": "s",
    }
    base.update(overrides)
    return base


def _setup_usage_svc(
    monkeypatch, tmp_path, *, api_key: str | None = "tok", name: str | None = None, **src_overrides
) -> None:
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    cfg_payload: dict = {"service_id": MOCK_SERVICE_ID}
    if name is not None:
        cfg_payload["name"] = name
    if api_key is not None:
        cfg_payload["fastly_api_key"] = api_key
    config.save_config(MOCK_SERVICE_ID, cfg_payload)
    app.dependency_overrides[get_source] = lambda: _complete_src(**src_overrides)


# ── Log Accounting ──
@pytest.mark.usefixtures("frozen_clock", "_clear_log_accounting_ttl_caches")
def test_log_accounting_aligns_buckets_and_detects_sustained_loss(log_accounting_client, log_accounting_source):
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0, b1, b2 = now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 900),
            (b2.strftime("%Y-%m-%dT%H:%M:%S"), 900),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
            {"start_time": int(b2.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp.status_code == 200
    body = resp.json()
    assert body["totals"]["fastly_requests"] == 3000
    assert body["totals"]["our_rows"] == 2700
    assert body["totals"]["gap"] == 300
    assert body["sustained_loss"] is not None
    assert body["sustained_loss"]["n_buckets"] == 3
    assert body["sustained_loss"]["total_lost_lines"] == 300


@pytest.mark.usefixtures("frozen_clock", "_clear_log_accounting_ttl_caches")
def test_log_accounting_normal_drift_and_missing_api_key(log_accounting_client, log_accounting_source):
    svc_id = log_accounting_source["name"]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    b0, b1 = now - timedelta(hours=2), now - timedelta(hours=1)
    _seed_ingested(
        svc_id,
        [
            (b0.strftime("%Y-%m-%dT%H:%M:%S"), 1020),
            (b1.strftime("%Y-%m-%dT%H:%M:%S"), 980),
        ],
    )
    fastly_payload = {
        "data": [
            {"start_time": int(b0.timestamp()), "requests": 1000},
            {"start_time": int(b1.timestamp()), "requests": 1000},
        ]
    }
    with (
        patch("backend.config.get_fastly_api_key", return_value="fake-api-key"),
        patch("urllib.request.urlopen", side_effect=_fake_urlopen(fastly_payload)),
    ):
        resp = log_accounting_client.get("/api/admin/log-accounting?hours=3&by=hour")
    assert resp.status_code == 200
    assert resp.json()["sustained_loss"] is None

    from backend.routers.admin import log_accounting as _la

    _la._FASTLY_COUNTS_CACHE.clear()
    with patch("backend.config.get_fastly_api_key", return_value=None):
        resp_no_key = log_accounting_client.get("/api/admin/log-accounting?hours=4&by=hour")
    assert resp_no_key.status_code == 400


# ── Admin Usage Logging Settings, Aggregates, CSV Export & RBAC ──
def test_usage_logging_settings_and_billing_zones(client, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    r = client.get("/api/admin/usage-logging")
    assert r.status_code == 200 and "enabled" in r.json()

    for zone, expected_rate in [("MBZ100", 0.08), ("MBZ200", 0.12), ("Global", 0.18)]:
        p = client.patch("/api/admin/usage-logging", json={"billing_zone": zone})
        assert p.status_code == 200
        assert p.json()["billing_zone"] == zone
        assert p.json()["cdn_egress_rate_per_gb"] == expected_rate

    bad_zone = client.patch("/api/admin/usage-logging", json={"billing_zone": "INVALID_ZONE"})
    assert bad_zone.status_code in (400, 422)


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
@pytest.mark.parametrize("bad_val", [0, -1])
def test_usage_logging_settings_rejects_non_positive(client, tmp_path, monkeypatch, field, bad_val):
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    resp = client.patch("/api/admin/usage-logging", json={field: bad_val})
    assert resp.status_code in (400, 422)


@patch("backend.config.load_usage_logging_config", return_value=dict(_DEFAULT_UL_CFG))
def test_usage_log_query_export_and_delete(mock_ul_cfg):
    svc = "svc-pg-usage-test"
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
            with TestClient(app) as c:
                res = c.get(
                    "/api/admin/usage-log",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
                assert res.status_code == 200 and res.json()["total"] == 1

                csv_res = c.get(
                    "/api/admin/usage-log/export",
                    params={"start": "2026-06-01T10:00:00Z", "end": "2026-06-01T11:00:00Z"},
                )
                assert csv_res.status_code == 200
                rows = list(csv.reader(io.StringIO(csv_res.text)))
                assert len(rows) == 2
                # Formula-injection prefixes (=, +, @, -) must be escaped with leading "'"
                assert rows[1][4].startswith("'=") and rows[1][7].startswith("'+")

                del_res = c.delete("/api/admin/usage-log")
                assert del_res.status_code == 200
        finally:
            app.dependency_overrides.pop(get_source, None)
    finally:
        metadata_db.clear_usage_log(svc)


@patch("backend.cron.decorators.get_system_job_status")
@patch("backend.cron.scheduler.get_scheduler")
def test_get_system_jobs_and_remote_access_blocks(mock_sched, mock_status):
    mock_status.return_value = {
        "bot_data_refresh": {"last_run_at": "2026-06-01T10:00:00Z", "status": "ok", "duration_s": 1.25}
    }
    fake_job = SimpleNamespace(next_run_time=datetime(2026, 6, 2, 3, 0, 0, tzinfo=UTC))
    sched_obj = MagicMock()
    sched_obj.get_job.side_effect = lambda jid: fake_job if jid == "bot_data_refresh" else None
    mock_sched.return_value = sched_obj

    with TestClient(app) as c:
        r = c.get("/api/admin/system-jobs")
    assert r.status_code == 200
    assert {j["id"] for j in r.json()["jobs"]} == {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}

    for path in ("/api/admin/usage-logging", "/api/admin/usage-log", "/api/admin/system-jobs"):
        assert _is_blocked_path(path) is True


# ── Usage Router Helpers & Endpoints ──
@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (
            {
                "object_storage_class_a_operations_count": 1234,
                "object_storage_class_b_operations_count": 5678,
                "object_storage": {"class_a_operations_count": 1, "class_b_operations_count": 1},
            },
            (1234, 5678),
        ),
        ({"object_storage": {"class_a_operations_count": 100, "class_b_operations_count": 200}}, (100, 200)),
        ({"object_storage_class_a_operations_count": None, "object_storage_class_b_operations_count": None}, (0, 0)),
        ({"other": 42}, (0, 0)),
    ],
)
def test_extract_fos_ops_variants(record, expected):
    assert _extract_fos_ops(record) == expected


def test_fastly_api_constructs_correct_url_with_auth_header():
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"data": []}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp) as mock_open:
        assert _fastly_api("/services/svc1/details", "test-token") == {"data": []}
    req = mock_open.call_args[0][0]
    assert req.full_url == "https://api.fastly.com/services/svc1/details"
    assert req.get_header("Fastly-key") == "test-token"


def test_usage_prefill_operations_and_rum_breakdown(client, tmp_path, monkeypatch):
    _setup_usage_svc(monkeypatch, tmp_path, api_key="api-tok", cdn_service_id="cdn-svc-id")
    fake_payload = {
        "data": [
            {
                "start_time": 1767225600,
                "requests": 1000,
                "edge_requests": 800,
                "object_storage_class_a_operations_count": 100,
                "object_storage_class_b_operations_count": 50,
            }
        ]
    }
    try:
        with (
            patch("backend.routers.usage.get_fastly_api_key", return_value="api-tok"),
            patch("backend.core.fastly.service.get_active_version", return_value=None),
            patch("backend.routers.usage._fastly_api", return_value=fake_payload),
        ):
            r_pre = client.get("/api/usage/prefill", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
            assert r_pre.status_code == 200
            assert "storage_rate_per_gb_month" in r_pre.json()
            assert r_pre.json()["requests_per_day"] == 1000

            r_ops = client.get(
                "/api/usage/operations",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                params={"by": "minute"},
            )
            assert r_ops.status_code == 200

        # RUM breakdown endpoint
        svc = "test_usage_rum_breakdown_svc"
        con = get_con(svc)
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, table_name, file_date) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("s3://bucket/2026-08-05.parquet", svc, 10, 100, "client_vitals", "2026-08-05"),
        )
        con.commit()
        app.dependency_overrides[get_source] = lambda: {"name": svc, "service_id": svc}
        with patch("backend.config.load_usage_logging_config", return_value={"class_a_rate_per_1k": 0.01}):
            r_rum = client.get(
                "/api/usage/rum-breakdown",
                params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
            )
            assert r_rum.status_code == 200
            assert r_rum.json()["total_beacons"] == 10
        con.execute("DELETE FROM ingested_files WHERE source_name = ?", (svc,))
        con.commit()
    finally:
        app.dependency_overrides.pop(get_source, None)
