"""Tests for ``backend.routers.services.core`` and ``backend.routers.services.cron``."""

from __future__ import annotations

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest
from backend.services import service_manager
from backend.services.service_manager import _get_dir_stats
from fastapi.testclient import TestClient

from backend import config
from backend.core import metadata as metadata_db
from backend.core.metadata.cron_log import (
    CronLeaseHeldError,
    finalize_cron_run_if_running,
    log_cron_run,
    start_cron_run,
)
from backend.cron_progress import _progress, _run_metadata, end_progress, start_progress
from backend.main import app
from backend.routers.services import core as _core
from backend.routers.services import cron as _cron_router
from tests._helpers.wire import parse_sse_events as _parse_sse_events
from tests.conftest import MOCK_SERVICE_ID

_SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


@pytest.fixture(autouse=True)
def _clear_cron_schedule_ttl_cache():
    _core._cron_schedule_cache.clear()
    yield
    _core._cron_schedule_cache.clear()


def _cfg_dir(tmp_path, monkeypatch, save: dict | None = None):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    if save is not None:
        config.save_config(MOCK_SERVICE_ID, {"service_id": MOCK_SERVICE_ID, **save})


def _empty_sched():
    return type("S", (), {"_sched": type("X", (), {"get_jobs": lambda self: []})()})()


def test_services_list_and_lake_info(client):
    fake_services = [
        {"service_id": "svc-1", "name": "Svc 1", "fos_bucket": "b1", "fos_region": "us-east-1"},
        {"service_id": "svc-2", "name": "Svc 2", "fos_bucket": "b2", "fos_region": "us-west-2"},
    ]
    with patch("backend.services.service_manager.get_enriched_services", return_value=fake_services):
        resp = client.get("/api/services")
    assert resp.status_code == 200
    assert [s["service_id"] for s in resp.json()["services"]] == ["svc-1", "svc-2"]

    with patch("backend.services.service_manager.get_enriched_services", return_value=[]):
        resp_empty = client.get("/api/services")
    assert resp_empty.status_code == 200 and resp_empty.json()["services"] == []

    fake_info = {"earliest": "2026-01-01T00:00:00Z", "latest": "2026-05-18T00:00:00Z", "calendar": {}}
    with patch("backend.core.iceberg.lake_info.fetch_lake_info", return_value=fake_info) as mock_fetch:
        resp_lake = client.get(f"/api/services/{MOCK_SERVICE_ID}/lake-info", headers=_SVC_HEADERS)
    assert resp_lake.status_code == 200
    assert resp_lake.json()["earliest"] == "2026-01-01T00:00:00Z"
    assert mock_fetch.call_args.kwargs.get("use_temp_cache") is False


def test_logging_settings_errors(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 404

    _cfg_dir(tmp_path, monkeypatch, {"fastly_api_key": "tok"})
    with patch("backend.core.fastly.service.get_active_version", return_value=None):
        assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 400

    with patch("backend.core.fastly.service.get_active_version", side_effect=RuntimeError("network")):
        assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 500


@pytest.mark.parametrize(
    ("cfg_extra", "resp_cond", "cond_stmt", "expected"),
    [
        (
            {},
            "Log Sampling",
            "randombool(25, 100) && req.restarts == 0",
            {"sample_rate": 25, "edge_only": True, "version": 42, "period": 60},
        ),
        (
            {},
            "Log Sampling",
            'randombool(100, 100) && (req.url ~ "^/api/")',
            {"custom_condition": 'req.url ~ "^/api/"'},
        ),
        (
            {},
            None,
            None,
            {"sample_rate": 100, "edge_only": False},
        ),
        (
            {"provisioning": {"custom_condition": "saved_predicate"}},
            None,
            None,
            {"custom_condition": "saved_predicate"},
        ),
    ],
)
def test_logging_settings_parsing_and_defaults(
    client, tmp_path, monkeypatch, cfg_extra, resp_cond, cond_stmt, expected
):
    _cfg_dir(tmp_path, monkeypatch, {"fastly_api_key": "tok", **cfg_extra})
    fake_ep = {"format": "x", "period": 60, "path": "/raw/", "response_condition": resp_cond}
    fake_cond = {"statement": cond_stmt} if cond_stmt else None
    with (
        patch("backend.core.fastly.service.get_active_version", return_value=42),
        patch("backend.core.fastly.client.fastly", return_value=fake_ep),
        patch("backend.core.fastly.service.find_condition", return_value=fake_cond),
    ):
        resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    for k, v in expected.items():
        assert body[k] == v


def test_log_fields_get_404_saved_and_default_preset(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers=_SVC_HEADERS).status_code == 404

    _cfg_dir(tmp_path, monkeypatch, {"log_fields": {"groups": ["A", "B"], "field_overrides": {}}})
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert {"log_fields", "estimate", "waf_warning"} <= set(body.keys()) and isinstance(body["history"], list)

    _cfg_dir(tmp_path, monkeypatch, {})
    resp_def = client.get(f"/api/services/{MOCK_SERVICE_ID}/log-fields", headers=_SVC_HEADERS)
    assert resp_def.status_code == 200 and len(resp_def.json()["log_fields"]["groups"]) > 0


def test_cron_schedule_endpoints_and_alerts_placeholders(client):
    fake_per_task = {
        "log_discovery": {
            "started_at": "2026-05-18T00:00:00Z",
            "status": "ok",
            "duration_s": 1.2,
            "summary": "120 files",
        }
    }
    with (
        patch("backend.core.metadata.latest_cron_per_task", return_value=fake_per_task),
        patch("backend.cron.scheduler.get_scheduler", return_value=_empty_sched()),
    ):
        resp = client.get("/api/cron-schedule", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    sync_entries = [s for s in resp.json()["schedules"] if s["task"] == "log_discovery"]
    assert (
        len(sync_entries) == 1
        and sync_entries[0]["last_run_status"] == "ok"
        and sync_entries[0]["next_run_time"] is None
    )

    _core._cron_schedule_cache.clear()
    with (
        patch("backend.core.metadata.latest_cron_per_task", side_effect=RuntimeError("locked")),
        patch("backend.core.metadata.count_alerts", side_effect=RuntimeError("locked")),
        patch("backend.cron.scheduler.get_scheduler", return_value=_empty_sched()),
    ):
        resp_err = client.get("/api/cron-schedule", headers=_SVC_HEADERS)
    assert resp_err.status_code == 200 and resp_err.json()["schedules"] == []

    _core._cron_schedule_cache.clear()
    with (
        patch("backend.core.metadata.latest_cron_per_task", return_value={}),
        patch("backend.core.metadata.count_alerts", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=_empty_sched()),
    ):
        resp_zero = client.get("/api/cron-schedule", headers=_SVC_HEADERS)
    alerts_zero = [s for s in resp_zero.json()["schedules"] if s["task"] == "alerts"]
    assert len(alerts_zero) == 1 and alerts_zero[0]["disabled_reason"] == "no_alerts_configured"

    _core._cron_schedule_cache.clear()
    with (
        patch(
            "backend.core.metadata.latest_cron_per_task",
            return_value={
                "alerts": {
                    "started_at": "2026-05-22T10:00:00Z",
                    "status": "success",
                    "duration_s": 0.5,
                    "summary": "No alerts to evaluate",
                    "error_message": None,
                }
            },
        ),
        patch("backend.core.metadata.count_alerts", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=_empty_sched()),
    ):
        resp_hist = client.get("/api/cron-schedule", headers=_SVC_HEADERS)
    alerts_hist = [s for s in resp_hist.json()["schedules"] if s["task"] == "alerts"]
    assert len(alerts_hist) == 1
    assert alerts_hist[0]["last_run_status"] == "success"
    assert alerts_hist[0]["disabled_reason"] == "no_alerts_configured"


def test_update_logging_settings_validation_and_sse(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update").status_code == 404

    _cfg_dir(
        tmp_path,
        monkeypatch,
        {
            "fastly_api_key": "tok",
            "log_period": 60,
            "fos_prefix": "old-prefix",
            "provisioning": {
                "sample_rate": 100,
                "edge_only": False,
                "endpoint_name": "ep",
                "cron_sync": {"interval_mins": 1},
            },
        },
    )
    r_p0 = client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update", params={"period": 0})
    assert r_p0.status_code == 400 and "1" in r_p0.json()["detail"]["error"]
    assert (
        client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update", params={"sample_rate": 0}).status_code
        == 400
    )

    fake_events = [{"type": "status", "message": "ok"}, {"type": "done", "changed": True, "version": 99}]
    with (
        patch("backend.provision.update_logging_endpoint", return_value=iter(fake_events)),
        patch("backend.provision._sync_crontab"),
    ):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update",
            params={"period": 120, "sample_rate": 50, "edge_only": True},
        )
    assert resp.status_code == 200 and "done" in resp.text
    fresh = config.load_config(MOCK_SERVICE_ID)
    assert (fresh["log_period"], fresh["fos_prefix"]) == (120, "")
    assert (fresh["provisioning"]["sample_rate"], fresh["provisioning"]["edge_only"]) == (50, True)

    with (
        patch("backend.provision.update_logging_endpoint", return_value=iter([{"type": "done", "changed": True}])),
        patch("backend.provision._sync_crontab"),
    ):
        client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update", params={"period": 30})
    cs = config.load_config(MOCK_SERVICE_ID)["provisioning"]["cron_sync"]
    assert cs.get("interval_seconds") == 30 and "interval_mins" not in cs

    with patch("backend.provision.update_logging_endpoint", side_effect=RuntimeError("API down")):
        resp_err = client.post(f"/api/services/{MOCK_SERVICE_ID}/logging-settings/update")
    assert resp_err.status_code == 200 and "API down" in resp_err.text and "error" in resp_err.text


def test_invite_analyst_success_and_error_mappings(client):
    fake_invite = {
        "service_id": "svc-1",
        "name": "MyService",
        "access_key_id": "AK",
        "secret_key": "SK",
        "fos_bucket": "b",
        "fos_region": "us-east-1",
        "fos_endpoint": "https://s3.us-east-1.amazonaws.com",
        "fos_prefix": "",
    }
    with patch("backend.provision.generate_analyst_invite", return_value=fake_invite):
        resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
    assert resp.status_code == 200
    body = resp.json()
    assert (body["access_key_id"], body["secret_key"]) == ("AK", "SK") and "_debug_calls" in body

    for msg, expected_code in (
        ("Service ghost not found", 404),
        ("Invite generation requires a read_write service configuration", 403),
        ("Fastly upstream timeout", 400),
    ):
        with patch("backend.provision.generate_analyst_invite", side_effect=RuntimeError(msg)):
            r = client.post(f"/api/services/{MOCK_SERVICE_ID}/generate-viewer-key")
        assert r.status_code == expected_code
        if expected_code == 400:
            assert "timeout" in r.json()["detail"]["error"]


def test_ngwaf_sync_sse_validation_errors(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    r1 = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert r1.status_code == 200 and "Service not found" in r1.text

    _cfg_dir(tmp_path, monkeypatch, {"fastly_api_key": "tok"})
    r2 = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert r2.status_code == 200 and "NGWAF workspace" in r2.text

    _cfg_dir(tmp_path, monkeypatch, {"ngwaf_workspace_id": "ws-1"})
    r3 = client.post(f"/api/services/{MOCK_SERVICE_ID}/ngwaf-sync")
    assert r3.status_code == 200 and "Fastly API key" in r3.text


def _poll_dir_stats(path: str, expected: tuple[int, int], timeout: float = 2.0) -> tuple[int, int]:
    deadline = time.monotonic() + timeout
    result = _get_dir_stats(path)
    while result != expected and time.monotonic() < deadline:
        time.sleep(0.02)
        result = _get_dir_stats(path)
    return result


def test_get_dir_stats_missing_recursive_symlinks_and_nonblocking(tmp_path, monkeypatch):
    assert _get_dir_stats(str(tmp_path / "does_not_exist")) == (0, 0)

    rec_dir = tmp_path / "rec"
    rec_dir.mkdir()
    (rec_dir / "a.txt").write_bytes(b"x" * 100)
    sub = rec_dir / "sub"
    sub.mkdir()
    (sub / "b.txt").write_bytes(b"y" * 200)
    assert _poll_dir_stats(str(rec_dir), (300, 2)) == (300, 2)

    sym_dir = tmp_path / "sym"
    sym_dir.mkdir()
    real = sym_dir / "real.txt"
    real.write_bytes(b"x" * 50)
    os.symlink(real, sym_dir / "link.txt")
    assert _poll_dir_stats(str(sym_dir), (50, 1)) == (50, 1)

    cold_dir = tmp_path / "cold"
    cold_dir.mkdir()
    (cold_dir / "a.txt").write_bytes(b"x" * 100)
    real_walk = service_manager._walk_dir_stats

    def slow_walk(path: str) -> tuple[int, int]:
        time.sleep(0.5)
        return real_walk(path)

    monkeypatch.setattr(service_manager, "_walk_dir_stats", slow_walk)
    started = time.monotonic()
    size, count = _get_dir_stats(str(cold_dir))
    elapsed = time.monotonic() - started
    assert (size, count) == (0, 0) and elapsed < 0.4


def test_export_and_import_custom_fields(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert client.get(f"/api/services/{MOCK_SERVICE_ID}/custom-fields/export", headers=_SVC_HEADERS).status_code == 404
    assert (
        client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers=_SVC_HEADERS,
            json={"custom_fields": []},
        ).status_code
        == 404
    )

    _cfg_dir(
        tmp_path,
        monkeypatch,
        {
            "log_fields": {
                "groups": ["A"],
                "field_overrides": {},
                "custom_fields": [
                    {
                        "name": "keep_me",
                        "label": "Existing",
                        "enabled": True,
                        "vcl_log_expression": "req.http.x-keep-me",
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "bytes_estimate": 20,
                    }
                ],
            }
        },
    )
    exp = client.get(f"/api/services/{MOCK_SERVICE_ID}/custom-fields/export", headers=_SVC_HEADERS)
    assert exp.status_code == 200
    assert exp.headers["content-type"].startswith("application/json")
    assert f"custom_fields_{MOCK_SERVICE_ID}.json" in exp.headers["content-disposition"]
    assert json.loads(exp.text)["custom_fields"][0]["name"] == "keep_me"

    bad_list = client.post(
        f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
        headers=_SVC_HEADERS,
        json={"custom_fields": {"not": "a list"}},
    )
    assert bad_list.status_code == 422
    assert any(e.get("loc", [])[-1] == "custom_fields" for e in bad_list.json()["detail"])

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        imp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers=_SVC_HEADERS,
            json={
                "custom_fields": [
                    {
                        "name": "new_one",
                        "label": "New",
                        "enabled": True,
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "vcl_log_expression": "req.http.x-new-one",
                        "bytes_estimate": 20,
                    }
                ]
            },
        )
    assert imp.status_code == 200 and imp.json()["imported_count"] == 1
    names = {cf["name"] for cf in config.load_config(MOCK_SERVICE_ID)["log_fields"]["custom_fields"]}
    assert names == {"keep_me", "new_one"}

    # Locked type change on import & PATCH
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc"}),
        patch("backend.routers.services.core._committed_duckdb_field_types", return_value={"keep_me": "VARCHAR"}),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        imp_locked = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers=_SVC_HEADERS,
            json={"custom_fields": [{"name": "keep_me", "label": "x", "duckdb_type": "INTEGER"}]},
        )
        patch_locked = client.patch(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/keep_me",
            headers=_SVC_HEADERS,
            json={"duckdb_type": "INTEGER"},
        )
    assert imp_locked.status_code == 422 and any("keep_me" in e for e in imp_locked.json()["detail"]["errors"])
    assert patch_locked.status_code == 422 and any(
        "Cannot change" in e for e in patch_locked.json()["detail"]["errors"]
    )

    # Format exceeds limit
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.provision.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG: would be 12000 chars"]),
    ):
        imp_long = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/import",
            headers=_SVC_HEADERS,
            json={
                "custom_fields": [
                    {
                        "name": "x",
                        "label": "x",
                        "vcl_log_expression": "req.http.x",
                        "duckdb_type": "VARCHAR",
                        "value_type": "string",
                        "bytes_estimate": 20,
                    }
                ]
            },
        )
    assert imp_long.status_code == 422 and any("LOG_FORMAT_TOO_LONG" in e for e in imp_long.json()["detail"]["errors"])


def test_validate_custom_vcl(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert (
        client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers=_SVC_HEADERS,
            json={"vcl_log_expression": "req.url", "collection_stage": "edge"},
        ).status_code
        == 404
    )

    _cfg_dir(tmp_path, monkeypatch, {})
    with (
        patch("backend.core.field_registry.validate_custom_field", return_value=[]),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.provision.load_log_format", return_value="format string"),
    ):
        resp_ok = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers=_SVC_HEADERS,
            json={"vcl_log_expression": "req.url", "collection_stage": "edge"},
        ).json()
    assert (resp_ok["valid"], resp_ok["errors"], resp_ok["format_length"]) == (True, [], len("format string"))

    with (
        patch(
            "backend.core.field_registry.validate_custom_field",
            return_value=["WARN: deprecated VCL function used", "Real syntax error"],
        ),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.provision.load_log_format") as mock_load,
    ):
        resp_err = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/custom-fields/validate-vcl",
            headers=_SVC_HEADERS,
            json={"vcl_log_expression": "bad VCL", "collection_stage": "edge"},
        ).json()
    assert resp_err["valid"] is False and resp_err["format_length"] is None
    assert any("Real syntax error" in e for e in resp_err["errors"])
    assert any("deprecated" in w for w in resp_err["warnings"])
    assert not any(w.startswith("WARN:") for w in resp_err["warnings"])
    mock_load.assert_not_called()


def test_cron_settings_sse_and_clear_time_range(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    r_missing = client.post(f"/api/services/{MOCK_SERVICE_ID}/cron-settings", json={"cron_sync": {"enabled": False}})
    assert r_missing.status_code == 200 and "Service not found" in r_missing.text
    assert client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range").status_code == 404

    _cfg_dir(tmp_path, monkeypatch, {})
    r_noop = client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range")
    assert r_noop.status_code == 200 and "No time_range" in r_noop.json()["message"]

    with patch("backend.provision._sync_crontab"), patch("backend.core.metadata.record_audit"):
        resp = client.post(
            f"/api/services/{MOCK_SERVICE_ID}/cron-settings",
            json={
                "cron_sync": {"enabled": True, "interval_mins": 5, "evil_arbitrary_key": "should-be-dropped"},
                "cron_compact": {"enabled": True, "log_retention_days": 30},
                "cron_ngwaf": {"enabled": False, "interval_mins": 10},
            },
        )
    assert resp.status_code == 200 and "done" in resp.text
    prov = config.load_config(MOCK_SERVICE_ID)["provisioning"]
    assert prov["cron_sync"] == {"enabled": True, "interval_mins": 5}
    assert prov["cron_compact"]["log_retention_days"] == 30 and prov["cron_ngwaf"]["enabled"] is False

    with (
        patch("backend.provision._sync_crontab", side_effect=CronLeaseHeldError("scheduler busy")),
        patch("backend.core.metadata.record_audit"),
    ):
        r_warn = client.post(f"/api/services/{MOCK_SERVICE_ID}/cron-settings", json={"cron_sync": {"enabled": False}})
    assert r_warn.status_code == 200 and "done" in r_warn.text
    assert "Cron sync failed" in r_warn.text or "scheduler busy" in r_warn.text

    _cfg_dir(tmp_path, monkeypatch, {"provisioning": {"time_range": {"start": "2026-01-01", "end": "2026-01-31"}}})
    with patch("backend.core.metadata.record_audit"):
        assert client.delete(f"/api/services/{MOCK_SERVICE_ID}/time-range").status_code == 200
    assert "time_range" not in config.load_config(MOCK_SERVICE_ID)["provisioning"]


def test_cron_log_hooks_bust_cache():
    fake_cursor = MagicMock(
        rowcount=1,
        lastrowid=123,
        **{"fetchone.return_value": {"status": "running", "outcome_counters": None}, "fetchall.return_value": []},
    )
    fake_con = MagicMock(**{"execute.return_value": fake_cursor})

    with (
        patch("backend.core.metadata.cron_log.get_con", return_value=fake_con),
        patch("backend.cron_runs_publisher.publisher.publish"),
    ):
        for fn in (
            lambda: start_cron_run("svc-1", "sync"),
            lambda: log_cron_run("svc-1", "sync", 10.0, "success"),
            lambda: finalize_cron_run_if_running("svc-1", "sync", 123),
        ):
            _core._cron_schedule_cache["svc-1"] = (123.45, {"data": "foo"})
            fn()
            assert "svc-1" not in _core._cron_schedule_cache


# ── Cron Runs Router Tests ──
def _seed_cron_runs(service_id: str, runs: list[dict]) -> list[int]:
    con = metadata_db.get_con(service_id)
    ids: list[int] = []
    for r in runs:
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys, summary) "
            "VALUES (?, ?, ?, ?, ?, '[]', ?)",
            (
                service_id,
                r.get("task", "sync"),
                r.get("started_at", "2026-05-15T00:00:00Z"),
                r.get("duration_s", 1.0),
                r.get("status", "success"),
                r.get("summary", "ok"),
            ),
        )
        ids.append(int(cur.lastrowid or 0))
    con.commit()
    return ids


def test_cron_logs_list_filters_pagination_and_since_id(client, test_service_source):
    svc = test_service_source["name"]
    ids = _seed_cron_runs(
        svc,
        [
            {"task": "sync", "status": "success"},
            {"task": "sync", "status": "running"},
            {"task": "commit", "status": "error"},
        ],
    )
    r_all = client.get("/api/cron-runs", headers=_SVC_HEADERS).json()
    assert r_all["total"] == 3 and len(r_all["entries"]) == 3

    r_task = client.get("/api/cron-runs", params={"task": "sync"}, headers=_SVC_HEADERS).json()
    assert r_task["total"] == 2 and all(e["task"] == "sync" for e in r_task["entries"])

    r_status = client.get("/api/cron-runs", params={"status": "running"}, headers=_SVC_HEADERS).json()
    assert r_status["total"] == 1 and r_status["entries"][0]["status"] == "running"

    r_since = client.get("/api/cron-runs", params={"since_id": ids[2]}, headers=_SVC_HEADERS).json()
    assert {e["id"] for e in r_since["entries"]} == {ids[1]} and r_since["total"] == 1

    assert client.get("/api/cron-runs", params={"per_page": 9999}, headers=_SVC_HEADERS).status_code == 422
    assert client.get("/api/cron-runs", params={"since_id": -1}, headers=_SVC_HEADERS).status_code == 422

    _seed_cron_runs(svc, [{"task": "sync"} for _ in range(7)])
    r_page = client.get("/api/cron-runs", params={"page": 2, "per_page": 4}, headers=_SVC_HEADERS).json()
    assert (r_page["total"], r_page["page"], r_page["per_page"], len(r_page["entries"])) == (10, 2, 4, 4)


def test_cron_logs_delete_and_purge(client, test_service_source):
    svc = test_service_source["name"]
    [keep, kill, c1] = _seed_cron_runs(svc, [{"task": "sync"}, {"task": "sync"}, {"task": "commit"}])

    r_del = client.delete(f"/api/cron-runs/{kill}", headers=_SVC_HEADERS)
    assert r_del.status_code == 204 and r_del.content == b""
    assert client.delete("/api/cron-runs/9999999", headers=_SVC_HEADERS).status_code == 204

    con = metadata_db.get_con(svc)
    remaining = [row[0] for row in con.execute("SELECT id FROM cron_runs").fetchall()]
    assert kill not in remaining and keep in remaining and c1 in remaining

    assert client.delete("/api/cron-runs", params={"task": "sync"}, headers=_SVC_HEADERS).status_code == 204
    assert [row[0] for row in con.execute("SELECT task FROM cron_runs").fetchall()] == ["commit"]

    assert client.delete("/api/cron-runs", headers=_SVC_HEADERS).status_code == 204
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("attr", "method", "path", "expected_code"),
    [
        ("get_cron_logs", "GET", "/api/cron-runs", "cron_logs_read_failed"),
        ("delete_cron_log", "DELETE", "/api/cron-runs/1", "cron_log_delete_failed"),
        ("purge_cron_logs", "DELETE", "/api/cron-runs", "cron_logs_purge_failed"),
    ],
)
def test_cron_logs_500_on_repo_failure(client, monkeypatch, attr, method, path, expected_code):
    def _boom(*_a, **_kw):
        raise RuntimeError("simulated repo failure with internal SQL leak")

    monkeypatch.setattr(_cron_router, attr, _boom)
    r = client.request(method, path, headers=_SVC_HEADERS)
    assert r.status_code == 500
    detail = r.json()["detail"]
    assert detail["error"] == expected_code
    assert "error_id" in detail and len(detail["error_id"]) == 8
    assert "simulated repo failure" not in detail["error"]


# ── Cron Runs Stream Tests ──
def test_orphan_and_live_run_stream():
    async def mock_sleep(_delay):
        pass

    with patch("asyncio.sleep", mock_sleep), TestClient(app) as c:
        t0 = time.monotonic()
        with c.stream("GET", "/api/cron-runs/99999999/stream") as resp:
            assert resp.status_code == 200
            body = "".join(resp.iter_text())
        elapsed = time.monotonic() - t0

    events = _parse_sse_events(body)
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None and elapsed < 10.0
    assert "interrupted" in err["message"].lower() or "no live progress" in err["message"].lower()

    run_id = 42_424_242
    start_progress(run_id, service_id="test-svc", task="sync")
    _progress[run_id].extend([{"type": "status", "message": "starting"}, {"type": "done", "message": "all good"}])
    end_progress(run_id)
    try:
        with TestClient(app) as c, c.stream("GET", f"/api/cron-runs/{run_id}/stream") as resp:
            live_body = "".join(resp.iter_text())
    finally:
        _progress.pop(run_id, None)
        _run_metadata.pop(run_id, None)

    live_events = _parse_sse_events(live_body)
    assert "done" in [e.get("type") for e in live_events]
    assert "starting" in (live_events[0].get("message") or "")


def test_completed_run_streams_from_db_and_cross_tenant_isolation(isolate_metadata_db, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    service_id = "test-svc"
    config.save_config(service_id, {"name": "Test Service", "active": True})
    run_id = metadata_db.start_cron_run(service_id, "sync")
    metadata_db.log_cron_run(
        service_id=service_id,
        task="sync",
        duration_s=10.0,
        status="done",
        log_output="[STATUS] Syncing logs...\n[STATUS] Downloaded 5 files.\n[DONE] Ingested 123 rows.",
        run_id=run_id,
    )
    with (
        TestClient(app) as c,
        c.stream("GET", f"/api/cron-runs/{run_id}/stream", headers={"x-service-id": service_id}) as resp,
    ):
        assert resp.status_code == 200
        body = "".join(resp.iter_text())
    assert _parse_sse_events(body) == [
        {"type": "status", "message": "Syncing logs..."},
        {"type": "status", "message": "Downloaded 5 files."},
        {"type": "done", "message": "Ingested 123 rows."},
    ]

    iso_run_id = 99_999_888
    start_progress(iso_run_id, service_id="tenant-b-svc", task="sync")
    _progress[iso_run_id].extend([{"type": "status", "message": "tenant-b secretive logs"}])
    try:
        with (
            TestClient(app) as c,
            c.stream("GET", f"/api/cron-runs/{iso_run_id}/stream", headers={"x-service-id": "tenant-a-svc"}) as resp,
        ):
            iso_body = "".join(resp.iter_text())
    finally:
        _progress.pop(iso_run_id, None)
        _run_metadata.pop(iso_run_id, None)

    for event in _parse_sse_events(iso_body):
        assert "tenant-b" not in (event.get("message") or "")
