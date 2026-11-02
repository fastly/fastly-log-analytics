"""HTTP-layer contract tests for service mutation, logging settings, NGWAF sync, custom fields, and audit log endpoints."""

from __future__ import annotations

import copy
import json
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from unittest.mock import patch

import botocore.exceptions
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import backend.core.metadata as metadata_db
from backend import config
from backend.core import log_fields as lf_module
from backend.core.metadata.usage_log import CronLeaseHeldError
from backend.main import app
from backend.models.common import CronSettingsPartial
from backend.provision import fastly_api
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES
from backend.routers.services import core as services_core
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.wire import parse_sse_events as _parse_sse_events
from tests.conftest import MOCK_SERVICE_ID

_BASE_CFG = {
    "service_id": "svc1",
    "name": "Test Service",
    "fos_bucket": "my-bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
    "log_period": 60,
    "provisioning": {
        "cron_sync": {"enabled": True, "interval_mins": 1},
        "cron_compact": {"enabled": True, "commit_interval_mins": 60},
    },
}

_BASE_LF = {"preset": "standard", "groups": ["A", "B"], "schema_version": 2}

_FIELD_CREATE_BODY = {
    "name": "cf_env",
    "label": "Environment",
    "vcl_log_expression": "req.http.X-Env",
    "collection_stage": "edge",
    "duckdb_type": "VARCHAR",
    "value_type": "string",
    "bytes_estimate": 10,
    "enabled": True,
}

client = TestClient(app)


def _client():
    return TestClient(app)


def _cfg(**overrides):
    c = copy.deepcopy(_BASE_CFG)
    c.update(overrides)
    return c


def _cfg_with_field():
    cfg = _cfg()
    now = datetime.now(UTC).isoformat()
    cfg["log_fields"] = {
        "preset": "standard",
        "groups": ["A"],
        "schema_version": 2,
        "custom_fields": [
            {
                **_FIELD_CREATE_BODY,
                "description": "",
                "origin_log_frequency": "all",
                "nullable": True,
                "show_in_dashboard": False,
                "show_in_logs": True,
                "filterable": True,
                "created_at": now,
                "updated_at": now,
            }
        ],
    }
    return cfg


@contextmanager
def _mutate_cfg_ctx(initial_cfg: dict | None = None, *extra_patches):
    """Patch load_config, mutate_config, _sync_crontab, record_audit, and export_admin_state; yield saved dict."""
    saved: dict = {}

    def _save(_sid, cfg):
        saved.update(copy.deepcopy(cfg))

    with ExitStack() as stack:
        stack.enter_context(
            patch("backend.config.load_config", return_value=_cfg() if initial_cfg is None else initial_cfg)
        )
        stack.enter_context(patch("backend.config.mutate_config", side_effect=mutate_via_save(_save)))
        stack.enter_context(patch("backend.provision._sync_crontab"))
        stack.enter_context(patch("backend.core.metadata.record_audit"))
        stack.enter_context(patch("backend.core.metadata.base.export_admin_state"))
        for p in extra_patches:
            stack.enter_context(p)
        yield saved


# ── Credentials & Cron Settings ──
def test_credentials_access_key_saves_to_config():
    with _mutate_cfg_ctx(_cfg()) as saved, patch("backend.core.duckdb._get_fos_client") as mock_fos:
        mock_fos.return_value.list_objects_v2.return_value = {"Contents": []}
        response = _client().patch("/api/services/svc1/credentials", json={"access_key": "AKID", "secret_key": "SEC"})

    assert response.status_code == 200 and response.json()["ok"] is True
    assert saved["fos_access_key_id"] == "AKID" and saved["fos_secret_access_key"] == "SEC"


def test_credentials_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().patch("/api/services/missing/credentials", json={"access_key": "A", "secret_key": "B"})
    assert response.status_code == 404


def test_credentials_missing_both_keys_returns_400():
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().patch("/api/services/svc1/credentials", json={"access_key": "AKID"})
    assert response.status_code == 400


def test_credentials_fos_validation_failure_returns_400():
    error_response = {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}
    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.core.duckdb._get_fos_client") as mock_fos,
    ):
        mock_fos.return_value.list_objects_v2.side_effect = botocore.exceptions.ClientError(
            error_response, "ListObjectsV2"
        )
        response = _client().patch("/api/services/svc1/credentials", json={"access_key": "BAD", "secret_key": "BAD"})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "fos_credentials_invalid" and "Validation failed" in detail["message"]


def test_cron_settings_saves_enabled_flag():
    with _mutate_cfg_ctx(_cfg()) as saved:
        response = _client().post("/api/services/svc1/cron-settings", json={"cron_sync": {"enabled": False}})
    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "done" for e in events), events
    assert saved["provisioning"]["cron_sync"]["enabled"] is False


def test_cron_settings_saves_interval_mins():
    with _mutate_cfg_ctx(_cfg()) as saved:
        response = _client().post("/api/services/svc1/cron-settings", json={"cron_sync": {"interval_mins": 15}})
    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "done" for e in events), events
    assert saved["provisioning"]["cron_sync"]["interval_mins"] == 15


def test_cron_settings_service_not_found_yields_error():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post("/api/services/missing/cron-settings", json={"cron_sync": {"enabled": True}})
    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "error" for e in events), events


def test_cron_settings_only_saves_allowed_keys():
    with _mutate_cfg_ctx(_cfg()) as saved:
        _client().post("/api/services/svc1/cron-settings", json={"cron_sync": {"enabled": True, "rogue_key": "evil"}})
    assert "rogue_key" not in saved.get("provisioning", {}).get("cron_sync", {})


# ── Log Fields & Time Range ──
def test_log_fields_saves_to_config():
    with _mutate_cfg_ctx(_cfg(log_fields=_BASE_LF)) as saved:
        new_lf = {"preset": "standard", "groups": ["A", "B", "C"], "schema_version": 2}
        response = _client().post("/api/services/svc1/log-fields", json={"log_fields": new_lf})
    assert response.status_code == 200 and response.json()["ok"] is True
    assert "C" in saved["log_fields"]["groups"]


def test_log_fields_set_reinjects_cmcd_fields_when_cmcd_enabled():
    initial = _cfg(log_fields=_BASE_LF, cmcd={"enabled": True, "mode": "query_string", "version": 1})
    with _mutate_cfg_ctx(initial) as saved:
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={
                "log_fields": {
                    "preset": "standard",
                    "groups": ["A", "B", "C"],
                    "schema_version": 2,
                    "custom_fields": [{"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}],
                }
            },
        )

    assert response.status_code == 200
    saved_names = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} was stripped by a log-fields write"
    assert "my_custom" in saved_names


def test_log_fields_set_strips_cmcd_fields_when_cmcd_disabled():
    with _mutate_cfg_ctx(_cfg(log_fields=_BASE_LF)) as saved:
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={
                "log_fields": {
                    "preset": "standard",
                    "groups": ["A", "B", "C"],
                    "schema_version": 2,
                    "custom_fields": [
                        {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True},
                        {"name": "cmcd_sid", "duckdb_type": "VARCHAR", "enabled": True},
                    ],
                }
            },
        )

    assert response.status_code == 200
    assert {cf["name"] for cf in saved["log_fields"]["custom_fields"]} == {"my_custom"}


def test_log_fields_no_changes_detected():
    stored_lf = {**_BASE_LF, "format_hash": lf_module.format_hash(_BASE_LF)}
    with _mutate_cfg_ctx(_cfg(log_fields=stored_lf)) as saved:
        response = _client().post("/api/services/svc1/log-fields", json={"log_fields": _BASE_LF})

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True and "No changes" in data["message"]
    assert saved == {}


def test_log_fields_missing_log_fields_key_returns_422():
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().post("/api/services/svc1/log-fields", json={})
    assert response.status_code == 422


def test_log_fields_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post("/api/services/missing/log-fields", json={"log_fields": _BASE_LF})
    assert response.status_code == 404


def test_time_range_delete_clears_provisioning_entry():
    cfg = _cfg()
    cfg["provisioning"]["time_range"] = {"start": "2026-01-01", "end": "2026-02-01"}
    with _mutate_cfg_ctx(cfg) as saved:
        response = _client().delete("/api/services/svc1/time-range")
    assert response.status_code == 200 and response.json()["ok"] is True
    assert "time_range" not in saved.get("provisioning", {})


def test_time_range_delete_no_op_when_not_set():
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().delete("/api/services/svc1/time-range")
    assert response.status_code == 200 and "No time_range" in response.json()["message"]


def test_time_range_delete_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().delete("/api/services/missing/time-range")
    assert response.status_code == 404


# ── Custom Fields CRUD & Type Evolution ──
def test_custom_field_create_saves_to_config():
    cfg_with_lf = _cfg(log_fields={"preset": "standard", "groups": ["A"], "schema_version": 2})
    with _mutate_cfg_ctx(cfg_with_lf, patch("backend.provision.validate_log_format", return_value=[])) as saved:
        response = _client().post("/api/services/svc1/custom-fields", json=_FIELD_CREATE_BODY)

    assert response.status_code == 200
    assert response.json()["field"]["name"] == "cf_env"
    assert any(f["name"] == "cf_env" for f in saved["log_fields"]["custom_fields"])


def test_custom_field_create_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post("/api/services/missing/custom-fields", json=_FIELD_CREATE_BODY)
    assert response.status_code == 404


def test_custom_field_delete_removes_field():
    with _mutate_cfg_ctx(_cfg_with_field()) as saved:
        response = _client().delete("/api/services/svc1/custom-fields/cf_env")
    assert response.status_code == 200
    assert all(f["name"] != "cf_env" for f in saved["log_fields"]["custom_fields"])


def test_custom_field_delete_missing_field_returns_404():
    with patch("backend.config.load_config", return_value=_cfg_with_field()):
        response = _client().delete("/api/services/svc1/custom-fields/nonexistent")
    assert response.status_code == 404


def test_custom_field_delete_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().delete("/api/services/missing/custom-fields/cf_env")
    assert response.status_code == 404


def test_custom_field_patch_updates_label():
    with _mutate_cfg_ctx(_cfg_with_field(), patch("backend.provision.validate_log_format", return_value=[])):
        response = _client().patch("/api/services/svc1/custom-fields/cf_env", json={"label": "Env Tag"})
    assert response.status_code == 200
    updated = response.json()["field"]
    assert updated["label"] == "Env Tag" and updated["name"] == "cf_env"


def test_custom_field_patch_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().patch("/api/services/missing/custom-fields/cf_env", json={"label": "x"})
    assert response.status_code == 404


def test_custom_field_patch_missing_field_returns_404():
    with patch("backend.config.load_config", return_value=_cfg_with_field()):
        response = _client().patch("/api/services/svc1/custom-fields/nonexistent", json={"label": "x"})
    assert response.status_code == 404


def test_custom_field_patch_rejects_type_change_when_field_in_storage():
    with _mutate_cfg_ctx(
        _cfg_with_field(),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch("backend.routers.services.core._committed_duckdb_field_types", return_value={"cf_env": "VARCHAR"}),
    ) as saved:
        response = _client().patch(
            "/api/services/svc1/custom-fields/cf_env",
            json={"duckdb_type": "BIGINT", "value_type": "numeric"},
        )

    assert response.status_code == 422
    errors = response.json().get("detail", {}).get("errors", [])
    assert any("duckdb_type" in e.lower() or "data type" in e.lower() for e in errors)
    assert saved == {}


def test_custom_field_patch_allows_type_change_before_storage_write():
    with _mutate_cfg_ctx(
        _cfg_with_field(),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch("backend.routers.services.core._committed_duckdb_field_types", return_value={}),
    ) as saved:
        response = _client().patch(
            "/api/services/svc1/custom-fields/cf_env",
            json={"duckdb_type": "BIGINT", "value_type": "numeric"},
        )

    assert response.status_code == 200
    updated = response.json()["field"]
    assert updated["duckdb_type"] == "BIGINT" and updated["value_type"] == "numeric"
    assert saved


# ── API Token Rotation ──
def test_credentials_api_token_rotation_creates_new_fos_key_and_saves():
    starting_cfg = _cfg()
    starting_cfg["provisioning"]["fos_key_id"] = "OLD_KEY"
    fastly_calls = []

    def fake_fastly(method, path, body=None, token=None, **kw):
        fastly_calls.append((method, path))
        if method == "POST":
            return {"access_key": "NEW_AK", "secret_key": "NEW_SK"}
        if method == "DELETE":
            return None
        raise AssertionError(f"unexpected fastly call: {method} {path}")

    with _mutate_cfg_ctx(starting_cfg, patch("backend.core.fastly.client.fastly", side_effect=fake_fastly)) as saved:
        resp = _client().patch("/api/services/svc1/credentials", json={"api_token": "tok_rotate_me"})

    assert resp.status_code == 200 and resp.json()["access_key_id"] == "NEW_AK"
    assert saved["fos_access_key_id"] == "NEW_AK" and saved["fos_secret_access_key"] == "NEW_SK"
    assert saved["provisioning"]["fos_key_id"] == "NEW_AK"
    methods = [m for m, _ in fastly_calls]
    assert "POST" in methods and "DELETE" in methods


def test_credentials_api_token_create_failure_returns_400():
    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly 401 unauthorized")),
    ):
        resp = _client().patch("/api/services/svc1/credentials", json={"api_token": "bad_token"})

    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "fos_key_create_failed" and "error_id" in detail


def test_credentials_api_token_skips_delete_when_old_key_same_as_new():
    starting_cfg = _cfg()
    starting_cfg["provisioning"]["fos_key_id"] = "SAME_KEY"
    fastly_calls = []

    def fake_fastly(method, path, body=None, token=None, **kw):
        fastly_calls.append((method, path))
        if method == "POST":
            return {"access_key": "SAME_KEY", "secret_key": "NEW_SK"}
        raise AssertionError(f"unexpected fastly call: {method} {path}")

    with _mutate_cfg_ctx(starting_cfg, patch("backend.core.fastly.client.fastly", side_effect=fake_fastly)):
        _client().patch("/api/services/svc1/credentials", json={"api_token": "tok"})

    assert [m for m, _ in fastly_calls] == ["POST"]


# ── NGWAF Sync ──
def test_ngwaf_sync_service_not_found_yields_error_event():
    with patch("backend.config.load_config", return_value=None):
        resp = _client().post("/api/services/missing/ngwaf-sync")
    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "Service not found" in e.get("message", "") for e in events)


def test_ngwaf_sync_no_workspace_yields_error_event():
    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")
    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "No NGWAF workspace" in e.get("message", "") for e in events)


def test_ngwaf_sync_no_api_key_yields_error_event():
    with (
        patch("backend.config.load_config", return_value=_cfg(fastly_api_key="")),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")
    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "No Fastly API key" in e.get("message", "") for e in events)


def test_ngwaf_sync_source_missing_yields_error_event():
    with (
        patch("backend.config.load_config", return_value=_cfg(fastly_api_key="key")),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")
    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "Service source not found" in e.get("message", "") for e in events)


def test_ngwaf_sync_start_cron_run_raises_yields_error_event():
    with (
        patch("backend.config.load_config", return_value=_cfg(fastly_api_key="key")),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "service_id": "svc1"}),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("already running")),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")
    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "already running" in e.get("message", "") for e in events)


@contextmanager
def _ngwaf_sync_ctx(
    *,
    oldest_ts: str | None = "2025-12-01T00:00:00Z",
    matcher=None,
    fetch_patch=None,
    upsert_calls: list | None = None,
):
    log_calls: list = []
    with ExitStack() as stack:
        stack.enter_context(patch("backend.config.load_config", return_value=_cfg(fastly_api_key="key")))
        stack.enter_context(patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"))
        stack.enter_context(
            patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "service_id": "svc1"})
        )
        stack.enter_context(patch("backend.core.duckdb.start_cron_run", return_value=42))
        stack.enter_context(
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **kw: log_calls.append((a, kw)))
        )
        stack.enter_context(patch("backend.utils.bot_sources.build_matcher", return_value=matcher or (lambda ua: ())))
        stack.enter_context(
            patch("backend.core.rollups.security_dims.oldest_unenriched_timestamp", return_value=oldest_ts)
        )
        mock_fetch = stack.enter_context(
            fetch_patch
            if fetch_patch is not None
            else patch("backend.core.rollups.security_dims.fetch_verified_bots_paged")
        )
        if upsert_calls is not None:
            stack.enter_context(
                patch(
                    "backend.core.rollups.security_dims.upsert_bots",
                    side_effect=lambda rows, ws, ts: upsert_calls.append((list(rows), ws, ts)),
                )
            )
            stack.enter_context(patch("backend.core.rollups.security_dims.cleanup_old_bots", return_value=0))
        yield log_calls, mock_fetch


def test_ngwaf_sync_no_pending_records_yields_already_enriched_done():
    with _ngwaf_sync_ctx(oldest_ts=None) as (log_calls, mock_fetch):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "done" and "already enriched" in e.get("message", "").lower() for e in events)
    mock_fetch.assert_not_called()
    assert len(log_calls) == 1 and log_calls[0][1].get("summary", "").lower().startswith("all requests")


def test_ngwaf_sync_happy_path_streams_page_status_then_done():
    upsert_calls: list = []
    pages = [([{"user_agent": "GoogleBot/2", "server_name": "www"}], "2026-05-01T00:00:00Z", 5)]

    with _ngwaf_sync_ctx(
        matcher=lambda ua: ({"id": "googlebot", "name": "Google Bot"},) if "Google" in (ua or "") else (),
        fetch_patch=patch("backend.core.rollups.security_dims.fetch_verified_bots_paged", return_value=iter(pages)),
        upsert_calls=upsert_calls,
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    statuses = [e for e in events if e.get("type") == "status"]
    dones = [e for e in events if e.get("type") == "done"]
    assert any("Scanning" in s.get("message", "") for s in statuses)
    assert any("Page 1" in s.get("message", "") for s in statuses)
    assert any("Synced 1" in d.get("message", "") for d in dones)
    assert upsert_calls[0][0][0]["wellknown_bot_id"] == "googlebot"


def test_ngwaf_sync_fetcher_exception_yields_error_event_and_logs():
    with _ngwaf_sync_ctx(
        fetch_patch=patch(
            "backend.core.rollups.security_dims.fetch_verified_bots_paged", side_effect=RuntimeError("NGWAF 503")
        )
    ) as (log_calls, _):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "NGWAF 503" in e.get("message", "") for e in events)
    assert any(
        (kwargs.get("status") == "error" or (len(args) > 3 and args[3] == "error")) for args, kwargs in log_calls
    )


# ── CronSettingsPartial Model & Persistence ──
def _post(body: dict, stored_cron_sync: dict) -> dict:
    cfg = {
        "service_id": "svc1",
        "name": "Test Service",
        "log_period": 60,
        "provisioning": {"cron_sync": dict(stored_cron_sync), "cron_compact": {"enabled": True}},
    }
    with _mutate_cfg_ctx(cfg) as saved:
        resp = TestClient(app).post("/api/services/svc1/cron-settings", json=body)
    assert resp.status_code == 200, resp.text
    assert any('"done"' in line for line in resp.text.splitlines()), resp.text
    return saved["provisioning"]["cron_sync"]


def test_interval_seconds_is_modelled():
    assert "interval_seconds" in CronSettingsPartial.model_fields
    assert CronSettingsPartial(**json.loads('{"interval_seconds": 90}')).interval_seconds == 90


def test_polling_mode_is_modelled_and_persisted():
    assert CronSettingsPartial(**json.loads('{"polling_mode": "adaptive"}')).polling_mode == "adaptive"
    assert _post({"cron_sync": {"polling_mode": "adaptive"}}, {"enabled": True})["polling_mode"] == "adaptive"


def test_polling_mode_rejects_unknown_values():
    with pytest.raises(ValidationError, match="polling_mode"):
        CronSettingsPartial(**json.loads('{"polling_mode": "continuous"}'))


def test_interval_seconds_is_persisted():
    out = _post({"cron_sync": {"interval_seconds": 90}}, {"enabled": True, "interval_seconds": 30})
    assert out["interval_seconds"] == 90


def test_setting_interval_seconds_drops_stale_interval_mins():
    out = _post({"cron_sync": {"interval_seconds": 45}}, {"enabled": True, "interval_seconds": 30, "interval_mins": 2})
    assert out["interval_seconds"] == 45 and "interval_mins" not in out


def test_explicit_interval_mins_still_wins_when_both_sent():
    out = _post({"cron_sync": {"interval_mins": 5, "interval_seconds": 45}}, {"enabled": True, "interval_seconds": 30})
    assert out["interval_mins"] == 5 and out["interval_seconds"] == 45


def test_unrelated_keys_are_preserved():
    out = _post(
        {"cron_sync": {"interval_seconds": 90}},
        {"enabled": True, "interval_seconds": 30, "data_retention_days": 120, "delete_after": True},
    )
    assert out["data_retention_days"] == 120 and out["delete_after"] is True and out["enabled"] is True


def test_expiry_tuning_fields_round_trip():
    out = _post(
        {"cron_sync": {"keep_snapshot_days": 3, "expire_interval_mins": 30}},
        {"enabled": True, "interval_seconds": 30},
    )
    assert out["keep_snapshot_days"] == 3 and out["expire_interval_mins"] == 30


# ── Viewer Key Generation ──
_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "fos_bucket": "test-bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
    "fos_prefix": "",
    "fos_access_key_id": "AKID",
    "fos_secret_access_key": "SECRET",
    "fastly_api_key": "stored-admin-token",
}

_FAKE_KEY = {"access_key": "ANALYST_KEY", "secret_key": "ANALYST_SECRET"}


def test_decrypt_invite_endpoint_does_not_exist():
    response = TestClient(app).post("/api/provision/decrypt-invite", json={"invite_block": "x", "secret_key": "y"})
    assert response.status_code in (404, 405)


# ── Logging Settings & Cache Pre-warming ──
def _seed_cache(service_id: str, **overrides) -> dict:
    payload = {
        "prefix": "stale_prefix",
        "period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "custom_condition": "",
        "format_match": False,
        "version": 1,
        **overrides,
    }
    services_core._logging_settings_cache[service_id] = payload
    return payload


@patch("backend.config.load_config")
@patch("backend.core.fastly.client.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.client.find_condition")
def test_logging_settings_extracts_custom_condition(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {"endpoint_name": "Test Endpoint"},
    }
    mock_get_active_version.return_value = 1
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 120,
        "path": "/logs/raw/%Y-%m-%d/",
        "response_condition": "Log Sampling",
    }
    mock_find_condition.return_value = {
        "statement": '!segmented_caching.is_inner_req && (req.restarts == 0 && fastly.ff.visits_this_service == 0) && randombool(50, 100) && (std.tolower(req.url) !~ "\\.(jpg|png)$")'
    }

    response = client.get("/api/services/test-service/logging-settings")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True and data["period"] == 120 and data["sample_rate"] == 50 and data["edge_only"] is True
    assert data["custom_condition"] == 'std.tolower(req.url) !~ "\\.(jpg|png)$"'


@patch("backend.config.load_config")
@patch("backend.core.fastly.client.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.client.find_condition")
def test_logging_settings_custom_condition_fallback(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {"endpoint_name": "Test Endpoint", "custom_condition": 'req.http.host == "example.com"'},
    }
    mock_get_active_version.return_value = 1
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 60,
        "path": "/logs/raw/",
        "response_condition": "Log Sampling",
    }
    mock_find_condition.return_value = {"statement": "!segmented_caching.is_inner_req && randombool(100, 100)"}

    response = client.get("/api/services/test-service/logging-settings")
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True and data["custom_condition"] == 'req.http.host == "example.com"'


def _run_logging_settings_update(
    service_id: str,
    params: dict,
    *,
    events: list[dict],
    initial_state: dict | None = None,
    mid_stream_hook=None,
) -> tuple[dict, dict]:
    disk_state = copy.deepcopy(
        initial_state
        or {
            "service_id": service_id,
            "log_period": 60,
            "provisioning": {"sample_rate": 100, "edge_only": False},
        }
    )
    captured_cfg: dict = {}

    def _load(_sid):
        return copy.deepcopy(disk_state)

    def _save(_sid, new_cfg):
        nonlocal disk_state
        disk_state = copy.deepcopy(new_cfg)

    def _gen(cfg, _token):
        captured_cfg.update(cfg)
        for idx, ev in enumerate(events):
            yield ev
            if idx == 0 and mid_stream_hook is not None:
                mid_stream_hook(disk_state)

    with (
        patch("backend.config.load_config", side_effect=_load),
        patch("backend.config.save_config", side_effect=_save) as mock_save,
        patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save)),
        patch("backend.provision.update_logging_endpoint", side_effect=_gen),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        response = client.post(f"/api/services/{service_id}/logging-settings/update", params=params)

    assert response.status_code == 200
    return captured_cfg, disk_state


def test_update_logging_settings_passes_custom_condition():
    captured_cfg, disk_state = _run_logging_settings_update(
        "test-service",
        {"period": 120, "sample_rate": 25, "edge_only": True, "custom_condition": 'req.http.x-test == "1"'},
        events=[{"type": "status", "message": "starting"}, {"type": "done", "changed": True}],
    )
    assert captured_cfg["log_period"] == 120 and captured_cfg["sample_rate"] == 25 and captured_cfg["edge_only"] is True
    assert captured_cfg["custom_condition"] == 'req.http.x-test == "1"'
    assert disk_state["provisioning"]["custom_condition"] == 'req.http.x-test == "1"'


def test_update_pre_warms_cache_on_changed_with_format():
    _seed_cache("svc-prewarm", version=1, format_match=False, period=60)
    _run_logging_settings_update(
        "svc-prewarm",
        {
            "period": 120,
            "sample_rate": 25,
            "edge_only": True,
            "custom_condition": 'req.http.x == "1"',
            "update_format": True,
        },
        events=[{"type": "status", "message": "starting"}, {"type": "done", "changed": True, "version": 42}],
    )
    cached = services_core._logging_settings_cache.get("svc-prewarm")
    assert cached == {
        "period": 120,
        "sample_rate": 25,
        "prefix": "",
        "edge_only": True,
        "custom_condition": 'req.http.x == "1"',
        "format_match": True,
        "version": 42,
        "cmcd": services_core.CmcdSettingsResponse(enabled=False, mode=None, version=None),
    }


def test_update_leaves_cache_on_no_change():
    seeded = _seed_cache("svc-nochange", version=7, format_match=True, period=60)
    _run_logging_settings_update(
        "svc-nochange",
        {"period": 60, "sample_rate": 100, "update_format": True},
        events=[{"type": "done", "changed": False, "version": 7}],
    )
    assert services_core._logging_settings_cache.get("svc-nochange") == seeded


def test_update_pops_cache_when_format_not_deployed():
    _seed_cache("svc-fallback", version=8, format_match=True)
    _run_logging_settings_update(
        "svc-fallback",
        {"period": 120, "update_format": False},
        events=[{"type": "done", "changed": True, "version": 9}],
    )
    assert services_core._logging_settings_cache.get("svc-fallback") is None


def test_long_running_endpoint_preserves_background_changes():
    _, disk_state = _run_logging_settings_update(
        "test-service",
        {"period": 3600, "sample_rate": 50, "edge_only": True},
        events=[{"type": "status", "message": "starting"}, {"type": "done", "changed": True}],
        initial_state={
            "service_id": "test-service",
            "name": "Initial Name",
            "log_period": 60,
            "provisioning": {"sample_rate": 100, "edge_only": False},
        },
        mid_stream_hook=lambda state: state.__setitem__("name", "Updated Background Name"),
    )
    assert disk_state["log_period"] == 3600
    assert disk_state["provisioning"]["sample_rate"] == 50 and disk_state["provisioning"]["edge_only"] is True
    assert disk_state["name"] == "Updated Background Name"


# ── Catalog & Custom Field Validation ──
def test_get_catalog():
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    assert "METRICS" in [g["id"] for g in data["groups"]]
    fields = data["fields"]
    field_ids = [f["id"] for f in fields]

    for rm in (
        "requests",
        "hit_rate",
        "5xx",
        "4xx",
        "p50_latency",
        "p95_latency",
        "p99_latency",
        "throughput",
        "req_size",
        "ttfb_ms",
    ):
        assert rm in field_ids
        assert next(f for f in fields if f["id"] == rm)["group"] == "METRICS"

    hit_rate = next(f for f in fields if f["id"] == "hit_rate")
    assert hit_rate["formatter"] == "percent" and hit_rate["unit"] == "%"


def test_api_create_custom_field_validation_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    svc_id = "test_svc_validation"
    config.save_config(
        svc_id, {"service_id": svc_id, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}}
    )

    payload = {**_BASE_PAYLOAD, "name": "INVALID NAME!", "label": "Invalid"}
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422 and "detail" in resp.json()

    payload["name"] = "valid_name"
    payload["vcl_log_expression"] = "req.http.Host;"
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    assert any("semicolon" in e for e in resp.json()["detail"]["errors"])

    payload["vcl_log_expression"] = "req.http.Host"
    assert client.post(f"/api/services/{svc_id}/custom-fields", json=payload).status_code == 200

    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    assert any("already exists" in e for e in resp.json()["detail"]["errors"])


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.core.duckdb.get_source_for_service")
def test_custom_fields_persist_on_log_fields_update(mock_get_source, mock_save_config, mock_load_config):
    mock_get_source.return_value = None
    mock_load_config.return_value = {
        "log_fields": {
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom_field", "label": "My Custom Field", "collection_stage": "edge", "enabled": True}
            ],
        }
    }

    with patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save_config)):
        response = client.post(
            "/api/services/test-service/log-fields",
            json={"log_fields": {"groups": ["A", "B", "C"], "field_overrides": {}}},
        )

    assert response.status_code == 200
    mock_save_config.assert_called_once()
    saved_lf = mock_save_config.call_args[0][1]["log_fields"]
    assert [cf["name"] for cf in saved_lf["custom_fields"]] == ["my_custom_field"]
    assert saved_lf.get("schema_version") == 2


# ── Custom Field Count Limits, Audit Logs & VCL Validation ──
_BASE_PAYLOAD = {
    "label": "L",
    "description": "",
    "vcl_log_expression": "req.http.Host",
    "collection_stage": "edge",
    "duckdb_type": "VARCHAR",
    "value_type": "string",
    "bytes_estimate": 20,
    "nullable": True,
    "enabled": True,
    "show_in_dashboard": False,
    "show_in_logs": True,
    "filterable": True,
}


def _seed_custom_field(i: int) -> dict:
    return {
        **_BASE_PAYLOAD,
        "name": f"cf{i:03d}",
        "created_at": "2026-05-27T00:00:00+00:00",
        "updated_at": "2026-05-27T00:00:00+00:00",
    }


def _make_cfg(svc_id: str, n_existing: int) -> dict:
    return {
        "service_id": svc_id,
        "log_fields": {
            "schema_version": 2,
            "groups": ["A"],
            "custom_fields": [_seed_custom_field(i) for i in range(n_existing)],
        },
    }


def _post_payload(field_name: str) -> dict:
    return {**_BASE_PAYLOAD, "name": field_name}


def _at_template_limit(svc_id: str) -> dict:
    for count in range(1000):
        overflow = _make_cfg(svc_id, n_existing=count + 1)
        if len(fastly_api.load_log_format(overflow["log_fields"])) > fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX:
            cfg = _make_cfg(svc_id, n_existing=count)
            assert len(fastly_api.load_log_format(cfg["log_fields"])) <= fastly_api.FASTLY_LOG_FORMAT_SAFE_MAX
            return cfg
    raise AssertionError("The fixture did not reach the template size limit")


def _seed_audit(source_name: str, n: int = 3, event_type: str = "service_renamed") -> None:
    for i in range(n):
        metadata_db.record_audit(source_name, event_type=event_type, details={"i": i})


_CFG = {
    "service_id": "svc-lint",
    "name": "Lint Test",
    "log_fields": {
        "preset": "standard",
        "groups": ["A"],
        "schema_version": 2,
        "custom_fields": [],
    },
}


class TestCustomFieldCountLimits:
    @pytest.fixture(autouse=True)
    def force_8000_safe_max(self, monkeypatch, request):
        if request.node.name.startswith("test_validate_vcl"):
            return
        monkeypatch.setattr(fastly_api, "FASTLY_LOG_FORMAT_SAFE_MAX", 8000)

    def test_no_numeric_cap_field_count_still_accepted(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_count_cap"
        config.save_config(svc_id, _make_cfg(svc_id, n_existing=78))

        with patch("backend.provision.fastly_api.shutil.which", return_value=None):
            resp = TestClient(app).post(f"/api/services/{svc_id}/custom-fields", json=_post_payload("cf078"))
        assert resp.status_code == 200
        assert resp.json()["field"]["name"] == "cf078"
        assert len(config.load_config(svc_id)["log_fields"]["custom_fields"]) == 79

    def test_creating_field_that_overflows_log_format_returns_422(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_logfmt_overflow"
        cfg = _at_template_limit(svc_id)
        count = len(cfg["log_fields"]["custom_fields"])
        config.save_config(svc_id, cfg)

        resp = TestClient(app).post(f"/api/services/{svc_id}/custom-fields", json=_post_payload(f"cf{count:03d}"))
        assert resp.status_code == 422
        errors: list[str] = resp.json()["detail"]["errors"]
        assert any("LOG_FORMAT_TOO_LONG" in e for e in errors)
        assert config.load_config(svc_id)["log_fields"]["custom_fields"] == cfg["log_fields"]["custom_fields"]

    def test_log_format_overflow_error_reports_chars_and_safe_max(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_logfmt_msg"
        config.save_config(svc_id, _make_cfg(svc_id, n_existing=118))

        resp = TestClient(app).post(f"/api/services/{svc_id}/custom-fields", json=_post_payload("cf118"))
        assert resp.status_code == 422
        errors: list[str] = resp.json()["detail"]["errors"]
        msg = next((e for e in errors if "LOG_FORMAT_TOO_LONG" in e), None)
        assert msg is not None and "chars" in msg and "8000" in msg

    def test_drop_a_field_at_the_size_limit_then_re_add_succeeds(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_drop_readd"
        cfg = _at_template_limit(svc_id)
        fields = cfg["log_fields"]["custom_fields"]
        count = len(fields)
        removed_name = fields[count // 2]["name"]
        config.save_config(svc_id, cfg)

        tc = TestClient(app)
        assert tc.post(f"/api/services/{svc_id}/custom-fields", json=_post_payload(f"cf{count:03d}")).status_code == 422
        assert tc.delete(f"/api/services/{svc_id}/custom-fields/{removed_name}").status_code == 200

        remaining = [cf["name"] for cf in config.load_config(svc_id)["log_fields"]["custom_fields"]]
        assert removed_name not in remaining and len(remaining) == count - 1

        with patch("backend.provision.fastly_api.shutil.which", return_value=None):
            resp = tc.post(f"/api/services/{svc_id}/custom-fields", json=_post_payload("cf999"))
        assert resp.status_code == 200

        names = {cf["name"] for cf in config.load_config(svc_id)["log_fields"]["custom_fields"]}
        assert removed_name not in names and "cf999" in names and len(names) == count

    def test_audit_logs_returns_seeded_entries(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=3, event_type="service_renamed")
        r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 3 and len(body["entries"]) == 3
        assert all(e["event_type"] == "service_renamed" for e in body["entries"])

    def test_audit_logs_filters_by_event_type(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=2, event_type="service_renamed")
        _seed_audit(test_service_source["name"], n=4, event_type="time_range_cleared")

        r = client.get(
            "/api/audit-logs",
            params={"event_type": "time_range_cleared"},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 4 and all(e["event_type"] == "time_range_cleared" for e in body["entries"])

    def test_audit_logs_pagination(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=10)
        r = client.get(
            "/api/audit-logs", params={"page": 2, "per_page": 3}, headers={"x-fastly-service-id": MOCK_SERVICE_ID}
        )
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 10 and body["page"] == 2 and body["per_page"] == 3 and len(body["entries"]) == 3

    def test_audit_logs_per_page_validation(self, client):
        r = client.get("/api/audit-logs", params={"per_page": 5000}, headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert r.status_code == 422

    def test_audit_logs_page_must_be_positive(self, client):
        r = client.get("/api/audit-logs", params={"page": 0}, headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert r.status_code == 422

    def test_audit_logs_default_dir_is_descending(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=5)
        r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert r.status_code == 200
        timestamps = [e["timestamp"] for e in r.json()["entries"]]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_validate_vcl_404_for_missing_service(self):
        with patch("backend.config.load_config", return_value=None):
            r = _client().post(
                "/api/services/missing/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
            )
        assert r.status_code == 404

    def test_validate_vcl_valid_expression_returns_valid_true(self):
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is True and body["errors"] == []
        assert isinstance(body["warnings"], list) and isinstance(body["format_length"], int)
        assert body["format_length"] > 0 and body["format_length_limit"] == 12000

    def test_validate_vcl_invalid_expression_returns_errors(self):
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.{{ not closed", "collection_stage": "edge"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is False and len(body["errors"]) >= 1

    def test_validate_vcl_response_shape_matches_drawer_contract(self):
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
            )
        missing = {"valid", "errors", "warnings", "format_length_limit"} - set(r.json().keys())
        assert not missing, f"drawer-contract response keys missing: {missing}"

    def test_validate_vcl_collection_stage_origin_accepted(self):
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "beresp.http.x-something", "collection_stage": "origin"},
            )
        assert r.status_code == 200 and "valid" in r.json()

    def test_validate_vcl_rejects_unknown_collection_stage(self):
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "midway"},
            )
        assert r.status_code == 422


# ── Merged from tests/routers/test_services_get_and_cron.py ──

"""Tests for ``backend.routers.services.core`` and ``backend.routers.services.core``."""


import os
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import backend.routers.services.core as _cron_router
from backend.core.metadata.usage_log import (
    finalize_cron_run_if_running,
    log_cron_run,
    start_cron_run,
)
from backend.cron.scheduler import _progress, _run_metadata, end_progress, start_progress
from backend.main import app
from backend.routers.services import core as _core
from backend.services import service_manager
from backend.services.service_manager import _get_dir_stats

_SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


@pytest.fixture()
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
def test_services_list(client):
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
def test_logging_settings_errors(client, tmp_path, monkeypatch):
    _cfg_dir(tmp_path, monkeypatch)
    assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 404

    _cfg_dir(tmp_path, monkeypatch, {"fastly_api_key": "tok"})
    with patch("backend.core.fastly.client.get_active_version", return_value=None):
        assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 400

    with patch("backend.core.fastly.client.get_active_version", side_effect=RuntimeError("network")):
        assert client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS).status_code == 500


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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
        patch("backend.core.fastly.client.get_active_version", return_value=42),
        patch("backend.core.fastly.client.fastly", return_value=fake_ep),
        patch("backend.core.fastly.client.find_condition", return_value=fake_cond),
    ):
        resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/logging-settings", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    for k, v in expected.items():
        assert body[k] == v


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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
        patch("backend.core.log_fields.validate_custom_field", return_value=[]),
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
            "backend.core.log_fields.validate_custom_field",
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
def test_cron_log_hooks_bust_cache():
    fake_cursor = MagicMock(
        rowcount=1,
        lastrowid=123,
        **{"fetchone.return_value": {"status": "running", "outcome_counters": None}, "fetchall.return_value": []},
    )
    fake_con = MagicMock(**{"execute.return_value": fake_cursor})

    with (
        patch("backend.core.metadata.usage_log.get_con", return_value=fake_con),
        patch("backend.cron.scheduler.publisher.publish"),
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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
@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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


@pytest.mark.usefixtures("_clear_cron_schedule_ttl_cache")
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
