"""HTTP-layer contract tests for service mutation, logging settings, NGWAF sync, custom fields, and audit log endpoints."""

from __future__ import annotations

import copy
import json
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from unittest.mock import patch

import botocore.exceptions
import pytest
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES
from fastapi.testclient import TestClient
from pydantic import ValidationError

from backend import config
from backend.core import log_fields as lf_module
from backend.core import metadata as metadata_db
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.main import app
from backend.models.services import CronSettingsPartial
from backend.provision import fastly_api
from backend.routers.services import core as services_core
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.wire import parse_sse_events as _parse_sse_events
from tests.conftest import MOCK_SERVICE_ID

_BASE_CFG = {
    "service_id": "svc1",
    "name": "Test Service",
    "access_level": "read_write",
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
        stack.enter_context(patch("backend.state_sync.export_admin_state"))
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


def test_credentials_api_token_rejected_for_read_only_service():
    with patch("backend.config.load_config", return_value=_cfg(access_level="read_only")):
        response = _client().patch("/api/services/svc1/credentials", json={"api_token": "tok_abc"})
    assert response.status_code == 403


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


def test_custom_field_patch_rejects_type_change_when_field_in_iceberg():
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


def test_custom_field_patch_allows_type_change_before_iceberg_write():
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
        stack.enter_context(patch("backend.utils.ngwaf.oldest_unenriched_timestamp", return_value=oldest_ts))
        mock_fetch = stack.enter_context(
            fetch_patch if fetch_patch is not None else patch("backend.utils.ngwaf.fetch_verified_bots_paged")
        )
        if upsert_calls is not None:
            stack.enter_context(
                patch(
                    "backend.utils.ngwaf.upsert_bots",
                    side_effect=lambda rows, ws, ts: upsert_calls.append((list(rows), ws, ts)),
                )
            )
            stack.enter_context(patch("backend.utils.ngwaf.cleanup_old_bots", return_value=0))
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
        fetch_patch=patch("backend.utils.ngwaf.fetch_verified_bots_paged", return_value=iter(pages)),
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
        fetch_patch=patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=RuntimeError("NGWAF 503"))
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
        "access_level": "read_write",
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
    "access_level": "read_write",
    "fos_bucket": "test-bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
    "fos_prefix": "",
    "fos_access_key_id": "AKID",
    "fos_secret_access_key": "SECRET",
    "fastly_api_key": "stored-admin-token",
}

_FAKE_KEY = {"access_key": "ANALYST_KEY", "secret_key": "ANALYST_SECRET"}


def test_generate_viewer_key_returns_raw_credentials():
    with (
        patch("backend.config.load_config", return_value=_FAKE_CFG),
        patch("backend.provision.orchestrator.fastly", return_value=_FAKE_KEY),
        patch("backend.config.config_to_source", return_value={**_FAKE_CFG}),
    ):
        response = TestClient(app).post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 200
    data = response.json()
    assert data["access_key_id"] == "ANALYST_KEY" and data["secret_key"] == "ANALYST_SECRET"
    assert data["fos_bucket"] == "test-bucket"
    assert "invite_block" not in data and "invite_secret" not in data and "expires_at" not in data


def test_generate_viewer_key_requires_stored_api_key():
    with patch("backend.config.load_config", return_value={**_FAKE_CFG, "fastly_api_key": ""}):
        response = TestClient(app).post("/api/services/svc123/generate-viewer-key")
    assert response.status_code == 400


def test_generate_viewer_key_rejects_celery_topology():
    with (
        patch("backend.config.DEPLOYMENT_MODE", "high_scale"),
        patch("backend.config.load_config", return_value=_FAKE_CFG),
    ):
        response = TestClient(app).post("/api/services/svc123/generate-viewer-key")
    assert response.status_code == 409 and "Path B" in response.json()["detail"]["error"]


def test_generate_viewer_key_requires_read_write_service():
    with patch("backend.config.load_config", return_value={**_FAKE_CFG, "access_level": "read_only"}):
        response = TestClient(app).post("/api/services/svc123/generate-viewer-key")
    assert response.status_code == 403


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
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
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
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
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
