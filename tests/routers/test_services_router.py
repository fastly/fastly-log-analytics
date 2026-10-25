"""HTTP-layer contract tests for service mutation endpoints.

These tests verify that:
  - request body fields are read from the correct location (body vs query param)
  - the correct config keys are written on save
  - error codes match the documented contract (400/403/404/422)

All tests mock the config layer and any external I/O so they run without
real credentials, a database file, or network access.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config
from backend.core import metadata as metadata_db
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.main import app
from backend.models.services import CronSettingsPartial
from backend.provision import fastly_api
from backend.routers.services import core as services_core
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.wire import parse_sse_events as _parse_sse_events
from tests.conftest import MOCK_SERVICE_ID

# ── Merged from tests/routers/test_service_mutations.py ──
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


def _client():
    return TestClient(app)


def _cfg(**overrides):
    import copy

    c = copy.deepcopy(_BASE_CFG)
    c.update(overrides)
    return c


def test_credentials_access_key_saves_to_config():
    """Supplying access_key + secret_key in the body saves them to config."""
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.core.duckdb._get_fos_client") as mock_fos,
    ):
        mock_fos.return_value.list_objects_v2.return_value = {"Contents": []}
        response = _client().patch(
            "/api/services/svc1/credentials",
            json={"access_key": "AKID", "secret_key": "SEC"},
        )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert saved["fos_access_key_id"] == "AKID"
    assert saved["fos_secret_access_key"] == "SEC"


def test_credentials_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().patch(
            "/api/services/missing/credentials",
            json={"access_key": "A", "secret_key": "B"},
        )
    assert response.status_code == 404


def test_credentials_missing_both_keys_returns_400():
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().patch(
            "/api/services/svc1/credentials",
            json={"access_key": "AKID"},  # missing secret_key
        )
    assert response.status_code == 400


def test_credentials_api_token_rejected_for_read_only_service():
    with patch("backend.config.load_config", return_value=_cfg(access_level="read_only")):
        response = _client().patch(
            "/api/services/svc1/credentials",
            json={"api_token": "tok_abc"},
        )
    assert response.status_code == 403


def test_credentials_fos_validation_failure_returns_400():
    """Invalid FOS credentials (AccessDenied) → 400."""
    import botocore.exceptions

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.core.duckdb._get_fos_client") as mock_fos,
    ):
        mock_fos.return_value.list_objects_v2.side_effect = botocore.exceptions.ClientError(
            error_response, "ListObjectsV2"
        )
        response = _client().patch(
            "/api/services/svc1/credentials",
            json={"access_key": "BAD", "secret_key": "BAD"},
        )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "fos_credentials_invalid"
    assert "Validation failed" in detail["message"]


def test_cron_settings_saves_enabled_flag():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        response = _client().post(
            "/api/services/svc1/cron-settings",
            json={"cron_sync": {"enabled": False}},
        )

    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "done" for e in events), events
    assert saved["provisioning"]["cron_sync"]["enabled"] is False


def test_cron_settings_saves_interval_mins():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        response = _client().post(
            "/api/services/svc1/cron-settings",
            json={"cron_sync": {"interval_mins": 15}},
        )

    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "done" for e in events), events
    assert saved["provisioning"]["cron_sync"]["interval_mins"] == 15


def test_cron_settings_service_not_found_yields_error():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post(
            "/api/services/missing/cron-settings",
            json={"cron_sync": {"enabled": True}},
        )

    events = _parse_sse_events(response.text)
    assert any(e.get("type") == "error" for e in events), events


def test_cron_settings_only_saves_allowed_keys():
    """Arbitrary body keys must not be written into the cron config."""
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        _client().post(
            "/api/services/svc1/cron-settings",
            json={"cron_sync": {"enabled": True, "rogue_key": "evil"}},
        )

    assert "rogue_key" not in saved.get("provisioning", {}).get("cron_sync", {})


_BASE_LF = {"preset": "standard", "groups": ["A", "B"], "schema_version": 2}


def test_log_fields_saves_to_config():
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    with (
        patch("backend.config.load_config", return_value=_cfg(log_fields=_BASE_LF)),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.core.metadata.record_audit"),
        patch("backend.state_sync.export_admin_state"),
    ):
        new_lf = {"preset": "standard", "groups": ["A", "B", "C"], "schema_version": 2}
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={"log_fields": new_lf},
        )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert "C" in saved["log_fields"]["groups"]


def test_log_fields_set_reinjects_cmcd_fields_when_cmcd_enabled():
    """REGRESSION: 2026-08-12 production incident — CMCD's 14 ``cmcd_*`` fields are
    system-managed and hidden from the user-editable list by
    ``_is_system_field``, so the UI always POSTs a ``custom_fields`` that omits
    them. The pre-existing merge guard only fired when the incoming list was
    absent/empty, and the re-injection block only covered scoring — so a POST
    carrying one user field silently stripped all 14 CMCD entries from the
    persisted config. ``reconcile_vcl_state`` then regenerated the Fastly log
    format without them while the CMCD extraction VCL stayed installed: the
    edge kept parsing CMCD into ``req.http.x-cmcd:*`` and nothing logged it,
    so every ``cmcd_*`` column ingested empty and /streaming showed all zeros
    with no error."""
    from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES

    saved = {}

    with (
        patch(
            "backend.config.load_config",
            return_value=_cfg(log_fields=_BASE_LF, cmcd={"enabled": True, "mode": "query_string", "version": 1}),
        ),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, cfg: saved.update(cfg))),
        patch("backend.core.metadata.record_audit"),
        patch("backend.state_sync.export_admin_state"),
    ):
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={
                "log_fields": {
                    "preset": "standard",
                    "groups": ["A", "B", "C"],
                    "schema_version": 2,
                    # Non-empty, and omits every cmcd_* entry — exactly what
                    # the UI sends. This is what used to strip them.
                    "custom_fields": [{"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}],
                }
            },
        )

    assert response.status_code == 200
    saved_names = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
    for name in _CMCD_FIELD_NAMES:
        assert name in saved_names, f"CMCD field {name!r} was stripped by a log-fields write"
    assert "my_custom" in saved_names, "user custom_field was wrongly stripped"


def test_log_fields_set_strips_cmcd_fields_when_cmcd_disabled():
    """The mirror case: with CMCD off, stale cmcd_* entries must not persist."""
    saved = {}

    with (
        patch("backend.config.load_config", return_value=_cfg(log_fields=_BASE_LF)),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, cfg: saved.update(cfg))),
        patch("backend.core.metadata.record_audit"),
        patch("backend.state_sync.export_admin_state"),
    ):
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
    saved_names = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
    assert saved_names == {"my_custom"}


def test_log_fields_no_changes_detected():
    """Posting the same log_fields hash returns 'No changes detected' without saving."""
    from backend.core import log_fields as lf_module

    # Pre-compute the hash so the stored config already matches what the POST will compute
    existing_hash = lf_module.format_hash(_BASE_LF)
    stored_lf = {**_BASE_LF, "format_hash": existing_hash}

    saved_calls = []

    with (
        patch("backend.config.load_config", return_value=_cfg(log_fields=stored_lf)),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda s, c: saved_calls.append(c))),
    ):
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={"log_fields": _BASE_LF},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert "No changes" in data["message"]
    assert len(saved_calls) == 0


def test_log_fields_missing_log_fields_key_returns_422():
    # Pydantic enforces the wrapper at request-validation time → 422,
    # not the manual 400 the handler raises for an empty dict. This pins
    # the schema contract that prevents the regression seen in 4805391
    # (frontend POSTing bare config with no log_fields wrapper).
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().post(
            "/api/services/svc1/log-fields",
            json={},  # log_fields key absent
        )
    assert response.status_code == 422


def test_log_fields_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post(
            "/api/services/missing/log-fields",
            json={"log_fields": _BASE_LF},
        )
    assert response.status_code == 404


def test_time_range_delete_clears_provisioning_entry():
    cfg = _cfg()
    cfg["provisioning"]["time_range"] = {"start": "2026-01-01", "end": "2026-02-01"}
    saved = {}

    def fake_save(sid, c):
        saved.update(c)

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.core.metadata.record_audit"),
    ):
        response = _client().delete("/api/services/svc1/time-range")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert "time_range" not in saved.get("provisioning", {})


def test_time_range_delete_no_op_when_not_set():
    with patch("backend.config.load_config", return_value=_cfg()):
        response = _client().delete("/api/services/svc1/time-range")

    assert response.status_code == 200
    assert "No time_range" in response.json()["message"]


def test_time_range_delete_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().delete("/api/services/missing/time-range")
    assert response.status_code == 404


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


def _cfg_with_field():
    from datetime import UTC, datetime

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


def test_custom_field_create_saves_to_config():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    cfg_with_lf = _cfg()
    cfg_with_lf["log_fields"] = {"preset": "standard", "groups": ["A"], "schema_version": 2}

    with (
        patch("backend.config.load_config", return_value=cfg_with_lf),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        response = _client().post(
            "/api/services/svc1/custom-fields",
            json=_FIELD_CREATE_BODY,
        )

    assert response.status_code == 200
    assert response.json()["field"]["name"] == "cf_env"
    assert any(f["name"] == "cf_env" for f in saved["log_fields"]["custom_fields"])


def test_custom_field_create_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().post(
            "/api/services/missing/custom-fields",
            json=_FIELD_CREATE_BODY,
        )
    assert response.status_code == 404


def test_custom_field_delete_removes_field():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
    ):
        response = _client().delete("/api/services/svc1/custom-fields/cf_env")

    assert response.status_code == 200
    remaining = saved["log_fields"]["custom_fields"]
    assert all(f["name"] != "cf_env" for f in remaining)


def test_custom_field_delete_missing_field_returns_404():
    with patch("backend.config.load_config", return_value=_cfg_with_field()):
        response = _client().delete("/api/services/svc1/custom-fields/nonexistent")
    assert response.status_code == 404


def test_custom_field_delete_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().delete("/api/services/missing/custom-fields/cf_env")
    assert response.status_code == 404


def test_custom_field_patch_updates_label():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision.validate_log_format", return_value=[]),
    ):
        response = _client().patch(
            "/api/services/svc1/custom-fields/cf_env",
            json={"label": "Env Tag"},
        )

    assert response.status_code == 200
    updated = response.json()["field"]
    assert updated["label"] == "Env Tag"
    # original name unchanged
    assert updated["name"] == "cf_env"


def test_custom_field_patch_missing_service_returns_404():
    with patch("backend.config.load_config", return_value=None):
        response = _client().patch(
            "/api/services/missing/custom-fields/cf_env",
            json={"label": "x"},
        )
    assert response.status_code == 404


def test_custom_field_patch_missing_field_returns_404():
    with patch("backend.config.load_config", return_value=_cfg_with_field()):
        response = _client().patch(
            "/api/services/svc1/custom-fields/nonexistent",
            json={"label": "x"},
        )
    assert response.status_code == 404


def test_custom_field_patch_rejects_type_change_when_field_in_iceberg():
    """PATCH that changes ``duckdb_type`` MUST 422 when the field already
    exists in the committed DuckLake table — type evolution after ingest would
    silently break readers. The route in services/core.py inspects committed
    column types via ``_committed_duckdb_field_types``; this test drives that branch.
    """
    saved: dict = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch(
            "backend.routers.services.core._committed_duckdb_field_types",
            return_value={"cf_env": "VARCHAR"},
        ),
    ):
        response = _client().patch(
            "/api/services/svc1/custom-fields/cf_env",
            json={"duckdb_type": "BIGINT", "value_type": "numeric"},
        )

    assert response.status_code == 422, (
        f"PATCH that changes type on iceberg-locked field must 422; got {response.status_code} body={response.text}"
    )
    body = response.json()
    errors = body.get("detail", {}).get("errors", [])
    assert any("duckdb_type" in e.lower() or "data type" in e.lower() for e in errors), (
        f"422 body should explain the type-lock rejection; got errors={errors!r}"
    )
    # Save MUST NOT have been called — the rejection happens before any
    # config mutation lands.
    assert saved == {}, f"save_config was called despite the 422; saved={saved!r}"


def test_custom_field_patch_allows_type_change_before_iceberg_write():
    """When the field has NOT yet been written to DuckLake (the committed
    schema doesn't contain it), changing duckdb_type is legal — PATCH 200s.
    """
    saved: dict = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch("backend.routers.services.core._committed_duckdb_field_types", return_value={}),
    ):
        response = _client().patch(
            "/api/services/svc1/custom-fields/cf_env",
            json={"duckdb_type": "BIGINT", "value_type": "numeric"},
        )

    assert response.status_code == 200, f"PATCH on un-ingested field should 200; got {response.text}"
    updated = response.json()["field"]
    assert updated["duckdb_type"] == "BIGINT"
    assert updated["value_type"] == "numeric"
    # save_config WAS called this time.
    assert saved, "save_config should be called on a successful type change"


def test_credentials_api_token_rotation_creates_new_fos_key_and_saves():
    """``api_token`` mode (admin only) calls Fastly to create a new
    object-storage access key, deletes the old one (if present and
    different), and saves the new key pair. Pinned because losing
    any of the three side-effects (create / delete / save) would
    leave the admin's "Rotate credentials" button silently broken
    in distinct ways — only the save check covers the end state."""
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    # The starting cfg has an OLD fos_key_id that's different from the new one,
    # so the DELETE path is exercised.
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

    with (
        patch("backend.config.load_config", return_value=starting_cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        resp = _client().patch(
            "/api/services/svc1/credentials",
            json={"api_token": "tok_rotate_me"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["access_key_id"] == "NEW_AK"
    # Saved keys are the new ones
    assert saved["fos_access_key_id"] == "NEW_AK"
    assert saved["fos_secret_access_key"] == "NEW_SK"
    assert saved["provisioning"]["fos_key_id"] == "NEW_AK"
    # Both Fastly API calls happened: POST to create, DELETE to remove old
    methods = [m for m, _ in fastly_calls]
    assert "POST" in methods
    assert "DELETE" in methods


def test_credentials_api_token_create_failure_returns_400():
    """If the Fastly key-create POST raises, the endpoint returns 400
    with the Fastly error text in `detail.error`. Pinned because
    losing this would let credential-rotation failures surface as a
    generic 500 — admins wouldn't know to recheck their api_token."""
    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch(
            "backend.core.fastly.client.fastly",
            side_effect=RuntimeError("Fastly 401 unauthorized"),
        ),
    ):
        resp = _client().patch(
            "/api/services/svc1/credentials",
            json={"api_token": "bad_token"},
        )

    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "fos_key_create_failed"
    assert "error_id" in detail


def test_credentials_api_token_skips_delete_when_old_key_same_as_new():
    """If the existing `fos_key_id` matches the newly-created one
    (re-running rotation immediately), the DELETE step is skipped.
    Pinned because deleting the just-created key would leave the
    service with no working credentials."""
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    starting_cfg = _cfg()
    starting_cfg["provisioning"]["fos_key_id"] = "SAME_KEY"

    fastly_calls = []

    def fake_fastly(method, path, body=None, token=None, **kw):
        fastly_calls.append((method, path))
        if method == "POST":
            return {"access_key": "SAME_KEY", "secret_key": "NEW_SK"}
        raise AssertionError(f"unexpected fastly call: {method} {path}")

    with (
        patch("backend.config.load_config", return_value=starting_cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(fake_save)),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        _client().patch(
            "/api/services/svc1/credentials",
            json={"api_token": "tok"},
        )

    # Only one fastly call happened (POST). No DELETE was made.
    assert [m for m, _ in fastly_calls] == ["POST"]


def test_ngwaf_sync_service_not_found_yields_error_event():
    """No config for the service_id → error SSE event with "Service
    not found". Pinned because the FE renders this exact substring
    in a styled toast on the manual-sync button."""
    with patch("backend.config.load_config", return_value=None):
        resp = _client().post("/api/services/missing/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "Service not found" in e.get("message", "") for e in events)


def test_ngwaf_sync_no_workspace_yields_error_event():
    """Config exists but no NGWAF workspace_id → error SSE event
    explaining the gap. Pinned because services without NGWAF still
    need a clean error rather than a confusing 500."""
    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "No NGWAF workspace" in e.get("message", "") for e in events)


def test_ngwaf_sync_no_api_key_yields_error_event():
    """Workspace set but no `fastly_api_key` → error event. Pinned
    because credentials can be revoked between page-load and click
    — the FE wants to differentiate "missing key" from other
    failure modes."""
    cfg = _cfg(fastly_api_key="")
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "No Fastly API key" in e.get("message", "") for e in events)


def test_ngwaf_sync_source_missing_yields_error_event():
    """`get_source_for_service` returns None → error event. Pinned
    because admins can have a config without an active source (mid-
    teardown, credentials cleared)."""
    cfg = _cfg(fastly_api_key="key")
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "Service source not found" in e.get("message", "") for e in events)


def test_ngwaf_sync_start_cron_run_raises_yields_error_event():
    """`start_cron_run` raising RuntimeError (busy) → error SSE event
    carrying the runtime error message. Pinned because the manual-
    sync button must not appear to hang when a cron run is already
    in flight — the FE shows the error text inline."""
    cfg = _cfg(fastly_api_key="key")
    src = {"name": "svc1", "service_id": "svc1"}
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("already running")),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "already running" in e.get("message", "") for e in events)


def test_ngwaf_sync_no_pending_records_yields_already_enriched_done():
    """`oldest_unenriched_timestamp` returning None → SSE done event
    with "All requests are already enriched" + `log_cron_run` recorded
    as success. Pinned because losing this would invoke the bot
    fetcher with from_ts=None and likely hammer the NGWAF API for
    the full account history."""
    cfg = _cfg(fastly_api_key="key")
    src = {"name": "svc1", "service_id": "svc1"}
    log_calls = []

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: ()),
        patch("backend.utils.ngwaf.oldest_unenriched_timestamp", return_value=None),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "done" and "already enriched" in e.get("message", "").lower() for e in events)
    mock_fetch.assert_not_called()
    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    assert kwargs.get("summary", "").lower().startswith("all requests")


def test_ngwaf_sync_happy_path_streams_page_status_then_done():
    """Happy path: per-page status events plus a final `done` summary
    after upsert + cleanup. Pinned because the FE renders each page
    status line as a progress chip; missing them would freeze the
    progress modal until the final summary."""
    cfg = _cfg(fastly_api_key="key")
    src = {"name": "svc1", "service_id": "svc1"}
    log_calls = []
    upsert_calls = []
    pages = [
        ([{"user_agent": "GoogleBot/2", "server_name": "www"}], "2026-05-01T00:00:00Z", 5),
    ]

    def fake_matcher(ua):
        return ({"id": "googlebot", "name": "Google Bot"},) if "Google" in (ua or "") else ()

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=fake_matcher),
        patch("backend.utils.ngwaf.oldest_unenriched_timestamp", return_value="2025-12-01T00:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", return_value=iter(pages)),
        patch(
            "backend.utils.ngwaf_bot_cache.upsert_bots",
            side_effect=lambda rows, ws, ts: upsert_calls.append((list(rows), ws, ts)),
        ),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    statuses = [e for e in events if e.get("type") == "status"]
    dones = [e for e in events if e.get("type") == "done"]
    # Initial "Scanning ..." status + per-page status
    assert any("Scanning" in s.get("message", "") for s in statuses)
    assert any("Page 1" in s.get("message", "") for s in statuses)
    # Final done event with a summary mentioning the synced count
    assert any("Synced 1" in d.get("message", "") for d in dones)
    # The matcher's wellknown_bot_id landed on the enriched record
    assert upsert_calls[0][0][0]["wellknown_bot_id"] == "googlebot"


def test_ngwaf_sync_fetcher_exception_yields_error_event_and_logs():
    """If the NGWAF paged fetcher raises mid-stream, an `error` SSE
    event is emitted AND `log_cron_run(status="error")` is recorded.
    Pinned because losing the log_cron_run would leave the cron-runs
    admin panel blank for the most-important failure mode."""
    cfg = _cfg(fastly_api_key="key")
    src = {"name": "svc1", "service_id": "svc1"}
    log_calls = []

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: ()),
        patch("backend.utils.ngwaf.oldest_unenriched_timestamp", return_value="2025-12-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            side_effect=RuntimeError("NGWAF 503"),
        ),
    ):
        resp = _client().post("/api/services/svc1/ngwaf-sync")

    events = _parse_sse_events(resp.text)
    assert any(e.get("type") == "error" and "NGWAF 503" in e.get("message", "") for e in events)
    # log_cron_run was called with status="error"
    assert any(
        (kwargs.get("status") == "error" or (len(args) > 3 and args[3] == "error")) for args, kwargs in log_calls
    )


def _post(body: dict, stored_cron_sync: dict) -> dict:
    cfg = {
        "service_id": "svc1",
        "name": "Test Service",
        "access_level": "read_write",
        "log_period": 60,
        "provisioning": {"cron_sync": dict(stored_cron_sync), "cron_compact": {"enabled": True}},
    }
    saved: dict = {}

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, c: saved.update(c))),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        resp = TestClient(app).post("/api/services/svc1/cron-settings", json=body)
    assert resp.status_code == 200, resp.text
    # SSE stream must have reached "done", not errored.
    assert any('"done"' in line for line in resp.text.splitlines()), resp.text
    return saved["provisioning"]["cron_sync"]


def test_interval_seconds_is_modelled():
    """It must survive request parsing — Pydantic drops unknown fields."""
    assert "interval_seconds" in CronSettingsPartial.model_fields
    parsed = CronSettingsPartial(**json.loads('{"interval_seconds": 90}'))
    assert parsed.interval_seconds == 90


def test_polling_mode_is_modelled_and_persisted():
    parsed = CronSettingsPartial(**json.loads('{"polling_mode": "adaptive"}'))
    assert parsed.polling_mode == "adaptive"

    out = _post({"cron_sync": {"polling_mode": "adaptive"}}, {"enabled": True})

    assert out["polling_mode"] == "adaptive"


def test_polling_mode_rejects_unknown_values():
    from pydantic import ValidationError

    try:
        CronSettingsPartial(**json.loads('{"polling_mode": "continuous"}'))
    except ValidationError as exc:
        assert "polling_mode" in str(exc)
    else:
        raise AssertionError("unsupported polling mode was accepted")


def test_interval_seconds_is_persisted():
    out = _post({"cron_sync": {"interval_seconds": 90}}, {"enabled": True, "interval_seconds": 30})
    assert out["interval_seconds"] == 90


def test_setting_interval_seconds_drops_stale_interval_mins():
    """THE REGRESSION: a persisted interval_mins would otherwise keep winning."""
    out = _post(
        {"cron_sync": {"interval_seconds": 45}},
        {"enabled": True, "interval_seconds": 30, "interval_mins": 2},
    )
    assert out["interval_seconds"] == 45
    assert "interval_mins" not in out, "stale interval_mins still wins in the scheduler"


def test_explicit_interval_mins_still_wins_when_both_sent():
    """Sending both is unambiguous — respect mins, don't second-guess."""
    out = _post(
        {"cron_sync": {"interval_mins": 5, "interval_seconds": 45}},
        {"enabled": True, "interval_seconds": 30},
    )
    assert out["interval_mins"] == 5
    assert out["interval_seconds"] == 45


def test_unrelated_keys_are_preserved():
    """Partial update must not clobber persisted siblings."""
    out = _post(
        {"cron_sync": {"interval_seconds": 90}},
        {"enabled": True, "interval_seconds": 30, "data_retention_days": 120, "delete_after": True},
    )
    assert out["data_retention_days"] == 120
    assert out["delete_after"] is True
    assert out["enabled"] is True


def test_expiry_tuning_fields_round_trip():
    out = _post(
        {"cron_sync": {"keep_snapshot_days": 3, "expire_interval_mins": 30}},
        {"enabled": True, "interval_seconds": 30},
    )
    assert out["keep_snapshot_days"] == 3
    assert out["expire_interval_mins"] == 30


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
    """Viewer-key endpoint uses stored fastly_api_key and returns credentials."""
    with (
        patch("backend.config.load_config", return_value=_FAKE_CFG),
        patch("backend.provision.orchestrator.fastly", return_value=_FAKE_KEY),
        patch("backend.config.config_to_source", return_value={**_FAKE_CFG}),
    ):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 200
    data = response.json()
    assert data["access_key_id"] == "ANALYST_KEY"
    assert data["secret_key"] == "ANALYST_SECRET"
    assert data["fos_bucket"] == "test-bucket"
    # Encrypted invite fields must not be present
    assert "invite_block" not in data
    assert "invite_secret" not in data
    assert "expires_at" not in data


def test_generate_viewer_key_requires_stored_api_key():
    """Missing fastly_api_key in config returns 400."""
    cfg_no_token = {**_FAKE_CFG, "fastly_api_key": ""}
    with patch("backend.config.load_config", return_value=cfg_no_token):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 400


def test_generate_viewer_key_rejects_celery_topology():
    """Scalable DuckLake services must use Path B instead of FOS-only invites."""
    with (
        patch("backend.config.DEPLOYMENT_MODE", "high_scale"),
        patch("backend.config.load_config", return_value=_FAKE_CFG),
    ):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 409
    assert "Path B" in response.json()["detail"]["error"]


def test_generate_viewer_key_requires_read_write_service():
    """Analyst services (read_only) cannot generate viewer keys."""
    ro_cfg = {**_FAKE_CFG, "access_level": "read_only"}
    with patch("backend.config.load_config", return_value=ro_cfg):
        client = TestClient(app)
        response = client.post("/api/services/svc123/generate-viewer-key")

    assert response.status_code == 403


def test_decrypt_invite_endpoint_does_not_exist():
    """/api/provision/decrypt-invite is not a valid endpoint."""
    client = TestClient(app)
    response = client.post(
        "/api/provision/decrypt-invite",
        json={"invite_block": "x", "secret_key": "y"},
    )
    assert response.status_code == 404 or response.status_code == 405


# ── Merged from tests/routers/test_logging_settings.py ──
client = TestClient(app)


def _seed_cache(service_id: str, **overrides) -> dict:
    """Drop a known payload into the logging-settings cache and return it."""
    payload = {
        "prefix": "stale_prefix",
        "period": 60,
        "sample_rate": 100,
        "edge_only": False,
        "custom_condition": "",
        "format_match": False,
        "version": 1,
    }
    payload.update(overrides)
    services_core._logging_settings_cache[service_id] = payload
    return payload


@patch("backend.config.load_config")
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
def test_logging_settings_extracts_custom_condition(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    """Verify that the GET endpoint extracts the custom_condition from the VCL statement."""
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {"endpoint_name": "Test Endpoint"},
    }
    mock_get_active_version.return_value = 1

    # Mock the Fastly S3 endpoint response
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 120,
        "path": "/logs/raw/%Y-%m-%d/",
        "response_condition": "Log Sampling",
    }

    # Mock the condition statement containing a custom condition
    mock_find_condition.return_value = {
        "statement": '!segmented_caching.is_inner_req && (req.restarts == 0 && fastly.ff.visits_this_service == 0) && randombool(50, 100) && (std.tolower(req.url) !~ "\\.(jpg|png)$")'
    }

    response = client.get("/api/services/test-service/logging-settings")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["period"] == 120
    assert data["sample_rate"] == 50
    assert data["edge_only"] is True
    assert data["custom_condition"] == 'std.tolower(req.url) !~ "\\.(jpg|png)$"'


@patch("backend.config.load_config")
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
def test_logging_settings_custom_condition_fallback(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    """Verify that the GET endpoint uses the config fallback if the VCL condition doesn't have it."""
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {
            "endpoint_name": "Test Endpoint",
            "custom_condition": 'req.http.host == "example.com"',
        },
    }
    mock_get_active_version.return_value = 1

    # Mock FOS endpoint
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 60,
        "path": "/logs/raw/",
        "response_condition": "Log Sampling",
    }

    # Mock the condition statement WITHOUT a custom condition
    mock_find_condition.return_value = {"statement": "!segmented_caching.is_inner_req && randombool(100, 100)"}

    response = client.get("/api/services/test-service/logging-settings")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["custom_condition"] == 'req.http.host == "example.com"'


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_update_logging_settings_passes_custom_condition(
    mock_get_source, mock_update, mock_save_config, mock_load_config
):
    """Verify that the UPDATE endpoint passes the custom_condition to the provisioning engine."""
    mock_get_source.return_value = None

    fake_disk_state = {
        "service_id": "test-service",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    def _dynamic_load(service_id):
        return json.loads(json.dumps(fake_disk_state))

    mock_load_config.side_effect = _dynamic_load

    def _dynamic_save(service_id, new_cfg):
        nonlocal fake_disk_state
        fake_disk_state = json.loads(json.dumps(new_cfg))

    mock_save_config.side_effect = _dynamic_save

    # Capture the update_cfg passed to update_logging_endpoint
    captured_cfg = {}

    def fake_generator(cfg, token):
        nonlocal captured_cfg
        captured_cfg = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "changed": True}

    mock_update.side_effect = fake_generator

    # Security: route moved from GET → POST/PATCH. Tests use POST.
    with patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save_config)):
        response = client.post(
            "/api/services/test-service/logging-settings/update",
            params={
                "period": 120,
                "sample_rate": 25,
                "edge_only": True,
                "custom_condition": 'req.http.x-test == "1"',
            },
        )

    assert response.status_code == 200

    # Verify the captured config
    assert captured_cfg["log_period"] == 120
    assert captured_cfg["sample_rate"] == 25
    assert captured_cfg["edge_only"] is True
    assert captured_cfg["custom_condition"] == 'req.http.x-test == "1"'

    # Verify it was saved to disk
    assert fake_disk_state["provisioning"]["custom_condition"] == 'req.http.x-test == "1"'


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_update_pre_warms_cache_on_changed_with_format(
    mock_get_source, mock_update, mock_save_config, mock_load_config
):
    """A successful Fastly mutation with update_format=true pre-warms the
    GET cache so the next read skips the 2-3 round-trip cold path."""
    mock_get_source.return_value = None
    mock_load_config.return_value = {
        "service_id": "svc-prewarm",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    def fake_generator(cfg, token):
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "changed": True, "version": 42}

    mock_update.side_effect = fake_generator

    _seed_cache("svc-prewarm", version=1, format_match=False, period=60)

    response = client.post(
        "/api/services/svc-prewarm/logging-settings/update",
        params={
            "period": 120,
            "sample_rate": 25,
            "edge_only": True,
            "custom_condition": 'req.http.x == "1"',
            "update_format": True,
        },
    )
    assert response.status_code == 200

    cached = services_core._logging_settings_cache.get("svc-prewarm")
    assert cached is not None, "cache must be pre-warmed, not popped"
    assert cached["period"] == 120
    assert cached["sample_rate"] == 25
    assert cached["prefix"] == ""
    assert cached["edge_only"] is True
    assert cached["custom_condition"] == 'req.http.x == "1"'
    assert cached["format_match"] is True
    assert cached["version"] == 42


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_update_leaves_cache_on_no_change(mock_get_source, mock_update, mock_save_config, mock_load_config):
    """changed=False means Fastly applied no diff; the cached payload is
    still consistent with deployed state and must not be popped."""
    mock_get_source.return_value = None
    mock_load_config.return_value = {
        "service_id": "svc-nochange",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    def fake_generator(cfg, token):
        yield {"type": "done", "changed": False, "version": 7}

    mock_update.side_effect = fake_generator

    seeded = _seed_cache("svc-nochange", version=7, format_match=True, period=60)

    response = client.post(
        "/api/services/svc-nochange/logging-settings/update",
        params={"period": 60, "sample_rate": 100, "update_format": True},
    )
    assert response.status_code == 200

    cached = services_core._logging_settings_cache.get("svc-nochange")
    assert cached == seeded, "no-op save must leave the cache untouched"


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_update_pops_cache_when_format_not_deployed(mock_get_source, mock_update, mock_save_config, mock_load_config):
    """changed=True without update_format=true means we can't prove the
    cached format_match still holds — fall back to invalidation."""
    mock_get_source.return_value = None
    mock_load_config.return_value = {
        "service_id": "svc-fallback",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    def fake_generator(cfg, token):
        yield {"type": "done", "changed": True, "version": 9}

    mock_update.side_effect = fake_generator

    _seed_cache("svc-fallback", version=8, format_match=True)

    response = client.post(
        "/api/services/svc-fallback/logging-settings/update",
        params={"period": 120, "update_format": False},
    )
    assert response.status_code == 200

    assert services_core._logging_settings_cache.get("svc-fallback") is None


# ── Merged from tests/routers/test_concurrency_persistence.py ──
@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_long_running_endpoint_preserves_background_changes(
    mock_get_source, mock_update, mock_save_config, mock_load_config
):
    """
    Regression test: Ensure that long-running streaming endpoints
    reload the config before saving, so they don't overwrite changes
    made by other requests during the stream.
    """
    mock_get_source.return_value = None

    # This dictionary simulates our database/disk state
    fake_disk_state = {
        "service_id": "test-service",
        "name": "Initial Name",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    # load_config will always return the CURRENT state of fake_disk_state
    def _dynamic_load(service_id):
        return json.loads(json.dumps(fake_disk_state))

    mock_load_config.side_effect = _dynamic_load

    def _dynamic_save(service_id, new_cfg):
        nonlocal fake_disk_state
        fake_disk_state = json.loads(json.dumps(new_cfg))

    mock_save_config.side_effect = _dynamic_save

    # We mock the long-running generator
    def fake_generator(cfg, token):
        yield {"type": "status", "message": "starting"}

        # SIMULATE BACKGROUND CHANGE HAPPENING MID-STREAM
        # We write directly to our "disk state" to simulate another worker saving
        fake_disk_state["name"] = "Updated Background Name"

        yield {"type": "done", "changed": True}

    mock_update.side_effect = fake_generator

    # Call the endpoint (security: GET → POST)
    with patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save_config)):
        response = client.post(
            "/api/services/test-service/logging-settings/update",
            params={"period": 3600, "sample_rate": 50, "edge_only": True},
        )

    assert response.status_code == 200

    # Let's read what the final "disk" state looks like
    assert fake_disk_state["log_period"] == 3600
    assert fake_disk_state["provisioning"]["sample_rate"] == 50
    assert fake_disk_state["provisioning"]["edge_only"] is True

    # Crucially, the background change must NOT have been overwritten
    assert fake_disk_state["name"] == "Updated Background Name", (
        "The background change was overwritten by a stale config!"
    )


def test_get_catalog():
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    groups = data["groups"]
    group_ids = [g["id"] for g in groups]
    assert "METRICS" in group_ids

    fields = data["fields"]
    field_ids = [f["id"] for f in fields]

    required_metrics = [
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
    ]
    for rm in required_metrics:
        assert rm in field_ids, f"Metric {rm} missing from catalog"
        field = next(f for f in fields if f["id"] == rm)
        assert field["group"] == "METRICS"

    hit_rate = next(f for f in fields if f["id"] == "hit_rate")
    assert hit_rate["formatter"] == "percent"
    assert hit_rate["unit"] == "%"


def test_api_create_custom_field_validation_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    svc_id = "test_svc_validation"
    cfg = {"service_id": svc_id, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}}
    config.save_config(svc_id, cfg)

    # 1. Invalid name
    payload = {
        "name": "INVALID NAME!",
        "label": "Invalid",
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

    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    assert "detail" in resp.json()

    # 2. Invalid VCL expression (semicolon)
    payload["name"] = "valid_name"
    payload["vcl_log_expression"] = "req.http.Host;"
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    err_json = resp.json()
    assert "errors" in err_json["detail"]
    assert any("semicolon" in e for e in err_json["detail"]["errors"])

    # 3. Create a valid one
    payload["vcl_log_expression"] = "req.http.Host"
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 200

    # 4. Try to create duplicate
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    err_json = resp.json()
    assert "errors" in err_json["detail"]
    assert any("already exists" in e for e in err_json["detail"]["errors"])


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.core.duckdb.get_source_for_service")
def test_custom_fields_persist_on_log_fields_update(mock_get_source, mock_save_config, mock_load_config):
    """
    Regression test: Ensure that updating standard log fields (groups)
    does not overwrite/delete existing custom fields in the shared config state.
    """
    mock_get_source.return_value = None  # Skip DB queries

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
    saved_cfg = mock_save_config.call_args[0][1]

    assert "my_custom_field" in [cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]], (
        "Custom fields were overwritten during save!"
    )
    assert saved_cfg["log_fields"].get("schema_version") == 2, "schema_version was lost during save!"

    read_lf = saved_cfg["log_fields"]
    assert len(read_lf.get("custom_fields", [])) == 1, "Custom fields were wiped during save!"


# ── Merged from tests/routers/services/test_custom_field_count_limits.py ──
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
    """Build a saved-shape custom field record for direct config seeding.

    Field names are 5 chars (``cf001``) so we can pre-load many fields
    without bumping the log_format size unintentionally — see the
    measured tipping point at ~119 fields in the test below.
    """
    return {
        **_BASE_PAYLOAD,
        "name": f"cf{i:03d}",
        "created_at": "2026-05-27T00:00:00+00:00",
        "updated_at": "2026-05-27T00:00:00+00:00",
    }


def _make_cfg(svc_id: str, n_existing: int) -> dict:
    """Pre-populate a config with ``n_existing`` saved custom fields."""
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
        """The Nth field is accepted because the gate is log_format size,
        not count. Pre-seeded ``n_existing`` short-named fields keep log_format
        well under the 8000 safe max; the new field's name adds ~75 chars
        more — still under. A refactor that adds a numeric ceiling would
        break this test loudly.

        The exact threshold drops as VCL helpers get longer (016 wrapped
        each string field in ``substr(..., 0, 2000)``, adding ~15 chars per
        field). Pick ``n_existing`` well below the new ceiling so this test
        stays as a "no numeric cap" assertion rather than a tipping-point
        measurement.
        """
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_count_cap"
        # 78 seeded + 1 new = 79 → log_format ≈ 7700 chars, comfortably
        # under the 8000-char safe max even with the new substr wrappers.
        config.save_config(svc_id, _make_cfg(svc_id, n_existing=78))

        client = TestClient(app)
        # Patch shutil.which on the module that imports it so the Falco
        # branch is skipped — keeps the test fast and deterministic across
        # dev boxes regardless of whether the binary is installed.
        with patch("backend.provision.fastly_api.shutil.which", return_value=None):
            resp = client.post(
                f"/api/services/{svc_id}/custom-fields",
                json=_post_payload("cf078"),
            )
        assert resp.status_code == 200, (
            f"79th field should be accepted (no numeric cap exists); got {resp.status_code} body={resp.text}"
        )
        body = resp.json()
        # Response wraps the saved field as ``body['field']`` — peer tests
        # (test_custom_fields_validation.py) rely on the 200 status alone;
        # we additionally assert the saved name to catch a routing bug
        # where the 200 came from a different handler.
        assert body["field"]["name"] == "cf078"

        # And the saved config now holds 79 fields.
        saved = config.load_config(svc_id)
        assert len(saved["log_fields"]["custom_fields"]) == 79

    def test_creating_field_that_overflows_log_format_returns_422(self, tmp_path, monkeypatch):
        """When the *next* field would push log_format past
        ``FASTLY_LOG_FORMAT_SAFE_MAX`` (8000), the route returns 422 with
        ``LOG_FORMAT_TOO_LONG`` in the errors list.

        Derive the seeded count from the real template producer so a single
        additional same-shaped field crosses the safe max even as built-in
        fields and capture expressions change.

        Pins TWO things at once:
          a) The exact 422 status code (not 400, not silent acceptance).
          b) The ``LOG_FORMAT_TOO_LONG`` error tag in the response body
             shape — the frontend's CustomFieldDrawer looks for this
             substring when surfacing the inline error.
        """
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_logfmt_overflow"
        cfg = _at_template_limit(svc_id)
        count = len(cfg["log_fields"]["custom_fields"])
        config.save_config(svc_id, cfg)

        client = TestClient(app)
        # No shutil.which patch needed — the length check at
        # backend/provision/fastly_api.py fires BEFORE Falco would be
        # invoked, so this branch is binary-independent.
        resp = client.post(
            f"/api/services/{svc_id}/custom-fields",
            json=_post_payload(f"cf{count:03d}"),
        )
        assert resp.status_code == 422, (
            f"field that overflows log_format should 422; got {resp.status_code} body={resp.text}"
        )
        body = resp.json()
        assert "detail" in body and "errors" in body["detail"], body
        errors: list[str] = body["detail"]["errors"]
        assert any("LOG_FORMAT_TOO_LONG" in e for e in errors), f"expected LOG_FORMAT_TOO_LONG in errors, got: {errors}"

        saved = config.load_config(svc_id)
        assert saved["log_fields"]["custom_fields"] == cfg["log_fields"]["custom_fields"]

    def test_log_format_overflow_error_reports_chars_and_safe_max(self, tmp_path, monkeypatch):
        """The error string surfaces the actual size and the safe-max so the
        user knows how much to trim. ``backend/provision/fastly_api.py:289``
        formats the message as
        ``LOG_FORMAT_TOO_LONG: Log format is N chars; Fastly's limit is
        ~8192 (safe max: 8000). ...``. Pinning the prose protects the
        drawer's UX from a silent rewording that omits the actionable
        numbers."""
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_logfmt_msg"
        config.save_config(svc_id, _make_cfg(svc_id, n_existing=118))

        client = TestClient(app)
        resp = client.post(
            f"/api/services/{svc_id}/custom-fields",
            json=_post_payload("cf118"),
        )
        assert resp.status_code == 422
        errors: list[str] = resp.json()["detail"]["errors"]
        msg = next((e for e in errors if "LOG_FORMAT_TOO_LONG" in e), None)
        assert msg is not None
        # Both numbers must appear; either order, with the message phrasing.
        assert "chars" in msg
        assert "8000" in msg, f"expected safe-max 8000 in message: {msg}"

    def test_drop_a_field_at_the_size_limit_then_re_add_succeeds(self, tmp_path, monkeypatch):
        """At the size limit, deleting one field frees template budget so a
        new (size-equivalent) field is accepted on the next POST. Pins the
        gate as DYNAMIC (size-based), not a static "high-water-mark" counter
        that wouldn't free up after deletes.
        """
        monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
        svc_id = "test_svc_drop_readd"
        cfg = _at_template_limit(svc_id)
        fields = cfg["log_fields"]["custom_fields"]
        count = len(fields)
        removed_name = fields[count // 2]["name"]
        config.save_config(svc_id, cfg)

        client = TestClient(app)
        overflow = client.post(
            f"/api/services/{svc_id}/custom-fields",
            json=_post_payload(f"cf{count:03d}"),
        )
        assert overflow.status_code == 422, f"setup invariant: expected 422 at limit; got {overflow.status_code}"

        assert config.load_config(svc_id)["log_fields"]["custom_fields"] == fields
        resp = client.delete(f"/api/services/{svc_id}/custom-fields/{removed_name}")
        assert resp.status_code == 200, f"DELETE expected 200; got {resp.status_code} body={resp.text}"
        saved = config.load_config(svc_id)
        remaining = [cf["name"] for cf in saved["log_fields"]["custom_fields"]]
        assert removed_name not in remaining, f"DELETE did not remove the field; remaining={remaining!r}"
        assert len(remaining) == count - 1

        # Re-add a same-sized field (5 char name) → the freed budget should
        # accept it. A static count-cap regression would still reject.
        with patch("backend.provision.fastly_api.shutil.which", return_value=None):
            resp = client.post(
                f"/api/services/{svc_id}/custom-fields",
                json=_post_payload("cf999"),
            )
        assert resp.status_code == 200, (
            f"after dropping one field, re-add at the same template size should 200; "
            f"got {resp.status_code} body={resp.text}"
        )

        saved = config.load_config(svc_id)
        names = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
        assert removed_name not in names
        assert "cf999" in names
        assert len(names) == count

    def test_audit_logs_returns_seeded_entries(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=3, event_type="service_renamed")

        r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 3
        assert len(body["entries"]) == 3
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
        assert body["total"] == 4
        assert all(e["event_type"] == "time_range_cleared" for e in body["entries"])

    def test_audit_logs_pagination(self, client, test_service_source):
        _seed_audit(test_service_source["name"], n=10)

        r = client.get(
            "/api/audit-logs",
            params={"page": 2, "per_page": 3},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 10
        assert body["page"] == 2
        assert body["per_page"] == 3
        assert len(body["entries"]) == 3

    def test_audit_logs_per_page_validation(self, client):
        """per_page > 1000 must be rejected by FastAPI Query validation."""
        r = client.get(
            "/api/audit-logs",
            params={"per_page": 5000},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert r.status_code == 422

    def test_audit_logs_page_must_be_positive(self, client):
        r = client.get(
            "/api/audit-logs",
            params={"page": 0},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert r.status_code == 422

    def test_audit_logs_default_dir_is_descending(self, client, test_service_source):
        """Most-recent-first is the default — verify the entries come back DESC by ts."""
        _seed_audit(test_service_source["name"], n=5)

        r = client.get("/api/audit-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert r.status_code == 200
        entries = r.json()["entries"]
        timestamps = [e["timestamp"] for e in entries]
        assert timestamps == sorted(timestamps, reverse=True), "default sort dir must be DESC"

    def test_validate_vcl_404_for_missing_service(self):
        with patch("backend.config.load_config", return_value=None):
            r = _client().post(
                "/api/services/missing/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
            )
        assert r.status_code == 404

    def test_validate_vcl_valid_expression_returns_valid_true(self):
        """A simple, well-formed expression must produce the contract the
        drawer relies on: ``valid=True``, empty ``errors``, a numeric
        ``format_length``, and a ``format_length_limit``."""
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={
                    "vcl_log_expression": "req.http.Host",
                    "collection_stage": "edge",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is True
        assert body["errors"] == []
        assert isinstance(body["warnings"], list)
        assert isinstance(body["format_length"], int)
        assert body["format_length"] > 0
        assert body["format_length_limit"] == 12000

    def test_validate_vcl_invalid_expression_returns_errors(self):
        """An obviously broken expression (unbalanced braces) must produce
        ``valid: False`` with at least one entry in ``errors``."""
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={
                    # Unbalanced + references no real VCL identifier — both
                    # the AST validator and the format-length validator
                    # should reject this.
                    "vcl_log_expression": "req.http.{{ not closed",
                    "collection_stage": "edge",
                },
            )
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is False
        assert len(body["errors"]) >= 1, f"expected at least one error, got: {body}"

    def test_validate_vcl_response_shape_matches_drawer_contract(self):
        """The drawer reads ``valid``, ``errors``, ``warnings``,
        ``format_length_limit``. Any rename breaks the UI silently because the
        component reads the fields untyped (``data as any``)."""
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={"vcl_log_expression": "req.http.Host", "collection_stage": "edge"},
            )
        keys = set(r.json().keys())
        required = {"valid", "errors", "warnings", "format_length_limit"}
        missing = required - keys
        assert not missing, f"drawer-contract response keys missing: {missing}"

    def test_validate_vcl_collection_stage_origin_accepted(self):
        """``origin`` stage is a different code path (custom field is
        promoted via beresp headers). The endpoint must accept it."""
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={
                    "vcl_log_expression": "beresp.http.x-something",
                    "collection_stage": "origin",
                },
            )
        assert r.status_code == 200
        assert "valid" in r.json()

    def test_validate_vcl_rejects_unknown_collection_stage(self):
        """Pydantic enforces the ``Literal['edge', 'origin']`` — a typo
        becomes a 422, not a silent passthrough."""
        with patch("backend.config.load_config", return_value=dict(_CFG)):
            r = _client().post(
                "/api/services/svc-lint/custom-fields/validate-vcl",
                json={
                    "vcl_log_expression": "req.http.Host",
                    "collection_stage": "midway",  # not a real stage
                },
            )
        assert r.status_code == 422
