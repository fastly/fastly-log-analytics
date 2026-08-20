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

from fastapi.testclient import TestClient

from backend.main import app

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# PATCH /api/services/{service_id}/credentials
# ---------------------------------------------------------------------------


def test_credentials_access_key_saves_to_config():
    """Supplying access_key + secret_key in the body saves them to config."""
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config", side_effect=fake_save),
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


# ---------------------------------------------------------------------------
# POST /api/services/{service_id}/cron-settings (SSE)
# ---------------------------------------------------------------------------


def _parse_sse_events(text: str) -> list[dict]:
    events = []
    for line in text.splitlines():
        if line.startswith("data: "):
            try:
                events.append(json.loads(line[6:]))
            except json.JSONDecodeError:
                pass
    return events


def test_cron_settings_saves_enabled_flag():
    saved = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    with (
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config", side_effect=fake_save),
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
        patch("backend.config.save_config", side_effect=fake_save),
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
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.metadata.record_audit"),
    ):
        _client().post(
            "/api/services/svc1/cron-settings",
            json={"cron_sync": {"enabled": True, "rogue_key": "evil"}},
        )

    assert "rogue_key" not in saved.get("provisioning", {}).get("cron_sync", {})


# ---------------------------------------------------------------------------
# POST /api/services/{service_id}/log-fields
# ---------------------------------------------------------------------------

_BASE_LF = {"preset": "standard", "groups": ["A", "B"], "schema_version": 2}


def test_log_fields_saves_to_config():
    saved = {}

    def fake_save(sid, cfg):
        saved.update(cfg)

    with (
        patch("backend.config.load_config", return_value=_cfg(log_fields=_BASE_LF)),
        patch("backend.config.save_config", side_effect=fake_save),
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
    """REGRESSION: 2026-08-12 SE-demo incident — CMCD's 14 ``cmcd_*`` fields are
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
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved.update(cfg)),
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
        patch("backend.config.save_config", side_effect=lambda sid, cfg: saved.update(cfg)),
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
        patch("backend.config.save_config", side_effect=lambda s, c: saved_calls.append(c)),
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


# ---------------------------------------------------------------------------
# DELETE /api/services/{service_id}/time-range
# ---------------------------------------------------------------------------


def test_time_range_delete_clears_provisioning_entry():
    cfg = _cfg()
    cfg["provisioning"]["time_range"] = {"start": "2026-01-01", "end": "2026-02-01"}
    saved = {}

    def fake_save(sid, c):
        saved.update(c)

    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config", side_effect=fake_save),
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


# ---------------------------------------------------------------------------
# Custom fields: POST / PATCH / DELETE
# ---------------------------------------------------------------------------

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
        patch("backend.config.save_config", side_effect=fake_save),
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
        patch("backend.config.save_config", side_effect=fake_save),
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
        patch("backend.config.save_config", side_effect=fake_save),
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


# ── PATCH duckdb_type change (audit follow-up) ──────────────────────────────


def _fake_iceberg_with_field(field_name: str):
    """Build a mock catalog/table/schema chain whose schema reports the
    given ``field_name`` — used to drive the PATCH route's inline
    "is field already in iceberg" check.
    """
    from unittest.mock import MagicMock

    fake_field = MagicMock()
    fake_field.name = field_name
    schema = MagicMock()
    schema.fields = [fake_field]
    table = MagicMock()
    table.schema.return_value = schema
    catalog = MagicMock()
    catalog.load_table.return_value = table
    return catalog


def test_custom_field_patch_rejects_type_change_when_field_in_iceberg():
    """PATCH that changes ``duckdb_type`` MUST 422 when the field already
    exists in the Iceberg table — type evolution after ingest would
    silently break readers. The route at services/core.py:1048-1069
    inspects the iceberg schema inline; this test drives that branch.
    """
    saved: dict = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    catalog = _fake_iceberg_with_field("cf_env")

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch("backend.core.iceberg._get_catalog", return_value=catalog),
        patch("backend.core.iceberg._table_identifier", return_value=("default", "logs")),
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
    """When the field has NOT yet been written to Iceberg (the schema
    doesn't contain it), changing duckdb_type is legal — PATCH 200s.
    """
    from unittest.mock import MagicMock

    saved: dict = {}

    def fake_save(sid, cfg):
        import copy

        saved.update(copy.deepcopy(cfg))

    # Schema with NO matching field → the check falls through to a normal save.
    schema = MagicMock()
    schema.fields = []  # empty
    table = MagicMock()
    table.schema.return_value = schema
    catalog = MagicMock()
    catalog.load_table.return_value = table

    with (
        patch("backend.config.load_config", return_value=_cfg_with_field()),
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.provision.validate_log_format", return_value=[]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc1", "bucket": "b"}),
        patch("backend.core.iceberg._get_catalog", return_value=catalog),
        patch("backend.core.iceberg._table_identifier", return_value=("default", "logs")),
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


# ---------------------------------------------------------------------------
# PATCH /api/services/{service_id}/credentials — api_token rotation path
# ---------------------------------------------------------------------------


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
        patch("backend.config.save_config", side_effect=fake_save),
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
        patch("backend.config.save_config", side_effect=fake_save),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        _client().patch(
            "/api/services/svc1/credentials",
            json={"api_token": "tok"},
        )

    # Only one fastly call happened (POST). No DELETE was made.
    assert [m for m, _ in fastly_calls] == ["POST"]


# ---------------------------------------------------------------------------
# POST /api/services/{service_id}/ngwaf-sync (SSE)
# ---------------------------------------------------------------------------


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
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("already running")),
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
