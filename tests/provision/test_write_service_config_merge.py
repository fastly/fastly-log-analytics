"""write_service_config merges its wizard-owned keys into the stored config, against the real (sandboxed) store.

It used to build a whole new config from the request body and ``save_config`` it over the file,
dropping every stored key the body did not carry beyond four hand-listed ones.
"""

from __future__ import annotations

from backend import config as svcconfig
from backend.provision import orchestrator
from tests._helpers.config_fakes import read_persisted_config as _persisted

SID = "logsvc1"


def _state(**overrides) -> dict:
    base = {
        "logging_service_id": SID,
        "service_name": "Test Service",
        "fos_region": "us-east-1",
        "fos_bucket_name": "test-bucket",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "secret",
        "cdn_url": "https://test.example",
        "cdn_secret": "cdn-secret",
        "cdn_service_id": "cdn-1",
        "admin_token": "tok",
        "log_period": 60,
        "log_fields": {"groups": ["A"]},
    }
    base.update(overrides)
    return base


def test_first_write_creates_the_config_with_a_cid_salt():

    assert svcconfig.load_config(SID) is None

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["service_id"] == SID
    assert saved["name"] == "Test Service"
    assert saved["fos_bucket"] == "test-bucket"
    assert saved["fastly_api_key"] == "tok"
    assert saved["provisioning"]["cron_sync"]["interval_seconds"] == 60
    assert saved["created_at"]
    assert saved["rum"]["cid_salt"]


def test_rewrite_keeps_code_managed_blocks_and_the_cid_salt():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "scoring": {"enabled": True, "scoring_service_id": "S"},
            "cmcd": {"enabled": True},
            "ngwaf_workspace_id": "ws",
            "rum": {"faro_version": "1.2.3"},
            "log_fields": {"groups": ["A"], "custom_fields": [{"name": "mine", "duckdb_type": "VARCHAR"}]},
        },
    )
    salt = svcconfig.load_config(SID)["rum"]["cid_salt"]
    created_at = svcconfig.load_config(SID)["created_at"]

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["scoring"] == {"enabled": True, "scoring_service_id": "S"}
    assert saved["cmcd"] == {"enabled": True}
    assert saved["ngwaf_workspace_id"] == "ws"
    assert saved["rum"]["faro_version"] == "1.2.3"
    assert saved["rum"]["cid_salt"] == salt
    assert saved["created_at"] == created_at
    assert [cf["name"] for cf in saved["log_fields"]["custom_fields"]] == ["mine"]


def test_rewrite_does_not_replace_the_cid_salt_with_one_from_the_request_body():
    svcconfig.save_config(SID, {"service_id": SID})
    salt = svcconfig.load_config(SID)["rum"]["cid_salt"]

    orchestrator.write_service_config(_state(rum={"enabled": True, "cid_salt": "attacker-chosen"}))

    saved = _persisted()
    assert saved["rum"]["cid_salt"] == salt
    assert saved["rum"]["enabled"] is True


def test_rewrite_keeps_stored_keys_the_wizard_does_not_own():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "fos_proxy": {"service_id": "cdn-proxy"},
            "status": {"state": "ok"},
            "last_activated_version": 7,
            "rum_vcl_sha": "sha",
        },
    )

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["fos_proxy"] == {"service_id": "cdn-proxy"}
    assert saved["status"] == {"state": "ok"}
    assert saved["last_activated_version"] == 7
    assert saved["rum_vcl_sha"] == "sha"
    assert saved["fos_bucket"] == "test-bucket"
