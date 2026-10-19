"""Config writes done by ensure_cdn_service / redeploy_cdn_vcl go through the config lock.

They used to ``write_text`` the service config directly: no lock, no atomic replace, no
cid-salt protection, no cache invalidation. These run against the real (sandboxed) config store.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backend import config as svcconfig
from backend.provision import fastly_api

SID = "logsvc1"


@pytest.fixture
def reconcile():
    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile,
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True),
    ):
        mock_reconcile.return_value = SimpleNamespace(activated_version=3)
        yield mock_reconcile


def test_ensure_cdn_service_creates_config_when_absent(reconcile):
    assert svcconfig.load_config(SID) is None

    reconcile.side_effect = lambda **kw: svcconfig.mutate_config(
        SID, lambda c: c.setdefault("fos_proxy", {}).update(service_id="cdn123")
    )
    out = fastly_api.ensure_cdn_service({"service_id": SID, "fos_bucket": "b"}, "AK", "SK", "tok")

    saved = json.loads(svcconfig.config_path(SID).read_text())
    assert saved["fos_bucket"] == "b"
    assert saved["fos_access_key_id"] == "AK"
    assert saved["fos_secret_access_key"] == "SK"
    assert saved["rum"]["cid_salt"]
    assert out["id"] == "cdn123"


def test_ensure_cdn_service_merges_into_existing_config_and_keeps_cid_salt(reconcile):
    svcconfig.save_config(SID, {"service_id": SID, "keep_me": 1, "rum": {"enabled": True}})
    salt = svcconfig.load_config(SID)["rum"]["cid_salt"]

    # the orchestrator's in-memory cfg replaces the rum block wholesale, without the salt
    fastly_api.ensure_cdn_service({"service_id": SID, "fos_bucket": "b", "rum": {"enabled": False}}, "AK", "SK", "tok")

    saved = json.loads(svcconfig.config_path(SID).read_text())
    assert saved["keep_me"] == 1
    assert saved["fos_bucket"] == "b"
    assert saved["fos_access_key_id"] == "AK"
    assert saved["rum"]["enabled"] is False
    assert saved["rum"]["cid_salt"] == salt


def test_redeploy_cdn_vcl_sets_rate_limiting_without_touching_other_keys():
    svcconfig.save_config(
        SID, {"service_id": SID, "cdn_service_id": "cdn9", "fos_proxy": {"service_id": "cdn9"}, "other": [1, 2]}
    )
    with patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile:
        mock_reconcile.return_value = SimpleNamespace(activated_version=4)
        assert fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=False) == 4

    saved = json.loads(svcconfig.config_path(SID).read_text())
    assert saved["fos_proxy"] == {"service_id": "cdn9", "rate_limiting_enabled": False}
    assert saved["other"] == [1, 2]


def test_redeploy_cdn_vcl_creates_missing_fos_proxy_block():
    svcconfig.save_config(SID, {"service_id": SID, "cdn_service_id": "cdn9"})
    with patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile:
        mock_reconcile.return_value = SimpleNamespace(activated_version=5)
        fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=True)

    assert svcconfig.load_config(SID)["fos_proxy"] == {"rate_limiting_enabled": True}
