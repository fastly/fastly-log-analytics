"""RUM enable/upgrade/disable persist only the keys they own, against the real (sandboxed) config store.

The orchestrators used to hold one in-memory config across minutes of Fastly calls and
``save_config`` the whole thing back (including in their rollback paths), erasing whatever a
concurrent writer changed meanwhile. They now apply their keys through ``mutate_config`` and
never hold the config lock across a Fastly/FOS call.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum, rum_vcl_fingerprint, upgrade_faro_version
from tests._helpers.config_fakes import config_lock_held as _lock_held
from tests._helpers.config_fakes import read_persisted_config as _persisted

SID = "logsvc1"
OLD_VERSION = "1.0.0"
NEW_VERSION = "1.2.3"

_FOS = {
    "fos_bucket": "bkt",
    "fos_region": "us-east-1",
    "fos_access_key_id": "AK",
    "fos_secret_access_key": "SK",
}


def _concurrent_writer(**top_level):
    """Stand-in for another request/cron writing to the config while a Fastly call is in flight."""

    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("rum", {})["concurrent_rum_key"] = "kept"

    assert not _lock_held(), "config lock held across a Fastly/FOS call"
    svcconfig.mutate_config(SID, _apply)


@pytest.fixture
def disabled_cfg():
    svcconfig.save_config(SID, {"service_id": SID, "keep_me": 1, "rum_enabled": False, **_FOS})
    return svcconfig.load_config(SID)


@pytest.fixture
def enabled_cfg():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "keep_me": 1,
            "rum_enabled": True,
            "rum_vcl_sha": "stale",
            "rum": {"faro_version": OLD_VERSION, "faro_content_hash": "oldhash", "faro_fos_etag_md5": "oldmd5"},
            **_FOS,
        },
    )
    return svcconfig.load_config(SID)


@pytest.fixture
def stubs():
    mod = "backend.provision.rum_orchestrator_v2"
    with (
        patch(f"{mod}.download_and_upload_faro", new_callable=AsyncMock) as upload,
        patch(f"{mod}.reconcile_vcl_state") as reconcile,
        patch(f"{mod}.upload_rum_tracker_js") as tracker,
        patch(f"{mod}.cleanup_old_faro_versions", new_callable=AsyncMock),
        patch(f"{mod}._purge_faro_surrogate_key"),
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as delete_js,
    ):
        reconcile.return_value = MagicMock(activated_version=7, draft_version=None)
        yield MagicMock(upload=upload, reconcile=reconcile, tracker=tracker, delete_js=delete_js)


# ── enable_rum ───────────────────────────────────────────────────────────────


def test_enable_persists_owned_keys_and_keeps_the_rest(disabled_cfg, stubs):
    salt = disabled_cfg["rum"]["cid_salt"]

    out = enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is True
    assert saved["rum_enabled_at"] == out["enabled_at"]
    assert saved["rum"]["faro_version"] == NEW_VERSION
    assert saved["rum"]["cid_salt"] == salt
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["keep_me"] == 1


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(disabled_cfg, stubs):
    held: list[bool] = []

    async def _upload(*_a, **_kw):
        held.append(_lock_held())

    def _reconcile(*_a, **_kw):
        held.append(_lock_held())
        return MagicMock(activated_version=7, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    enable_rum(SID, "tok", faro_version=NEW_VERSION)

    assert held == [False, False]


def test_enable_rollback_after_upload_failure_keeps_concurrent_writes(disabled_cfg, stubs):
    async def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("upload failed")

    stubs.upload.side_effect = _boom

    with pytest.raises(RuntimeError, match="upload failed"):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "rum_vcl_sha" not in saved
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == disabled_cfg["rum"]["cid_salt"]


def test_enable_rollback_after_reconcile_failure_keeps_concurrent_writes(disabled_cfg, stubs):
    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"


def test_enable_rollback_restores_the_previous_pin(stubs):
    svcconfig.save_config(
        SID,
        {"service_id": SID, "rum_enabled": False, "rum": {"faro_version": OLD_VERSION}, **_FOS},
    )
    stubs.reconcile.side_effect = RuntimeError("nope")

    with pytest.raises(RuntimeError):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    assert _persisted()["rum"]["faro_version"] == OLD_VERSION


# ── upgrade_faro_version ─────────────────────────────────────────────────────


def test_upgrade_reconcile_failure_restores_the_pin_and_keeps_concurrent_writes(enabled_cfg, stubs):
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(
            SID,
            lambda c: c["rum"].update(
                faro_version=NEW_VERSION, faro_content_hash="newhash", faro_fos_etag_md5="newmd5"
            ),
        )

    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        upgrade_faro_version(SID, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum"]["faro_version"] == OLD_VERSION
    assert saved["rum"]["faro_content_hash"] == "oldhash"
    assert saved["rum"]["faro_fos_etag_md5"] == "oldmd5"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]
    assert saved["concurrent_key"] == "kept"


def test_upgrade_success_refreshes_the_fingerprint_and_keeps_concurrent_writes(enabled_cfg, stubs):
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(SID, lambda c: c["rum"].update(faro_version=NEW_VERSION))

    def _reconcile(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        return MagicMock(activated_version=9, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    upgrade_faro_version(SID, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["rum_vcl_sha"] != "stale"
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"


# ── disable_rum ──────────────────────────────────────────────────────────────


def test_disable_persists_owned_keys_and_keeps_the_rest(enabled_cfg, stubs):
    disable_rum(SID, "tok")

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "rum_vcl_sha" not in saved
    assert saved["rum"] == enabled_cfg["rum"]
    assert saved["keep_me"] == 1


def test_disable_does_not_hold_the_config_lock_across_fastly_calls(enabled_cfg, stubs):
    held: list[bool] = []
    stubs.delete_js.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    stubs.reconcile.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), MagicMock(activated_version=2))[1]

    disable_rum(SID, "tok")

    assert held == [False, False]


def test_disable_rollback_restores_enabled_and_keeps_concurrent_writes(enabled_cfg, stubs):
    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        disable_rum(SID, "tok")

    saved = _persisted()
    assert saved["rum_enabled"] is True
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]
