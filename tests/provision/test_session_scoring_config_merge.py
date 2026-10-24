"""Session-scoring flows persist only the keys they own, against the real (sandboxed) config store.

enable/disable used to hold one config snapshot across minutes of Fastly/Compute calls and
``save_config`` the whole thing back, including in the rollback path, erasing whatever a
concurrent writer changed meanwhile. They now apply their keys (the ``scoring`` block and the
scoring ``log_fields.custom_fields``) through ``mutate_config`` and never hold the config lock
across a Fastly call.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision import session_scoring_orchestrator as sso
from tests._helpers.config_fakes import config_lock_held as _lock_held
from tests._helpers.config_fakes import read_persisted_config as _persisted

SID = "logsvc1"
TOKEN = "FAKE_TOKEN"
USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}

_META = {
    "scoring_service_id": "ScoringSvcZ",
    "scoring_service_name": f"Session Scoring Service for {SID}",
    "scoring_domain": "scorer.example.test",
    "scoring_keys_store_id": "KEYS",
    "scoring_config_store_id": "CFG",
    "scoring_matrix_store_id": "MATRIX",
    "request_secret": "s" * 64,
}


def _concurrent_write(**top_level) -> None:

    assert not _lock_held(), "config lock held across a Fastly call"

    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("log_fields", {}).setdefault("custom_fields", []).append(dict(USER_FIELD))

    svcconfig.mutate_config(SID, _apply)


def _field_names(cfg: dict) -> set[str]:
    return {cf["name"] for cf in cfg["log_fields"]["custom_fields"]}


@pytest.fixture
def base_cfg():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "keep_me": 1,
            "log_fields": {"groups": ["A"], "custom_fields": []},
            "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        },
    )
    return svcconfig.load_config(SID)


@pytest.fixture
def enable_stubs():
    with (
        patch.object(sso, "ensure_scoring_service", return_value=dict(_META)) as ensure,
        patch.object(
            sso, "_deploy_wasm_package", return_value={"sha": "pkg", "version": 3, "files_hash": "fh"}
        ) as deploy,
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch.object(sso, "get_active_version", return_value=10),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile,
    ):
        reconcile.return_value = MagicMock(error=None, activated_version=11)
        yield MagicMock(ensure=ensure, deploy=deploy, reconcile=reconcile)


@contextmanager
def _fake_draft(*_a, **_kw):
    yield 201


@pytest.fixture
def disable_stubs():
    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "draft_version", _fake_draft),
        patch.object(sso, "_remove_scoring_snippets") as snippets,
        patch.object(sso, "_remove_scoring_backend"),
        patch.object(sso, "ensure_vcl_snippet"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch("backend.core.fastly.service.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.generate_capture_vcl", return_value={"recv": ""}),
        patch.object(sso, "delete_scoring_service", return_value=[]) as delete,
        patch.object(sso, "_delete_scoring_matrix_from_fos"),
        patch("backend.state_sync.export_admin_state"),
    ):
        yield MagicMock(snippets=snippets, delete=delete)


@pytest.fixture
def enabled_cfg(base_cfg):
    def _apply(c: dict) -> None:
        c["scoring"] = {
            "enabled": True,
            "scoring_service_id": "ScoringSvcZ",
            "scoring_keys_store_id": "KEYS",
            "scoring_config_store_id": "CFG",
            "scoring_matrix_store_id": "MATRIX",
            "exclude_url_regex": "operator-set",
        }
        sso._add_scoring_custom_fields(c)

    svcconfig.mutate_config(SID, _apply)
    return svcconfig.load_config(SID)


# ── enable_scoring ───────────────────────────────────────────────────────────


def test_enable_persists_scoring_block_and_fields_and_keeps_the_rest(base_cfg, enable_stubs):
    salt = base_cfg["rum"]["cid_salt"]

    sso.enable_scoring(SID, TOKEN)

    saved = _persisted()
    assert saved["scoring"]["enabled"] is True
    assert saved["scoring"]["scoring_service_id"] == "ScoringSvcZ"
    assert saved["scoring"]["deployed_package_sha"] == "pkg"
    assert saved["scoring"]["request_secret"] == "s" * 64
    assert set(sso._SCORING_FIELD_NAMES) <= _field_names(saved)
    assert saved["keep_me"] == 1
    assert saved["log_fields"]["groups"] == ["A"]
    assert saved["rum"]["cid_salt"] == salt


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(base_cfg, enable_stubs):
    held: list[bool] = []
    enable_stubs.ensure.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), dict(_META))[1]
    enable_stubs.deploy.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        {"sha": "pkg", "version": 3, "files_hash": "fh"},
    )[1]
    enable_stubs.reconcile.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        MagicMock(error=None, activated_version=11),
    )[1]

    sso.enable_scoring(SID, TOKEN)

    assert held == [False, False, False]


def test_enable_keeps_writes_made_while_the_scorer_deploys(base_cfg, enable_stubs):
    def _deploy(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        return {"sha": "pkg", "version": 3, "files_hash": "fh"}

    enable_stubs.deploy.side_effect = _deploy

    sso.enable_scoring(SID, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "my_custom" in _field_names(saved)
    assert set(sso._SCORING_FIELD_NAMES) <= _field_names(saved)
    assert saved["scoring"]["enabled"] is True


def test_enable_keeps_an_operator_regex_change_made_while_deploying(enabled_cfg, enable_stubs):
    def _deploy(*_a, **_kw):
        svcconfig.mutate_config(SID, lambda c: c["scoring"].update(exclude_url_regex="changed-mid-deploy"))
        return {"sha": "pkg", "version": 3, "files_hash": "fh"}

    enable_stubs.deploy.side_effect = _deploy

    sso.enable_scoring(SID, TOKEN)

    assert _persisted()["scoring"]["exclude_url_regex"] == "changed-mid-deploy"


def test_enable_rollback_removes_only_scoring_state_and_keeps_concurrent_writes(base_cfg, enable_stubs):
    def _reconcile(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        raise RuntimeError("reconcile exploded")

    enable_stubs.reconcile.side_effect = _reconcile

    with pytest.raises(RuntimeError, match="reconcile exploded"):
        sso.enable_scoring(SID, TOKEN)

    saved = _persisted()
    assert "scoring" not in saved
    assert not (set(sso._SCORING_FIELD_NAMES) & _field_names(saved))
    assert "my_custom" in _field_names(saved)
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == base_cfg["rum"]["cid_salt"]


# ── disable_scoring ──────────────────────────────────────────────────────────


def test_disable_strips_scoring_and_keeps_user_fields(enabled_cfg, disable_stubs):
    sso.disable_scoring(SID, TOKEN)

    saved = _persisted()
    assert "scoring" not in saved
    assert not (set(sso._SCORING_FIELD_NAMES) & _field_names(saved))
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]


def test_disable_keeps_writes_made_during_the_vcl_phase(enabled_cfg, disable_stubs):
    disable_stubs.snippets.side_effect = lambda *_a, **_kw: _concurrent_write(concurrent_key="kept")

    sso.disable_scoring(SID, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "my_custom" in _field_names(saved)
    assert "scoring" not in saved


def test_disable_keeps_writes_made_during_compute_teardown(enabled_cfg, disable_stubs):
    def _delete(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        return []

    disable_stubs.delete.side_effect = _delete

    sso.disable_scoring(SID, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "scoring" not in saved


# ── update_recv_exclusion_regex / update_enforce_status_code ─────────────────


@pytest.fixture
def swap_stubs():
    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "_clone_swap_snippet", return_value=201),
    ):
        yield


def test_update_recv_exclusion_regex_sets_only_that_key(enabled_cfg, swap_stubs):
    sso.update_recv_exclusion_regex(SID, TOKEN, new_regex="  \\.foo$ ")

    saved = _persisted()
    assert saved["scoring"]["exclude_url_regex"] == "\\.foo$"
    assert saved["scoring"]["scoring_service_id"] == "ScoringSvcZ"
    assert saved["keep_me"] == 1


def test_update_enforce_status_code_sets_only_that_key(enabled_cfg, swap_stubs):
    sso.update_enforce_status_code(SID, TOKEN, new_status_code=403)

    saved = _persisted()
    assert saved["scoring"]["enforce_status_code"] == 403
    assert saved["scoring"]["exclude_url_regex"] == "operator-set"
    assert saved["keep_me"] == 1


@pytest.mark.parametrize(
    "call",
    [
        lambda: sso.update_recv_exclusion_regex(SID, TOKEN, new_regex="x"),
        lambda: sso.update_enforce_status_code(SID, TOKEN, new_status_code=403),
    ],
)
def test_updates_refuse_and_write_nothing_when_scoring_is_disabled(base_cfg, swap_stubs, call):
    before = _persisted()

    with pytest.raises(RuntimeError, match="not enabled"):
        call()

    assert _persisted() == before
    assert "scoring" not in _persisted()
