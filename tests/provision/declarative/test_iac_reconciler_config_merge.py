"""reconcile_infrastructure persists and rolls back only the keys it owns, against the real (sandboxed) store.

It used to snapshot the whole config, spend minutes on Fastly/Compute calls, then replace the
file with the stale in-memory copy (and restore the stale snapshot on failure), erasing any
concurrent writer's changes. The owned keys are the ``scoring`` block and the scoring
``log_fields.custom_fields``; everything else, including the cid salt, is left as found.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision.declarative.iac_reconciler import reconcile_infrastructure
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

SID = "srv_123"
MOD = "backend.provision.declarative.iac_reconciler"
SCORING_FIELD = {"name": sorted(_SCORING_FIELD_NAMES)[0], "duckdb_type": "VARCHAR", "enabled": True}
USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


def _lock_held() -> bool:
    return bool(getattr(svcconfig._lock_state, "held", {}).get(SID))


def _persisted() -> dict:
    return json.loads(svcconfig.config_path(SID).read_text())


def _concurrent_write() -> None:
    assert not _lock_held(), "config lock held across a Fastly call"
    svcconfig.mutate_config(SID, lambda c: c.update(concurrent_key="kept"))


def _seed(**extra) -> dict:
    svcconfig.save_config(SID, {"service_id": SID, "log_period": 60, "keep_me": 1, **extra})
    return svcconfig.load_config(SID)


@pytest.fixture
def enable_stubs():
    with (
        patch(f"{MOD}.discover_current_state", return_value={"kv_stores": {}, "compute_scorer": None}),
        patch(f"{MOD}.ensure_scoring_service") as ensure,
        patch(f"{MOD}._write_matrix_to_kv"),
        patch(f"{MOD}._deploy_wasm_package", return_value={"sha": "sha1", "files_hash": "fh1"}) as deploy,
        patch(f"{MOD}.verify_compute_scorer_readiness"),
        patch(f"{MOD}.reconcile_vcl_state") as reconcile,
    ):
        ensure.return_value = {"scoring_service_id": "compute_id", "scoring_matrix_store_id": "matrix_id"}
        yield MagicMock(ensure=ensure, deploy=deploy, reconcile=reconcile)


@pytest.fixture
def disable_stubs():
    with (
        patch(
            f"{MOD}.discover_current_state",
            return_value={
                "kv_stores": {"scoring_matrix_srv_123": {"id": "matrix_id"}},
                "compute_scorer": {"id": "compute_id"},
            },
        ),
        patch(f"{MOD}.reconcile_vcl_state") as reconcile,
        patch(f"{MOD}.delete_scoring_service") as delete,
        patch(f"{MOD}.time.sleep"),
    ):
        yield MagicMock(reconcile=reconcile, delete=delete)


@pytest.fixture
def scoring_desired():
    """The desired state is built from the stored config: scoring enabled means a compute scorer is wanted."""
    with patch(f"{MOD}.build_desired_state") as build:
        build.return_value = MagicMock(compute_scorer=MagicMock())
        yield build


@pytest.fixture
def scoring_not_desired():
    with patch(f"{MOD}.build_desired_state") as build:
        build.return_value = MagicMock(compute_scorer=None)
        yield build


def test_enable_persists_only_scoring_and_keeps_concurrent_writes(enable_stubs, scoring_desired):
    seeded = _seed(scoring={"enabled": True, "request_secret": "r" * 8})

    def _ensure(*_a, **_kw):
        _concurrent_write()
        return {"scoring_service_id": "compute_id", "scoring_matrix_store_id": "matrix_id"}

    enable_stubs.ensure.side_effect = _ensure

    reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"]["scoring_service_id"] == "compute_id"
    assert saved["scoring"]["deployed_package_sha"] == "sha1"
    assert saved["scoring"]["request_secret"] == "r" * 8
    assert saved["concurrent_key"] == "kept"
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(enable_stubs, scoring_desired):
    held: list[bool] = []
    enable_stubs.deploy.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        {"sha": "sha1", "files_hash": "fh1"},
    )[1]
    enable_stubs.reconcile.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    _seed(scoring={"enabled": True})

    reconcile_infrastructure(SID, "tok")

    assert held == [False, False]


def test_failure_after_persist_rolls_back_only_scoring_and_keeps_concurrent_writes(enable_stubs, scoring_desired):
    original_scoring = {"enabled": True, "scoring_domain": "old.example.test"}
    seeded = _seed(scoring=original_scoring)

    def _boom(*_a, **_kw):
        assert _persisted()["scoring"]["scoring_service_id"] == "compute_id"
        _concurrent_write()
        raise RuntimeError("vcl sync failed")

    enable_stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"] == original_scoring
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]


def test_failure_after_persist_removes_scoring_that_did_not_exist_before(enable_stubs, scoring_desired):
    _seed()
    enable_stubs.reconcile.side_effect = RuntimeError("vcl sync failed")

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    assert "scoring" not in _persisted()


def test_failure_before_any_persist_leaves_the_file_untouched(enable_stubs, scoring_desired):
    _seed(scoring={"enabled": True})
    before = svcconfig.config_path(SID).read_text()
    enable_stubs.ensure.side_effect = RuntimeError("fastly 429")

    with pytest.raises(RuntimeError, match="fastly 429"):
        reconcile_infrastructure(SID, "tok")

    assert svcconfig.config_path(SID).read_text() == before


def test_disable_strips_scoring_fields_and_block_and_keeps_concurrent_writes(disable_stubs, scoring_not_desired):
    seeded = _seed(
        scoring={"enabled": True, "scoring_keys_store_id": "k", "scoring_config_store_id": "c"},
        log_fields={"custom_fields": [dict(SCORING_FIELD), dict(USER_FIELD)]},
    )
    disable_stubs.reconcile.side_effect = lambda *_a, **_kw: _concurrent_write()

    reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert "scoring" not in saved
    assert [cf["name"] for cf in saved["log_fields"]["custom_fields"]] == ["my_custom"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]
    assert disable_stubs.delete.call_args.kwargs["scoring_keys_store_id"] == "k"


def test_disable_failure_restores_scoring_block_and_fields_and_keeps_concurrent_writes(
    disable_stubs, scoring_not_desired
):
    original_scoring = {"enabled": True, "scoring_keys_store_id": "k"}
    _seed(scoring=original_scoring, log_fields={"custom_fields": [dict(SCORING_FIELD), dict(USER_FIELD)]})

    def _boom(*_a, **_kw):
        stripped = _persisted()
        assert stripped["scoring"]["enabled"] is False
        assert [cf["name"] for cf in stripped["log_fields"]["custom_fields"]] == ["my_custom"]
        _concurrent_write()
        raise RuntimeError("vcl sync failed")

    disable_stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"] == original_scoring
    assert {cf["name"] for cf in saved["log_fields"]["custom_fields"]} == {SCORING_FIELD["name"], "my_custom"}
    assert saved["concurrent_key"] == "kept"
    disable_stubs.delete.assert_not_called()
