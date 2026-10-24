"""update_logging_endpoint persists through ``mutate_config`` against the real (sandboxed) config store.

It used to load the whole config, spend the Fastly rate-limit probe in between, then
``save_config`` the stale copy twice: a concurrent writer's keys were lost and the cid salt
only survived through the save-time re-adoption guard.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend import config as svcconfig
from backend.provision import fastly_api
from backend.provision.declarative.reconciler import ReconciliationResult
from tests._helpers.config_fakes import config_lock_held as _lock_held
from tests._helpers.config_fakes import read_persisted_config as _persisted

SID = "logsvc1"


def _drain(gen):
    return list(gen)


@pytest.fixture
def stored():
    svcconfig.save_config(SID, {"service_id": SID, "keep_me": 1, "provisioning": {"keep_prov": "x"}})
    return svcconfig.load_config(SID)


@pytest.fixture
def fastly_stubs():
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None) as rl,
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id=SID, activated_version=11, changes_applied=["fmt"]),
        ) as reconcile,
    ):
        yield rl, reconcile


def test_persists_requested_fields_and_keeps_everything_else(stored, fastly_stubs):

    salt = stored["rum"]["cid_salt"]

    events = _drain(
        fastly_api.update_logging_endpoint(
            {
                "logging_service_id": SID,
                "provisioning": {"endpoint_name": "EP"},
                "sample_rate": 50,
                "edge_only": True,
                "log_period": 120,
                "fos_path": "/raw/%H/",
            },
            "tok",
        )
    )

    saved = _persisted()
    assert saved["provisioning"] == {"keep_prov": "x", "sample_rate": 50, "edge_only": True, "endpoint_name": "EP"}
    assert saved["log_period"] == 120
    assert saved["fos_path"] == "/raw/%H/"
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == salt
    assert events[-1]["type"] == "done"


def test_rate_limit_probe_result_lands_without_clobbering_a_concurrent_writer(stored, fastly_stubs):
    rl, _ = fastly_stubs

    def probe(_token, _sid):
        assert not _lock_held(), "config lock held across a Fastly call"
        svcconfig.mutate_config(SID, lambda c: c.update(concurrent_key="kept"))
        return True

    rl.side_effect = probe

    _drain(fastly_api.update_logging_endpoint({"logging_service_id": SID, "log_period": 60}, "tok"))

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert saved["provisioning"]["rate_limiting"] is True
    assert saved["keep_me"] == 1


def test_config_lock_is_not_held_while_reconciling(stored, fastly_stubs):
    _, reconcile = fastly_stubs
    seen: list[bool] = []

    def spy(*_a, **_kw):
        seen.append(_lock_held())
        return ReconciliationResult(service_id=SID, activated_version=11, changes_applied=["fmt"])

    reconcile.side_effect = spy

    _drain(fastly_api.update_logging_endpoint({"logging_service_id": SID, "log_period": 60}, "tok"))

    assert seen == [False]


def test_missing_config_raises_before_any_fastly_call(fastly_stubs):
    rl, reconcile = fastly_stubs

    with pytest.raises(RuntimeError, match="not found|empty"):
        _drain(fastly_api.update_logging_endpoint({"logging_service_id": SID, "log_period": 60}, "tok"))

    rl.assert_not_called()
    reconcile.assert_not_called()
    assert not svcconfig.config_path(SID).exists()
