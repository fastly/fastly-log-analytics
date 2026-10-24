"""Coverage for CMCD custom-field cleanup helper."""

from __future__ import annotations

from backend.provision import cmcd_orchestrator as orch
from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES, reconcile_cmcd_custom_fields


def test_remove_custom_fields_strips_cmcd_fields_only():
    cfg = {
        "log_fields": {
            "custom_fields": reconcile_cmcd_custom_fields([{"name": "mine"}], enabled=True),
        }
    }
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert _CMCD_FIELD_NAMES <= names and "mine" in names

    orch._remove_cmcd_custom_fields(cfg)
    names = {c["name"] for c in cfg["log_fields"]["custom_fields"]}
    assert not (_CMCD_FIELD_NAMES & names)
    assert "mine" in names


def test_remove_custom_fields_tolerates_missing_blocks():
    for cfg in ({}, {"log_fields": {}}, {"log_fields": {"custom_fields": []}}):
        orch._remove_cmcd_custom_fields(cfg)  # must not raise
