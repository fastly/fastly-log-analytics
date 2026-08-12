"""Regression coverage for system-managed custom-field reconciliation.

Pins the invariant behind the 2026-08-12 SE-demo CMCD incident: CMCD's 14
``cmcd_*`` custom fields are generated from code and hidden from the
user-editable list by ``_is_system_field``, so any writer that persists
``log_fields`` from a list it did not author omits them. The omission removes
them from the generated Fastly log format while the extraction VCL stays
installed, so ``req.http.x-cmcd:*`` is still populated at the edge and never
logged — every ``cmcd_*`` column ingests empty and /streaming renders all
zeros with no error.

Scoring already had this guard (2026-06-02 incident); CMCD did not.
"""

from __future__ import annotations

from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES
from backend.provision.system_fields import (
    reconcile_cfg_system_custom_fields,
    reconcile_system_custom_fields,
    system_feature_flags,
)

_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


def _names(fields: list[dict]) -> set[str]:
    return {cf["name"] for cf in fields}


# ── system_feature_flags ─────────────────────────────────────────────────────


def test_flags_read_both_features():
    assert system_feature_flags({}) == (False, False)
    assert system_feature_flags({"cmcd": {"enabled": True}}) == (False, True)
    assert system_feature_flags({"scoring": {"enabled": True}}) == (True, False)
    assert system_feature_flags({"scoring": {"enabled": True}, "cmcd": {"enabled": True}}) == (True, True)


def test_flags_tolerate_null_blocks():
    """A config that carries ``cmcd: null`` must not raise."""
    assert system_feature_flags({"cmcd": None, "scoring": None}) == (False, False)


# ── reconcile_system_custom_fields ───────────────────────────────────────────


def test_cmcd_fields_reinjected_when_enabled():
    """THE REGRESSION: a non-empty list that omits cmcd_* must regain them."""
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=False, cmcd_enabled=True)
    names = _names(out)
    for name in _CMCD_FIELD_NAMES:
        assert name in names, f"CMCD field {name!r} was not re-injected"
    assert "my_custom" in names, "user custom_field was wrongly stripped"


def test_cmcd_fields_stripped_when_disabled():
    """Disable must converge too — stale cmcd_* entries are removed."""
    stale = [_USER_FIELD, {"name": "cmcd_sid", "duckdb_type": "VARCHAR"}]
    out = reconcile_system_custom_fields(stale, scoring_enabled=False, cmcd_enabled=False)
    assert _names(out) == {"my_custom"}


def test_stale_cmcd_entries_replaced_by_canonical():
    """A remote/partial cmcd_* entry is replaced, not duplicated."""
    stale = [{"name": "cmcd_sid", "duckdb_type": "WRONG", "enabled": False}]
    out = reconcile_system_custom_fields(stale, scoring_enabled=False, cmcd_enabled=True)
    sids = [cf for cf in out if cf["name"] == "cmcd_sid"]
    assert len(sids) == 1, "cmcd_sid duplicated instead of replaced"
    assert sids[0]["duckdb_type"] == "VARCHAR"
    assert sids[0]["enabled"] is True


def test_both_features_coexist():
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=True)
    names = _names(out)
    assert _CMCD_FIELD_NAMES <= names
    assert _SCORING_FIELD_NAMES <= names
    assert "my_custom" in names


def test_scoring_enabled_does_not_drag_in_cmcd():
    """The two features are independent — scoring on, CMCD off."""
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=False)
    names = _names(out)
    assert _SCORING_FIELD_NAMES <= names
    assert not (_CMCD_FIELD_NAMES & names)


def test_none_input_is_safe():
    out = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    assert _CMCD_FIELD_NAMES <= _names(out)


def test_returned_entries_are_copies():
    """Callers mutating the result must not corrupt the module-level canon."""
    first = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    first[0]["label"] = "MUTATED"
    second = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    assert second[0]["label"] != "MUTATED"


def test_idempotent():
    once = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=True)
    twice = reconcile_system_custom_fields(once, scoring_enabled=True, cmcd_enabled=True)
    assert _names(once) == _names(twice)
    assert len(once) == len(twice), "repeat reconcile duplicated entries"


# ── reconcile_cfg_system_custom_fields ───────────────────────────────────────


def test_cfg_level_reconcile_mutates_in_place():
    cfg = {
        "cmcd": {"enabled": True},
        "log_fields": {"schema_version": 2, "custom_fields": [_USER_FIELD]},
    }
    reconcile_cfg_system_custom_fields(cfg)
    names = _names(cfg["log_fields"]["custom_fields"])
    assert _CMCD_FIELD_NAMES <= names
    assert "my_custom" in names


def test_cfg_level_reconcile_creates_missing_log_fields():
    """A config with no log_fields block still gets the v2 default shape."""
    cfg = {"cmcd": {"enabled": True}}
    reconcile_cfg_system_custom_fields(cfg)
    assert cfg["log_fields"]["schema_version"] == 2
    assert _CMCD_FIELD_NAMES <= _names(cfg["log_fields"]["custom_fields"])


def test_cfg_level_reconcile_strips_when_feature_off():
    cfg = {
        "log_fields": {
            "schema_version": 2,
            "custom_fields": [_USER_FIELD, {"name": "cmcd_br", "duckdb_type": "INTEGER"}],
        }
    }
    reconcile_cfg_system_custom_fields(cfg)
    assert _names(cfg["log_fields"]["custom_fields"]) == {"my_custom"}
