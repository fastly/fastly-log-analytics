"""A feature turns on the log fields it needs and gives back only what it turned on."""

from __future__ import annotations

import pytest

from backend.core.catalog import resolve_enabled_fields
from backend.provision import feature_fields
from backend.provision.feature_fields import claim_feature_fields, release_feature_fields

pytestmark = pytest.mark.no_postgres


def _cfg(**log_fields) -> dict:
    return {"rum_enabled": True, "log_fields": {"groups": ["A"], "field_overrides": {}, **log_fields}}


def _rid_on(cfg: dict) -> bool:
    return "rid" in resolve_enabled_fields(cfg["log_fields"])


def test_rum_turns_on_the_request_id_and_turns_it_off_again_on_disable() -> None:
    cfg = _cfg()
    assert not _rid_on(cfg)
    claim_feature_fields(cfg, "rum")
    assert _rid_on(cfg) and cfg["feature_enabled_fields"] == {"rum": ["rid"]}
    assert "prid" not in resolve_enabled_fields(cfg["log_fields"])  # not the rest of group L
    first_hash = cfg["log_fields"]["format_hash"]
    claim_feature_fields(cfg, "rum")  # idempotent
    assert cfg["feature_enabled_fields"] == {"rum": ["rid"]}

    cfg["rum_enabled"] = False
    release_feature_fields(cfg, "rum")
    assert not _rid_on(cfg) and "rid" not in cfg["log_fields"]["field_overrides"]
    assert cfg["feature_enabled_fields"] == {} and cfg["log_fields"]["format_hash"] != first_hash


@pytest.mark.parametrize("already_on", [{"groups": ["A", "L"]}, {"field_overrides": {"rid": True}}])
def test_a_field_that_was_already_on_is_left_on_when_rum_is_disabled(already_on: dict) -> None:
    cfg = _cfg(**already_on)
    claim_feature_fields(cfg, "rum")
    assert cfg.get("feature_enabled_fields", {}).get("rum", []) == []
    cfg["rum_enabled"] = False
    release_feature_fields(cfg, "rum")
    assert _rid_on(cfg)


def test_a_shared_field_stays_on_while_another_enabled_feature_still_needs_it(monkeypatch) -> None:
    monkeypatch.setitem(feature_fields.FEATURE_LOG_FIELDS, "other", ("rid",))
    monkeypatch.setitem(feature_fields._FEATURE_ENABLED, "other", lambda cfg: bool(cfg.get("other_enabled")))
    cfg = _cfg(other_enabled=True) | {"other_enabled": True}
    claim_feature_fields(cfg, "rum")
    cfg["rum_enabled"] = False
    release_feature_fields(cfg, "rum")
    # Still needed by the other feature, which now owns turning it off.
    assert _rid_on(cfg) and cfg["feature_enabled_fields"] == {"other": ["rid"]}

    cfg["other_enabled"] = False
    release_feature_fields(cfg, "other")
    assert not _rid_on(cfg)
