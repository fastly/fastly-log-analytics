"""Enabled log fields are resolved when a config is saved and read back without resolving again."""

import pytest

from backend import config as svcconfig
from backend.core import catalog

SID = "svcActiveFields1"
LOG_FIELDS = {
    "groups": ["A"],
    "field_overrides": {"ttl": True, "url": False},
    "custom_fields": [{"name": "tenant", "enabled": True}, {"name": "gone", "enabled": False}],
}


def _expected(lf: dict) -> list[str]:
    return sorted(catalog.resolve_enabled_fields(lf)) + ["tenant"]


def test_save_stores_the_resolved_ids_under_the_catalog_fingerprint(isolated_configs_dir):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    stored = svcconfig.load_config(SID)["active_log_fields"]
    assert stored == {"catalog": catalog.CATALOG_FINGERPRINT, "ids": _expected(LOG_FIELDS)}
    assert "ttl" in stored["ids"] and "url" not in stored["ids"] and "gone" not in stored["ids"]


def test_reads_use_the_stored_ids_without_resolving(isolated_configs_dir, monkeypatch):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    cfg = svcconfig.load_config(SID)
    want = _expected(LOG_FIELDS)

    def boom(_cfg):
        raise AssertionError("resolved on read")

    monkeypatch.setattr(catalog, "resolve_enabled_fields", boom)
    assert svcconfig.active_log_field_ids(cfg) == want


@pytest.mark.parametrize("stored", [None, {"catalog": "older-catalog", "ids": ["stale"]}])
def test_a_missing_or_other_catalog_entry_is_resolved_from_the_settings(stored):
    cfg = {"service_id": SID, "log_fields": LOG_FIELDS, **({"active_log_fields": stored} if stored else {})}
    assert svcconfig.active_log_field_ids(cfg) == _expected(LOG_FIELDS)


def test_a_log_field_edit_replaces_the_stored_ids(isolated_configs_dir):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    edited = {**LOG_FIELDS, "field_overrides": {"url": True}}
    svcconfig.mutate_config(SID, lambda cfg: cfg.__setitem__("log_fields", edited))
    assert svcconfig.load_config(SID)["active_log_fields"]["ids"] == _expected(edited)
