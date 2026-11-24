"""Enabled log fields are resolved when a config is saved and read back without resolving again."""

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


def test_save_stores_the_resolved_ids(isolated_configs_dir):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    stored = svcconfig.load_config(SID)["active_log_field_ids"]
    assert stored == _expected(LOG_FIELDS)
    assert "ttl" in stored and "url" not in stored and "gone" not in stored


def test_reads_use_the_stored_ids_without_resolving(isolated_configs_dir, monkeypatch):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    cfg = svcconfig.load_config(SID)
    cfg["active_log_field_ids"] = ["as_deployed"]

    def boom(_cfg):
        raise AssertionError("resolved on read")

    monkeypatch.setattr(catalog, "resolve_enabled_fields", boom)
    assert svcconfig.active_log_field_ids(cfg) == ["as_deployed"]


def test_a_config_saved_before_ids_were_stored_is_resolved_from_its_settings():
    assert svcconfig.active_log_field_ids({"service_id": SID, "log_fields": LOG_FIELDS}) == _expected(LOG_FIELDS)


def test_a_log_field_edit_replaces_the_stored_ids(isolated_configs_dir):
    svcconfig.save_config(SID, {"service_id": SID, "log_fields": LOG_FIELDS})
    edited = {**LOG_FIELDS, "field_overrides": {"url": True}}
    svcconfig.mutate_config(SID, lambda cfg: cfg.__setitem__("log_fields", edited))
    assert svcconfig.load_config(SID)["active_log_field_ids"] == _expected(edited)
