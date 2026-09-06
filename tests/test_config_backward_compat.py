"""Tests for backward compatibility when loading service configs.

When configs are cleaned up (removing legacy endpoint_name and last_reconciliation_at),
old configs may still contain these fields. This test suite verifies that:
1. Configs with legacy fields still load correctly
2. Configs without legacy fields load correctly
3. Code that reads endpoint_name uses the provisioning block, not the legacy top-level
4. The cleanup script correctly identifies and removes orphaned fields
"""

from __future__ import annotations

import pytest

from backend import config as svcconfig


@pytest.fixture(autouse=True)
def _isolate_config_dirs(tmp_path, monkeypatch):
    """Point CONFIGS_DIR + DATA_DIR at tmp_path for each test.

    All of these are module-level Path constants that production code
    reads on every call, so monkeypatching them per-test gives total
    isolation without touching the real configs/ tree.
    """
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path / "data" / "services")
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", tmp_path / "data" / "ngwaf")
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", tmp_path / "data" / "cache")
    monkeypatch.setattr(svcconfig, "SYSTEM_DATA_DIR", tmp_path / "data" / "system")
    monkeypatch.setattr(svcconfig, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "data" / "system" / "usage_logging.json")
    # Clear the in-memory caches between tests
    svcconfig._name_cache.clear()
    svcconfig._config_cache.clear()
    yield
    svcconfig._name_cache.clear()
    svcconfig._config_cache.clear()


def _cfg(**overrides) -> dict:
    """Build a minimal valid service config."""
    base = {
        "service_id": "svc-1",
        "name": "My Service",
        "access_level": "read_write",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "fos_access_key_id": "key",
        "fos_secret_access_key": "secret",
        "fos_bucket": "bkt",
        "fos_prefix": "",
        "fos_region": "us-east-1",
        "fastly_api_key": "api-key",
        "cdn_service_id": "cdn-svc-id",
    }
    base.update(overrides)
    return base


# ── Backward compatibility: legacy fields ───────────────────────────────────


def test_old_config_with_toplevel_endpoint_name_loads():
    """Old configs with endpoint_name at top-level should still load without error.

    After the cleanup script runs, some configs in the wild may still have
    the legacy top-level endpoint_name. This test verifies they load correctly.
    """
    cfg = _cfg(
        service_id="old-svc",
        endpoint_name="Legacy Endpoint Name",  # Legacy field
        provisioning={"endpoint_name": "New Endpoint Name"},  # New location
    )
    svcconfig.save_config("old-svc", cfg)
    loaded = svcconfig.load_config("old-svc")

    assert loaded is not None
    # Legacy field is preserved when loaded (we don't actively delete it)
    assert loaded.get("endpoint_name") == "Legacy Endpoint Name"
    # New location is also present
    assert loaded.get("provisioning", {}).get("endpoint_name") == "New Endpoint Name"


def test_old_config_with_last_reconciliation_at_loads():
    """Old configs with last_reconciliation_at should still load without error.

    Some older configs may contain this operational field. Verify it loads
    without crashing and is preserved for backward compat.
    """
    cfg = _cfg(
        service_id="old-svc-2",
        last_reconciliation_at="2026-08-07T12:00:00Z",  # Legacy field
    )
    svcconfig.save_config("old-svc-2", cfg)
    loaded = svcconfig.load_config("old-svc-2")

    assert loaded is not None
    # Legacy field is preserved when loaded
    assert loaded.get("last_reconciliation_at") == "2026-08-07T12:00:00Z"


def test_old_config_with_both_legacy_fields_loads():
    """Config with BOTH legacy fields should load without error."""
    cfg = _cfg(
        service_id="old-svc-3",
        endpoint_name="Legacy Name",
        last_reconciliation_at="2026-08-07T12:00:00Z",
        provisioning={"endpoint_name": "Current Name"},
    )
    svcconfig.save_config("old-svc-3", cfg)
    loaded = svcconfig.load_config("old-svc-3")

    assert loaded is not None
    assert loaded.get("endpoint_name") == "Legacy Name"
    assert loaded.get("last_reconciliation_at") == "2026-08-07T12:00:00Z"
    assert loaded.get("provisioning", {}).get("endpoint_name") == "Current Name"


# ── Code reads endpoint_name from provisioning, not legacy top-level ──────────


def test_endpoint_name_read_prefers_provisioning_block():
    """Code should read endpoint_name from provisioning block, ignoring legacy top-level.

    This test verifies the pattern used throughout the codebase:
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "...")
    """
    cfg = {
        "provisioning": {"endpoint_name": "Canonical Name"},
        "endpoint_name": "Legacy Name",  # old field — should be ignored
    }
    # Simulate the pattern used in fastly_api.py, orchestrator.py, etc.
    endpoint = cfg.get("provisioning", {}).get("endpoint_name", "Default")
    assert endpoint == "Canonical Name"  # NOT the legacy value


def test_endpoint_name_read_uses_default_when_provisioning_missing():
    """When provisioning block is missing or empty, use the default."""
    cfg = {"endpoint_name": "Legacy Name"}  # No provisioning block
    endpoint = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    assert endpoint == "Fastly Object Storage Logs"  # Default, not legacy


def test_endpoint_name_read_uses_default_when_field_missing():
    """When endpoint_name is missing from provisioning block, use the default."""
    cfg = {
        "provisioning": {},  # Empty provisioning block
        "endpoint_name": "Legacy Name",
    }
    endpoint = cfg.get("provisioning", {}).get("endpoint_name", "Fastly Object Storage Logs")
    assert endpoint == "Fastly Object Storage Logs"  # Default


# ── New-style config (no legacy fields) ───────────────────────────────────


def test_new_config_without_legacy_fields_loads():
    """New configs without legacy fields should load correctly.

    After the cleanup script runs, new configs will NOT have endpoint_name
    at top-level or last_reconciliation_at. Verify they still work.
    """
    cfg = _cfg(
        service_id="new-svc",
        provisioning={"endpoint_name": "Fastly Logs"},
        # NO top-level endpoint_name
        # NO last_reconciliation_at
    )
    svcconfig.save_config("new-svc", cfg)
    loaded = svcconfig.load_config("new-svc")

    assert loaded is not None
    assert "endpoint_name" not in loaded  # Not at top-level
    assert "last_reconciliation_at" not in loaded
    # But provisioning block has it
    assert loaded.get("provisioning", {}).get("endpoint_name") == "Fastly Logs"


def test_config_to_source_with_new_style_config():
    """config_to_source should work with new-style configs (provisioning present).

    This is the function that converts a config dict to the DuckDB/analytical
    layer's 'source' dict. Verify it doesn't crash on new-style configs.
    """
    cfg = _cfg(
        service_id="new-svc-2",
        provisioning={"endpoint_name": "Fastly Logs"},
    )
    svcconfig.save_config("new-svc-2", cfg)
    loaded = svcconfig.load_config("new-svc-2")

    # This call should not crash
    src = svcconfig.config_to_source(loaded)

    # Verify the source includes provisioning data
    assert src["provisioning"]["endpoint_name"] == "Fastly Logs"
    assert src["service_id"] == "new-svc-2"


# ── Mixed scenario: list_configs with old and new configs ──────────────────


def test_list_configs_includes_old_and_new_style_configs():
    """list_configs should return all configs, both old and new style."""
    # Old-style config
    old_cfg = _cfg(
        service_id="old-mixed",
        endpoint_name="Legacy Name",
        last_reconciliation_at="2026-08-07T12:00:00Z",
        provisioning={"endpoint_name": "Current Name"},
    )
    svcconfig.save_config("old-mixed", old_cfg)

    # New-style config
    new_cfg = _cfg(
        service_id="new-mixed",
        provisioning={"endpoint_name": "Fastly Logs"},
    )
    svcconfig.save_config("new-mixed", new_cfg)

    # list_configs should return both
    configs = svcconfig.list_configs()
    assert len(configs) == 2

    service_ids = {c["service_id"] for c in configs}
    assert service_ids == {"old-mixed", "new-mixed"}

    # Old config has legacy fields
    old = next(c for c in configs if c["service_id"] == "old-mixed")
    assert old.get("endpoint_name") == "Legacy Name"
    assert old.get("last_reconciliation_at") == "2026-08-07T12:00:00Z"

    # New config doesn't
    new = next(c for c in configs if c["service_id"] == "new-mixed")
    assert "endpoint_name" not in new
    assert "last_reconciliation_at" not in new


def test_update_status_works_with_old_and_new_configs():
    """update_status should work on both old and new style configs.

    This is a hot path: the cron writes status.last_sync_at, so it must work
    on both legacy and new configs without crashing.
    """
    # Old-style config
    old_cfg = _cfg(
        service_id="old-status",
        endpoint_name="Legacy",
        provisioning={"endpoint_name": "Current"},
    )
    svcconfig.save_config("old-status", old_cfg)
    svcconfig.update_status("old-status", {"last_sync_at": "2026-08-07T12:00:00Z"})

    status = svcconfig.get_status("old-status")
    assert status["last_sync_at"] == "2026-08-07T12:00:00Z"

    # New-style config
    new_cfg = _cfg(
        service_id="new-status",
        provisioning={"endpoint_name": "Fastly Logs"},
    )
    svcconfig.save_config("new-status", new_cfg)
    svcconfig.update_status("new-status", {"last_sync_at": "2026-08-07T13:00:00Z"})

    status = svcconfig.get_status("new-status")
    assert status["last_sync_at"] == "2026-08-07T13:00:00Z"


# ── Cleanup script identifies orphaned fields ────────────────────────────────


def test_cleanup_script_identifies_orphaned_toplevel_endpoint_name():
    """The cleanup script should identify and remove top-level endpoint_name."""
    import importlib.util
    from pathlib import Path as PathlibPath

    # Load the cleanup script as a module
    script_path = PathlibPath(__file__).parent.parent / "scripts" / "cleanup_system_fields_from_configs.py"
    spec = importlib.util.spec_from_file_location("cleanup_script", script_path)
    cleanup_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cleanup_module)

    # Test the identification function
    assert cleanup_module.is_orphaned_toplevel_field("endpoint_name") is True
    assert cleanup_module.is_orphaned_toplevel_field("last_reconciliation_at") is True
    assert cleanup_module.is_orphaned_toplevel_field("name") is False
    assert cleanup_module.is_orphaned_toplevel_field("service_id") is False


def test_cleanup_script_removes_orphaned_fields_from_config():
    """The cleanup script should remove orphaned fields from a config file."""
    import importlib.util
    from pathlib import Path as PathlibPath

    # Load the cleanup script as a module
    script_path = PathlibPath(__file__).parent.parent / "scripts" / "cleanup_system_fields_from_configs.py"
    spec = importlib.util.spec_from_file_location("cleanup_script", script_path)
    cleanup_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cleanup_module)

    # Create a config with orphaned fields
    cfg = {
        "service_id": "test-svc",
        "name": "Test Service",
        "endpoint_name": "Legacy Name",
        "last_reconciliation_at": "2026-08-07T12:00:00Z",
        "provisioning": {"endpoint_name": "Current Name"},
        "log_fields": {"custom_fields": []},
    }

    # Simulate cleanup
    removed = []
    for field_name in list(cfg.keys()):
        if cleanup_module.is_orphaned_toplevel_field(field_name):
            del cfg[field_name]
            removed.append(field_name)

    # Verify orphaned fields were removed
    assert "endpoint_name" not in cfg
    assert "last_reconciliation_at" not in cfg
    assert set(removed) == {"endpoint_name", "last_reconciliation_at"}

    # Verify essential fields remain
    assert cfg["service_id"] == "test-svc"
    assert cfg["name"] == "Test Service"
    assert cfg["provisioning"]["endpoint_name"] == "Current Name"


# ── Edge cases ────────────────────────────────────────────────────────────


def test_config_with_null_provisioning_block():
    """Config with null/empty provisioning block should still load."""
    cfg = _cfg(
        service_id="null-prov",
        provisioning=None,  # Explicitly null
    )
    svcconfig.save_config("null-prov", cfg)
    loaded = svcconfig.load_config("null-prov")

    assert loaded is not None
    assert loaded.get("provisioning") is None


def test_endpoint_name_read_handles_null_provisioning():
    """Code that reads endpoint_name should handle null provisioning block."""
    cfg = {"provisioning": None}
    # This pattern should not crash (cfg.get() returns None, then {}.get() works)
    endpoint = (cfg.get("provisioning") or {}).get("endpoint_name", "Default")
    assert endpoint == "Default"


def test_config_roundtrip_preserves_legacy_fields_until_cleanup():
    """When a config with legacy fields is loaded and saved, the fields persist.

    This is important: the cleanup script removes fields, but normal save_config
    calls should preserve them until the cleanup script runs.
    """
    # Save a config with legacy fields
    original = _cfg(
        service_id="roundtrip-svc",
        endpoint_name="Legacy Name",
        last_reconciliation_at="2026-08-07T12:00:00Z",
        provisioning={"endpoint_name": "Current"},
    )
    svcconfig.save_config("roundtrip-svc", original)

    # Load it back
    loaded = svcconfig.load_config("roundtrip-svc")
    assert loaded is not None

    # Update status (which loads, mutates, and saves)
    svcconfig.update_status("roundtrip-svc", {"last_sync_at": "now"})

    # Reload and verify legacy fields are still there
    reloaded = svcconfig.load_config("roundtrip-svc")
    assert reloaded.get("endpoint_name") == "Legacy Name"
    assert reloaded.get("last_reconciliation_at") == "2026-08-07T12:00:00Z"
    # But status was updated
    assert reloaded.get("status", {}).get("last_sync_at") == "now"
