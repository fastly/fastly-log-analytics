from backend.provision.declarative.config_migration import config_changed, migrate_config


def test_migrate_flat_cmcd_to_nested():
    """Flat CMCD fields migrate to nested object."""
    cfg = {
        "service_id": "svc123",
        "cmcd_enabled": True,
        "cmcd_mode": "headers",
        "cmcd_version": 2,
    }
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 2}
    assert "cmcd_enabled" not in migrated
    assert "cmcd_mode" not in migrated
    assert "cmcd_version" not in migrated


def test_migrate_flat_scoring_to_nested():
    """Flat Scoring fields migrate to nested object."""
    cfg = {
        "service_id": "svc123",
        "scoring_enabled": True,
        "scoring_domain": "score.example.com",
        "scoring_request_secret": "secret123",
        "scoring_exclude_url_regex": "^/health",
        "scoring_enforce_status_code": 429,
    }
    migrated = migrate_config(cfg)
    assert migrated["scoring"] == {
        "enabled": True,
        "domain": "score.example.com",
        "request_secret": "secret123",
        "exclude_url_regex": "^/health",
        "enforce_status_code": 429,
    }
    assert "scoring_enabled" not in migrated
    assert "scoring_domain" not in migrated


def test_migrate_both_flat_and_nested_cmcd():
    """When both flat and nested exist, flat overrides and is consolidated."""
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": False, "mode": "query_string"},
        "cmcd_enabled": True,  # Flat overrides
        "cmcd_mode": "headers",
    }
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 1}
    assert "cmcd_enabled" not in migrated
    assert "cmcd_mode" not in migrated


def test_migrate_already_nested_unchanged():
    """Already-nested config is returned unchanged."""
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": True, "mode": "headers", "version": 2},
        "scoring": {"enabled": False, "domain": ""},
    }
    migrated = migrate_config(cfg)
    assert migrated == cfg
    assert config_changed(cfg, migrated) is False


def test_migrate_empty_config():
    """Empty config returns empty config."""
    cfg = {}
    migrated = migrate_config(cfg)
    assert migrated == {}
    assert config_changed(cfg, migrated) is False


def test_config_changed_detects_flat_to_nested():
    """config_changed returns True when flat fields are consolidated."""
    before = {
        "service_id": "svc123",
        "cmcd_enabled": True,
    }
    after = {
        "service_id": "svc123",
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
    }
    assert config_changed(before, after) is True
