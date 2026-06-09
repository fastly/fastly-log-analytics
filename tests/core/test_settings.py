"""Tests for `backend.core.settings`.

Covers:
- Defaults match the os.environ.get() shape they replace
- Field aliases resolve case-insensitively
- Trust-topology validator fires when REQUIRE_PROXY_HEADERS=1 without TRUSTED_PROXY_IPS
- get_settings() caching behaviour + reset_settings() invalidation
- Boolean coercion ("1" / "true" / "0" / "false")
"""

from __future__ import annotations

import pytest

from backend.core import settings as settings_mod


@pytest.fixture(autouse=True)
def _reset_cache():
    """Clear the cached Settings instance per test so env-var monkeypatches
    take effect."""
    settings_mod.reset_settings()
    yield
    settings_mod.reset_settings()


def test_defaults_when_env_is_empty(monkeypatch):
    for var in (
        "TRUSTED_PROXY_IPS",
        "UVICORN_FORWARDED_ALLOW_IPS",
        "REQUIRE_PROXY_HEADERS",
        "STRICT_DATA_DIR_CHECK",
        "OTEL_ENABLED",
        "STRUCTLOG_FORMAT",
        "DEBUG_RESPONSES",
        "DUCKDB_MEMORY_LIMIT",
        "DUCKDB_THREADS",
        "DUCKDB_POOL_MAX_SIZE",
        "FASTLY_API_KEY",
        "SCORING_REQUIRE_FALCO",
    ):
        monkeypatch.delenv(var, raising=False)

    s = settings_mod.Settings()
    assert s.trusted_proxy_ips == ""
    assert s.uvicorn_forwarded_allow_ips == ""
    assert s.require_proxy_headers is False
    assert s.strict_data_dir_check is False
    assert s.otel_enabled is True
    assert s.structlog_format == "console"
    assert s.debug_responses is False
    assert s.duckdb_memory_limit is None
    assert s.duckdb_threads is None
    assert s.duckdb_pool_max_size == 8
    assert s.fastly_api_key is None
    assert s.scoring_require_falco is False


def test_aliases_are_case_insensitive(monkeypatch):
    monkeypatch.setenv("trusted_proxy_ips", "127.0.0.1")
    monkeypatch.setenv("OTEL_ENABLED", "0")
    s = settings_mod.Settings()
    assert s.trusted_proxy_ips == "127.0.0.1"
    assert s.otel_enabled is False


def test_boolean_coercion_handles_zero_and_false(monkeypatch):
    monkeypatch.setenv("OTEL_ENABLED", "0")
    s = settings_mod.Settings()
    assert s.otel_enabled is False

    monkeypatch.setenv("OTEL_ENABLED", "false")
    s = settings_mod.Settings()
    assert s.otel_enabled is False


def test_boolean_coercion_handles_one_and_true(monkeypatch):
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_IPS", "127.0.0.1")  # required by validator
    s = settings_mod.Settings()
    assert s.require_proxy_headers is True

    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "true")
    s = settings_mod.Settings()
    assert s.require_proxy_headers is True


def test_strict_mode_requires_trusted_proxy_ips(monkeypatch):
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("UVICORN_FORWARDED_ALLOW_IPS", raising=False)

    with pytest.raises(ValueError, match="TRUSTED_PROXY_IPS is unset"):
        settings_mod.Settings()


def test_strict_data_dir_check_also_requires_trusted_proxy_ips(monkeypatch):
    monkeypatch.setenv("STRICT_DATA_DIR_CHECK", "1")
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("UVICORN_FORWARDED_ALLOW_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)

    with pytest.raises(ValueError, match="TRUSTED_PROXY_IPS is unset"):
        settings_mod.Settings()


def test_strict_mode_accepts_uvicorn_forwarded_allow_ips_alternative(monkeypatch):
    """Either of the two env vars satisfies the validator."""
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.setenv("UVICORN_FORWARDED_ALLOW_IPS", "127.0.0.1")

    s = settings_mod.Settings()
    assert s.require_proxy_headers is True
    assert s.uvicorn_forwarded_allow_ips == "127.0.0.1"


def test_get_settings_caches_instance():
    a = settings_mod.get_settings()
    b = settings_mod.get_settings()
    assert a is b


def test_reset_settings_invalidates_cache(monkeypatch):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    a = settings_mod.get_settings()
    assert a.otel_enabled is True

    monkeypatch.setenv("OTEL_ENABLED", "0")
    # Without reset, cached value persists.
    b = settings_mod.get_settings()
    assert b.otel_enabled is True  # cache wins

    settings_mod.reset_settings()
    c = settings_mod.get_settings()
    assert c.otel_enabled is False  # re-read after reset


def test_is_strict_mode_shortcut(monkeypatch):
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("STRICT_DATA_DIR_CHECK", raising=False)
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("UVICORN_FORWARDED_ALLOW_IPS", raising=False)
    assert settings_mod.is_strict_mode() is False

    settings_mod.reset_settings()
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")
    monkeypatch.setenv("TRUSTED_PROXY_IPS", "127.0.0.1")
    assert settings_mod.is_strict_mode() is True


def test_unknown_env_var_does_not_raise(monkeypatch):
    """extra='ignore' means unrelated env vars don't break instantiation."""
    monkeypatch.setenv("SOMETHING_COMPLETELY_UNRELATED", "value")
    s = settings_mod.Settings()
    assert s.otel_enabled is True
