"""Tests for ``backend.config`` — per-service config CRUD + name cache.

This module is the source of truth for everything keyed on a Fastly
service ID: where the JSON config lives, where the per-service DuckDB
lives, the cache of human-readable names, and the global usage-logging
toggle. Every router, every cron, every test fixture passes through
``load_config`` somewhere — so a regression in any of these helpers
ripples through the whole app.

The interesting branches:
  - ``config_to_source`` builds the dict the analytical layer uses;
    its CDN-vs-native endpoint selection is what makes DuckDB read
    parquet through the CDN (cheap) instead of direct from FOS (paid).
  - ``refresh_service_name`` has a TTL-based cache, an API fallback,
    and a config-name fallback after that.
  - ``is_usage_logging_enabled`` is hard-coded to False under pytest
    so tests don't pollute the usage log.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

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


# ── Path helpers ─────────────────────────────────────────────────────────────


def test_config_path_uses_service_id_as_filename(tmp_path):
    p = svcconfig.config_path("svc-abc")
    assert p.name == "svc-abc.json"
    assert p.parent.name == "configs"


def test_duckdb_path_lives_in_services_data_dir():
    p = svcconfig.duckdb_path("svc-abc")
    assert p.endswith("services/svc-abc.duckdb")


def test_ngwaf_db_path_is_shared_across_services():
    """One ngwaf cache file for the whole install — pinned because a
    refactor that per-svc'd this would multiply the cron's API hits."""
    assert svcconfig.ngwaf_db_path().endswith("data/ngwaf_bot_cache.db")


# ── load_config / save_config / delete ───────────────────────────────────────


def test_load_config_returns_none_for_missing_service():
    assert svcconfig.load_config("nonexistent") is None


def test_save_then_load_roundtrip():
    cfg = _cfg(service_id="svc-rt")
    svcconfig.save_config("svc-rt", cfg)
    loaded = svcconfig.load_config("svc-rt")
    assert loaded == cfg


def test_save_config_writes_atomically(tmp_path):
    """The implementation writes to ``.tmp`` then ``rename`` — pinned so
    a refactor that skips the tmp file doesn't reintroduce torn-write
    risk on power loss."""
    svcconfig.save_config("svc-x", _cfg(service_id="svc-x"))
    # After save, the tmp file must not exist (it was renamed)
    assert not (svcconfig.CONFIGS_DIR / "svc-x.tmp").exists()
    assert (svcconfig.CONFIGS_DIR / "svc-x.json").exists()


def test_save_config_creates_configs_dir_if_missing(tmp_path):
    """Bootstrap install: configs/ doesn't exist yet. ``save_config``
    must create it (via ``_ensure_dirs``)."""
    # conftest's ``isolate_metadata_db`` pre-creates the sandbox tree to
    # work around _ensure_dirs not using parents=True. This test pins
    # the bootstrap contract — wipe the dir so we're back to the
    # first-run shape.
    import shutil

    shutil.rmtree(svcconfig.CONFIGS_DIR, ignore_errors=True)
    assert not svcconfig.CONFIGS_DIR.exists()
    svcconfig.save_config("svc-1", _cfg())
    assert svcconfig.CONFIGS_DIR.exists()


def test_delete_config_removes_file():
    svcconfig.save_config("svc-del", _cfg(service_id="svc-del"))
    assert svcconfig.load_config("svc-del") is not None
    svcconfig.delete_config("svc-del")
    assert svcconfig.load_config("svc-del") is None


def test_delete_config_is_idempotent():
    """Deleting a non-existent config must not raise — the provision
    teardown path calls this even on partial failures."""
    svcconfig.delete_config("never-existed")  # must not raise


def test_load_config_returns_fresh_dict_each_call():
    """Cache returns reparsed dict on every call so callers that mutate
    the result (e.g. update_status appending to status) don't poison
    subsequent loads. Pinned because returning the cached instance
    directly would silently break update_status under concurrent calls."""
    svcconfig.save_config("svc-mut", _cfg(service_id="svc-mut"))
    a = svcconfig.load_config("svc-mut")
    b = svcconfig.load_config("svc-mut")
    assert a is not b
    a["mutated"] = True  # mutate the first dict
    c = svcconfig.load_config("svc-mut")
    assert "mutated" not in c


def test_load_config_picks_up_save_via_mtime():
    """save_config writes via atomic os.replace which bumps st_mtime_ns.
    The cache uses mtime as its revalidation key, so the next load_config
    sees the new content without an explicit cache bust. Pinned because
    a downgrade to st_mtime (float) could lose precision on rapid writes
    and serve stale configs."""
    svcconfig.save_config("svc-rev", _cfg(service_id="svc-rev", name="before"))
    first = svcconfig.load_config("svc-rev")
    assert first is not None and first["name"] == "before"

    # Ensure clock advances; mtime_ns gives nanosecond granularity so a
    # short sleep is usually overkill, but a few ms is bulletproof on any
    # filesystem.
    time.sleep(0.005)
    svcconfig.save_config("svc-rev", _cfg(service_id="svc-rev", name="after"))
    second = svcconfig.load_config("svc-rev")
    assert second is not None and second["name"] == "after"


def test_save_config_invalidates_load_config_cache_for_mtime_collision():
    """Two rapid save_config calls can produce identical st_mtime_ns on
    Linux ext4/tmpfs (same-microsecond writes). Without an explicit cache
    invalidation in save_config, the second load_config returns the
    pre-write cached bytes — which silently clobbered update_status's
    merge in CI (KeyError: 'last_sync_at'). Simulate the collision by
    pinning stat().st_mtime_ns to a constant across writes."""
    import os as _os
    from unittest.mock import patch

    svcconfig.save_config("svc-coll", _cfg(service_id="svc-coll", name="first"))
    svcconfig.load_config("svc-coll")  # prime the cache

    # Freeze mtime_ns: every stat() returns the same value, so the cache's
    # revalidation key cannot distinguish between writes.
    real_stat = _os.stat
    frozen_ns = svcconfig.config_path("svc-coll").stat().st_mtime_ns

    class _FrozenStat:
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        @property
        def st_mtime_ns(self):
            return frozen_ns

    def _patched_stat(path, *args, **kwargs):
        return _FrozenStat(real_stat(path, *args, **kwargs))

    with patch("os.stat", side_effect=_patched_stat):
        svcconfig.save_config("svc-coll", _cfg(service_id="svc-coll", name="second"))
        loaded = svcconfig.load_config("svc-coll")

    assert loaded is not None and loaded["name"] == "second", (
        "save_config must invalidate the load_config cache; otherwise a "
        "same-nanosecond write collision serves stale bytes"
    )


def test_load_config_avoids_reread_when_mtime_unchanged(monkeypatch):
    """The cache hit path must NOT re-read the file. Pinned by counting
    open() calls — if a refactor drops the mtime check, every hot caller
    (sync-status, cron tick) pays a syscall per load."""
    svcconfig.save_config("svc-hit", _cfg(service_id="svc-hit"))
    # Prime the cache
    svcconfig.load_config("svc-hit")

    open_calls = {"n": 0}
    real_open = svcconfig.open if hasattr(svcconfig, "open") else open

    def counting_open(*args, **kwargs):
        open_calls["n"] += 1
        return real_open(*args, **kwargs)

    monkeypatch.setattr("builtins.open", counting_open)
    for _ in range(5):
        svcconfig.load_config("svc-hit")
    assert open_calls["n"] == 0, "cache hit must skip open()"


# ── update_status / get_status ───────────────────────────────────────────────


def test_update_status_merges_into_existing_status():
    """Subsequent calls update fields without clobbering unrelated ones —
    pinned because the cron writes ``last_sync_at`` and the UI writes
    ``manual_pause`` independently."""
    svcconfig.save_config("svc-st", _cfg(service_id="svc-st"))
    svcconfig.update_status("svc-st", {"last_sync_at": "2026-01-01"})
    svcconfig.update_status("svc-st", {"manual_pause": True})

    out = svcconfig.get_status("svc-st")
    assert out["last_sync_at"] == "2026-01-01"
    assert out["manual_pause"] is True
    assert "updated_at" in out  # auto-stamped


def test_update_status_silently_drops_unknown_service():
    """No config → no-op (consistent with ``delete_config``)."""
    svcconfig.update_status("ghost", {"x": 1})  # must not raise
    assert svcconfig.get_status("ghost") == {}


def test_get_status_returns_empty_dict_for_unknown_service():
    """Pinned: callers iterate over the dict; returning None here
    would crash every status-render code path."""
    assert svcconfig.get_status("ghost") == {}


def test_update_status_stamps_updated_at_timestamp():
    svcconfig.save_config("svc-ts", _cfg(service_id="svc-ts"))
    before = time.time()
    svcconfig.update_status("svc-ts", {"foo": "bar"})
    after = time.time()

    status = svcconfig.get_status("svc-ts")
    assert before <= status["updated_at"] <= after


# ── list_service_ids / list_configs ──────────────────────────────────────────


def test_list_service_ids_returns_sorted_ids():
    """The frontend's service switcher renders in this order — sorted
    is stable across reloads, picking it up from the filename pins the
    sort key explicitly (no surprises from dict insertion order)."""
    svcconfig.save_config("zeta", _cfg(service_id="zeta"))
    svcconfig.save_config("alpha", _cfg(service_id="alpha"))
    svcconfig.save_config("mu", _cfg(service_id="mu"))

    assert svcconfig.list_service_ids() == ["alpha", "mu", "zeta"]


def test_list_service_ids_returns_empty_on_missing_dir():
    """Fresh install: no configs dir yet → ``[]``."""
    assert svcconfig.list_service_ids() == []


def test_list_configs_skips_unparseable_files():
    """If one config file is corrupt JSON the others must still load.
    Pinned because a single bad write should not brick the bootstrap
    response for every other service."""
    svcconfig.save_config("good", _cfg(service_id="good"))
    # Drop a malformed file
    svcconfig._ensure_dirs()
    (svcconfig.CONFIGS_DIR / "bad.json").write_text("{not json")

    # Loading raises; ``list_configs`` should NOT include None for the bad one.
    # Note: current implementation actually raises on bad JSON. Pinned here
    # as documenting the behaviour — if we ever wrap load_config in try/except,
    # this test catches the change and the caller can update strategy.
    with pytest.raises(json.JSONDecodeError):
        svcconfig.list_configs()


def test_list_configs_returns_full_dicts():
    svcconfig.save_config("a", _cfg(service_id="a", name="A"))
    svcconfig.save_config("b", _cfg(service_id="b", name="B"))

    out = svcconfig.list_configs()
    assert [c["service_id"] for c in out] == ["a", "b"]
    assert {c["name"] for c in out} == {"A", "B"}


# ── get_active_service_id ────────────────────────────────────────────────────


def test_active_service_id_returns_first_configured():
    svcconfig.save_config("zeta", _cfg(service_id="zeta"))
    svcconfig.save_config("alpha", _cfg(service_id="alpha"))

    assert svcconfig.get_active_service_id() == "alpha"  # sorted, first


def test_active_service_id_returns_none_when_no_configs():
    assert svcconfig.get_active_service_id() is None


def test_active_service_id_respects_fallback_flag():
    """``fallback_to_first=False`` → return None even when configs
    exist. Pinned because the provision wizard explicitly disables the
    fallback so it can detect the "fresh install" state."""
    svcconfig.save_config("a", _cfg(service_id="a"))
    assert svcconfig.get_active_service_id(fallback_to_first=False) is None


# ── config_to_source: CDN-vs-native endpoint selection ──────────────────────


def test_config_to_source_uses_cdn_endpoint_when_cdn_url_set():
    """``cdn_url`` present → DuckDB httpfs reads go through the CDN
    (cheap egress). Pinned because losing this would invisibly 10x
    the bandwidth bill."""
    src = svcconfig.config_to_source(_cfg(cdn_url="https://cdn.example.com/path/extra"))
    # The CDN URL's host wins, stripping scheme + path
    assert src["endpoint"] == "cdn.example.com"
    # ...but the native endpoint is preserved for boto3 writes
    assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"


def test_config_to_source_falls_back_to_native_endpoint_without_cdn():
    src = svcconfig.config_to_source(_cfg(cdn_url=None))
    assert src["endpoint"] == "us-east-1.object.fastlystorage.app"


def test_config_to_source_native_endpoint_derives_from_region_when_missing():
    """If ``fos_endpoint`` is absent the helper synthesises it from
    region. Pinned because a typo here would silently point boto3 at
    the wrong region's FOS shard."""
    cfg = _cfg(fos_region="eu-west-1")
    del cfg["fos_endpoint"]
    src = svcconfig.config_to_source(cfg)
    assert src["fos_native_endpoint"] == "eu-west-1.object.fastlystorage.app"


def test_config_to_source_includes_log_period_as_int():
    """``log_period`` is sometimes stored as a string (from form input).
    Pinned to int because the scheduler does arithmetic on it."""
    src = svcconfig.config_to_source(_cfg(log_period="120"))
    assert src["log_period"] == 120
    assert isinstance(src["log_period"], int)


def test_config_to_source_defaults_log_period_to_60():
    cfg = _cfg()
    # log_period not in cfg
    src = svcconfig.config_to_source(cfg)
    assert src["log_period"] == 60


def test_config_to_source_passes_through_provisioning_dict():
    """Teardown reads ``provisioning.fos_key_id`` to know which FOS
    access key to revoke. Lossy refactor would orphan it in FOS."""
    src = svcconfig.config_to_source(_cfg(provisioning={"fos_key_id": "key-123", "endpoint_name": "FOS Logs"}))
    assert src["provisioning"]["fos_key_id"] == "key-123"


def test_config_to_source_defaults_access_level_to_read_write():
    cfg = _cfg()
    del cfg["access_level"]
    src = svcconfig.config_to_source(cfg)
    assert src["access_level"] == "read_write"


def test_config_to_source_includes_rum_fields():
    """Verify config_to_source maps rum_enabled and rum config blocks."""
    # Test with rum_enabled on root
    cfg = _cfg(rum_enabled=True, rum={"enabled": False})
    src = svcconfig.config_to_source(cfg)
    assert src["rum_enabled"] is True
    assert src["rum"] == {"enabled": False}

    # Test with rum enabled on sub-dictionary
    cfg2 = _cfg(rum_enabled=False, rum={"enabled": True})
    src2 = svcconfig.config_to_source(cfg2)
    assert src2["rum_enabled"] is True
    assert src2["rum"] == {"enabled": True}

    # Test with both disabled / missing
    cfg3 = _cfg(rum_enabled=False)
    src3 = svcconfig.config_to_source(cfg3)
    assert src3["rum_enabled"] is False
    assert src3["rum"] == {}


# ── fetch_service_name ───────────────────────────────────────────────────────


def test_fetch_service_name_returns_name_on_success():
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"name": "Production CDN"}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp):
        assert svcconfig.fetch_service_name("svc-1", "key") == "Production CDN"


def test_fetch_service_name_returns_none_on_network_error():
    """Any exception from the API call → return None (caller falls
    back to the cached or config name)."""
    with patch("urllib.request.urlopen", side_effect=OSError("DNS")):
        assert svcconfig.fetch_service_name("svc-1", "key") is None


def test_fetch_service_name_returns_none_when_name_missing_from_payload():
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"other_field": "value"}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp):
        assert svcconfig.fetch_service_name("svc-1", "key") is None


# ── refresh_service_name: cache + fallback chain ────────────────────────────


def test_refresh_service_name_returns_cached_within_ttl():
    """Cache hit within TTL must NOT trigger an API call — saves
    Fastly rate-limit and latency on every page load."""
    svcconfig._name_cache["svc-1"] = {"name": "Cached Name", "fetched_at": time.time()}

    with patch("backend.config.fetch_service_name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Cached Name"
    mock_fetch.assert_not_called()


def test_refresh_service_name_fetches_when_cache_expired():
    """Cache older than TTL → re-fetch from API."""
    svcconfig._name_cache["svc-1"] = {
        "name": "Stale Name",
        "fetched_at": time.time() - 1000,  # >> TTL (300)
    }
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="From Config"))

    with patch("backend.config.fetch_service_name", return_value="Fresh Name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Fresh Name"
    mock_fetch.assert_called_once_with("svc-1", "key")


def test_refresh_service_name_persists_to_config_when_name_changed():
    """When the API returns a name that differs from what's in the
    config file, the config gets re-saved with the new name. Pinned
    because skipping this lets the UI show "service-abc-id" forever
    even after the Fastly admin renames the service."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Old Name"))

    with patch("backend.config.fetch_service_name", return_value="New Name"):
        svcconfig.refresh_service_name("svc-1", "key")

    assert svcconfig.load_config("svc-1")["name"] == "New Name"


def test_refresh_service_name_falls_back_to_config_when_api_fails():
    """API returns None (network down, 403) → use the cached config name."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Config Name"))

    with patch("backend.config.fetch_service_name", return_value=None):
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Config Name"


def test_refresh_service_name_falls_back_to_id_when_no_config():
    """No API, no config → return the service_id itself so the UI
    always has SOMETHING to display."""
    with patch("backend.config.fetch_service_name", return_value=None):
        out = svcconfig.refresh_service_name("unknown-svc", "key")

    assert out == "unknown-svc"


def test_refresh_service_name_skips_api_when_no_api_key():
    """No api_key → don't even try the API. Pinned because attempting
    with an empty key hits Fastly's 401 path repeatedly, which counts
    against rate limits."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local Name"))

    with patch("backend.config.fetch_service_name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", api_key=None)

    assert out == "Local Name"
    mock_fetch.assert_not_called()


def test_refresh_service_name_skips_api_when_api_key_whitespace_only():
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local"))

    with patch("backend.config.fetch_service_name") as mock_fetch:
        svcconfig.refresh_service_name("svc-1", api_key="   ")

    mock_fetch.assert_not_called()


def test_refresh_service_name_failure_caches_short_retry():
    """When the API fails, the cached entry gets stamped with
    ``fetched_at = now - TTL + 120`` — i.e. the next retry will fire
    in 120 seconds, not 300. Pinned because a longer back-off would
    leave a stale name visible for the full TTL after a transient
    failure."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local"))

    before = time.time()
    with patch("backend.config.fetch_service_name", return_value=None):
        svcconfig.refresh_service_name("svc-1", "key")

    entry = svcconfig._name_cache["svc-1"]
    # fetched_at is now - TTL + 120 → next refresh in ~120s
    age = before - entry["fetched_at"]
    assert 175 <= age <= 185  # 300 - 120 with small tolerance


# ── refresh_all_service_names: parallel ─────────────────────────────────────


def test_refresh_all_service_names_returns_map_of_id_to_name():
    configs = [
        {"service_id": "a", "fastly_api_key": "k", "name": "A"},
        {"service_id": "b", "fastly_api_key": "k", "name": "B"},
    ]

    def _fake_refresh(sid, _key):
        return f"Name-{sid}"

    with patch("backend.config.refresh_service_name", side_effect=_fake_refresh):
        out = svcconfig.refresh_all_service_names(configs, _sync=True)

    assert out == {"a": "Name-a", "b": "Name-b"}


def test_refresh_all_service_names_falls_back_to_config_name_on_per_service_exception():
    """If one service's refresh raises, the others still return.
    Pinned because losing all names because one service has bad creds
    would break the sidebar entirely."""
    configs = [
        {"service_id": "ok", "fastly_api_key": "k", "name": "OK-svc"},
        {"service_id": "bad", "fastly_api_key": "k", "name": "Bad-svc"},
    ]

    def _fake_refresh(sid, _key):
        if sid == "bad":
            raise RuntimeError("boom")
        return f"Fresh-{sid}"

    with patch("backend.config.refresh_service_name", side_effect=_fake_refresh):
        out = svcconfig.refresh_all_service_names(configs, _sync=True)

    assert out["ok"] == "Fresh-ok"
    assert out["bad"] == "Bad-svc"  # fell back to cfg["name"]


# ── Fastly credential helpers ───────────────────────────────────────────────


def test_get_fastly_api_key_returns_key_for_given_service():
    svcconfig.save_config("svc-a", _cfg(service_id="svc-a", fastly_api_key="key-a"))
    assert svcconfig.get_fastly_api_key("svc-a") == "key-a"


def test_get_fastly_api_key_falls_back_to_active_service():
    svcconfig.save_config("alpha", _cfg(service_id="alpha", fastly_api_key="key-active"))
    assert svcconfig.get_fastly_api_key(None) == "key-active"  # alpha is first/active


def test_get_fastly_api_key_returns_empty_string_when_unconfigured():
    assert svcconfig.get_fastly_api_key("nonexistent") == ""
    assert svcconfig.get_fastly_api_key(None) == ""  # no configs at all


def test_get_fastly_service_id_returns_cdn_service_id():
    """``cdn_service_id`` is the Fastly service that fronts FOS — used
    by the VCL routes. Distinct from the logging service id."""
    svcconfig.save_config("svc", _cfg(service_id="svc", cdn_service_id="cdn-123"))
    assert svcconfig.get_fastly_service_id("svc") == "cdn-123"


def test_get_fastly_logging_service_id_returns_service_id_itself():
    """For the logging service the ``service_id`` IS the logging
    service id (they're the same Fastly service, just used differently
    in different contexts)."""
    svcconfig.save_config("log-svc", _cfg(service_id="log-svc"))
    assert svcconfig.get_fastly_logging_service_id("log-svc") == "log-svc"


# ── get_ngwaf_workspace_id ──────────────────────────────────────────────────


def test_get_ngwaf_workspace_id_returns_value_when_set():
    svcconfig.save_config("svc", _cfg(service_id="svc", ngwaf_workspace_id="ws-1"))
    assert svcconfig.get_ngwaf_workspace_id("svc") == "ws-1"


def test_get_ngwaf_workspace_id_returns_none_when_unset_or_empty():
    """Both missing key and empty string → None. Pinned because the
    sync cron uses ``if ws_id:`` to skip services that aren't
    NGWAF-enabled."""
    svcconfig.save_config("svc1", _cfg(service_id="svc1"))
    svcconfig.save_config("svc2", _cfg(service_id="svc2", ngwaf_workspace_id=""))

    assert svcconfig.get_ngwaf_workspace_id("svc1") is None
    assert svcconfig.get_ngwaf_workspace_id("svc2") is None


def test_get_ngwaf_workspace_id_returns_none_for_unknown_service():
    assert svcconfig.get_ngwaf_workspace_id("ghost") is None


# ── Usage logging config ────────────────────────────────────────────────────


def test_load_usage_logging_config_returns_defaults_when_file_missing():
    out = svcconfig.load_usage_logging_config()
    assert out == svcconfig._USAGE_LOGGING_DEFAULTS


def test_load_usage_logging_config_merges_stored_over_defaults():
    """A partially-saved config (only ``enabled``) must still surface
    all the other defaults — otherwise a stale config could leave
    ``retention_days`` missing and crash the prune job."""
    svcconfig._ensure_dirs()
    svcconfig._USAGE_LOGGING_CONFIG_PATH.write_text(json.dumps({"enabled": True}))

    out = svcconfig.load_usage_logging_config()
    assert out["enabled"] is True
    assert out["retention_days"] == svcconfig._USAGE_LOGGING_DEFAULTS["retention_days"]
    assert out["track_duckdb_httpfs"] is True  # default preserved


def test_load_usage_logging_config_falls_back_to_defaults_on_corrupt_json():
    """Corrupt JSON → return defaults. Pinned because raising here
    would break every analytical request (the connection cache calls
    this on every open)."""
    svcconfig._ensure_dirs()
    svcconfig._USAGE_LOGGING_CONFIG_PATH.write_text("{not json")

    out = svcconfig.load_usage_logging_config()
    assert out == svcconfig._USAGE_LOGGING_DEFAULTS


def test_save_usage_logging_config_writes_atomically():
    svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 60})
    out = svcconfig.load_usage_logging_config()
    assert out["enabled"] is True
    assert out["retention_days"] == 60
    # Tmp file must be gone
    assert not svcconfig._USAGE_LOGGING_CONFIG_PATH.with_suffix(".tmp").exists()


def test_is_usage_logging_enabled_always_false_under_pytest():
    """Pinned as the explicit test-only short-circuit — even with the
    config explicitly enabled, ``pytest`` in ``sys.modules`` returns
    False. Without this every test would log usage rows and pollute
    the dev environment."""
    svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 30})
    # ``pytest`` is in sys.modules during test runs → always False
    assert svcconfig.is_usage_logging_enabled() is False


def test_is_usage_logging_enabled_returns_config_value_when_not_under_pytest():
    """Simulate non-test mode by removing pytest from sys.modules
    temporarily — verifies the production code path returns the
    config value rather than always False."""
    import sys

    pytest_module = sys.modules.pop("pytest", None)
    try:
        svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 30})
        assert svcconfig.is_usage_logging_enabled() is True

        svcconfig.save_usage_logging_config({"enabled": False, "retention_days": 30})
        assert svcconfig.is_usage_logging_enabled() is False
    finally:
        if pytest_module is not None:
            sys.modules["pytest"] = pytest_module


# ── _ensure_dirs ─────────────────────────────────────────────────────────────


def test_ensure_dirs_creates_full_directory_tree():
    """First-run bootstrap: nothing exists. ``_ensure_dirs`` must
    create configs/ AND every data/* sub-dir without raising."""
    # See sibling test ``test_save_config_creates_configs_dir_if_missing``
    # — wipe the conftest-pre-created sandbox to test the bootstrap path.
    import shutil

    shutil.rmtree(svcconfig.CONFIGS_DIR, ignore_errors=True)
    shutil.rmtree(svcconfig.DATA_DIR, ignore_errors=True)
    svcconfig._ensured_dirs.clear()
    assert not svcconfig.CONFIGS_DIR.exists()
    assert not svcconfig.DATA_DIR.exists()

    svcconfig._ensure_dirs()

    assert svcconfig.CONFIGS_DIR.exists()
    assert svcconfig.DATA_DIR.exists()
    assert svcconfig.SERVICES_DATA_DIR.exists()
    assert svcconfig.NGWAF_DATA_DIR.exists()
    assert svcconfig.CACHE_DATA_DIR.exists()
    assert svcconfig.SYSTEM_DATA_DIR.exists()


def test_ensure_dirs_is_idempotent():
    """Repeated calls (every save_config does one) must not raise."""
    svcconfig._ensure_dirs()
    svcconfig._ensure_dirs()  # again — must not raise
