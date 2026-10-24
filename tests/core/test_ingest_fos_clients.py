"""Credential-cache invalidation for the boto3 FOS client.

A teardown→re-provision of the same service id can rotate FOS or CDN
credentials. The process-wide ``_fos_client_cache`` must not keep serving
stale auth — otherwise every ingest GET/HEAD + parquet read can 401 until the
backend restarts (the prod incident this guards against). Two mechanisms are pinned:

  * the cache is keyed on ``(name, access_key_id, cdn_url, cdn_secret)`` so
    rotated credentials MISS the cache and rebuild with fresh auth
    (defense-in-depth that auto-heals a rotation a caller forgets to invalidate), and
  * ``clear_fos_client`` drops every entry for a service name — the explicit
    invalidation called from the provision/teardown/ingest router seams.

Mirrors ``tests/utils/test_telemetry_proxy_phase4.py`` in exercising the real
``_get_fos_client`` (boto3 client construction is offline; the telemetry proxy
start is idempotent).
"""

from __future__ import annotations

from backend.core import duckdb as _ddb


def _src(name: str, akid: str, *, cdn_secret: str = "") -> dict:
    return {
        "name": name,
        "service_id": name,
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "https://us-east-1.object.fastlystorage.app",
        "access_key_id": akid,
        "secret_access_key": "secret",
        "cdn_url": "https://cdn.example.test",
        "cdn_secret": cdn_secret,
    }


def _reset_cache() -> None:
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()


def test_same_creds_reuse_cached_client():
    _reset_cache()
    try:
        c1 = _ddb._get_fos_client(_src("svc-a", "KEY1"))
        c2 = _ddb._get_fos_client(_src("svc-a", "KEY1"))
        assert c1 is c2, "same credentials must reuse the cached client"
    finally:
        _reset_cache()


def test_rotated_key_misses_cache():
    """A new access key for the same service id is a cache MISS — the client is
    rebuilt rather than serving the stale (now-deleted) key."""
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-rot", "OLDKEY"))
        c_new = _ddb._get_fos_client(_src("svc-rot", "NEWKEY"))
        assert c_old is not c_new, "rotated key must not return the stale client"
        keys = {k[:2] for k in _ddb._fos_client_cache if k[0] == "svc-rot"}
        assert ("svc-rot", "OLDKEY") in keys and ("svc-rot", "NEWKEY") in keys
    finally:
        _reset_cache()


def test_rotated_cdn_secret_misses_cache():
    """A changed CDN secret must rebuild the hook that injects x-fastly-key."""
    _reset_cache()
    try:
        c_old = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="OLDSECRET"))
        c_new = _ddb._get_fos_client(_src("svc-cdn-rot", "KEY1", cdn_secret="NEWSECRET"))
        assert c_old is not c_new, "rotated CDN secret must not reuse the stale client hook"
    finally:
        _reset_cache()


def test_clear_fos_client_drops_all_entries_for_name():
    _reset_cache()
    try:
        _ddb._get_fos_client(_src("svc-clear", "OLDKEY"))
        _ddb._get_fos_client(_src("svc-clear", "NEWKEY"))
        _ddb._get_fos_client(_src("other", "K"))

        # accepts a bare service name
        _ddb.clear_fos_client("svc-clear")
        remaining = set(_ddb._fos_client_cache)
        assert not any(k[0] == "svc-clear" for k in remaining), "every svc-clear entry cleared"
        assert any(k[:2] == ("other", "K") for k in remaining), "other services untouched"

        # ...and a source dict
        _ddb.clear_fos_client({"name": "other"})
        assert not any(k[:2] == ("other", "K") for k in _ddb._fos_client_cache)
    finally:
        _reset_cache()


def test_clear_fos_client_is_noop_when_absent():
    _reset_cache()
    # Must not raise on an unknown service / empty cache.
    _ddb.clear_fos_client("never-built")
    _ddb.clear_fos_client({"name": "never-built"})
