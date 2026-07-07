"""Security /: teardown endpoint auth-gate regression tests.

Pins the contract that any destructive teardown (logging / CDN / bucket) must
present a caller-supplied Fastly API token that /tokens/self confirms has the
``global`` scope. Cache-only teardown bypasses the gate because it never calls
Fastly. The endpoint must NEVER fall back to the server-stored
``fastly_api_key`` field for destructive ops — that was the unauthenticated
infrastructure-teardown attack vector.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app

# Destructive-teardown auth gate is a verified-fix surface: refactors
# must preserve the "no fallback to server-stored fastly_api_key" rule.
pytestmark = pytest.mark.security_regression


@pytest.fixture
def isolated_configs_dir(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", cfg_dir)
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    return cfg_dir


def _seed_cfg(sid: str) -> None:
    svcconfig.save_config(
        sid,
        {
            "service_id": sid,
            "logging_service_id": sid,
            "name": f"Auth Svc {sid}",
            "fos_bucket": f"auth-bucket-{sid}",
            "fos_region": "us-east-1",
            "fos_access_key_id": "AKIA",
            "fos_secret_access_key": "SECRET",
            "fastly_api_key": "server-stored-key-MUST-NOT-BE-USED",
            "cdn_secret": "x",
            "provisioning": {"fos_key_id": "fk", "cdn_service_id": "cdn_svc", "cdn_url": "https://x/"},
        },
    )


def test_destructive_teardown_without_token_rejected_401(isolated_configs_dir):
    """No token + destructive flags = 401, no fallback to server creds."""
    sid = "svc-auth-1"
    _seed_cfg(sid)

    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={
                "service_id": sid,
                "remove_logging": True,
                "remove_cdn": True,
                "remove_bucket": True,
                "remove_cache": False,
            },
        )

    assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text[:300]}"
    assert r.json()["detail"]["error"] == "token_required"


def test_destructive_teardown_with_read_only_scope_rejected(isolated_configs_dir):
    """``global:read`` does not grant destructive ops — must be rejected."""
    sid = "svc-auth-2"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        assert path == "/tokens/self"
        # Read-only token: has global:read but NOT plain global.
        return {"id": "tok_ro", "user_id": "u", "scope": "global:read", "services": []}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "ro-token",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


def test_destructive_teardown_with_purge_scope_rejected(isolated_configs_dir):
    """purge_select / purge_all also reject — only ``global`` qualifies."""
    sid = "svc-auth-3"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        return {"id": "tok_purge", "scope": "purge_select purge_all", "services": []}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "purge-token",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


def test_destructive_teardown_token_bound_to_wrong_service_rejected(isolated_configs_dir):
    """Token has 'global' but services=[other-id] — must reject (service mismatch)."""
    sid = "svc-auth-4"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        return {"id": "tok_bound", "scope": "global", "services": ["different-svc"]}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "bound-token",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "service_not_authorized"


def test_destructive_teardown_with_global_token_proceeds(isolated_configs_dir):
    """Valid 'global' token + matching/empty services → proceeds (200 SSE)."""
    sid = "svc-auth-5"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        if path == "/tokens/self":
            return {
                "id": "tok_ok",
                "user_id": "u",
                "scope": "global",
                "services": [],
                "customer_id": "cust-A",
            }
        # 035 tenant cross-check: /service/{sid} returns the owning customer.
        return {"id": sid, "customer_id": "cust-A"}

    def _fake_teardown(state, token, opts=None):
        yield {"type": "done", "message": "ok"}

    with (
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly),
        patch("backend.provision.perform_teardown", side_effect=_fake_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "valid-global-token",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 200, r.text[:500]


def test_destructive_teardown_service_in_bound_list_proceeds(isolated_configs_dir):
    """Token bound to [sid, other] — matches target → proceeds."""
    sid = "svc-auth-6"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        if path == "/tokens/self":
            return {
                "id": "tok_bound",
                "scope": "global",
                "services": [sid, "other-svc"],
                "customer_id": "cust-B",
            }
        return {"id": sid, "customer_id": "cust-B"}

    def _fake_teardown(state, token, opts=None):
        yield {"type": "done", "message": "ok"}

    with (
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly),
        patch("backend.provision.perform_teardown", side_effect=_fake_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "bound-valid",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 200


def test_cache_only_teardown_requires_auth_gate(isolated_configs_dir):
    """remove_logging=false + remove_cdn=false + remove_bucket=false = cache-only
    cleanup. Still requires a token to prevent unauthenticated destruction of local state.
    """
    sid = "svc-auth-7"
    _seed_cfg(sid)

    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={
                "service_id": sid,
                "remove_logging": False,
                "remove_cdn": False,
                "remove_bucket": False,
                "remove_cache": True,
                "remove_cron": False,
            },
        )

    # Cache-only teardown should trigger token_required 401 gate
    assert r.status_code == 401
    assert "token_required" in r.json()["detail"]["error"]


def test_destructive_teardown_fastly_unreachable_rejects(isolated_configs_dir):
    """If Fastly's /tokens/self errors out (network, 5xx), we must fail closed
    rather than fall back to server creds."""
    sid = "svc-auth-8"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        raise RuntimeError("Network error on GET /tokens/self: unreachable (mock)")

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "any",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "token_validation_failed"


def test_destructive_teardown_scope_as_list_accepted(isolated_configs_dir):
    """Some Fastly token responses return scope as a JSON list. The validator
    must normalize both forms; here we exercise the list-shape branch."""
    sid = "svc-auth-9"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        if path == "/tokens/self":
            return {
                "id": "tok_list",
                "scope": ["global", "purge_all"],
                "services": [],
                "customer_id": "cust-L",
            }
        return {"id": sid, "customer_id": "cust-L"}

    def _fake_teardown(state, token, opts=None):
        yield {"type": "done", "message": "ok"}

    with (
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly),
        patch("backend.provision.perform_teardown", side_effect=_fake_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "list-scope",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 200


def test_destructive_teardown_tenant_mismatch_rejected(isolated_configs_dir):
    """Token has 'global' scope, no services binding, BUT its customer_id
    differs from the target service's customer_id. This blocks the
    "use a global token from MY Fastly account against someone else's
    service" attack."""
    sid = "svc-auth-tenant-mismatch"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        if path == "/tokens/self":
            # Attacker's token under their own Fastly account.
            return {"id": "tok_attacker", "scope": "global", "services": [], "customer_id": "cust-ATTACKER"}
        # Victim's service under a different tenant.
        return {"id": sid, "customer_id": "cust-VICTIM"}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "cross-tenant-token",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                },
            )

    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "tenant_mismatch"


def test_destructive_teardown_get_method_rejected(isolated_configs_dir):
    """CSRF defense: teardown must not be triggerable via GET. A cross-site
    ``<img src="/api/provision/teardown?…">`` would fire on any visit to
    an attacker page; POST + Content-Type: application/json forces the
    browser to preflight and blocks silent cross-origin invocation."""
    sid = "svc-csrf-rejected"
    _seed_cfg(sid)

    with TestClient(app) as client:
        r = client.get(
            "/api/provision/teardown",
            params={
                "service_id": sid,
                "token": "anything",
                "remove_logging": "true",
            },
        )

    # FastAPI may surface "no matching route" as 404 rather than 405. Either
    # is acceptable; both mean the GET-CSRF vector is closed. What MUST NOT
    # happen is a 200 SSE stream.
    assert r.status_code in (404, 405), f"GET must be rejected; got {r.status_code}: {r.text[:300]}"


def test_destructive_teardown_text_plain_content_type_rejected(isolated_configs_dir):
    """Regression for audit finding 012: a malicious HTML form with
    ``enctype=text/plain`` can POST a JSON-shaped body without triggering a
    CORS preflight, bypassing the intended same-origin gate. The teardown
    handler must require ``Content-Type: application/json`` explicitly so
    the browser is forced to preflight."""
    sid = "svc-csrf-text-plain"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            content='{"service_id":"' + sid + '","remove_logging":true}',
            headers={"Content-Type": "text/plain"},
        )
    assert r.status_code == 415, f"text/plain must be rejected with 415; got {r.status_code}: {r.text[:300]}"
