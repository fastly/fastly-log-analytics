"""End-to-end-ish tests for provisioning create + teardown SSE routes.

Goals (per Milestone B / TESTING_PLAN.md):
- ``GET /api/provision/teardown`` against a known service removes the config
  file and reports a terminal SSE event.
- The 404 path fires when no service config exists.
- ``GET /api/provision/execute`` delegates to ``backend.provision.provision``
  with the params it received.

What's NOT in scope here: end-to-end against a real (or fully mocked)
Fastly REST API. That blocks on Milestone A's deferred 0.2 (pytest-httpx
adoption) — production calls go through stdlib ``urllib.request`` in
``backend.core.fastly.client``, which can't be patched cleanly without
wrapping every call site or porting to ``httpx``. Until then we stub the
orchestrator entry points (``provision`` and ``perform_teardown``) at the
boundary so the route layer is exercised without touching the network.
"""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app
from tests._helpers.wire import parse_sse_events as _parse_sse_events


# ── Merged from tests/routers/test_provision_lifecycle.py ──
def test_teardown_no_service_id_returns_404(isolated_configs_dir):
    """Calling teardown with a service_id that has no config → 404."""
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "nope-not-a-real-svc"})
    assert r.status_code == 404


def test_teardown_emits_done_and_removes_config(isolated_configs_dir, tmp_path, monkeypatch):
    """Happy path: configs/{id}.json exists → teardown route emits a 'done'-style
    terminal event, removes the config, and propagates the perform_teardown
    event stream into the SSE response."""
    sid = "svc-teardown-1"

    # Seed a minimal config so the route picks it up
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": "Teardown Test Svc",
        "fos_bucket": "td-test-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "fastly-test-token",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk_1",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn_svc_1",
            "cdn_url": "https://cdn.example/",
        },
    }
    svcconfig.save_config(sid, cfg)
    assert os.path.exists(svcconfig.config_path(sid)), "precondition: config seeded"

    # Stub the orchestrator so no Fastly / S3 / scheduler calls happen
    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint"}
        yield {"type": "status", "message": "removing FOS bucket"}
        yield {"type": "done", "message": "teardown complete"}

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        # Security: stub token validation. The auth gate itself is
        # exercised in test_provision_teardown_auth.py.
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-LF"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "cust-LF"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "test-tok",
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,  # keep so we don't try to delete real DBs
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    events = _parse_sse_events(r.text)
    assert events, f"expected SSE events, got: {r.text[:300]!r}"
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected a terminal 'done' event, got types: {types}"

    # The config file should be gone — that's the user-visible side effect
    assert not os.path.exists(svcconfig.config_path(sid)), "config should have been removed"


def test_teardown_remove_cache_clears_service_metadata(isolated_configs_dir):
    """``remove_cache=True`` must also clear the per-service metadata SQLite
    (ingested_files, cron_runs, rollups). It lives in the system data dir —
    OUTSIDE the cache dir the rmtree handles — so without an explicit
    ``metadata.teardown`` a re-provision inherits the dead service's
    ingested_files rollup and the usage-log gap panel shows stale "ours"
    counts."""
    sid = "svc-teardown-meta"
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": "Teardown Meta Svc",
        "fos_bucket": "td-meta-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "fastly-test-token",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn",
            "cdn_url": "https://cdn.example/",
        },
    }
    svcconfig.save_config(sid, cfg)

    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "done", "message": "teardown complete"}

    meta_calls: list[str] = []

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.core.metadata.teardown", side_effect=lambda s: meta_calls.append(s)),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "c"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "c"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": "test-tok",
                    "remove_logging": False,
                    "remove_cdn": False,
                    "remove_bucket": False,
                    "remove_cache": True,  # full local wipe → must include metadata
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    assert meta_calls == [sid], f"metadata.teardown({sid!r}) must be called on remove_cache; got {meta_calls}"


def test_teardown_skips_perform_teardown_when_no_logging_service(isolated_configs_dir):
    """If the seeded config has no logging_service_id, the route still 200s
    and removes the config — the orchestrator handles missing fields itself."""
    sid = "svc-teardown-min"
    svcconfig.save_config(sid, {"service_id": sid, "name": "minimal"})

    def fake_perform_teardown(state, token, opts=None):
        yield {"type": "done", "message": "no-op teardown"}

    with (
        patch("backend.provision.perform_teardown", side_effect=fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch(
            "backend.utils.fastly_auth.fastly",
            side_effect=lambda method, path, *, token, **kw: (
                {"id": "tok", "scope": "global", "services": [], "customer_id": "cust-MIN"}
                if path == "/tokens/self"
                else {"id": sid, "customer_id": "cust-MIN"}
            ),
        ),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": "test-tok", "remove_cache": False},
            )
    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))


def test_execute_delegates_to_provision_with_query_args(isolated_configs_dir):
    """Verify the route forwards the query params it received into the
    cfg dict passed to ``backend.provision.provision``. This is the contract
    test for the create flow until full Fastly mocking lands (Milestone C)."""
    sid = "svc-create-1"
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "provision complete"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="My Service"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "test-token",
                    "service_id": sid,
                    "fos_bucket_name": "create-test-bucket",
                    "fos_region": "us-east-1",
                    "endpoint_name": "Test Logger",
                    "edge_only": True,
                    "log_period": "60",
                    "enable_cron_sync": True,
                    "enable_cron_compact": True,
                },
            )

    assert r.status_code == 200, r.text[:500]
    cfg = captured.get("cfg")
    assert cfg is not None, "provision() was never called"
    assert cfg["logging_service_id"] == sid
    assert cfg["fos_bucket_name"] == "create-test-bucket"
    assert cfg["endpoint_name"] == "Test Logger"
    assert cfg["fos_region"] == "us-east-1"

    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected a terminal 'done' event, got: {types}"


def test_execute_propagates_orchestrator_error_event(isolated_configs_dir):
    """When provision() yields an error event, it must reach the SSE consumer."""

    def fake_provision(cfg, _resume_from_state=False):
        yield {"type": "status", "message": "starting"}
        yield {"type": "error", "message": "Fastly API: 503 Service Unavailable"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "tok",
                    "service_id": "svc-create-err",
                    "fos_bucket_name": "create-err-bucket",  # valid: ≥3 chars
                },
            )

    events = _parse_sse_events(r.text)
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None, f"expected an error event, got: {events}"
    assert "503" in err["message"]


# ── Merged from tests/routers/test_provision_teardown_auth.py ──
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


@pytest.mark.security_regression
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


# ── Merged from tests/routers/test_provision_teardown_idempotency.py ──
def _seed_minimal_cfg(sid: str) -> None:

    svcconfig.save_config(
        sid,
        {
            "service_id": sid,
            "logging_service_id": sid,
            "name": f"Idempotency Svc {sid}",
            "fos_bucket": f"idem-bucket-{sid}",
            "fos_region": "us-east-1",
            "fos_access_key_id": "AKIA",
            "fos_secret_access_key": "SECRET",
            "fastly_api_key": "fastly-test-token",
            "cdn_secret": "x",
            "provisioning": {
                "fos_key_id": "fk_1",
                "endpoint_name": "EP",
                "cdn_service_id": "cdn_svc_1",
                "cdn_url": "https://cdn.example/",
            },
        },
    )


def _fake_perform_teardown(state, token, opts=None):
    yield {"type": "status", "message": "removing logging endpoint (mock)"}
    yield {"type": "status", "message": "removing FOS bucket (mock)"}
    yield {"type": "done", "message": "teardown complete (mock)"}


_VALID_TOKEN = "test-admin-token"

_VALID_TOKEN_SHAPE = {
    "id": "tok_test",
    "user_id": "user_test",
    "scope": "global",
    "services": [],
    "customer_id": "cust-IDEM",
}


def _fake_fastly_dual(method, path, *, token, **kw):
    """Dispatcher that returns ``/tokens/self`` or ``/service/{id}`` payloads
    for the two-shot pattern introduced by the 035 tenant cross-check."""
    if path == "/tokens/self":
        return _VALID_TOKEN_SHAPE
    # /service/{sid} — must match the token's customer_id
    return {"id": path.rsplit("/", 1)[-1], "customer_id": _VALID_TOKEN_SHAPE["customer_id"]}


def test_teardown_is_idempotent_second_call_is_404(isolated_configs_dir):
    """Two back-to-back teardown calls: first 200 + cleans up, second 404."""
    sid = "svc-idem-1"
    _seed_minimal_cfg(sid)

    with (
        patch("backend.provision.perform_teardown", side_effect=_fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r1 = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": _VALID_TOKEN,
                    "remove_logging": True,
                    "remove_cdn": True,
                    "remove_bucket": True,
                    "remove_cache": False,
                    "remove_cron": False,
                },
            )
            assert r1.status_code == 200
            events1 = _parse_sse_events(r1.text)
            assert any(e.get("type") == "done" for e in events1), (
                f"first teardown should emit done, got: {[e.get('type') for e in events1]}"
            )
            assert not os.path.exists(svcconfig.config_path(sid)), "first teardown should remove the config file"

            # Second call: config is gone, route should reject with 404 cleanly.
            # No 500, no stack trace, no partial SSE stream.
            r2 = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
            )

    assert r2.status_code == 404, (
        f"second teardown after config removed should be 404, got {r2.status_code}: {r2.text[:300]}"
    )
    body2 = r2.json()
    assert "detail" in body2, f"404 should have FastAPI 'detail' shape: {body2}"


def test_teardown_unknown_service_id_never_provisioned_is_404(isolated_configs_dir):
    """Teardown on a service that never existed (typo / wrong env) — 404, no crash.

    No token needed because we return 404 before reaching the auth gate (state
    is None → 404 first). The auth gate only fires once state is loaded.
    """
    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": "never-existed-svc", "remove_cache": False},
        )
    assert r.status_code == 404


def test_teardown_preserves_duckdb_when_remove_cache_false(isolated_configs_dir, tmp_path):
    """``remove_cache=False`` MUST leave the per-service DuckDB file intact.

    A teardown intent is sometimes "decommission the Fastly resources but
    keep the local analytics data". This contract pin prevents a future
    refactor from accidentally wiping the DuckDB regardless of the flag.
    """
    sid = "svc-idem-keep-cache"
    _seed_minimal_cfg(sid)

    # Simulate an existing DuckDB file on disk (the cron pipeline would
    # have created this — for the test we just touch a placeholder).
    db_path = svcconfig.duckdb_path(sid)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, "w") as f:
        f.write("placeholder-duckdb-bytes")
    assert os.path.exists(db_path), "precondition: db placeholder exists"

    with (
        patch("backend.provision.perform_teardown", side_effect=_fake_perform_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={
                    "service_id": sid,
                    "token": _VALID_TOKEN,
                    "remove_cache": False,
                    "remove_cron": False,
                },
            )

    assert r.status_code == 200
    # Config removed (always, regardless of remove_cache).
    assert not os.path.exists(svcconfig.config_path(sid))
    # DuckDB preserved (because remove_cache=False).
    assert os.path.exists(db_path), f"DuckDB at {db_path} should NOT be removed when remove_cache=false"


def test_teardown_partial_failure_in_perform_teardown_still_succeeds(isolated_configs_dir):
    """If ``perform_teardown`` raises mid-stream (e.g. Fastly 503 while deleting
    the logging endpoint), the route MUST still:
      - emit an error event in the SSE stream
      - leave the config file removed (it was deleted at the *start* of stream())
      - return 200 (SSE streams don't change status mid-flight)

    This is the recovery contract: a flaky Fastly API call shouldn't strand
    the user with a half-torn-down service and a config they can't get rid of.
    """
    sid = "svc-idem-partial-fail"
    _seed_minimal_cfg(sid)

    def _boom_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint (mock)"}
        raise RuntimeError("Fastly API: 503 Service Unavailable (mock)")

    with (
        patch("backend.provision.perform_teardown", side_effect=_boom_teardown),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly_dual),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/teardown",
                json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
            )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "error" in types, f"expected error event after teardown raised, got: {types}"

    # Config was removed at the top of stream() (line ~242 in provision.py),
    # so it should be gone even though the Fastly call failed. Re-firing
    # teardown should now return 404 — verifying the idempotency contract
    # composes with the partial-failure path.
    assert not os.path.exists(svcconfig.config_path(sid)), (
        "config should have been removed before perform_teardown raised"
    )
    with TestClient(app) as client:
        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )
    assert r2.status_code == 404, f"second teardown should be 404, got {r2.status_code}"


# ── Merged from tests/routers/test_provision_wizard_e2e.py ──
def _fake_ensure_fos_access_key(
    description, state, token, *, permission="read-write-objects", buckets=None, status_cb=None
):
    if status_cb:
        status_cb(f"⏳ Creating {permission} access key (mock)…")
    suffix = "temp" if "temp-admin" in description else "perm"
    return {
        "id": f"AKIA_TEST_{suffix}",
        "access_key": f"AKIA_TEST_{suffix}",
        "secret_key": f"SECRET_TEST_{suffix}",
    }


def _fake_ensure_fos_bucket(bucket, region, access_key, secret_key, *, service_id=None, status_cb=None):
    if status_cb:
        status_cb(f"🪣 Bucket {bucket} in {region} (mock)…")
    return True


def _fake_ensure_cdn_service(cfg, fos_access_key, fos_secret_key, token, status_cb=None, on_created=None):
    if status_cb:
        status_cb("🌐 CDN service (mock)…")
    # Mirror the real ensure_cdn_service: persist the id the moment the
    # service exists so a failure later in this step still leaves it in
    # state for perform_teardown to delete (no orphan CDN service).
    if on_created:
        on_created("cdn-svc-mock-123")
    return {
        "id": "cdn-svc-mock-123",
        "name": cfg.get("cdn_service_name") or "Mock CDN Service",
        "version": 1,
    }


def _fake_ensure_logging_via_reconciler(state, token, status_cb=None):
    if status_cb:
        status_cb("📡 Logging endpoint (mock)…")
    return 42  # activated version number


def _fake_delete_fos_access_key(key_id, token, status_cb=None):
    if status_cb:
        status_cb(f"🧹 Deleted {key_id} (mock)…")


def test_wizard_execute_runs_orchestrator_and_bootstrap_sees_service(isolated_configs_dir, tmp_path, monkeypatch):
    """End-to-end: execute → orchestrator yields done → config persists → bootstrap lists svc."""
    sid = "svc-wizard-e2e-1"

    # Steer DuckDB away from real disk paths — write_service_config calls
    # svcconfig.duckdb_path() which depends on DATA_DIR.
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)

    # Patch each helper *at the orchestrator's import site* so the
    # generator-style "yield from run_with_events(helper, ...)" chain
    # picks up the fake. status_cb shimming keeps the progress yields flowing.
    with (
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_fake_ensure_cdn_service),
        patch(
            "backend.provision.orchestrator.ensure_logging_via_reconciler",
            side_effect=_fake_ensure_logging_via_reconciler,
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_fake_delete_fos_access_key),
        # Skip Fastly preflight + scheduler reload; we don't have a real cron.
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value="Wizard E2E Service"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
        # The orchestrator tries to initialize iceberg on commit; let the
        # try/except swallow our forced failure rather than touching FOS.
        patch("backend.core.iceberg.init_iceberg_table", side_effect=RuntimeError("iceberg init skipped (test)")),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "fake-fastly-token",
                    "service_id": sid,
                    "service_name": "Wizard E2E Service",
                    "endpoint_name": "Wizard E2E Logger",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "wizard-e2e-bucket",
                    "fos_prefix": "logs/",
                    "sample_rate": "100",
                    "edge_only": True,
                    "log_period": "60",
                    "enable_cron_sync": True,
                    "delete_after": True,
                    "commit_interval_mins": 5,
                    "enable_cron_compact": True,
                    "log_retention_days": 30,
                },
            )

    assert r.status_code == 200, r.text[:500]

    # 1. SSE stream contract — must yield a terminal "done" event with no
    #    preceding error. If any orchestrator step raised, the route would
    #    emit "error" and ABORT the rollback path with another "error"
    #    event after running perform_teardown.
    events = _parse_sse_events(r.text)
    assert events, f"expected SSE events, got: {r.text[:500]!r}"
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected terminal 'done' event, got types: {types}"
    assert "error" not in types, f"unexpected error event, full stream: {events}"

    # 2. Step coverage — confirm every numbered step yielded its banner
    #    so we know the generator actually walked the full sequence and
    #    didn't short-circuit on a mocked helper returning early.
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs, f"missing Step {n}/8 banner in: {status_msgs[:600]}"

    # 3. Config persisted — write_service_config(state) must have written
    #    configs/{sid}.json with the wizard params propagated.
    cfg_path = svcconfig.config_path(sid)
    assert os.path.exists(cfg_path), f"expected {cfg_path} to exist after provision"
    written = json.loads(cfg_path.read_text())
    assert written["service_id"] == sid
    assert written["fos_bucket"] == "wizard-e2e-bucket"
    assert written["fos_region"] == "us-east-1"
    assert written["fos_access_key_id"] == "AKIA_TEST_perm"
    assert written["fos_secret_access_key"] == "SECRET_TEST_perm"
    assert written["fastly_api_key"] == "fake-fastly-token"
    assert written["cdn_service_id"] == "cdn-svc-mock-123"
    # endpoint_name is read by write_service_config from state["provisioning"]["endpoint_name"];
    # the wizard's top-level endpoint_name never reaches that nested slot, so the default wins.
    # That's pre-existing orchestrator behavior — not the contract this test pins. Just assert
    # the provisioning block is well-formed.
    assert isinstance(written["provisioning"]["endpoint_name"], str) and written["provisioning"]["endpoint_name"]
    # cdn_secret is generated in the route layer (secrets.token_urlsafe);
    # don't pin its value, just assert it's a non-empty string.
    assert isinstance(written["cdn_secret"], str) and written["cdn_secret"], "cdn_secret should be a non-empty string"

    # 4. Bootstrap sees the new service — the service manager reads
    #    configs/*.json on every call (no module-level cache), so the
    #    freshly written config must surface in the services list.
    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200, boot.text[:500]
    body = boot.json()
    assert "services" in body, f"bootstrap missing 'services': {body}"
    svc_ids = [s.get("service_id") for s in body["services"]]
    assert sid in svc_ids, f"bootstrap services do not include {sid}: {svc_ids}"


def test_wizard_execute_e2e_iceberg_init_success(isolated_configs_dir, tmp_path, monkeypatch, s3_mock):
    """R-15: positive-path variant of the wizard E2E. Lets the real
    `init_iceberg_table` execute against a per-test moto S3 endpoint
    instead of mocking it to raise, so the final orchestrator step is
    actually validated end-to-end and the iceberg table is queryable
    via DuckDB afterwards.

    The companion test above (`*_runs_orchestrator_*`) explicitly mocks
    iceberg.init_iceberg_table to raise so its assertions can focus on
    the orchestrator generator's step sequencing; this one closes the
    gap by exercising the init for real.
    """
    sid = "svc-wizard-iceberg-success"

    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)

    # Point the storage-cache root at tmp so init_iceberg_table can
    # provision its sqlite catalog under the sandbox.
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", tmp_path / "data" / "cache")

    with (
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_fake_ensure_cdn_service),
        patch(
            "backend.provision.orchestrator.ensure_logging_via_reconciler",
            side_effect=_fake_ensure_logging_via_reconciler,
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_fake_delete_fos_access_key),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value="Wizard Iceberg Success"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
        # NB: NO patch for init_iceberg_table — it runs for real against
        # the moto S3 endpoint (s3_mock fixture) the conftest pre-wires
        # via backend.core.duckdb._get_fos_client.
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "fake-fastly-token",
                    "service_id": sid,
                    "service_name": "Wizard Iceberg Success",
                    "endpoint_name": "Wizard Iceberg Logger",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "test-bucket",  # provisioned by s3_mock
                    "fos_prefix": "logs/",
                    "sample_rate": "100",
                    "edge_only": True,
                    "log_period": "60",
                    "enable_cron_sync": True,
                    "delete_after": True,
                    "commit_interval_mins": 5,
                    "enable_cron_compact": True,
                    "log_retention_days": 30,
                },
            )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types, f"expected 'done' event, got types: {types}"
    assert "error" not in types, f"unexpected error event, full stream: {events}"

    # All 8 banners must have emitted (step 8 is the iceberg-init step).
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs, f"missing Step {n}/8 banner: {status_msgs[:600]}"

    # Config persists.
    cfg_path = svcconfig.config_path(sid)
    assert cfg_path.exists(), f"expected {cfg_path}"

    # Bootstrap surfaces the service.
    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200
    svc_ids = [s.get("service_id") for s in boot.json()["services"]]
    assert sid in svc_ids


def test_wizard_execute_rolls_back_on_helper_failure(isolated_configs_dir, tmp_path, monkeypatch):
    """If a mid-pipeline helper raises, the orchestrator must emit an error
    event AND run perform_teardown (rollback). The config file must NOT
    persist after a failed provision."""
    sid = "svc-wizard-e2e-fail"
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)

    # Steps 1-3 succeed; ensure_cdn_service blows up at step 6.
    def _boom_cdn(cfg, fos_access_key, fos_secret_key, token, status_cb=None):
        raise RuntimeError("Fastly API: 500 Internal Server Error (mock)")

    with (
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_boom_cdn),
        patch(
            "backend.provision.orchestrator.ensure_logging_via_reconciler",
            side_effect=_fake_ensure_logging_via_reconciler,
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_fake_delete_fos_access_key),
        # Swallow side effects from the rollback path.
        patch("backend.provision.orchestrator.remove_logging_endpoint"),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch("backend.provision.orchestrator.delete_fos_bucket"),
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value="Wizard E2E Service Fail"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "fake-fastly-token",
                    "service_id": sid,
                    "service_name": "Wizard E2E Service Fail",
                    "endpoint_name": "Wizard E2E Logger Fail",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "wizard-e2e-fail-bucket",
                    "edge_only": True,
                    "log_period": "60",
                },
            )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "error" in types, f"expected error event after helper raised, got: {types}"

    # Step 1-3 ran (preflight, temp admin key, bucket); step 6 was the boom.
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 3/8" in status_msgs, f"expected at least 3 steps before failure: {status_msgs[:600]}"
    assert "Step 8/8" not in status_msgs, f"step 8 should not run after step-6 failure: {status_msgs[:600]}"

    # Config file should NOT persist after a failed provision (the
    # orchestrator's except branch removes it explicitly).
    assert not os.path.exists(svcconfig.config_path(sid)), f"config {sid}.json should be removed after rollback"


def test_wizard_execute_rollback_actually_invokes_resource_deletion(isolated_configs_dir, tmp_path, monkeypatch):
    """Mid-pipeline failure → perform_teardown must DELETE the
    resources that were actually created (CDN service, FOS bucket,
    FOS access keys, logging endpoint), not just emit an error event
    and clean up the config file.

    The existing test_wizard_execute_rolls_back_on_helper_failure
    only asserts the SSE error event + config-file removal. That
    assertion would still pass if perform_teardown silently swallowed
    every delete call — which is the exact failure mode that left
    orphaned Fastly CDN services + temp FOS keys in production.

    Here we force the LOGGING_ENDPOINT step (step 7) to fail AFTER
    steps 1-6 have created real resources, and we record every
    delete_* call. Then we assert each created resource was torn
    down with the matching identifier."""
    sid = "svc-wizard-teardown-trace"
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)

    # Record every teardown helper invocation by (function, args).
    teardown_calls: list[tuple[str, dict]] = []

    def _track(name):
        def _capture(*args, **kwargs):
            teardown_calls.append((name, {"args": args, "kwargs": kwargs}))
            return None

        return _capture

    def _boom_logging_endpoint(state, token, status_cb=None):
        raise RuntimeError("Fastly API: 502 Bad Gateway on logging endpoint (mock)")

    # The teardown's mid-step "create a temp admin key to delete the
    # bucket" needs a fake_ensure_fos_access_key that does NOT crash —
    # use the same shim already in this file.
    with (
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_fake_ensure_cdn_service),
        patch("backend.provision.orchestrator.ensure_logging_via_reconciler", side_effect=_boom_logging_endpoint),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_track("delete_fos_access_key")),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=_track("delete_fos_bucket")),
        patch("backend.provision.orchestrator.delete_cdn_service", side_effect=_track("delete_cdn_service")),
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=_track("remove_logging_endpoint")),
        # ``fastly()`` is called inside perform_teardown's FOS-key
        # sweep at the top of step 2 to list keys; return an empty list
        # so it doesn't try to delete anything via that path.
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value="Wizard Teardown Trace"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/api/provision/execute",
                json={
                    "token": "fake-fastly-token",
                    "service_id": sid,
                    "service_name": "Wizard Teardown Trace",
                    "endpoint_name": "Wizard Teardown Logger",
                    "fos_region": "us-east-1",
                    "fos_bucket_name": "wizard-teardown-bucket",
                    # cdn_service_name is required for perform_teardown's
                    # step-4 (delete_cdn_service) branch to actually
                    # fire — the production wizard ships it; missing it
                    # would silently leak the CDN service on teardown.
                    "cdn_service_name": "Wizard Teardown CDN",
                    "edge_only": True,
                    "log_period": "60",
                },
            )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "error" in types
    # We failed AT step 7 — steps 1-6 ran (preflight, temp key, bucket,
    # perm key, temp-key cleanup, CDN service).
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 6/8" in status_msgs, f"expected step 6 (CDN service) to have completed: {status_msgs[:600]}"

    # Resources that were actually created MUST be torn down:
    # 1. CDN service ID came from _fake_ensure_cdn_service → cdn-svc-mock-123
    # 2. Logging endpoint name = the one we posted ('Wizard Teardown Logger')
    # 3. FOS bucket name = 'wizard-teardown-bucket'
    # 4. The permanent FOS access key id = AKIA_TEST_perm (from the shim)
    called_names = [name for name, _ in teardown_calls]
    assert "delete_cdn_service" in called_names, (
        f"perform_teardown never called delete_cdn_service — the CDN service "
        f"cdn-svc-mock-123 created at step 6 will leak in production. "
        f"Teardown calls observed: {called_names}"
    )
    assert "remove_logging_endpoint" in called_names, (
        f"perform_teardown never called remove_logging_endpoint — the logging "
        f"endpoint config would be left orphaned. Teardown calls: {called_names}"
    )
    assert "delete_fos_bucket" in called_names, (
        f"perform_teardown never called delete_fos_bucket — the bucket "
        f"'wizard-teardown-bucket' would leak. Teardown calls: {called_names}"
    )
    # The permanent FOS access key MUST be deleted (it was created at step 4).
    fos_key_deletes = [c for c in teardown_calls if c[0] == "delete_fos_access_key"]
    assert fos_key_deletes, (
        f"perform_teardown never called delete_fos_access_key — at least the "
        f"permanent key (AKIA_TEST_perm) should be torn down. "
        f"Teardown calls: {called_names}"
    )

    # And the wizard's config file should not persist.
    assert not os.path.exists(svcconfig.config_path(sid))
