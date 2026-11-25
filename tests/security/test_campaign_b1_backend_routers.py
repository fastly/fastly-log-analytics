"""Campaign B1 security regression tests for High-Scale Backend Routers."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import oauth as oidc
from backend.core import share_db
from backend.routers import alerts as alerts_mod
from backend.routers.share_admin import invites as invites_mod
from backend.routers.share_admin import sessions_and_tos as sessions_mod
from backend.routers.share_admin import tunnel_routes as tunnel_routes_mod
from backend.utils import tunnel, webhook_url

pytestmark = pytest.mark.no_postgres


def _make_memory_share_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    for stmt in share_db._SCHEMA:
        con.execute(stmt)
    con.commit()
    return con


# ── 1. gdpr_erase audit-log PII leak ─────────────────────────────────────────


def test_gdpr_erase_does_not_write_plaintext_email_to_audit_log() -> None:
    con = _make_memory_share_db()
    target_email = "pii.subject@example.com"
    share_db.create_remote_invite(
        name="Subject",
        email=target_email,
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svc-a"],
        con=con,
    )
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?, ?, ?, ?, ?)",
        ("2020-01-01T00:00:00Z", "LOGIN_SUCCESS", target_email, "198.51.100.1", "old"),
    )
    con.commit()

    out = share_db.gdpr_erase(target_email, reason="DSR-B1-001", con=con)
    assert out["deleted_invites"] == 1
    assert out["redacted_log_rows"] == 1

    erasure_rows = [r for r in share_db.get_share_audit_logs(con=con) if r["event_type"] == "GDPR_ERASURE"]
    assert len(erasure_rows) == 1
    row = erasure_rows[0]
    assert row["email"] is None
    assert target_email not in row["details"]
    details = json.loads(row["details"])
    assert "erased_email" not in details
    expected_hash = hashlib.sha256(target_email.encode("utf-8")).hexdigest()[:16]
    assert details["erased_email_hash"] == expected_hash
    assert details["reason"] == "DSR-B1-001"


# ── 2. OAuth PKCE flow-state cookie Secure flag behind TLS proxy ─────────────


def test_oauth_authorize_sets_secure_flow_state_cookie_behind_tls_proxy(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.remote_access import conftest as H

    con = _make_memory_share_db()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    tunnel.reset_for_tests()
    H.configure_registry(monkeypatch, tmp_path)
    oidc.set_test_transport(H.make_transport())

    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.include_router(invites_mod.share_oauth_router)

    # Case A: Internal HTTP request behind TLS proxy (public_url is https://)
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        r_pub = client.get(
            "/api/share/oauth/authorize",
            params={"provider": "google"},
            headers={"X-Proxied-By-Caddy": "true"},
            follow_redirects=False,
        )
    assert r_pub.status_code == 302
    cookies_pub = r_pub.headers.get_list("set-cookie")
    flow_cookie_pub = next(c for c in cookies_pub if c.startswith("oauth_flow_state="))
    assert "secure" in flow_cookie_pub.lower()
    assert "httponly" in flow_cookie_pub.lower()
    assert "samesite=lax" in flow_cookie_pub.lower()

    # Case B: Internal HTTP request with X-Forwarded-Proto: https even if public_url is http://
    monkeypatch.setattr(mgr, "public_url", lambda: "http://internal-backend:8000")
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        r_xfp = client.get(
            "/api/share/oauth/authorize",
            params={"provider": "google"},
            headers={"X-Forwarded-Proto": "https"},
            follow_redirects=False,
        )
    assert r_xfp.status_code == 302
    cookies_xfp = r_xfp.headers.get_list("set-cookie")
    flow_cookie_xfp = next(c for c in cookies_xfp if c.startswith("oauth_flow_state="))
    assert "secure" in flow_cookie_xfp.lower()
    assert "httponly" in flow_cookie_xfp.lower()
    assert "samesite=lax" in flow_cookie_xfp.lower()


# ── 3. share_claim IP whitelist + no token[:6] logging; share_login oracle ───


def test_share_claim_enforces_ip_whitelist_and_never_logs_raw_token_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = _make_memory_share_db()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    tunnel.reset_for_tests()
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.include_router(sessions_mod.share_auth_router)

    invite = share_db.create_remote_invite(
        name="Scoped Analyst",
        email="scoped@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist="10.10.0.0/16",
        service_ids=["svc-a"],
        con=con,
    )

    # Non-whitelisted IP must be rejected with 403 and must not receive invite PII
    blocked_token = share_db.create_claim_token(invite["id"], ttl_hours=1, con=con)
    with TestClient(app, client=("198.51.100.77", 50000)) as client:
        r_blocked = client.post(f"/api/share/claim/{blocked_token}")
    assert r_blocked.status_code == 403
    assert r_blocked.json()["detail"]["error"] == "ip_not_whitelisted"
    assert "scoped@example.com" not in r_blocked.text

    # Whitelisted IP succeeds
    allowed_token = share_db.create_claim_token(invite["id"], ttl_hours=1, con=con)
    with TestClient(app, client=("10.10.1.5", 50000)) as client:
        r_ok = client.post(f"/api/share/claim/{allowed_token}")
    assert r_ok.status_code == 200
    assert r_ok.json()["email"] == "scoped@example.com"

    # Invalid token attempt also must not leak raw token[:6] into audit logs
    bogus_token = "claim-invalid-" + "0" * 12
    with TestClient(app, client=("10.10.1.5", 50000)) as client:
        r_bad = client.post(f"/api/share/claim/{bogus_token}")
    assert r_bad.status_code == 404

    audit_rows = share_db.get_share_audit_logs(limit=50, con=con)
    all_details = "\n".join(r["details"] for r in audit_rows)
    for tok in (blocked_token, allowed_token, bogus_token):
        assert tok[:6] not in all_details, f"Raw token prefix leaked in audit logs: {tok[:6]}"
        expected_tag = hashlib.sha256(tok.encode("utf-8")).hexdigest()[:8]
        assert expected_tag in all_details


def test_share_login_normalizes_ip_whitelist_rejection_to_401_no_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    con = _make_memory_share_db()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    tunnel.reset_for_tests()
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.include_router(sessions_mod.share_auth_router)

    share_db.create_remote_invite(
        name="Locked Analyst",
        email="locked@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist="10.0.0.0/8",
        service_ids=["svc-a"],
        con=con,
    )

    with TestClient(app, client=("203.0.113.55", 50000)) as client:
        r_wrong_pw = client.post(
            "/api/share/login",
            json={"email": "locked@example.com", "passcode": "wrong-passcode-entirely-99"},
        )
        r_right_pw_bad_ip = client.post(
            "/api/share/login",
            json={"email": "locked@example.com", "passcode": "ocean-breeze-cabin-42"},
        )

    # Must be byte-identical status & error body so attacker cannot enumerate valid credentials
    assert r_wrong_pw.status_code == 401
    assert r_right_pw_bad_ip.status_code == 401
    assert r_right_pw_bad_ip.json() == r_wrong_pw.json() == {"detail": {"error": "invalid_credentials"}}

    # Admin audit log still records the specific reason for defenders
    audit_rows = share_db.get_share_audit_logs(limit=20, con=con)
    assert any(r["event_type"] == "LOGIN_FAIL" and "IP not in whitelist" in r["details"] for r in audit_rows)


# ── 4. mock_idp_router mount gating & redirect validation ────────────────────


def test_mock_idp_router_enforces_enabled_gate_and_rejects_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    app.include_router(tunnel_routes_mod.mock_idp_router)

    # Disabled by default -> 404 even if router is mounted directly
    monkeypatch.delenv("OAUTH_MOCK_IDP", raising=False)
    monkeypatch.delenv("FLA_PRODUCTION", raising=False)
    with TestClient(app) as client:
        r_off = client.get("/mock-idp/.well-known/openid-configuration")
    assert r_off.status_code == 404

    # Production with OAUTH_MOCK_IDP=1 -> refuses (RuntimeError)
    monkeypatch.setenv("OAUTH_MOCK_IDP", "1")
    monkeypatch.setenv("FLA_PRODUCTION", "1")
    with pytest.raises(RuntimeError, match="OAUTH_MOCK_IDP must not be enabled in production"):
        tunnel_routes_mod.mock_idp_enabled()
    with TestClient(app, raise_server_exceptions=False) as client:
        r_prod = client.get("/mock-idp/.well-known/openid-configuration")
    assert r_prod.status_code >= 400


@pytest.mark.parametrize(
    "bad_redirect",
    [
        "https://evil.example.com/steal",
        "http://attacker.tld/callback",
        "//evil.example.com/callback",
        "///evil.example.com/callback",
        "\\\\evil.example.com\\callback",
        "javascript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
        "http://127.0.0.1@evil.example.com/callback",
        "/callback\r\nSet-Cookie: evil=1",
        "https://127.0.0.1.evil.com/callback",
    ],
)
def test_mock_idp_authorize_rejects_open_redirects(monkeypatch: pytest.MonkeyPatch, bad_redirect: str) -> None:
    monkeypatch.setenv("OAUTH_MOCK_IDP", "1")
    monkeypatch.delenv("FLA_PRODUCTION", raising=False)
    monkeypatch.delenv("OAUTH_REDIRECT_BASE", raising=False)

    app = FastAPI()
    app.include_router(tunnel_routes_mod.mock_idp_router)

    with TestClient(app) as client:
        r = client.get(
            "/mock-idp/authorize",
            params={"redirect_uri": bad_redirect, "state": "st-1", "nonce": "n-1"},
            follow_redirects=False,
        )
    assert r.status_code == 400, f"{bad_redirect!r} -> {r.status_code} {r.headers.get('location')}"
    assert r.json()["detail"]["error"] == "invalid_redirect_uri"


@pytest.mark.parametrize(
    "good_redirect",
    [
        "/api/share/oauth/callback",
        "http://127.0.0.1:18004/api/share/oauth/callback",
        "http://localhost:3000/api/share/oauth/callback",
    ],
)
def test_mock_idp_authorize_allows_local_and_relative_redirects(
    monkeypatch: pytest.MonkeyPatch, good_redirect: str
) -> None:
    monkeypatch.setenv("OAUTH_MOCK_IDP", "1")
    monkeypatch.delenv("FLA_PRODUCTION", raising=False)

    app = FastAPI()
    app.include_router(tunnel_routes_mod.mock_idp_router)

    with TestClient(app) as client:
        r = client.get(
            "/mock-idp/authorize",
            params={"redirect_uri": good_redirect, "state": "st-ok", "nonce": "n-ok"},
            follow_redirects=False,
        )
    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith(good_redirect)
    qs = parse_qs(urlsplit(loc).query)
    assert qs["state"] == ["st-ok"]
    assert "code" in qs


# ── 5. webhook_url SSRF hardening + alerts.py deduplication ──────────────────


@pytest.mark.parametrize(
    "blocked_host",
    [
        "localhost",
        "localhost.",
        "foo.localhost",
        "sub.foo.localhost",
        "foo.localhost.",
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::7f00:1",
        "64:ff9b::8.8.8.8",
        "64:ff9b:1::a9fe:a9fe",
        "64:ff9b:1::1",
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",
        "::ffff:100.64.0.1",
        "::ffff:0:127.0.0.1",
        "::ffff:0:169.254.169.254",
        "::ffff:0:10.0.0.1",
        "::ffff:0:100.64.0.1",
        "::127.0.0.1",
        "::169.254.169.254",
        "::10.0.0.1",
        "::100.64.0.1",
        "2002:7f00:0001::1",
        "2002:a9fe:a9fe::1",
        "2002:0a00:0001::1",
        "2002:6440:0001::1",
        "2001:0000:4136:e378:8000:63bf:80ff:fffe",  # Teredo inverted 127.0.0.1
        "2001:0000:4136:e378:8000:63bf:5601:5601",  # Teredo inverted 169.254.169.254
        "100.64.0.1",
        "169.254.169.254",
    ],
)
def test_webhook_url_blocks_localhost_suffix_nat64_and_ipv6_transitions(blocked_host: str) -> None:
    url = f"https://[{blocked_host}]/hook" if ":" in blocked_host else f"https://{blocked_host}/hook"
    assert webhook_url.restricted_webhook_host(blocked_host) is True, blocked_host
    with pytest.raises(ValueError):
        webhook_url.validate_webhook_url_syntax(url)
    with pytest.raises(alerts_mod.HTTPException) as exc_info:
        alerts_mod.validate_webhook_url(url)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["error"] == "invalid_webhook_url"


@pytest.mark.parametrize(
    "allowed_url",
    [
        "https://hooks.slack.com/services/T00/B00/X00",
        "https://8.8.8.8/webhook",
        "https://[2606:4700:4700::1111]/webhook",
    ],
)
def test_webhook_url_allows_public_destinations(allowed_url: str) -> None:
    assert webhook_url.validate_webhook_url_syntax(allowed_url) == allowed_url
    assert alerts_mod.validate_webhook_url(allowed_url) == allowed_url


def test_alerts_router_delegates_webhook_validation_to_webhook_url_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def _spy(url: str) -> str:
        seen.append(url)
        raise ValueError("spy_rejected")

    monkeypatch.setattr(alerts_mod, "validate_webhook_url_syntax", _spy)
    with pytest.raises(alerts_mod.HTTPException) as exc_info:
        alerts_mod.validate_webhook_url("https://hooks.example.com/test")
    assert seen == ["https://hooks.example.com/test"]
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == {"error": "invalid_webhook_url", "message": "spy_rejected"}
