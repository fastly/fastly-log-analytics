"""Branch-coverage tests for backend/utils/remote_access.py.

Extends the integration-style suite in
``tests/remote_access/test_middleware.py``. That file covers the
"happy path" through ``RemoteAccessMiddleware.dispatch`` via TestClient
against a built-up FastAPI app; this file fills the gaps the coverage
report flagged:

* helper functions (`_is_private_or_loopback`, `client_ip`,
  `_local_host_allowed`, `_remote_host_allowed`, `_origin_allowed`,
  `_strip_analyst_envelope`, `_body_service_ids`)
* `_StaticAssetLimiter.check` + eviction
* `TimeBounds.clamp` + `get_analyst_time_bounds`
* middleware branches that need session manipulation (fingerprint
  mismatch boot, tos_pending, IP-roaming whitelist, static-asset rate
  limit, cdn_map fallback resolution)

We re-use the same per-test isolation fixture from
``tests/remote_access/conftest.py`` by importing the FastAPI app builder
+ helpers from the integration test module, then drop in narrow
fixtures for the helper-function unit tests.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.responses import Response, StreamingResponse

from backend.core import share_db
from backend.utils import tunnel
from backend.utils.remote_access import (
    RemoteAccessMiddleware,
    TimeBounds,
    _body_service_ids,
    _is_private_or_loopback,
    _local_host_allowed,
    _origin_allowed,
    _remote_host_allowed,
    _StaticAssetLimiter,
    _strip_analyst_envelope,
    apply_response_hardening,
    clamp_or_400,
    client_ip,
    get_analyst_time_bounds,
    is_request_remote,
)

# ── Shared isolation: mirror tests/remote_access/conftest.py ───────────────


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Per-test share DB + reset tunnel singleton.

    Without this the helper-function tests would share state with each
    other (and worse, with whatever test_middleware.py left behind in a
    shared pytest run).
    """
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    share_db.reset_for_tests()
    tunnel.reset_for_tests()
    yield
    share_db.close_all_connections()
    tunnel.reset_for_tests()


# ── App builder + helpers (lifted from test_middleware to keep the suite self-contained) ──


def _build_app() -> FastAPI:
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/dashboard/aggregates")
    def _dash_agg(payload: dict | None = None):
        # Read-allowed POST under /api/dashboard/ for the body-service-id branch.
        return {"ok": True}

    @app.get("/api/services/{service_id}/scoring/labels")
    def _scoring_labels(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/_next/static/foo.js")
    def _static():
        return {"ok": True}

    return app


@pytest.fixture
def app():
    return _build_app()


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


def _seed_invite(service_ids=None, ip_whitelist=None) -> dict:
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids or ["svcA"],
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    return share_db.get_remote_invite(invite["id"])


def _start_share():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://testserver")
    return mgr


def _login_analyst(client, invite, *, host="testserver") -> str:
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": host,
            "Origin": f"https://{host}",
        },
    )
    assert r.status_code == 200, r.text
    # L1: the session id is cookie-only now (not in the JSON body). Pull it from
    # the response Set-Cookie (pending before TOS accept, full after); the
    # TestClient jar refuses `secure` cookies over http://, so set it manually.
    sid = ""
    for raw in r.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')  # the deletion cookie carries value="" (quoted)
        if name in ("analyst_pending_session_id", "analyst_session_id") and val:
            sid = val
    client.cookies.set("analyst_session_id", sid)
    return sid


# ── Helper: _is_private_or_loopback ────────────────────────────────────────


def test_is_private_or_loopback_loopback_ip_true():
    assert _is_private_or_loopback("127.0.0.1") is True
    assert _is_private_or_loopback("::1") is True


def test_is_private_or_loopback_public_ip_false():
    # Drops the over-broad "private" rule — RFC1918 must NOT count as local.
    assert _is_private_or_loopback("10.0.0.5") is False
    assert _is_private_or_loopback("192.168.1.1") is False
    assert _is_private_or_loopback("169.254.169.254") is False  # AWS metadata
    assert _is_private_or_loopback("8.8.8.8") is False


def test_is_private_or_loopback_hostname_stubs():
    # ValueError fallback for non-IP strings — only the two stubs return True.
    assert _is_private_or_loopback("testclient") is True
    assert _is_private_or_loopback("localhost") is True
    assert _is_private_or_loopback("evil.example.com") is False
    assert _is_private_or_loopback("") is False


# ── Helper: LOCAL_ADMIN_CIDRS opt-in trust segment ─────────────────────────


@pytest.mark.security_regression
def test_local_admin_cidrs_default_off(monkeypatch):
    """With the env unset, RFC1918 stays remote — prod behavior unchanged."""
    monkeypatch.delenv("LOCAL_ADMIN_CIDRS", raising=False)
    assert _is_private_or_loopback("172.28.0.1") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_scoped_trust(monkeypatch):
    """A declared infra subnet classifies as admin; everything else stays
    remote (a VPN analyst on another private range must NOT be promoted)."""
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "172.28.0.0/16")
    assert _is_private_or_loopback("172.28.0.1") is True
    assert _is_private_or_loopback("172.28.44.9") is True
    assert _is_private_or_loopback("10.0.0.5") is False
    assert _is_private_or_loopback("192.168.1.1") is False
    assert _is_private_or_loopback("169.254.169.254") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_rejects_catch_all(monkeypatch):
    """0.0.0.0/0 (or anything catch-all-sized) is the Host-spoof admin bypass
    reborn — it must be refused, not honored."""
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "0.0.0.0/0")
    assert _is_private_or_loopback("8.8.8.8") is False
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "::/0")
    assert _is_private_or_loopback("2001:db8::1") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_rejects_historical_bypass_classes(monkeypatch):
    """The guard must make the two removed bypasses unconfigurable: broad
    RFC1918 (VPN analyst → admin) and link-local (169.254.169.254 cloud
    metadata SSRF → admin). Public ranges are refused outright."""
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "10.0.0.0/8")
    assert _is_private_or_loopback("10.1.2.3") is False
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "169.254.0.0/16")
    assert _is_private_or_loopback("169.254.169.254") is False
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "169.254.169.254/32")
    assert _is_private_or_loopback("169.254.169.254") is False
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "8.8.8.0/24")
    assert _is_private_or_loopback("8.8.8.8") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_ignores_garbage(monkeypatch):
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "not-a-cidr, ,172.28.0.0/16")
    assert _is_private_or_loopback("172.28.0.1") is True
    assert _is_private_or_loopback("8.8.8.8") is False


# ── Helper: client_ip ────────────────────────────────────────────────────


def test_client_ip_returns_default_when_no_client():
    """Starlette gives request.client = None for ASGI scopes without a peer."""

    class _FakeReq:
        client = None

    assert client_ip(_FakeReq(), default="custom-default") == "custom-default"
    assert client_ip(_FakeReq()) == "0.0.0.0"


# ── Helper: _local_host_allowed / _remote_host_allowed / _origin_allowed ──


def test_local_host_allowed_empty_string_false():
    assert _local_host_allowed("") is False


def test_local_host_allowed_loopback_ip_true():
    # Hits the "ip.is_loopback" branch via _is_private_or_loopback inside.
    assert _local_host_allowed("127.0.0.1") is True
    assert _local_host_allowed("127.0.0.1:8000") is True


def test_local_host_allowed_named_in_allowlist():
    # "testserver" is in the built-in allowlist.
    assert _local_host_allowed("testserver") is True
    assert _local_host_allowed("backend:8000") is True


def test_local_host_allowed_not_in_allowlist():
    assert _local_host_allowed("attacker.example.com") is False


def test_remote_host_allowed_empty_string_false():
    assert _remote_host_allowed("") is False


def test_remote_host_allowed_matches_registered_endpoint():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://tun-xyz.lhr.life")
    assert _remote_host_allowed("tun-xyz.lhr.life") is True
    assert _remote_host_allowed("tun-xyz.lhr.life:443") is True
    assert _remote_host_allowed("other.lhr.life") is False


def test_remote_host_allowed_no_endpoint_registered():
    # No start_sharing call → public_endpoint is None → nothing matches.
    assert _remote_host_allowed("anything.example.com") is False


def test_origin_allowed_empty_string_false():
    assert _origin_allowed("") is False


def test_origin_allowed_no_hostname_in_url():
    # urlparse on a scheme-only string yields no hostname → False.
    assert _origin_allowed("https://") is False


def test_origin_allowed_matches_endpoint_hostname():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://tun-abc.lhr.life")
    assert _origin_allowed("https://tun-abc.lhr.life") is True
    assert _origin_allowed("https://attacker.example.com") is False


# ── Helper: _strip_analyst_envelope ────────────────────────────────────────


def _make_streaming(body: bytes, *, content_type: str, status: int = 200) -> StreamingResponse:
    async def _gen():
        yield body

    resp = StreamingResponse(
        _gen(),
        status_code=status,
        media_type=content_type,
    )
    resp.headers["content-type"] = content_type
    return resp


def test_strip_envelope_skips_non_json():
    resp = _make_streaming(b"hello world", content_type="text/plain")
    out = asyncio.run(_strip_analyst_envelope(resp))
    # Non-JSON returns the SAME StreamingResponse object (no body buffering).
    assert out is resp


def test_strip_envelope_invalid_json_passes_through():
    resp = _make_streaming(b"<not json>", content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    assert out.status_code == 200
    assert out.body == b"<not json>"


def test_strip_envelope_no_stripped_keys_passes_through():
    payload = {"data": [1, 2, 3], "_section_timings": {"summary": 0.1}}
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    parsed = json.loads(out.body)
    assert parsed == payload  # _section_timings kept; nothing dropped


def test_strip_envelope_removes_debug_keys():
    payload = {
        "data": [1],
        "_debug_queries": ["SELECT 1"],
        "_debug_calls": [{"url": "https://api.fastly.com/foo"}],
        "_debug_sqlite": [{"seq": 1, "sql": "SELECT session_id FROM scoring_labels"}],
        "_is_cached": True,
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    parsed = json.loads(out.body)
    assert "_debug_queries" not in parsed
    assert "_debug_calls" not in parsed
    assert "_debug_sqlite" not in parsed
    assert "_is_cached" not in parsed
    assert parsed["data"] == [1]
    assert out.headers["content-length"] == str(len(out.body))


def test_strip_envelope_list_body_passes_through():
    # Top-level lists hit the `isinstance(data, dict)` False branch — no
    # changes recorded, original bytes returned unmodified.
    body = json.dumps([1, 2, 3]).encode()
    resp = _make_streaming(body, content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    assert json.loads(out.body) == [1, 2, 3]


def test_strip_envelope_removes_bare_debug_queries():
    # /api/query and /api/dashboard/bundle emit `debug_queries` (no
    # underscore prefix) directly from plain dict responses. The strip
    # list now includes that bare form so analysts don't see the
    # internal DuckDB Iceberg view-resolution SQL.
    payload = {
        "columns": ["x"],
        "data": [{"x": 1}],
        "debug_queries": [{"sql": "-- DuckDB Iceberg View Resolution\nCREATE VIEW logs ..."}],
        "debug_sqlite": [{"seq": 1, "sql": "SELECT * FROM cron_runs"}],
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    parsed = json.loads(out.body)
    assert "debug_queries" not in parsed
    assert "debug_sqlite" not in parsed
    assert parsed["data"] == [{"x": 1}]


def test_strip_envelope_removes_bare_debug_calls():
    payload = {
        "data": [],
        "debug_calls": [{"url": "https://api.fastly.com/services/foo"}],
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    parsed = json.loads(out.body)
    assert "debug_calls" not in parsed


def test_strip_envelope_keeps_bare_section_timings():
    # Pin the intentional-keep decision: section_timings (both bare and
    # underscore-prefixed) is observability — phase names only, no SQL /
    # data / infra. If a future PR extends the strip set, this test
    # forces the decision to be re-considered explicitly.
    payload = {
        "data": [1],
        "section_timings": [{"section": "summary", "time_ms": 1.2}],
        "_section_timings": [{"section": "validate", "time_ms": 0.5}],
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp))
    parsed = json.loads(out.body)
    assert "section_timings" in parsed
    assert "_section_timings" in parsed
    assert parsed["section_timings"][0]["section"] == "summary"


# ── R-2: mask_ips PII masking via analyst session policy ───────────────────


def test_strip_envelope_masks_ips_when_session_policy_enabled():
    """When analyst_session.pii_policy.mask_ips is True, the helper walks
    the response body and masks any `ip` / `client_ip` / `ip_address` /
    `remote_addr` field via apply_pii_policy. Last-octet-xxx for IPv4."""
    payload = {
        "sessions": [
            {"ip": "1.2.3.4", "country": "US", "request_count": 7},
            {"ip": "5.6.7.8", "country": "GB", "request_count": 3},
        ],
        "total": 2,
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")

    class _Session:
        pii_policy = {"mask_ips": True}

    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=_Session()))
    parsed = json.loads(out.body)
    ips = [row["ip"] for row in parsed["sessions"]]
    assert ips == ["1.2.3.xxx", "5.6.7.xxx"], f"expected masked IPs, got {ips}"
    # Aggregates (country, request_count) untouched.
    assert parsed["sessions"][0]["country"] == "US"
    assert parsed["sessions"][0]["request_count"] == 7
    assert out.headers["content-length"] == str(len(out.body))


def test_strip_envelope_does_not_mask_when_session_policy_disabled():
    payload = {"sessions": [{"ip": "1.2.3.4"}]}
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")

    class _Session:
        pii_policy = {"mask_ips": False}

    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=_Session()))
    parsed = json.loads(out.body)
    assert parsed["sessions"][0]["ip"] == "1.2.3.4"


def test_strip_envelope_no_session_does_not_mask():
    """Admin path (no analyst session) must not mask IPs."""
    payload = {"sessions": [{"ip": "1.2.3.4"}]}
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=None))
    parsed = json.loads(out.body)
    assert parsed["sessions"][0]["ip"] == "1.2.3.4"


def test_strip_envelope_combines_strip_and_mask_in_one_pass():
    """Both the envelope strip AND the PII mask apply in the same buffered
    body — we re-walk only once."""
    payload = {
        "data": [{"ip": "9.9.9.9"}],
        "_debug_queries": [{"sql": "SELECT 1"}],
        "section_timings": [{"section": "summary", "time_ms": 1.0}],
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")

    class _Session:
        pii_policy = {"mask_ips": True}

    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=_Session()))
    parsed = json.loads(out.body)
    assert "_debug_queries" not in parsed
    assert parsed["data"][0]["ip"] == "9.9.9.xxx"
    assert parsed["section_timings"][0]["section"] == "summary"


@pytest.mark.security_regression
def test_strip_envelope_masks_dashboard_top_n_ip_panel():
    """PII LEAK REGRESSION (Top IPs card): the /api/dashboard/aggregates +
    /bundle top-N panels emit the dimension value under the GENERIC key
    ``value`` keyed by field name (``data["ip"]["top"][i]["value"]`` =
    backend/repositories/dashboard.py:608/655), NOT under ``ip``. The
    field-name masker in apply_pii_policy only masks keys in
    {ip, ip_address, client_ip, remote_addr}, so with mask_ips=True the
    analyst's Top IPs card leaked every raw client IP while the Sessions
    list (which emits ``ip``) was correctly masked.

    Invariant: with mask_ips=True the IP top-N panel's ``value`` strings
    are masked, while non-IP panels (url, ua) and map_data country values
    stay verbatim. ua/url are NEVER masked for analysts (analyst-needs-ua-url).
    """
    payload = {
        "data": {
            "ip": {
                "top": [
                    {"value": "203.0.113.45", "count": 12},
                    {"value": "198.51.100.7", "count": 5},
                ],
                "total": 17,
            },
            "url": {"top": [{"value": "/login", "count": 9}], "total": 9},
            "ua": {"top": [{"value": "curl/8.4.0", "count": 4}], "total": 4},
            "country": {"top": [{"value": "US", "count": 17}], "total": 17},
        },
        "map_data": [{"country": "US", "count": 17}],
        "total_rows": 17,
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")

    class _Session:
        pii_policy = {"mask_ips": True}

    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=_Session()))
    parsed = json.loads(out.body)

    ip_values = [r["value"] for r in parsed["data"]["ip"]["top"]]
    assert ip_values == ["203.0.113.xxx", "198.51.100.xxx"], (
        f"Top IPs card leaked raw client IPs to a mask_ips analyst: {ip_values}"
    )
    # ua + url stay visible for analysts (only ip is masked).
    assert parsed["data"]["url"]["top"][0]["value"] == "/login"
    assert parsed["data"]["ua"]["top"][0]["value"] == "curl/8.4.0"
    # country panel + map_data are not IPs and must be untouched.
    assert parsed["data"]["country"]["top"][0]["value"] == "US"
    assert parsed["map_data"][0]["country"] == "US"
    assert out.headers["content-length"] == str(len(out.body))


@pytest.mark.security_regression
def test_strip_envelope_admin_top_n_ip_panel_not_masked():
    """Admin path (no analyst session) must NOT mask the Top IPs panel —
    operators see real IPs by design (ADMIN = network-trusted)."""
    payload = {"data": {"ip": {"top": [{"value": "203.0.113.45", "count": 1}], "total": 1}}}
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=None))
    parsed = json.loads(out.body)
    assert parsed["data"]["ip"]["top"][0]["value"] == "203.0.113.45"


@pytest.mark.security_regression
def test_strip_envelope_leaves_oip_panel_visible_for_analyst():
    """ACCEPTED-INTENT pin (2026-06-24 PII audit): the origin IP (``oip``)
    top-N panel is INTENTIONALLY left visible to a mask_ips analyst.

    ``oip`` values are the operator's origin / Fastly anycast shield IPs
    (151.101.x / 199.232.x / 146.75.x) — CDN/operator infrastructure, already
    public, NOT end-user PII — and the Origin IP-Health card exists to show
    them. ``oip`` is deliberately absent from apply_pii_policy's masked_keys,
    so its top-N ``value`` cell passes through unmasked even while the client
    ``ip`` panel is masked. Pinned both ways: a future audit shouldn't re-flag
    this as a leak, and nobody should "fix" it by masking ``oip`` (which would
    gut the card for analysts). Contrast
    test_strip_envelope_masks_dashboard_top_n_ip_panel (client ip IS masked)."""
    payload = {
        "data": {
            "ip": {"top": [{"value": "203.0.113.45", "count": 9}], "total": 9},
            "oip": {"top": [{"value": "151.101.1.51", "count": 9}], "total": 9},
        },
    }
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")

    class _Session:
        pii_policy = {"mask_ips": True}

    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=_Session()))
    parsed = json.loads(out.body)
    # client ip masked …
    assert parsed["data"]["ip"]["top"][0]["value"] == "203.0.113.xxx"
    # … origin/CDN ip intentionally NOT masked.
    assert parsed["data"]["oip"]["top"][0]["value"] == "151.101.1.51"


# ── apply_response_hardening ───────────────────────────────────────────────


def test_apply_response_hardening_sets_defaults_only():
    resp = Response(content="x")
    apply_response_hardening(resp)
    assert resp.headers["Cache-Control"] == "private, no-store"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"
    # Split-directive CSP — pin the per-directive shape (5a14cdf). A
    # regression that collapses to a monolithic default-src or restores
    # 'unsafe-inline' on connect-src would silently re-open the
    # ex-filtration channel the per-directive scoping closes.
    csp = resp.headers["Content-Security-Policy"]
    assert "script-src" in csp
    assert "connect-src 'self'" in csp
    # 'unsafe-inline' must be scoped to script-src / style-src only;
    # any later directive picking it up regresses the hardening.
    assert "connect-src 'self' 'unsafe-inline'" not in csp
    assert "img-src 'self' 'unsafe-inline'" not in csp
    # COOP same-origin closes the cross-origin-opener leak side channel.
    assert resp.headers["Cross-Origin-Opener-Policy"] == "same-origin"


def test_apply_response_hardening_preserves_existing_headers():
    resp = Response(content="x", headers={"Cache-Control": "public, max-age=300"})
    apply_response_hardening(resp)
    # setdefault must NOT overwrite a route-supplied value.
    assert resp.headers["Cache-Control"] == "public, max-age=300"


# ── _StaticAssetLimiter ────────────────────────────────────────────────────


def test_static_limiter_allows_under_threshold():
    lim = _StaticAssetLimiter()
    for _ in range(10):
        assert lim.check("1.2.3.4", 100) is True


def test_static_limiter_blocks_over_request_threshold():
    lim = _StaticAssetLimiter()
    for _ in range(lim.REQ_LIMIT):
        lim.check("1.2.3.4", 0)
    assert lim.check("1.2.3.4", 0) is False


def test_static_limiter_blocks_over_byte_threshold():
    lim = _StaticAssetLimiter()
    # First call records BYTE_LIMIT + 1 bytes; second call sees the sum
    # exceed and returns False.
    assert lim.check("1.2.3.4", lim.BYTE_LIMIT) is True
    # Next request pushes total above the byte cap.
    assert lim.check("1.2.3.4", 100) is False


def test_static_limiter_eviction_resets_when_over_cap(monkeypatch):
    """When tracked IPs exceed MAX_TRACKED_IPS, _evict_locked sweeps stale
    entries and clears everything if still over cap."""
    lim = _StaticAssetLimiter()
    # Shrink for test speed.
    monkeypatch.setattr(lim, "MAX_TRACKED_IPS", 3)
    # Fill past the cap with stale entries (timestamp in the past).
    old_now = time.time() - 9999
    lim._reqs = {f"ip-{i}": [old_now] for i in range(5)}
    lim._bytes = {f"ip-{i}": [(old_now, 100)] for i in range(5)}
    # A fresh check triggers _evict_locked because len > cap.
    assert lim.check("ip-new", 50) is True
    # All stale entries were swept; the new IP is the only one tracked.
    assert "ip-new" in lim._reqs
    # Stale entries are gone (all their timestamps were before cutoff).
    for i in range(5):
        assert f"ip-{i}" not in lim._reqs


def test_static_limiter_cardinality_bomb_clears_all(monkeypatch):
    """If after stale-sweep we're STILL over cap, _evict_locked clears the
    dicts entirely (the DoS guard)."""
    lim = _StaticAssetLimiter()
    monkeypatch.setattr(lim, "MAX_TRACKED_IPS", 2)
    now = time.time()
    # All entries are fresh — sweep won't drop them.
    lim._reqs = {f"ip-{i}": [now] for i in range(5)}
    lim._bytes = {f"ip-{i}": [(now, 10)] for i in range(5)}
    # Force _evict_locked: check sees len > cap, calls evict, which clears.
    lim.check("ip-trigger", 0)
    # After clear, only the trigger IP remains.
    assert set(lim._reqs.keys()) == {"ip-trigger"}


# ── TimeBounds.clamp + get_analyst_time_bounds ────────────────────────────


def test_timebounds_clamp_uses_max_of_starts():
    bounds = TimeBounds(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 30, tzinfo=UTC),
    )
    req_start = datetime(2026, 5, 1, tzinfo=UTC)
    req_end = datetime(2026, 7, 1, tzinfo=UTC)
    start, end = bounds.clamp(req_start, req_end)
    assert start == datetime(2026, 6, 1, tzinfo=UTC)  # session start wins (later)
    assert end == datetime(2026, 6, 30, tzinfo=UTC)  # session end wins (earlier)


def test_timebounds_clamp_request_inside_session_window():
    bounds = TimeBounds(
        start=datetime(2026, 6, 1, tzinfo=UTC),
        end=datetime(2026, 6, 30, tzinfo=UTC),
    )
    req_start = datetime(2026, 6, 10, tzinfo=UTC)
    req_end = datetime(2026, 6, 20, tzinfo=UTC)
    start, end = bounds.clamp(req_start, req_end)
    assert start == req_start
    assert end == req_end


def test_timebounds_clamp_no_inputs_defaults_to_last_hour():
    bounds = TimeBounds()  # unrestricted
    start, end = bounds.clamp(None, None)
    # Both anchored to now; the window should be 1h wide.
    delta = end - start
    assert timedelta(minutes=59) < delta < timedelta(minutes=61)


def test_timebounds_clamp_only_end_supplied():
    bounds = TimeBounds()
    fixed_end = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
    start, end = bounds.clamp(None, fixed_end)
    assert end == fixed_end
    assert (end - start) == timedelta(hours=1)


def test_timebounds_clamp_only_start_supplied():
    bounds = TimeBounds()
    fixed_start = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
    start, end = bounds.clamp(fixed_start, None)
    assert start == fixed_start
    # end defaults to "now" → strictly greater than fixed_start (which is
    # in the past relative to clock).
    assert end > start


def test_timebounds_clamp_empty_range_raises():
    bounds = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="empty"):
        bounds.clamp(None, None)


def test_get_analyst_time_bounds_no_session_returns_open():
    class _State:
        pass

    class _Req:
        state = _State()

    out = get_analyst_time_bounds(_Req())
    assert out.start is None
    assert out.end is None


def test_get_analyst_time_bounds_uses_session_fields():
    from backend.utils.tunnel.session import AnalystSession

    session = AnalystSession(
        session_id="sid",
        invite_id="iid",
        name="n",
        email="e@x",
        ip_address="1.2.3.4",
        user_agent="ua",
        fingerprint_signature="fp",
        pii_policy={},
        query_window_hours=None,
        query_start_time="2026-06-01T00:00:00Z",
        query_end_time="2026-06-30T00:00:00Z",
        login_time="2026-06-15T00:00:00Z",
        last_active_time="2026-06-15T00:00:00Z",
    )

    class _State:
        analyst_session = session

    class _Req:
        state = _State()

    out = get_analyst_time_bounds(_Req())
    assert out.start == datetime(2026, 6, 1, tzinfo=UTC)
    assert out.end == datetime(2026, 6, 30, tzinfo=UTC)


# ── clamp_or_400 — route-layer helper (audit R-1) ──────────────────────────


def test_clamp_or_400_admin_no_bounds_is_passthrough():
    """Admin (no analyst session) with both bounds None must pass through
    as (None, None) so the repo's own default range applies. Otherwise
    TimeBounds().clamp(None, None) would silently force now-1h..now even
    for admin requests that mean 'no filter'."""
    tb = TimeBounds()
    out = clamp_or_400(tb, None, None, analyst_session=None)
    assert out == (None, None)


def test_clamp_or_400_admin_with_bounds_clamps_against_open():
    """Admin with explicit bounds: TimeBounds() is open, so the clamp is
    a no-op pass-through of the requested ISO strings."""
    tb = TimeBounds()
    out = clamp_or_400(
        tb,
        "2026-06-01T00:00:00Z",
        "2026-06-02T00:00:00Z",
        analyst_session=None,
    )
    assert out[0].startswith("2026-06-01T00:00:00")
    assert out[1].startswith("2026-06-02T00:00:00")


def test_clamp_or_400_analyst_request_outside_window_is_clamped():
    """Analyst with start=T1, end=T3 and request [T0..T4] (T0<T1, T4>T3)
    should be clamped to [T1, T3]."""
    tb = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 20, tzinfo=UTC),
    )
    out = clamp_or_400(
        tb,
        "2026-06-01T00:00:00Z",  # earlier than session.start
        "2026-06-30T00:00:00Z",  # later than session.end
        analyst_session=object(),  # sentinel — non-None triggers clamp
    )
    # Clamped start = max(req_start, session.start) = 2026-06-10
    assert out[0].startswith("2026-06-10")
    # Clamped end = min(req_end, session.end) = 2026-06-20
    assert out[1].startswith("2026-06-20")


def test_clamp_or_400_empty_window_raises_400():
    """Request fully outside the analyst's allowed window must 400."""
    from fastapi import HTTPException

    tb = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 5, tzinfo=UTC),  # end before start → empty
    )
    with pytest.raises(HTTPException) as exc_info:
        clamp_or_400(tb, None, None, analyst_session=object())
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail.get("time_range_empty") is True
    assert "empty" in exc_info.value.detail.get("error", "").lower()


def test_clamp_or_400_analyst_no_bounds_still_clamps_to_default():
    """Analyst with no request bounds must NOT short-circuit; the clamp
    falls into TimeBounds.clamp's default (now-1h..now if session.start/end
    are also None). This is the analyst-can't-bypass-by-omitting case."""
    tb = TimeBounds()
    out = clamp_or_400(tb, None, None, analyst_session=object())
    # Both should be ISO strings (not None) because the analyst path always
    # invokes the clamp default.
    assert out[0] is not None
    assert out[1] is not None
    # The default window is roughly the last hour up to now.
    start_dt = datetime.fromisoformat(out[0].replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(out[1].replace("Z", "+00:00"))
    assert (end_dt - start_dt) == timedelta(hours=1)


def test_get_analyst_time_bounds_window_hours_overrides_old_start():
    """A relative window (e.g. last 24h) should TIGHTEN — `start` is the
    max of the absolute start and the relative one."""
    from backend.utils.tunnel.session import AnalystSession

    session = AnalystSession(
        session_id="sid",
        invite_id="iid",
        name="n",
        email="e@x",
        ip_address="1.2.3.4",
        user_agent="ua",
        fingerprint_signature="fp",
        pii_policy={},
        query_window_hours=24,
        query_start_time="2020-01-01T00:00:00Z",  # ancient — should be overridden
        query_end_time=None,
        login_time="2026-06-15T00:00:00Z",
        last_active_time="2026-06-15T00:00:00Z",
    )

    class _State:
        analyst_session = session

    class _Req:
        state = _State()

    out = get_analyst_time_bounds(_Req())
    # A rolling-window invite ceilings end to the anchor (now), so the upper
    # bound is non-None and a future req_end can't widen the window forward.
    assert out.end is not None
    # The relative window starts ~24h ago, far more recent than 2020-01-01.
    assert out.start is not None
    assert out.start > datetime(2026, 1, 1, tzinfo=UTC)
    # end ≈ now and the span is exactly the 24h window.
    assert (out.end - out.start) == timedelta(hours=24)


# ── Middleware integration: branches that need a logged-in analyst ─────────


def test_static_asset_rate_limit_429(client, monkeypatch):
    """Hammer /_next/* past the per-IP cap → 429."""
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)

    from backend.utils import remote_access as ra

    # Shrink the limit for test speed.
    monkeypatch.setattr(ra._static_limiter, "REQ_LIMIT", 3)
    monkeypatch.setattr(ra._static_limiter, "_reqs", {})
    monkeypatch.setattr(ra._static_limiter, "_bytes", {})

    headers = {"X-Remote-Analyst": "1", "Host": "testserver"}
    for _ in range(3):
        client.get("/_next/static/foo.js", headers=headers)
    r = client.get("/_next/static/foo.js", headers=headers)
    assert r.status_code == 429
    assert r.json()["error"] == "rate_limited"


def test_tos_pending_session_returns_403(client):
    """A session whose linked invite has not accepted the current TOS gets
    its ``tos_pending`` flag re-synced by ``validate_session`` on every
    request, then the middleware returns 403 tos_pending."""
    _start_share()
    invite = _seed_invite()
    sid = _login_analyst(client, invite)
    # Clear the TOS acceptance on the invite — validate_session re-derives
    # session.tos_pending from this on every request.
    con = share_db.get_global_share_con()
    con.execute(
        "UPDATE remote_invites SET tos_accepted_at=NULL, tos_version=NULL WHERE id=?",
        (invite["id"],),
    )
    con.commit()

    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    # The branch only fires if there IS a current TOS row to compare
    # against. _seed_invite's mark_tos_accepted path only runs when one
    # exists, so confirm.
    tos = share_db.get_latest_tos()
    if tos is None:
        # No TOS row exists in this test DB → tos_pending stays False and
        # the request flows through. Skip rather than mis-asserting.
        pytest.skip("No TOS row seeded; tos_pending branch unreachable.")
    assert r.status_code == 403
    assert r.json()["error"] == "tos_pending"
    # Restore in-memory session state — autouse fixture resets anyway.
    _ = sid


def test_fingerprint_mismatch_boots_session(client):
    """Changing User-Agent mid-session → 401 fingerprint_mismatch + the
    session is removed from the manager."""
    _start_share()
    invite = _seed_invite()
    sid = _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    assert sid in mgr._sessions

    r = client.get(
        "/api/dashboard?service=svcA",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "User-Agent": "Mozilla/5.0 EvilBrowser/99 Linux",  # wholly different UA
        },
    )
    assert r.status_code == 401
    assert r.json()["error"] == "fingerprint_mismatch"
    # boot_session was called: in-memory session evicted.
    assert sid not in mgr._sessions
    # Audit log gained a FINGERPRINT_MISMATCH row.
    logs = share_db.get_share_audit_logs(limit=20, event_type="FINGERPRINT_MISMATCH")
    assert any(row.get("email") == invite["email"] for row in logs)


def test_ip_roaming_blocks_when_not_whitelisted(client):
    """Login captures the request IP. If a subsequent request comes from a
    different IP and the invite has an explicit whitelist that does NOT
    include the new IP, the middleware rejects with ip_not_whitelisted."""
    _start_share()
    # Invite restricts to a single IP that isn't TestClient's 127.0.0.1.
    invite = _seed_invite(ip_whitelist="10.20.30.40")
    # Bypass IP whitelist for login by patching ip_in_whitelist temporarily.
    with patch("backend.core.share_db.ip_in_whitelist", return_value=True):
        sid = _login_analyst(client, invite)
    # Reset to real check for the subsequent request — 127.0.0.1 is NOT in
    # "10.20.30.40", so the IP-roaming branch fires.
    mgr = tunnel.get_tunnel_manager()
    # Pin session ip_address to something different so the != branch triggers.
    mgr._sessions[sid].ip_address = "10.20.30.40"

    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "ip_not_whitelisted"


def test_ip_roaming_allows_when_whitelist_passes(client):
    """If the new IP IS in the whitelist (or whitelist is None), the
    session ip_address is updated rather than booted."""
    _start_share()
    invite = _seed_invite()  # no whitelist → "" → permits all
    sid = _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    mgr._sessions[sid].ip_address = "9.9.9.9"  # force a delta

    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200
    # Session IP got updated to the request IP (TestClient default).
    assert mgr._sessions[sid].ip_address == "testclient"


def test_body_service_id_mismatch_blocked(client):
    """M-3: a forged service_id in the JSON body must be checked. If the
    body's service_id is not in the analyst's allowlist, return 403."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)

    r = client.post(
        "/api/dashboard/aggregates",
        json={"service_id": "svcZ", "filter": {}},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 403
    assert r.json()["error"] == "service_not_authorized"
    assert r.json()["service"] == "svcZ"


def test_body_service_id_match_allowed(client):
    """Mirror of the above — when body service matches allowlist, request goes through."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)

    r = client.post(
        "/api/dashboard/aggregates",
        json={"service_id": "svcA"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 200


def test_body_service_ids_helper_skips_non_json():
    """Direct unit test of _body_service_ids: non-JSON content-type → []."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"text/plain")],
        "query_string": b"",
    }

    async def _recv():
        return {"type": "http.request", "body": b"", "more_body": False}

    req = Request(scope, _recv)
    assert asyncio.run(_body_service_ids(req)) == []


def test_body_service_ids_helper_skips_non_post():
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }

    async def _recv():
        return {"type": "http.request", "body": b"", "more_body": False}

    req = Request(scope, _recv)
    assert asyncio.run(_body_service_ids(req)) == []


def test_body_service_ids_helper_handles_invalid_json():
    """A POST with content-type application/json but unparseable body → []."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    sent = {"v": False}

    async def _recv():
        if sent["v"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["v"] = True
        return {"type": "http.request", "body": b"<not json>", "more_body": False}

    req = Request(scope, _recv)
    assert asyncio.run(_body_service_ids(req)) == []


def test_body_service_ids_helper_extracts_both_keys():
    from starlette.requests import Request

    payload = json.dumps({"service_id": "svcA", "service": "svcB"}).encode()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    sent = {"v": False}

    async def _recv():
        if sent["v"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["v"] = True
        return {"type": "http.request", "body": payload, "more_body": False}

    req = Request(scope, _recv)
    out = asyncio.run(_body_service_ids(req))
    assert set(out) == {"svcA", "svcB"}


def test_body_service_ids_helper_skips_non_dict_body():
    """A JSON array body returns [] (the helper only inspects dict shapes)."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    sent = {"v": False}

    async def _recv():
        if sent["v"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["v"] = True
        return {"type": "http.request", "body": b"[1, 2, 3]", "more_body": False}

    req = Request(scope, _recv)
    assert asyncio.run(_body_service_ids(req)) == []


def test_body_service_ids_helper_coerces_integer_service_id():
    """An int ``service_id`` must be coerced to str so the scope check sees
    the same value Pydantic will coerce downstream.

    Regression for F002 (audit run 7ba15352): the prior check
    ``isinstance(v, str) and v`` silently dropped ``{"service_id": 12345}``
    payloads, falling back to the analyst's active service while the
    downstream Pydantic ``str`` field coerced 12345 → "12345" and executed
    the request for an unauthorized service.
    """
    from starlette.requests import Request

    payload = json.dumps({"service_id": 12345, "service": 67890}).encode()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    sent = {"v": False}

    async def _recv():
        if sent["v"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["v"] = True
        return {"type": "http.request", "body": payload, "more_body": False}

    req = Request(scope, _recv)
    out = asyncio.run(_body_service_ids(req))
    assert set(out) == {"12345", "67890"}


def test_body_service_ids_helper_caps_buffered_body_at_4mib():
    """Streaming an arbitrarily large chunked body must not unboundedly
    accumulate in memory.

    Regression for F003 (audit run 7ba15352): the prior ``while more_body:``
    loop appended every chunk with no cumulative cap, so an authenticated
    attacker could stream a multi-GB body and OOM the worker. The helper
    must stop accumulating once the running total exceeds
    BODY_INSPECT_MAX_BYTES (4 MiB) — we verify by feeding it more chunks
    than the cap allows and asserting it returns without consuming the
    full stream.
    """
    from starlette.requests import Request

    from backend.utils.remote_access import BODY_INSPECT_MAX_BYTES

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    # Feed 2 MiB per chunk, far past the cap; if the helper honors the cap
    # it stops reading after the 3rd chunk (~6 MiB > 4 MiB cap).
    chunk_size = 2 * 1024 * 1024
    chunks_served = {"n": 0}
    big_chunk = b"x" * chunk_size

    async def _recv():
        chunks_served["n"] += 1
        # Always return more_body=True to simulate an attacker who never
        # closes the stream; the helper's cap is what must break the loop.
        if chunks_served["n"] > 100:  # safety net so a regression doesn't hang the test
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": big_chunk, "more_body": True}

    req = Request(scope, _recv)
    asyncio.run(_body_service_ids(req))
    # If the cap fires correctly, the loop breaks after the chunk that
    # pushed the cumulative total past 4 MiB — that's at most 3 reads.
    assert chunks_served["n"] <= 3, (
        f"helper read {chunks_served['n']} chunks of {chunk_size} bytes — "
        f"expected ≤ 3 once cumulative > {BODY_INSPECT_MAX_BYTES}"
    )


# ── is_request_remote: loopback peer + X-Remote-Analyst, sharing inactive ──


def test_is_request_remote_loopback_without_sharing_false():
    """Loopback + X-Remote-Analyst: 1 but sharing inactive → still local."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "headers": [(b"host", b"testserver"), (b"x-remote-analyst", b"1")],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
    }

    async def _recv():
        return {"type": "http.disconnect"}

    req = Request(scope, _recv)
    # Sharing was not started → mgr.is_sharing_active() returns False.
    assert is_request_remote(req) is False


def test_is_request_remote_public_peer_true():
    """A non-loopback peer is treated as remote regardless of sharing state."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "headers": [],
        "query_string": b"",
        "client": ("8.8.8.8", 12345),
    }

    async def _recv():
        return {"type": "http.disconnect"}

    req = Request(scope, _recv)
    assert is_request_remote(req) is True


def test_is_request_remote_no_client_peer_fails_closed():
    """An ASGI scope with no socket peer (``request.client is None``) must
    classify as REMOTE (analyst-gated), not trusted local admin.

    Pre-fix, ``client_ip(default="127.0.0.1")`` turned the no-client case
    into a loopback classification, so an unknown peer skipped the entire
    analyst firewall. Unreachable in prod (Caddy always populates the peer)
    but a fail-open default direction, so it fails closed now.
    """
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "headers": [],
        "query_string": b"",
        "client": None,
    }

    async def _recv():
        return {"type": "http.disconnect"}

    req = Request(scope, _recv)
    assert is_request_remote(req) is True


# ── Additional defensive-branch coverage ─────────────────────────────────


def test_admin_shared_secret_reads_env_each_call(monkeypatch):
    """Re-reads env each call so tests can flip values without reload."""
    from backend.utils.remote_access import _admin_shared_secret

    monkeypatch.setenv("ADMIN_SHARED_SECRET", "secret-1")
    assert _admin_shared_secret() == "secret-1"

    monkeypatch.setenv("ADMIN_SHARED_SECRET", "  secret-2  ")
    # Trimmed on every call.
    assert _admin_shared_secret() == "secret-2"

    monkeypatch.delenv("ADMIN_SHARED_SECRET", raising=False)
    assert _admin_shared_secret() == ""


def test_is_private_or_loopback_invalid_string_falls_through_to_stub_check():
    """ValueError from ipaddress.ip_address() falls through to the
    hostname-stub check at line 287 — pinned because the stub list
    is the only way TestClient peers (``testclient``) are accepted."""
    from backend.utils.remote_access import _is_private_or_loopback

    # arbitrary non-IP, non-stub string → False
    assert _is_private_or_loopback("not-an-ip-or-stub") is False
    # known stubs
    assert _is_private_or_loopback("testclient") is True
    assert _is_private_or_loopback("localhost") is True


def test_client_ip_uses_default_when_client_absent():
    """Defensive: a request with no .client (rare ASGI shape, but
    possible in starlette internals) → fallback marker is returned."""
    from starlette.requests import Request

    from backend.utils.remote_access import client_ip

    scope = {"type": "http", "client": None, "method": "GET", "path": "/", "headers": []}
    req = Request(scope, lambda: None)
    assert client_ip(req, default="0.0.0.0") == "0.0.0.0"
    # Custom marker also threads through.
    assert client_ip(req, default="admin") == "admin"


def test_remote_host_allowed_returns_false_when_no_public_endpoint(monkeypatch):
    """Line 371-380: no tunnel.state.public_endpoint registered → no
    candidates → False for every host_header."""
    from unittest.mock import MagicMock

    from backend.utils import remote_access as ra

    mgr = MagicMock()
    mgr.state.public_endpoint = None
    monkeypatch.setattr(ra, "get_tunnel_manager", lambda: mgr)
    assert ra._remote_host_allowed("anything.example.com") is False


def test_origin_allowed_returns_false_when_url_has_no_hostname():
    """Origins without hostnames (e.g. just a scheme) → False."""
    from backend.utils.remote_access import _origin_allowed

    assert _origin_allowed("file://") is False
    assert _origin_allowed("") is False


def test_origin_allowed_returns_false_on_endpoint_mismatch(monkeypatch):
    """A non-matching origin hostname → False even if the tunnel state
    has an endpoint registered."""
    from unittest.mock import MagicMock

    from backend.utils import remote_access as ra

    mgr = MagicMock()
    mgr.state.public_endpoint = "https://my.endpoint.example/"
    monkeypatch.setattr(ra, "get_tunnel_manager", lambda: mgr)
    assert ra._origin_allowed("https://other.example.com/path") is False


def test_origin_allowed_returns_true_on_endpoint_match(monkeypatch):
    from unittest.mock import MagicMock

    from backend.utils import remote_access as ra

    mgr = MagicMock()
    mgr.state.public_endpoint = "https://my.endpoint.example/"
    monkeypatch.setattr(ra, "get_tunnel_manager", lambda: mgr)
    assert ra._origin_allowed("https://my.endpoint.example/path") is True


# ── _is_blocked_path: each layer ────────────────────────────────────────


def test_is_blocked_path_prefix_match():
    from backend.utils.remote_access import _is_blocked_path

    # /api/admin/ is in _ANALYST_BLOCKED_PREFIXES
    assert _is_blocked_path("/api/admin/anything") is True
    assert _is_blocked_path("/api/admin/share/foo") is True


def test_is_blocked_path_normalises_trailing_slash():
    """Trailing slash MUST NOT bypass the gate — line 430 normalisation."""
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path("/api/download/") is True
    assert _is_blocked_path("/api/download") is True
    # Multiple trailing slashes also collapse.
    assert _is_blocked_path("/api/download///") is True


def test_is_blocked_path_subpath_exact_match():
    from backend.utils.remote_access import _is_blocked_path

    # /api/sync-status is in _ANALYST_BLOCKED_SUBPATHS
    assert _is_blocked_path("/api/sync-status") is True
    # Sub-path under that prefix also blocked.
    assert _is_blocked_path("/api/sync-status/anything") is True


def test_is_blocked_path_sibling_not_swallowed():
    """A bare ``/api/download`` entry must NOT block ``/api/download-foo``
    (sibling routes that share the prefix). Pinned to defend against a
    regex-style mistake in the blocklist."""
    from backend.utils.remote_access import _is_blocked_path

    # The current _ANALYST_BLOCKED_PREFIXES tuple itself uses some bare
    # prefixes (e.g. /api/alerts) without trailing slash — those WILL
    # match any path starting with that prefix. So /api/alerts-foo would
    # also be blocked, which is intentional per the security audit. Pin
    # the documented sibling-safe case: /api/cron-runs is in prefixes,
    # so /api/cron-runs-anything is also blocked, but /api/download is
    # in SUBPATHS which uses stricter matching.
    assert _is_blocked_path("/api/download-foo") is False
    assert _is_blocked_path("/api/cron-schedule-x") is False


def test_is_blocked_path_scoring_suffix_admin_only():
    """Paths containing ``/scoring/`` AND ending with an admin-only
    suffix (/config, /status, etc.) → blocked."""
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path("/api/services/abc/scoring/config") is True
    assert _is_blocked_path("/api/services/abc/scoring/status") is True
    # Analyst-allowed suffixes stay open.
    assert _is_blocked_path("/api/services/abc/scoring/labels") is False


def test_is_blocked_path_subpath_regex_match():
    """Path-parameter routes match via regex fullmatch (line 438)."""
    from backend.utils.remote_access import _is_blocked_path

    # /api/services/{id}/lake-info is regex-blocked
    assert _is_blocked_path("/api/services/abc/lake-info") is True
    # /api/services/{id}/custom-fields/anything also blocked
    assert _is_blocked_path("/api/services/xyz/custom-fields") is True
    assert _is_blocked_path("/api/services/xyz/custom-fields/items") is True
    # Analyst-allowed under same service prefix stays open.
    assert _is_blocked_path("/api/services/abc/scoring/labels") is False


def test_is_blocked_path_returns_false_for_unrelated_path():
    """Bog-standard analyst-reachable path → False."""
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path("/api/dashboard") is False
    assert _is_blocked_path("/") is False


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "suffix",
    ["/enable", "/disable", "/versions", "/upgrade"],
)
def test_is_blocked_path_rum_suffix_admin_only(suffix):
    """RUM suffix gate: paths containing ``/rum/`` AND ending with an
    admin-only suffix are blocked. Covers the pre-existing /rum/enable,
    /rum/disable endpoints too — before this gate was added they had no
    ``/rum/`` entry anywhere in the analyst blocklists and were reachable
    by an authenticated analyst. /rum/status is deliberately NOT in this
    list (F1 audit fix) — see test_is_blocked_path_rum_reads_stay_open."""
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path(f"/api/services/abc/rum{suffix}") is True


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/services/abc/rum/status",
        "/api/services/abc/rum/beacon-health",
        "/api/services/abc/rum/analytics",
        "/api/services/abc/rum/live-events",
    ],
)
def test_is_blocked_path_rum_reads_stay_open(path):
    """Negative control: read-only RUM telemetry must NOT be shadowed by
    the suffix gate (mirrors the scoring-suffix negative control).
    /rum/status was blocked pre-fix (F1 audit finding), which made the
    analystVisible RUM page 403 → dead; the route itself now projects an
    analyst-safe body ({enabled, enabled_at}, no VCL fingerprint fields)."""
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path(path) is False


# ── _is_sse_route ───────────────────────────────────────────────────────


def test_is_sse_route_matches_sse_in_path():
    from backend.utils.remote_access import _is_sse_route

    assert _is_sse_route("/api/sse/foo") is True
    assert _is_sse_route("/foo/sse/bar") is True


def test_is_sse_route_matches_stream_suffix():
    from backend.utils.remote_access import _is_sse_route

    assert _is_sse_route("/api/cron-runs/stream") is True


def test_is_sse_route_returns_false_for_unrelated():
    from backend.utils.remote_access import _is_sse_route

    assert _is_sse_route("/api/dashboard") is False


@pytest.mark.security_regression
def test_is_sse_route_detects_hyphenated_stream_routes():
    """The analyst SSE gate is documented as "default OFF; explicit allow
    only" (see the `_ANALYST_SSE_ALLOWLIST` docstring in remote_access.py:
    "New SSE routes default to *off* for analysts; an explicit add here is
    the only way to expose one") — but that promise is only as strong as
    `_is_sse_route`'s detection. The check is
    ``"/sse" in path or path.endswith("/stream")``: a route whose final
    path segment is hyphen-joined before "stream" (e.g.
    ``/api/services/{id}/realtime-stream``, mounted in
    backend/routers/control_room.py:269) does NOT end with the literal
    substring "/stream" — the character immediately before "stream" is
    "-", not "/" — so it is never classified as an SSE route at all. The
    SSE-allowlist gate (remote_access.py's
    ``if _is_sse_route(path) and path not in _ANALYST_SSE_ALLOWLIST``)
    never even fires for it, regardless of whether it's listed in
    ``_ANALYST_SSE_ALLOWLIST``.

    Today that happens to line up with the S-1 decision that
    ``/realtime-stream`` should be analyst-visible (see
    tests/routers/test_rbac_audit_fixes.py) — but the MECHANISM securing
    that decision is broken, not deliberate: the next hyphen-named SSE
    route (e.g. a hypothetical ``cost-governor-stream``) would silently
    inherit the same bypass whether or not anyone intended it to be
    analyst-reachable, because the default-closed gate can't see it
    either. Fix `_is_sse_route` to recognize any "*-stream"-suffixed
    final path segment (not just an exact "/stream" boundary) so the
    allowlist gate is actually consulted for every SSE-shaped route.
    """
    from backend.utils.remote_access import _is_sse_route

    # Real production route — backend/routers/control_room.py:269.
    assert _is_sse_route("/api/services/svc-A/realtime-stream") is True
    # Generic canary: the NEXT hyphenated "*-stream" route must also be
    # recognized, not just this one — otherwise each new one repeats the
    # same silent bypass of the default-closed SSE policy.
    assert _is_sse_route("/api/services/svc-A/cost-governor-stream") is True


# ── L5: max date-range span cap ──────────────────────────────────────────────


def test_clamp_max_span_caps_window():
    from backend.utils.remote_access import TimeBounds

    end = datetime(2026, 6, 17, tzinfo=UTC)
    start = end - timedelta(days=400)
    s, e = TimeBounds().clamp(start, end, max_span=timedelta(days=366))
    assert e == end
    assert e - s == timedelta(days=366)


def test_clamp_without_max_span_is_uncapped():
    from backend.utils.remote_access import TimeBounds

    end = datetime(2026, 6, 17, tzinfo=UTC)
    start = end - timedelta(days=400)
    s, e = TimeBounds().clamp(start, end)
    assert e - s == timedelta(days=400)


def test_clamp_or_400_caps_analyst_span_not_admin():
    from backend.utils.date_utils import parse_iso_utc
    from backend.utils.remote_access import MAX_ANALYST_QUERY_SPAN, TimeBounds, clamp_or_400

    end = datetime(2026, 6, 17, tzinfo=UTC)
    start = end - timedelta(days=400)
    # analyst → capped at MAX_ANALYST_QUERY_SPAN
    s, e = clamp_or_400(TimeBounds(), start.isoformat(), end.isoformat(), analyst_session=object())
    assert parse_iso_utc(e) - parse_iso_utc(s) == MAX_ANALYST_QUERY_SPAN
    # admin with explicit bounds → uncapped
    s2, e2 = clamp_or_400(TimeBounds(), start.isoformat(), end.isoformat(), analyst_session=None)
    assert parse_iso_utc(e2) - parse_iso_utc(s2) == timedelta(days=400)


# ── L6: analyst API rate limiter (parametrized + separate bucket) ────────────


def test_limiter_req_limit_is_parametrized():
    from backend.utils.remote_access import _StaticAssetLimiter

    lim = _StaticAssetLimiter(req_limit=2)
    assert lim.check("9.9.9.9", 0) is True
    assert lim.check("9.9.9.9", 0) is True
    assert lim.check("9.9.9.9", 0) is False  # 3rd exceeds req_limit=2
    assert lim.check("8.8.8.8", 0) is True  # a different IP is independent


def test_analyst_api_limiter_is_a_separate_bucket():
    from backend.utils.remote_access import _analyst_api_limiter, _static_limiter

    assert _static_limiter is not _analyst_api_limiter


# ── L7: OpenAPI surface blocked for analysts ─────────────────────────────────


def test_is_blocked_path_blocks_openapi_surface():
    from backend.utils.remote_access import _is_blocked_path

    for p in ("/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"):
        assert _is_blocked_path(p) is True, p
    assert _is_blocked_path("/") is False


# ── PII boundary: the IP-family set is one source of truth across the stack ───


@pytest.mark.security_regression
def test_ip_family_set_single_source_of_truth_across_stack():
    """The analyst filter-lock, the response masker, and the frontend
    drill-down hide list MUST all reference the same IP-family field set.

    If they drift, an analyst could be denied filtering on one alias while
    another alias still leaks raw IPs (mask-in-response but filter-allowed,
    or vice-versa). The two backend uses are the same object; the frontend
    copy crosses the Python/TS boundary so it is pinned by parsing the real
    ``lib/pii.ts`` literal (not a hand-copied list, which would itself drift).
    """
    import re
    from pathlib import Path

    from backend.core.share_db import validation
    from backend.utils.remote_access import _PII_FORBIDDEN_FILTER_COLS

    # Backend: the filter-lock is the UNION of the PII families the response
    # masker redacts — the IP family (masked via mask_ip) and the session-id
    # family (cookie_session, redacted wholesale, Phase-4 Track C). Both come
    # from validation.py so the filter-lock and the response masker can't drift
    # (mask-in-response but filter-allowed, or vice-versa).
    assert _PII_FORBIDDEN_FILTER_COLS == validation.IP_FAMILY_KEYS | validation.SESSION_ID_KEYS
    assert validation.IP_FAMILY_KEYS <= _PII_FORBIDDEN_FILTER_COLS
    assert validation.SESSION_ID_KEYS <= _PII_FORBIDDEN_FILTER_COLS

    # Frontend: parse the actual IP_FAMILY_FIELDS literal from lib/pii.ts.
    repo_root = Path(__file__).resolve().parents[2]
    pii_ts = (repo_root / "frontend" / "lib" / "pii.ts").read_text(encoding="utf-8")
    m = re.search(r"IP_FAMILY_FIELDS\s*=\s*\[([^\]]*)\]", pii_ts)
    assert m is not None, "IP_FAMILY_FIELDS literal not found in frontend/lib/pii.ts"
    fe_fields = set(re.findall(r"""['"]([^'"]+)['"]""", m.group(1)))

    assert fe_fields == set(validation.IP_FAMILY_KEYS)


# ── insights clamp helpers: stable cache key + request-free resolver ──────────


def test_analyst_clamp_cache_key_is_param_based():
    from backend.utils.remote_access import analyst_clamp_cache_key

    assert analyst_clamp_cache_key(None, None, 24) == "||24"
    assert (
        analyst_clamp_cache_key("2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", None)
        == "2026-01-01T00:00:00+00:00|2026-01-02T00:00:00+00:00|"
    )


def test_resolve_analyst_insights_clamp_stable_key_rolling_bounds():
    """The cache key is identical across two anchors (keyed on invite params)
    while the resolved clamp bounds roll forward with the anchor — the crux of
    the analyst-prewarm fix."""
    from backend.utils.date_utils import parse_iso_utc
    from backend.utils.remote_access import resolve_analyst_insights_clamp

    t0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=5)
    cs0, ce0, k0 = resolve_analyst_insights_clamp(None, None, 24, baseline_hours=168.0, window_hours=1.0, now=t0)
    cs1, ce1, k1 = resolve_analyst_insights_clamp(None, None, 24, baseline_hours=168.0, window_hours=1.0, now=t1)
    assert k0 == k1 == "||24"  # stable key
    assert ce0 != ce1  # end rolled with the anchor
    assert parse_iso_utc(ce0) == t0 and parse_iso_utc(ce1) == t1
    # relative 24h window floors the start at anchor-24h
    assert parse_iso_utc(cs0) == t0 - timedelta(hours=24)


def test_resolve_analyst_insights_clamp_empty_window_raises():
    """A start floor after the end ceiling → empty clamp → ValueError so the
    prewarmer skips that shape."""
    from backend.utils.remote_access import resolve_analyst_insights_clamp

    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    qs = (now + timedelta(hours=1)).isoformat()  # start in the future
    qe = (now - timedelta(hours=1)).isoformat()  # end in the past
    with pytest.raises(ValueError):
        resolve_analyst_insights_clamp(qs, qe, None, baseline_hours=168.0, window_hours=1.0, now=now)


def test_time_bounds_from_params_matches_request_dep():
    """``get_analyst_time_bounds`` delegates to ``_time_bounds_from_params``.

    A rolling-window invite ceilings its upper bound to the anchor (``now``),
    so the helper and the request dependency agree on a non-``None`` end.
    """
    from types import SimpleNamespace

    from backend.utils.remote_access import _time_bounds_from_params, get_analyst_time_bounds

    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    tb_helper = _time_bounds_from_params(None, None, 48, now=now)
    assert tb_helper.start == now - timedelta(hours=48)
    assert tb_helper.end == now  # rolling invite ceilings end to the anchor

    session = SimpleNamespace(query_start_time=None, query_end_time=None, query_window_hours=48)
    req = SimpleNamespace(state=SimpleNamespace(analyst_session=session))
    tb_req = get_analyst_time_bounds(req)
    assert tb_req.start is not None and tb_req.end is not None


@pytest.mark.security_regression
def test_rolling_invite_ceilings_future_req_end_to_anchor():
    """A rolling-window invite must not let a caller-supplied *future* ``req_end``
    widen the effective window forward.

    The wire-token path resolves ``req_end`` from a 60s-quantized anchor that can
    round up past ``now``; a skewed legacy client can also send a future end. The
    invite's upper bound is ceilinged to the anchor, so ``clamp`` returns at most
    the anchor no matter how far ahead the request reaches. (Past-only data means
    no rows actually leak, but the window must not silently widen.)
    """
    from backend.utils.remote_access import _time_bounds_from_params

    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    tb = _time_bounds_from_params(None, None, 24, now=now)  # rolling 24h invite

    # Caller reaches an hour into the future (skewed / round-up-quantized anchor).
    eff_start, eff_end = tb.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert eff_end == now  # ceilinged to the anchor, NOT now+1h
    assert eff_start == now - timedelta(hours=24)

    # Legacy in-range case (req_end at/under the anchor) is unchanged.
    _, e2 = tb.clamp(now - timedelta(hours=24), now - timedelta(minutes=5))
    assert e2 == now - timedelta(minutes=5)

    # Combo invite (explicit end cap + window): keep the more-restrictive end.
    tb_cap = _time_bounds_from_params(None, (now - timedelta(hours=2)).isoformat(), 24, now=now)
    _, e3 = tb_cap.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert e3 == now - timedelta(hours=2)  # past absolute cap beats the anchor

    # ...but a FUTURE explicit cap collapses to the anchor (never widens past now).
    tb_future = _time_bounds_from_params(None, (now + timedelta(days=365)).isoformat(), 24, now=now)
    _, e4 = tb_future.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert e4 == now
