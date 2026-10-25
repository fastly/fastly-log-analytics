"""Integration tests for RemoteAccessMiddleware, PII policy, admin gateway, route tripwire, and debug envelopes."""

from __future__ import annotations

import asyncio
import copy
import json
import re
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from backend.core import share_db
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, _mask_ip_scalar, apply_pii_policy, mask_ip
from backend.main import app as _main_app
from backend.repositories.utils.filters import normalize_filter_key
from backend.utils import tunnel
from backend.utils.remote_access import (
    _ANALYST_ALLOWED_WRITE_PREFIXES,
    _PII_FORBIDDEN_FILTER_COLS,
    _UNAUTH_ANALYST_PATHS,
    RemoteAccessMiddleware,
    _is_blocked_path,
    _strip_analyst_envelope,
    is_request_remote,
    validate_admin_gateway_config,
)
from tests.conftest import override_request_context

_SVC = "svcA"
_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_WRITE_HEADERS = {**_HEADERS, "Origin": "https://testserver"}
_MASK = {"mask_ips": True}
SECRET = "test-gateway-secret-with-at-least-32-characters"
HEADER = "X-Admin-Gateway-Token"
_ENVELOPE_PREFIXES = ("_debug", "debug_")


def _build_test_app() -> FastAPI:
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    get_routes = [
        "/api/dashboard",
        "/api/cron/stream",
        "/api/log-extents/stream",
        "/api/usage/summary",
        "/api/download",
        "/api/download-all",
        "/api/download-folder",
        "/api/download-foo",
        "/api/cron-schedule",
    ]
    for path in get_routes:
        app.add_api_route(path, lambda: {"ok": True}, methods=["GET"])

    post_routes = ["/api/views", "/api/dashboard/aggregates", "/api/web-vitals", "/api/ux-events"]
    for path in post_routes:
        app.add_api_route(path, lambda: {"ok": True}, methods=["POST"])

    @app.get("/api/cron-runs/{run_id}/stream")
    def _cron_run_stream(run_id: str):
        return {"ok": True}

    svc_get_routes = [
        "/api/services/{service_id}/scoring/status",
        "/api/services/{service_id}/realtime-stream",
        "/api/services/{service_id}/cost-governor-stream",
        "/api/alerts/{service_id}",
        "/api/custom-endpoint/{service_id}/data",
        "/api/services/{service_id}/rum/status",
        "/api/services/{service_id}/rum/versions",
        "/api/services/{service_id}/rum/beacon-health",
        "/api/services/{service_id}/rum/analytics",
        "/api/services/{service_id}/rum/live-events",
        "/api/services/{service_id}/lake-info",
        "/api/services/{service_id}/scoring/config",
        "/api/services/{service_id}/scoring/audit",
        "/api/services/{service_id}/scoring/threshold",
        "/api/services/{service_id}/scoring/exclude-regex",
        "/api/services/{service_id}/scoring/enforce-status-code",
        "/api/services/{service_id}/scoring/labels",
        "/api/services/{service_id}/scoring/top-flagged",
        "/api/services/{service_id}/scoring/threshold-preview",
    ]

    def _svc_handler(service_id: str):
        return {"ok": True, "service_id": service_id}

    for path in svc_get_routes:
        app.add_api_route(path, _svc_handler, methods=["GET"])

    for path in (
        "/api/services/{service_id}/rum/enable",
        "/api/services/{service_id}/rum/disable",
        "/api/services/{service_id}/rum/upgrade",
    ):
        app.add_api_route(path, _svc_handler, methods=["POST"])

    @app.get("/api/services/{service_id}/scoring/sessions/{sid}/events")
    def _scoring_sess_events(service_id: str, sid: str):
        return {"ok": True, "service_id": service_id, "sid": sid}

    return app


@pytest.fixture
def app():
    return _build_test_app()


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


def _seed_invite(service_ids=None, ip_whitelist=None, pii_policy=None) -> dict:
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids or ["svcA", "svcB"],
        pii_policy=pii_policy,
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
        headers={"X-Remote-Analyst": "1", "Host": host, "Origin": f"https://{host}"},
    )
    assert r.status_code == 200, r.text
    sid = ""
    for raw in r.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name in ("analyst_pending_session_id", "analyst_session_id") and val:
            sid = val
    client.cookies.set("analyst_session_id", sid)
    return sid


def _as_analyst(client, service_ids=None, pii_policy=None) -> str:
    _start_share()
    invite = _seed_invite(service_ids=service_ids, pii_policy=pii_policy)
    return _login_analyst(client, invite)


# ── DNS-rebinding & Local Pass-Through ─────────────────────────────────────


def test_host_header_and_local_passthrough_rules(client, monkeypatch):
    r = client.get("/api/dashboard", headers={"Host": "evil.com"})
    assert r.status_code == 400 and r.json()["error"] == "host_not_allowed"

    assert client.get("/api/dashboard").status_code == 200
    assert client.get("/api/admin/share/status").status_code == 200
    assert client.post("/api/views").status_code == 200

    _start_share()
    r2 = client.get("/api/dashboard", headers={"X-Remote-Analyst": "1", "Host": "random.lhr.life"})
    assert r2.status_code == 400 and r2.json()["error"] == "host_not_allowed"

    monkeypatch.setattr(
        "backend.config.list_configs",
        lambda: [
            {
                "service_id": "svc_test_rf",
                "remote_frontend": {"service_id": "fastly_test_123", "domain_name": "fla-test.global.ssl.fastly.net"},
            }
        ],
    )
    r3 = client.get("/api/dashboard", headers={"X-Remote-Analyst": "1", "Host": "fla-test.global.ssl.fastly.net"})
    assert r3.status_code == 401 and r3.json()["error"] == "unauthenticated"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/admin/clickhouse/status?service_id=svcA"),
        ("POST", "/api/admin/clickhouse/replay"),
    ],
)
def test_clickhouse_controls_block_authenticated_analyst(client, method, path):
    from backend.routers.admin import router

    client.app.include_router(router)
    _as_analyst(client)
    assert client.get("/api/dashboard?service_id=svcA", headers=_WRITE_HEADERS).status_code == 200
    response = client.request(
        method, path, headers=_WRITE_HEADERS, json={"service_id": "svcA", "dataset_id": "dataset"}
    )
    assert response.status_code == 403 and response.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_gateway_required_keeps_authenticated_analyst_restrictions(client, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    _as_analyst(client, service_ids=["svcA"])
    assert client.get("/api/dashboard", headers=_HEADERS, params={"service_id": "svcA"}).status_code == 200
    denied = client.get("/api/admin/share/status", headers=_HEADERS)
    assert denied.status_code == 403 and denied.json()["error"] == "admin_only"


# ── Analyst Path, SSE, Read-Only, and Scope Gates ──────────────────────────


def test_analyst_without_session_blocked(client):
    _start_share()
    r = client.get("/api/dashboard", headers=_HEADERS)
    assert r.status_code == 401 and r.json()["error"] == "unauthenticated"


@pytest.mark.parametrize(
    "path,expected_status,expected_error",
    [
        ("/api/admin/share/status", 403, "admin_only"),
        ("/api/cron/stream", 403, "sse_blocked"),
        ("/api/services/svcA/realtime-stream", 200, None),
        ("/api/services/svcA/cost-governor-stream", 403, "sse_blocked"),
        ("/api/admin/events/stream?channels=sync-status,cron-runs,system-metrics", 403, "admin_only"),
        ("/api/alerts/svcB", 403, "admin_only"),
        ("/api/usage/summary", 403, "admin_only"),
        ("/api/download", 403, "admin_only"),
        ("/api/download-all", 403, "admin_only"),
        ("/api/download-folder", 403, "admin_only"),
        ("/api/download-foo", 403, "service_not_authorized"),
        ("/api/cron-schedule", 403, "admin_only"),
        ("/api/services/svcA/lake-info", 403, "admin_only"),
    ],
)
def test_analyst_route_blocks_and_allows(client, path, expected_status, expected_error):
    _as_analyst(client, service_ids=["svcA"])
    r = client.get(path, headers=_HEADERS)
    assert r.status_code == expected_status, f"{path}: {r.status_code} {r.text}"
    if expected_error:
        assert r.json()["error"] == expected_error


def test_analyst_read_only_blocks_writes_and_mutating_verbs(client):
    _as_analyst(client, service_ids=["svcA"])
    r = client.post("/api/views", headers=_WRITE_HEADERS)
    assert r.status_code == 403 and r.json()["error"] == "read_only"

    for method in ("put", "patch", "delete"):
        r2 = getattr(client, method)("/api/dashboard/some-mutating-endpoint?service=svcA", headers=_WRITE_HEADERS)
        assert r2.status_code == 403 and r2.json()["error"] == "read_only"


def test_analyst_service_scope_enforcement(client):
    _as_analyst(client, service_ids=["svcA"])
    r_unauth = client.get("/api/dashboard?service=svcZ", headers=_HEADERS)
    assert r_unauth.status_code == 403 and r_unauth.json()["error"] == "service_not_authorized"
    assert client.get("/api/dashboard?service=svcA", headers=_HEADERS).status_code == 200

    with patch("backend.config.get_active_service_id", return_value="svcB"):
        r_omitted = client.get("/api/dashboard", headers=_HEADERS)
    assert r_omitted.status_code == 403 and r_omitted.json()["error"] == "service_not_authorized"

    with patch("backend.config.get_active_service_id", return_value="svcA"):
        r_path = client.get("/api/services/svcB/scoring/labels", headers=_HEADERS)
    assert r_path.status_code == 403 and r_path.json()["service"] == "svcB"

    r_both = client.get("/api/services/svcB/scoring/labels?service=svcA", headers=_HEADERS)
    assert r_both.status_code == 403 and r_both.json()["error"] == "service_not_authorized"

    r_desync = client.get("/api/custom-endpoint/svcB/data?service=svcA", headers=_HEADERS)
    assert r_desync.status_code == 403 and r_desync.json()["error"] == "service_not_authorized"
    assert client.get("/api/custom-endpoint/svcA/data?service=svcA", headers=_HEADERS).status_code == 200


def test_analyst_path_param_service_allowed_when_authorized(client):
    _as_analyst(client, service_ids=["svcA", "svcB"])
    r = client.get("/api/services/svcB/scoring/labels", headers=_HEADERS)
    assert r.status_code == 200 and r.json()["service_id"] == "svcB"


@pytest.mark.parametrize(
    "suffix", ["/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code"]
)
def test_analyst_blocked_from_scoring_admin_suffix(client, suffix):
    _as_analyst(client, service_ids=["svcA"])
    r = client.get(f"/api/services/svcA/scoring{suffix}", headers=_HEADERS)
    assert r.status_code == 403 and r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svcA/scoring/labels",
        "/api/services/svcA/scoring/sessions/sess-1/events",
        "/api/services/svcA/scoring/top-flagged",
        "/api/services/svcA/scoring/threshold-preview",
        "/api/services/svcA/rum/status",
        "/api/services/svcA/rum/beacon-health",
        "/api/services/svcA/rum/analytics",
        "/api/services/svcA/rum/live-events",
    ],
)
def test_analyst_not_blocked_from_scoring_and_rum_reads_they_need(client, path):
    _as_analyst(client, service_ids=["svcA"])
    r = client.get(path, headers=_HEADERS)
    assert r.status_code == 200, f"{path}: {r.status_code} {r.text}"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/api/services/svcA/rum/enable"),
        ("POST", "/api/services/svcA/rum/disable"),
        ("GET", "/api/services/svcA/rum/versions"),
        ("POST", "/api/services/svcA/rum/upgrade"),
    ],
)
def test_analyst_blocked_from_rum_admin_suffix(client, method, path):
    _as_analyst(client, service_ids=["svcA"])
    r = client.request(method, path, headers=_WRITE_HEADERS)
    assert r.status_code == 403 and r.json()["error"] == "admin_only"


# ── PII Lock & Masking ─────────────────────────────────────────────────────


def _post_filter(client, *, filters):
    return client.post(
        "/api/dashboard/aggregates", json={"service_id": "svcA", "filters": filters}, headers=_WRITE_HEADERS
    )


def _post_field(client, *, field):
    return client.post("/api/dashboard/aggregates", json={"service_id": "svcA", "field": field}, headers=_WRITE_HEADERS)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field",
    [
        "ip",
        "ip.",
        "ip ",
        "client_ip.",
        "cookie_session",
        "cookie_session.",
        "cookie_session ",
        "IP",
        "Cookie_Session",
        "Client_IP",
    ],
)
def test_analyst_masking_blocks_pii_field_values_enumeration(client, field):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_field(client, field=field)
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"


@pytest.mark.security_regression
def test_analyst_masking_allows_non_pii_field_values_enumeration(client):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    assert _post_field(client, field="url").status_code == 200


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["ip", "filter_ip", "xfilter_ip", "ip_2", "client_ip"])
def test_analyst_masking_blocks_ip_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["1.2.3.4"]}})
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"
    assert r.json()["field"] == ("client_ip" if key == "client_ip" else "ip")


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["cookie_session", "filter_cookie_session", "cookie_session_2"])
def test_analyst_masking_blocks_cookie_session_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"
    assert r.json()["field"] == "cookie_session"


@pytest.mark.security_regression
def test_analyst_masking_allows_non_ip_filter(client):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    assert _post_filter(client, filters={"country": {"mode": "include", "values": ["US"]}}).status_code == 200


@pytest.mark.security_regression
def test_analyst_masking_allows_oip_filter(client):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    assert _post_filter(client, filters={"oip": {"mode": "include", "values": ["151.101.1.51"]}}).status_code == 200


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "key",
    [
        "cookie_session.",
        "cookie_session ",
        "cookie_session\t",
        "cookie_session%",
        "cookie_session-",
        "filter_cookie_session.",
        "ip.",
        "ip ",
        "client_ip.",
    ],
)
def test_analyst_masking_blocks_safecol_evasion_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"


def test_analyst_without_masking_allows_ip_filter(client):
    _as_analyst(client, service_ids=["svcA"], pii_policy={"mask_ips": False})
    assert _post_filter(client, filters={"ip": {"mode": "include", "values": ["1.2.3.4"]}}).status_code == 200


# ── Origin Gate, Login Rate Limiter, Hardening Headers, Idle Timer ─────────


def test_origin_rate_limit_and_hardening_headers(client):
    _start_share()
    invite = _seed_invite()
    r_origin = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://attacker.example.com"},
    )
    assert r_origin.status_code == 403 and r_origin.json()["error"] == "origin_not_allowed"

    r_ok = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers=_WRITE_HEADERS,
    )
    assert r_ok.headers.get("cache-control") == "private, no-store"
    assert r_ok.headers.get("x-frame-options") == "DENY"
    assert r_ok.headers.get("referrer-policy") == "no-referrer"

    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        client.post(
            "/api/share/login",
            json={"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"},
            headers=_WRITE_HEADERS,
        )
    r_rl = client.post(
        "/api/share/login",
        json={"email": "drew@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers=_WRITE_HEADERS,
    )
    assert r_rl.status_code == 429 and r_rl.json()["detail"]["error"] == "rate_limited"


@pytest.mark.security_regression
def test_share_login_brute_force_lockout_shape_matches_e2e(client):
    _start_share()
    statuses, first_429 = [], None
    for i in range(35):
        r = client.post(
            "/api/share/login",
            json={"email": "attacker@example.com", "passcode": f"wrong-{i}"},
            headers=_WRITE_HEADERS,
        )
        statuses.append(r.status_code)
        if r.status_code == 429 and first_429 is None:
            first_429 = r.json()["detail"]
    assert statuses[: tunnel.LOGIN_FAILURE_THRESHOLD] == [401] * tunnel.LOGIN_FAILURE_THRESHOLD
    assert set(statuses[tunnel.LOGIN_FAILURE_THRESHOLD :]) == {429}
    assert first_429 is not None and first_429["error"] == "rate_limited"
    assert isinstance(first_429["retry_after_s"], int) and 0 < first_429["retry_after_s"] <= tunnel.LOGIN_LOCKOUT_S


@pytest.mark.parametrize(
    "method,path,extra_headers,body,expect_touch",
    [
        ("GET", "/api/dashboard?service=svcA", {}, None, True),
        ("GET", "/api/dashboard?service=svcA", {"X-User-Active": "1"}, None, True),
        ("GET", "/api/dashboard?service=svcA", {"X-User-Active": "0"}, None, False),
        ("GET", "/api/log-extents/stream?service=svcA", {}, None, False),
        (
            "POST",
            "/api/web-vitals?service=svcA",
            {"Origin": "https://testserver"},
            {"id": "x", "name": "LCP", "value": 1.0, "rating": "good"},
            False,
        ),
        (
            "POST",
            "/api/ux-events?service=svcA",
            {"Origin": "https://testserver"},
            {"id": "x", "name": "LCP", "value": 1.0, "rating": "good"},
            False,
        ),
    ],
)
def test_session_touch_and_exemptions(client, monkeypatch, method, path, extra_headers, body, expect_touch):
    _as_analyst(client, service_ids=["svcA"])
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    r = client.request(method, path, json=body, headers={**_HEADERS, **extra_headers})
    assert r.status_code != 403, f"{path}: {r.text}"
    assert any("last_activity" in kw for kw in calls) is expect_touch


@pytest.mark.security_regression
def test_dispatch_idle_timeout_returns_401(client):
    sid = _as_analyst(client, service_ids=["svcA"])
    mgr = tunnel.get_tunnel_manager()
    session = mgr._sessions.get(sid)
    assert session is not None
    session.last_active_time = "2020-01-01T00:00:00Z"
    r = client.get("/api/dashboard?service=svcA", headers=_HEADERS)
    assert r.status_code == 401 and r.json()["error"] == "unauthenticated"


@pytest.mark.security_regression
def test_dispatch_analyst_post_to_non_allowlisted_path_returns_403(client):
    _as_analyst(client, service_ids=["svcA"])
    r = client.post("/api/views", headers=_WRITE_HEADERS)
    assert r.status_code == 403 and r.json()["error"] == "read_only"


@pytest.mark.security_regression
def test_unauthenticated_telemetry_beacon_allowed_without_session(client):
    _start_share()
    client.cookies.clear()
    for path, payload in (
        ("/api/web-vitals?service=svcA", {"id": "unauth-1", "name": "LCP", "value": 1.0, "rating": "good"}),
        ("/api/ux-events?service=svcA", {"event": "some_ui_action", "pathname": "/share-login", "details": {}}),
    ):
        r = client.post(path, json=payload, headers=_WRITE_HEADERS)
        assert r.status_code == 200 and r.json().get("ok") is True


# ── Merged from test_pii_policy_evasion.py & test_mask_ip_scalar_padding.py ─


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field", ["ip", "ip.", "ip ", "ip\t", "client_ip.", "remote_addr.", "IP", "Ip", "Client_IP", "IP."]
)
def test_apply_pii_policy_masks_ip_field_values_despite_junk_suffix(field):
    obj = {"field": field, "values": [{"value": "203.0.113.7", "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    got = out["values"][0]["value"]
    assert got != "203.0.113.7" and got.endswith("xxx"), (field, got)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field",
    [
        "cookie_session",
        "cookie_session.",
        "cookie_session ",
        "cookie_session\t",
        "cookie_session%",
        "Cookie_Session",
        "COOKIE_SESSION",
        "Cookie_Session.",
    ],
)
def test_apply_pii_policy_redacts_session_field_values_despite_junk_suffix(field):
    obj = {"field": field, "values": [{"value": "a" * 64, "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    assert out["values"][0]["value"] == "[redacted]", (field, out)


@pytest.mark.security_regression
def test_apply_pii_policy_leaves_non_pii_field_values_verbatim():
    obj = {"field": "url", "values": [{"value": "/index.html", "count": 9}]}
    assert apply_pii_policy(copy.deepcopy(obj), _MASK)["values"][0]["value"] == "/index.html"


@pytest.mark.security_regression
def test_apply_pii_policy_noop_without_mask_ips():
    obj = {"field": "ip", "values": [{"value": "203.0.113.7", "count": 1}]}
    assert apply_pii_policy(copy.deepcopy(obj), {})["values"][0]["value"] == "203.0.113.7"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "key",
    [
        "ip",
        "ip.",
        "ip ",
        "client_ip.",
        "filter_ip",
        "ip_2",
        "cookie_session",
        "cookie_session.",
        ".cookie_session",
        "filter_cookie_session.",
        "IP",
        "Ip",
        "Client_IP",
        "Cookie_Session",
        "COOKIE_SESSION",
        "filter_Cookie_Session.",
    ],
)
def test_normalize_filter_key_resolves_junk_pii_keys_into_forbidden_set(key):
    assert normalize_filter_key(key) in _PII_FORBIDDEN_FILTER_COLS, key


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["url", "status", "country", "oip", "waf_sig_ind", "_bot_name"])
def test_normalize_filter_key_leaves_non_pii_keys_allowed(key):
    assert normalize_filter_key(key) not in _PII_FORBIDDEN_FILTER_COLS, key
    assert _PII_FORBIDDEN_FILTER_COLS == IP_FAMILY_KEYS | SESSION_ID_KEYS


@pytest.mark.security_regression
@pytest.mark.parametrize("padded", ["  1.2.3.4", "1.2.3.4\n", "\t1.2.3.4 \r\n", " 2001:db8::1 "])
def test_padded_ip_is_masked_and_padding_is_not_preserved(padded):
    out = _mask_ip_scalar(padded)
    assert out == mask_ip(padded.strip()) and out == out.strip()


@pytest.mark.security_regression
def test_xff_list_is_normalised_to_comma_space_separated_masked_elements():
    expected = ", ".join([mask_ip("1.2.3.4"), mask_ip("5.6.7.8")])
    assert _mask_ip_scalar("1.2.3.4,5.6.7.8") == expected
    assert _mask_ip_scalar(" 1.2.3.4 ,\n5.6.7.8 ") == expected


@pytest.mark.security_regression
def test_non_ip_values_keep_their_padding_verbatim():
    assert _mask_ip_scalar("  hello world\n") == "  hello world\n"
    assert _mask_ip_scalar("1.2.3.4, not-an-ip ") == "1.2.3.4, not-an-ip "


# ── Merged from test_admin_gateway.py ──────────────────────────────────────


def _request(peer: str, headers: dict[str, str] | None = None) -> StarletteRequest:
    return StarletteRequest(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/bootstrap",
            "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
            "client": (peer, 1234),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }
    )


@pytest.fixture
def gateway_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    manager = MagicMock()
    manager.is_sharing_active.return_value = False
    manager.validate_session.return_value = None
    monkeypatch.setattr("backend.utils.remote_access.get_tunnel_manager", lambda: manager)
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)

    @app.get("/api/admin/probe")
    def probe(request: Request) -> dict[str, bool]:
        return {"is_remote": request.state.is_remote}

    @app.get("/api/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/api/admin/probe")
    def mutate() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/admin/recheck")
    def recheck(request: Request) -> dict[str, bool]:
        return {"is_remote": is_request_remote(request), "credential_visible": HEADER in request.headers}

    with TestClient(app) as c:
        yield c


@pytest.mark.security_regression
@pytest.mark.parametrize("peer", ["127.0.0.1", "10.12.1.2", "203.0.113.1"])
def test_valid_gateway_authenticates_independently_of_peer(monkeypatch, peer):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    assert not is_request_remote(_request(peer, {HEADER: SECRET}))


@pytest.mark.security_regression
@pytest.mark.parametrize("peer", ["127.0.0.1", "10.12.1.2", "203.0.113.1"])
def test_required_gateway_disables_ip_based_admin(monkeypatch, peer):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "10.12.0.0/16")
    assert is_request_remote(_request(peer))


@pytest.mark.security_regression
@pytest.mark.parametrize("supplied", ["forged", "", "non-ascii-\u00e9"])
def test_wrong_gateway_token_does_not_authenticate(monkeypatch, supplied):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    assert is_request_remote(_request("127.0.0.1", {HEADER: supplied}))


@pytest.mark.security_regression
def test_short_or_missing_configured_secret_cannot_authenticate(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "short")
    assert is_request_remote(_request("127.0.0.1", {HEADER: "short"}))
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET")
    assert is_request_remote(_request("127.0.0.1", {HEADER: SECRET}))


def test_gateway_config_and_loopback_behaviors(monkeypatch, gateway_client):
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    assert not is_request_remote(_request("127.0.0.1"))
    assert is_request_remote(_request("203.0.113.1"))

    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "short")
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    validate_admin_gateway_config()
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "true")
    with pytest.raises(ValueError, match="must be 0 or 1"):
        validate_admin_gateway_config()

    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    response = gateway_client.get("/api/admin/recheck", headers={HEADER: SECRET})
    assert response.status_code == 200 and response.json() == {"is_remote": False, "credential_visible": False}
    assert gateway_client.get("/api/health").json() == {"ok": True}
    assert (
        gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": "https://testserver"}).status_code
        == 200
    )
    assert gateway_client.post("/api/admin/probe", headers={HEADER: SECRET}).status_code == 200


@pytest.mark.security_regression
def test_forged_gateway_header_is_rejected_even_on_loopback(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: "forged"})
    assert response.status_code == 401 and response.json() == {"error": "admin_gateway_invalid"}


@pytest.mark.security_regression
def test_uncredentialed_loopback_cannot_reach_admin_in_required_mode(gateway_client):
    response = gateway_client.get("/api/admin/probe")
    assert response.status_code == 401 and response.json()["error"] == "unauthenticated"


@pytest.mark.security_regression
def test_public_marker_cannot_be_promoted_with_gateway_token(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "X-Proxied-By-Caddy": "1"})
    assert response.status_code == 401 and response.json()["error"] == "admin_gateway_invalid"


@pytest.mark.security_regression
def test_duplicate_gateway_headers_are_rejected(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers=[(HEADER, SECRET), (HEADER, "forged")])
    assert response.status_code == 401


@pytest.mark.security_regression
@pytest.mark.parametrize("origin", ["https://attacker.example", "null", "https://testserver.attacker.example"])
def test_gateway_blocks_cross_origin_browser_mutations(gateway_client, origin):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": origin})
    assert response.status_code == 403 and response.json()["error"] == "admin_origin_not_allowed"


@pytest.mark.security_regression
def test_gateway_rejects_browser_cross_site_fetch_metadata(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "Sec-Fetch-Site": "cross-site"})
    assert response.status_code == 403


# ── Merged from test_route_table_tripwire.py ───────────────────────────────

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")
_PATH_PARAM_SUBSTITUTIONS = {
    "service_id": "svc1",
    "domain": "rum_vitals",
    "export_id": "export1",
    "job_id": "job1",
    "alert_id": "alert1",
    "invite_id": "invite1",
    "session_id": "sess1",
    "quarantine_id": "quar1",
    "source_id": "src1",
    "qid": "q1",
    "log_id": "1",
    "run_id": "run1",
    "field_name": "field1",
    "label_id": "label1",
    "item_id": "1",
    "version": "v1",
    "sid": "sid1",
    "view_id": "view1",
    "tab": "overview",
    "token": "tok1",
}
_PARAM_RE = re.compile(r"\{([^}]+)\}")


def _substitute(path: str) -> str:
    def _rep(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in _PATH_PARAM_SUBSTITUTIONS:
            raise AssertionError(f"route path parameter {name!r} (from {path!r}) missing in _PATH_PARAM_SUBSTITUTIONS")
        return _PATH_PARAM_SUBSTITUTIONS[name]

    return _PARAM_RE.sub(_rep, path)


def _real_routes() -> list[tuple[str, str]]:
    paths = _main_app.openapi()["paths"]
    return sorted((m.upper(), p) for p in paths for m in _HTTP_METHODS if m in paths[p])


_READ_ALLOWLIST: set[tuple[str, str]] = {
    ("GET", "/api/health"),
    ("GET", "/api/bootstrap"),
    ("GET", "/api/share/auth-config"),
    ("GET", "/api/share/heartbeat"),
    ("GET", "/api/share/tos"),
    ("GET", "/api/share/oauth/authorize"),
    ("GET", "/api/share/oauth/callback"),
    ("GET", "/js/rum.js"),
    ("GET", "/js/faro-sdk.js"),
    ("GET", "/api/log-fields/catalog"),
    ("GET", "/api/presets"),
    ("GET", "/api/schema"),
    ("GET", "/api/services"),
    ("GET", "/api/insight-availability"),
    ("GET", "/api/views/{service_id}"),
    ("GET", "/api/log-extents"),
    ("GET", "/api/log-extents/stream"),
    ("GET", "/api/services/{service_id}/scoring/analytics"),
    ("GET", "/api/services/{service_id}/scoring/compliance-breakdown"),
    ("GET", "/api/services/{service_id}/scoring/curves"),
    ("GET", "/api/services/{service_id}/scoring/evaluation"),
    ("GET", "/api/services/{service_id}/scoring/health"),
    ("GET", "/api/services/{service_id}/scoring/labels"),
    ("GET", "/api/services/{service_id}/scoring/latency-timeseries"),
    ("GET", "/api/services/{service_id}/scoring/score-distribution"),
    ("GET", "/api/services/{service_id}/scoring/sessions/{sid}/events"),
    ("GET", "/api/services/{service_id}/scoring/threshold-preview"),
    ("GET", "/api/services/{service_id}/scoring/top-flagged"),
    ("GET", "/api/services/{service_id}/rum/status"),
    ("GET", "/api/services/{service_id}/rum/beacon-health"),
    ("GET", "/api/services/{service_id}/rum/analytics"),
    ("GET", "/api/services/{service_id}/rum/live-events"),
    ("GET", "/api/services/{service_id}/realtime-stream"),
    ("GET", "/api/services/{service_id}/control-room/error-stream"),
    ("GET", "/api/security/proxies/export"),
    ("GET", "/api/assets/aggregates"),
    ("GET", "/api/network/bundle"),
    ("GET", "/api/network/pop-health"),
    ("GET", "/api/security/bundle"),
    ("GET", "/api/security/threat-intel"),
    ("GET", "/api/query/schema/{service_id}"),
    ("GET", "/api/high-scale/request-facts"),
    ("GET", "/api/high-scale/services/{service_id}/exports/{export_id}"),
    ("GET", "/api/high-scale/services/{service_id}/queries/{job_id}"),
    ("GET", "/api/services/{service_id}/cmcd/status"),
    ("GET", "/api/services/{service_id}/control-room/wizard/state"),
    ("GET", "/api/services/{service_id}/control-room/{tab}"),
    ("GET", "/api/services/{service_id}/realtime-seed"),
}

_WRITE_VERB_GATE_ROUTES: set[tuple[str, str]] = {
    ("POST", "/api/share/acknowledge"),
    ("POST", "/api/share/claim/{token}"),
    ("POST", "/api/share/login"),
    ("POST", "/api/share/logout"),
    ("POST", "/api/assets/aggregates"),
    ("POST", "/api/dashboard/aggregates"),
    ("POST", "/api/dashboard/bundle"),
    ("POST", "/api/dashboard/field-values"),
    ("POST", "/api/dashboard/raw/csv"),
    ("POST", "/api/security/aggregates"),
    ("POST", "/api/origin/aggregates"),
    ("POST", "/api/origin/ip-health"),
    ("POST", "/api/origin/path-breakdown"),
    ("POST", "/api/origin/pop-latency"),
    ("POST", "/api/origin/shielding-analysis"),
    ("POST", "/api/origin/slow-urls"),
    ("POST", "/api/origin/status-codes"),
    ("POST", "/api/origin/summary"),
    ("POST", "/api/origin/timeseries"),
    ("POST", "/api/performance/aggregates"),
    ("POST", "/api/insights"),
    ("POST", "/api/insights/cache-collapse-detail"),
    ("POST", "/api/network-health"),
    ("POST", "/api/network-quality"),
    ("POST", "/api/network/bundle"),
    ("POST", "/api/origin/drilldown"),
    ("POST", "/api/query"),
    ("POST", "/api/security/bundle"),
    ("POST", "/api/sessions"),
    ("POST", "/api/sessions/detail"),
    ("POST", "/api/cmcd/aggregates"),
    ("POST", "/api/value/summary"),
    ("POST", "/api/web-vitals"),
    ("POST", "/api/ux-events"),
    ("POST", "/api/high-scale/request-facts"),
    ("POST", "/api/high-scale/services/{service_id}/request-facts"),
    ("POST", "/api/high-scale/services/{service_id}/rum-facts/{domain}"),
    ("POST", "/api/high-scale/services/{service_id}/cmcd-facts"),
    ("POST", "/api/high-scale/services/{service_id}/aggregates"),
    ("POST", "/api/high-scale/services/{service_id}/queries"),
    ("POST", "/api/high-scale/services/{service_id}/queries/{job_id}/cancel"),
    ("POST", "/api/high-scale/services/{service_id}/exports"),
    ("POST", "/api/high-scale/services/{service_id}/exports/{export_id}/cancel"),
}

_ALL_INVENTORIED = _READ_ALLOWLIST | _WRITE_VERB_GATE_ROUTES


def _is_unauth_path(path: str) -> bool:
    return path in _UNAUTH_ANALYST_PATHS or path.startswith("/api/share/claim/") or path.startswith("/api/share/oauth/")


def _verb_gate_blocks(method: str, substituted_path: str) -> bool:
    if method in ("PUT", "PATCH", "DELETE"):
        return True
    if method == "POST":
        if _is_unauth_path(substituted_path):
            return False
        return not any(substituted_path.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES)
    return False


@pytest.mark.security_regression
def test_every_real_route_is_blocked_or_inventoried():
    uninventoried = [
        f"{m} {p}"
        for m, p in _real_routes()
        if not _is_blocked_path(_substitute(p))
        and not _verb_gate_blocks(m, _substitute(p))
        and (m, p) not in _ALL_INVENTORIED
    ]
    assert not uninventoried, f"Unclassified analyst-reachable route(s): {uninventoried}"


@pytest.mark.security_regression
def test_inventory_has_no_stale_entries():
    real = set(_real_routes())
    stale = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if (m, p) not in real]
    assert not stale, f"Stale inventory entries: {stale}"
    now_blocked = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if _is_blocked_path(_substitute(p))]
    assert not now_blocked, f"Now-blocked inventory entries: {now_blocked}"


@pytest.mark.security_regression
def test_write_gate_routes_actually_rely_on_the_verb_gate():
    for method, path in _WRITE_VERB_GATE_ROUTES:
        assert method == "POST"
        substituted = _substitute(path)
        assert not _is_blocked_path(substituted)
        assert any(substituted.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES)


@pytest.mark.security_regression
def test_unclassified_route_fails_the_tripwire():
    dummy_path = "/api/__tripwire_dummy_unclassified_route__"

    async def _dummy_handler():
        return {"ok": True}

    _main_app.router.add_api_route(dummy_path, _dummy_handler, methods=["GET"])
    _main_app.openapi_schema = None
    try:
        paths = _main_app.openapi()["paths"]
        assert dummy_path in paths
        found = any(
            m in paths[dummy_path]
            and not _is_blocked_path(dummy_path)
            and not _verb_gate_blocks(m.upper(), dummy_path)
            and (m.upper(), dummy_path) not in _ALL_INVENTORIED
            for m in _HTTP_METHODS
        )
        assert found
    finally:
        _main_app.router.routes[:] = [r for r in _main_app.router.routes if getattr(r, "path", None) != dummy_path]
        _main_app.openapi_schema = None


# ── Merged from test_rum_debug_envelope.py & test_debug_envelope_sites.py ──


def _rum_app(*, with_telemetry: bool = False) -> FastAPI:
    from backend.core.request_context import build_request_context
    from backend.routers import rum, share_auth
    from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    if with_telemetry:
        app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(share_auth.router)
    app.include_router(rum.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    return app


@pytest.fixture(autouse=True)
def _reset_debug_visibility_settings():
    yield
    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "disabled")


@pytest.fixture
def analyst_client():
    with TestClient(_rum_app()) as c:
        _as_analyst(c, service_ids=[_SVC])
        yield c


class _Holder:
    def __init__(self, *_a, **_k):
        pass

    def __enter__(self):
        return object()

    def __exit__(self, *_a):
        return False


def _beacon_health_disabled(client):
    return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _beacon_health_check_failed(client):
    with (
        patch("backend.routers.rum.svcconfig.load_config", return_value={"rum": {"enabled": True}}),
        patch("backend.routers.rum.rum_source_for", side_effect=RuntimeError("boom")),
    ):
        return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _analytics_no_data(client):
    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value={"no_data": True}),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


def _analytics_success(client):
    db_res = {
        "no_data": False,
        "total_beacons": 7,
        "pageviews": 5,
        "interactions": 1,
        "errors_count": 1,
        "vitals_rows": [("LCP", 2100.0, 10, 8, 1, 1)],
        "worst_pages_rows": [("/p", 4, 120.0, 2100.0, 0.05, 0.1)],
        "errors_rows": [("boom", "app.js", 3, 4, 2)],
        "trends_rows": [],
        "browsers": {"Firefox": 7},
        "os": {"macOS": 7},
        "devices": {"desktop": 7},
    }
    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value=db_res),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_HS_ANALYTICS = {
    "is_mock": False,
    "no_data": False,
    "beacon_count": 3,
    "pageview_count": 3,
    "interaction_count": 0,
    "error_count": 0,
    "vitals": {
        "lcp": {"p75": 2.1, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
        "cls": {"p75": 0.05, "distribution": {"good": 90, "needs_improvement": 8, "poor": 2}},
        "inp": {"p75": 180, "distribution": {"good": 85, "needs_improvement": 10, "poor": 5}},
        "fid": {"p75": 12.0, "fcp": 1.1, "ttfb": 0.4},
    },
    "worst_pages": [],
    "errors": [],
    "trends": {
        "timestamps": ["2026-10-17T00:00:00Z"],
        "lcp": [2.1],
        "cls": [0.05],
        "error_rate": [0.0],
        "pageviews": [3],
        "interactions": [0],
        "errors": [0],
    },
    "environments": {"browsers": {}, "os": {}, "devices": {}},
}


def _hs_beacon_health(client):
    class _Client:
        def execute(self, sql, params=None):
            return [{"c": 3}]

    class _Service:
        service_id = _SVC
        client = _Client()

    with patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs:
        hs.return_value = _Service()
        return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _hs_analytics(client):
    with (
        patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs,
        patch("backend.high_scale.rum.rum_analytics", return_value=dict(_HS_ANALYTICS)),
    ):
        hs.return_value = object()
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_ROUTES = [
    _beacon_health_disabled,
    _beacon_health_check_failed,
    _analytics_no_data,
    _analytics_success,
    _hs_beacon_health,
    _hs_analytics,
]


def _envelope_keys(body: dict) -> list[str]:
    return [k for k in body if k.startswith(_ENVELOPE_PREFIXES)]


@pytest.mark.security_regression
@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_analyst_rum_response_has_no_debug_envelope_by_default(analyst_client, call) -> None:
    resp = call(analyst_client)
    assert resp.status_code == 200 and _envelope_keys(resp.json()) == []


@pytest.mark.security_regression
@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_share_setting_opens_only_the_matching_envelope_keys(analyst_client, call) -> None:
    share_db.set_setting("query_debug_visibility", "analysts")
    body = call(analyst_client).json()
    assert "_debug_queries" in body and "_debug_calls" not in body

    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "both")
    body = call(analyst_client).json()
    assert "_debug_calls" in body and "_debug_queries" not in body and "_debug_sqlite" not in body


@pytest.mark.security_regression
def test_admin_rum_response_still_carries_the_envelope() -> None:
    with TestClient(_rum_app()) as c:
        body = c.get(f"/api/services/{_SVC}/rum/beacon-health").json()
    assert {"_debug_queries", "_debug_calls"} <= set(body)


@pytest.mark.parametrize("audience", ["admin", "both", "query", "api", "disabled"])
@pytest.mark.parametrize(
    ("master_enabled", "opt_in", "force_include"),
    [
        (False, False, False),
        (False, True, False),
        (False, False, True),
        (True, False, False),
        (True, True, False),
        (True, False, True),
    ],
)
def test_rum_pre_attached_envelope_respects_master_switch(
    monkeypatch, audience: str, master_enabled: bool, opt_in: bool, force_include: bool
) -> None:
    from backend.utils import telemetry

    monkeypatch.setenv("DEBUG_RESPONSES", "1" if master_enabled else "")
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "1" if force_include else "")
    queries = [{"sql": "SELECT 1", "time_ms": 0.1}]
    calls = [{"method": "GET", "path": "/example"}]
    monkeypatch.setattr(telemetry, "get_queries", lambda: queries)
    monkeypatch.setattr(telemetry, "get_sqlite_queries", lambda: queries)
    monkeypatch.setattr(telemetry, "get_tracked_calls", lambda: calls)
    remote = audience != "admin"
    query_visible = audience in ("admin", "both", "query")
    api_visible = audience in ("admin", "both", "api")
    expected = {
        "enabled": False,
        "beacon_fire_rate": 0,
        "recent_beacons": 0,
        "last_beacon_time": None,
        "setup_complete": False,
        "message": "RUM not enabled for this service",
    }
    if master_enabled and (opt_in or force_include):
        if query_visible:
            expected.update(_debug_queries=queries, _debug_sqlite=queries)
        if api_visible:
            expected["_debug_calls"] = calls
        expected["_is_cached"] = False

    with TestClient(_rum_app(with_telemetry=True)) as client:
        if remote:
            _as_analyst(client, service_ids=[_SVC])
            share_db.set_setting("query_debug_visibility", "analysts" if query_visible else "disabled")
            share_db.set_setting("api_call_debug_visibility", "analysts" if api_visible else "disabled")
        headers = dict(_HEADERS) if remote else {}
        if opt_in:
            headers["x-debug-responses"] = "1"
        with patch("backend.routers.rum.svcconfig.load_config", return_value={}):
            response = client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=headers)

    assert response.status_code == 200 and response.json() == expected
    assert response.headers["content-type"] == "application/json" and int(response.headers["content-length"]) == len(
        response.content
    )


@pytest.mark.security_regression
def test_analyst_high_scale_query_response_has_no_debug_envelope() -> None:
    from backend.core.request_context import build_request_context
    from backend.routers import query, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(query.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    hs_result = {
        "columns": ["n"],
        "data": [{"n": 1}],
        "row_count": 1,
        "total_rows": 1,
        "truncated": False,
        "elapsed_ms": 1,
        "debug_queries": [{"sql": "SELECT secret FROM ch_table", "time_ms": 1, "engine": "ClickHouse"}],
        "_debug_queries": [{"sql": "SELECT secret FROM ch_table", "time_ms": 1, "engine": "ClickHouse"}],
        "debug_calls": [{"method": "GET", "path": "/internal"}],
    }

    def _post(c):
        with (
            patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock) as hs,
            patch("backend.high_scale.query.query_endpoint", return_value=dict(hs_result)),
        ):
            hs.return_value = object()
            return c.post(f"/api/query?service_id={_SVC}", json={"sql": "SELECT 1"}, headers=_WRITE_HEADERS)

    with TestClient(app) as c:
        _as_analyst(c, service_ids=[_SVC])
        resp = _post(c)
        assert resp.status_code == 200 and _envelope_keys(resp.json()) == [] and "ch_table" not in resp.text

        share_db.set_setting("query_debug_visibility", "analysts")
        body = _post(c).json()
        assert "_debug_queries" in body and "debug_calls" not in body and "_debug_calls" not in body


@pytest.mark.security_regression
def test_analyst_debug_aliases_are_stripped_when_master_switch_is_off(monkeypatch) -> None:
    from backend.routers import share_auth
    from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware

    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(share_auth.router)
    app.add_api_route(
        f"/api/services/{_SVC}/debug-aliases",
        lambda: {
            "value": "preserved",
            "debug_queries": [{"sql": "SELECT secret FROM logs"}],
            "debug_calls": [{"path": "/internal"}],
        },
        methods=["GET"],
    )

    with TestClient(app) as client:
        _as_analyst(client, service_ids=[_SVC])
        share_db.set_setting("query_debug_visibility", "analysts")
        share_db.set_setting("api_call_debug_visibility", "analysts")
        response = client.get(f"/api/services/{_SVC}/debug-aliases", headers={**_HEADERS, "x-debug-responses": "1"})

    assert response.status_code == 200 and response.json() == {"value": "preserved"}


_ENVELOPE = {
    "ok": True,
    "_debug_queries": [{"sql": "SELECT secret FROM t"}],
    "_debug_sqlite": [{"sql": "SELECT 1"}],
    "_debug_postgres": [{"sql": "SELECT 2"}],
    "_debug_calls": [{"path": "/service/abc"}],
    "_is_cached": False,
}


def _strip(payload: dict, *, query: str = "disabled", api: str = "disabled") -> dict:
    body = json.dumps(payload).encode()

    async def _gen():
        yield body

    resp = StreamingResponse(_gen(), media_type="application/json")
    resp.headers["content-type"] = "application/json"
    settings = {"query_debug_visibility": query, "api_call_debug_visibility": api}
    with patch("backend.core.share_db.settings.get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
        out = asyncio.run(_strip_analyst_envelope(resp))
    return json.loads(out.body)


@pytest.mark.security_regression
def test_strip_removes_debug_data_but_preserves_cache_marker_when_visibility_is_disabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    assert _strip(_ENVELOPE) == {"ok": True, "_is_cached": False}


@pytest.mark.security_regression
def test_scoring_cache_contract_preserves_marker_for_analyst_when_debug_is_disabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    from backend.routers.session_scoring import _finalize_cached

    assert _strip(_finalize_cached({"ok": True}, is_cached=True)) == {"ok": True, "_is_cached": True}


@pytest.mark.security_regression
def test_strip_removes_the_postgres_spelling_of_the_query_envelope():
    out = _strip({**_ENVELOPE, "debug_postgres": [{"sql": "SELECT 3"}]})
    assert not [k for k in out if "postgres" in k]


@pytest.mark.security_regression
def test_strip_keeps_the_postgres_envelope_only_when_query_visibility_is_enabled_for_analysts():
    out = _strip(_ENVELOPE, query="analysts")
    assert (
        out["_debug_postgres"] == [{"sql": "SELECT 2"}]
        and out["_debug_sqlite"] == [{"sql": "SELECT 1"}]
        and "_debug_calls" not in out
    )


def _admin_app() -> FastAPI:
    from backend.core.request_context import build_request_context
    from backend.routers import admin, provision, services, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    for r in (share_auth.router, provision.router, services.router, admin.router):
        app.include_router(r)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    return app


@pytest.fixture
def admin_analyst_client():
    with TestClient(_admin_app()) as c:
        _as_analyst(c, service_ids=[_SVC])
        yield c


_ADMIN_SITES = [
    ("POST", "/api/provision/validate", {"token": "t", "service_id": _SVC}),
    ("GET", "/api/provision/check-domain?prefix=abc", None),
    ("POST", "/api/provision/check-fos", {"bucket": "b", "region": "us-east-1", "access_key": "a", "secret_key": "s"}),
    ("GET", "/api/audit-logs", None),
    ("GET", "/api/admin/iceberg-calendar", None),
    ("GET", f"/api/services/{_SVC}/log-fields", None),
    ("POST", f"/api/services/{_SVC}/generate-viewer-key", None),
]


@pytest.mark.parametrize("visibility", ["disabled", "both"])
@pytest.mark.parametrize(("method", "path", "body"), _ADMIN_SITES)
@pytest.mark.security_regression
def test_analyst_cannot_reach_envelope_bearing_admin_routes(admin_analyst_client, method, path, body, visibility):
    share_db.set_setting("query_debug_visibility", visibility)
    share_db.set_setting("api_call_debug_visibility", visibility)
    r = admin_analyst_client.request(method, path, json=body, headers=_HEADERS)
    assert r.status_code == 403 and not [k for k in r.json() if k.startswith(("_debug", "debug_"))]
