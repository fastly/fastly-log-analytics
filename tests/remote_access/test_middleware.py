"""Integration tests for RemoteAccessMiddleware, PII policy, admin gateway, route tripwire, and debug envelopes."""

from __future__ import annotations

import asyncio
import copy
import json
import re
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, _mask_ip_scalar, apply_pii_policy, mask_ip
from backend.repositories.utils.filters import normalize_filter_key
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware
from backend.utils.tunnel.manager import AnalystSession, get_tunnel_manager
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from backend.core import share_db
from backend.core.request_context import build_request_context
from backend.main import app as _main_app
from backend.routers import admin, provision, query, rum, services, share_admin, share_auth
from backend.routers.session_scoring import _finalize_cached
from backend.routers.share_admin import build_share_live_payload, build_share_status
from backend.utils import telemetry, tunnel
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
from tests.remote_access import conftest as H

_SVC = "svcA"
_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_WRITE_HEADERS = {**_HEADERS, "Origin": "https://testserver"}
_MASK = {"mask_ips": True}
SECRET = "test-gateway-secret-with-at-least-32-characters"
HEADER = "X-Admin-Gateway-Token"
_ENVELOPE_PREFIXES = ("_debug", "debug_")


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)
    for path in (
        "/api/dashboard /api/cron/stream /api/log-extents/stream /api/usage/summary /api/download "
        "/api/download-all /api/download-folder /api/download-foo /api/cron-schedule"
    ).split():
        app.add_api_route(path, lambda: {"ok": True}, methods=["GET"])
    for path in "/api/views /api/dashboard/aggregates /api/web-vitals /api/ux-events".split():
        app.add_api_route(path, lambda: {"ok": True}, methods=["POST"])
    app.add_api_route("/api/cron-runs/{run_id}/stream", lambda run_id: {"ok": True}, methods=["GET"])

    def _svc_handler(service_id: str):
        return {"ok": True, "service_id": service_id}

    rum_sub = "status versions beacon-health analytics live-events".split()
    score_sub = (
        "status config audit threshold exclude-regex enforce-status-code labels top-flagged threshold-preview".split()
    )
    svc_gets = [
        "/api/services/{service_id}/realtime-stream",
        "/api/services/{service_id}/cost-governor-stream",
        "/api/alerts/{service_id}",
        "/api/custom-endpoint/{service_id}/data",
        "/api/services/{service_id}/lake-info",
        *(f"/api/services/{{service_id}}/rum/{s}" for s in rum_sub),
        *(f"/api/services/{{service_id}}/scoring/{s}" for s in score_sub),
    ]
    for path in svc_gets:
        app.add_api_route(path, _svc_handler, methods=["GET"])
    for s in ("enable", "disable", "upgrade"):
        app.add_api_route(f"/api/services/{{service_id}}/rum/{s}", _svc_handler, methods=["POST"])
    app.add_api_route(
        "/api/services/{service_id}/scoring/sessions/{sid}/events",
        lambda service_id, sid: {"ok": True, "service_id": service_id, "sid": sid},
        methods=["GET"],
    )
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
        name="Drew", email="drew@example.com", passcode="ocean-breeze-cabin-42",
        expires_at_utc=None, ip_whitelist=ip_whitelist, service_ids=service_ids or ["svcA", "svcB"], pii_policy=pii_policy,
    )  # fmt: skip
    if tos := share_db.get_latest_tos():
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
        if name in ("analyst_pending_session_id", "analyst_session_id") and val.strip('"'):
            sid = val.strip('"')
    client.cookies.set("analyst_session_id", sid)
    return sid


def _as_analyst(client, service_ids=None, pii_policy=None) -> str:
    _start_share()
    return _login_analyst(client, _seed_invite(service_ids=service_ids, pii_policy=pii_policy))


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
        lambda: [{"service_id": "svc_rf", "remote_frontend": {"service_id": "f_123", "domain_name": "fla.fastly.net"}}],
    )
    r3 = client.get("/api/dashboard", headers={"X-Remote-Analyst": "1", "Host": "fla.fastly.net"})
    assert r3.status_code == 401 and r3.json()["error"] == "unauthenticated"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "method,path", [("GET", "/api/admin/clickhouse/status?service_id=svcA"), ("POST", "/api/admin/clickhouse/replay")]
)
def test_clickhouse_controls_block_authenticated_analyst(client, method, path):
    client.app.include_router(admin.router)
    _as_analyst(client)
    assert client.get("/api/dashboard?service_id=svcA", headers=_WRITE_HEADERS).status_code == 200
    r = client.request(method, path, headers=_WRITE_HEADERS, json={"service_id": "svcA", "dataset_id": "dataset"})
    assert r.status_code == 403 and r.json()["error"] == "admin_only"


@pytest.mark.security_regression
def test_gateway_required_keeps_authenticated_analyst_restrictions(client, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    _as_analyst(client, service_ids=["svcA"])
    assert client.get("/api/dashboard", headers=_HEADERS, params={"service_id": "svcA"}).status_code == 200
    denied = client.get("/api/admin/share/status", headers=_HEADERS)
    assert denied.status_code == 403 and denied.json()["error"] == "admin_only"


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


@pytest.mark.parametrize("suffix", "/config /status /audit /threshold /exclude-regex /enforce-status-code".split())
def test_analyst_blocked_from_scoring_admin_suffix(client, suffix):
    _as_analyst(client, service_ids=["svcA"])
    r = client.get(f"/api/services/svcA/scoring{suffix}", headers=_HEADERS)
    assert r.status_code == 403 and r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    (
        "/api/services/svcA/scoring/labels /api/services/svcA/scoring/sessions/sess-1/events "
        "/api/services/svcA/scoring/top-flagged /api/services/svcA/scoring/threshold-preview "
        "/api/services/svcA/rum/status /api/services/svcA/rum/beacon-health "
        "/api/services/svcA/rum/analytics /api/services/svcA/rum/live-events"
    ).split(),
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


def _post_filter(client, *, filters):
    return client.post(
        "/api/dashboard/aggregates", json={"service_id": "svcA", "filters": filters}, headers=_WRITE_HEADERS
    )


def _post_field(client, *, field):
    return client.post("/api/dashboard/aggregates", json={"service_id": "svcA", "field": field}, headers=_WRITE_HEADERS)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field",
    "ip|ip.|ip |client_ip.|cookie_session|cookie_session.|cookie_session |IP|Cookie_Session|Client_IP".split("|"),
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
@pytest.mark.parametrize("key", "ip filter_ip xfilter_ip ip_2 client_ip".split())
def test_analyst_masking_blocks_ip_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["1.2.3.4"]}})
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"
    assert r.json()["field"] == ("client_ip" if key == "client_ip" else "ip")


@pytest.mark.security_regression
@pytest.mark.parametrize("key", "cookie_session filter_cookie_session cookie_session_2".split())
def test_analyst_masking_blocks_cookie_session_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert (
        r.status_code == 403 and r.json()["error"] == "pii_policy_violation" and r.json()["field"] == "cookie_session"
    )


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
    "cookie_session.|cookie_session |cookie_session\t|cookie_session%|cookie_session-|filter_cookie_session.|ip.|ip |client_ip.".split(
        "|"
    ),
)
def test_analyst_masking_blocks_safecol_evasion_filter(client, key):
    _as_analyst(client, service_ids=["svcA"], pii_policy=_MASK)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert r.status_code == 403 and r.json()["error"] == "pii_policy_violation"


def test_analyst_without_masking_allows_ip_filter(client):
    _as_analyst(client, service_ids=["svcA"], pii_policy={"mask_ips": False})
    assert _post_filter(client, filters={"ip": {"mode": "include", "values": ["1.2.3.4"]}}).status_code == 200


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
        "/api/share/login", json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"}, headers=_WRITE_HEADERS
    )
    assert r_ok.headers.get("cache-control") == "private, no-store"
    assert r_ok.headers.get("x-frame-options") == "DENY" and r_ok.headers.get("referrer-policy") == "no-referrer"

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
            "/api/share/login", json={"email": "attacker@example.com", "passcode": f"wrong-{i}"}, headers=_WRITE_HEADERS
        )
        statuses.append(r.status_code)
        if r.status_code == 429 and first_429 is None:
            first_429 = r.json()["detail"]
    assert statuses[: tunnel.LOGIN_FAILURE_THRESHOLD] == [401] * tunnel.LOGIN_FAILURE_THRESHOLD
    assert set(statuses[tunnel.LOGIN_FAILURE_THRESHOLD :]) == {429}
    assert first_429 is not None and first_429["error"] == "rate_limited"
    assert isinstance(first_429["retry_after_s"], int) and 0 < first_429["retry_after_s"] <= tunnel.LOGIN_LOCKOUT_S


_BEACON_PAYLOAD = {"id": "x", "name": "LCP", "value": 1.0, "rating": "good"}


@pytest.mark.parametrize(
    "method,path,extra_headers,body,expect_touch",
    [
        ("GET", "/api/dashboard?service=svcA", {}, None, True),
        ("GET", "/api/dashboard?service=svcA", {"X-User-Active": "1"}, None, True),
        ("GET", "/api/dashboard?service=svcA", {"X-User-Active": "0"}, None, False),
        ("GET", "/api/log-extents/stream?service=svcA", {}, None, False),
        ("POST", "/api/web-vitals?service=svcA", {"Origin": "https://testserver"}, _BEACON_PAYLOAD, False),
        ("POST", "/api/ux-events?service=svcA", {"Origin": "https://testserver"}, _BEACON_PAYLOAD, False),
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
    session = tunnel.get_tunnel_manager()._sessions.get(sid)
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


@pytest.mark.security_regression
@pytest.mark.parametrize("field", "ip|ip.|ip |ip\t|client_ip.|remote_addr.|IP|Ip|Client_IP|IP.".split("|"))
def test_apply_pii_policy_masks_ip_field_values_despite_junk_suffix(field):
    obj = {"field": field, "values": [{"value": "203.0.113.7", "count": 9}]}
    got = apply_pii_policy(copy.deepcopy(obj), _MASK)["values"][0]["value"]
    assert got != "203.0.113.7" and got.endswith("xxx"), (field, got)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field",
    "cookie_session|cookie_session.|cookie_session |cookie_session\t|cookie_session%|Cookie_Session|COOKIE_SESSION|Cookie_Session.".split(
        "|"
    ),
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
    "ip|ip.|ip |client_ip.|filter_ip|ip_2|cookie_session|cookie_session.|.cookie_session|filter_cookie_session.|IP|Ip|Client_IP|Cookie_Session|COOKIE_SESSION|filter_Cookie_Session.".split(
        "|"
    ),
)
def test_normalize_filter_key_resolves_junk_pii_keys_into_forbidden_set(key):
    assert normalize_filter_key(key) in _PII_FORBIDDEN_FILTER_COLS, key


@pytest.mark.security_regression
@pytest.mark.parametrize("key", "url status country oip waf_sig_ind _bot_name".split())
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


def _request(peer: str, headers: dict[str, str] | None = None) -> StarletteRequest:
    hdrs = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http", "method": "GET", "path": "/api/bootstrap", "headers": hdrs,
        "client": (peer, 1234), "server": ("testserver", 80), "scheme": "http", "query_string": b"",
    }  # fmt: skip
    return StarletteRequest(scope)


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
    app.add_api_route("/api/admin/probe", lambda request: {"is_remote": request.state.is_remote}, methods=["GET"])
    app.add_api_route("/api/health", lambda: {"ok": True}, methods=["GET"])
    app.add_api_route("/api/admin/probe", lambda: {"ok": True}, methods=["POST"])

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
    hdr = {HEADER: SECRET, "Origin": "https://testserver"}
    assert gateway_client.post("/api/admin/probe", headers=hdr).status_code == 200
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


_HTTP_METHODS = ("get", "post", "put", "patch", "delete")
_PATH_PARAM_SUBSTITUTIONS = dict(
    kv.split("=")
    for kv in (
        "service_id=svc1 domain=rum_vitals export_id=export1 job_id=job1 alert_id=alert1 invite_id=invite1 "
        "session_id=sess1 quarantine_id=quar1 source_id=src1 qid=q1 log_id=1 run_id=run1 field_name=field1 "
        "label_id=label1 item_id=1 version=v1 sid=sid1 view_id=view1 tab=overview token=tok1"
    ).split()
)
_PARAM_RE = re.compile(r"\{([^}]+)\}")


def _substitute(path: str) -> str:
    def _rep(m: re.Match[str]) -> str:
        name = m.group(1)
        assert name in _PATH_PARAM_SUBSTITUTIONS, f"missing path param {name!r} in {path!r}"
        return _PATH_PARAM_SUBSTITUTIONS[name]

    return _PARAM_RE.sub(_rep, path)


def _real_routes() -> list[tuple[str, str]]:
    paths = _main_app.openapi()["paths"]
    return sorted((m.upper(), p) for p in paths for m in _HTTP_METHODS if m in paths[p])


_SCORING_READS = (
    "analytics compliance-breakdown curves evaluation health labels latency-timeseries "
    "score-distribution sessions/{sid}/events threshold-preview top-flagged"
).split()
_READ_PATHS = (
    "/api/health /api/bootstrap /api/share/auth-config /api/share/heartbeat /api/share/tos "
    "/api/share/oauth/authorize /api/share/oauth/callback /js/rum.js /js/faro-sdk.js "
    "/api/log-fields/catalog /api/presets /api/schema /api/services /api/insight-availability "
    "/api/views/{service_id} /api/log-extents /api/log-extents/stream "
    "/api/services/{service_id}/realtime-stream /api/services/{service_id}/control-room/error-stream "
    "/api/security/proxies/export /api/assets/aggregates /api/network/bundle /api/network/pop-health "
    "/api/security/bundle /api/security/threat-intel /api/query/schema/{service_id} "
    "/api/high-scale/request-facts /api/high-scale/services/{service_id}/exports/{export_id} "
    "/api/high-scale/services/{service_id}/queries/{job_id} /api/services/{service_id}/cmcd/status "
    "/api/services/{service_id}/control-room/wizard/state /api/services/{service_id}/control-room/{tab} "
    "/api/services/{service_id}/realtime-seed"
).split() + [
    *(f"/api/services/{{service_id}}/scoring/{s}" for s in _SCORING_READS),
    *(f"/api/services/{{service_id}}/rum/{s}" for s in "status beacon-health analytics live-events".split()),
]
_READ_ALLOWLIST: set[tuple[str, str]] = {("GET", p) for p in _READ_PATHS}

_ORIGIN_WRITES = "aggregates ip-health path-breakdown pop-latency shielding-analysis slow-urls status-codes summary timeseries drilldown".split()
_HS_WRITES = "request-facts rum-facts/{domain} cmcd-facts aggregates queries queries/{job_id}/cancel exports exports/{export_id}/cancel".split()
_WRITE_PATHS = (
    "/api/share/acknowledge /api/share/claim/{token} /api/share/login /api/share/logout "
    "/api/assets/aggregates /api/dashboard/aggregates /api/dashboard/bundle /api/dashboard/field-values "
    "/api/dashboard/raw/csv /api/security/aggregates /api/performance/aggregates /api/insights "
    "/api/insights/cache-collapse-detail /api/network-health /api/network-quality /api/network/bundle "
    "/api/query /api/security/bundle /api/sessions /api/sessions/detail /api/cmcd/aggregates "
    "/api/value/summary /api/web-vitals /api/ux-events /api/high-scale/request-facts"
).split() + [
    *(f"/api/origin/{s}" for s in _ORIGIN_WRITES),
    *(f"/api/high-scale/services/{{service_id}}/{s}" for s in _HS_WRITES),
]
_WRITE_VERB_GATE_ROUTES: set[tuple[str, str]] = {("POST", p) for p in _WRITE_PATHS}
_ALL_INVENTORIED = _READ_ALLOWLIST | _WRITE_VERB_GATE_ROUTES


def _is_unauth_path(path: str) -> bool:
    return path in _UNAUTH_ANALYST_PATHS or path.startswith(("/api/share/claim/", "/api/share/oauth/"))


def _verb_gate_blocks(method: str, substituted_path: str) -> bool:
    if method in ("PUT", "PATCH", "DELETE"):
        return True
    if method == "POST":
        return not _is_unauth_path(substituted_path) and not any(
            substituted_path.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES
        )
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
        substituted = _substitute(path)
        assert method == "POST" and not _is_blocked_path(substituted)
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
        assert dummy_path in paths and any(
            m in paths[dummy_path]
            and not _is_blocked_path(dummy_path)
            and not _verb_gate_blocks(m.upper(), dummy_path)
            and (m.upper(), dummy_path) not in _ALL_INVENTORIED
            for m in _HTTP_METHODS
        )
    finally:
        _main_app.router.routes[:] = [r for r in _main_app.router.routes if getattr(r, "path", None) != dummy_path]
        _main_app.openapi_schema = None


def _rum_app(*, with_telemetry: bool = False) -> FastAPI:
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
        "no_data": False, "total_beacons": 7, "pageviews": 5, "interactions": 1, "errors_count": 1,
        "vitals_rows": [("LCP", 2100.0, 10, 8, 1, 1)], "worst_pages_rows": [("/p", 4, 120.0, 2100.0, 0.05, 0.1)],
        "errors_rows": [("boom", "app.js", 3, 4, 2)], "trends_rows": [],
        "browsers": {"Firefox": 7}, "os": {"macOS": 7}, "devices": {"desktop": 7},
    }  # fmt: skip
    with (
        patch("backend.routers.rum.rum_source_for", return_value={"name": f"{_SVC}_rum"}),
        patch("backend.routers.rum._ConnectionHolder", _Holder),
        patch("backend.routers.rum.execute_with_stale_view_retry", return_value=db_res),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_DIST = {"good": 80, "needs_improvement": 15, "poor": 5}
_HS_ANALYTICS = {
    "is_mock": False, "no_data": False, "beacon_count": 3, "pageview_count": 3, "interaction_count": 0, "error_count": 0,
    "vitals": {
        "lcp": {"p75": 2.1, "distribution": _DIST}, "cls": {"p75": 0.05, "distribution": _DIST},
        "inp": {"p75": 180, "distribution": _DIST}, "fid": {"p75": 12.0, "fcp": 1.1, "ttfb": 0.4},
    },
    "worst_pages": [], "errors": [],
    "trends": {
        "timestamps": ["2026-10-17T00:00:00Z"], "lcp": [2.1], "cls": [0.05],
        "error_rate": [0.0], "pageviews": [3], "interactions": [0], "errors": [0],
    },
    "environments": {"browsers": {}, "os": {}, "devices": {}},
}  # fmt: skip


def _hs_beacon_health(client):
    svc = MagicMock(service_id=_SVC)
    svc.client.execute.return_value = [{"c": 3}]
    with patch("backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock, return_value=svc):
        return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _hs_analytics(client):
    with (
        patch(
            "backend.core.request_context.RequestContext.high_scale", new_callable=PropertyMock, return_value=object()
        ),
        patch("backend.high_scale.rum.rum_analytics", return_value=dict(_HS_ANALYTICS)),
    ):
        return client.get(f"/api/services/{_SVC}/rum/analytics", headers=_HEADERS)


_ROUTES = [
    _beacon_health_disabled, _beacon_health_check_failed, _analytics_no_data,
    _analytics_success, _hs_beacon_health, _hs_analytics,
]  # fmt: skip


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


@pytest.mark.parametrize("audience", "admin both query api disabled".split())
@pytest.mark.parametrize(
    ("master_enabled", "opt_in", "force_include"),
    [(m, o, f) for m in (False, True) for o, f in ((False, False), (True, False), (False, True))],
)
def test_rum_pre_attached_envelope_respects_master_switch(
    monkeypatch, audience: str, master_enabled: bool, opt_in: bool, force_include: bool
) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "1" if master_enabled else "")
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "1" if force_include else "")
    queries, calls = [{"sql": "SELECT 1", "time_ms": 0.1}], [{"method": "GET", "path": "/example"}]
    monkeypatch.setattr(telemetry, "get_queries", lambda: queries)
    monkeypatch.setattr(telemetry, "get_sqlite_queries", lambda: queries)
    monkeypatch.setattr(telemetry, "get_tracked_calls", lambda: calls)
    remote = audience != "admin"
    query_visible, api_visible = audience in ("admin", "both", "query"), audience in ("admin", "both", "api")
    expected = {
        "enabled": False, "beacon_fire_rate": 0, "recent_beacons": 0,
        "last_beacon_time": None, "setup_complete": False, "message": "RUM not enabled for this service",
    }  # fmt: skip
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
        headers = {**(_HEADERS if remote else {}), **({"x-debug-responses": "1"} if opt_in else {})}
        with patch("backend.routers.rum.svcconfig.load_config", return_value={}):
            response = client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=headers)

    assert response.status_code == 200 and response.json() == expected
    assert response.headers["content-type"] == "application/json" and int(response.headers["content-length"]) == len(
        response.content
    )


@pytest.mark.security_regression
def test_analyst_high_scale_query_response_has_no_debug_envelope() -> None:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(query.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    q_dbg = [{"sql": "SELECT secret FROM ch_table", "time_ms": 1, "engine": "ClickHouse"}]
    hs_result = {
        "columns": ["n"], "data": [{"n": 1}], "row_count": 1, "total_rows": 1, "truncated": False, "elapsed_ms": 1,
        "debug_queries": q_dbg, "_debug_queries": q_dbg, "debug_calls": [{"method": "GET", "path": "/internal"}],
    }  # fmt: skip

    def _post(c):
        with (
            patch(
                "backend.core.request_context.RequestContext.high_scale",
                new_callable=PropertyMock,
                return_value=object(),
            ),
            patch("backend.high_scale.query.query_endpoint", return_value=dict(hs_result)),
        ):
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
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(share_auth.router)
    payload = {
        "value": "preserved",
        "debug_queries": [{"sql": "SELECT secret FROM logs"}],
        "debug_calls": [{"path": "/internal"}],
    }
    app.add_api_route(f"/api/services/{_SVC}/debug-aliases", lambda: payload, methods=["GET"])
    with TestClient(app) as client:
        _as_analyst(client, service_ids=[_SVC])
        share_db.set_setting("query_debug_visibility", "analysts")
        share_db.set_setting("api_call_debug_visibility", "analysts")
        response = client.get(f"/api/services/{_SVC}/debug-aliases", headers={**_HEADERS, "x-debug-responses": "1"})
    assert response.status_code == 200 and response.json() == {"value": "preserved"}


_ENVELOPE = {
    "ok": True, "_debug_queries": [{"sql": "SELECT secret FROM t"}], "_debug_sqlite": [{"sql": "SELECT 1"}],
    "_debug_postgres": [{"sql": "SELECT 2"}], "_debug_calls": [{"path": "/service/abc"}], "_is_cached": False,
}  # fmt: skip


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


# ── Merged from tests/remote_access/test_share_routes.py ──

_ANALYST_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}
_ANALYST_GET_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_DEFAULT_PASSCODE = "ocean-breeze-cabin-42"


def _app() -> FastAPI:
    return H.make_remote_access_app(share_admin.router, share_auth.router)


@pytest.fixture
def share_routes_client():
    with TestClient(_app()) as c:
        yield c


def _seed_invite_from_share_routes(**overrides) -> dict:
    return share_db.create_remote_invite(
        name=overrides.get("name", "Drew"),
        email=overrides.get("email", "drew@example.com"),
        passcode=overrides.get("passcode", _DEFAULT_PASSCODE),
        expires_at_utc=overrides.get("expires_at_utc"),
        ip_whitelist=overrides.get("ip_whitelist"),
        service_ids=overrides.get("service_ids", ["svcA"]),
    )


def _create_invite_via_api(share_routes_client, **extra) -> dict:
    payload = {
        "name": "Drew",
        "email": "drew@example.com",
        "passcode": _DEFAULT_PASSCODE,
        "service_ids": ["svcA"],
        **extra,
    }
    r = share_routes_client.post("/api/admin/share/invites", json=payload)
    assert r.status_code == 200, r.text
    return r.json()


def _activate_share():
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")


def _parse_set_cookies(response) -> dict[str, dict[str, str]]:
    """Parse Set-Cookie headers into ``{name: {key: value, ...}}``."""
    out: dict[str, dict[str, str]] = {}
    headers = (
        response.headers.get_list("set-cookie")
        if hasattr(response.headers, "get_list")
        else ([response.headers.get("set-cookie", "")] if response.headers.get("set-cookie") else [])
    )
    for raw in headers:
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        if not parts or "=" not in parts[0]:
            continue
        name, value = parts[0].split("=", 1)
        attrs = {"_value": value}
        for attr in parts[1:]:
            if "=" in attr:
                k, v = attr.split("=", 1)
                attrs[k.strip().lower()] = v.strip()
            else:
                attrs[attr.strip().lower()] = ""
        out[name.strip()] = attrs
    return out


def _sid_from_login(response) -> str:
    """Return the live session id from a login/acknowledge response's Set-Cookie headers."""
    cookies = _parse_set_cookies(response)
    for name in ("analyst_pending_session_id", "analyst_session_id"):
        c = cookies.get(name)
        if c and c.get("_value") and c.get("max-age") != "0":
            return c["_value"]
    return ""


def _login_and_get_session(share_routes_client):
    invite = _seed_invite_from_share_routes()
    r = share_routes_client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    sid = _sid_from_login(r)
    share_routes_client.cookies.set("analyst_session_id", sid)
    return tunnel.get_tunnel_manager(), sid


# ── /status & /audit-logs ──────────────────────────────────────────────────


def test_status_returns_expected_keys(share_routes_client):
    r = share_routes_client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text
    body = r.json()
    expected_keys = (
        "sharing_active",
        "public_endpoint",
        "public_url",
        "active_session_count",
        "max_concurrent_sessions",
        "invites",
        "sessions",
        "audit_logs",
        "services",
        "rate_limits",
        "telemetry",
    )
    for key in expected_keys:
        assert key in body, f"missing {key}"
    assert body["sharing_active"] is False
    assert body["active_session_count"] == 0
    assert "failures" in body["rate_limits"] and "lockouts" in body["rate_limits"]
    assert "heartbeat_unauth_count" in body["telemetry"]


def test_audit_logs_endpoint_returns_filtered_rows_and_rejects_bad_limit(share_routes_client):
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    r = share_routes_client.get("/api/admin/share/audit-logs", params={"event_type": "LOGIN_FAIL"})
    assert r.status_code == 200
    body = r.json()
    assert "audit_logs" in body
    assert all(row["event_type"] == "LOGIN_FAIL" for row in body["audit_logs"])
    assert share_routes_client.get("/api/admin/share/audit-logs", params={"limit": 0}).status_code == 422


@pytest.mark.parametrize(
    ("name", "email", "log_events", "expect_non_null"),
    [
        ("Alice", "alice@example.com", [("LOGIN_SUCCESS", "1.1.1.1"), ("LOGIN_FAIL", "1.1.1.1")], True),
        ("Bob", "bob@example.com", [], False),
    ],
)
def test_status_invites_last_login_derivation(share_routes_client, name, email, log_events, expect_non_null):
    inv = _seed_invite_from_share_routes(name=name, email=email, service_ids=[])
    for ev_type, ip in log_events:
        share_db.log_share_audit_event(event_type=ev_type, email=email, ip_address=ip, details="x")
    body = share_routes_client.get("/api/admin/share/status").json()
    row = next(i for i in body["invites"] if i["id"] == inv["id"])
    assert (row["last_login_at"] is not None) is expect_non_null


# ── /start /stop /panic ────────────────────────────────────────────────────


def test_start_stop_and_panic(share_routes_client):
    r_bad = share_routes_client.post("/api/admin/share/start", json={"public_endpoint": "http://example.com"})
    assert r_bad.status_code == 400
    assert r_bad.json()["detail"]["error"] == "invalid_request"

    r = share_routes_client.post("/api/admin/share/start", json={"public_endpoint": "https://share.example.com"})
    assert r.status_code == 200, r.text
    assert r.json()["public_url"] == "https://share.example.com"
    assert share_routes_client.post("/api/admin/share/stop").status_code == 200

    r_panic = share_routes_client.post("/api/admin/share/panic")
    assert r_panic.status_code == 200
    assert r_panic.json().get("sessions_booted") == 0


# ── /invites ───────────────────────────────────────────────────────────────


def test_create_invite_happy_path_and_concurrent_session_flags(share_routes_client):
    body = _create_invite_via_api(share_routes_client, duration_hours=24)
    assert body["email"] == "drew@example.com"
    assert "id" in body
    assert body["allow_concurrent_sessions"] is False

    body_concurrent = _create_invite_via_api(
        share_routes_client, email="drew2@example.com", allow_concurrent_sessions=True
    )
    assert body_concurrent["allow_concurrent_sessions"] is True


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        ({"passcode": "shrt"}, "weak_passcode"),
        ({"name": "<script>"}, "invalid_input"),
        ({"service_ids": []}, "invalid_request"),
    ],
)
def test_create_invite_validation_errors(share_routes_client, overrides, expected_error):
    payload = {
        "name": "Drew",
        "email": "drew@example.com",
        "passcode": _DEFAULT_PASSCODE,
        "service_ids": ["svcA"],
        **overrides,
    }
    r = share_routes_client.post("/api/admin/share/invites", json=payload)
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == expected_error


def test_update_invite_services_and_empty_rejected(share_routes_client):
    invite_id = _create_invite_via_api(share_routes_client)["id"]
    r2 = share_routes_client.patch(
        f"/api/admin/share/invites/{invite_id}/services", json={"service_ids": ["svcA", "svcB"]}
    )
    assert r2.status_code == 200
    assert sorted(r2.json()["service_ids"]) == ["svcA", "svcB"]

    r_empty = share_routes_client.patch(f"/api/admin/share/invites/{invite_id}/services", json={"service_ids": []})
    assert r_empty.status_code == 400
    assert r_empty.json()["detail"]["error"] == "invalid_request"


def test_update_invite_pii_and_sharing_toggles(share_routes_client):
    inv = _create_invite_via_api(share_routes_client, pii_policy={"mask_ips": False})
    invite_id = inv["id"]
    assert inv["pii_policy"]["mask_ips"] is False
    assert inv["allow_concurrent_sessions"] is False

    r_pii = share_routes_client.patch(f"/api/admin/share/invites/{invite_id}/pii", json={"mask_ips": True})
    assert r_pii.status_code == 200, r_pii.text
    assert r_pii.json()["pii_policy"]["mask_ips"] is True

    r_sh1 = share_routes_client.patch(
        f"/api/admin/share/invites/{invite_id}/sharing", json={"allow_concurrent_sessions": True}
    )
    assert r_sh1.status_code == 200 and r_sh1.json()["allow_concurrent_sessions"] is True
    r_sh2 = share_routes_client.patch(
        f"/api/admin/share/invites/{invite_id}/sharing", json={"allow_concurrent_sessions": False}
    )
    assert r_sh2.status_code == 200 and r_sh2.json()["allow_concurrent_sessions"] is False


def test_update_invite_passcode_happy_and_weak_rejected(share_routes_client):
    invite = _seed_invite_from_share_routes(email="drew-passcode@example.com")
    r_weak = share_routes_client.patch(f"/api/admin/share/invites/{invite['id']}/passcode", json={"passcode": "weak"})
    assert r_weak.status_code == 400

    r = share_routes_client.patch(
        f"/api/admin/share/invites/{invite['id']}/passcode",
        json={"passcode": "river-stone-mountain-99"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True

    refreshed = share_db.get_remote_invite(invite["id"])
    assert not share_db.verify_passcode(_DEFAULT_PASSCODE, refreshed["passcode"])
    assert share_db.verify_passcode("river-stone-mountain-99", refreshed["passcode"])


def test_revoke_delete_and_issue_claim_token_happy_paths(share_routes_client):
    inv1 = _seed_invite_from_share_routes(email="drew@example.com")
    r_claim = share_routes_client.post(f"/api/admin/share/invites/{inv1['id']}/claim-token")
    assert r_claim.status_code == 200 and len(r_claim.json()["token"]) >= 16

    r_rev = share_routes_client.post(f"/api/admin/share/invites/{inv1['id']}/revoke")
    assert r_rev.status_code == 200 and r_rev.json()["ok"] is True

    inv2 = _seed_invite_from_share_routes(email="drew-delete@example.com")
    r_del = share_routes_client.delete(f"/api/admin/share/invites/{inv2['id']}")
    assert r_del.status_code == 200 and r_del.json()["ok"] is True
    assert share_db.get_remote_invite(inv2["id"]) is None


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("PATCH", "/api/admin/share/invites/no-such-invite/services", {"service_ids": ["svcA"]}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/pii", {"mask_ips": True}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/sharing", {"allow_concurrent_sessions": True}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/passcode", {"passcode": _DEFAULT_PASSCODE}),
        ("POST", "/api/admin/share/invites/missing/revoke", None),
        ("DELETE", "/api/admin/share/invites/missing", None),
        ("POST", "/api/admin/share/invites/missing/claim-token", None),
    ],
)
def test_unknown_invite_routes_return_404(share_routes_client, method, path, payload):
    r = (
        share_routes_client.request(method, path, json=payload)
        if payload is not None
        else share_routes_client.request(method, path)
    )
    assert r.status_code == 404


# ── Backup, GDPR, Settings, Wordphrase ─────────────────────────────────────


def test_backup_export_import_round_trip_and_errors(share_routes_client):
    r_weak = share_routes_client.post("/api/admin/share/backup/export", json={"passphrase": "short"})
    assert r_weak.status_code == 400 and r_weak.json()["detail"]["error"] == "weak_passphrase"

    _seed_invite_from_share_routes()
    r = share_routes_client.post("/api/admin/share/backup/export", json={"passphrase": "correct-horse-battery-staple"})
    assert r.status_code == 200 and len(r.content) > 100
    blob = r.content

    r_bad_mode = share_routes_client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "merge-all-the-things"},
        files={"file": ("backup.enc", b"junk", "application/octet-stream")},
    )
    assert r_bad_mode.status_code == 400 and r_bad_mode.json()["detail"]["error"] == "invalid_mode"

    r_bad_pass = share_routes_client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "wrong-horse-battery-staple", "mode": "skip-collisions"},
        files={"file": ("backup.enc", blob, "application/octet-stream")},
    )
    assert r_bad_pass.status_code == 400 and r_bad_pass.json()["detail"]["error"] == "import_failed"

    r2 = share_routes_client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "skip-collisions"},
        files={"file": ("backup.enc", blob, "application/octet-stream")},
    )
    assert r2.status_code == 200, r2.text
    assert isinstance(r2.json(), dict)
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", _DEFAULT_PASSCODE) is not None


def test_gdpr_erase_settings_and_wordphrase(share_routes_client):
    _seed_invite_from_share_routes()
    r_erase = share_routes_client.post(
        "/api/admin/share/gdpr/erase", json={"email": "drew@example.com", "reason": "user request"}
    )
    assert r_erase.status_code == 200, r_erase.text
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", _DEFAULT_PASSCODE) is None

    r_blank = share_routes_client.post("/api/admin/share/gdpr/erase", json={"email": "", "reason": "user request"})
    assert r_blank.status_code == 400 and r_blank.json()["detail"]["error"] == "invalid_request"

    r_cap = share_routes_client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 25})
    assert r_cap.status_code == 200 and r_cap.json()["max_concurrent_analyst_sessions"] == 25
    assert (
        share_routes_client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 0}).status_code
        == 400
    )

    r_wp = share_routes_client.get("/api/admin/share/wordphrase")
    assert r_wp.status_code == 200
    passcode = r_wp.json()["passcode"]
    assert isinstance(passcode, str) and len(passcode) >= 16 and "-" in passcode


# ── share-live freshness & wire contracts ──────────────────────────────────


def test_dedicated_stream_route_removed_and_live_payload_shape(share_routes_client):
    app = FastAPI()
    app.include_router(share_admin.router)
    paths = app.openapi()["paths"]
    assert "/api/admin/share/stream" not in paths
    assert "/api/admin/share/live" in paths

    payload = build_share_live_payload()
    for key in ("sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"):
        assert key in payload, f"missing {key}"


def _seed_status_fixtures():
    _seed_invite_from_share_routes(name="Carol", email="carol@example.com", service_ids=["svc-a"])
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email=None, ip_address="2.2.2.2", details="d")
    mgr = get_tunnel_manager()
    mgr._sessions["sess-1"] = AnalystSession(
        session_id="sess-1",
        invite_id="inv-1",
        name="Carol",
        email="carol@example.com",
        ip_address="2.2.2.2",
        user_agent="ua",
        fingerprint_signature="fp",
        pii_policy={"mask_ips": True},
        query_window_hours=None,
        query_start_time=None,
        query_end_time=None,
        login_time="2026-01-01T00:00:00Z",
        last_active_time="2026-01-01T00:01:00Z",
        service_ids=["svc-a"],
        auth_method="oauth",
        oauth_provider="google",
    )
    mgr._tunnel_uptime_history.append(
        {"started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:05:00Z", "duration_s": 300, "reason": "stop"}
    )
    mgr.record_login_failure("9.9.9.9", None)
    mgr.record_heartbeat_unauth()


def test_status_live_and_audit_logs_match_producer(share_routes_client):
    _seed_status_fixtures()
    expected = jsonable_encoder(build_share_status())
    body = share_routes_client.get("/api/admin/share/status").json()

    assert body == expected
    assert [list(s) for s in body["sessions"]] == [list(s) for s in expected["sessions"]]
    assert body["sessions"][0]["auth_method"] == "oauth"
    assert body["rate_limits"]["failures"][0]["ip"] == "9.9.9.9"
    assert body["telemetry"]["heartbeat_unauth_count"] == 1
    assert body["telemetry"]["tunnel_uptime_history"][0]["duration_s"] == 300
    assert body["audit_logs"] and set(body["audit_logs"][0]) >= {"id", "timestamp", "event_type", "email"}

    assert share_routes_client.get("/api/admin/share/live").json() == jsonable_encoder(build_share_live_payload())
    assert share_routes_client.get("/api/admin/share/audit-logs").json()["audit_logs"] == jsonable_encoder(
        share_db.get_share_audit_logs(limit=200)
    )


def test_invite_revoked_is_integer_flag_on_the_wire(share_routes_client):
    inv = _seed_invite_from_share_routes(name="Dan", email="dan@example.com", service_ids=[])
    row = next(i for i in share_routes_client.get("/api/admin/share/status").json()["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 0 and type(row["revoked"]) is int
    assert share_db.revoke_remote_invite(inv["id"])
    row = next(i for i in share_routes_client.get("/api/admin/share/status").json()["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 1 and type(row["revoked"]) is int


# ── /api/share/login & /logout ─────────────────────────────────────────────


def test_login_success_sets_cookie_and_logout_clears_session(share_routes_client):
    _activate_share()
    invite = _seed_invite_from_share_routes()
    r = share_routes_client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE},
        headers=_ANALYST_HEADERS,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["email"] == invite["email"]
    assert "session_id" not in body
    assert body["service_ids"] == ["svcA"]
    cookie = r.headers.get("set-cookie", "")
    assert "analyst_session_id" in cookie and "secure" in cookie.lower()

    sid = _sid_from_login(r)
    share_routes_client.cookies.set("analyst_session_id", sid)
    r2 = share_routes_client.post("/api/share/logout", headers=_ANALYST_HEADERS)
    assert r2.status_code == 200
    assert tunnel.get_tunnel_manager().validate_session(sid) is None


@pytest.mark.parametrize(
    ("seed_kw", "login_body", "extra_headers", "expected_status", "expected_error"),
    [
        ({}, {"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"}, {}, 401, "invalid_credentials"),
        (None, {"email": "ghost@nowhere.tld", "passcode": _DEFAULT_PASSCODE}, {}, 401, None),
        (
            {"ip_whitelist": "10.0.0.0/8"},
            {"email": "drew@example.com", "passcode": _DEFAULT_PASSCODE},
            {"X-Forwarded-For": "203.0.113.5"},
            403,
            "ip_not_whitelisted",
        ),
    ],
)
def test_login_rejection_cases(
    share_routes_client, seed_kw, login_body, extra_headers, expected_status, expected_error
):
    _activate_share()
    if seed_kw is not None:
        _seed_invite_from_share_routes(**seed_kw)
    r = share_routes_client.post("/api/share/login", json=login_body, headers={**_ANALYST_HEADERS, **extra_headers})
    assert r.status_code == expected_status
    if expected_error:
        assert r.json()["detail"]["error"] == expected_error


def test_login_capacity_cap_blocks_when_full(share_routes_client):
    _activate_share()
    a = _seed_invite_from_share_routes(name="A", email="a@example.com", passcode=_DEFAULT_PASSCODE)
    b = _seed_invite_from_share_routes(name="B", email="b@example.com", passcode="seagull-mountain-river-77")
    share_db.set_setting("max_concurrent_analyst_sessions", "1")
    r1 = share_routes_client.post(
        "/api/share/login", json={"email": a["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r1.status_code == 200, r1.text
    r2 = share_routes_client.post(
        "/api/share/login",
        json={"email": b["email"], "passcode": "seagull-mountain-river-77"},
        headers=_ANALYST_HEADERS,
    )
    assert r2.status_code == 503 and r2.json()["detail"]["error"] == "capacity_exceeded"


# ── /api/share/acknowledge & /tos ──────────────────────────────────────────


def test_acknowledge_marks_tos_and_validates_version_and_session(share_routes_client):
    _activate_share()
    assert (
        share_routes_client.post(
            "/api/share/acknowledge", json={"version": "2026.05"}, headers=_ANALYST_HEADERS
        ).status_code
        == 401
    )
    assert share_routes_client.get("/api/share/tos", headers=_ANALYST_GET_HEADERS).status_code == 401

    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite_from_share_routes()
    r = share_routes_client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r.json()["tos_pending"] is True
    sid = _sid_from_login(r)
    share_routes_client.cookies.set("analyst_session_id", sid)

    # Mismatched version must fail with 400 invalid_tos_version (finding 021).
    r_bad = share_routes_client.post(
        "/api/share/acknowledge", json={"version": "0000.00-fabricated"}, headers=_ANALYST_HEADERS
    )
    assert r_bad.status_code == 400 and r_bad.json()["detail"]["error"] == "invalid_tos_version"
    assert share_db.get_remote_invite(invite["id"])["tos_version"] != "0000.00-fabricated"

    # Matching version succeeds.
    r2 = share_routes_client.post("/api/share/acknowledge", json={"version": tos["version"]}, headers=_ANALYST_HEADERS)
    assert r2.status_code == 200
    refreshed = share_db.get_remote_invite(invite["id"])
    assert refreshed["tos_version"] == tos["version"] and refreshed["tos_accepted_at"] is not None


def test_get_tos_returns_current_version_with_pending_cookie(share_routes_client):
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite_from_share_routes()
    r = share_routes_client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r.json()["tos_pending"] is True
    share_routes_client.cookies.clear()
    share_routes_client.cookies.set("analyst_pending_session_id", _sid_from_login(r))

    r2 = share_routes_client.get("/api/share/tos", headers=_ANALYST_GET_HEADERS)
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["version"] == tos["version"] and body["text"] == tos["text"]

    r3 = share_routes_client.post("/api/share/acknowledge", json={"version": body["version"]}, headers=_ANALYST_HEADERS)
    assert r3.status_code == 200, r3.text
    assert share_db.get_remote_invite(invite["id"])["tos_version"] == tos["version"]


# ── /api/share/heartbeat & /claim/{token} ──────────────────────────────────


def test_heartbeat_session_and_activity_tracking(share_routes_client, monkeypatch):
    _activate_share()
    assert share_routes_client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS).status_code == 401

    mgr, _sid = _login_and_get_session(share_routes_client)
    r_ok = share_routes_client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_ok.status_code == 200 and r_ok.json()["ok"] is True

    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda s, **kw: (calls.append(kw), real(s, **kw))[1])

    # Active user resets idle clock.
    r_act = share_routes_client.get("/api/share/heartbeat", headers={**_ANALYST_GET_HEADERS, "X-User-Active": "1"})
    assert r_act.status_code == 200 and calls

    # Idle user (X-User-Active: 0 or absent) does NOT touch session.
    for hdrs in ({**_ANALYST_GET_HEADERS, "X-User-Active": "0"}, _ANALYST_GET_HEADERS):
        calls.clear()
        r_idle = share_routes_client.get("/api/share/heartbeat", headers=hdrs)
        assert r_idle.status_code == 200 and not calls


def test_claim_token_one_shot_invalid_and_revoked(share_routes_client):
    assert share_routes_client.post("/api/share/claim/not-a-real-token").status_code == 404

    invite = _seed_invite_from_share_routes()
    token = share_db.create_claim_token(invite["id"], ttl_hours=1)
    r = share_routes_client.post(f"/api/share/claim/{token}")
    assert r.status_code == 200 and r.json()["email"] == invite["email"]
    r2 = share_routes_client.post(f"/api/share/claim/{token}")
    assert r2.status_code == 404 and r2.json()["detail"]["error"] == "invalid_or_used"

    rev_invite = _seed_invite_from_share_routes(email="revoked-claim@example.com")
    rev_token = share_db.create_claim_token(rev_invite["id"], ttl_hours=1)
    share_db.revoke_remote_invite(rev_invite["id"])
    r_rev = share_routes_client.post(f"/api/share/claim/{rev_token}", headers={"X-Page-Load-ID": "pl-claim-410"})
    assert r_rev.status_code == 410 and r_rev.json()["detail"]["error"] == "invite_gone"


# ── TOS Cookie Isolation, Upgrade, Rehydration & Brute-Force Protection ────


def test_tos_pending_flow_isolation_and_upgrade(share_routes_client):
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite_from_share_routes()

    # 1. Login with pending TOS sets analyst_pending_session_id ONLY.
    r_login = share_routes_client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_login.status_code == 200 and r_login.json()["tos_pending"] is True
    sid = _sid_from_login(r_login)

    login_cookies = _parse_set_cookies(r_login)
    assert "analyst_pending_session_id" in login_cookies
    assert login_cookies["analyst_pending_session_id"]["_value"] == sid
    assert login_cookies["analyst_pending_session_id"].get("max-age") == "86400"
    if "analyst_session_id" in login_cookies:
        assert login_cookies["analyst_session_id"].get("max-age") == "0"

    share_routes_client.cookies.clear()
    share_routes_client.cookies.set("analyst_pending_session_id", sid)

    # 2. Protected endpoints reject pending cookie with 401.
    r_sources = share_routes_client.get("/api/sources", params={"service": "svcA"}, headers=_ANALYST_GET_HEADERS)
    assert r_sources.status_code == 401 and r_sources.json()["error"] == "unauthenticated"

    # 3. Heartbeat works with pending cookie.
    r_hb = share_routes_client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_hb.status_code == 200 and r_hb.json()["ok"] is True

    # 4. Acknowledge TOS upgrades cookie and rotates session id.
    r_ack = share_routes_client.post(
        "/api/share/acknowledge", json={"version": tos["version"]}, headers=_ANALYST_HEADERS
    )
    assert r_ack.status_code == 200
    ack_cookies = _parse_set_cookies(r_ack)
    assert "analyst_session_id" in ack_cookies
    full_attrs = ack_cookies["analyst_session_id"]
    assert full_attrs.get("max-age") == "86400"
    new_sid = full_attrs["_value"]
    assert new_sid and new_sid != sid
    assert ack_cookies["analyst_pending_session_id"].get("max-age") == "0"

    share_routes_client.cookies.clear()
    share_routes_client.cookies.set("analyst_session_id", new_sid)

    # 5. Protected endpoints pass middleware (404 on test-only router).
    assert (
        share_routes_client.get("/api/sources", params={"service": "svcA"}, headers=_ANALYST_GET_HEADERS).status_code
        == 404
    )

    # 6. Pre-rotation session id is no longer valid.
    share_routes_client.cookies.clear()
    share_routes_client.cookies.set("analyst_session_id", sid)
    assert share_routes_client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS).status_code == 401


def test_on_demand_session_rehydration(share_routes_client):
    _activate_share()
    invite = _seed_invite_from_share_routes()
    r_login = share_routes_client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_login.status_code == 200
    sid = _sid_from_login(r_login)

    mgr = get_tunnel_manager()
    with mgr._lock:
        assert sid in mgr._sessions
        del mgr._sessions[sid]

    share_routes_client.cookies.clear()
    share_routes_client.cookies.set("analyst_pending_session_id", sid)
    r_hb = share_routes_client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_hb.status_code == 200 and r_hb.json()["ok"] is True
    with mgr._lock:
        assert sid in mgr._sessions


def test_login_rate_limiting_and_audit_email_sanitization(share_routes_client):
    from backend.routers.share_auth import _safe_audit_email

    # L3: audit email sanitization
    assert _safe_audit_email("a@b.com") == "a@b.com"
    assert _safe_audit_email(None) == "-" and _safe_audit_email("") == "-"
    out = _safe_audit_email("a@b.com\nLOGIN_SUCCESS forged-line")
    assert "\n" not in out and "\r" not in out
    assert len(_safe_audit_email("x" * 9999)) <= 254

    # L2: successful login does not clear failure counter (finding 016)
    _activate_share()
    _seed_invite_from_share_routes(email="l2@example.com")
    rl = tunnel.get_tunnel_manager()._rate_limiter
    rl.record_failure("127.0.0.1")
    r_ok = share_routes_client.post(
        "/api/share/login", json={"email": "l2@example.com", "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_ok.status_code == 200 and rl._events.get("127.0.0.1")

    # Lockout after 5 failures returns 429 with retry_after_s and Retry-After header
    _seed_invite_from_share_routes(email="lockout-target@example.com")
    headers = {**_ANALYST_HEADERS, "X-Forwarded-For": "198.51.100.77", "X-Page-Load-ID": "pl-lockout-test"}
    for _ in range(5):
        r_fail = share_routes_client.post(
            "/api/share/login",
            json={"email": "lockout-target@example.com", "passcode": "wrong-passcode"},
            headers=headers,
        )
        assert r_fail.status_code == 401

    r_locked = share_routes_client.post(
        "/api/share/login", json={"email": "lockout-target@example.com", "passcode": _DEFAULT_PASSCODE}, headers=headers
    )
    assert r_locked.status_code == 429
    detail = r_locked.json()["detail"]
    assert detail["error"] == "rate_limited" and detail["retry_after_s"] > 0
    assert int(r_locked.headers.get("retry-after", "0")) > 0


def test_auth_config_returns_typed_schema_with_page_load_id(share_routes_client):
    r = share_routes_client.get("/api/share/auth-config", headers={"X-Page-Load-ID": "pl-auth-cfg-1"})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"passcode_enabled", "providers"}
    assert isinstance(body["passcode_enabled"], bool) and isinstance(body["providers"], list)
