from __future__ import annotations

"""Integration tests for RemoteAccessMiddleware.

Uses TestClient against the real ``backend.main.app`` with the share DB
isolated per-test. The middleware classifies requests via socket peer +
``X-Remote-Analyst`` header coordination, so tests just toggle that header
to flip between local-admin and analyst branches.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

# ── Test app with a few stub routes wired through the middleware ────────────


def _build_test_app() -> FastAPI:
    """Tiny FastAPI app with the middleware + the share auth/admin routers.

    We don't load backend.main here because it brings up the scheduler/duckdb
    initialization paths. The middleware logic doesn't need them.
    """
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/views")
    def _create_view():
        return {"ok": True}

    # Analyst-allowed read-shaped POST (filters live in the JSON body). Used by
    # the IP-filter PII-lock tests below.
    @app.post("/api/dashboard/aggregates")
    def _aggregates():
        return {"ok": True}

    @app.post("/api/web-vitals")
    def _web_vitals():
        return {"ok": True}

    @app.post("/api/ux-events")
    def _ux_events():
        return {"ok": True}

    @app.get("/api/cron/stream")
    def _sse():
        return {"ok": True}

    @app.get("/api/cron-runs/{run_id}/stream")
    def _cron_run_stream(run_id: str):
        return {"ok": True}

    # Analyst-allowlisted SSE channel (header-badge push). Used by the
    # idle-timer exemption test below — it passes the SSE allowlist gate,
    # so it's a true positive for "reached the touch logic but skipped it".
    @app.get("/api/log-extents/stream")
    def _badge_stream():
        return {"ok": True}

    @app.get("/api/services/{service_id}/scoring/status")
    def _scoring_status(service_id: str):
        return {"ok": True, "service_id": service_id}

    # S-1: Control Room's live metrics feed. Parameterized by service_id,
    # so it's a suffix (not exact-string) allowlist match — see
    # _ANALYST_SSE_ALLOWLIST's "/realtime-stream" entry.
    @app.get("/api/services/{service_id}/realtime-stream")
    def _realtime_stream(service_id: str):
        return {"ok": True, "service_id": service_id}

    # A hypothetical hyphenated stream route NOT on the allowlist — must
    # stay blocked even though it's shaped just like realtime-stream.
    @app.get("/api/services/{service_id}/cost-governor-stream")
    def _cost_governor_stream(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/alerts/{service_id}")
    def _alerts_for_service(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/custom-endpoint/{service_id}/data")
    def _custom_endpoint(service_id: str):
        return {"ok": True, "service_id": service_id}

    # RUM suffix gate: admin-only mutate/config-disclosure endpoints.
    @app.post("/api/services/{service_id}/rum/enable")
    def _rum_enable(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.post("/api/services/{service_id}/rum/disable")
    def _rum_disable(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/rum/status")
    def _rum_status(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/rum/versions")
    def _rum_versions(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.post("/api/services/{service_id}/rum/upgrade")
    def _rum_upgrade(service_id: str):
        return {"ok": True, "service_id": service_id}

    # Analyst-NEEDED RUM reads — must NOT be shadowed by the suffix gate.
    @app.get("/api/services/{service_id}/rum/beacon-health")
    def _rum_beacon_health(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/rum/analytics")
    def _rum_analytics(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/rum/live-events")
    def _rum_live_events(service_id: str):
        return {"ok": True, "service_id": service_id}

    # H-1: usage / cost surface (entire /api/usage/ tree is admin-only).
    @app.get("/api/usage/summary")
    def _usage_summary():
        return {"ok": True}

    # H-2: raw object download endpoints.
    @app.get("/api/download")
    def _download():
        return {"ok": True}

    @app.get("/api/download-all")
    def _download_all():
        return {"ok": True}

    @app.get("/api/download-folder")
    def _download_folder():
        return {"ok": True}

    # Negative-control sibling for the /api/download exact-match — must
    # NOT get blocked by a naive prefix check.
    @app.get("/api/download-foo")
    def _download_foo():
        return {"ok": True}

    # H-3: per-service config / cron leakage.
    @app.get("/api/cron-schedule")
    def _cron_schedule():
        return {"ok": True}

    @app.get("/api/services/{service_id}/lake-info")
    def _lake_info(service_id: str):
        return {"ok": True, "service_id": service_id}

    # H-4: scoring admin-config endpoints + the analyst-needed reads that
    # must stay reachable.
    @app.get("/api/services/{service_id}/scoring/config")
    def _scoring_config(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/audit")
    def _scoring_audit(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/threshold")
    def _scoring_threshold(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/exclude-regex")
    def _scoring_exclude(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/enforce-status-code")
    def _scoring_enforce(service_id: str):
        return {"ok": True, "service_id": service_id}

    # Analyst-NEEDED scoring reads — these must NOT be blocked by the
    # suffix gate. The flag column / modal / dashboard depend on them.
    @app.get("/api/services/{service_id}/scoring/labels")
    def _scoring_labels(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/sessions/{sid}/events")
    def _scoring_sess_events(service_id: str, sid: str):
        return {"ok": True, "service_id": service_id, "sid": sid}

    @app.get("/api/services/{service_id}/scoring/top-flagged")
    def _scoring_top(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/threshold-preview")
    def _scoring_threshold_preview(service_id: str):
        # /threshold-preview is intentionally NOT in the suffix block list:
        # the threshold is caller-supplied via query param, and the
        # response is equivalent in sensitivity to /score-distribution
        # which analysts already see.
        return {"ok": True, "service_id": service_id}

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
    """Mark the tunnel manager as sharing so X-Remote-Analyst is honored."""
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://testserver")
    return mgr


def _login_analyst(client, invite, *, host="testserver") -> str:
    """Log in and return the session_id (also installs it on the TestClient
    cookie jar so subsequent calls carry it over plain HTTP)."""
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
    # L1: the session id is no longer in the JSON body — it's the bearer token
    # in the httponly cookie (pending before TOS accept, full after). Pull it
    # from the response Set-Cookie. The TestClient jar refuses `secure` cookies
    # over http://, so set analyst_session_id manually for follow-up calls.
    sid = ""
    for raw in r.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')  # the deletion cookie carries value="" (quoted)
        if name in ("analyst_pending_session_id", "analyst_session_id") and val:
            sid = val
    client.cookies.set("analyst_session_id", sid)
    return sid


# ── DNS-rebinding gate ─────────────────────────────────────────────────────


def test_local_request_with_non_local_host_header_rejected(client):
    """A 127.0.0.1 connection that sends ``Host: evil.com`` is the
    DNS-rebinding shape we refuse."""
    r = client.get("/api/dashboard", headers={"Host": "evil.com"})
    assert r.status_code == 400
    assert r.json()["error"] == "host_not_allowed"


def test_local_request_with_local_host_passes(client):
    r = client.get("/api/dashboard")
    assert r.status_code == 200


def test_remote_request_unknown_host_rejected(client):
    """When sharing is active but Host doesn't match the registered endpoint."""
    _start_share()
    r = client.get(
        "/api/dashboard",
        headers={"X-Remote-Analyst": "1", "Host": "random.lhr.life"},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "host_not_allowed"


def test_remote_frontend_configured_host_allowed(client, monkeypatch):
    """A host configured in a service's remote_frontend block is permitted."""
    _start_share()
    fake_config = [
        {
            "service_id": "svc_test_rf",
            "remote_frontend": {
                "service_id": "fastly_test_123",
                "domain_name": "fla-test.global.ssl.fastly.net",
            },
        }
    ]
    monkeypatch.setattr("backend.config.list_configs", lambda: fake_config)
    r = client.get(
        "/api/dashboard",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "fla-test.global.ssl.fastly.net",
        },
    )
    # The host check must succeed (not return 400 host_not_allowed).
    # Since it's an unauthenticated remote request to /api/dashboard, it receives 401 unauthenticated.
    assert r.status_code == 401
    assert r.json()["error"] == "unauthenticated"


# ── Local pass-through ─────────────────────────────────────────────────────


def test_local_admin_can_hit_admin_paths(client):
    """No X-Remote-Analyst header → local-admin → admin endpoints work."""
    r = client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text


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
    _start_share()
    _login_analyst(client, _seed_invite())
    headers = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}
    allowed = client.get("/api/dashboard?service_id=svcA", headers=headers)
    assert allowed.status_code == 200, allowed.text
    response = client.request(method, path, headers=headers, json={"service_id": "svcA", "dataset_id": "dataset"})
    assert response.status_code == 403
    assert response.json()["error"] == "admin_only"


def test_local_admin_writes_pass_through(client):
    r = client.post("/api/views")
    assert r.status_code == 200


@pytest.mark.security_regression
def test_gateway_required_keeps_authenticated_analyst_restrictions(client, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "test-gateway-secret-with-at-least-32-characters")
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    headers = {"X-Remote-Analyst": "1", "Host": "testserver"}
    allowed = client.get("/api/dashboard", headers=headers, params={"service_id": "svcA"})
    assert allowed.status_code == 200
    denied = client.get("/api/admin/share/status", headers=headers)
    assert denied.status_code == 403
    assert denied.json()["error"] == "admin_only"


# ── Analyst path ───────────────────────────────────────────────────────────


def test_analyst_without_session_blocked(client):
    """No cookie → 401, never 500."""
    _start_share()
    r = client.get(
        "/api/dashboard",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 401
    assert r.json()["error"] == "unauthenticated"


def test_analyst_blocked_from_admin_path(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/admin/share/status",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "admin_only"


def test_analyst_blocked_from_sse_route(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/cron/stream",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "sse_blocked"


def test_analyst_allowed_on_realtime_stream(client):
    """S-1: /realtime-stream is a hyphenated *-stream route (not an exact
    "/stream" suffix) — _is_sse_route now correctly classifies it as SSE
    (closing the hyphenated-route detection gap), so it must be listed on
    _ANALYST_SSE_ALLOWLIST as a suffix match (it's parameterized by
    service_id, not an exact path) to keep passing the gate it was
    previously bypassing by accident rather than by design."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/services/svcA/realtime-stream",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


def test_analyst_blocked_from_unlisted_hyphenated_stream_route(client):
    """Counterpart: a hyphenated *-stream route that ISN'T on the
    allowlist must still be blocked — the fix closes the detection gap
    for _is_sse_route generally, not just for realtime-stream
    specifically. Default-closed, explicit-allow-only stays intact."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/services/svcA/cost-governor-stream",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "sse_blocked"


def test_analyst_blocked_from_admin_events_stream(client):
    """The multiplexed admin event stream carries admin-only channels
    (sync-status, cron-runs, system-metrics). It lives under ``/api/admin/``
    so the prefix block 403s analysts before the SSE allowlist is even
    consulted — pins that the consolidation didn't open an analyst hole."""
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/admin/events/stream",
        params={"channels": "sync-status,cron-runs,system-metrics"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "admin_only"


def test_analyst_read_only_blocks_writes(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.post(
        "/api/views",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "read_only"


def test_analyst_put_patch_delete_blocked_even_on_allowed_prefix(client):
    """Regression for audit finding 005: the analyst read-only gate previously
    grouped PUT/PATCH/DELETE with POST and let them through whenever the path
    matched _ANALYST_ALLOWED_WRITE_PREFIXES (POST-allowed read-shaped query
    endpoints under /api/dashboard, etc.). PUT/PATCH/DELETE must be rejected
    unconditionally — the allowlist only applies to POST."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    for method in ("put", "patch", "delete"):
        r = getattr(client, method)(
            "/api/dashboard/some-mutating-endpoint?service=svcA",
            headers={
                "X-Remote-Analyst": "1",
                "Host": "testserver",
                "Origin": "https://testserver",
            },
        )
        assert r.status_code == 403, f"{method.upper()} should be 403, got {r.status_code}"
        assert r.json()["error"] == "read_only"


def test_analyst_service_scope_blocks_unauthorized(client):
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/dashboard?service=svcZ",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "service_not_authorized"


def test_analyst_service_scope_allows_authorized(client):
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200


# ── PII lock: masking analysts cannot filter by an IP-family column ─────────


def _post_filter(client, *, filters):
    """POST an analytics body carrying ``filters`` as an authenticated analyst."""
    return client.post(
        "/api/dashboard/aggregates",
        json={"service_id": "svcA", "filters": filters},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )


def _post_field(client, *, field):
    """POST an analytics body carrying a top-level ``field`` (the field-values
    enumeration dimension) as an authenticated analyst. The PII lock is
    path-agnostic within the analyst-allowed /api/dashboard/ prefix, so the
    stub aggregates route stands in for /api/dashboard/field-values."""
    return client.post(
        "/api/dashboard/aggregates",
        json={"service_id": "svcA", "field": field},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )


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
        # case variants (DuckDB identifiers are case-insensitive)
        "IP",
        "Cookie_Session",
        "Client_IP",
    ],
)
def test_analyst_masking_blocks_pii_field_values_enumeration(client, field):
    """A mask_ips analyst must not ENUMERATE a PII column's distinct values via
    the field-values ``field`` param. Even with values masked, the per-value
    COUNT + search-prefix matching form an enumeration oracle, so reject at the
    boundary like a PII filter key — including junk-suffixed evasions."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_field(client, field=field)
    assert r.status_code == 403, (field, r.status_code, r.text)
    assert r.json()["error"] == "pii_policy_violation", (field, r.text)


@pytest.mark.security_regression
def test_analyst_masking_allows_non_pii_field_values_enumeration(client):
    """Enumerating a non-PII dimension (url) stays allowed — the dimension lock
    must not over-block legitimate field-values pickers."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_field(client, field="url")
    assert r.status_code == 200, r.text


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["ip", "filter_ip", "xfilter_ip", "ip_2", "client_ip"])
def test_analyst_masking_blocks_ip_filter(client, key):
    """A masking analyst filtering by ANY IP-family column (incl. prefixed /
    dedup-suffixed variants the SQL builder normalizes) is rejected with 403
    pii_policy_violation. Display masking is response-side only, so an
    un-blocked ip filter would be a presence oracle for a guessed real IP."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["1.2.3.4"]}})
    assert r.status_code == 403, (key, r.text)
    assert r.json()["error"] == "pii_policy_violation"
    # field is the NORMALIZED column (prefixes + dedup suffix stripped).
    assert r.json()["field"] == ("client_ip" if key == "client_ip" else "ip")


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["cookie_session", "filter_cookie_session", "cookie_session_2"])
def test_analyst_masking_blocks_cookie_session_filter(client, key):
    """Phase-4 Track C: a masking analyst filtering by the session-id column
    (incl. prefixed / dedup-suffixed variants the SQL builder normalizes) is
    rejected with 403 — the same presence-oracle guard as the IP filter-lock,
    so a mask_ips analyst can't probe for / enumerate a specific session hash."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert r.status_code == 403, (key, r.text)
    assert r.json()["error"] == "pii_policy_violation"
    # field is the NORMALIZED column (prefixes + dedup suffix stripped).
    assert r.json()["field"] == "cookie_session"


@pytest.mark.security_regression
def test_analyst_masking_allows_non_ip_filter(client):
    """A masking analyst can still use non-IP filters (e.g. country)."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={"country": {"mode": "include", "values": ["US"]}})
    assert r.status_code == 200, r.text


@pytest.mark.security_regression
def test_analyst_masking_allows_oip_filter(client):
    """``oip`` (origin/CDN IP) is operator infra, not end-user PII, and stays
    filterable even for a masking analyst — the Origin IP-Health drill-down
    depends on it. Pinned alongside the ip-block so the two never converge."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={"oip": {"mode": "include", "values": ["151.101.1.51"]}})
    assert r.status_code == 200, r.text


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "key",
    [
        # Trailing / embedded non-word chars that ``normalize_filter_key`` does
        # NOT strip but ``build_where_clause``'s ``_SAFE_COL_RE.sub("", col)``
        # DOES — so the middleware lock (which only runs normalize_filter_key)
        # sees a "different" column while the SQL builder still filters the REAL
        # cookie_session / ip column. Each of these MUST be blocked.
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
    """Regression: the presence-oracle filter-lock must normalize a filter key
    the SAME way ``build_where_clause`` resolves it to a real column.

    ``build_where_clause`` applies ``normalize_filter_key`` AND THEN
    ``_SAFE_COL_RE.sub("", col)`` (strip every non-word char), but the analyst
    filter-lock in ``remote_access`` applies ONLY ``normalize_filter_key``.
    A key like ``"cookie_session."`` therefore slips past the lock
    (normalized == "cookie_session." not in the forbidden set) yet still emits
    ``CAST(cookie_session AS VARCHAR) IN (?)`` in the WHERE clause — a presence
    oracle / correlation vector on the very session-id (and IP) columns the
    mask_ips policy exists to protect. It must 403 pii_policy_violation."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": True})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={key: {"mode": "include", "values": ["a1b2c3d4deadbeef"]}})
    assert r.status_code == 403, (key, r.status_code, r.text)
    assert r.json()["error"] == "pii_policy_violation", (key, r.text)


def test_analyst_without_masking_allows_ip_filter(client):
    """A NON-masking analyst sees real IPs, so filtering by ip is allowed —
    the lock is gated on pii_policy.mask_ips, not on analyst-hood."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"], pii_policy={"mask_ips": False})
    _login_analyst(client, invite)
    r = _post_filter(client, filters={"ip": {"mode": "include", "values": ["1.2.3.4"]}})
    assert r.status_code == 200, r.text


def test_analyst_service_scope_blocks_omitted(client):
    """If service is omitted, the middleware resolves the effective service ID
    via get_active_service_id() and validates it, blocking if unauthorized."""
    from unittest.mock import patch

    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    with patch("backend.config.get_active_service_id", return_value="svcB"):
        r2 = client.get(
            "/api/dashboard",
            headers={"X-Remote-Analyst": "1", "Host": "testserver"},
        )
    assert r2.status_code == 403
    assert r2.json()["error"] == "service_not_authorized"


def test_analyst_path_param_service_blocked_when_unauthorized(client):
    """Audit finding 006: an analyst scoped only to svcA must NOT be able to
    read /api/services/svcB/scoring/labels by relying on the active-service
    fallback to satisfy the per-request scope check while the path parameter
    targets a different service. The middleware now extracts the service ID
    from known path templates.

    Uses /scoring/labels (analyst-allowed read) rather than /scoring/status
    (admin-only) so the failure-mode under test is the scope check, not the
    admin-suffix block.
    """
    from unittest.mock import patch

    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    # Active default points at svcA (analyst's authorized service) — the
    # pre-fix code would resolve svcA, pass the scope gate, and forward the
    # request to the path-svcB route handler.
    with patch("backend.config.get_active_service_id", return_value="svcA"):
        r = client.get(
            "/api/services/svcB/scoring/labels",
            headers={"X-Remote-Analyst": "1", "Host": "testserver"},
        )
    assert r.status_code == 403
    assert r.json()["error"] == "service_not_authorized"
    assert r.json()["service"] == "svcB"


def test_analyst_path_param_service_allowed_when_authorized(client):
    """Mirror of the above: when the analyst IS authorized for the
    path-param service, the request goes through. Uses /scoring/labels
    (analyst-allowed) — /scoring/status is admin-only post-H-4."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA", "svcB"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/services/svcB/scoring/labels",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200
    assert r.json()["service_id"] == "svcB"


def test_analyst_path_alerts_service_blocked_when_unauthorized(client):
    """``/api/alerts`` is now in ``_ANALYST_BLOCKED_PREFIXES`` (H-7,
    2026-06-10): the entire alerts surface is operator-only per directive.
    The prefix block fires before the per-service scope check, so the
    response code is now ``admin_only`` rather than the older
    ``service_not_authorized``. Both are 403 from the analyst's POV."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/alerts/svcB",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "admin_only"


def test_analyst_path_and_query_service_must_both_be_authorized(client):
    """If the request carries svcA in the query AND svcB in the path,
    BOTH must be in the analyst's allowlist. Previously the middleware only
    checked the query candidate. Uses /scoring/labels (analyst-allowed) so
    the failure-mode under test is the scope desync, not the admin-suffix
    block."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/services/svcB/scoring/labels?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "service_not_authorized"


def test_analyst_custom_un_regexed_route_desync_blocked(client):
    """Ensure custom routes with custom un-regexed prefixes with service_id path parameters
    are fully protected from path-to-query desync bypass attempts by route-matching."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/custom-endpoint/svcB/data?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "service_not_authorized"

    # But if authorized, it should work
    r2 = client.get(
        "/api/custom-endpoint/svcA/data?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200
    assert r2.json()["service_id"] == "svcA"


# ── Expanded analyst-blocked paths (H-1 through H-4) ──────────────────────


def test_analyst_blocked_from_usage_surface(client):
    """H-1: /api/usage/* exposes cost/billing data — operator-only."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/usage/summary",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/download",
        "/api/download-all",
        "/api/download-folder",
    ],
)
def test_analyst_blocked_from_download_endpoints(client, path):
    """H-2: raw download endpoints are admin-only."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        path,
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403, f"{path} expected 403, got {r.status_code}: {r.text}"
    assert r.json()["error"] == "admin_only"


def test_analyst_download_block_does_not_prefix_match_sibling(client):
    """H-2 follow-up: a bare /api/download entry must not block /api/download-foo.
    The exact-subpath check requires path == p, path startswith p+"/", or p+"?".
    """
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/download-foo",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    # The sibling endpoint should be reachable (200) — the middleware's
    # service-scope gate doesn't fire because there's no service in the
    # path/query/headers. (Plain pass-through GET.)
    assert r.status_code != 403 or r.json().get("error") != "admin_only", (
        f"/api/download-foo should not be admin-blocked; got {r.status_code} {r.text}"
    )


def test_analyst_blocked_from_cron_schedule(client):
    """H-3: per-service cron cadence config is admin-only."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/cron-schedule",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "admin_only"


def test_analyst_blocked_from_lake_info(client):
    """H-3: Iceberg/object-store layout for a service is admin-only.
    Even when the analyst is authorized for the service, the lake-info
    route is still gated."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        "/api/services/svcA/lake-info",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "suffix",
    [
        "/config",
        "/status",
        "/audit",
        "/threshold",
        "/exclude-regex",
        "/enforce-status-code",
    ],
)
def test_analyst_blocked_from_scoring_admin_suffix(client, suffix):
    """H-4: scoring admin-config endpoints (suffix gate). Authorizing the
    analyst for the service must NOT bypass the suffix block."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        f"/api/services/svcA/scoring{suffix}",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403, f"{suffix}: expected 403, got {r.status_code}: {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svcA/scoring/labels",
        "/api/services/svcA/scoring/sessions/sess-1/events",
        "/api/services/svcA/scoring/top-flagged",
        "/api/services/svcA/scoring/threshold-preview",
    ],
)
def test_analyst_NOT_blocked_from_scoring_reads_they_need(client, path):
    """H-4 negative-control: the flag column, modal, and dashboard rely on
    these endpoints — the suffix gate must NOT shadow them."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        path,
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200, f"{path} should be reachable; got {r.status_code}: {r.text}"


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
    """RUM suffix gate, same shape as H-4's scoring gate. Pins the fix for a
    real pre-existing gap: before this gate was added, /rum/enable and
    /rum/disable had no ``/rum/`` entry anywhere in the analyst blocklists
    and were reachable by an authenticated analyst even though they mutate
    deployed edge config. Authorizing the analyst for the service (svcA)
    must NOT bypass the suffix block. /rum/status is deliberately NOT in
    this list (F1 audit fix) — see test_analyst_NOT_blocked_from_rum_reads_they_need."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.request(
        method,
        path,
        # Origin header needed so POSTs clear the CSRF origin gate and reach
        # the admin_only suffix check this test targets, rather than
        # tripping the (separately-tested) origin_not_allowed gate first.
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.status_code == 403, f"{path}: expected 403, got {r.status_code}: {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svcA/rum/status",
        "/api/services/svcA/rum/beacon-health",
        "/api/services/svcA/rum/analytics",
        "/api/services/svcA/rum/live-events",
    ],
)
def test_analyst_NOT_blocked_from_rum_reads_they_need(client, path):
    """Negative control: RUM dashboards and the beacon-health setup check
    rely on these read-only endpoints — the suffix gate must NOT shadow
    them. /rum/status was blocked pre-fix (F1 audit finding), which made
    the analyst RUM page 403 → dead on every load."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.get(
        path,
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200, f"{path} should be reachable; got {r.status_code}: {r.text}"


# ── Origin gate ────────────────────────────────────────────────────────────


def test_analyst_write_origin_must_match(client):
    """Writes from a foreign Origin (CSRF shape) are rejected."""
    _start_share()
    invite = _seed_invite()
    # Use direct share-login POST, which is allowed but goes through origin check.
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://attacker.example.com",
        },
    )
    assert r.status_code == 403
    assert r.json()["error"] == "origin_not_allowed"


# ── Login rate limiter ─────────────────────────────────────────────────────


def test_login_rate_limit_triggers_after_threshold(client):
    _start_share()
    _seed_invite()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        client.post(
            "/api/share/login",
            json={"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"},
            headers={
                "X-Remote-Analyst": "1",
                "Host": "testserver",
                "Origin": "https://testserver",
            },
        )
    r = client.post(
        "/api/share/login",
        json={"email": "drew@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 429
    assert r.json()["detail"]["error"] == "rate_limited"


@pytest.mark.security_regression
def test_share_login_brute_force_lockout_shape_matches_e2e(client):
    """Mirror of frontend/e2e/zz-analyst-share-login-lockout.spec.ts: 35 wrong
    passcodes -> 401s up to the threshold, then 429 {error, retry_after_s}."""
    _start_share()
    headers = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}
    statuses, first_429 = [], None
    for i in range(35):
        r = client.post(
            "/api/share/login", json={"email": "attacker@example.com", "passcode": f"wrong-{i}"}, headers=headers
        )
        statuses.append(r.status_code)
        if r.status_code == 429 and first_429 is None:
            first_429 = r.json()["detail"]
    assert statuses[: tunnel.LOGIN_FAILURE_THRESHOLD] == [401] * tunnel.LOGIN_FAILURE_THRESHOLD
    assert set(statuses[tunnel.LOGIN_FAILURE_THRESHOLD :]) == {429}
    assert first_429 is not None
    assert first_429["error"] == "rate_limited"
    assert isinstance(first_429["retry_after_s"], int) and 0 < first_429["retry_after_s"] <= tunnel.LOGIN_LOCKOUT_S


# ── Hardening headers ──────────────────────────────────────────────────────


def test_remote_response_carries_hardening_headers(client):
    _start_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.headers.get("cache-control") == "private, no-store"
    assert r.headers.get("x-frame-options") == "DENY"
    assert r.headers.get("referrer-policy") == "no-referrer"


# ── Telemetry beacons must not extend the analyst idle timer ────────────────


def test_normal_analyst_request_touches_session(client, monkeypatch):
    """A real analyst request bumps last_active_time (sliding idle reset)."""
    invite = _seed_invite()
    _start_share()
    _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200, r.text
    assert any("last_activity" in kw for kw in calls), "normal request should touch the session"


def test_telemetry_beacon_does_not_touch_session(client, monkeypatch):
    """web-vitals / ux-events beacons reach the handler but must NOT bump
    last_active_time — a backgrounded tab beaconing can't keep a session
    alive forever (regression guard for _TELEMETRY_IDLE_EXEMPT_PREFIXES)."""
    invite = _seed_invite()
    _start_share()
    _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    for telemetry_path in ("/api/web-vitals", "/api/ux-events"):
        calls.clear()
        r = client.post(
            f"{telemetry_path}?service=svcA",
            json={"id": "x", "name": "LCP", "value": 1.0, "rating": "good"},
            headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
        )
        # Reached the handler (passed auth + scope gates), so the skip is
        # meaningful — not a 403 short-circuit before the touch logic.
        assert r.status_code != 403, f"{telemetry_path}: {r.text}"
        assert not any("last_activity" in kw for kw in calls), f"{telemetry_path} must NOT touch the session"


def test_sse_stream_does_not_touch_session(client, monkeypatch):
    """An allowlisted analyst SSE stream (the header-badge channel) reaches
    the handler but must NOT bump last_active_time.

    SSE streams are fetch-based and keep reconnecting even when the tab is
    backgrounded, re-stamping last_active_time every ~30s. Counting them as
    activity prevents the 2h idle timeout from ever firing on an open tab, so
    the session only ever hit the 24h absolute cap (observed prod: an analyst
    session whose last_activity was a perpetual GET /api/log-extents/stream).
    Regression guard for the SSE arm of the idle-timer exemption."""
    invite = _seed_invite(service_ids=["svcA"])
    _start_share()
    _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    r = client.get(
        "/api/log-extents/stream?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    # Passed auth + SSE allowlist + service-scope gates (not a 403 short-circuit
    # before the touch logic), so the skip is meaningful.
    assert r.status_code != 403, r.text
    assert not any("last_activity" in kw for kw in calls), "SSE stream must NOT touch the session"


def test_idle_signal_suppresses_touch(client, monkeypatch):
    """A normal analyst request carrying X-User-Active: 0 must NOT bump
    last_active_time. The client sends "0" when no genuine user gesture
    (mouse/keyboard/scroll) happened recently, so automated react-query
    refetches on a foreground tab (e.g. the ~12s dashboard/bundle poll the
    badge stream invalidates) can't keep an idle session alive."""
    invite = _seed_invite(service_ids=["svcA"])
    _start_share()
    _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "X-User-Active": "0"},
    )
    assert r.status_code == 200, r.text
    assert not any("last_activity" in kw for kw in calls), "X-User-Active: 0 must NOT touch the session"


def test_active_signal_touches(client, monkeypatch):
    """The complement: X-User-Active: 1 (genuine recent interaction) bumps
    last_active_time. Together with test_normal_analyst_request_touches_session
    (no header → touch) this pins the back-compat default: only an explicit
    "0" suppresses the idle reset."""
    invite = _seed_invite(service_ids=["svcA"])
    _start_share()
    _login_analyst(client, invite)
    mgr = tunnel.get_tunnel_manager()
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda sid, **kw: (calls.append(kw), real(sid, **kw))[1])
    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "X-User-Active": "1"},
    )
    assert r.status_code == 200, r.text
    assert any("last_activity" in kw for kw in calls), "X-User-Active: 1 should touch the session"


# ── Dispatch composition: idle timeout reaches the HTTP client ────────────


@pytest.mark.security_regression
def test_dispatch_idle_timeout_returns_401(client):
    """End-to-end: an analyst whose session has been idle beyond
    IDLE_TIMEOUT_S gets 401 unauthenticated from the middleware dispatch,
    not a 500 or a pass-through. Verifies that the ``validate_session``
    None return propagates correctly through the full dispatch path."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    sid = _login_analyst(client, invite)

    mgr = tunnel.get_tunnel_manager()
    session = mgr._sessions.get(sid)
    assert session is not None
    session.last_active_time = "2020-01-01T00:00:00Z"

    r = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 401
    assert r.json()["error"] == "unauthenticated"


@pytest.mark.security_regression
def test_dispatch_analyst_post_to_non_allowlisted_path_returns_403(client):
    """Dispatch composition: an authenticated analyst POSTing to a path not
    in ``_ANALYST_ALLOWED_WRITE_PREFIXES`` gets 403 read_only. The unit
    tests on ``_is_blocked_path`` don't cover the full dispatch ordering
    (host check → session → scope → read-only → handler)."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r = client.post(
        "/api/views",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 403
    assert r.json()["error"] == "read_only"


@pytest.mark.security_regression
def test_unauthenticated_telemetry_beacon_allowed_without_session(client):
    """An unauthenticated remote request to /api/web-vitals or /api/ux-events
    must bypass the session gate (regression guard for _UNAUTH_ANALYST_PATHS)."""
    _start_share()
    # Explicitly clear cookies to simulate no session
    client.cookies.clear()

    # /api/web-vitals
    r = client.post(
        "/api/web-vitals?service=svcA",
        json={"id": "unauth-1", "name": "LCP", "value": 1.0, "rating": "good"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    # Since it is exempt from auth, it should reach the handler and return 200 (not 401)
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True

    # /api/ux-events
    r = client.post(
        "/api/ux-events?service=svcA",
        json={"event": "some_ui_action", "pathname": "/share-login", "details": {}},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True


# ── Merged from test_pii_policy_evasion.py ──

import copy

import pytest

from backend.core.share_db.validation import (
    IP_FAMILY_KEYS,
    SESSION_ID_KEYS,
    apply_pii_policy,
)
from backend.repositories.utils.filters import normalize_filter_key
from backend.utils.remote_access import _PII_FORBIDDEN_FILTER_COLS

_MASK = {"mask_ips": True}


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field", ["ip", "ip.", "ip ", "ip\t", "client_ip.", "remote_addr.", "IP", "Ip", "Client_IP", "IP."]
)
def test_apply_pii_policy_masks_ip_field_values_despite_junk_suffix(field):
    """field-values echoes the analyst's raw ``field`` verbatim; the query
    resolved the real ``ip`` column by stripping non-word chars. Masking MUST
    canonicalize the same way, else ``field="ip."`` leaks raw client IPs."""
    obj = {"field": field, "values": [{"value": "203.0.113.7", "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    got = out["values"][0]["value"]
    assert got != "203.0.113.7", (field, got)
    assert got.endswith("xxx"), (field, got)  # last octet zeroed by mask_ip


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
    """Same evasion on the session-id column — a raw SHA-256 session hash must
    never reach a mask_ips analyst regardless of the echoed field spelling."""
    obj = {"field": field, "values": [{"value": "a" * 64, "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    assert out["values"][0]["value"] == "[redacted]", (field, out)


@pytest.mark.security_regression
def test_apply_pii_policy_leaves_non_pii_field_values_verbatim():
    """A non-PII dimension (url) is untouched — the canonicalization must not
    over-mask legitimate columns."""
    obj = {"field": "url", "values": [{"value": "/index.html", "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    assert out["values"][0]["value"] == "/index.html"


@pytest.mark.security_regression
def test_apply_pii_policy_noop_without_mask_ips():
    """No policy → no masking (a non-masking analyst / admin sees raw values)."""
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
        # case variants — DuckDB resolves identifiers case-insensitively, so the
        # lock must too (else filters={"IP":...} / {"Cookie_Session":...} slips).
        "IP",
        "Ip",
        "Client_IP",
        "Cookie_Session",
        "COOKIE_SESSION",
        "filter_Cookie_Session.",
    ],
)
def test_normalize_filter_key_resolves_junk_pii_keys_into_forbidden_set(key):
    """The shared invariant behind BOTH middleware locks (filter keys and the
    field-values dimension): ``normalize_filter_key`` must resolve any junk
    variant of a PII column to the bare column so the forbidden-set membership
    check can't be evaded."""
    assert normalize_filter_key(key) in _PII_FORBIDDEN_FILTER_COLS, key


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["url", "status", "country", "oip", "waf_sig_ind", "_bot_name"])
def test_normalize_filter_key_leaves_non_pii_keys_allowed(key):
    """The strip is a no-op for legitimate word-only columns — it must not start
    blocking non-PII filters (waf_sig_ind / _bot_name are word-only)."""
    assert normalize_filter_key(key) not in _PII_FORBIDDEN_FILTER_COLS, key


def test_forbidden_set_covers_both_pii_families():
    """Sanity: the forbidden set the locks compare against is exactly the union
    of the two PII key families the masking policy protects."""
    assert _PII_FORBIDDEN_FILTER_COLS == IP_FAMILY_KEYS | SESSION_ID_KEYS


# ── Merged from test_mask_ip_scalar_padding.py ──

import pytest

from backend.core.share_db.validation import _mask_ip_scalar, mask_ip


@pytest.mark.security_regression
@pytest.mark.parametrize("padded", ["  1.2.3.4", "1.2.3.4\n", "\t1.2.3.4 \r\n", " 2001:db8::1 "])
def test_padded_ip_is_masked_and_padding_is_not_preserved(padded):
    out = _mask_ip_scalar(padded)
    assert out == mask_ip(padded.strip())
    assert out == out.strip()


@pytest.mark.security_regression
def test_xff_list_is_normalised_to_comma_space_separated_masked_elements():
    assert _mask_ip_scalar("1.2.3.4,5.6.7.8") == ", ".join([mask_ip("1.2.3.4"), mask_ip("5.6.7.8")])
    assert _mask_ip_scalar(" 1.2.3.4 ,\n5.6.7.8 ") == ", ".join([mask_ip("1.2.3.4"), mask_ip("5.6.7.8")])


@pytest.mark.security_regression
def test_non_ip_values_keep_their_padding_verbatim():
    assert _mask_ip_scalar("  hello world\n") == "  hello world\n"
    assert _mask_ip_scalar("1.2.3.4, not-an-ip ") == "1.2.3.4, not-an-ip "


# ── Merged from test_admin_gateway.py ──────────────────────────────────────────────────────

from unittest.mock import MagicMock

import pytest
from fastapi import Request
from starlette.requests import Request as StarletteRequest

from backend.utils.remote_access import (
    is_request_remote,
    validate_admin_gateway_config,
)

SECRET = "test-gateway-secret-with-at-least-32-characters"
HEADER = "X-Admin-Gateway-Token"


def _request(peer: str, headers: dict[str, str] | None = None) -> StarletteRequest:
    return StarletteRequest(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/bootstrap",
            "headers": [(key.lower().encode(), value.encode()) for key, value in (headers or {}).items()],
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
        return {
            "is_remote": is_request_remote(request),
            "credential_visible": HEADER in request.headers,
        }

    with TestClient(app) as client:
        yield client


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


def test_legacy_loopback_behavior_remains_when_gateway_unconfigured(monkeypatch):
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    assert not is_request_remote(_request("127.0.0.1"))
    assert is_request_remote(_request("203.0.113.1"))


def test_gateway_scrubs_credentials_and_retains_authenticated_request_state(gateway_client):
    response = gateway_client.get("/api/admin/recheck", headers={HEADER: SECRET})
    assert response.status_code == 200
    assert response.json() == {"is_remote": False, "credential_visible": False}


@pytest.mark.security_regression
def test_forged_gateway_header_is_rejected_even_on_loopback(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: "forged"})
    assert response.status_code == 401
    assert response.json() == {"error": "admin_gateway_invalid"}


@pytest.mark.security_regression
def test_uncredentialed_loopback_cannot_reach_admin_in_required_mode(gateway_client):
    response = gateway_client.get("/api/admin/probe")
    assert response.status_code == 401
    assert response.json()["error"] == "unauthenticated"


def test_required_gateway_does_not_break_cheap_liveness(gateway_client):
    response = gateway_client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.security_regression
def test_public_marker_cannot_be_promoted_with_gateway_token(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "X-Proxied-By-Caddy": "1"})
    assert response.status_code == 401
    assert response.json()["error"] == "admin_gateway_invalid"


@pytest.mark.security_regression
def test_duplicate_gateway_headers_are_rejected(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers=[(HEADER, SECRET), (HEADER, "forged")])
    assert response.status_code == 401


def test_required_gateway_without_secret_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()


def test_configured_short_gateway_secret_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "short")
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()


def test_configured_gateway_passes_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    validate_admin_gateway_config()


def test_invalid_required_mode_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "true")
    with pytest.raises(ValueError, match="must be 0 or 1"):
        validate_admin_gateway_config()


@pytest.mark.security_regression
@pytest.mark.parametrize("origin", ["https://attacker.example", "null", "https://testserver.attacker.example"])
def test_gateway_blocks_cross_origin_browser_mutations(gateway_client, origin):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": origin})
    assert response.status_code == 403
    assert response.json()["error"] == "admin_origin_not_allowed"


def test_gateway_accepts_same_origin_browser_mutations(gateway_client):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": "https://testserver"})
    assert response.status_code == 200


def test_gateway_accepts_certificate_authenticated_cli_without_origin(gateway_client):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET})
    assert response.status_code == 200


@pytest.mark.security_regression
def test_gateway_rejects_browser_cross_site_fetch_metadata(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "Sec-Fetch-Site": "cross-site"})
    assert response.status_code == 403


# ── Merged from test_route_table_tripwire.py ──────────────────────────────────────────────────────

"""Tripwire: every real route must be consciously classified for analyst reach.

Why this exists (2026-08 RBAC audit): the analyst blocklist in
``backend.utils.remote_access`` is hand-curated. A new route is analyst-
reachable by default unless someone remembers to add it to a blocked-prefix /
blocked-subpath / suffix-gate entry. That is exactly how
``/api/services/{id}/rum/status``, ``/rum/enable``, and ``/rum/disable``
shipped without gating in the first place — and ``tests/remote_access/
test_middleware.py`` could not have caught it because that suite builds its
test app from HAND-COPIED STUB ROUTES, so a real route nobody wrote a stub
for is invisible to it.

This test closes that gap by walking the REAL app's route table
(``backend.main.app.openapi()["paths"]``, the convention this repo already
uses for whole-surface OpenAPI checks — see
``tests/test_error_envelope_contract.py``) and forcing every single
``(method, path)`` into one of exactly three buckets:

  1. Blocked — ``_is_blocked_path()`` returns True for the (parameter-
     substituted) path. No inventory entry needed; the blocklist function
     itself is already covered by ``tests/routers/test_rbac_audit_fixes.py``.
  2. ``_READ_ALLOWLIST`` — a GET/HEAD route an analyst may reach, grouped by
     rationale below. This is the reviewable "what can an analyst see"
     surface.
  3. ``_WRITE_VERB_GATE_ROUTES`` — a POST route an analyst may reach ONLY
     because it isn't blocked AND its path starts with an entry in
     ``_ANALYST_ALLOWED_WRITE_PREFIXES`` (the read-shaped-POST verb gate in
     remote_access.py). Listed separately from the read allowlist — and
     cross-checked against the actual prefix set below — because a route
     landing here is NOT individually reviewed; it rides on a blanket prefix
     allowance. Treating that as equivalent to a deliberate per-route
     classification would hide exactly the kind of drift this test exists to
     catch, so ``test_write_gate_routes_actually_rely_on_the_verb_gate``
     makes the reliance explicit and asserts nothing in this bucket is
     independently blocklisted.

A route that lands in NEITHER "blocked" NOR either inventory reds this
suite — that is the entire point. See
``test_unclassified_route_fails_the_tripwire`` for a live demonstration
using a route injected straight into the real app's router (proving the
mechanism, not a copy of it).

Populated from CURRENT behavior so this passes on today's tree — it is a
tripwire for future drift, not a re-litigation of existing classifications.
Four routes below are marked FLAGGED FOR TRIAGE: the audit found them
analyst-reachable and unclassified, but nobody has confirmed whether that's
correct, so they are listed as-is (not endorsed as reviewed-safe) purely so
the suite reflects reality today.
"""

import re

import pytest

from backend.main import app as _main_app
from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES, _UNAUTH_ANALYST_PATHS, _is_blocked_path

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")

# Plausible, non-identifying placeholder values for every path parameter name
# used across the route table. No real service/session/invite IDs — this is
# a public repo (see infra-leak-sweep).
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
    """Replace every ``{param}`` segment with a plausible placeholder.

    Raises if a route introduces a path-parameter name this file doesn't
    know about yet — fail loudly rather than silently leaving a brace in
    the path (which would never match any blocklist regex and could mask
    a real gap).
    """

    def _rep(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in _PATH_PARAM_SUBSTITUTIONS:
            raise AssertionError(
                f"route path parameter {name!r} (from {path!r}) has no placeholder in "
                "_PATH_PARAM_SUBSTITUTIONS — add one so this tripwire can classify the route"
            )
        return _PATH_PARAM_SUBSTITUTIONS[name]

    return _PARAM_RE.sub(_rep, path)


def _real_routes() -> list[tuple[str, str]]:
    """Every ``(METHOD, path_template)`` from the real app's OpenAPI surface.

    Sorted so the test is deterministic regardless of dict/route
    registration order.
    """
    paths = _main_app.openapi()["paths"]
    out: list[tuple[str, str]] = []
    for path in paths:
        for method in _HTTP_METHODS:
            if method in paths[path]:
                out.append((method.upper(), path))
    return sorted(out)


# ── Bucket 2: analyst-reachable GET/HEAD reads ──────────────────────────────
#
# Each group names WHY the class of routes is analyst-appropriate. Keep this
# reviewable — new entries should read as a deliberate decision, not a bare
# string appended to make the test pass.

_READ_ALLOWLIST: set[tuple[str, str]] = {
    # Pre-session / unauthenticated share flow. Mirrors _UNAUTH_ANALYST_PATHS
    # and the /api/share/oauth/* prefix exemption in the middleware — reached
    # before any analyst session cookie exists.
    ("GET", "/api/health"),
    ("GET", "/api/bootstrap"),
    ("GET", "/api/share/auth-config"),
    ("GET", "/api/share/heartbeat"),
    ("GET", "/api/share/tos"),
    ("GET", "/api/share/oauth/authorize"),
    ("GET", "/api/share/oauth/callback"),
    # Public-facing dynamic JS assets and beacon telemetry endpoints.
    # Safe to load anonymously (no credentials required).
    ("GET", "/js/rum.js"),
    ("GET", "/js/faro-sdk.js"),
    # Reference / catalog data: no per-service secrets, needed to drive the
    # analyst UI's filter and field pickers.
    ("GET", "/api/log-fields/catalog"),
    ("GET", "/api/presets"),
    ("GET", "/api/schema"),
    ("GET", "/api/services"),
    ("GET", "/api/insight-availability"),
    ("GET", "/api/views/{service_id}"),
    # Header-badge data + its live-push channel — analyst-safe siblings of
    # the admin-only /api/sync-status / /api/sync-status/stream (see the
    # _ANALYST_SSE_ALLOWLIST comment on /api/log-extents/stream).
    ("GET", "/api/log-extents"),
    ("GET", "/api/log-extents/stream"),
    # Session-scoring analyst-safe reads: the suffix gate
    # (_ANALYST_BLOCKED_SCORING_SUFFIXES) already keeps /config, /status,
    # /audit, /threshold, /exclude-regex, /enforce-*, /matrix-versions,
    # /dashboard, and /evaluation/per-reason admin-only. Everything below is
    # the deliberately-open complement (see the module docstring on that
    # suffix gate in remote_access.py).
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
    # RUM analyst-safe reads: the /rum/ suffix gate
    # (_ANALYST_BLOCKED_RUM_SUFFIXES) blocks /enable, /disable, /versions,
    # /upgrade. /rum/status is intentionally open (route projects down to
    # {enabled, enabled_at} for analysts) and the beacon-telemetry reads
    # below carry no operator-config disclosure — see the module comment
    # above _ANALYST_BLOCKED_RUM_SUFFIXES.
    ("GET", "/api/services/{service_id}/rum/status"),
    ("GET", "/api/services/{service_id}/rum/beacon-health"),
    ("GET", "/api/services/{service_id}/rum/analytics"),
    ("GET", "/api/services/{service_id}/rum/live-events"),
    # Control Room's live metrics push channel (S-1 audit decision):
    # aggregate rps/error-rate/cache-ratio, no PII, no infra details.
    ("GET", "/api/services/{service_id}/realtime-stream"),
    # Control Room's bounded recent 4xx/5xx request-event feed (analyst-safe masked client IPs).
    ("GET", "/api/services/{service_id}/control-room/error-stream"),
    # Security proxies export: read-only CSV export of security proxies.
    ("GET", "/api/security/proxies/export"),
    # Network pop-health & security threat-intel (read-only analytical endpoints)
    ("GET", "/api/assets/aggregates"),
    ("GET", "/api/network/bundle"),
    ("GET", "/api/network/pop-health"),
    ("GET", "/api/security/bundle"),
    ("GET", "/api/security/threat-intel"),
    ("GET", "/api/query/schema/{service_id}"),
    ("GET", "/api/high-scale/request-facts"),
    ("GET", "/api/high-scale/services/{service_id}/exports/{export_id}"),
    ("GET", "/api/high-scale/services/{service_id}/queries/{job_id}"),
    # ── FLAGGED FOR TRIAGE ───────────────────────────────────────────────
    # The 2026-08 RBAC audit found these four analyst-reachable with NO
    # explicit classification anywhere (not in a blocked-prefix/subpath/
    # suffix entry, not previously reviewed as safe). They are listed here
    # ONLY so this tripwire reflects today's actual behavior — this is NOT
    # an endorsement that they should stay open. See the follow-up report
    # for the triage recommendation on each.
    ("GET", "/api/services/{service_id}/cmcd/status"),
    ("GET", "/api/services/{service_id}/control-room/wizard/state"),
    ("GET", "/api/services/{service_id}/control-room/{tab}"),
    ("GET", "/api/services/{service_id}/realtime-seed"),
}

# ── Bucket 3: mutating routes that reach the handler ONLY via the blunt
# read-only-verb gate (POST + a matching _ANALYST_ALLOWED_WRITE_PREFIXES
# prefix), not because anyone reviewed the individual route. Kept separate
# from _READ_ALLOWLIST on purpose — see the module docstring and
# test_write_gate_routes_actually_rely_on_the_verb_gate below, which proves
# the reliance rather than asserting it by fiat.
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
    """Mirror the read-only-verb gate in ``RemoteAccessMiddleware.dispatch``
    (backend/utils/remote_access.py): PUT/PATCH/DELETE are unconditionally
    refused for analysts, and POST is refused unless the path is unauthenticated
    or starts with an ``_ANALYST_ALLOWED_WRITE_PREFIXES`` entry. Kept in lockstep with that
    logic on purpose — this is what makes ``_WRITE_VERB_GATE_ROUTES`` mean
    "reachable ONLY via this gate" rather than an assumption.
    """
    if method in ("PUT", "PATCH", "DELETE"):
        return True
    if method == "POST":
        if _is_unauth_path(substituted_path):
            return False
        return not any(substituted_path.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES)
    return False


@pytest.mark.security_regression
def test_every_real_route_is_blocked_or_inventoried():
    """The tripwire. A route that is neither blocked nor in an inventory
    means someone shipped a new analyst-reachable endpoint without
    consciously classifying it — exactly the RUM/status gap this test
    exists to prevent from recurring silently."""
    uninventoried: list[str] = []
    for method, path in _real_routes():
        substituted = _substitute(path)
        if _is_blocked_path(substituted):
            continue
        if _verb_gate_blocks(method, substituted):
            continue
        if (method, path) in _ALL_INVENTORIED:
            continue
        uninventoried.append(f"{method} {path}")

    assert not uninventoried, (
        "Route(s) reachable by an authenticated analyst with NO explicit classification:\n  "
        + "\n  ".join(uninventoried)
        + "\n\nEither add a blocklist entry in backend/utils/remote_access.py "
        "(_ANALYST_BLOCKED_PREFIXES / _ANALYST_BLOCKED_SUBPATHS / a suffix gate / "
        "_ANALYST_BLOCKED_SUBPATH_REGEX) if the route should be admin-only, "
        "or add it to _READ_ALLOWLIST / _WRITE_VERB_GATE_ROUTES in this file "
        "with a comment explaining why an analyst may reach it."
    )


@pytest.mark.security_regression
def test_inventory_has_no_stale_entries():
    """Catches the opposite drift: an inventory entry for a route that no
    longer exists, or one that got blocked out from under the inventory
    (which would make the entry misleadingly imply "reviewed and open" for
    a route that's actually closed). Keeps the inventory an accurate map of
    current behavior rather than an ever-growing junk drawer."""
    real = set(_real_routes())
    stale = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if (m, p) not in real]
    assert not stale, f"Inventory entries for routes that no longer exist in the app: {stale}"

    now_blocked = [f"{m} {p}" for (m, p) in _ALL_INVENTORIED if _is_blocked_path(_substitute(p))]
    assert not now_blocked, (
        f"Inventory entries for routes that are now blocked by _is_blocked_path: {now_blocked}. "
        "Remove them from _READ_ALLOWLIST/_WRITE_VERB_GATE_ROUTES — they're covered by the "
        "blocklist now, the inventory entry is stale."
    )


@pytest.mark.security_regression
def test_write_gate_routes_actually_rely_on_the_verb_gate():
    """Makes explicit what _WRITE_VERB_GATE_ROUTES membership actually means:
    every entry is a POST route, is NOT individually blocklisted, and is
    reachable ONLY because its path starts with an
    _ANALYST_ALLOWED_WRITE_PREFIXES entry. If a future change moves one of
    these onto an explicit per-route review, move it to _READ_ALLOWLIST
    (renaming aside, that bucket means "reviewed"); if the write-prefix set
    ever stops covering one of these paths the route becomes blocked and
    belongs in neither bucket — test_inventory_has_no_stale_entries catches
    that flip.
    """
    for method, path in _WRITE_VERB_GATE_ROUTES:
        assert method == "POST", f"{method} {path} is in _WRITE_VERB_GATE_ROUTES but isn't a POST route"
        substituted = _substitute(path)
        assert not _is_blocked_path(substituted), (
            f"{method} {path} is individually blocklisted — it doesn't need the verb gate, "
            "this entry is redundant/misclassified"
        )
        assert any(substituted.startswith(pfx) for pfx in _ANALYST_ALLOWED_WRITE_PREFIXES), (
            f"{method} {path} is not covered by any _ANALYST_ALLOWED_WRITE_PREFIXES entry — "
            "it shouldn't be reachable at all, so it doesn't belong in _WRITE_VERB_GATE_ROUTES"
        )


@pytest.mark.security_regression
def test_unclassified_route_fails_the_tripwire():
    """Proves the tripwire actually fires. Injects a throwaway route into
    the REAL app's router (the same object test_every_real_route_is_blocked_or_inventoried
    walks) that is neither blocked nor inventoried, re-runs the same
    classification logic inline, and asserts it is reported as
    unclassified. The route is removed in a ``finally`` so it can't leak
    into any other test in the session.

    This is the "prove the test can fail" requirement — a route that is
    analyst-reachable and unclassified must show up, or this tripwire is
    decorative.
    """
    dummy_path = "/api/__tripwire_dummy_unclassified_route__"

    async def _dummy_handler():
        return {"ok": True}

    # Must be a proper APIRoute (via add_api_route), not a bare
    # starlette.routing.Route — FastAPI's OpenAPI generator only inspects
    # APIRoute instances, so a plain Route would silently vanish from
    # app.openapi()["paths"] and this proof-of-mechanism would be testing
    # nothing.
    _main_app.router.add_api_route(dummy_path, _dummy_handler, methods=["GET"])
    # FastAPI caches the generated OpenAPI schema on first access (including
    # from any earlier test in this session) — invalidate it so the schema
    # we inspect actually reflects the just-added dummy route.
    _main_app.openapi_schema = None
    try:
        paths = _main_app.openapi()["paths"]
        assert dummy_path in paths, "test setup bug: dummy route didn't register in the OpenAPI surface"

        found_uninventoried = False
        for method in _HTTP_METHODS:
            if method not in paths[dummy_path]:
                continue
            if _is_blocked_path(dummy_path):
                continue
            if _verb_gate_blocks(method.upper(), dummy_path):
                continue
            if (method.upper(), dummy_path) in _ALL_INVENTORIED:
                continue
            found_uninventoried = True

        assert found_uninventoried, (
            "Injected an unclassified dummy route but the tripwire logic did not flag it — "
            "the tripwire itself is broken"
        )
    finally:
        _main_app.router.routes[:] = [r for r in _main_app.router.routes if getattr(r, "path", None) != dummy_path]
        # FastAPI/Starlette cache the generated OpenAPI schema on first
        # access; clear it so later tests in the same process (or a rerun
        # of this test) see a route table without the dummy entry.
        _main_app.openapi_schema = None


# ── Merged from test_rum_debug_envelope.py ──────────────────────────────────────────────────────

"""Analyst responses from the RUM routes must not carry ``_debug_*`` telemetry unless the share setting allows it.

``routers/rum._inject_telemetry`` injects ``_debug_queries`` / ``_debug_sqlite`` / ``_debug_calls`` /
``_is_cached`` UNCONDITIONALLY (it ignores ``DEBUG_RESPONSES``); the only thing keeping SQL text and
FOS/CDN call details away from an analyst is ``RemoteAccessMiddleware._strip_analyst_envelope``. These
tests drive the REAL RUM router behind the REAL middleware so that guard cannot silently regress.
"""

from unittest.mock import PropertyMock, patch

import pytest

from tests.conftest import override_request_context

_SVC = "svcA"
_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_ENVELOPE_PREFIXES = ("_debug", "debug_")


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
    """Share settings persist in the (per-worker) metadata DB; restore the defaults so a test that
    opens the debug envelope cannot leak it into whatever test runs next on the same worker."""
    yield
    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "disabled")


@pytest.fixture
def analyst_client():
    with TestClient(_rum_app()) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
        yield c


def _beacon_health_disabled(client):
    return client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=_HEADERS)


def _beacon_health_check_failed(client):
    cfg = {"rum": {"enabled": True}}
    with (
        patch("backend.routers.rum.svcconfig.load_config", return_value=cfg),
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


class _Holder:
    def __init__(self, *_a, **_k):
        pass

    def __enter__(self):
        return object()

    def __exit__(self, *_a):
        return False


def _analytics_success(client):
    """The data-bearing branch builds its own dict and calls ``_inject_telemetry`` separately from no_data."""
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
    """High-scale branch: real ``rum_beacon_health`` over a stub ClickHouse client."""

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
    return [k for k in body if k.startswith(_ENVELOPE_PREFIXES) or k == "_is_cached"]


@pytest.mark.security_regression
@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_analyst_rum_response_has_no_debug_envelope_by_default(analyst_client, call) -> None:
    resp = call(analyst_client)
    assert resp.status_code == 200, resp.text
    assert _envelope_keys(resp.json()) == []


@pytest.mark.security_regression
@pytest.mark.parametrize("call", _ROUTES, ids=lambda f: f.__name__)
def test_share_setting_opens_only_the_matching_envelope_keys(analyst_client, call) -> None:
    """The injection is real (otherwise the default-case test proves nothing): the share settings are
    what release the keys, and each setting releases only its own half."""
    share_db.set_setting("query_debug_visibility", "analysts")
    body = call(analyst_client).json()
    assert "_debug_queries" in body
    assert "_debug_calls" not in body

    share_db.set_setting("query_debug_visibility", "disabled")
    share_db.set_setting("api_call_debug_visibility", "both")
    body = call(analyst_client).json()
    assert "_debug_calls" in body
    assert "_debug_queries" not in body
    assert "_debug_sqlite" not in body


@pytest.mark.security_regression
def test_admin_rum_response_still_carries_the_envelope() -> None:
    """Control: the loopback admin (no analyst session) keeps the unconditional injection."""
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
        if query_visible or api_visible:
            expected["_is_cached"] = False

    with TestClient(_rum_app(with_telemetry=True)) as client:
        if remote:
            _start_share()
            _login_analyst(client, _seed_invite(service_ids=[_SVC]))
            share_db.set_setting("query_debug_visibility", "analysts" if query_visible else "disabled")
            share_db.set_setting("api_call_debug_visibility", "analysts" if api_visible else "disabled")
        headers = dict(_HEADERS) if remote else {}
        if opt_in:
            headers["x-debug-responses"] = "1"
        with patch("backend.routers.rum.svcconfig.load_config", return_value={}):
            response = client.get(f"/api/services/{_SVC}/rum/beacon-health", headers=headers)

    assert response.status_code == 200, response.text
    assert response.json() == expected
    assert response.headers["content-type"] == "application/json"
    assert int(response.headers["content-length"]) == len(response.content)


@pytest.mark.security_regression
def test_analyst_high_scale_query_response_has_no_debug_envelope() -> None:
    """``backend/high_scale/query.py`` returns BOTH ``debug_queries`` and ``_debug_queries`` (plus
    ``debug_calls``) at the top level; the analyst middleware must strip every spelling."""
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
            return c.post(
                f"/api/query?service_id={_SVC}",
                json={"sql": "SELECT 1"},
                headers={**_HEADERS, "Origin": "https://testserver"},
            )

    with TestClient(app) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
        resp = _post(c)
        assert resp.status_code == 200, resp.text
        assert _envelope_keys(resp.json()) == []
        assert "ch_table" not in resp.text

        share_db.set_setting("query_debug_visibility", "analysts")
        body = _post(c).json()
        assert "_debug_queries" in body
        assert "debug_calls" not in body and "_debug_calls" not in body


@pytest.mark.security_regression
def test_analyst_debug_aliases_are_stripped_when_master_switch_is_off(monkeypatch) -> None:
    from backend.routers import share_auth
    from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware

    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(TelemetryResponseBodyMiddleware)
    app.include_router(share_auth.router)

    @app.get(f"/api/services/{_SVC}/debug-aliases")
    def debug_aliases():
        return {
            "value": "preserved",
            "debug_queries": [{"sql": "SELECT secret FROM logs"}],
            "debug_calls": [{"path": "/internal"}],
        }

    with TestClient(app) as client:
        _start_share()
        _login_analyst(client, _seed_invite(service_ids=[_SVC]))
        share_db.set_setting("query_debug_visibility", "analysts")
        share_db.set_setting("api_call_debug_visibility", "analysts")
        response = client.get(
            f"/api/services/{_SVC}/debug-aliases",
            headers={**_HEADERS, "x-debug-responses": "1"},
        )

    assert response.status_code == 200
    assert response.json() == {"value": "preserved"}


# ── Merged from test_debug_envelope_sites.py ──

import asyncio
import json

import pytest
from fastapi.responses import StreamingResponse

from backend.utils.remote_access import _strip_analyst_envelope

_ENVELOPE = {
    "ok": True,
    "_debug_queries": [{"sql": "SELECT secret FROM t"}],
    "_debug_sqlite": [{"sql": "SELECT 1"}],
    "_debug_postgres": [{"sql": "SELECT 2"}],
    "_debug_calls": [{"path": "/service/abc"}],
    "_is_cached": False,
}


def _json_stream(payload: dict) -> StreamingResponse:
    body = json.dumps(payload).encode()

    async def _gen():
        yield body

    resp = StreamingResponse(_gen(), media_type="application/json")
    resp.headers["content-type"] = "application/json"
    return resp


def _strip(payload: dict, *, query: str = "disabled", api: str = "disabled") -> dict:
    settings = {"query_debug_visibility": query, "api_call_debug_visibility": api}
    with patch("backend.core.share_db.settings.get_setting", side_effect=lambda k, d=None: settings.get(k, d)):
        out = asyncio.run(_strip_analyst_envelope(_json_stream(payload)))
    return json.loads(out.body)


@pytest.mark.security_regression
def test_strip_removes_every_debug_key_when_analyst_visibility_is_disabled():
    assert _strip(_ENVELOPE) == {"ok": True}


@pytest.mark.security_regression
def test_strip_removes_the_postgres_spelling_of_the_query_envelope():
    out = _strip({**_ENVELOPE, "debug_postgres": [{"sql": "SELECT 3"}]})

    assert not [k for k in out if "postgres" in k]


@pytest.mark.security_regression
def test_strip_keeps_the_postgres_envelope_only_when_query_visibility_is_enabled_for_analysts():
    out = _strip(_ENVELOPE, query="analysts")

    assert out["_debug_postgres"] == [{"sql": "SELECT 2"}]
    assert out["_debug_sqlite"] == [{"sql": "SELECT 1"}]
    assert "_debug_calls" not in out


def _admin_app() -> FastAPI:
    from backend.core.request_context import build_request_context
    from backend.routers import admin, provision, services, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(provision.router)
    app.include_router(services.router)
    app.include_router(admin.router)
    app.dependency_overrides[build_request_context] = override_request_context(
        source={"name": _SVC, "service_id": _SVC}
    )
    return app


@pytest.fixture
def admin_analyst_client():
    with TestClient(_admin_app()) as c:
        _start_share()
        _login_analyst(c, _seed_invite(service_ids=[_SVC]))
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

    assert r.status_code == 403
    assert not [k for k in r.json() if k.startswith(("_debug", "debug_"))]
