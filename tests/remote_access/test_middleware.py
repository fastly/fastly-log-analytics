"""Integration tests for RemoteAccessMiddleware.

Uses TestClient against the real ``backend.main.app`` with the share DB
isolated per-test. The middleware classifies requests via socket peer +
``X-Remote-Analyst`` header coordination, so tests just toggle that header
to flip between local-admin and analyst branches.
"""

from __future__ import annotations

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

    @app.get("/api/cron/stream")
    def _sse():
        return {"ok": True}

    @app.get("/api/services/{service_id}/scoring/status")
    def _scoring_status(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/alerts/{service_id}")
    def _alerts_for_service(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/custom-endpoint/{service_id}/data")
    def _custom_endpoint(service_id: str):
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


def _seed_invite(service_ids=None, ip_whitelist=None) -> dict:
    return share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids or ["svcA", "svcB"],
    )


def _start_share():
    """Mark the tunnel manager as sharing so X-Remote-Analyst is honored."""
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(use_tunnel=False, public_endpoint="https://testserver")
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
    sid = r.json()["session_id"]
    # TestClient cookie jar refuses `secure` cookies over http://; set it
    # manually so analyst follow-up calls carry it.
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


# ── Local pass-through ─────────────────────────────────────────────────────


def test_local_admin_can_hit_admin_paths(client):
    """No X-Remote-Analyst header → local-admin → admin endpoints work."""
    r = client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text


def test_local_admin_writes_pass_through(client):
    r = client.post("/api/views")
    assert r.status_code == 200


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
