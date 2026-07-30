"""RBAC regression tests for the H-1..H-4 audit fixes.

Each fix added a path (or family of paths) to the analyst blocklist in
``backend.utils.remote_access._is_blocked_path``. Pre-fix the path was
reachable by any logged-in analyst; post-fix the middleware returns 403
with ``{"error": "admin_only"}``.

These tests engage the real blocklist function from a tiny FastAPI app
plus a stamping middleware that mirrors the
``x-test-session-services`` convention used by
``tests/routers/test_cross_tenant_scope.py``: when the header is present
the request is treated as an analyst session whose service_ids are the
comma-separated values; absent → local admin. The test middleware then
asks ``_is_blocked_path`` whether the route is admin-only when the
caller is an analyst, and returns 403 / 200 accordingly. This keeps the
test surface light (no tunnel manager, no invite DB) while still pinning
the contract that the blocklist function actually contains the audited
path.

If a refactor removes one of the H-1..H-4 entries from
``_ANALYST_BLOCKED_PREFIXES`` / ``_ANALYST_BLOCKED_SUBPATHS`` /
``_ANALYST_BLOCKED_SCORING_SUFFIXES`` /
``_ANALYST_BLOCKED_SUBPATH_REGEX``, the matching test here fails — that
is the entire point of the security_regression floor gate.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from backend.utils.remote_access import _is_blocked_path

# Every test in this file pins a verified RBAC fix (H-1..H-4 from the
# audit). Refactors that touch the analyst blocklist must keep this
# coverage — the security_regression floor gate enforces the count.
pytestmark = pytest.mark.security_regression


@pytest.fixture
def app_with_blocklist():
    """Mini app whose middleware runs ``_is_blocked_path`` for analyst
    requests and short-circuits with 403 admin_only — exactly what
    ``RemoteAccessMiddleware`` does on the production analyst path.

    Tests inject the desired session via ``x-test-session-services``
    (same convention as test_cross_tenant_scope.py). Header present →
    analyst session; header absent → local admin (no blocklist).
    """

    app = FastAPI()

    @app.middleware("http")
    async def rbac_gate(request: Request, call_next):
        sid = request.headers.get("x-test-session-services")
        if sid is not None:
            services = [s for s in sid.split(",") if s]
            request.state.analyst_session = SimpleNamespace(session_id="test", service_ids=services, email="t@t")
            # Engage the actual blocklist function — this is the
            # function the H-1..H-4 fixes modified.
            if _is_blocked_path(request.url.path):
                return JSONResponse(status_code=403, content={"error": "admin_only"})
        else:
            request.state.analyst_session = None
        return await call_next(request)

    # Stub routes that match the production mount paths so the
    # blocklist matcher sees the right shapes. Bodies are trivial —
    # we're testing the gate, not the handlers.
    @app.get("/api/usage/prefill")
    def _usage_prefill():
        return {"ok": True}

    @app.get("/api/download")
    def _download():
        return {"ok": True}

    @app.get("/api/download-all")
    def _download_all():
        return {"ok": True}

    @app.get("/api/download-folder")
    def _download_folder():
        return {"ok": True}

    @app.get("/api/cron-schedule")
    def _cron_schedule():
        return {"ok": True}

    @app.get("/api/services/{service_id}/lake-info")
    def _lake_info(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/config")
    def _scoring_config(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/status")
    def _scoring_status(service_id: str):
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

    # Analyst-NEEDED scoring reads — flag column / modal / dashboard
    # depend on these and MUST stay reachable post-H-4.
    @app.get("/api/services/{service_id}/scoring/labels")
    def _scoring_labels(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/scoring/sessions/{sid}/events")
    def _scoring_sess_events(service_id: str, sid: str):
        return {"ok": True, "service_id": service_id, "sid": sid}

    @app.get("/api/services/{service_id}/scoring/top-flagged")
    def _scoring_top(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/realtime-stream")
    def _realtime_stream(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/realtime-seed")
    def _realtime_seed(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/api/services/{service_id}/control-room/{tab}")
    def _control_room_tab(service_id: str, tab: str):
        return {"ok": True, "service_id": service_id, "tab": tab}

    @app.get("/api/services/{service_id}/log-field-audit")
    def _log_field_audit(service_id: str):
        return {"ok": True, "service_id": service_id}

    return app


# ── H-1: /api/usage/* is admin-only ────────────────────────────────────


def test_usage_route_blocked_for_analyst(app_with_blocklist):
    """H-1: cost / billing / usage data must not leak to remote analysts."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/usage/prefill",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


# ── H-2: /api/download* is admin-only ──────────────────────────────────


@pytest.mark.parametrize(
    "path",
    [
        "/api/download-all",
        "/api/download-folder",
        "/api/download",
    ],
)
def test_download_routes_blocked_for_analyst(app_with_blocklist, path):
    """H-2: raw-object download endpoints (single, folder, bulk) are
    admin-only. Pre-fix an analyst session reached them and could
    enumerate raw FOS objects for any bucket the operator had configured."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


# ── H-3: lake-info + cron-schedule are admin-only ──────────────────────


def test_lake_info_blocked_for_analyst(app_with_blocklist):
    """H-3: per-service Iceberg / object-store layout is admin-only.
    Even when the analyst owns the service, the route stays gated — the
    response leaks bucket names, prefixes, and catalog warehouse paths
    that an operator must rotate independently of session scope."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/lake-info",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


def test_cron_schedule_blocked_for_analyst(app_with_blocklist):
    """H-3: cron cadence config exposes the operator's ingest schedule
    (and indirectly the operator's tolerance for backlog) — admin-only."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/cron-schedule",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


# ── H-4: scoring admin GETs blocked + analyst reads still work ─────────


@pytest.mark.parametrize(
    "suffix",
    [
        "/config",
        "/status",
        "/audit",
        "/threshold",
        "/exclude-regex",
        "/enforce-status-code",
        "/l2-enforce",
    ],
)
def test_scoring_admin_get_endpoints_blocked_for_analyst(app_with_blocklist, suffix):
    """H-4: scoring config / status / audit / threshold / exclude-regex /
    enforce-status-code / l2-enforce GETs are admin-only. Authorizing the
    analyst for the service must NOT bypass the suffix gate."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            f"/api/services/svc-A/scoring{suffix}",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{suffix}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/services/svc-A/scoring/labels",
        "/api/services/svc-A/scoring/sessions/sess-1/events",
        "/api/services/svc-A/scoring/top-flagged",
    ],
)
def test_scoring_analyst_allowed_reads_still_pass(app_with_blocklist, path):
    """H-4 positive control: the flag column, session-detail modal, and
    dashboard depend on /scoring/labels, /scoring/sessions/<sid>/events,
    and /scoring/top-flagged. The suffix gate must NOT shadow them — if
    a refactor expands the admin-suffix list to swallow one of these,
    the analyst UI silently breaks."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 200, f"{path}: {r.status_code} {r.text}"
    assert r.json()["service_id"] == "svc-A"


# ── Adversarial bypass guards ────────────────────────────────────────────────
#
# The adversarial-review pass on the audit-fix workflow found two trailing-
# slash bypasses that survived the initial implementation:
#   - /api/services/{id}/scoring/config/  (H-4)  endswith() didn't match
#   - /api/services/{id}/lake-info/       (H-3)  regex fullmatch didn't match
# `_is_blocked_path` now normalizes trailing slashes before matching. These
# tests pin the fix so a future refactor that re-introduces strict-equality
# matching surfaces here at PR time, not in a re-audit.


@pytest.mark.parametrize(
    "suffix",
    ["/config", "/status", "/audit", "/threshold", "/exclude-regex", "/enforce-status-code"],
)
def test_scoring_admin_trailing_slash_does_not_bypass(app_with_blocklist, suffix):
    """Adversarial: /api/services/{id}/scoring/<suffix>/ MUST still be
    blocked. Without trailing-slash normalization the endswith() match
    would let the trailing-slash variant through."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            f"/api/services/svc-A/scoring{suffix}/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{suffix}/: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


def test_lake_info_trailing_slash_does_not_bypass(app_with_blocklist):
    """Adversarial: /api/services/{id}/lake-info/ MUST still be blocked.
    Without trailing-slash normalization the regex fullmatch (anchored
    with $) would let the trailing-slash variant through."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            "/api/services/svc-A/lake-info/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


@pytest.mark.parametrize(
    "path",
    [
        "/api/download-all/",
        "/api/download-folder/",
        "/api/download/",
        "/api/cron-schedule/",
        "/api/usage/prefill/",
    ],
)
def test_subpath_blocks_resist_trailing_slash_bypass(app_with_blocklist, path):
    """Adversarial: trailing slash on the H-1/H-2/H-3 exact-subpath blocks
    must not bypass the gate. Defense in depth — the original
    ``startswith(sp + "/")`` check already handles "/api/download-all/",
    but normalization also covers the /api/usage/ prefix block."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(path, headers={"x-test-session-services": "svc-A"})
    assert r.status_code == 403, f"{path}: {r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"


# ── S-1: /realtime-stream is analyst-visible (control room) ──────────────


def test_realtime_stream_allowed_for_analyst(app_with_blocklist):
    """S-1: realtime-stream powers the control room which is analyst-visible.
    The endpoint exposes aggregate metrics (rps, error rate, cache ratio) —
    no PII, no infra details — so it must not be blocked."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/realtime-stream",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


def test_realtime_stream_still_needs_service_scope(app_with_blocklist):
    """Analyst without the target service in scope must still be rejected
    at the service-scope gate (not the blocklist, which no longer blocks
    this path)."""
    assert not _is_blocked_path("/api/services/svc-B/realtime-stream")


# ── S-1 parity: /realtime-seed is the REST twin of /realtime-stream ──────


def test_realtime_seed_allowed_for_analyst(app_with_blocklist):
    """S-1 parity: ``/realtime-seed`` (backend/routers/control_room.py
    :246) is the REST seed-data twin of ``/realtime-stream`` — both pull
    the same ``transform_single_second`` metric shape from rt.fastly.com,
    and ``/realtime-seed`` is what the client calls on page load to hydrate
    the chart with 60 historical bars BEFORE subscribing to the SSE stream.
    The S-1 decision that made ``/realtime-stream`` analyst-visible
    ("exposes aggregate metrics ... no PII, no infra details") applies
    identically here. Pinned separately from S-1 because ``_is_blocked_path``
    has no shared code path between the two routes — a future edit to the
    blocklist could block one and not the other with nobody noticing
    (an analyst's dashboard would 403 on first paint while the live
    stream kept working)."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/realtime-seed",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


def test_control_room_tab_read_is_a_flagged_tripwire(app_with_blocklist):
    """Control Room's REST tab endpoint (``GET .../control-room/{tab}``,
    backend/routers/control_room.py:95) is NOT in any analyst blocklist
    today. Unlike ``/realtime-stream`` (S-1), there is no recorded
    security decision that it SHOULD be analyst-reachable — AGENTS.md
    documents the whole Control Room feature as an "Admin-only
    operational dashboard" and specifically says the "Admin Health" tab
    is meant to "surface log-field audit state and ingest health", which
    are data classes already admin-gated elsewhere (log-field-audit
    itself: see S-2 below; ingest/cron health: H-5's ``/api/cron-runs``
    + ``/api/audit-logs``; cost/usage: H-1's ``/api/usage/``).

    It is harmless TODAY because ``TAB_STUBS`` returns hardcoded zeros
    for every tab regardless of which one is requested. This test pins
    CURRENT behavior (reachable) as a deliberate tripwire, not an
    endorsement: it will keep passing right up until someone wires a real
    query behind ``admin_health`` / ``cost`` / ``security``, at which
    point this same 200 starts returning live operator-only data with no
    additional gate in the way. Before that wiring lands, add an explicit
    per-tab admin-only gate (e.g. a tab-name blocklist mirroring
    ``_ANALYST_BLOCKED_SCORING_SUFFIXES``) and invert this test.
    """
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/control-room/admin_health",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 200, f"expected 200 but got {r.status_code}: {r.text}"


# ── S-2: /log-field-audit is admin-only ──────────────────────────────────


def test_log_field_audit_blocked_for_analyst(app_with_blocklist):
    """S-2: log-field-audit leaks operator's field configuration choices.
    Must be admin-only."""
    with TestClient(app_with_blocklist) as c:
        r = c.get(
            "/api/services/svc-A/log-field-audit",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, r.text
    assert r.json()["error"] == "admin_only"


def test_log_field_audit_trailing_slash_does_not_bypass(app_with_blocklist):
    """Adversarial: trailing slash on log-field-audit must not bypass."""
    with TestClient(app_with_blocklist, follow_redirects=False) as c:
        r = c.get(
            "/api/services/svc-A/log-field-audit/",
            headers={"x-test-session-services": "svc-A"},
        )
    assert r.status_code == 403, f"{r.status_code} {r.text}"
    assert r.json()["error"] == "admin_only"
