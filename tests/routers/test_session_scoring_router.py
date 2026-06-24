"""Tests for backend.routers.session_scoring — enable/disable/status endpoints.

The actual orchestrator work is mocked; these tests verify the HTTP
contract (status codes, SSE event shape, token resolution, scoring-block
visibility)."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.main import app

LOG_SVC = "TestScoringRouterSvc"


@pytest.fixture
def client():
    """Plain TestClient(app) — deliberately shadows conftest's ``client``
    fixture (which installs DuckDB/source dependency overrides) because
    these tests mock ``backend.config.load_config`` directly and don't
    need the in-memory DB plumbing."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_analytics_cache_between_tests():
    """The _analytics_cache + _inflight dicts are module-globals; without
    a between-test reset, a test that caches a {"rows": [...]} payload
    poisons the next test that expects a different shape (test-order
    dependent failures observed locally)."""
    from backend.routers import session_scoring as _ss

    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()
    yield
    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    _ss._scoring_svc_version_cache.clear()


@pytest.fixture
def with_config(monkeypatch):
    """Return a writable container so individual tests can stash a fake
    service config that backend.config.load_config picks up."""
    container: dict = {}

    def fake_load(svc_id):
        return container.get(svc_id)

    monkeypatch.setattr("backend.config.load_config", fake_load)
    return container


# ── /scoring/status ──────────────────────────────────────────────────────────


def test_status_returns_disabled_when_no_scoring_block(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    # M1 telemetry middleware injects _debug_queries / _debug_calls / _is_cached
    # into plain-dict responses when DEBUG_RESPONSES is set (it is in tests).
    # Assert the meaningful keys instead of full equality.
    assert r.json()["enabled"] is False


def test_status_returns_disabled_when_block_present_but_false(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.json()["enabled"] is False


def test_status_returns_block_when_enabled(client, with_config):
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "scoring_service_id": "scoring_xyz",
            "scoring_service_name": f"Session Scoring Service for {LOG_SVC}",
            "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
        },
    }
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["scoring_service_id"] == "scoring_xyz"


def test_status_includes_active_version_when_token_present(client, with_config):
    """When the config has a fastly_api_key + scoring_service_id, the status
    endpoint surfaces the scoring Compute service's live active version and
    activation timestamp (best-effort via the Fastly API)."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "fastly_api_key": "tok_abc123",
        "scoring": {"enabled": True, "scoring_service_id": "scoring_present"},
    }
    with patch(
        "backend.core.fastly.service.get_active_version_info",
        return_value={"number": 42, "updated_at": "2026-06-17T18:41:04Z", "created_at": "2026-06-17T18:40:00Z"},
    ) as mock_info:
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["scoring_active_version"] == 42
    assert body["scoring_activated_at"] == "2026-06-17T18:41:04Z"
    mock_info.assert_called_once_with("scoring_present", "tok_abc123")


def test_status_omits_active_version_without_token(client, with_config):
    """No fastly_api_key → no Fastly call, no version fields (the status page
    still works on a scrubbed/local config that has the IDs but no token)."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "scoring_service_id": "scoring_notoken"},
    }
    with patch("backend.core.fastly.service.get_active_version_info") as mock_info:
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert "scoring_active_version" not in body
    assert "scoring_activated_at" not in body
    mock_info.assert_not_called()


def test_status_survives_fastly_lookup_failure(client, with_config):
    """A Fastly error during the active-version lookup must not break the
    status page — the version fields are simply omitted."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "fastly_api_key": "tok_abc123",
        "scoring": {"enabled": True, "scoring_service_id": "scoring_fail"},
    }
    with patch("backend.core.fastly.service.get_active_version_info", side_effect=RuntimeError("fastly down")):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert "scoring_active_version" not in body


def test_status_strips_aes_key_if_somehow_present(client, with_config):
    """Belt-and-suspenders: the AES key should never be in cfg, but if it
    is the status endpoint must not echo it back."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "scoring_service_id": "x",
            "aes_key_hex": "secret-never-show",
        },
    }
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert "secret-never-show" not in r.text
    assert "aes_key_hex" not in r.json()


# ── /scoring/status: edge drift ───────────────────────────────────────────────

_SHIPPED = "backend.provision.session_scoring_orchestrator.shipped_scorer_identity"


def _enabled_cfg(**scoring):
    base = {"enabled": True, "scoring_service_id": "scoring_drift"}
    base.update(scoring)
    return {"service_id": LOG_SVC, "scoring": base}


def test_status_scorer_drift_true_when_wasm_sha_differs(client, with_config):
    with_config[LOG_SVC] = _enabled_cfg(deployed_package_sha="old_pkg", deployed_vcl_sha="vcl1")
    with patch(_SHIPPED, return_value={"package_sha": "new_pkg", "vcl_sha": "vcl1"}):
        body = client.get(f"/api/services/{LOG_SVC}/scoring/status").json()
    assert body["scorer_drift"] is True
    assert body["drift_detail"] == "wasm"


def test_status_scorer_drift_true_when_vcl_sha_differs(client, with_config):
    with_config[LOG_SVC] = _enabled_cfg(deployed_package_sha="pkg1", deployed_vcl_sha="old_vcl")
    with patch(_SHIPPED, return_value={"package_sha": "pkg1", "vcl_sha": "new_vcl"}):
        body = client.get(f"/api/services/{LOG_SVC}/scoring/status").json()
    assert body["scorer_drift"] is True
    assert body["drift_detail"] == "vcl"


def test_status_scorer_drift_reports_both_parts(client, with_config):
    with_config[LOG_SVC] = _enabled_cfg(deployed_package_sha="old_pkg", deployed_vcl_sha="old_vcl")
    with patch(_SHIPPED, return_value={"package_sha": "new_pkg", "vcl_sha": "new_vcl"}):
        body = client.get(f"/api/services/{LOG_SVC}/scoring/status").json()
    assert body["scorer_drift"] is True
    assert body["drift_detail"] == "wasm+vcl"


def test_status_no_drift_when_hashes_match(client, with_config):
    with_config[LOG_SVC] = _enabled_cfg(deployed_package_sha="pkg1", deployed_vcl_sha="vcl1")
    with patch(_SHIPPED, return_value={"package_sha": "pkg1", "vcl_sha": "vcl1"}):
        body = client.get(f"/api/services/{LOG_SVC}/scoring/status").json()
    assert body["scorer_drift"] is False
    assert body["drift_detail"] is None


def test_status_no_drift_when_stamp_absent(client, with_config):
    """A service enabled before drift-stamping shipped has no stamp → unknown,
    not stale. Don't nag with a false 'redeploy needed' badge."""
    with_config[LOG_SVC] = _enabled_cfg()  # no deployed_*_sha
    with patch(_SHIPPED, return_value={"package_sha": "anything", "vcl_sha": "anything"}):
        body = client.get(f"/api/services/{LOG_SVC}/scoring/status").json()
    assert body["scorer_drift"] is False
    assert body["drift_detail"] is None


def test_status_drift_check_survives_exception(client, with_config):
    """A failure computing the shipped identity must not break the status panel."""
    with_config[LOG_SVC] = _enabled_cfg(deployed_package_sha="pkg1", deployed_vcl_sha="vcl1")
    with patch(_SHIPPED, side_effect=RuntimeError("hash boom")):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["scorer_drift"] is False
    assert body["drift_detail"] is None


def test_status_404_on_unknown_service(client, with_config):
    r = client.get("/api/services/does-not-exist/scoring/status")
    assert r.status_code == 404


def test_scoring_admin_routes_reject_service_id_with_invalid_chars(client):
    """Defense in depth: the ``ServiceId`` Annotated type on every
    /scoring/* admin endpoint rejects path params containing characters
    outside ``[A-Za-z0-9_-]`` at the FastAPI boundary (422), so malformed
    ids never reach load_config / SQL / filesystem code paths. The
    application layer also rejects unknown ids (via load_config →
    404), but this catches anything stage-shaped like ``svc;DROP`` or
    ``svc.dot`` before the request handler even runs.

    Use endpoints that have the ServiceId type guard — /scoring/status
    is on the main session_scoring router (no guard); the admin routes
    in session_scoring_admin.py are what we're pinning.
    """
    # Semicolon and dot both fall outside [A-Za-z0-9_-] but pass through
    # FastAPI's route-matching (they're URL-safe inside a single segment).
    r = client.get("/api/services/svc;DROP/scoring/threshold")
    assert r.status_code == 422
    r = client.get("/api/services/svc.dot/scoring/threshold")
    assert r.status_code == 422


# ── /scoring/enable: token resolution ────────────────────────────────────────


def test_enable_400_when_no_token_anywhere(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": ""}
    r = client.post(f"/api/services/{LOG_SVC}/scoring/enable")
    assert r.status_code == 400
    assert "token" in r.json()["detail"]["error"].lower()


def test_enable_uses_config_token_when_query_token_absent(client, with_config):
    """Token resolution: prefer query-param token, fall back to
    cfg.fastly_api_key. Without either we 400."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "FROM_CONFIG"}

    captured_token: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured_token["t"] = args[1]
        yield {"type": "status", "message": "fake"}

    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=fake_run_with_events,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable")
    assert r.status_code == 200
    assert captured_token["t"] == "FROM_CONFIG"


def test_enable_query_token_overrides_config_token(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "FROM_CONFIG"}

    captured_token: dict = {}

    def fake_run_with_events(func, *args, **kwargs):
        captured_token["t"] = args[1]
        yield {"type": "status", "message": "fake"}

    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=fake_run_with_events,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "FROM_QUERY"})
    assert r.status_code == 200
    assert captured_token["t"] == "FROM_QUERY"


# ── /scoring/enable: SSE event stream ────────────────────────────────────────


def test_enable_streams_status_events_then_done(client, with_config):
    """Orchestrator emits status callbacks; the endpoint wraps each in an
    SSE 'data: {...}' line plus a final 'done' event with the scoring
    block."""
    cfg = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN"}
    with_config[LOG_SVC] = cfg

    enabled_cfg = {
        **cfg,
        "scoring": {
            "enabled": True,
            "scoring_service_id": "scoring_xyz",
            "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
        },
    }

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "step 1"}
        yield {"type": "status", "message": "step 2"}
        # When orchestrator finishes, the router re-loads config to surface
        # the final scoring block. Flip the fake config now.
        with_config[LOG_SVC] = enabled_cfg

    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=fake_run_with_events,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")

    # Parse SSE: each "data: {...}" line is a JSON event.
    events = []
    for line in r.text.splitlines():
        if line.startswith("data: "):
            events.append(json.loads(line[len("data: ") :]))

    types = [e["type"] for e in events]
    assert "status" in types
    assert "done" in types
    done_event = next(e for e in events if e["type"] == "done")
    assert done_event["scoring"]["scoring_service_id"] == "scoring_xyz"


def test_enable_streams_error_event_on_orchestrator_failure(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN"}

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "got partway"}
        raise RuntimeError("boom — validation failed at step 7")

    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=fake_run_with_events,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})

    assert r.status_code == 200  # streaming endpoint always 200; error is in the body
    events = [json.loads(line[len("data: ") :]) for line in r.text.splitlines() if line.startswith("data: ")]
    types = [e["type"] for e in events]
    assert "error" in types
    assert "done" not in types  # error short-circuits before done
    err = next(e for e in events if e["type"] == "error")
    assert "validation" in err["message"].lower()


# ── /scoring/disable ─────────────────────────────────────────────────────────


def test_disable_streams_status_events_then_done(client, with_config):
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "fastly_api_key": "TOKEN",
        "scoring": {"enabled": True, "scoring_service_id": "x"},
    }

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "tearing down"}

    with patch(
        "backend.provision.orchestrator.run_with_events",
        side_effect=fake_run_with_events,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/disable", json={"token": "TOKEN"})

    assert r.status_code == 200
    events = [json.loads(line[len("data: ") :]) for line in r.text.splitlines() if line.startswith("data: ")]
    assert any(e["type"] == "done" for e in events)


# ── /scoring/labels CRUD ─────────────────────────────────────────────────────


def test_labels_create_and_list_round_trip(client):
    # Create
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "deadbeef1234", "label": "bad", "notes": "scraper", "sample_ip": "1.2.3.4"},
    )
    assert r.status_code == 200, r.text
    saved = r.json()
    assert saved["sid"] == "deadbeef1234"
    assert saved["label"] == "bad"

    # List
    r = client.get(f"/api/services/{LOG_SVC}/scoring/labels")
    assert r.status_code == 200
    body = r.json()
    sids = [row["sid"] for row in body["labels"]]
    assert "deadbeef1234" in sids
    assert body["counts"]["bad"] == 1
    assert body["counts"]["good"] == 0


def test_labels_create_400_when_sid_missing(client):
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"label": "bad"},
    )
    assert r.status_code == 400
    body = r.json()["detail"]
    assert body["error"] == "invalid_label"
    assert "sid" in body["message"].lower()


def test_labels_create_400_when_label_invalid(client):
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "abc", "label": "ugly"},
    )
    assert r.status_code == 400


def test_labels_create_accepts_neutral(client):
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "uncertain1", "label": "neutral"},
    )
    assert r.status_code == 200
    assert r.json()["label"] == "neutral"


def test_labels_create_upserts_on_sid(client):
    """Re-labeling the same sid via the endpoint must overwrite, not duplicate."""
    sid = "samesid01"
    r1 = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": sid, "label": "bad"},
    )
    r2 = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": sid, "label": "good", "notes": "actually fine"},
    )
    assert r1.json()["id"] == r2.json()["id"]
    assert r2.json()["label"] == "good"

    listing = client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()
    matches = [row for row in listing["labels"] if row["sid"] == sid]
    assert len(matches) == 1


def test_labels_patch_updates_notes(client):
    created = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "patchme", "label": "bad", "notes": "initial"},
    ).json()
    r = client.patch(
        f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}",
        json={"notes": "revised"},
    )
    assert r.status_code == 200
    assert r.json()["notes"] == "revised"
    assert r.json()["label"] == "bad"  # untouched


def test_labels_patch_400_on_invalid_label(client):
    created = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "x", "label": "good"},
    ).json()
    r = client.patch(
        f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}",
        json={"label": "ugly"},
    )
    assert r.status_code == 400


def test_labels_delete_is_idempotent(client):
    created = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "deleteme", "label": "bad"},
    ).json()
    r1 = client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}")
    r2 = client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}")
    assert r1.status_code == 200
    assert r2.status_code == 200  # second delete no-ops cleanly


# ── /scoring/labels analyst PII projection (audit R-4) ──────────────────────

_LABEL_PII_KEYS = ("notes", "flagged_by", "sample_ip", "sample_ua", "sample_url")
_LABEL_SAFE_KEYS = ("id", "service_id", "sid", "label", "created_at", "updated_at")


def test_project_label_for_analyst_strips_pii_fields_only():
    """The helper must strip exactly the PII fields and nothing else."""
    from backend.routers.session_scoring import _project_label_for_analyst

    full = {
        "id": "abc",
        "service_id": "svc",
        "sid": "sid1",
        "label": "bad",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "notes": "user@example.com flagged this",
        "flagged_by": "analyst@example.com",
        "sample_ip": "1.2.3.4",
        "sample_ua": "Mozilla/5.0",
        "sample_url": "/admin/super-secret-path",
    }
    out = _project_label_for_analyst(full)
    for k in _LABEL_PII_KEYS:
        assert k not in out, f"PII field {k!r} should be stripped"
    for k in _LABEL_SAFE_KEYS:
        assert k in out, f"safe field {k!r} should be kept"


def test_labels_list_admin_returns_full_pii(client):
    """Admin path (no analyst session) must still return PII for the
    scoring-labels admin UI."""
    client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={
            "sid": "admin-pii-test",
            "label": "bad",
            "notes": "n",
            "sample_ip": "9.9.9.9",
            "sample_ua": "ua",
            "sample_url": "/x",
        },
    )
    listing = client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()
    row = next(r for r in listing["labels"] if r["sid"] == "admin-pii-test")
    # All PII fields present for admin
    for k in _LABEL_PII_KEYS:
        assert k in row, f"admin row missing {k!r}"
    assert row["sample_ip"] == "9.9.9.9"
    assert row["sample_url"] == "/x"


def test_labels_list_analyst_omits_pii(client, monkeypatch):
    """Analyst path must get rows without PII / operator-attribution fields.

    Simulate by monkeypatching the analyst-detection helper to flip every
    request into the analyst branch; this is the same shape the middleware
    achieves by stamping request.state.analyst_session.
    """
    from backend.routers import session_scoring as _ss

    client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "analyst-pii-test", "label": "bad", "notes": "n", "sample_ip": "1.1.1.1"},
    )
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    listing = client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()
    row = next(r for r in listing["labels"] if r["sid"] == "analyst-pii-test")
    for k in _LABEL_PII_KEYS:
        assert k not in row, f"analyst response should NOT include {k!r}"
    for k in _LABEL_SAFE_KEYS:
        assert k in row, f"analyst response should include {k!r}"


def test_labels_create_response_analyst_omits_pii(client, monkeypatch):
    """POST response (the echoed row) must also be projected for analyst."""
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "analyst-post-test", "label": "good", "notes": "n", "sample_ip": "2.2.2.2"},
    )
    assert r.status_code == 200
    body = r.json()
    for k in _LABEL_PII_KEYS:
        assert k not in body, f"analyst POST response should NOT include {k!r}"


def test_labels_patch_response_analyst_omits_pii(client, monkeypatch):
    """PATCH response must also be projected for analyst."""
    from backend.routers import session_scoring as _ss

    created = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": "analyst-patch-test", "label": "bad", "notes": "initial"},
    ).json()
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.patch(
        f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}",
        json={"notes": "revised"},
    )
    assert r.status_code == 200
    body = r.json()
    for k in _LABEL_PII_KEYS:
        assert k not in body, f"analyst PATCH response should NOT include {k!r}"


def test_labels_create_analyst_does_not_persist_pii(client, monkeypatch):
    """Finding 003 (run 80e9f210), defense-in-depth: the WRITE path must
    strip operator-attribution / PII fields for analyst callers, not just
    the response. (The middleware already blocks analyst writes to this
    path; this is the belt-and-suspenders router-level gate, mirroring the
    cross-tenant write gates.) Read the full row back via the labels module
    to prove the analyst's injected values were never persisted."""
    from backend.routers import session_scoring as _ss
    from backend.scoring import labels as _labels

    sid = "analyst-write-pii"
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={
            "sid": sid,
            "label": "bad",
            "notes": "injected",
            "flagged_by": "spoofed-operator",
            "sample_ip": "9.9.9.9",
            "sample_ua": "evil-agent",
            "sample_url": "/admin/secret",
        },
    )
    assert r.status_code == 200, r.text

    stored = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == sid)
    assert stored["label"] == "bad"  # the legitimate classification still persists
    assert stored["notes"] == ""
    assert stored["flagged_by"] == "admin"  # model default, NOT "spoofed-operator"
    assert stored["sample_ip"] == ""
    assert stored["sample_ua"] == ""
    assert stored["sample_url"] == ""


def test_labels_patch_analyst_cannot_overwrite_notes(client, monkeypatch):
    """Finding 003 (update path): an analyst PATCH must not modify ``notes``,
    but may still change the label classification."""
    from backend.routers import session_scoring as _ss
    from backend.scoring import labels as _labels

    sid = "analyst-patch-notes"
    created = client.post(
        f"/api/services/{LOG_SVC}/scoring/labels",
        json={"sid": sid, "label": "bad", "notes": "admin-original"},
    ).json()

    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.patch(
        f"/api/services/{LOG_SVC}/scoring/labels/{created['id']}",
        json={"label": "good", "notes": "analyst-overwrite"},
    )
    assert r.status_code == 200, r.text

    stored = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == sid)
    assert stored["notes"] == "admin-original"  # analyst PATCH left notes untouched
    assert stored["label"] == "good"  # label is not restricted


# ── /scoring/analytics composite block-bypass (audit R-3) ───────────────────
#
# The /evaluation/per-reason endpoint is admin-only via
# _ANALYST_BLOCKED_SCORING_SUFFIXES. The composite at /scoring/analytics
# must mirror that — analysts get the four analyst-safe sub-results, admins
# get all six. Without this, the path-suffix block was bypassable.

_ANALYST_COMPOSITE_KEYS = {"top_flagged", "score_distribution", "compliance_breakdown", "latency_timeseries", "health"}
_ADMIN_ONLY_COMPOSITE_KEYS = {"evaluation", "evaluation_per_reason"}


def _patch_composite_subcalls(monkeypatch, *, eval_raises=False):
    """Stub the six sub-functions the composite calls; raise from the two
    admin-only ones if eval_raises so the test proves we don't even call
    them on the analyst path."""
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(_ss, "scoring_top_flagged", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_score_distribution", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False})
    monkeypatch.setattr(_ss, "scoring_health", lambda **kw: {"ok": True})
    if eval_raises:

        def _boom(**kw):
            raise RuntimeError("scoring_evaluation must not be called on the analyst path")

        monkeypatch.setattr(_ss, "scoring_evaluation", _boom)
        import backend.routers.session_scoring_admin as _admin

        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", _boom)
    else:
        monkeypatch.setattr(_ss, "scoring_evaluation", lambda **kw: {"score": 0.5})
        import backend.routers.session_scoring_admin as _admin

        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []})


def test_analytics_composite_admin_includes_all_six_keys(client, monkeypatch):
    """Admin path (no analyst session) must keep all six sub-keys."""
    _patch_composite_subcalls(monkeypatch)
    r = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r.status_code == 200, r.text
    body = r.json()
    keys = set(body.keys())
    assert _ANALYST_COMPOSITE_KEYS.issubset(keys)
    assert _ADMIN_ONLY_COMPOSITE_KEYS.issubset(keys), f"admin missing keys: {_ADMIN_ONLY_COMPOSITE_KEYS - keys}"


def test_analytics_composite_analyst_omits_evaluation_keys(client, monkeypatch):
    """Analyst path must NOT include evaluation / evaluation_per_reason."""
    from backend.routers import session_scoring as _ss

    _patch_composite_subcalls(monkeypatch)
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r.status_code == 200, r.text
    body = r.json()
    keys = set(body.keys())
    assert _ANALYST_COMPOSITE_KEYS.issubset(keys), f"analyst missing safe keys: {_ANALYST_COMPOSITE_KEYS - keys}"
    assert "evaluation" not in keys, "analyst response leaked 'evaluation'"
    assert "evaluation_per_reason" not in keys, "analyst response leaked 'evaluation_per_reason'"


def test_analytics_composite_analyst_does_not_call_evaluation_subfunctions(client, monkeypatch):
    """Analyst path must SKIP the evaluation sub-calls entirely — not just
    omit them from the response. Pinned via a sub-function that raises."""
    from backend.routers import session_scoring as _ss

    _patch_composite_subcalls(monkeypatch, eval_raises=True)
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    # The eval functions are wired to RAISE; if the composite called them
    # we'd see a 500 here. 200 proves the analyst path never invoked them.
    assert r.status_code == 200, r.text


# ── /scoring/{top-flagged,score-distribution,compliance-breakdown} ──────────


def _patch_query_logs(rows: list[dict]):
    """Patch the router's _query_logs helper to return canned rows so we
    don't need a live DuckDB connection for these tests."""
    return patch("backend.repositories.session_scoring.query_logs", return_value=rows)


def test_top_flagged_returns_query_rows(client):
    canned = [
        {"timestamp": "2026-06-01 10:00:00", "edge_sid": "aaa", "edge_score": 95, "ip": "1.1.1.1"},
        {"timestamp": "2026-06-01 09:00:00", "edge_sid": "bbb", "edge_score": 80, "ip": "2.2.2.2"},
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=24&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["since_hours"] == 24
    assert len(body["rows"]) == 2
    assert body["rows"][0]["edge_sid"] == "aaa"


def test_top_flagged_clamps_since_hours_range(client):
    """Query is validated by FastAPI; 0 and 1000 should both be rejected."""
    with _patch_query_logs([]):
        r_low = client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=0")
        r_high = client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=999")
    assert r_low.status_code == 422
    assert r_high.status_code == 422


def test_score_distribution_returns_bucket_rows(client):
    canned = [
        {"hour": "2026-06-01 10:00:00", "bucket": "75-100", "count": 5},
        {"hour": "2026-06-01 10:00:00", "bucket": "0-25", "count": 100},
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/score-distribution")
    assert r.status_code == 200
    body = r.json()
    assert {row["bucket"] for row in body["rows"]} == {"75-100", "0-25"}


def test_compliance_breakdown_returns_grouped_rows(client):
    canned = [
        {"hour": "2026-06-01 10:00:00", "compliance": "ok", "count": 200},
        {"hour": "2026-06-01 10:00:00", "compliance": "missing", "count": 30},
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/compliance-breakdown")
    assert r.status_code == 200
    compliances = {row["compliance"] for row in r.json()["rows"]}
    assert compliances == {"ok", "missing"}


# ── /scoring/health latency + /scoring/latency-timeseries ───────────────────
#
# Fixtures use the column ALIASES the SQL actually emits (rtt_p95_us,
# fail_open_count, ...) — derived from the producer, not the consumer's
# .get() args (see test-fixture-from-producer-not-consumer). _table_columns
# is patched directly so the tests don't need a live DESCRIBE.


def test_health_includes_latency_when_columns_present(client, monkeypatch):
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(
        _ss,
        "_table_columns",
        lambda sid: {
            "edge_score",
            "edge_score_l2",
            "edge_score_reason",
            "edge_cookie_compliance",
            "edge_sid",
            "edge_score_rtt_us",
            "edge_score_exec_us",
        },
    )
    canned = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 800,
            "distinct_sids": 50,
            "avg_score": 12.5,
            "p50_score": 5,
            "p95_score": 60,
            "max_score": 100,
            "scorer_errors": 40,
            "top_reasons": [],
            "l2_evaluated": 200,
            "l2_high_count": 10,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 42000,
            "rtt_p99_us": 91000,
            "rtt_max_us": 100000,
            "exec_p50_us": 540,
            "exec_p95_us": 880,
        }
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r.status_code == 200, r.text
    lat = r.json()["latency"]
    assert lat["available"] is True
    assert lat["rtt_p95_us"] == 42000
    assert lat["exec_p95_us"] == 880


def test_health_omits_latency_when_columns_absent(client, monkeypatch):
    """Older service without the latency columns: latency.available is
    False and the percentile fields are null — no binder error, no 500."""
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(_ss, "_table_columns", lambda sid: {"edge_score", "edge_score_reason"})
    canned = [
        {
            "total_edge_rows": 10,
            "scored_rows": 8,
            "distinct_sids": 2,
            "avg_score": 1.0,
            "p50_score": 0,
            "p95_score": 0,
            "max_score": 5,
            "scorer_errors": 0,
            "top_reasons": [],
            "l2_evaluated": 0,
            "l2_high_count": 0,
        }
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=12")
    assert r.status_code == 200, r.text
    lat = r.json()["latency"]
    assert lat["available"] is False
    assert lat["rtt_p95_us"] is None


def test_latency_timeseries_returns_rows_with_latency(client, monkeypatch):
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(
        _ss,
        "_table_columns",
        lambda sid: {"edge_score", "edge_score_reason", "edge_score_rtt_us", "edge_score_exec_us"},
    )
    canned = [
        {
            "hour": "2026-06-17 10:00:00",
            "scored_count": 500,
            "fail_open_count": 30,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 45000,
            "rtt_p99_us": 95000,
            "exec_p50_us": 540,
            "exec_p95_us": 900,
        }
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=24")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["has_latency"] is True
    assert body["rows"][0]["fail_open_count"] == 30
    assert body["rows"][0]["rtt_p95_us"] == 45000


def test_latency_timeseries_errors_only_when_columns_absent(client, monkeypatch):
    """Without latency columns the endpoint still returns the fail-open
    series (errors over time) with has_latency=False."""
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(_ss, "_table_columns", lambda sid: {"edge_score", "edge_score_reason"})
    canned = [{"hour": "2026-06-17 10:00:00", "scored_count": 500, "fail_open_count": 30}]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=6")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["has_latency"] is False
    assert body["rows"][0]["fail_open_count"] == 30


def test_latency_timeseries_buckets_by_minute_for_1h_window(client, monkeypatch):
    """A 1h window at hourly granularity collapses to a single bar, so the
    endpoint buckets by MINUTE for since_hours<=1 (granularity='minute') and
    stays hourly for wider windows. Powers the per-minute Scorer errors +
    Scorer latency charts at the 1h range."""
    from backend.routers import session_scoring as _ss

    monkeypatch.setattr(_ss, "_table_columns", lambda sid: {"edge_score", "edge_score_reason"})

    captured: dict[str, str] = {}

    def _fake_query_logs(*args, **kwargs):
        captured["sql"] = next(
            (a for a in [*args, *kwargs.values()] if isinstance(a, str) and "date_trunc" in a),
            "",
        )
        return [{"hour": "2026-06-17 10:00:00", "scored_count": 1, "fail_open_count": 0}]

    with patch("backend.repositories.session_scoring.query_logs", side_effect=_fake_query_logs):
        _ss._analytics_cache.clear()
        r1 = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=1")
        assert r1.status_code == 200, r1.text
        assert r1.json()["granularity"] == "minute"
        assert "date_trunc('minute'" in captured["sql"]

        _ss._analytics_cache.clear()
        r2 = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=24")
        assert r2.status_code == 200, r2.text
        assert r2.json()["granularity"] == "hour"
        assert "date_trunc('hour'" in captured["sql"]


def test_bust_analytics_cache_actually_invalidates_targeted_service():
    """REGRESSION: _bust_analytics_cache(service_id) used to compare
    ``k[0] == service_id`` but k[0] is always the endpoint name (the cache
    keys are tuples like ("top-flagged", svc_id, since_hours, limit)).
    The bust was a silent no-op — labels mutations only invalidated via
    the 20s TTL. Fix: match by membership so the service_id at index 1
    triggers the match regardless of key shape."""
    from backend.routers import session_scoring as _ss

    # Seed the cache directly.
    _ss._analytics_cache.clear()
    _ss._analytics_cache[("top-flagged", "svc-a", 24, 50)] = (12345.0, {"rows": [1]})
    _ss._analytics_cache[("score-distribution", "svc-a", 24)] = (12345.0, {"rows": [2]})
    _ss._analytics_cache[("top-flagged", "svc-b", 24, 50)] = (12345.0, {"rows": [3]})

    _ss._bust_analytics_cache("svc-a")

    # svc-a entries must be gone; svc-b must survive.
    remaining = list(_ss._analytics_cache.keys())
    assert ("top-flagged", "svc-a", 24, 50) not in remaining
    assert ("score-distribution", "svc-a", 24) not in remaining
    assert ("top-flagged", "svc-b", 24, 50) in remaining


def test_bust_analytics_cache_with_none_service_id_clears_everything():
    from backend.routers import session_scoring as _ss

    _ss._analytics_cache.clear()
    _ss._analytics_cache[("top-flagged", "svc-a", 24, 50)] = (12345.0, {"rows": []})
    _ss._analytics_cache[("score-distribution", "svc-b", 24)] = (12345.0, {"rows": []})

    _ss._bust_analytics_cache(None)
    # _analytics_cache is now a BoundedTTLCache, not a plain dict, so the
    # idiomatic emptiness check is via len() rather than `== {}`.
    assert len(_ss._analytics_cache) == 0


# ── /scoring/evaluation (ROC-AUC against labels) ─────────────────────────────


def test_evaluation_returns_min_samples_cta_when_under_threshold(client, with_config):
    """With <3 labels of either class, return has_min_samples=false so
    the StatusPanel renders the 'Need N+ good / N+ bad' CTA instead of
    a noisy AUC. The endpoint must NOT touch DuckDB or the matrix in
    this branch — purely a label-count check."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}

    with (
        patch("backend.scoring.labels.list_labels", return_value=[]),
        patch("backend.scoring.labels.counts_by_label", return_value={"good": 1, "bad": 0, "neutral": 0}),
    ):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
    assert r.status_code == 200
    body = r.json()
    assert body["has_min_samples"] is False
    assert body["n_good"] == 1
    assert body["n_bad"] == 0
    assert body["min_per_class"] == 3
    assert "auc" not in body, "AUC must be omitted in the gated branch"


def test_evaluation_returns_auc_when_min_samples_met(client, with_config):
    """With >=3 labels of each class + a matrix on disk, return the
    computed AUC + pass/fail."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}

    # 3 good + 3 bad labels, all with synthetic sids.
    fake_labels = [{"sid": f"good{i:04x}", "label": "good"} for i in range(3)] + [
        {"sid": f"bad{i:04x}", "label": "bad"} for i in range(3)
    ]
    fake_counts = {"good": 3, "bad": 3, "neutral": 0}

    # Reconstruct returns one event per label sid — evaluate's
    # _session_l2_score returns 0 for <2 events but we just need a
    # non-degenerate AUC computation pathway here. Mock evaluate to
    # return a known result so this test stays isolated from the
    # scoring/matrix internals.
    from backend.scoring.evaluate import EvaluationResult

    fake_result = EvaluationResult(
        auc=0.85,
        pass_threshold=0.85,
        passed=True,
        n_good=3,
        n_bad=3,
    )

    with (
        patch("backend.scoring.labels.list_labels", return_value=fake_labels),
        patch("backend.scoring.labels.counts_by_label", return_value=fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value={"transitions": {}}),
        patch(
            "backend.repositories.session_scoring.reconstruct_labeled_sessions",
            return_value=[
                ({"session_id": lbl["sid"], "events": [], "max_edge_score": 0}, lbl["label"]) for lbl in fake_labels
            ],
        ),
        patch("backend.scoring.evaluate.evaluate_from_persisted_scores", return_value=fake_result),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
    assert r.status_code == 200
    body = r.json()
    assert body["has_min_samples"] is True
    assert body["auc"] == 0.85
    assert body["passed"] is True
    assert body["n_good"] == 3
    assert body["n_bad"] == 3
    assert body["matrix_version"] == "2026-06-01-a"


def test_curves_returns_min_samples_cta_under_threshold(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    with (
        patch("backend.scoring.labels.list_labels", return_value=[]),
        patch("backend.scoring.labels.counts_by_label", return_value={"good": 1, "bad": 0, "neutral": 0}),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/curves")
    body = r.json()
    assert body["has_min_samples"] is False
    assert "roc" not in body
    assert "auc" not in body


def test_curves_computes_perfect_separation_correctly(client, with_config):
    """If all bad sessions score above all good ones, the ROC curve is
    a single right-angle (FPR=0, TPR=1) and AUC = 1.0."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    fake_labels = [
        {"sid": "good1", "label": "good"},
        {"sid": "good2", "label": "good"},
        {"sid": "good3", "label": "good"},
        {"sid": "bad1", "label": "bad"},
        {"sid": "bad2", "label": "bad"},
        {"sid": "bad3", "label": "bad"},
    ]
    fake_counts = {"good": 3, "bad": 3, "neutral": 0}
    reconstructed = [
        ({"session_id": "good1", "events": [], "max_edge_score": 0}, "good"),
        ({"session_id": "good2", "events": [], "max_edge_score": 10}, "good"),
        ({"session_id": "good3", "events": [], "max_edge_score": 20}, "good"),
        ({"session_id": "bad1", "events": [], "max_edge_score": 75}, "bad"),
        ({"session_id": "bad2", "events": [], "max_edge_score": 80}, "bad"),
        ({"session_id": "bad3", "events": [], "max_edge_score": 90}, "bad"),
    ]
    with (
        patch("backend.scoring.labels.list_labels", return_value=fake_labels),
        patch("backend.scoring.labels.counts_by_label", return_value=fake_counts),
        patch("backend.repositories.session_scoring.reconstruct_labeled_sessions", return_value=reconstructed),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/curves")
    body = r.json()
    assert body["has_min_samples"] is True
    assert body["n_good"] == 3
    assert body["n_bad"] == 3
    assert body["auc"] == 1.0  # perfect ranking
    assert len(body["roc"]) == 101  # one point per integer threshold
    assert len(body["pr"]) == 101


def test_threshold_preview_buckets_sessions_correctly(client, with_config):
    """At threshold 50, sessions with max_score>=50 land in `flagged`,
    others in `passed`. Within each bucket, breakdown by label is
    accurate. Precision = bad-flagged / total-flagged-labeled.

    Post-009 implementation: route now issues a SINGLE CTE query
    that joins sid_scores against a labels(VALUES…) inline relation
    and emits ``total / flagged_total / flagged_good / flagged_bad /
    passed_good / passed_bad`` in one row. The test mocks query_logs
    to return the row that single SQL would produce for the seeded
    fixture.
    """
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}

    # Fixture: 6 sessions, threshold = 50
    #   bad1 (80) bad — flagged_bad
    #   bad2 (75) bad — flagged_bad
    #   good1 (60) good — flagged_good (false positive)
    #   unlbl1 (55) unlabeled — flagged_unlabeled (falls out by subtraction)
    #   good2 (10) good — passed_good
    #   bad3 (20) bad — passed_bad (false negative)
    aggregate_row = {
        "total": 6,
        "flagged_total": 4,
        "flagged_good": 1,
        "flagged_bad": 2,
        "passed_good": 1,
        "passed_bad": 1,
    }

    fake_labels = [
        {"sid": "bad1", "label": "bad"},
        {"sid": "bad2", "label": "bad"},
        {"sid": "bad3", "label": "bad"},
        {"sid": "good1", "label": "good"},
        {"sid": "good2", "label": "good"},
    ]
    fake_counts = {"good": 2, "bad": 3, "neutral": 0}

    with (
        patch("backend.repositories.session_scoring.query_logs", return_value=[aggregate_row]),
        patch("backend.scoring.labels.list_labels", return_value=fake_labels),
        patch("backend.scoring.labels.counts_by_label", return_value=fake_counts),
        patch("backend.routers.session_scoring._bust_analytics_cache"),
    ):
        # Bust the in-process cache between asserts so different threshold
        # queries don't collide.
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=50&since_hours=24")
    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] == 50
    assert body["flagged"]["bad"] == 2
    assert body["flagged"]["good"] == 1  # false positive
    assert body["flagged"]["unlabeled"] == 1  # 4 flagged_total − 1 good − 2 bad
    assert body["passed"]["good"] == 1
    assert body["passed"]["bad"] == 1  # false negative
    # Precision = 2 bad of 3 labeled flagged = 0.6667
    assert abs(body["precision"] - 2 / 3) < 0.01
    # Recall = 2 bad flagged of 3 bad total = 0.6667
    assert abs(body["recall"] - 2 / 3) < 0.01


def test_threshold_preview_extreme_thresholds(client, with_config):
    """threshold=0 should flag everything; threshold=100 should flag
    nothing. Both edges must be off-by-one-safe.

    009: returns aggregate counts from SQL — the labeled-sid query
    isn't reached when no labels exist.
    """
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    # Two unlabeled sids: a@100, b@0. At threshold 0 both flagged; at
    # threshold 100 only a flagged.
    agg_low = [{"total": 2, "flagged_total": 2, "passed_total": 0}]
    agg_high = [{"total": 2, "flagged_total": 1, "passed_total": 1}]

    call_count = {"n": 0}

    # The core scoring columns a provisioned service carries — so the
    # _table_columns() probe reports a full schema and _scoring_source
    # passes the table through unwrapped.
    _provisioned_cols = [
        {"column_name": c}
        for c in (
            "edge",
            "edge_sid",
            "edge_score",
            "edge_score_l1",
            "edge_score_l2",
            "edge_score_reason",
            "edge_cookie_compliance",
        )
    ]

    def _route_query(_service_id, sql, *_args, **_kwargs):
        # _table_columns() fires a DESCRIBE before the aggregate query;
        # answer it with a provisioned schema and don't count it.
        if sql.strip().upper().startswith("DESCRIBE"):
            return _provisioned_cols
        # No labels in this test → only the aggregate query fires.
        call_count["n"] += 1
        return agg_low if call_count["n"] == 1 else agg_high

    with (
        patch("backend.repositories.session_scoring.query_logs", side_effect=_route_query),
        patch("backend.scoring.labels.list_labels", return_value=[]),
        patch("backend.scoring.labels.counts_by_label", return_value={"good": 0, "bad": 0, "neutral": 0}),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r_low = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=0")
        _ss._analytics_cache.clear()
        r_high = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=100")

    # threshold=0: both sessions flagged (score>=0 is always true)
    assert r_low.json()["flagged"]["total"] == 2
    assert r_low.json()["passed"]["good"] + r_low.json()["passed"]["bad"] + r_low.json()["passed"]["unlabeled"] == 0
    # threshold=100: only the score=100 row flagged
    assert r_high.json()["flagged"]["total"] == 1
    assert r_high.json()["passed"]["unlabeled"] == 1


def test_retrain_smoke(client, with_config, tmp_path, monkeypatch):
    """REGRESSION: catches MatrixStats attribute renames + missing
    imports in the retrain pipeline before they hit prod. Mocks the
    DuckDB pull + matrix build so the test stays hermetic; the
    important thing is the wire shape and that the endpoint returns
    200 with the documented keys."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "scoring_service_id": "scorer-x", "scoring_matrix_store_id": "MATRIX_STORE"},
    }

    from backend.scoring.matrix import MatrixStats, TransitionMatrix

    fake_matrix = TransitionMatrix()
    fake_matrix.session_count = 5
    fake_matrix.transition_count = 12
    fake_matrix.vocab = {"/", "/login"}
    fake_stats = MatrixStats(
        sessions_in=10,
        sessions_dropped_short=3,
        sessions_dropped_fast=2,
        sessions_kept=5,
        transitions=12,
        routes_seen=2,
    )

    with (
        patch(
            "backend.core.duckdb.get_source_for_service", return_value={"name": LOG_SVC, "access_level": "read_write"}
        ),
        patch("backend.core.duckdb.get_connection") as mock_get_con,
        patch("backend.scoring.fixtures.extract_traces", return_value=iter([])),
        patch("backend.scoring.matrix.build_matrix", return_value=(fake_matrix, fake_stats)),
        patch("backend.scoring.labels.list_labels", return_value=[]),
        patch("backend.scoring.labels.counts_by_label", return_value={"good": 0, "bad": 0, "neutral": 0}),
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", tmp_path / "matrix.json"),
        patch("backend.state_sync.publish_matrix_to_fos"),
        patch("backend.routers.session_scoring_admin._resolve_token", return_value="tok"),
        patch("backend.core.fastly.client.fastly_raw") as mock_raw,
    ):
        mock_get_con.return_value.close = lambda: None
        r = client.post(f"/api/services/{LOG_SVC}/scoring/retrain?since_days=7")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["sessions_trained_on"] == 5
    assert body["transitions"] == 12
    assert body["vocab_size"] == 2
    assert body["rejected"]["kept"] == 5
    assert body["rejected"]["too_few_events"] == 3
    assert body["local_matrix_saved"] is True
    # New matrix pushed to the live scorer via KV — no Wasm rebuild.
    assert body["matrix_kv_written"] is True
    assert mock_raw.call_count == 1
    kv_path = mock_raw.call_args.args[1]
    assert kv_path == "/resources/stores/kv/MATRIX_STORE/keys/matrix"


def test_session_events_returns_event_timeline(client, with_config):
    """The events endpoint exposes _fetch_session_events: returns
    timestamped url sequence for one sid. The frontend SessionEventsDialog
    consumes this to render the per-session detail view."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        {
            "edge_sid": "abc123",
            "ts": "2026-06-02T20:00:00",
            "url": "/",
            "status": 200,
            "ip": "1.1.1.1",
            "ua": "browser",
            "edge_score": 0,
            "edge_cookie_compliance": "ok",
            "edge_score_reason": "",
        },
        {
            "edge_sid": "abc123",
            "ts": "2026-06-02T20:00:05",
            "url": "/login",
            "status": 200,
            "ip": "1.1.1.1",
            "ua": "browser",
            "edge_score": 10,
            "edge_cookie_compliance": "ok",
            "edge_score_reason": "",
        },
    ]
    with patch("backend.repositories.session_scoring.query_logs", return_value=canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/abc123/events")
    assert r.status_code == 200
    body = r.json()
    assert body["sid"] == "abc123"
    assert body["event_count"] == 2
    assert body["events"][0]["url"] == "/"
    assert body["events"][1]["url"] == "/login"
    # Status + score + compliance fields should round-trip
    assert body["events"][1]["edge_score"] == 10


def test_session_events_empty_when_sid_not_in_duckdb(client, with_config):
    """A label exists but the corresponding sid has no rows ingested
    yet (or rotated away). Return event_count=0, NOT 404 — the UI
    surfaces a 'no events yet' message."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    with patch("backend.repositories.session_scoring.query_logs", return_value=[]):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/nosuch/events")
    assert r.status_code == 200
    assert r.json()["event_count"] == 0


def test_evaluation_reports_missing_matrix_gracefully(client, with_config):
    """If the matrix.json file is missing or unreadable, surface an
    error string to the UI instead of 500ing."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "v"}}

    fake_labels = [{"sid": f"x{i:04x}", "label": "good" if i < 3 else "bad"} for i in range(6)]
    fake_counts = {"good": 3, "bad": 3, "neutral": 0}

    with (
        patch("backend.scoring.labels.list_labels", return_value=fake_labels),
        patch("backend.scoring.labels.counts_by_label", return_value=fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value=None),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
    assert r.status_code == 200
    body = r.json()
    assert body["has_min_samples"] is True
    assert "error" in body
    assert "matrix" in body["error"].lower()
    assert "auc" not in body


# ── /scoring/health (router-level test) ──────────────────────────────────────


def test_scoring_health_returns_expected_shape(client, with_config):
    """Pin the wire shape of /scoring/health — fire_rate_pct, distinct_sids,
    top_reasons list, matrix_staleness sub-object. SQL is mocked so this
    test stays hermetic; the goal is that any future SQL refactor that
    changes the column set or aggregate names trips this test before
    landing."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 200,
            "distinct_sids": 50,
            "avg_score": 45.5,
            "p50_score": 50.0,
            "p95_score": 75.0,
            "max_score": 100,
            "scorer_errors": 0,
            "top_reasons": [{"reason": "cookie-missing", "count": 10}],
            "l2_evaluated": 100,
            "l2_high_count": 5,
        }
    ]
    with patch("backend.repositories.session_scoring.query_logs", return_value=canned):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r.status_code == 200
    body = r.json()
    assert body["fire_rate_pct"] == 20.0  # 200/1000 = 20%
    assert body["distinct_sids"] == 50
    assert body["top_reasons"][0]["reason"] == "cookie-missing"
    assert "matrix_staleness" in body
    assert body["matrix_staleness"]["is_stale"] is False  # 5% < 25% threshold


def test_scoring_health_surfaces_fail_open_breakdown(client, with_config):
    """/scoring/health passes the fail-open-by-reason breakdown through so the
    admin page can group fail-opens by EXACT status (compute-unavailable-503
    vs -500 vs internal-error-keys) rather than a single lumped count."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 200,
            "distinct_sids": 50,
            "avg_score": 10.0,
            "p50_score": 0.0,
            "p95_score": 0.0,
            "max_score": 75,
            "scorer_errors": 7,
            "top_reasons": [{"reason": "cookie-missing", "count": 180}],
            "fail_open_breakdown": [
                {"reason": "compute-unavailable-503", "count": 5},
                {"reason": "compute-unavailable-500", "count": 1},
                {"reason": "internal-error-keys", "count": 1},
            ],
            "l2_evaluated": 100,
            "l2_high_count": 5,
        }
    ]
    with patch("backend.repositories.session_scoring.query_logs", return_value=canned):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r.status_code == 200
    body = r.json()
    # Highest-count fail-open first; each exact status is its own bucket.
    assert body["fail_open_breakdown"][0] == {"reason": "compute-unavailable-503", "count": 5}
    assert {b["reason"] for b in body["fail_open_breakdown"]} == {
        "compute-unavailable-503",
        "compute-unavailable-500",
        "internal-error-keys",
    }
    # SRE-15: the traffic-normalized fail-open rate (7 / 1000 = 0.7%) so the
    # admin tile can tone on a spike instead of an absolute count that moves
    # with traffic.
    assert body["fail_open_rate_pct"] == 0.7


def test_scoring_health_fail_open_breakdown_defaults_empty(client, with_config):
    """When the SQL row omits fail_open_breakdown (no fail-opens in window),
    the endpoint returns an empty list rather than null so the UI can render
    a clean 'no fail-opens' state."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [{"total_edge_rows": 10, "scored_rows": 10, "top_reasons": []}]
    with patch("backend.repositories.session_scoring.query_logs", return_value=canned):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r.status_code == 200
    assert r.json()["fail_open_breakdown"] == []


# ── /scoring/dashboard composite endpoint ────────────────────────────────────


def test_scoring_dashboard_returns_all_subobjects(client, with_config):
    """The composite returns every sub-endpoint's payload under a known key
    so the frontend can swap to a single useDashboard() hook."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scorer-x"}}
    with (
        patch("backend.scoring.labels.list_labels", return_value=[]),
        patch("backend.scoring.labels.counts_by_label", return_value={"good": 0, "bad": 0, "neutral": 0}),
        patch("backend.repositories.session_scoring.query_logs", return_value=[]),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/dashboard?since_hours=24&threshold=75")
    assert r.status_code == 200
    body = r.json()
    for key in (
        "since_hours",
        "threshold",
        "status",
        "evaluation",
        "evaluation_per_reason",
        "health",
        "top_flagged",
        "score_distribution",
        "compliance_breakdown",
        "curves",
        "threshold_preview",
        # Config block — folded in so the FE can drop the separate
        # /scoring/config + /scoring/evaluation/per-reason round-trips
        # and read everything off the single dashboard payload.
        "config_threshold",
        "exclude_regex",
        "enforce_status_code",
    ):
        assert key in body, f"missing key {key!r}"
    assert body["since_hours"] == 24
    assert body["threshold"] == 75
    # The config sub-objects use the shapes their per-endpoint twins
    # already return — pin the discriminating keys so a future shape
    # drift breaks here before the FE silently renders the wrong card.
    assert "threshold" in body["config_threshold"], "config_threshold should mirror /scoring/threshold shape"
    assert "enforced" in body["config_threshold"]
    assert "current" in body["exclude_regex"], "exclude_regex should mirror /scoring/exclude-regex shape"
    assert "default" in body["exclude_regex"]
    assert "current" in body["enforce_status_code"], (
        "enforce_status_code should mirror /scoring/enforce-status-code shape"
    )
    assert "effective" in body["enforce_status_code"]


# ── /scoring/threshold GET/PUT (operator's chosen threshold) ────────────────


def test_scoring_threshold_get_returns_null_when_unset(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    r = client.get(f"/api/services/{LOG_SVC}/scoring/threshold")
    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] is None
    assert body["enforced"] is False


def test_scoring_threshold_put_persists_value_and_returns_it(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    saved: dict = {}

    def fake_save(service_id, cfg):
        saved["sid"] = service_id
        saved["cfg"] = cfg

    monkeypatch.setattr("backend.config.save_config", fake_save)

    r = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 80})
    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] == 80
    assert body["set_at"] is not None
    assert body["enforced"] is False
    assert saved["cfg"]["scoring"]["operator_threshold"] == 80


def test_scoring_threshold_put_rejects_out_of_range(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    r = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 150})
    assert r.status_code == 400


def test_scoring_threshold_put_clears_when_null(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "operator_threshold": 80, "operator_threshold_set_at": "2026-01-01"},
    }
    saved: dict = {}
    monkeypatch.setattr("backend.config.save_config", lambda sid, cfg: saved.update(cfg=cfg))

    r = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": None})
    assert r.status_code == 200
    assert r.json()["threshold"] is None
    assert "operator_threshold" not in saved["cfg"]["scoring"]


# ── /scoring/matrix-versions (history + restore) ────────────────────────────


def test_matrix_versions_list_returns_empty_when_scoring_not_enabled(client, with_config):
    """The list endpoint does not gate on scoring.enabled — it surfaces
    whatever the FOS history bucket has and the cfg's matrix_version.
    With no scoring block configured, current_version is None and we
    expect an empty version list (the state_sync helper is best-effort
    and returns [] when no source / no objects)."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    with patch("backend.state_sync.list_scoring_matrix_versions", return_value=[]):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions")
    assert r.status_code == 200
    body = r.json()
    # M1 backstop adds _debug_* keys; check meaningful fields explicitly.
    assert body["versions"] == []
    assert body["current_version"] is None


def test_matrix_versions_list_empty_history_returns_current_version(client, with_config):
    """When scoring is enabled and a matrix is in use but nothing has
    been archived yet (first deploy), versions is empty but
    current_version is surfaced from cfg.scoring.matrix_version so the
    UI can show 'current: vX (no history)'."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"},
    }
    with patch("backend.state_sync.list_scoring_matrix_versions", return_value=[]):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions")
    assert r.status_code == 200
    body = r.json()
    assert body["versions"] == []
    assert body["current_version"] == "2026-06-01-a"


def test_matrix_versions_list_returns_history_newest_first(client, with_config):
    """When state_sync returns 3 archived versions (already sorted
    desc by last_modified), the endpoint passes them through unchanged
    and exposes the cfg's matrix_version as current_version."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c"},
    }
    fake_versions = [
        {
            "version": "2026-06-03-c",
            "key": "iceberg/meta/scoring_matrix_history/2026-06-03-c.json",
            "size_bytes": 4096,
            "last_modified": "2026-06-03T10:00:00+00:00",
        },
        {
            "version": "2026-06-02-b",
            "key": "iceberg/meta/scoring_matrix_history/2026-06-02-b.json",
            "size_bytes": 4000,
            "last_modified": "2026-06-02T10:00:00+00:00",
        },
        {
            "version": "2026-06-01-a",
            "key": "iceberg/meta/scoring_matrix_history/2026-06-01-a.json",
            "size_bytes": 3900,
            "last_modified": "2026-06-01T10:00:00+00:00",
        },
    ]
    with patch("backend.state_sync.list_scoring_matrix_versions", return_value=fake_versions):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions")
    assert r.status_code == 200
    body = r.json()
    assert len(body["versions"]) == 3
    assert body["versions"][0]["version"] == "2026-06-03-c"
    assert body["versions"][-1]["version"] == "2026-06-01-a"
    assert body["current_version"] == "2026-06-03-c"


def test_matrix_versions_restore_requires_confirm_flag(client, with_config):
    """Operator safety gate: without ?confirm=true the endpoint must
    400, NOT silently rewind the live matrix. restore_scoring_matrix_version
    must not be called."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    with patch("backend.state_sync.restore_scoring_matrix_version") as mock_restore:
        r = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore")
    assert r.status_code == 400
    assert "confirm" in r.json()["detail"]["error"].lower()
    mock_restore.assert_not_called()


def test_matrix_versions_restore_happy_path(client, with_config, monkeypatch, tmp_path):
    """With ?confirm=true and a valid version, the endpoint calls
    restore_scoring_matrix_version, unlinks the local matrix.json so
    _load_matrix falls through to the FOS-restored copy, records a
    'matrix_restored' audit, updates cfg.scoring.matrix_version, and
    returns ok + restored_version + deploy_hint, and pushes the restored
    matrix into the scoring_matrix KV Store for the live scorer."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "matrix_version": "2026-06-03-c",
            "scoring_matrix_store_id": "MATRIX_STORE",
        },
    }
    saved: dict = {}
    monkeypatch.setattr("backend.config.save_config", lambda sid, cfg: saved.update(sid=sid, cfg=cfg))

    # Create a real on-disk matrix.json so we can verify it gets unlinked.
    fake_matrix_path = tmp_path / "matrix.json"
    fake_matrix_path.write_text('{"transitions": {}}')

    audit_calls: list = []

    def fake_audit(svc, action, details=None):
        audit_calls.append({"service_id": svc, "action": action, "details": details})

    with (
        patch(
            "backend.state_sync.restore_scoring_matrix_version",
            return_value={"version": "2026-06-01-a", "restored_at": "2026-06-03T11:00:00+00:00"},
        ) as mock_restore,
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
        patch("backend.routers.session_scoring_admin._resolve_token", return_value="tok"),
        patch("backend.provision.session_scoring_orchestrator._write_matrix_to_kv") as mock_kv,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["restored_version"] == "2026-06-01-a"
    assert body["restored_at"] == "2026-06-03T11:00:00+00:00"
    # Restored matrix pushed to the live scorer via KV (no Wasm rebuild).
    assert body["matrix_kv_written"] is True
    assert "KV" in body["deploy_hint"]
    mock_kv.assert_called_once_with("MATRIX_STORE", LOG_SVC, "tok")

    # state_sync was invoked with (service_id, version)
    mock_restore.assert_called_once_with(LOG_SVC, "2026-06-01-a")

    # local matrix.json must be gone so the next _load_matrix call
    # falls through to FOS instead of shadowing the restore.
    assert not fake_matrix_path.exists()

    # cfg.scoring.matrix_version was rolled back to the restored version
    assert saved["cfg"]["scoring"]["matrix_version"] == "2026-06-01-a"

    # Audit log recorded the mutation
    assert any(c["action"] == "matrix_restored" for c in audit_calls)
    restored_audit = next(c for c in audit_calls if c["action"] == "matrix_restored")
    assert restored_audit["details"]["restored_version"] == "2026-06-01-a"


def test_matrix_versions_restore_404_when_version_missing_in_fos(client, with_config):
    """If state_sync.restore_scoring_matrix_version returns None (the
    version key isn't present in FOS history), surface a 404 — and do
    NOT touch cfg or unlink the local matrix.json."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c"},
    }
    with (
        patch("backend.state_sync.restore_scoring_matrix_version", return_value=None),
        patch("backend.core.metadata.record_scoring_audit") as mock_audit,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/no-such-version/restore?confirm=true")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"]["error"].lower()
    # No mutation → no audit entry
    mock_audit.assert_not_called()


def test_matrix_versions_restore_rejects_path_traversal_at_framework(client, with_config):
    """The Path(...) pattern regex on the route is ``^[A-Za-z0-9._-]+$``
    — characters outside that set (slashes, shell-metas, etc.) are
    rejected before the handler runs. A URL-encoded ``..%2Fetc%2Fpasswd``
    gets decoded by Starlette and becomes ``../etc/passwd``, which the
    router treats as a different path (405/404) — the version string
    never reaches our handler with a slash. We also exercise a literal
    in-segment metacharacter (``$``, ``;``) to confirm the pattern
    regex rejects with 422 when the URL DOES route to the handler."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}

    with patch("backend.state_sync.restore_scoring_matrix_version") as mock_restore:
        # Path traversal via slash: URL routing won't even match this to our
        # endpoint (the literal ``../`` rewrites the path). 404/405 is fine —
        # the critical assertion is that state_sync is not invoked.
        r_traversal = client.post(
            f"/api/services/{LOG_SVC}/scoring/matrix-versions/..%2Fetc%2Fpasswd/restore?confirm=true"
        )
        # In-segment metacharacters (NOT slashes) DO reach the handler and
        # trip the pattern regex → 422.
        r_meta = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/bad;rm-rf/restore?confirm=true")

    assert r_traversal.status_code in (404, 405, 422), r_traversal.text
    assert r_meta.status_code == 422, r_meta.text
    mock_restore.assert_not_called()


def test_matrix_versions_restore_rejects_overlong_version(client, with_config):
    """Path constraint max_length=64 — a 65-char version trips 422 at
    the framework boundary. Defends against absurd FOS keys / accidental
    paste of a full JWT in the URL slot."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    overlong = "a" * 65
    with patch("backend.state_sync.restore_scoring_matrix_version") as mock_restore:
        r = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/{overlong}/restore?confirm=true")
    assert r.status_code == 422
    mock_restore.assert_not_called()


def test_matrix_versions_restore_500s_when_state_sync_raises(client, with_config):
    """If state_sync.restore_scoring_matrix_version raises (e.g.
    transient S3 outage during copy_object), FastAPI surfaces a 500 —
    the handler doesn't silently swallow the failure or return ok:true.
    Audit must not record a successful restore on a failed call.

    TestClient(raise_server_exceptions=False) so the framework returns
    the 500 response instead of re-raising into the test."""
    from fastapi.testclient import TestClient as _TC

    no_reraise_client = _TC(app, raise_server_exceptions=False)
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c"},
    }
    with (
        patch(
            "backend.state_sync.restore_scoring_matrix_version",
            side_effect=RuntimeError("S3 connection reset"),
        ),
        patch("backend.core.metadata.record_scoring_audit") as mock_audit,
    ):
        r = no_reraise_client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true")
    assert r.status_code == 500
    mock_audit.assert_not_called()


# ── /scoring/audit — operator action log readout ─────────────────────────────


def test_audit_returns_empty_list_when_no_rows(client, with_config):
    """A freshly enabled service with zero mutations yet → empty audit
    array, not 404. The admin UI relies on ``audit: []`` to render a
    'no operator actions yet' placeholder; 404 would falsely imply
    the service itself doesn't exist."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    with patch("backend.core.metadata.list_scoring_audit", return_value=[]):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit")
    assert r.status_code == 200
    assert r.json()["audit"] == []


def test_audit_returns_rows_newest_first(client, with_config):
    """The endpoint is a thin pass-through; verify rows reach the wire
    in the order the DB layer produced them (DESC by id/timestamp).
    Mocking at the metadata_db boundary keeps this test off SQLite."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        {
            "id": 3,
            "timestamp": "2026-06-03T10:00:00",
            "action": "threshold_committed",
            "actor": "operator",
            "details": {"new_threshold": 80},
        },
        {
            "id": 2,
            "timestamp": "2026-06-03T09:00:00",
            "action": "matrix_retrained",
            "actor": "operator",
            "details": None,
        },
        {
            "id": 1,
            "timestamp": "2026-06-03T08:00:00",
            "action": "scoring_enabled",
            "actor": "operator",
            "details": None,
        },
    ]
    with patch("backend.core.metadata.list_scoring_audit", return_value=canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit")
    assert r.status_code == 200
    body = r.json()
    assert len(body["audit"]) == 3
    # Pass-through preserves the metadata_db-layer ordering (newest first).
    assert [row["id"] for row in body["audit"]] == [3, 2, 1]
    assert body["audit"][0]["action"] == "threshold_committed"


def test_audit_limit_default_is_100(client, with_config):
    """Default limit must be 100 — pinned so a careless refactor of the
    Query() default doesn't silently inflate response sizes (every audit
    row carries a JSON details blob; 1000 rows is multi-KB)."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    captured: dict = {}

    def fake_list(service_id, *, limit, since=None):
        captured["limit"] = limit
        captured["since"] = since
        return []

    with patch("backend.core.metadata.list_scoring_audit", side_effect=fake_list):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit")
    assert r.status_code == 200
    assert captured["limit"] == 100
    assert captured["since"] is None
    assert r.json()["limit"] == 100


def test_audit_limit_capped_at_1000(client, with_config):
    """FastAPI's Query(le=1000) enforces the upper bound; values above
    must 422 instead of being silently clamped (a 5000-row request
    likely indicates the caller is paginating wrong)."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    with patch("backend.core.metadata.list_scoring_audit", return_value=[]):
        r_ok = client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1000")
        r_too_big = client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1001")
    assert r_ok.status_code == 200
    assert r_too_big.status_code == 422


def test_audit_since_param_forwarded_to_db_layer(client, with_config):
    """The since query param must reach list_scoring_audit verbatim —
    the metadata_db layer is what does the timestamp comparison; the
    router only validates/forwards."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    captured: dict = {}

    def fake_list(service_id, *, limit, since=None):
        captured["service_id"] = service_id
        captured["since"] = since
        return [
            {
                "id": 5,
                "timestamp": "2026-06-03T12:00:00",
                "action": "key_rotated",
                "actor": "operator",
                "details": None,
            }
        ]

    with patch("backend.core.metadata.list_scoring_audit", side_effect=fake_list):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit?since=2026-06-03T11:00:00")
    assert r.status_code == 200
    assert captured["service_id"] == LOG_SVC
    assert captured["since"] == "2026-06-03T11:00:00"
    # And the filtered row reaches the client.
    assert len(r.json()["audit"]) == 1
    assert r.json()["audit"][0]["action"] == "key_rotated"


def test_audit_404_on_unknown_service(client, with_config):
    """Service not in config registry → 404, matching the /scoring/status
    contract. Without this, an audit lookup for a non-existent service
    would falsely return ``{audit: []}`` and the UI would silently render
    a phantom service page."""
    # with_config left empty → load_config returns None → 404 path.
    r = client.get("/api/services/no-such-service/scoring/audit")
    assert r.status_code == 404
    assert "no config" in r.json()["detail"]["error"].lower()


# ── /scoring/enforce-threshold GET/PUT (live edge enforcement via ConfigStore) ─
#
# These cover the two new endpoints added in v1.1.0 that toggle live blocking
# at the Compute edge by writing the `enforce_threshold` key to the scoring
# ConfigStore. The Rust scorer re-reads the ConfigStore each request, so the
# round-trip from PUT -> effective blocking is ~seconds.
#
# Mocking strategy mirrors the /scoring/threshold tests above:
#   - `with_config` controls what backend.config.load_config returns (so the
#     scoring_config_store_id + fastly_api_key are visible to the handler).
#   - `backend.core.fastly.client.fastly` is patched per-test to fake the
#     ConfigStore HTTP layer (raise RuntimeError("404 ...") to simulate
#     not-present, raise RuntimeError for the read-failure case, return dicts
#     for the happy paths).
#   - `backend.core.metadata_db.record_scoring_audit` is captured so PUT tests
#     can assert the audit action name + details payload (best-effort writer,
#     so no exception propagation to worry about).
#
# Kept self-contained at the bottom of the file so parallel edits to the
# threshold/matrix-versions/audit sections above don't collide.


class _EnforceThresholdFixtures:
    """Reusable cfg snippets for the enforce-threshold tests.

    The handler requires both:
      - scoring.scoring_config_store_id (otherwise it 400s before touching the
        Fastly API),
      - a resolvable token (either via ?token= or fastly_api_key in cfg).
    """

    @staticmethod
    def enabled_cfg() -> dict:
        return {
            "service_id": LOG_SVC,
            "fastly_api_key": "TOKEN",
            "scoring": {
                "enabled": True,
                "scoring_config_store_id": "cs_abc123",
            },
        }


def test_scoring_enforce_threshold_get_400_when_scoring_not_enabled(client, with_config):
    """No scoring block -> no config_store_id -> handler 400s before any API call.

    Asserts the explicit error message so we catch regressions where the
    handler starts silently returning {threshold: null} for unconfigured
    services (which would mask "scoring is off" in the UI)."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC}  # no scoring block at all
    r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold")
    assert r.status_code == 400
    assert "Scoring not enabled" in r.json()["detail"]["error"]


def test_scoring_enforce_threshold_get_returns_unset_when_configstore_404s(client, with_config):
    """404 from ConfigStore = key never written = enforcement off.

    The handler converts the RuntimeError("404 ...") that fastly() raises
    into threshold=None rather than bubbling up an HTTP 502 - this is the
    pre-rollout default state for every newly-provisioned service."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    def fake_fastly(method, path, *args, **kwargs):
        # mirror the runtime error fastly() raises for a missing item
        raise RuntimeError("Fastly API 404: not found")

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold")

    assert r.status_code == 200
    body = r.json()
    # M1 backstop adds _debug_* keys; assert the meaningful fields explicitly.
    assert body["threshold"] is None
    assert body["enforced"] is False
    assert body["key"] == "enforce_threshold"


def test_scoring_enforce_threshold_get_returns_value_when_set(client, with_config):
    """Happy path: ConfigStore has the key set to an int -> handler returns
    threshold + enforced=True so the UI can show the live blocking state."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    def fake_fastly(method, path, *args, **kwargs):
        assert method == "GET"
        assert path.endswith("/item/enforce_threshold")
        return {"item_key": "enforce_threshold", "item_value": "75"}

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold")

    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] == 75
    assert body["enforced"] is True
    assert body["key"] == "enforce_threshold"


def test_scoring_enforce_threshold_get_502_on_generic_configstore_error(client, with_config):
    """Any non-404 RuntimeError from fastly() should surface as HTTP 502
    (narrowed exception handling - we don't want a silent threshold=None
    response masking real Fastly API outages)."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    def fake_fastly(method, path, *args, **kwargs):
        raise RuntimeError("Fastly API 503: service unavailable")

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold")

    assert r.status_code == 502
    detail = r.json()["detail"]
    assert detail["error"] == "enforce_threshold_read_failed"
    assert "error_id" in detail


def test_scoring_enforce_threshold_put_requires_confirm_flag(client, with_config):
    """Without ?confirm=true the PUT must 400 - this is the kill switch that
    prevents an accidental click from flipping live edge blocking."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-threshold",
        json={"threshold": 75},
    )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_scoring_enforce_threshold_put_writes_value_and_records_audit(client, with_config):
    """Happy path: confirm=true + valid int -> upsert into ConfigStore + audit.

    Asserts:
      - the fastly() upsert is called (either PATCH or POST - handler tries
        PATCH first, falls back to POST on failure; we accept either),
      - record_scoring_audit fires with the 'threshold_enforced' action and
        the threshold echoed in the details payload,
      - response carries enforced=True + the chosen threshold."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    fastly_calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        fastly_calls.append((method, path, body))
        return {}  # PATCH succeeds, no POST fallback needed

    audit_calls: list[tuple] = []

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true",
            json={"threshold": 75},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] == 75
    assert body["enforced"] is True
    assert body.get("ok") is True

    # ConfigStore upsert called with the stringified value (ConfigStore items
    # are always strings even when they semantically represent ints)
    assert len(fastly_calls) >= 1
    method, path, payload = fastly_calls[0]
    assert method in ("PATCH", "POST")
    assert "enforce_threshold" in path or (payload or {}).get("item_key") == "enforce_threshold"
    assert (payload or {}).get("item_value") == "75"

    # Audit captured with the 'set' action name + threshold detail
    assert len(audit_calls) == 1
    svc, action, details = audit_calls[0]
    assert svc == LOG_SVC
    assert action == "threshold_enforced"
    assert details == {"threshold": 75}


def test_scoring_enforce_threshold_put_clears_when_null(client, with_config):
    """threshold=null path: upserts an empty string into the ConfigStore key
    (the scorer treats empty == not-set) and records the 'disabled' audit
    action so the operator can see when enforcement was turned off."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    fastly_calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        fastly_calls.append((method, path, body))
        return {}

    audit_calls: list[tuple] = []

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true",
            json={"threshold": None},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["threshold"] is None
    assert body["enforced"] is False

    # value written is the empty string (scorer's "no enforcement" sentinel)
    assert fastly_calls, "expected at least one fastly() call to upsert the cleared value"
    _method, _path, payload = fastly_calls[0]
    assert (payload or {}).get("item_value") == ""

    # Audit logs the 'disabled' action with threshold=None
    assert len(audit_calls) == 1
    _svc, action, details = audit_calls[0]
    assert action == "threshold_enforce_disabled"
    assert details == {"threshold": None}


def test_scoring_enforce_threshold_put_rejects_out_of_range(client, with_config):
    """Same 0-100 validator as /scoring/threshold - threshold > 100 -> 400.
    Mirrors test_scoring_threshold_put_rejects_out_of_range above."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true",
        json={"threshold": 150},
    )
    assert r.status_code == 400
    assert "0-100" in r.json()["detail"]["error"]


def test_scoring_enforce_threshold_put_rejects_negative(client, with_config):
    """Lower bound of the validator: negative ints also 400."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true",
        json={"threshold": -5},
    )
    assert r.status_code == 400


def test_scoring_enforce_threshold_put_400_when_scoring_not_enabled(client, with_config):
    """PUT against a service with no scoring block -> 400 before any audit or
    Fastly side-effect. Note the confirm gate fires first so we must include
    ?confirm=true to actually reach the scoring-enabled check."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC}  # no scoring block
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true",
        json={"threshold": 75},
    )
    assert r.status_code == 400
    assert "Scoring not enabled" in r.json()["detail"]["error"]


# ── /scoring/l2-enforce GET/PUT (operator opt-in for edge L2 enforcement) ─────
#
# L2's contribution to the *enforced* combined score is gated by an explicit
# operator opt-in (l2_enforce_enabled in the scoring_config ConfigStore) and
# fades in over 3 days from the l2_enabled_at anchor — NOT from deployment age.
# These mirror the enforce-threshold tests above (same with_config + fastly mock
# strategy); the pure ramp math is pinned separately in
# test_build_l2_enforce_block_* so the HTTP tests can stay shape-focused.


def test_build_l2_enforce_block_ramp_math():
    """Pin the opt-in fade-in + readiness math directly (no HTTP, fixed ``now``).
    This is the new-behaviour pinning test: ramp_progress tracks days-since-opt-in
    over a 3-day window, ``ready`` is the advisory deploy-age≥7 gauge, and a future
    anchor clamps to 0 — all decoupled from deployment age."""
    from backend.routers.session_scoring_admin import _build_l2_enforce_block

    day = 86_400
    now = 1_000_000_000
    # Opted in 1.5 days ago, deployed 10 days ago → half-ramped, ready.
    b = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 10 * day),
        l2_enforce_raw="1",
        l2_enabled_at_raw=str(now - int(1.5 * day)),
        now_epoch=now,
    )
    assert b["available"] is True
    assert b["enabled"] is True
    assert b["days_since_optin"] == pytest.approx(1.5, abs=1e-6)
    assert b["ramp_progress"] == pytest.approx(0.5, abs=1e-6)
    assert b["fully_ramped"] is False
    assert b["warmup_days_remaining"] == pytest.approx(1.5, abs=1e-6)
    assert b["deployment_age_days"] == pytest.approx(10.0, abs=1e-6)
    assert b["ready"] is True

    # Flag off → enabled False even with a stale (fully-ramped) anchor.
    off = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 30 * day),
        l2_enforce_raw="0",
        l2_enabled_at_raw=str(now - 30 * day),
        now_epoch=now,
    )
    assert off["enabled"] is False
    assert off["fully_ramped"] is True  # anchor math still reported
    assert off["ready"] is True

    # Just opted in (anchor == now) → ramp opens at 0; young deploy → not ready.
    fresh = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 2 * day),
        l2_enforce_raw="1",
        l2_enabled_at_raw=str(now),
        now_epoch=now,
    )
    assert fresh["ramp_progress"] == 0.0
    assert fresh["fully_ramped"] is False
    assert fresh["ready"] is False  # 2 days < 7-day readiness gauge

    # No anchor seeded → opt-in fields null, ramp 0 (the pre-opt-in default).
    none = _build_l2_enforce_block(
        scoring_enabled_at_raw=None,
        l2_enforce_raw=None,
        l2_enabled_at_raw=None,
        now_epoch=now,
    )
    assert none["enabled"] is False
    assert none["days_since_optin"] is None
    assert none["ramp_progress"] == 0.0
    assert none["deployment_age_days"] is None
    assert none["ready"] is False


def test_scoring_l2_enforce_get_400_when_scoring_not_enabled(client, with_config):
    """No scoring_config_store_id → 400 before any Fastly call, matching the
    enforce-threshold GET contract."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC}  # no scoring block
    r = client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce")
    assert r.status_code == 400
    assert "Scoring not enabled" in r.json()["detail"]["error"]


def test_scoring_l2_enforce_get_returns_state_shape(client, with_config):
    """Happy path: GET reads the flag + anchors and returns the enabled/ramp/
    readiness shape. Uses anchors deep in the past so the ramp/readiness are
    fully saturated regardless of the wall clock."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    def fake_fastly(method, path, *args, **kwargs):
        assert method == "GET"
        if path.endswith("/item/l2_enforce_enabled"):
            return {"item_value": "1"}
        if path.endswith("/item/l2_enabled_at"):
            return {"item_value": "1700000000"}  # 2023 → fully ramped
        if path.endswith("/item/scoring_enabled_at"):
            return {"item_value": "1700000000"}  # 2023 → ready
        raise AssertionError(f"unexpected GET {path}")

    with patch("backend.core.fastly.client.fastly", side_effect=fake_fastly):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce")

    assert r.status_code == 200
    body = r.json()
    assert body["available"] is True
    assert body["enabled"] is True
    assert body["fully_ramped"] is True
    assert body["ramp_progress"] == 1.0
    assert body["ready"] is True
    assert body["l2_enabled_at"] == 1700000000


def test_scoring_l2_enforce_put_requires_confirm_flag(client, with_config):
    """Without ?confirm=true the PUT must 400 — same kill switch as
    enforce-threshold (it changes live edge blocking)."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/l2-enforce",
        json={"enabled": True},
    )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_scoring_l2_enforce_put_enable_writes_flag_anchor_and_audit(client, with_config):
    """Enable from off: writes l2_enforce_enabled="1" AND stamps a fresh
    l2_enabled_at anchor (epoch seconds), and audits 'l2_enforce_enabled'."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    writes: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        if method == "GET":
            # currently-off + no anchor yet.
            raise RuntimeError("Fastly API 404: not found")
        writes.append((method, path, body))
        return {}

    audit_calls: list[tuple] = []

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true",
            json={"enabled": True},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["ok"] is True
    assert isinstance(body["l2_enabled_at"], int)

    # Both the flag and the anchor were written.
    def _wrote(key, value_pred):
        for method, path, payload in writes:
            payload = payload or {}
            key_match = path.endswith(f"/item/{key}") or payload.get("item_key") == key
            if key_match and value_pred(payload.get("item_value")):
                return True
        return False

    assert _wrote("l2_enforce_enabled", lambda v: v == "1"), writes
    assert _wrote("l2_enabled_at", lambda v: v and str(v).isdigit()), writes

    assert len(audit_calls) == 1
    svc, action, details = audit_calls[0]
    assert svc == LOG_SVC
    assert action == "l2_enforce_enabled"
    assert details["enabled"] is True
    assert isinstance(details["l2_enabled_at"], int)


def test_scoring_l2_enforce_put_enable_when_already_on_preserves_anchor(client, with_config):
    """Re-confirming an already-on service must NOT reset an in-progress fade-in:
    the flag is re-written but the existing l2_enabled_at anchor is preserved
    (no anchor write)."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    writes: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        if method == "GET":
            if path.endswith("/item/l2_enforce_enabled"):
                return {"item_value": "1"}  # already on
            if path.endswith("/item/l2_enabled_at"):
                return {"item_value": "1700000000"}  # existing anchor
            raise RuntimeError("Fastly API 404: not found")
        writes.append((method, path, body))
        return {}

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=lambda *a, **k: None),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true",
            json={"enabled": True},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is True
    assert body["l2_enabled_at"] == 1700000000  # preserved, not reset

    # The anchor key must NOT have been (re)written.
    for method, path, payload in writes:
        payload = payload or {}
        is_anchor = path.endswith("/item/l2_enabled_at") or payload.get("item_key") == "l2_enabled_at"
        assert not is_anchor, f"anchor must be preserved, but a write targeted it: {(method, path, payload)}"


def test_scoring_l2_enforce_put_disable_writes_zero(client, with_config):
    """Disable: writes l2_enforce_enabled="0" and audits 'l2_enforce_disabled'.
    The anchor is left untouched so a later re-enable restarts the fade-in."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    writes: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        if method == "GET":
            raise RuntimeError("Fastly API 404: not found")
        writes.append((method, path, body))
        return {}

    audit_calls: list[tuple] = []

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true",
            json={"enabled": False},
        )

    assert r.status_code == 200
    assert r.json()["enabled"] is False

    # Wrote "0" to the flag; never wrote the anchor.
    flag_writes = [
        p
        for (m, path, p) in writes
        if path.endswith("/item/l2_enforce_enabled") or (p or {}).get("item_key") == "l2_enforce_enabled"
    ]
    assert flag_writes, writes
    assert (flag_writes[0] or {}).get("item_value") == "0"
    for method, path, payload in writes:
        payload = payload or {}
        is_anchor = path.endswith("/item/l2_enabled_at") or payload.get("item_key") == "l2_enabled_at"
        assert not is_anchor

    assert len(audit_calls) == 1
    _svc, action, details = audit_calls[0]
    assert action == "l2_enforce_disabled"
    assert details["enabled"] is False


def test_scoring_l2_enforce_body_defaults_to_false():
    """ScoringL2EnforceBody.enabled defaults to False so an empty body is a
    no-op-toward-off, never an accidental enable."""
    from backend.models.session_scoring import ScoringL2EnforceBody

    assert ScoringL2EnforceBody().enabled is False
    assert ScoringL2EnforceBody(enabled=True).enabled is True


def test_scoring_health_includes_l2_enforce_readiness_block(client, with_config):
    """scoring_health carries the admin-only l2_enforce readiness block built
    from the scoring_config ConfigStore (best-effort, separately cached). Pin its
    presence + shape so the L2EnforcementCard can read it off the composite."""
    cfg = _EnforceThresholdFixtures.enabled_cfg()
    with_config[LOG_SVC] = cfg
    canned = [{"total_edge_rows": 100, "scored_rows": 50, "top_reasons": [], "l2_evaluated": 0, "l2_high_count": 0}]

    def fake_fastly(method, path, *args, **kwargs):
        if path.endswith("/item/l2_enforce_enabled"):
            return {"item_value": "1"}
        if path.endswith("/item/l2_enabled_at"):
            return {"item_value": "1700000000"}
        if path.endswith("/item/scoring_enabled_at"):
            return {"item_value": "1700000000"}
        raise AssertionError(f"unexpected fastly call {method} {path}")

    with (
        patch("backend.repositories.session_scoring.query_logs", return_value=canned),
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
    ):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")

    assert r.status_code == 200
    block = r.json()["l2_enforce"]
    assert block["available"] is True
    assert block["enabled"] is True
    assert block["ready"] is True
    assert block["fully_ramped"] is True


def test_scoring_health_l2_enforce_unavailable_without_config_store(client, with_config):
    """When the service has no scoring_config_store_id, the readiness block
    degrades to available=False (no Fastly call) rather than breaking health."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [{"total_edge_rows": 0, "scored_rows": 0, "top_reasons": []}]
    with patch("backend.repositories.session_scoring.query_logs", return_value=canned):
        from backend.routers import session_scoring as _ss

        _ss._analytics_cache.clear()
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r.status_code == 200
    assert r.json()["l2_enforce"]["available"] is False


# ── /scoring/enforce-status-code (operator-overridable HTTP code) ──────────


def test_scoring_enforce_status_code_get_returns_default_when_unset(client, with_config):
    """GET with cfg.scoring.enforce_status_code absent → returns the
    built-in default (429) with is_default=True."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] is None
    assert body["default"] == 429
    assert body["effective"] == 429
    assert body["is_default"] is True
    assert body["min"] == 400
    assert body["max"] == 599


def test_scoring_enforce_status_code_get_returns_override_when_set(client, with_config):
    """GET with an operator-supplied value returns it as both current +
    effective, and flips is_default to False."""
    cfg = _EnforceThresholdFixtures.enabled_cfg()
    cfg["scoring"]["enforce_status_code"] = 403
    with_config[LOG_SVC] = cfg
    r = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code")
    assert r.status_code == 200
    body = r.json()
    assert body["current"] == 403
    assert body["effective"] == 403
    assert body["is_default"] is False


def test_scoring_enforce_status_code_put_requires_confirm_flag(client, with_config):
    """Without ?confirm=true the PUT must 400 — same kill-switch shape as
    enforce-threshold."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-status-code",
        json={"status_code": 403},
    )
    assert r.status_code == 400
    assert "confirm=true" in r.json()["detail"]["error"]


def test_scoring_enforce_status_code_put_rejects_out_of_range(client, with_config):
    """Status code outside HTTP 400-599 → 400."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    for bad in (399, 600, 200, 0, -1):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
            json={"status_code": bad},
        )
        assert r.status_code == 400, f"expected 400 for status_code={bad}"
        assert "400-599" in r.json()["detail"]["error"]


def test_scoring_enforce_status_code_put_rejects_non_int(client, with_config):
    """status_code that can't be coerced to int → 422 from the Pydantic
    body validator (matches the body/query validation classification
    introduced in commit 3c036cf)."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
        json={"status_code": "not-an-int"},
    )
    assert r.status_code == 422
    errors = r.json()["detail"]
    assert any(e.get("loc", [])[-1] == "status_code" for e in errors)


def test_scoring_enforce_status_code_put_400_when_scoring_not_enabled(client, with_config):
    """No scoring block → 400 before any Fastly side-effect."""
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
        json={"status_code": 403},
    )
    assert r.status_code == 400
    assert "Session scoring is not enabled" in r.json()["detail"]["error"]


def test_scoring_enforce_status_code_put_happy_path(client, with_config):
    """Valid 4xx code + confirm=true + scoring enabled + resolvable token →
    calls the orchestrator, records an audit row, returns 200 with the new
    effective code."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    orchestrator_calls: list[dict] = []

    def fake_update(service_id, token, *, new_status_code):
        orchestrator_calls.append({"service_id": service_id, "token": token, "new_status_code": new_status_code})
        return {
            "effective_status_code": new_status_code or 429,
            "is_default": new_status_code is None,
            "logging_service_active_version": 42,
        }

    audit_calls: list[tuple] = []

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch(
            "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
            side_effect=fake_update,
        ),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
            json={"status_code": 451},
        )

    assert r.status_code == 200, r.json()
    body = r.json()
    assert body["ok"] is True
    assert body["effective_status_code"] == 451
    assert body["is_default"] is False
    assert body["logging_service_active_version"] == 42

    assert len(orchestrator_calls) == 1
    assert orchestrator_calls[0]["new_status_code"] == 451

    assert len(audit_calls) == 1
    _svc, action, details = audit_calls[0]
    assert action == "scoring_enforce_status_code_changed"
    assert details["effective_status_code"] == 451
    assert details["is_default"] is False


def test_scoring_enforce_status_code_put_null_resets_to_default(client, with_config):
    """status_code=null → orchestrator is called with new_status_code=None
    and the response reports is_default=True."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()

    def fake_update(service_id, token, *, new_status_code):
        return {
            "effective_status_code": 429,
            "is_default": True,
            "logging_service_active_version": 7,
        }

    with (
        patch(
            "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
            side_effect=fake_update,
        ),
        patch("backend.core.metadata.record_scoring_audit"),
    ):
        r = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
            json={"status_code": None},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["is_default"] is True
    assert body["effective_status_code"] == 429
    assert "Reset to default" in body["message"]


def test_cached_drops_inflight_entry_on_cache_hit():
    """Regression: _cached previously skipped the ``_inflight.pop(key)``
    cleanup whenever the cache-hit branch early-returned, because the
    cleanup lived in the producer-path try/finally. The result was at
    most one stuck Lock object per distinct key — bounded by key
    cardinality but slow growth across the TTL window — and a runtime
    contract that didn't match the comment above the pop line. Pinned
    so a regression that puts the try/finally back inside the producer
    branch fails this test immediately.
    """
    from backend.routers import session_scoring as _ss

    _ss._analytics_cache.clear()
    _ss._inflight.clear()
    key = ("test_endpoint", "svc-test", 24)

    # Prime the cache via the first call (producer runs once).
    _ss._cached(key, lambda: {"foo": 1})
    # First call's finally already cleared _inflight.
    assert key not in _ss._inflight

    # Second call hits the cache. The fix's outer try/finally must also
    # clear _inflight on this path, even though the producer never runs.
    # If a regression collapses the try/finally back around just the
    # producer, this would leak a Lock here.
    produced = {"flag": False}

    def producer():
        produced["flag"] = True
        return {"foo": 999}

    _ss._cached(key, producer)
    assert produced["flag"] is False, "cache hit must not invoke producer"
    assert key not in _ss._inflight, (
        "regression: _inflight retains a Lock after a cache hit. The fix's "
        "outer try/finally was reverted into a producer-branch-only finally."
    )


# ── H3: analyst time-window clamp on scoring reads ───────────────────────────
#
# H3 clamps the per-invite query window on the analyst-reachable scoring reads
# (the scope bypass). IP masking stays the response middleware's job (key-name
# pass over the ``ip`` column); ua/url are intentionally left intact — analysts
# triage flagged sessions on them.


def _fake_request(analyst_session):
    """Minimal stand-in for the per-request object the scoring helpers read.

    The middleware stamps ``request.state.analyst_session``; ``_scoring_time_window``
    and the ``get_analyst_time_bounds`` dependency only touch that attribute.
    """
    from types import SimpleNamespace

    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def test_scoring_time_window_admin_uses_relative_interval():
    from backend.routers import session_scoring as _ss

    pred, disc = _ss._scoring_time_window(_fake_request(None), 24)
    assert pred == "timestamp >= now() - INTERVAL 24 HOUR"
    assert disc is None


def test_scoring_time_window_analyst_clamps_to_invite_window():
    """An analyst scoped to 1h who asks for 168h gets an absolute window
    clamped to ~1h — not the relative 168h interval."""
    import re
    from types import SimpleNamespace

    from backend.routers import session_scoring as _ss
    from backend.utils.date_utils import parse_iso_utc

    session = SimpleNamespace(query_window_hours=1, query_start_time=None, query_end_time=None, pii_policy={})
    pred, disc = _ss._scoring_time_window(_fake_request(session), 168)

    assert "INTERVAL" not in pred  # not the relative form
    assert pred.startswith("timestamp >= TIMESTAMPTZ '")
    assert "AND timestamp < TIMESTAMPTZ '" in pred
    assert disc is not None  # discriminator keeps scoped results out of the admin cache

    lits = re.findall(r"TIMESTAMPTZ '([^']+)'", pred)
    span_h = (parse_iso_utc(lits[1]) - parse_iso_utc(lits[0])).total_seconds() / 3600
    assert 0.9 <= span_h <= 1.1, f"expected ~1h clamp, got {span_h}h"


def test_top_flagged_analyst_clamps_window_and_keeps_ua_url(monkeypatch):
    """End-to-end through the handler: analyst → SQL uses the clamped absolute
    window (not INTERVAL 168), and ua/url stay in the rows (analysts need
    them; ip is masked downstream by the middleware)."""
    from types import SimpleNamespace

    from backend.routers import session_scoring as _ss

    captured: dict = {}

    def fake_query_logs(service_id, sql, params=()):
        captured["sql"] = sql
        return [{"ip": "1.2.3.4", "ua": "Mozilla", "url": "/secret", "edge_score": 91, "edge_sid": "s1"}]

    monkeypatch.setattr(_ss, "_query_logs", fake_query_logs)
    session = SimpleNamespace(
        query_window_hours=1, query_start_time=None, query_end_time=None, pii_policy={"mask_ips": True}
    )
    out = _ss.scoring_top_flagged(request=_fake_request(session), service_id="svc", since_hours=168, limit=50)

    assert "INTERVAL 168 HOUR" not in captured["sql"]
    assert "TIMESTAMPTZ" in captured["sql"]
    # ua/url preserved for the analyst triage view; ip masking is the middleware's job.
    assert out["rows"][0]["ua"] == "Mozilla"
    assert out["rows"][0]["url"] == "/secret"


def test_top_flagged_admin_keeps_full_window(monkeypatch):
    from backend.routers import session_scoring as _ss

    captured: dict = {}

    def fake_query_logs(service_id, sql, params=()):
        captured["sql"] = sql
        return [{"ip": "1.2.3.4", "ua": "Mozilla", "url": "/x", "edge_score": 91}]

    monkeypatch.setattr(_ss, "_query_logs", fake_query_logs)
    out = _ss.scoring_top_flagged(request=_fake_request(None), service_id="svc", since_hours=24, limit=50)

    assert "INTERVAL 24 HOUR" in captured["sql"]
    assert out["rows"][0]["ua"] == "Mozilla"
    assert out["rows"][0]["url"] == "/x"


def test_session_events_analyst_clamps_window(monkeypatch):
    """The admin events route is analyst-reachable: an analyst gets the
    lookback clamped (a ts_predicate is passed to the repo); ua/url stay."""
    from types import SimpleNamespace

    from backend.routers import session_scoring_admin as _admin

    captured: dict = {}

    def fake_fetch(service_id, sids, since_days=30, limit_per_sid=500, *, ts_predicate=None):
        captured["ts_predicate"] = ts_predicate
        return {sids[0]: [{"ts": "t", "url": "/secret", "status": 200, "ip": "1.2.3.4", "ua": "Mozilla"}]}

    monkeypatch.setattr(_admin, "_fetch_session_events", fake_fetch)
    session = SimpleNamespace(
        query_window_hours=1, query_start_time=None, query_end_time=None, pii_policy={"mask_ips": True}
    )
    out = _admin.scoring_session_events(request=_fake_request(session), service_id="svc", sid="abc", since_days=90)

    # Analyst → the repo received a clamped absolute predicate, not None.
    assert captured["ts_predicate"] is not None
    assert "TIMESTAMPTZ" in captured["ts_predicate"]
    ev = out["events"][0]
    assert ev["ua"] == "Mozilla"
    assert ev["url"] == "/secret"


def test_session_events_admin_full_window(monkeypatch):
    from backend.routers import session_scoring_admin as _admin

    captured: dict = {}

    def fake_fetch(service_id, sids, since_days=30, limit_per_sid=500, *, ts_predicate=None):
        captured["ts_predicate"] = ts_predicate
        return {sids[0]: [{"ts": "t", "url": "/x", "status": 200, "ip": "1.2.3.4", "ua": "Mozilla"}]}

    monkeypatch.setattr(_admin, "_fetch_session_events", fake_fetch)
    out = _admin.scoring_session_events(request=_fake_request(None), service_id="svc", sid="abc", since_days=90)

    # Admin → no clamp predicate; repo uses its own relative since_days window.
    assert captured["ts_predicate"] is None
    assert out["events"][0]["ua"] == "Mozilla"


# ── _scoring_source: graceful degradation pre-enablement ──────────────────────
#
# Before scoring is provisioned on a service, the parquet schema lacks the
# edge_score* / edge_sid / edge_cookie_compliance columns the scoring VCL adds.
# Every scoring read endpoint binds those columns directly, so without the
# _scoring_source wrapper DuckDB raises a binder error and the whole admin page
# 500s before the operator has even turned scoring on. These tests pin the
# wrapper + verify the endpoints degrade to empty/zero (HTTP 200) instead.


def test_scoring_source_passthrough_when_all_present():
    from backend.routers import session_scoring as _ss

    cols = set(_ss._SCORING_COLUMN_TYPES) | {"edge", "timestamp"}
    assert _ss._scoring_source("logs_x", cols) == "logs_x"


def test_scoring_source_passthrough_on_empty_cols():
    """Empty col set = schema unknown / view not ready → don't synthesize
    (avoids a duplicate-column error if a column actually exists)."""
    from backend.routers import session_scoring as _ss

    assert _ss._scoring_source("logs_x", set()) == "logs_x"


def test_scoring_source_synthesizes_missing_core_columns():
    from backend.routers import session_scoring as _ss

    # Base-only schema: has `edge` but none of the scoring columns.
    src = _ss._scoring_source("logs_x", {"edge", "timestamp", "ip"})
    assert src.startswith("(SELECT *, ") and src.endswith(" FROM logs_x)")
    for col, typ in _ss._SCORING_COLUMN_TYPES.items():
        assert f"CAST(NULL AS {typ}) AS {col}" in src
    # Latency cols are handled separately and must NOT be synthesized here.
    assert "edge_score_rtt_us" not in src
    assert "edge_score_exec_us" not in src


@pytest.fixture
def _noscore_table(monkeypatch):
    """Seed a real in-memory DuckDB ``logs_noscore`` table with BASE columns
    only (no edge_score*), and route ``session_scoring._query_logs`` at it so
    both ``_table_columns``' DESCRIBE and the endpoint SQL execute for real.

    Yields the service_id whose ``_safe_table_name`` maps to ``logs_noscore``.
    """
    import duckdb

    from backend.routers import session_scoring as _ss

    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE logs_noscore (
            timestamp TIMESTAMP, edge BOOLEAN, ip VARCHAR,
            ua VARCHAR, url VARCHAR, status INTEGER, country VARCHAR
        )
        """
    )
    # One edge row, no scores — inside the default 24h window.
    con.execute("INSERT INTO logs_noscore VALUES (now()::TIMESTAMP, true, '1.2.3.4', 'UA', '/', 200, 'US')")

    def fake_query_logs(service_id, sql, params=()):
        cur = con.execute(sql, params) if params else con.execute(sql)
        rows = cur.fetchall()
        cnames = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cnames, r)) for r in rows]

    monkeypatch.setattr(_ss, "_query_logs", fake_query_logs)
    yield "noscore"
    con.close()


def test_scoring_endpoints_degrade_when_scoring_never_enabled(_noscore_table):
    """The reported bug: every scoring card showed a DuckDB binder error
    ('Referenced column "edge_score" not found') before scoring was enabled.
    With _scoring_source the endpoints must return 200 empty/zero shapes."""
    from backend.routers import session_scoring as _ss

    svc = _noscore_table
    req = _fake_request(None)  # admin (no analyst session) → relative window

    # List endpoints: no scored rows → empty rows, no binder error.
    assert _ss.scoring_top_flagged(request=req, service_id=svc, since_hours=24, limit=50)["rows"] == []
    assert _ss.scoring_score_distribution(request=req, service_id=svc, since_hours=24)["rows"] == []
    assert _ss.scoring_compliance_breakdown(request=req, service_id=svc, since_hours=24)["rows"] == []

    # Latency timeseries: runs over edge rows; has_latency False (no rtt/exec).
    lat = _ss.scoring_latency_timeseries(request=req, service_id=svc, since_hours=24)
    assert lat["has_latency"] is False

    # Threshold preview: no scored sids → zero totals.
    prev = _ss.scoring_threshold_preview(request=req, service_id=svc, threshold=75, since_hours=24)
    assert prev["total_scored_sessions"] == 0

    # Health: the big composite — degrades to all-zero, no binder error.
    health = _ss.scoring_health(request=req, service_id=svc, since_hours=24)
    assert health["scored_rows"] == 0
    assert health["fire_rate_pct"] == 0.0
    assert health["avg_score"] == 0
    assert health["scorer_errors"] == 0
    assert health["top_reasons"] == []
    assert health["fail_open_breakdown"] == []
