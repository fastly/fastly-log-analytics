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
    yield
    _ss._analytics_cache.clear()
    _ss._inflight.clear()


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
    assert "sid" in r.json()["detail"]["error"].lower()


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

    009: the route now issues TWO queries (aggregate counts across all
    sids, then per-sid scores for the bounded labeled set) instead of
    materialising one row per sid in Python. The mock returns the
    right shape per call based on the SQL it sees.
    """
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}

    # 6 sessions: 2 labeled-bad above threshold, 1 labeled-good above
    # (false positive), 1 unlabeled above, 1 labeled-good below, 1
    # labeled-bad below (false negative).
    labeled_score_rows = [
        {"edge_sid": "bad1", "max_score": 80},
        {"edge_sid": "bad2", "max_score": 75},
        {"edge_sid": "good1", "max_score": 60},  # false positive at threshold 50
        {"edge_sid": "good2", "max_score": 10},
        {"edge_sid": "bad3", "max_score": 20},  # false negative at threshold 50
    ]
    # Aggregate row: 6 total, 4 flagged (bad1, bad2, good1, unlbl1),
    # 2 passed (good2, bad3). Mirrors the labeled_score_rows + the
    # un-labeled unlbl1 sid at max_score=55.
    agg_row = {"total": 6, "flagged_total": 4, "passed_total": 2}

    def _route_query(_service_id, sql, *_args, **_kwargs):
        # Aggregate-counts SQL uses ``WITH sid_scores AS (...) SELECT
        # COUNT(*) ...``. The labeled-sid query uses ``WHERE edge_sid
        # IN (?, ?, ...)``. Route on the ``IN (`` marker.
        return labeled_score_rows if " IN (" in sql else [agg_row]

    fake_labels = [
        {"sid": "bad1", "label": "bad"},
        {"sid": "bad2", "label": "bad"},
        {"sid": "bad3", "label": "bad"},
        {"sid": "good1", "label": "good"},
        {"sid": "good2", "label": "good"},
    ]
    fake_counts = {"good": 2, "bad": 3, "neutral": 0}

    with (
        patch("backend.repositories.session_scoring.query_logs", side_effect=_route_query),
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
    assert body["flagged"]["unlabeled"] == 1
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

    def _route_query(_service_id, sql, *_args, **_kwargs):
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
        "scoring": {"enabled": True, "scoring_service_id": "scorer-x"},
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
        "health",
        "top_flagged",
        "score_distribution",
        "compliance_breakdown",
        "curves",
        "threshold_preview",
    ):
        assert key in body, f"missing key {key!r}"
    assert body["since_hours"] == 24
    assert body["threshold"] == 75


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
    returns ok + restored_version + deploy_hint."""
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c"},
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
        patch("backend.core.metadata_db.record_scoring_audit", side_effect=fake_audit),
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["restored_version"] == "2026-06-01-a"
    assert body["restored_at"] == "2026-06-03T11:00:00+00:00"
    assert "deploy_hint" in body
    assert "deploy_wasm.sh" in body["deploy_hint"]

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
        patch("backend.core.metadata_db.record_scoring_audit") as mock_audit,
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
        patch("backend.core.metadata_db.record_scoring_audit") as mock_audit,
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
    with patch("backend.core.metadata_db.list_scoring_audit", return_value=[]):
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
    with patch("backend.core.metadata_db.list_scoring_audit", return_value=canned):
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

    with patch("backend.core.metadata_db.list_scoring_audit", side_effect=fake_list):
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
    with patch("backend.core.metadata_db.list_scoring_audit", return_value=[]):
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

    with patch("backend.core.metadata_db.list_scoring_audit", side_effect=fake_list):
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
    assert "failed to read enforce threshold" in detail["error"]


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
        patch("backend.core.metadata_db.record_scoring_audit", side_effect=fake_audit),
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
        patch("backend.core.metadata_db.record_scoring_audit", side_effect=fake_audit),
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
    """status_code that can't be coerced to int → 400 with a clear message."""
    with_config[LOG_SVC] = _EnforceThresholdFixtures.enabled_cfg()
    r = client.put(
        f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true",
        json={"status_code": "not-an-int"},
    )
    assert r.status_code == 400
    assert "integer" in r.json()["detail"]["error"]


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
        patch("backend.core.metadata_db.record_scoring_audit", side_effect=fake_audit),
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
        patch("backend.core.metadata_db.record_scoring_audit"),
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
