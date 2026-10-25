"""Tests for backend.routers.session_scoring — enable/disable/status/admin/analytics endpoints."""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.models.session_scoring import ScoringL2EnforceBody
from backend.repositories.session_scoring import _SCORING_COLUMN_TYPES
from backend.routers import session_scoring as _ss
from backend.routers import session_scoring_admin as _admin
from backend.routers.session_scoring_admin import _build_l2_enforce_block
from backend.scoring import labels as _labels
from backend.scoring.evaluate import EvaluationResult
from backend.scoring.matrix import MatrixStats, TransitionMatrix
from backend.utils.date_utils import parse_iso_utc
from tests._helpers.config_fakes import mutate_via_save

LOG_SVC = "TestScoringRouterSvc"
_SHIPPED = "backend.provision.session_scoring_orchestrator.shipped_scorer_identity"
_LABEL_PII_KEYS = ("notes", "flagged_by", "sample_ip", "sample_ua", "sample_url")
_LABEL_SAFE_KEYS = ("id", "service_id", "sid", "label", "created_at", "updated_at")
_ANALYST_COMPOSITE_KEYS = {"top_flagged", "score_distribution", "compliance_breakdown", "latency_timeseries", "health"}
_ADMIN_ONLY_COMPOSITE_KEYS = {"evaluation", "evaluation_per_reason"}


@pytest.fixture(autouse=True)
def _clear_analytics_cache_between_tests(monkeypatch):
    monkeypatch.setattr("backend.provision.fastly_api.service_has_scoring_pragmas", lambda *a, **kw: True)
    for cache in (_ss._analytics_cache, _ss._inflight, _ss._scoring_svc_version_cache, _ss._scoring_pragma_cache):
        cache.clear()
    yield
    for cache in (_ss._analytics_cache, _ss._inflight, _ss._scoring_svc_version_cache, _ss._scoring_pragma_cache):
        cache.clear()


def _parse_sse(resp) -> list[dict]:
    return [json.loads(line[len("data: ") :]) for line in resp.text.splitlines() if line.startswith("data: ")]


def _patch_run_with_events(
    events=({"type": "status", "message": "fake"},),
    *,
    capture_token: dict | None = None,
    on_done=None,
    raise_exc: Exception | None = None,
):
    def _fake(func, *args, **kwargs):
        if capture_token is not None:
            capture_token["t"] = args[1]
        yield from events
        if raise_exc is not None:
            raise raise_exc
        if on_done is not None:
            on_done()

    return patch("backend.provision.orchestrator.run_with_events", side_effect=_fake)


@contextmanager
def _patch_labels(labels: list[dict], counts: dict[str, int]):
    with (
        patch("backend.scoring.labels.list_labels", return_value=labels),
        patch("backend.scoring.labels.counts_by_label", return_value=counts),
    ):
        yield


@contextmanager
def _capture_fastly_and_audit(*, get_handler=None):
    fastly_calls: list[tuple] = []
    audit_calls: list[tuple] = []

    def fake_fastly(method, path, body=None, *args, **kwargs):
        if method == "GET" and get_handler is not None:
            return get_handler(path)
        fastly_calls.append((method, path, body))
        return {}

    def fake_audit(service_id, action, *, actor="operator", details=None):
        audit_calls.append((service_id, action, details))

    with (
        patch("backend.core.fastly.client.fastly", side_effect=fake_fastly),
        patch("backend.core.metadata.record_scoring_audit", side_effect=fake_audit),
    ):
        yield fastly_calls, audit_calls


def _health_row(**overrides) -> dict:
    return {
        "total_edge_rows": 1000,
        "scored_rows": 200,
        "distinct_sids": 50,
        "avg_score": 10.0,
        "p50_score": 0.0,
        "p95_score": 0.0,
        "max_score": 100,
        "scorer_errors": 0,
        "top_reasons": [],
        "l2_evaluated": 100,
        "l2_high_count": 5,
        **overrides,
    }


def _enabled_cfg(**scoring) -> dict:
    return {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scoring_drift", **scoring}}


def _enforce_cfg(**scoring) -> dict:
    return {
        "service_id": LOG_SVC,
        "fastly_api_key": "TOKEN",
        "scoring": {"enabled": True, "scoring_config_store_id": "cs_abc123", **scoring},
    }


def _capture_cfg_save(monkeypatch) -> dict:
    saved: dict = {}

    def save_fn(sid, cfg):
        saved.update(sid=sid, cfg=cfg)

    monkeypatch.setattr("backend.config.save_config", save_fn)
    monkeypatch.setattr("backend.config.mutate_config", mutate_via_save(save_fn))
    return saved


def _patch_query_logs(rows: list[dict]):
    return patch("backend.repositories.session_scoring.query_logs", return_value=rows)


def _fake_request(analyst_session):
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _assert_analyst_projected(row: dict, *, check_safe: bool = False) -> None:
    for k in _LABEL_PII_KEYS:
        assert k not in row, f"analyst response should NOT include {k!r}"
    if check_safe:
        for k in _LABEL_SAFE_KEYS:
            assert k in row, f"analyst response should include {k!r}"


# ── /scoring/status & edge drift ─────────────────────────────────────────────


@pytest.mark.parametrize(
    ("cfg", "expected_enabled", "expected_id"),
    [
        ({"service_id": LOG_SVC}, False, None),
        ({"service_id": LOG_SVC, "scoring": {"enabled": False}}, False, None),
        (
            {
                "service_id": LOG_SVC,
                "scoring": {
                    "enabled": True,
                    "scoring_service_id": "scoring_xyz",
                    "scoring_service_name": f"Session Scoring Service for {LOG_SVC}",
                    "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
                    "aes_key_hex": "secret-never-show",
                },
            },
            True,
            "scoring_xyz",
        ),
    ],
)
def test_status_variants(client, with_config, cfg, expected_enabled, expected_id):
    with_config[LOG_SVC] = cfg
    r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is expected_enabled
    if expected_id:
        assert body["scoring_service_id"] == expected_id
    assert "secret-never-show" not in r.text
    assert "aes_key_hex" not in body


def test_status_surfaces_preflight_pragmas_ready_when_disabled_and_token_present(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "tok_123", "scoring": {"enabled": False}}
    with patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=False) as mock_probe:
        r1 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        assert r1.status_code == 200 and r1.json()["enabled"] is False
        assert r1.json()["preflight_pragmas_ready"] is False
        assert mock_probe.call_count == 1

    with patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=True) as mock_probe2:
        r2 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        r3 = client.get(f"/api/services/{LOG_SVC}/scoring/status")
        assert r2.json()["preflight_pragmas_ready"] is True
        assert r3.json()["preflight_pragmas_ready"] is True
        assert mock_probe2.call_count == 1


@pytest.mark.parametrize(
    ("token", "info_return", "info_exc", "expect_version"),
    [
        (
            "tok_abc123",
            {"number": 42, "updated_at": "2026-06-17T18:41:04Z", "created_at": "2026-06-17T18:40:00Z"},
            None,
            42,
        ),
        (None, None, None, None),
        ("tok_abc123", None, RuntimeError("fastly down"), None),
    ],
)
def test_status_active_version_lookup(client, with_config, token, info_return, info_exc, expect_version):
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scoring_svc"}}
    if token:
        cfg["fastly_api_key"] = token
    with_config[LOG_SVC] = cfg
    kwargs = {"side_effect": info_exc} if info_exc else {"return_value": info_return}
    with patch("backend.core.fastly.service.get_active_version_info", **kwargs) as mock_info:
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    if expect_version is not None:
        assert body["scoring_active_version"] == expect_version
        assert body["scoring_activated_at"] == "2026-06-17T18:41:04Z"
        mock_info.assert_called_once_with("scoring_svc", token)
    else:
        assert "scoring_active_version" not in body
        if not token:
            mock_info.assert_not_called()


@pytest.mark.parametrize(
    ("deployed", "shipped", "shipped_exc", "expect_drift", "expect_detail"),
    [
        (
            {"deployed_package_sha": "old_pkg", "deployed_vcl_sha": "vcl1"},
            {"package_sha": "new_pkg", "vcl_sha": "vcl1"},
            None,
            True,
            "wasm",
        ),
        (
            {"deployed_package_sha": "pkg1", "deployed_vcl_sha": "old_vcl"},
            {"package_sha": "pkg1", "vcl_sha": "new_vcl"},
            None,
            True,
            "vcl",
        ),
        (
            {"deployed_package_sha": "old_pkg", "deployed_vcl_sha": "old_vcl"},
            {"package_sha": "new_pkg", "vcl_sha": "new_vcl"},
            None,
            True,
            "wasm+vcl",
        ),
        (
            {"deployed_package_sha": "pkg1", "deployed_vcl_sha": "vcl1"},
            {"package_sha": "pkg1", "vcl_sha": "vcl1"},
            None,
            False,
            None,
        ),
        ({}, {"package_sha": "anything", "vcl_sha": "anything"}, None, False, None),
        ({"deployed_package_sha": "pkg1", "deployed_vcl_sha": "vcl1"}, None, RuntimeError("hash boom"), False, None),
    ],
)
def test_status_scorer_drift(client, with_config, deployed, shipped, shipped_exc, expect_drift, expect_detail):
    with_config[LOG_SVC] = _enabled_cfg(**deployed)
    kwargs = {"side_effect": shipped_exc} if shipped_exc else {"return_value": shipped}
    with patch(_SHIPPED, **kwargs):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/status")
    assert r.status_code == 200
    body = r.json()
    assert body["scorer_drift"] is expect_drift
    assert body["drift_detail"] == expect_detail


def test_status_and_admin_service_id_validation(client, with_config):
    assert client.get("/api/services/does-not-exist/scoring/status").status_code == 404
    assert client.get("/api/services/no-such-service/scoring/audit").status_code == 404
    for bad_svc in ("svc;DROP", "svc.dot"):
        assert client.get(f"/api/services/{bad_svc}/scoring/threshold").status_code == 422


# ── /scoring/enable & /scoring/disable ───────────────────────────────────────


def test_enable_token_resolution(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": ""}
    r400 = client.post(f"/api/services/{LOG_SVC}/scoring/enable")
    assert r400.status_code == 400 and "token" in r400.json()["detail"]["error"].lower()

    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "FROM_CONFIG"}
    cap: dict = {}
    with _patch_run_with_events(capture_token=cap):
        assert client.post(f"/api/services/{LOG_SVC}/scoring/enable").status_code == 200
    assert cap["t"] == "FROM_CONFIG"

    with _patch_run_with_events(capture_token=cap):
        assert client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "FROM_QUERY"}).status_code == 200
    assert cap["t"] == "FROM_QUERY"


def test_enable_and_disable_sse_streams(client, with_config):
    cfg = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN"}
    with_config[LOG_SVC] = cfg
    enabled_cfg = {**cfg, "scoring": {"enabled": True, "scoring_service_id": "scoring_xyz"}}

    with _patch_run_with_events(
        [{"type": "status", "message": "step 1"}, {"type": "status", "message": "step 2"}],
        on_done=lambda: with_config.__setitem__(LOG_SVC, enabled_cfg),
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(r)
    types = [e["type"] for e in events]
    assert "status" in types and types[-1] == "done"
    assert events[-1]["scoring"]["scoring_service_id"] == "scoring_xyz"

    with _patch_run_with_events(
        [{"type": "status", "message": "got partway"}],
        raise_exc=RuntimeError("boom — validation failed at step 7"),
    ):
        r_err = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})
    err_events = _parse_sse(r_err)
    assert "error" in [e["type"] for e in err_events] and "done" not in [e["type"] for e in err_events]
    assert "validation" in next(e for e in err_events if e["type"] == "error")["message"].lower()

    with _patch_run_with_events([{"type": "status", "message": "tearing down"}]):
        r_dis = client.post(f"/api/services/{LOG_SVC}/scoring/disable", json={"token": "TOKEN"})
    assert r_dis.status_code == 200 and any(e["type"] == "done" for e in _parse_sse(r_dis))


def test_enable_blocked_when_preflight_pragmas_missing(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "fastly_api_key": "TOKEN", "scoring": {"enabled": False}}
    mutated = {"called": False}
    monkeypatch.setattr("backend.config.mutate_config", lambda *a, **kw: mutated.__setitem__("called", True))

    with (
        patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=False),
        patch("backend.provision.orchestrator.run_with_events") as mock_run,
    ):
        r = client.post(f"/api/services/{LOG_SVC}/scoring/enable", json={"token": "TOKEN"})

    assert r.status_code == 200 and mutated["called"] is False
    mock_run.assert_not_called()
    events = _parse_sse(r)
    assert len(events) == 1 and events[0]["type"] == "error" and events[0]["code"] == "missing_scoring_pragmas"
    msg = events[0]["message"]
    assert "account team" in msg.lower()
    for internal in ("fix_unsent_body_drain", "no_body_if_bereq_is_get_or_head", "hctl"):
        assert internal not in msg


# ── /scoring/labels CRUD & analyst PII projection ────────────────────────────


def _post_label(client, **payload):
    return client.post(f"/api/services/{LOG_SVC}/scoring/labels", json=payload)


def _patch_label(client, label_id: str, **payload):
    return client.patch(f"/api/services/{LOG_SVC}/scoring/labels/{label_id}", json=payload)


def test_labels_crud_and_validation(client):
    r = _post_label(client, sid="deadbeef1234", label="bad", notes="scraper", sample_ip="1.2.3.4")
    assert r.status_code == 200 and r.json()["sid"] == "deadbeef1234" and r.json()["label"] == "bad"

    listing = client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()
    assert "deadbeef1234" in [row["sid"] for row in listing["labels"]]
    assert listing["counts"]["bad"] >= 1

    assert _post_label(client, label="bad").status_code == 400
    assert _post_label(client, sid="abc", label="ugly").status_code == 400
    assert _post_label(client, sid="uncertain1", label="neutral").json()["label"] == "neutral"

    # Upsert on same sid
    r1 = _post_label(client, sid="samesid01", label="bad")
    r2 = _post_label(client, sid="samesid01", label="good", notes="actually fine")
    assert r1.json()["id"] == r2.json()["id"] and r2.json()["label"] == "good"
    matches = [
        row
        for row in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if row["sid"] == "samesid01"
    ]
    assert len(matches) == 1

    # Patch notes & invalid label
    created = _post_label(client, sid="patchme", label="bad", notes="initial").json()
    rp = _patch_label(client, created["id"], notes="revised")
    assert rp.status_code == 200 and rp.json()["notes"] == "revised" and rp.json()["label"] == "bad"
    assert _patch_label(client, created["id"], label="ugly").status_code == 400

    # Idempotent delete
    del_id = _post_label(client, sid="deleteme", label="bad").json()["id"]
    assert client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{del_id}").status_code == 200
    assert client.delete(f"/api/services/{LOG_SVC}/scoring/labels/{del_id}").status_code == 200


def test_labels_analyst_pii_projection_and_write_protection(client, monkeypatch):
    full = {
        "id": "abc",
        "service_id": "svc",
        "sid": "sid1",
        "label": "bad",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "notes": "user@example.com",
        "flagged_by": "analyst@example.com",
        "sample_ip": "1.2.3.4",
        "sample_ua": "Mozilla/5.0",
        "sample_url": "/admin/super-secret-path",
    }
    _assert_analyst_projected(_ss._project_label_for_analyst(full), check_safe=True)

    # Admin gets full PII
    created = _post_label(
        client,
        sid="admin-pii-test",
        label="bad",
        notes="admin-original",
        sample_ip="9.9.9.9",
        sample_ua="ua",
        sample_url="/x",
    ).json()
    admin_row = next(
        r
        for r in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if r["sid"] == "admin-pii-test"
    )
    for k in _LABEL_PII_KEYS:
        assert k in admin_row

    # Analyst GET/POST/PATCH responses omit PII, and write paths strip PII / protect notes
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    analyst_row = next(
        r
        for r in client.get(f"/api/services/{LOG_SVC}/scoring/labels").json()["labels"]
        if r["sid"] == "admin-pii-test"
    )
    _assert_analyst_projected(analyst_row, check_safe=True)

    r_post = _post_label(
        client,
        sid="analyst-write-pii",
        label="bad",
        notes="injected",
        flagged_by="spoofed-operator",
        sample_ip="9.9.9.9",
        sample_ua="evil-agent",
        sample_url="/admin/secret",
    )
    assert r_post.status_code == 200
    _assert_analyst_projected(r_post.json())
    stored = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == "analyst-write-pii")
    assert stored["label"] == "bad" and stored["notes"] == "" and stored["flagged_by"] == "admin"
    assert stored["sample_ip"] == "" and stored["sample_ua"] == "" and stored["sample_url"] == ""

    r_patch = _patch_label(client, created["id"], label="good", notes="analyst-overwrite")
    assert r_patch.status_code == 200
    _assert_analyst_projected(r_patch.json())
    stored_patched = next(row for row in _labels.list_labels(LOG_SVC) if row["sid"] == "admin-pii-test")
    assert stored_patched["notes"] == "admin-original" and stored_patched["label"] == "good"


# ── /scoring/analytics composite & read endpoints ────────────────────────────


def _patch_composite_subcalls(monkeypatch, *, eval_raises=False):
    monkeypatch.setattr(_ss, "scoring_top_flagged", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_score_distribution", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []})
    monkeypatch.setattr(_ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False})
    monkeypatch.setattr(_ss, "scoring_health", lambda **kw: {"ok": True})
    if eval_raises:

        def _boom(**kw):
            raise RuntimeError("scoring_evaluation must not be called on the analyst path")

        monkeypatch.setattr(_ss, "scoring_evaluation", _boom)
        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", _boom)
    else:
        monkeypatch.setattr(_ss, "scoring_evaluation", lambda **kw: {"score": 0.5})
        monkeypatch.setattr(_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []})


def test_analytics_composite_admin_vs_analyst(client, monkeypatch):
    _patch_composite_subcalls(monkeypatch)
    r_admin = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r_admin.status_code == 200
    assert (_ANALYST_COMPOSITE_KEYS | _ADMIN_ONLY_COMPOSITE_KEYS).issubset(r_admin.json().keys())

    _ss._analytics_cache.clear()
    _patch_composite_subcalls(monkeypatch, eval_raises=True)
    monkeypatch.setattr(_ss, "_is_analyst_request", lambda req: True)
    r_analyst = client.get(f"/api/services/{LOG_SVC}/scoring/analytics?since_hours=24")
    assert r_analyst.status_code == 200
    keys = set(r_analyst.json().keys())
    assert _ANALYST_COMPOSITE_KEYS.issubset(keys)
    assert keys.isdisjoint(_ADMIN_ONLY_COMPOSITE_KEYS)


def test_scoring_list_endpoints_and_bounds(client):
    with _patch_query_logs(
        [{"timestamp": "2026-06-01 10:00:00", "edge_sid": "aaa", "edge_score": 95, "ip": "1.1.1.1"}]
    ):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=24&limit=10")
        assert r.status_code == 200 and r.json()["since_hours"] == 24 and r.json()["rows"][0]["edge_sid"] == "aaa"
        assert client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=0").status_code == 422
        assert client.get(f"/api/services/{LOG_SVC}/scoring/top-flagged?since_hours=999").status_code == 422

    with _patch_query_logs([{"hour": "2026-06-01 10:00:00", "bucket": "75-100", "count": 5}]):
        r_dist = client.get(f"/api/services/{LOG_SVC}/scoring/score-distribution")
        assert r_dist.status_code == 200 and r_dist.json()["rows"][0]["bucket"] == "75-100"

    with _patch_query_logs([{"hour": "2026-06-01 10:00:00", "compliance": "ok", "count": 200}]):
        r_comp = client.get(f"/api/services/{LOG_SVC}/scoring/compliance-breakdown")
        assert r_comp.status_code == 200 and r_comp.json()["rows"][0]["compliance"] == "ok"


@pytest.mark.parametrize("has_latency_cols", [True, False])
def test_health_and_latency_timeseries_columns(client, monkeypatch, has_latency_cols):
    cols = {"edge_score", "edge_score_reason"}
    if has_latency_cols:
        cols |= {"edge_score_l2", "edge_cookie_compliance", "edge_sid", "edge_score_rtt_us", "edge_score_exec_us"}
    monkeypatch.setattr(_ss, "_table_columns", lambda sid: cols)

    h_row = (
        _health_row(
            rtt_p50_us=8000, rtt_p95_us=42000, rtt_p99_us=91000, rtt_max_us=100000, exec_p50_us=540, exec_p95_us=880
        )
        if has_latency_cols
        else _health_row()
    )
    with _patch_query_logs([h_row]):
        rh = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert rh.status_code == 200
    lat = rh.json()["latency"]
    assert lat["available"] is has_latency_cols
    assert lat["rtt_p95_us"] == (42000 if has_latency_cols else None)

    ts_row = {"hour": "2026-06-17 10:00:00", "scored_count": 500, "fail_open_count": 30, "total_count": 530}
    if has_latency_cols:
        ts_row.update(rtt_p50_us=8000, rtt_p95_us=45000, rtt_p99_us=95000, exec_p50_us=540, exec_p95_us=900)
    with _patch_query_logs([ts_row]):
        rts = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours=24")
    assert rts.status_code == 200
    assert rts.json()["has_latency"] is has_latency_cols
    assert rts.json()["rows"][0]["fail_open_count"] == 30


def test_latency_timeseries_buckets_by_minute_for_1h_window(client, monkeypatch):
    monkeypatch.setattr(_ss, "_table_columns", lambda sid: {"edge_score", "edge_score_reason"})
    captured: dict[str, str] = {}

    def _fake_query_logs(*args, **kwargs):
        captured["sql"] = next((a for a in [*args, *kwargs.values()] if isinstance(a, str) and "date_trunc" in a), "")
        return [{"hour": "2026-06-17 10:00:00", "scored_count": 1, "fail_open_count": 0}]

    with patch("backend.repositories.session_scoring.query_logs", side_effect=_fake_query_logs):
        for hours, gran in ((1, "minute"), (24, "hour")):
            _ss._analytics_cache.clear()
            r = client.get(f"/api/services/{LOG_SVC}/scoring/latency-timeseries?since_hours={hours}")
            assert r.status_code == 200 and r.json()["granularity"] == gran
            assert f"date_trunc('{gran}'" in captured["sql"]


def test_analytics_cache_bust_and_inflight_cleanup():
    _ss._analytics_cache.clear()
    _ss._analytics_cache[("top-flagged", "svc-a", 24, 50)] = (12345.0, {"rows": [1]})
    _ss._analytics_cache[("score-distribution", "svc-a", 24)] = (12345.0, {"rows": [2]})
    _ss._analytics_cache[("top-flagged", "svc-b", 24, 50)] = (12345.0, {"rows": [3]})

    _ss._bust_analytics_cache("svc-a")
    assert list(_ss._analytics_cache.keys()) == [("top-flagged", "svc-b", 24, 50)]

    _ss._bust_analytics_cache(None)
    assert len(_ss._analytics_cache) == 0

    key = ("test_endpoint", "svc-test", 24)
    _ss._cached(key, lambda: {"foo": 1})
    assert key not in _ss._inflight
    produced = {"flag": False}
    _ss._cached(key, lambda: produced.__setitem__("flag", True) or {"foo": 999})
    assert produced["flag"] is False and key not in _ss._inflight


# ── /scoring/evaluation, /curves, /threshold-preview, /retrain, /sessions, /health, /dashboard ──


@pytest.mark.parametrize(
    ("path", "flag_key"),
    [
        ("evaluation", "has_min_samples"),
        ("curves", "has_min_samples"),
        ("evaluation/per-reason", "has_min_samples_overall"),
    ],
)
def test_min_samples_cta_under_threshold(client, with_config, path, flag_key):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}
    with _patch_labels([], {"good": 1, "bad": 0, "neutral": 0}):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/{path}")
    assert r.status_code == 200
    body = r.json()
    assert body[flag_key] is False
    assert "auc" not in body


def test_evaluation_and_curves_when_min_samples_met(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "matrix_version": "2026-06-01-a"}}
    fake_labels = [{"sid": f"good{i}", "label": "good"} for i in range(3)] + [
        {"sid": f"bad{i}", "label": "bad"} for i in range(3)
    ]
    fake_counts = {"good": 3, "bad": 3, "neutral": 0}
    reconstructed = [({"session_id": f"good{i}", "events": [], "max_edge_score": i * 10}, "good") for i in range(3)] + [
        ({"session_id": f"bad{i}", "events": [], "max_edge_score": 75 + i * 5}, "bad") for i in range(3)
    ]
    fake_result = EvaluationResult(auc=0.85, pass_threshold=0.85, passed=True, n_good=3, n_bad=3)

    with (
        _patch_labels(fake_labels, fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value={"transitions": {}}),
        patch("backend.repositories.session_scoring.reconstruct_labeled_sessions", return_value=reconstructed),
        patch("backend.scoring.evaluate.evaluate_from_persisted_scores", return_value=fake_result),
    ):
        r_eval = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
        r_curves = client.get(f"/api/services/{LOG_SVC}/scoring/curves")

    assert r_eval.status_code == 200 and r_eval.json()["auc"] == 0.85 and r_eval.json()["passed"] is True
    c_body = r_curves.json()
    assert c_body["has_min_samples"] is True and c_body["auc"] == 1.0
    assert len(c_body["roc"]) == 101 and len(c_body["pr"]) == 101

    # Missing matrix graceful error
    _ss._analytics_cache.clear()
    with (
        _patch_labels(fake_labels, fake_counts),
        patch("backend.routers.session_scoring._load_matrix", return_value=None),
    ):
        r_missing = client.get(f"/api/services/{LOG_SVC}/scoring/evaluation")
    assert (
        r_missing.status_code == 200 and "matrix" in r_missing.json()["error"].lower() and "auc" not in r_missing.json()
    )


def test_threshold_preview_bucketing_and_extremes(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    aggregate_row = {
        "total": 6,
        "flagged_total": 4,
        "flagged_good": 1,
        "flagged_bad": 2,
        "passed_good": 1,
        "passed_bad": 1,
    }
    fake_labels = [{"sid": f"bad{i}", "label": "bad"} for i in (1, 2, 3)] + [
        {"sid": f"good{i}", "label": "good"} for i in (1, 2)
    ]

    with (
        _patch_query_logs([aggregate_row]),
        _patch_labels(fake_labels, {"good": 2, "bad": 3, "neutral": 0}),
        patch("backend.routers.session_scoring._bust_analytics_cache"),
    ):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=50&since_hours=24")
    body = r.json()
    assert body["threshold"] == 50 and body["flagged"] == {"bad": 2, "good": 1, "unlabeled": 1, "total": 4}
    assert body["passed"]["good"] == 1 and body["passed"]["bad"] == 1
    assert abs(body["precision"] - 2 / 3) < 0.01 and abs(body["recall"] - 2 / 3) < 0.01

    # Extreme thresholds (0 vs 100)
    _provisioned_cols = [{"column_name": c} for c in ("edge", *tuple(_SCORING_COLUMN_TYPES))]
    call_count = {"n": 0}

    def _route_query(_service_id, sql, *_args, **_kwargs):
        if sql.strip().upper().startswith("DESCRIBE"):
            return _provisioned_cols
        call_count["n"] += 1
        return (
            [{"total": 2, "flagged_total": 2, "passed_total": 0}]
            if call_count["n"] == 1
            else [{"total": 2, "flagged_total": 1, "passed_total": 1}]
        )

    with (
        patch("backend.repositories.session_scoring.query_logs", side_effect=_route_query),
        _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}),
    ):
        _ss._analytics_cache.clear()
        r_low = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=0")
        _ss._analytics_cache.clear()
        r_high = client.get(f"/api/services/{LOG_SVC}/scoring/threshold-preview?threshold=100")

    assert r_low.json()["flagged"]["total"] == 2
    assert r_high.json()["flagged"]["total"] == 1 and r_high.json()["passed"]["unlabeled"] == 1


def test_retrain_smoke(client, with_config, tmp_path):
    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "scoring_service_id": "scorer-x", "scoring_matrix_store_id": "MATRIX_STORE"},
    }
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
        _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}),
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", tmp_path / "matrix.json"),
        patch("backend.state_sync.publish_matrix_to_fos"),
        patch("backend.routers.session_scoring_admin.resolve_fastly_token", return_value="tok"),
        patch("backend.core.fastly.client.fastly_raw") as mock_raw,
    ):
        mock_get_con.return_value.close = lambda: None
        r = client.post(f"/api/services/{LOG_SVC}/scoring/retrain?since_days=7")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["sessions_trained_on"] == 5 and body["transitions"] == 12
    assert body["local_matrix_saved"] is True and body["matrix_kv_written"] is True
    assert mock_raw.call_args.args[1] == "/resources/stores/kv/MATRIX_STORE/keys/matrix"


def test_session_events_timeline_and_empty(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    base_ev = {
        "edge_sid": "abc123",
        "status": 200,
        "ip": "1.1.1.1",
        "ua": "browser",
        "edge_cookie_compliance": "ok",
        "edge_score_reason": "",
    }
    canned = [
        {**base_ev, "ts": "2026-06-02T20:00:00", "url": "/", "edge_score": 0},
        {**base_ev, "ts": "2026-06-02T20:00:05", "url": "/login", "edge_score": 10},
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/abc123/events")
    assert r.status_code == 200 and r.json()["event_count"] == 2 and r.json()["events"][1]["edge_score"] == 10

    with _patch_query_logs([]):
        r_empty = client.get(f"/api/services/{LOG_SVC}/scoring/sessions/nosuch/events")
    assert r_empty.status_code == 200 and r_empty.json()["event_count"] == 0


def test_scoring_health_wire_shape_and_fail_open_breakdown(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    canned = [
        _health_row(
            avg_score=45.5,
            p50_score=50.0,
            p95_score=75.0,
            scorer_errors=7,
            top_reasons=[{"reason": "cookie-missing", "count": 10}],
            fail_open_breakdown=[
                {"reason": "compute-unavailable-503", "count": 5},
                {"reason": "compute-unavailable-500", "count": 1},
                {"reason": "internal-error-keys", "count": 1},
            ],
        )
    ]
    with _patch_query_logs(canned):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    body = r.json()
    assert body["fire_rate_pct"] == 20.0 and body["distinct_sids"] == 50
    assert body["top_reasons"][0]["reason"] == "cookie-missing"
    assert body["matrix_staleness"]["is_stale"] is False
    assert body["fail_open_breakdown"][0] == {"reason": "compute-unavailable-503", "count": 5}
    assert body["fail_open_rate_pct"] == 0.7
    assert body["l2_enforce"]["available"] is False

    _ss._analytics_cache.clear()
    with _patch_query_logs([{"total_edge_rows": 10, "scored_rows": 10, "top_reasons": []}]):
        r_empty = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24")
    assert r_empty.json()["fail_open_breakdown"] == []


def test_scoring_dashboard_returns_all_subobjects(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "scoring_service_id": "scorer-x"}}
    with _patch_labels([], {"good": 0, "bad": 0, "neutral": 0}), _patch_query_logs([]):
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
        "config_threshold",
        "exclude_regex",
        "enforce_status_code",
    ):
        assert key in body
    assert "threshold" in body["config_threshold"] and "enforced" in body["config_threshold"]
    assert "current" in body["exclude_regex"] and "default" in body["exclude_regex"]
    assert "current" in body["enforce_status_code"] and "effective" in body["enforce_status_code"]


# ── /scoring/threshold, /matrix-versions, /audit ────────────────────────────


def test_scoring_threshold_get_and_put(client, with_config, monkeypatch):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    r_get = client.get(f"/api/services/{LOG_SVC}/scoring/threshold")
    assert r_get.status_code == 200 and r_get.json()["threshold"] is None and r_get.json()["enforced"] is False

    saved = _capture_cfg_save(monkeypatch)
    r_put = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 80})
    assert (
        r_put.status_code == 200
        and r_put.json()["threshold"] == 80
        and saved["cfg"]["scoring"]["operator_threshold"] == 80
    )

    assert client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": 150}).status_code == 400

    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True, "operator_threshold": 80}}
    r_clear = client.put(f"/api/services/{LOG_SVC}/scoring/threshold", json={"threshold": None})
    assert r_clear.status_code == 200 and r_clear.json()["threshold"] is None
    assert "operator_threshold" not in saved["cfg"]["scoring"]


def test_matrix_versions_list_and_restore(client, with_config, monkeypatch, tmp_path):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    with patch("backend.state_sync.list_scoring_matrix_versions", return_value=[]):
        r0 = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions").json()
    assert r0["versions"] == [] and r0["current_version"] is None

    with_config[LOG_SVC] = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "matrix_version": "2026-06-03-c", "scoring_matrix_store_id": "MATRIX_STORE"},
    }
    fake_versions = [
        {
            "version": v,
            "key": f"iceberg/meta/scoring_matrix_history/{v}.json",
            "size_bytes": 4000,
            "last_modified": f"{v[:10]}T10:00:00+00:00",
        }
        for v in ("2026-06-03-c", "2026-06-02-b", "2026-06-01-a")
    ]
    with patch("backend.state_sync.list_scoring_matrix_versions", return_value=fake_versions):
        r_list = client.get(f"/api/services/{LOG_SVC}/scoring/matrix-versions").json()
    assert len(r_list["versions"]) == 3 and r_list["current_version"] == "2026-06-03-c"

    # Restore validation gates
    with patch("backend.state_sync.restore_scoring_matrix_version") as mock_restore:
        assert client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore").status_code == 400
        assert client.post(
            f"/api/services/{LOG_SVC}/scoring/matrix-versions/..%2Fetc%2Fpasswd/restore?confirm=true"
        ).status_code in (404, 405, 422)
        assert (
            client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/bad;rm-rf/restore?confirm=true").status_code
            == 422
        )
        assert (
            client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/{'a' * 65}/restore?confirm=true").status_code
            == 422
        )
        mock_restore.assert_not_called()

    # Restore 404 when missing in FOS
    with patch("backend.state_sync.restore_scoring_matrix_version", return_value=None):
        assert (
            client.post(
                f"/api/services/{LOG_SVC}/scoring/matrix-versions/no-such-version/restore?confirm=true"
            ).status_code
            == 404
        )

    # Restore 500 when state_sync raises
    no_reraise_client = TestClient(app, raise_server_exceptions=False)
    with patch("backend.state_sync.restore_scoring_matrix_version", side_effect=RuntimeError("S3 reset")):
        assert (
            no_reraise_client.post(
                f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true"
            ).status_code
            == 500
        )

    # Restore happy path
    saved = _capture_cfg_save(monkeypatch)
    fake_shared_path = tmp_path / "matrix.json"
    fake_matrix_path = tmp_path / f"matrix_{LOG_SVC}.json"
    fake_matrix_path.write_text('{"transitions": {}}')
    audit_calls: list = []
    with (
        patch(
            "backend.state_sync.restore_scoring_matrix_version",
            return_value={"version": "2026-06-01-a", "restored_at": "2026-06-03T11:00:00+00:00"},
        ),
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_shared_path),
        patch(
            "backend.core.metadata.record_scoring_audit",
            side_effect=lambda svc, action, details=None: audit_calls.append((svc, action, details)),
        ),
        patch("backend.routers.session_scoring_admin.resolve_fastly_token", return_value="tok"),
        patch("backend.provision.session_scoring_orchestrator._write_matrix_to_kv") as mock_kv,
    ):
        r_ok = client.post(f"/api/services/{LOG_SVC}/scoring/matrix-versions/2026-06-01-a/restore?confirm=true")
    assert (
        r_ok.status_code == 200
        and r_ok.json()["restored_version"] == "2026-06-01-a"
        and r_ok.json()["matrix_kv_written"] is True
    )
    mock_kv.assert_called_once_with("MATRIX_STORE", LOG_SVC, "tok")
    assert not fake_matrix_path.exists()
    assert saved["cfg"]["scoring"]["matrix_version"] == "2026-06-01-a"
    assert any(a == "matrix_restored" for _, a, _ in audit_calls)


def test_audit_readout_and_params(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    captured: dict = {}

    def fake_list(service_id, *, limit, since=None):
        captured.update(service_id=service_id, limit=limit, since=since)
        return [
            {
                "id": 3,
                "timestamp": "2026-06-03T10:00:00",
                "action": "threshold_committed",
                "actor": "operator",
                "details": {"new_threshold": 80},
            }
        ]

    with patch("backend.core.metadata.list_scoring_audit", side_effect=fake_list):
        r = client.get(f"/api/services/{LOG_SVC}/scoring/audit?since=2026-06-03T09:00:00")
    assert r.status_code == 200 and r.json()["limit"] == 100 and r.json()["audit"][0]["id"] == 3
    assert captured == {"service_id": LOG_SVC, "limit": 100, "since": "2026-06-03T09:00:00"}

    with patch("backend.core.metadata.list_scoring_audit", return_value=[]):
        assert client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1000").status_code == 200
        assert client.get(f"/api/services/{LOG_SVC}/scoring/audit?limit=1001").status_code == 422


# ── /scoring/enforce-threshold, /l2-enforce, /enforce-status-code ───────────


def test_enforce_threshold_get_and_put(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").status_code == 400
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": 75}
        ).status_code
        == 400
    )

    from backend.utils.bounded_cache import CacheRegistry

    with_config[LOG_SVC] = _enforce_cfg()
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly API 404: not found")):
        r404 = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").json()
    assert r404["threshold"] is None and r404["enforced"] is False

    CacheRegistry.clear_all()
    with patch("backend.core.fastly.client.fastly", return_value={"item_key": "enforce_threshold", "item_value": "75"}):
        r_set = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").json()
    assert r_set["threshold"] == 75 and r_set["enforced"] is True

    CacheRegistry.clear_all()
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly API 503: unavailable")):
        assert client.get(f"/api/services/{LOG_SVC}/scoring/enforce-threshold").status_code == 502

    assert client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold", json={"threshold": 75}).status_code == 400
    for bad in (150, -5):
        assert (
            client.put(
                f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": bad}
            ).status_code
            == 400
        )

    with _capture_fastly_and_audit() as (fastly_calls, audit_calls):
        r_put = client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": 75})
    assert r_put.status_code == 200 and r_put.json()["threshold"] == 75 and r_put.json()["enforced"] is True
    assert fastly_calls[0][2]["item_value"] == "75"
    assert audit_calls == [(LOG_SVC, "threshold_enforced", {"threshold": 75})]

    with _capture_fastly_and_audit() as (fastly_calls_clr, audit_calls_clr):
        r_clr = client.put(f"/api/services/{LOG_SVC}/scoring/enforce-threshold?confirm=true", json={"threshold": None})
    assert r_clr.status_code == 200 and r_clr.json()["threshold"] is None and r_clr.json()["enforced"] is False
    assert fastly_calls_clr[0][2]["item_value"] == ""
    assert audit_calls_clr == [(LOG_SVC, "threshold_enforce_disabled", {"threshold": None})]


def test_build_l2_enforce_block_ramp_math():
    day = 86_400
    now = 1_000_000_000
    b = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 10 * day),
        l2_enforce_raw="1",
        l2_enabled_at_raw=str(now - int(1.5 * day)),
        now_epoch=now,
    )
    assert b["available"] is True and b["enabled"] is True and b["ready"] is True and b["fully_ramped"] is False
    assert b["ramp_progress"] == pytest.approx(0.5, abs=1e-6)

    off = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 30 * day),
        l2_enforce_raw="0",
        l2_enabled_at_raw=str(now - 30 * day),
        now_epoch=now,
    )
    assert off["enabled"] is False and off["fully_ramped"] is True and off["ready"] is True

    fresh = _build_l2_enforce_block(
        scoring_enabled_at_raw=str(now - 2 * day), l2_enforce_raw="1", l2_enabled_at_raw=str(now), now_epoch=now
    )
    assert fresh["ramp_progress"] == 0.0 and fresh["fully_ramped"] is False and fresh["ready"] is False

    none = _build_l2_enforce_block(
        scoring_enabled_at_raw=None, l2_enforce_raw=None, l2_enabled_at_raw=None, now_epoch=now
    )
    assert none["enabled"] is False and none["days_since_optin"] is None and none["ready"] is False
    assert ScoringL2EnforceBody().enabled is False and ScoringL2EnforceBody(enabled=True).enabled is True


def _get_l2_saturated(path: str) -> dict:
    if path.endswith("/item/l2_enforce_enabled"):
        return {"item_value": "1"}
    if path.endswith(("/item/l2_enabled_at", "/item/scoring_enabled_at")):
        return {"item_value": "1700000000"}
    raise AssertionError(f"unexpected GET {path}")


def test_l2_enforce_get_and_put(client, with_config):
    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce").status_code == 400

    with_config[LOG_SVC] = _enforce_cfg()
    with _capture_fastly_and_audit(get_handler=_get_l2_saturated):
        r_get = client.get(f"/api/services/{LOG_SVC}/scoring/l2-enforce").json()
    assert r_get["available"] is True and r_get["enabled"] is True and r_get["fully_ramped"] is True

    with (
        _patch_query_logs([_health_row(total_edge_rows=100, scored_rows=50, l2_evaluated=0, l2_high_count=0)]),
        _capture_fastly_and_audit(get_handler=_get_l2_saturated),
    ):
        rh = client.get(f"/api/services/{LOG_SVC}/scoring/health?since_hours=24").json()
    assert rh["l2_enforce"]["available"] is True and rh["l2_enforce"]["enabled"] is True

    assert client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce", json={"enabled": True}).status_code == 400

    # Enable from off writes flag + anchor
    with _capture_fastly_and_audit(
        get_handler=lambda _: (_ for _ in ()).throw(RuntimeError("Fastly API 404: not found"))
    ) as (writes, audits):
        r_en = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": True}).json()
    assert r_en["enabled"] is True and isinstance(r_en["l2_enabled_at"], int)
    assert any((p or {}).get("item_value") == "1" for _, _, p in writes)
    assert audits[0][1] == "l2_enforce_enabled"

    # Re-enable when already on preserves anchor
    with _capture_fastly_and_audit(get_handler=_get_l2_saturated) as (writes_on, _):
        r_on = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": True}).json()
    assert r_on["l2_enabled_at"] == 1700000000
    assert not any(
        path.endswith("/item/l2_enabled_at") or (p or {}).get("item_key") == "l2_enabled_at" for _, path, p in writes_on
    )

    # Disable writes "0"
    with _capture_fastly_and_audit(
        get_handler=lambda _: (_ for _ in ()).throw(RuntimeError("Fastly API 404: not found"))
    ) as (writes_off, audits_off):
        r_off = client.put(f"/api/services/{LOG_SVC}/scoring/l2-enforce?confirm=true", json={"enabled": False}).json()
    assert r_off["enabled"] is False and audits_off[0][1] == "l2_enforce_disabled"


def test_enforce_status_code_get_and_put(client, with_config):
    with_config[LOG_SVC] = _enforce_cfg()
    r_def = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code").json()
    assert r_def["current"] is None and r_def["effective"] == 429 and r_def["is_default"] is True

    with_config[LOG_SVC] = _enforce_cfg(enforce_status_code=403)
    r_ovr = client.get(f"/api/services/{LOG_SVC}/scoring/enforce-status-code").json()
    assert r_ovr["current"] == 403 and r_ovr["effective"] == 403 and r_ovr["is_default"] is False

    assert (
        client.put(f"/api/services/{LOG_SVC}/scoring/enforce-status-code", json={"status_code": 403}).status_code == 400
    )
    for bad in (399, 600, 200, 0, -1):
        assert (
            client.put(
                f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": bad}
            ).status_code
            == 400
        )
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": "not-an-int"}
        ).status_code
        == 422
    )

    with_config[LOG_SVC] = {"service_id": LOG_SVC}
    assert (
        client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": 403}
        ).status_code
        == 400
    )

    with_config[LOG_SVC] = _enforce_cfg()
    with (
        patch(
            "backend.provision.session_scoring_orchestrator.update_enforce_status_code",
            side_effect=lambda sid, tok, *, new_status_code: {
                "effective_status_code": new_status_code or 429,
                "is_default": new_status_code is None,
                "logging_service_active_version": 42,
            },
        ),
        _capture_fastly_and_audit() as (_, audit_calls),
    ):
        r_set = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": 451}
        ).json()
        r_rst = client.put(
            f"/api/services/{LOG_SVC}/scoring/enforce-status-code?confirm=true", json={"status_code": None}
        ).json()

    assert r_set["effective_status_code"] == 451 and r_set["is_default"] is False
    assert r_rst["effective_status_code"] == 429 and r_rst["is_default"] is True
    assert audit_calls[0][1] == "scoring_enforce_status_code_changed"


# ── H3: analyst time-window clamp & _scoring_source degradation ──────────────


def test_scoring_time_window_and_endpoint_clamping(monkeypatch):
    pred_admin, disc_admin = _ss._scoring_time_window(_fake_request(None), 24)
    assert pred_admin == "timestamp >= now() - INTERVAL 24 HOUR" and disc_admin is None

    session = SimpleNamespace(
        query_window_hours=1, query_start_time=None, query_end_time=None, pii_policy={"mask_ips": True}
    )
    pred, disc = _ss._scoring_time_window(_fake_request(session), 168)
    assert "INTERVAL" not in pred and disc is not None
    lits = re.findall(r"TIMESTAMPTZ '([^']+)'", pred)
    span_h = (parse_iso_utc(lits[1]) - parse_iso_utc(lits[0])).total_seconds() / 3600
    assert 0.9 <= span_h <= 1.1

    captured: dict = {}
    monkeypatch.setattr(
        "backend.repositories.session_scoring.query_logs",
        lambda sid, sql, params=(): (
            captured.__setitem__("sql", sql) or [{"ip": "1.2.3.4", "ua": "Mozilla", "url": "/secret", "edge_score": 91}]
        ),
    )
    out_analyst = _ss.scoring_top_flagged(request=_fake_request(session), service_id="svc", since_hours=168, limit=50)
    assert "TIMESTAMPTZ" in captured["sql"] and out_analyst["rows"][0]["url"] == "/secret"

    out_admin = _ss.scoring_top_flagged(request=_fake_request(None), service_id="svc", since_hours=24, limit=50)
    assert "INTERVAL 24 HOUR" in captured["sql"] and out_admin["rows"][0]["url"] == "/secret"

    monkeypatch.setattr(
        _admin,
        "_fetch_session_events",
        lambda sid, sids, since_days=30, limit_per_sid=500, *, ts_predicate=None: (
            captured.__setitem__("ts_predicate", ts_predicate)
            or {sids[0]: [{"ts": "t", "url": "/secret", "status": 200, "ip": "1.2.3.4", "ua": "Mozilla"}]}
        ),
    )
    _admin.scoring_session_events(request=_fake_request(session), service_id="svc", sid="abc", since_days=90)
    assert captured["ts_predicate"] is not None and "TIMESTAMPTZ" in captured["ts_predicate"]
    _admin.scoring_session_events(request=_fake_request(None), service_id="svc", sid="abc", since_days=90)
    assert captured["ts_predicate"] is None


def test_scoring_source_and_pre_enablement_degradation(monkeypatch):
    assert _ss._scoring_source("logs_x", set(_SCORING_COLUMN_TYPES) | {"edge", "timestamp"}) == "logs_x"
    assert _ss._scoring_source("logs_x", set()) == "logs_x"
    src = _ss._scoring_source("logs_x", {"edge", "timestamp", "ip"})
    assert src.startswith("(SELECT *, ") and src.endswith(" FROM logs_x)")
    for col, typ in _SCORING_COLUMN_TYPES.items():
        assert f"CAST(NULL AS {typ}) AS {col}" in src
    assert "edge_score_rtt_us" not in src

    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE logs_noscore (timestamp TIMESTAMP, edge BOOLEAN, ip VARCHAR, ua VARCHAR, url VARCHAR, status INTEGER, country VARCHAR)"
        )
        con.execute("INSERT INTO logs_noscore VALUES (now()::TIMESTAMP, true, '1.2.3.4', 'UA', '/', 200, 'US')")

        def fake_query_logs(service_id, sql, params=()):
            cur = con.execute(sql, params) if params else con.execute(sql)
            rows = cur.fetchall()
            cnames = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cnames, r)) for r in rows]

        monkeypatch.setattr("backend.repositories.session_scoring.query_logs", fake_query_logs)
        req = _fake_request(None)
        assert _ss.scoring_top_flagged(request=req, service_id="noscore", since_hours=24, limit=50)["rows"] == []
        assert _ss.scoring_score_distribution(request=req, service_id="noscore", since_hours=24)["rows"] == []
        assert _ss.scoring_compliance_breakdown(request=req, service_id="noscore", since_hours=24)["rows"] == []
        assert _ss.scoring_latency_timeseries(request=req, service_id="noscore", since_hours=24)["has_latency"] is False
        assert (
            _ss.scoring_threshold_preview(request=req, service_id="noscore", threshold=75, since_hours=24)[
                "total_scored_sessions"
            ]
            == 0
        )
        health = _ss.scoring_health(request=req, service_id="noscore", since_hours=24)
        assert health["scored_rows"] == 0 and health["fire_rate_pct"] == 0.0 and health["top_reasons"] == []
    finally:
        con.close()
